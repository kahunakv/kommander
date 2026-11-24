# Partition Quiescence — Developer Guide

Quiescence stops the heartbeats of an idle partition. In a cluster with hundreds or thousands of mostly idle
partitions, this removes most of the keep-alive traffic. The partition wakes when a write arrives or when the
leader's node looks dead.

Quiescence builds on the SWIM failure detector. If the terms `Alive`, `Suspect` and `Dead` are new to you, read
the *Failure detection (SWIM)* section of the
[Dynamic Membership Developer Guide](dynamic-membership-developer-guide.md).

---

## Table of contents

1. [Summary](#summary)
2. [Node liveness and partition leadership](#node-liveness-and-partition-leadership)
3. [Core concepts](#core-concepts)
4. [Flow 1 — A leader quiesces an idle partition](#flow-1--a-leader-quiesces-an-idle-partition)
5. [Flow 2 — A quiesced follower stays calm](#flow-2--a-quiesced-follower-stays-calm)
6. [Flow 3 — A write wakes the partition](#flow-3--a-write-wakes-the-partition)
7. [Flow 4 — Failover when a quiesced leader dies](#flow-4--failover-when-a-quiesced-leader-dies)
8. [Flow 5 — Check-quorum probes a quiesced leader](#flow-5--check-quorum-probes-a-quiesced-leader)
9. [Failover timing rule](#failover-timing-rule)
10. [Configuration](#configuration)
11. [Code map](#code-map)
12. [Invariants you must not break](#invariants-you-must-not-break)
13. [Testing](#testing)

---

## Summary

Each Raft leader sends heartbeats for each partition it leads. With `N` nodes and `M` partitions, that is
`O(N × M)` messages per interval, and for idle partitions they carry no data.

> When a partition has had no writes for a while, its leader sends **one final quiesce marker** to the
> followers and **stops heartbeating** that partition. The followers then watch the **SWIM failure detector**,
> which already tracks whether the leader's node is alive, once per node. When a write arrives or the leader's
> node looks dead, the partition **wakes up** and normal Raft resumes.

```
   BEFORE quiescence (idle partition):     AFTER quiescence (idle partition):
   leader ──hb──► follower  (every 500ms)  leader        follower
   leader ──hb──► follower                        (silent — no per-partition heartbeats)
   leader ──hb──► follower                  SWIM ──ping──► (once per node, shared by all partitions)
   ... forever, per partition ...           follower watches SWIM, stays calm
```

---

## Node liveness and partition leadership

A Raft heartbeat has a node-scoped job and a partition-scoped job.

| Heartbeat job | Scope | Can SWIM cover it? |
|---|---|---|
| "This node is alive / reachable" | **node** | **Yes** — SWIM does this once per node |
| "I am the leader of *this partition* in term *T*; suppress your election timer" | **partition** | **No** — A can lead P1 but follow P5 |

A node ping cannot replace the leadership assertion. Do not coalesce the heartbeats of several partitions into
one message: the heartbeat of one partition then suppresses the others, and followers go into perpetual
elections. Quiescence stops sending the assertion only while the partition is idle.

---

## Core concepts

| Term | Meaning |
|---|---|
| **Heartbeat** | A (usually empty) `AppendLogs` that asserts leadership of one partition. |
| **Quiesced** | A per-partition, per-node flag: skip the heartbeats (leader) or the election timer (follower). Decided **locally** and not replicated. |
| **Quiesce marker** | The leader's final `AppendLogs` with `Quiesce = true`. Followers enter the quiesced state at a known log point. |
| **`Alive` / `Suspect` / `Dead`** | SWIM's view of a node. `Suspect` fires after about one `PingInterval` of failed probes; `Dead` after `SuspicionTimeout`. |
| **`lastProposalAt`** | Per-partition timestamp of the last *real* (non-heartbeat) append. Seeded at election. |
| **Wake / un-quiesce** | Return to normal heartbeats and election timing: on a write, an inbound append, or when the leader's node is no longer `Alive`. |

Each node decides from local observations. A wrong guess cannot violate Raft safety. It costs at most one extra
heartbeat or one extra election cycle.

---

## Flow 1 — A leader quiesces an idle partition

On a leadership tick, a leader quiesces a partition when **all** of these conditions hold.

- Quiescence is enabled (`EnableQuiescence`).
- The partition isn't quiesced yet.
- No proposal is short of quorum. A settled proposal that is retained for ticket lookup does not count.
- No peer is behind the leader's committed frontier.
- The partition has been idle for `QuiesceAfter`, measured from `lastProposalAt`.

`lastProposalAt` is seeded at election and refreshed on each write. A new leader of a never-written partition
will quiesce it `QuiesceAfter` later.

The leader then sends the quiesce marker to the followers, sets `quiesced = true`, and sends no more periodic
heartbeats. It is still the leader: it answers reads and will replicate the next write. If a peer falls behind
or a new peer appears, the leader wakes to bring it up to date.

```
   leader (partition idle > QuiesceAfter):
     ── AppendLogs { Quiesce = true } ──► follower 1
     ── AppendLogs { Quiesce = true } ──► follower 2
     then: stop sending heartbeats for this partition
```

---

## Flow 2 — A quiesced follower stays calm

A quiesced follower ignores its election timer. On each leadership tick it checks two facts about the leader's
node.

```
   quiesced follower tick:
     leaderNode = expectedLeaders[currentTerm]
     if leaderNode is no longer a peer of this partition → un-quiesce and start a pre-vote election
     if SWIM.GetState(leaderNode) == Alive  → do nothing (stay calm)
     else (Suspect or Dead)                 → un-quiesce and start a pre-vote election
```

The peer check is necessary because SWIM knows only node liveness. A leader whose replica was removed from the
range, for example by a placement trim, continues to answer pings. If the follower looked at liveness alone, the
range would have no leader until the next write.

The same check exempts the pre-vote deny gate. When a partition-map change drops a partition's known leader, the
partition also returns to the hot set. This means that the vacancy is noticed within `CheckLeaderInterval`,
before the next safety sweep (`UpdateNodesInterval`).

---

## Flow 3 — A write wakes the partition

Quiescence is invisible to clients.

- **Leader side:** a client proposal clears `quiesced`, records `lastProposalAt = now`, appends the entry, and
  resumes heartbeats. The entry replicates as an ordinary `AppendLogs` (`Quiesce = false`).
- **Follower side:** any `AppendLogs` with `Quiesce = false` clears the follower's `quiesced` flag and
  resets its heartbeat timer.

```
   client ──write──► leader
     leader: quiesced = false; append; resume heartbeats
     ── AppendLogs { Quiesce = false, logs = [entry] } ──► followers
     followers: quiesced = false; normal replication resumes
```

---

## Flow 4 — Failover when a quiesced leader dies

Quiescence must not delay recovery. The follower un-quiesces on `Suspect`, which arrives before `Dead`.

```
   leader node crashes / is partitioned
      │
      ▼
   SWIM probes from followers fail → leader marked Suspect  (~ one PingInterval)
      │
      ▼
   quiesced follower sees GetState(leader) != Alive
      → un-quiesces, runs a normal pre-vote election
      │
      ▼
   new leader elected; partition resumes (un-quiesced)
```

> **Limitation:** if the leader's node still answers SWIM pings but stops driving one specific partition (for
> example, that partition's executor wedges), quiesced followers do not elect a new leader for it.

---

## Flow 5 — Check-quorum probes a quiesced leader

A quiesced leader receives no acks by design. With `EnableCheckQuorum` on (the default), the leader tick measures
the time since a majority of voters was last simultaneously fresh. After half of `CheckQuorumWindow`, the leader
un-quiesces and forces one heartbeat round.

In a healthy cluster the voters answer and the leader re-quiesces on its next heartbeat tick. An idle partition
therefore costs one probe round per half window. A leader that is cut off from its voters steps down when the
full window elapses, the same bound as for a non-quiesced leader. Followers leave quiescence on the probe and
re-enter it on the next marker.

## Failover timing rule

After a leader dies, SWIM marks its node `Suspect` after about one `PingInterval`, and `Dead` one
`SuspicionTimeout` later (5s by default). Followers fail over on `Suspect`, so this signal must arrive before
the election timer would have fired. `RaftConfiguration.Validate()` enforces two rules, and the constructor
throws if one is violated.

> **`PingInterval < StartElectionTimeout`**

> **`EnableQuiescence` requires `PingInterval > 0`** — with SWIM disabled, a quiesced follower could not notice
> a dead leader.

Both checks are skipped when `EnableQuiescence = false`.

---

## Configuration

| Key | Default | Meaning |
|---|---|---|
| `EnableQuiescence` | `true` | Master switch. `false` restores per-partition heartbeats on each interval. |
| `QuiesceAfter` | `1500 ms` | How long a partition must be idle before its leader quiesces it. ≈ 3× `HeartbeatInterval`. |

Quiescence also uses the SWIM and election settings.

| Key | Default | Role in quiescence |
|---|---|---|
| `PingInterval` | `1 s` | SWIM probe cadence, and the approximate time to detect a dead leader. **Must be `> 0` and `< StartElectionTimeout` when quiescence is on.** |
| `SuspicionTimeout` | `5 s` | `Suspect → Dead` delay. Does **not** gate failover latency. |
| `StartElectionTimeout` | `2000 ms` | Upper bound for `PingInterval`. |

A lower `QuiesceAfter` makes idle partitions go quiet sooner, with more quiesce/wake churn under bursty load. A
lower `PingInterval` detects a dead leader sooner and sends more SWIM probes.

---

## Code map

| What | Where |
|---|---|
| Config knobs + `Validate()` invariants | `Kommander/RaftConfiguration.cs` (`EnableQuiescence`, `QuiesceAfter`, `Validate`) |
| SWIM state exposed to the state machine | `Kommander/Scheduling/IRaftPartitionHost.cs` → `GetNodeLiveness`; impl `RaftPartitionHostAdapter` → `RaftManager.Liveness.GetState` |
| SWIM failure detector | `Kommander/Gossip/LivenessTable.cs`; probes in `GossipService.PingAsync` / `ReceivePing` |
| Quiesce state + idle clock | `Kommander/Consensus/RaftPartitionCoreState.cs` (`Quiesced`, `LastProposalAt`) |
| Leader quiesce decision | `RaftPartitionStateMachine.CheckPartitionLeadershipAsync` (leader branch) |
| Quiesced follower election gate | `CheckPartitionLeadershipAsync` (quiesced-follower branch) |
| Heartbeat suppression | `HeartbeatDriver.SendHeartbeat` (early-returns when quiesced and not `force`) |
| Quiesce marker broadcast | `HeartbeatDriver.SendQuiesceMarker` → `AppendLogToNode(..., quiesce: true)` |
| Leader bookkeeping at election (seeds `lastProposalAt`) | `RaftPartitionStateMachine.BecomeLeaderAsync` |
| Follower applies / clears the flag | `FollowerAppendHandler.AppendLogsCoreAsync` (`SetQuiesced(quiesce)`) |
| Un-quiesce on a new write | `LogReplicator` proposal entry point |
| Wire field | `Data/AppendLogsRequest.cs` (`Quiesce`); `Data/RaftRequest.cs`; proto `Communication/Grpc/Protos/raft.proto` (`Quiesce = 10`); mapped in `RaftService` + `GrpcCommunication` (both single and batch paths) |

---

## Invariants you must not break

- **Quiescence is local, never replicated.** Do not put the `quiesced` flag or `lastProposalAt` in the log, and
  do not make a quiesce decision depend on another node's quiesce state. That would couple safety to a local
  optimization.
- **A quiesced follower must still fail over.** It gates elections on SWIM `Suspect` (≠ `Alive`). Keep
  `PingInterval < StartElectionTimeout` so this is no slower than a normal election.
- **Check hosting before liveness.** A leader that left the partition's replica set is vacated even while its node
  is `Alive`. Both quiesced gates (the follower tick and the pre-vote deny) check peer membership.
- **Quiescence requires SWIM.** Don't weaken the guard that fails construction for `EnableQuiescence = true` with
  `PingInterval <= 0`.
- **Any normal append wakes a follower.** `AppendLogsCoreAsync` mirrors the message's `Quiesce` flag. Don't add a
  code path that appends and leaves the flag as it was.
- **Election entry seeds `lastProposalAt`.** A new become-leader path that skips `BecomeLeaderAsync` would leave
  idle partitions unable to quiesce.
- **The election/vote rules are untouched.** Quiescence changes when a follower starts to campaign. The term
  checks, log-up-to-date checks, and quorum math stay the same.

---

## Testing

| Layer | File | Coverage |
|---|---|---|
| Unit | `Kommander.Tests/Scheduling/TestQuiescedLeader.cs` | Marker after the idle window; heartbeat suppression; no quiesce when `lastProposalAt == 0` |
| Unit | `Kommander.Tests/Scheduling/TestQuiescedFollower.cs` | Calm while the leader is `Alive`; pre-vote on `Suspect`/`Dead`; an append clears the flag |
| Config | `Kommander.Tests/TestRaftConfigurationValidation.cs` | Both startup rules, and that they are skipped when quiescence is off |
| Integration | `Kommander.Tests/TestQuiescence.cs` | See below |

The integration tests use real in-memory clusters and are `[Collection]`-serialized.

- An idle cluster has one stable leader across several election-timeout windows.
- The heartbeat counter (`raft.heartbeats_sent_total`) stops advancing after quiesce.
- A write after quiesce un-quiesces, commits, and replicates to all nodes.
- A partitioned quiesced leader is replaced well before `SuspicionTimeout`.
- With `EnableQuiescence = false`, heartbeats continue.

Tests that disable SWIM (`PingInterval = 0`) or use fast election timeouts set `EnableQuiescence = false` to
satisfy the startup rules.
