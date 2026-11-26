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

A quiesced follower ignores its election timer. On each leadership tick it checks three facts about the leader
it remembers.

```
   quiesced follower tick:
     leaderNode = expectedLeaders[currentTerm]
     if no leader is recorded for currentTerm           → un-quiesce (the election timer decides)
     if leaderNode is no longer a peer of this partition → un-quiesce and start a pre-vote election
     if leaderNode stated that it leads nothing          → un-quiesce and start a pre-vote election
     if SWIM.GetState(leaderNode) == Alive  → do nothing (stay calm)
     else (Suspect or Dead)                 → un-quiesce and start a pre-vote election
```

The first three checks are necessary because SWIM knows only node liveness. A node can be `Alive` and lead
nothing:

- **It left the range.** A leader whose replica was removed, for example by a placement trim, continues to
  answer pings.
- **It restarted or stepped down.** A process that restarts in less than one `PingInterval` is never `Suspect`.
  A leader that steps down on a storage fault stays `Alive`.

If the follower looked at liveness alone, the range would have no leader until the next write.

The same checks exempt the pre-vote deny gate. When a partition-map change drops a partition's known leader, the
partition also returns to the hot set. This means that the vacancy is noticed within `CheckLeaderInterval`,
before the next safety sweep (`UpdateNodesInterval`).

### How a follower learns that its leader leads nothing

A vote request is a statement about its sender. A pre-vote for term `V` comes from a follower at term `V - 1`.
A real vote request for term `V` comes from a candidate at term `V`. In both cases the sender leads no term
below `V`, and it cannot lead one later: a node becomes the leader of a term only through a candidacy that
starts one term below it. The statement stays true when the message arrives late or out of order.

A follower records this for each peer. When the sender is the leader that the follower remembers for its current
term, that leadership is vacated:

- The follower leaves quiescence, whether it then grants the vote or denies it.
- The follower refuses a later quiesce marker from that node at that term. Such a marker was sent before the
  leadership ended, for example by a process that died and left it in a send buffer. It tells the follower to
  trust SWIM for a leader that is gone.

Only the marker is refused. A late heartbeat or a late entry batch has a bounded effect, and the follower
processes it as before. A leader that wins a later term is not affected, because the record is bounded by the
term.

### The leaderless notice

A restarted leader normally campaigns, and its pre-vote is the statement above. A replica whose candidacy is
held sends no pre-vote. Three holds have no time bound:

| Hold | Ends when |
|---|---|
| Storage quarantine | A leader replicates to the replica, or the node is repaired |
| Stalled disk | The pending WAL write completes |
| Candidacy withheld by the application | The application releases it |

On each election timeout that such a replica yields, it sends a **leaderless notice**: a pre-vote probe with
`NoticeOnly = true`. The notice asks for no grant, and the sender opens no round.

```
   held replica (no leader, election timeout expired):
     ── RequestVotes { PreVote = true, NoticeOnly = true, Term = currentTerm + 1 } ──► each voter

   receiver:
     follower that expects the sender to lead → that leadership is vacated (see above)
     quiesced leader                          → un-quiesce, so its next append teaches the sender its leader
     never answers with a grant
```

Without the notice the hold and quiescence wait for each other. The quiesced followers wait for the leader, the
leader cannot campaign, and a storage quarantine ends only when a leader replicates to the node.

A node that predates `NoticeOnly` reads the notice as an ordinary pre-vote and can answer with a pre-grant. The
sender has no open round and drops the grant.

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
| Record that a peer leads nothing | `RaftPartitionCoreState.NoteNotLeading` / `IsKnownNotLeading`; written by `ElectionCoordinator.NoteSenderDoesNotLead` |
| Vacancy check for both quiesced gates | `ElectionCoordinator.ExpectedLeadershipVacated` |
| Refusal of a quiesce marker from a vacated leadership | `FollowerAppendHandler.AppendLogsCoreAsync` (vacated-leadership fence) |
| Leaderless notice | `ElectionCoordinator.SendLeaderlessNoticeAsync`; wire field `Data/RequestVotesRequest.cs` (`NoticeOnly`), proto `NoticeOnly = 10` |
| Un-quiesce on a new write | `LogReplicator` proposal entry point |
| Wire field | `Data/AppendLogsRequest.cs` (`Quiesce`); `Data/RaftRequest.cs`; proto `Communication/Grpc/Protos/raft.proto` (`Quiesce = 10`); mapped in `RaftService` + `GrpcCommunication` (both single and batch paths) |

---

## Invariants you must not break

- **Quiescence is local, never replicated.** Do not put the `quiesced` flag or `lastProposalAt` in the log, and
  do not make a quiesce decision depend on another node's quiesce state. That would couple safety to a local
  optimization.
- **A quiesced follower must still fail over.** It gates elections on SWIM `Suspect` (≠ `Alive`). Keep
  `PingInterval < StartElectionTimeout` so this is no slower than a normal election.
- **Check vacancy before liveness.** A leader that left the partition's replica set, or that stated on the vote
  path that it leads nothing, is vacated even while its node is `Alive`. Both quiesced gates (the follower tick
  and the pre-vote deny) call `ExpectedLeadershipVacated` before they look at SWIM.
- **Record the sender's statement before any grant decision.** A voter that denies its restarted leader (storage
  quarantine, outdated log) must still leave quiescence. Do not move `NoteSenderDoesNotLead` below a denial.
- **A replica that yields its election timeout without a bound must send the leaderless notice.** If you add a
  new unbounded candidacy hold, send the notice from it. A hold that says nothing recreates the deadlock.
- **Quiescence belongs to one leadership.** A follower is quiesced under the leader that sent the marker, in the
  term of the marker. Do not accept a marker from a node that is known not to lead that term.
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
| Unit | `Kommander.Tests/Scheduling/TestLeaderlessNotice.cs` | A held candidacy sends the notice, once per election timeout; a notice or a vote request from the remembered leader ends quiescence; a late quiesce marker is refused; a quiesced leader wakes |
| Integration | `Kommander.Tests/TestEvictionRejoin.cs` | A quiesced leader restarts with a held candidacy and a survivor takes the leadership |
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
