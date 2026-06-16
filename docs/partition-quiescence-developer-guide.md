# Partition Quiescence — Developer Guide

This guide explains how Kommander stops an **idle partition** from wasting network and CPU on
keep-alive traffic: the **quiescence** feature. It is written for two readers — someone who wants to
*understand* why idle partitions go quiet, and someone who wants to *extend or debug* it. No prior Raft
expertise is assumed; we build the idea up first, then walk the real flows and the code.

For the design rationale and history, see `specs/quiescence-node-liveness-spec.md`. This guide is the
user-facing companion to that spec. Quiescence builds directly on the SWIM failure detector documented
in the [Dynamic Membership Developer Guide](dynamic-membership-developer-guide.md) — read its
*Failure detection (SWIM)* section if the terms `Alive`/`Suspect`/`Dead` are unfamiliar.

---

## Table of contents

1. [The 60-second mental model](#the-60-second-mental-model)
2. [Why quiescence exists (the problem)](#why-quiescence-exists-the-problem)
3. [The two jobs of a heartbeat](#the-two-jobs-of-a-heartbeat)
4. [Core concepts](#core-concepts)
5. [Flow 1 — A leader quiesces an idle partition](#flow-1--a-leader-quiesces-an-idle-partition)
6. [Flow 2 — A quiesced follower stays calm](#flow-2--a-quiesced-follower-stays-calm)
7. [Flow 3 — A write wakes the partition](#flow-3--a-write-wakes-the-partition)
8. [Flow 4 — Failover when a quiesced leader dies](#flow-4--failover-when-a-quiesced-leader-dies)
9. [The timing rule that keeps failover fast](#the-timing-rule-that-keeps-failover-fast)
10. [Configuration](#configuration)
11. [Code map](#code-map)
12. [Invariants you must not break](#invariants-you-must-not-break)
13. [Testing](#testing)
14. [Glossary](#glossary)

---

## The 60-second mental model

A Raft leader normally sends a **heartbeat** to every follower a few times a second, forever — even
when nothing is happening. The heartbeat says "I'm still your leader; don't start an election." For one
busy partition that's fine. But Kommander is built to run **hundreds or thousands of partitions**, most
of them idle most of the time. With `N` nodes and `M` partitions, that's `O(N × M)` heartbeat messages
every interval, *carrying no data* — pure overhead that grows with the partition count.

**Quiescence** is how an idle partition goes quiet:

> When a leader notices a partition has had no writes for a while, it sends **one final "I'm going
> quiet" marker** to its followers and then **stops heartbeating** that partition. The followers stop
> expecting heartbeats. Instead of watching a per-partition timer, they lean on the cluster-wide **SWIM
> failure detector** — which already tracks "is that node alive?" once per node, independent of how many
> partitions it leads. The moment a write arrives (or the leader's node looks dead), the partition
> **wakes up** and normal Raft resumes.

The key insight: *"the leader is still alive"* is a fact about the **node**, not about each partition.
If node A leads 500 partitions, it shouldn't send 500 separate "still alive" messages to node B — SWIM
already tells B that A is alive, once.

```
   BEFORE quiescence (idle partition):     AFTER quiescence (idle partition):
   leader ──hb──► follower  (every 500ms)  leader        follower
   leader ──hb──► follower                        (silent — no per-partition heartbeats)
   leader ──hb──► follower                  SWIM ──ping──► (once per node, shared by all partitions)
   ... forever, per partition ...           follower watches SWIM, stays calm
```

---

## Why quiescence exists (the problem)

Heartbeats in Kommander are **per-partition**. Each leader, on every leadership-check tick, sends an
`AppendLogs` heartbeat to each follower of each partition it leads. A follower resets its election timer
only when it receives *that partition's* heartbeat. So an idle 50-partition leader talking to two
followers emits ~100 heartbeats per interval to say one thing 100 times: "I'm up."

You cannot fix this by simply coalescing heartbeats ("one message proves liveness for all
partitions") — an earlier attempt to do exactly that caused a **leader-flapping bug**, because one
partition's heartbeat suppressed every other partition's, starving followers into perpetual elections.
The reason that naive coalescing is unsafe is the subject of the next section.

Quiescence takes a different route: instead of sending the keep-alive more cleverly, it **stops sending
it at all** when there's nothing to keep alive, and falls back to a liveness signal that is *already*
node-scoped — SWIM.

---

## The two jobs of a heartbeat

This is the idea everything else hangs on. A Raft heartbeat quietly does **two** different jobs, and
only one of them is about the node:

| Heartbeat job | Scope | Can SWIM cover it? |
|---|---|---|
| "This node is alive / reachable" | **node** | **Yes** — SWIM already does exactly this, once per node |
| "I am still the leader of *this partition* in term *T*; suppress your election timer" | **partition** | **No** — leadership is per-partition; A can lead P1 but follow P5 |

So we can't replace per-partition leadership assertion with a node ping — that's the part that must stay
per-partition. But we *can* **avoid sending it when there's nothing to assert** (the partition is idle),
and back the followers with the node-level detector so they stay calm while the leader's node is alive.
That's quiescence.

---

## Core concepts

| Term | Meaning |
|---|---|
| **Heartbeat** | A (usually empty) `AppendLogs` the leader sends to assert "I'm still leader of this partition." |
| **Quiesced** | A per-partition, per-node flag meaning "this partition is idle; skip its heartbeats / election timer." Decided **locally**, never replicated. |
| **Quiesce marker** | The leader's final `AppendLogs` with the `Quiesce` flag set, telling followers to enter the quiesced state at a known log point. |
| **SWIM failure detector** | The cluster-wide gossip+ping layer that tracks each node as `Alive`, `Suspect`, or `Dead`. See the membership guide. |
| **`Alive` / `Suspect` / `Dead`** | SWIM's view of a node. `Suspect` fires after ~one `PingInterval` of failed probes; `Dead` after `SuspicionTimeout`. |
| **`lastProposalAt`** | Per-partition timestamp of the last *real* (non-heartbeat) append. Drives the idle clock. Seeded at election. |
| **`QuiesceAfter`** | How long a partition must be idle before its leader quiesces it. |
| **Wake / un-quiesce** | Returning a partition to normal heartbeating + election timing (on a write, an inbound append, or the leader's node going non-`Alive`). |

Quiescence is a **local optimization**: each node decides independently whether a partition is quiesced,
from its own observations. It is *not* part of the replicated log, so a wrong guess can never violate
Raft safety — at worst it costs one extra heartbeat or one extra election cycle.

---

## Flow 1 — A leader quiesces an idle partition

A leader checks each partition it owns on every leadership tick. For a partition to quiesce, **all** of
these must hold:

- quiescence is enabled (`EnableQuiescence`);
- the partition isn't already quiesced;
- there are **no in-flight proposals**;
- it has been idle longer than `QuiesceAfter` — i.e. `now - lastProposalAt > QuiesceAfter`.

`lastProposalAt` is seeded **at election** (so a freshly-elected leader of a never-written partition
still quiesces `QuiesceAfter` later — the common case), and refreshed on every real write.

When the conditions are met, the leader does two things, once:

1. Sends a **quiesce marker** — one final `AppendLogs` with `Quiesce = true` — to every follower. This
   is a *deterministic handoff*: followers learn to quiesce at a known index/term rather than each
   guessing from silence.
2. Sets its own `quiesced = true` and **stops emitting** that partition's periodic heartbeats.

```
   leader (partition idle > QuiesceAfter):
     ── AppendLogs { Quiesce = true } ──► follower 1
     ── AppendLogs { Quiesce = true } ──► follower 2
     then: stop sending heartbeats for this partition
```

The leader is still the leader — it still answers reads and will replicate the next write. It simply
stops the keep-alive chatter.

---

## Flow 2 — A quiesced follower stays calm

A normal (non-quiesced) follower starts an election if it hasn't heard a heartbeat within its election
timeout. A **quiesced** follower ignores that timer entirely. Instead, on each leadership tick it asks
SWIM one question: *is the node I believe leads this partition still `Alive`?*

```
   quiesced follower tick:
     leaderNode = expectedLeaders[currentTerm]
     if SWIM.GetState(leaderNode) == Alive  → do nothing (stay calm)
     else (Suspect or Dead)                 → un-quiesce and start a pre-vote election
```

So a quiesced follower will sit silently through any amount of per-partition silence, as long as SWIM
says the leader's node is up. It only challenges leadership when the **node** actually looks gone. This
is why the leader can stop heartbeating without triggering spurious elections — exactly the failure mode
the old naive coalescing caused.

---

## Flow 3 — A write wakes the partition

Quiescence is invisible to clients. The instant real work arrives, the partition wakes:

- **Leader side:** a client proposal clears `quiesced`, records `lastProposalAt = now`, appends the
  entry, and resumes normal heartbeating. The entry replicates as an ordinary `AppendLogs`
  (`Quiesce = false`).
- **Follower side:** receiving *any* `AppendLogs` whose `Quiesce` flag is false clears the follower's
  `quiesced` flag and resets its heartbeat timer. (A quiesce marker sets it; any normal append clears
  it — the follower simply mirrors the flag on the message.)

```
   client ──write──► leader
     leader: quiesced = false; append; resume heartbeats
     ── AppendLogs { Quiesce = false, logs = [entry] } ──► followers
     followers: quiesced = false; normal replication resumes
```

After the burst of writes ends and the partition goes idle again for `QuiesceAfter`, it re-quiesces.

---

## Flow 4 — Failover when a quiesced leader dies

This is the case that has to be exactly right: a quiesced partition must **still fail over promptly** if
its leader genuinely dies — quiescence must never *delay* recovery.

Because a quiesced follower watches SWIM, the failover path is:

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

The follower un-quiesces on **`Suspect`**, not on the slower `Dead` — see the next section for why that
matters.

> **Documented limitation:** if the leader's *node* stays alive (still answering SWIM pings) but stops
> driving one *specific* partition (e.g. that partition's executor wedges), quiesced followers will not
> elect a new leader for it. In practice a wedged partition and a wedged node tend to coincide, and SWIM
> shares the node's process health. This is accepted; finer per-partition liveness can be layered on
> later if needed.

---

## The timing rule that keeps failover fast

There is one **safety-relevant timing constraint**, and it's worth understanding because misconfiguring
it would silently slow failover (or break it).

SWIM has two relevant moments after a leader dies:

- **`Suspect`** — fires after roughly one `PingInterval` of failed probes (fast).
- **`Dead`** — fires `SuspicionTimeout` *after* `Suspect` (slow; the default `SuspicionTimeout` is 5s).

Quiesced followers deliberately trigger failover on **`Suspect`**, so detection is on the order of
`PingInterval`, not `SuspicionTimeout`. For this to actually beat a normal election, SWIM's fast signal
must arrive before the election timer would have fired anyway. Kommander enforces that at startup:

> **`PingInterval < StartElectionTimeout`** — validated in `RaftConfiguration.Validate()`; the
> constructor throws if violated.

There is a second, more fundamental guard:

> **`EnableQuiescence` requires `PingInterval > 0`** — quiescence depends on SWIM. With SWIM disabled
> (`PingInterval = 0`), a quiesced follower could never notice a dead leader and could never fail over.
> `Validate()` rejects this combination so the footgun is caught at construction, not in production.

Both checks are skipped when `EnableQuiescence = false`, so the classic per-partition heartbeat model is
always available as a clean off-switch.

---

## Configuration

| Key | Default | Meaning |
|---|---|---|
| `EnableQuiescence` | `true` | Master switch. `false` restores per-partition heartbeating on every interval, independent of idle time. |
| `QuiesceAfter` | `1500 ms` | How long a partition must be idle (no proposals, no in-flight replication) before its leader quiesces it. ≈ 3× `HeartbeatInterval`. |

Quiescence reuses the existing SWIM knobs rather than adding its own failure detector:

| Key | Default | Role in quiescence |
|---|---|---|
| `PingInterval` | `1 s` | SWIM probe cadence. A quiesced follower detects a dead leader ≈ one `PingInterval` after the crash. **Must be `> 0` and `< StartElectionTimeout` when quiescence is on.** |
| `SuspicionTimeout` | `5 s` | `Suspect → Dead` delay. Quiescence triggers on `Suspect`, so this does **not** gate failover latency. |
| `StartElectionTimeout` | `2000 ms` | Upper bound the `PingInterval` invariant is checked against. |

**Tuning notes.** Lower `QuiesceAfter` to make idle partitions go quiet sooner (less idle traffic,
slightly more quiesce/wake churn under bursty load). Lower `PingInterval` to tighten quiesced-failover
detection (at the cost of more SWIM probe traffic). The defaults are valid out of the box
(`1000 ms < 2000 ms`).

---

## Code map

| What | Where |
|---|---|
| Config knobs + `Validate()` invariants | `Kommander/RaftConfiguration.cs` (`EnableQuiescence`, `QuiesceAfter`, `Validate`) |
| SWIM state exposed to the state machine | `Kommander/Scheduling/IRaftPartitionHost.cs` → `GetNodeLiveness`; impl `RaftPartitionHostAdapter` → `RaftManager.Liveness.GetState` |
| SWIM failure detector | `Kommander/Gossip/LivenessTable.cs`; probing in `RaftManager.PingAsync` / `ReceivePing` |
| Quiesce state + idle clock | `Kommander/RaftPartitionStateMachine.cs` (`quiesced`, `lastProposalAt`) |
| Leader quiesce decision | `RaftPartitionStateMachine.CheckPartitionLeadershipAsync` (leader branch) |
| Quiesced follower election gate | `CheckPartitionLeadershipAsync` (quiesced-follower branch) |
| Heartbeat suppression | `SendHeartbeat` (early-returns when `quiesced` and not `force`) |
| Quiesce marker broadcast | `SendQuiesceMarker` → `AppendLogToNode(..., quiesce: true)` |
| Leader bookkeeping at election (seeds `lastProposalAt`) | `BecomeLeader` |
| Follower applies / clears the flag | `AppendLogsCoreAsync` (`quiesced = quiesce`) |
| Un-quiesce on a new write | `ReplicateLogs` proposal entry point |
| Wire field | `Data/AppendLogsRequest.cs` (`Quiesce`); `Data/RaftRequest.cs`; proto `Communication/Grpc/Protos/raft.proto` (`Quiesce = 10`); mapped in `RaftService` + `GrpcCommunication` (both single and batch paths) |

---

## Invariants you must not break

- **Quiescence is local, never replicated.** The `quiesced` flag and `lastProposalAt` are per-node
  decisions. Never put them in the log or make a quiesce decision depend on another node's quiesce
  state — that would couple safety to a local optimization.
- **A quiesced follower must still fail over.** It gates elections on SWIM `Suspect` (≠ `Alive`), not on
  `Dead`. Keep `PingInterval < StartElectionTimeout` so this is no slower than a normal election.
- **Quiescence requires SWIM.** `EnableQuiescence = true` with `PingInterval <= 0` is rejected at
  startup. Don't weaken that guard.
- **Any normal append wakes a follower.** `AppendLogsCoreAsync` mirrors the message's `Quiesce` flag, so
  a `Quiesce = false` append (real log or normal heartbeat) always clears `quiesced`. Don't add a code
  path that appends without updating the flag.
- **Election entry seeds `lastProposalAt`.** All become-leader paths go through `BecomeLeader`, which
  seeds it at the election timestamp. A new become-leader path that skips it would make idle partitions
  never quiesce.
- **The election/vote rules are untouched.** Quiescence only changes *when a follower starts*
  campaigning. The term checks, log-up-to-date checks, and quorum math are exactly as before.

---

## Testing

Quiescence is covered at two layers:

- **Unit tests** (`Kommander.Tests/Scheduling/TestQuiescedLeader.cs`,
  `TestQuiescedFollower.cs`): drive the state machine directly with fake hosts and a real `LivenessTable`
  — leader quiesces after the idle window, heartbeats are suppressed, a quiesce marker is sent, a
  follower stays calm while the leader is `Alive` and pre-votes on `Suspect`/`Dead`, and any append
  clears the flag. Guard coverage for `lastProposalAt == 0`.
- **Config validation** (`Kommander.Tests/TestRaftConfigurationValidation.cs`): both invariants
  (`PingInterval < StartElectionTimeout`, and `PingInterval > 0` when quiescence is on), plus that they
  are skipped when quiescence is off.
- **Integration tests** (`Kommander.Tests/TestQuiescence.cs`), `[Collection]`-serialized, real
  in-memory clusters:
  - an idle cluster stays stable with one leader across several election-timeout windows (no spurious
    elections — the headline regression test);
  - the per-partition heartbeat counter (`raft.heartbeats_sent_total`) stops advancing after quiesce;
  - a write after quiesce un-quiesces, commits, and replicates to all nodes;
  - a partitioned quiesced leader is replaced **well before `SuspicionTimeout`** (proving failover is
    `Suspect`-driven, not `Dead`-driven);
  - feature-flag parity: with `EnableQuiescence = false`, heartbeats keep coming.

Tests that deliberately disable SWIM (`PingInterval = 0`) or use very fast election timeouts set
`EnableQuiescence = false`, since quiescence depends on SWIM and on the `PingInterval < StartElectionTimeout`
invariant.

---

## Glossary

| Term | Meaning |
|---|---|
| **Quiescence** | Suppressing an idle partition's per-partition heartbeats, relying on SWIM for leader-node liveness. |
| **Heartbeat** | An empty `AppendLogs` asserting continued leadership of one partition. |
| **Quiesce marker** | The final `AppendLogs` (`Quiesce = true`) that tells followers to enter the quiesced state. |
| **Quiesced** | Local flag: this partition skips heartbeats (leader) / the election timer (follower). |
| **SWIM** | The gossip+ping failure detector tracking each node as `Alive`/`Suspect`/`Dead`. |
| **`Suspect`** | SWIM state after ~one `PingInterval` of failed probes; the trigger for quiesced failover. |
| **`Dead`** | SWIM state after `SuspicionTimeout`; *not* used to gate failover (too slow). |
| **`lastProposalAt`** | Timestamp of the last real write; drives the `QuiesceAfter` idle clock; seeded at election. |
| **`QuiesceAfter`** | Idle duration before a leader quiesces a partition. |
| **Wake / un-quiesce** | Returning a partition to normal heartbeating and election timing. |
| **O(N×M)** | The pre-quiescence idle heartbeat cost: `N` nodes × `M` partitions, every interval. |
