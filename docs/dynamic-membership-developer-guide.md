# Dynamic Cluster Membership — Developer Guide

Welcome! This guide is for the next developer who has to **use, support, or extend** Kommander's
dynamic membership system. It explains not just *how* the pieces work but *why* they were built this
way, so when something misbehaves at 3 a.m. you understand the model well enough to reason about it.

---

## Table of contents

1. [Summary](#summary)
2. [Why this design? Motivations & advantages](#why-this-design-motivations--advantages)
3. [Core concepts](#core-concepts)
4. [The API surface](#the-api-surface)
5. [Lifecycle walkthroughs](#lifecycle-walkthroughs)
6. [Catch-up: how a new node gets up to date](#catch-up-how-a-new-node-gets-up-to-date)
7. [Failure detection (SWIM)](#failure-detection-swim)
8. [Configuration reference](#configuration-reference)
9. [Code map — where everything lives](#code-map--where-everything-lives)
10. [Transport support matrix](#transport-support-matrix)
11. [Operations runbook](#operations-runbook)
12. [Troubleshooting](#troubleshooting)
13. [Invariants you must not break](#invariants-you-must-not-break)
14. [Testing](#testing)
15. [Glossary](#glossary)

---

## Summary

A Kommander cluster keeps a single **roster** — the authoritative list of who belongs to the cluster
and in what role. The roster is not gossiped state or a config file; it is a **committed Raft record**
living on the system partition (P0), versioned by `MembershipVersion`.

Membership changes one node at a time through three operations — **AddMember**, **PromoteMember**,
**RemoveMember** — each committed exactly like any other Raft entry. A new node joins as a non-voting
**Learner**, catches up, and is automatically **promoted** to **Voter**. Voters and only voters count
toward quorum.

On top of that sits a **gossip + SWIM** layer that spreads the roster faster and detects dead nodes —
but it is strictly advisory. It can *recommend* a removal; only the P0 leader, through a committed
`RemoveMember`, ever actually changes who can vote.

That separation — **truth by consensus, dissemination by gossip** — is the whole design in one
sentence.

```
   ┌──────────────────────────── TRUTH (consensus) ────────────────────────────┐
   │  Committed roster on P0:  ClusterMembership { MembershipVersion, Members }  │
   │  Changed only by AddMember / PromoteMember / RemoveMember (one at a time)   │
   └───────────────▲──────────────────────────────────────────┬────────────────┘
                   │ "this node is Dead" (advisory)            │ derives the voter set
                   │                                           ▼
   ┌───────────────┴───────────── DISSEMINATION ──────────┐   host.Nodes = roster voters
   │  Gossip (anti-entropy) + SWIM (failure detection)     │   → per-partition quorum input
   │  Spreads {version, roster, liveness}; never commits.  │
   └──────────────────────────────────────────────────────┘
```

---

## Why this design? Motivations & advantages

If you only read one section to "get" the system, read this one.

### The problem we started with

Originally a cluster's member set was **static for the process lifetime**. The only source of peers
was `IDiscovery`: `ClusterHandler.UpdateNodes()` did `manager.Nodes = discovery.GetNodes()` wholesale,
on a timer. That peer set fed quorum math directly. This made runtime membership change unsafe:

- **No ordering or atomicity.** Replacing `Nodes` from discovery is not a consensus operation. Two
  nodes could observe different member sets at the same instant, and since quorum is computed from
  that set, divergent views directly enable **split-brain and lost commits**.
- **No catch-up.** A node appearing in discovery was immediately counted as a peer. A fresh node has
  an empty log; counting it as a voter shrinks the *effective* quorum (it can't ack) and — absent
  PreVote — could even win an election and truncate committed history.
- **No failure-driven removal.** A permanently dead voter degraded availability until an operator
  hand-edited discovery.

### The design choices, and what each buys you

**1. The roster is consensus-anchored (committed on P0), not gossip-authoritative.**
Membership is the one piece of state where disagreement is catastrophic — it's the denominator of
every quorum. So we make it the *most* strongly consistent thing in the system, not the least. By
storing the roster exactly like the partition map (same commit path, same versioning), it inherits
P0's linearizability for free.
*Advantage:* every node that has applied version *N* of the roster agrees on exactly who can vote.
There is no window where two nodes compute quorum from different member sets.

**2. New nodes join as Learners and are promoted only after catching up.**
A joining node receives replication but is excluded from quorum until it has nearly caught up.
*Advantage:* adding a node can **never stall commits** — the existing voters keep their quorum
throughout. And a far-behind node can never win an election to truncate history, because it isn't a
voter and (belt and suspenders) Learners are gated out of campaigning entirely.

**3. Changes are single-server (one node at a time), not joint consensus.**
Raft offers two ways to change membership: joint consensus (Cold,new) or single-server changes. We
chose single-server: add/promote/remove exactly one member per committed step.
*Advantage:* any two consecutive configurations differ by one member and therefore **always share a
majority** — no two disjoint quorums can both commit. This gives the core safety guarantee with far
less machinery than joint consensus, which is notoriously fiddly to implement correctly.

**4. `MembershipVersion` is an optimistic-concurrency fence.**
Every change request carries the version it was computed against. The P0 leader rejects it
(`StaleMembership`) if the committed version moved, and rejects a second change while one is in flight
(`ConcurrentMembershipChange`).
*Advantage:* concurrent or racing membership operations are detected and serialized instead of
silently clobbering each other — the same discipline the partition map already uses (`MapVersion`).

**5. Gossip disseminates; it never decides.**
The SWIM/gossip layer spreads the roster epidemically (so a node learns of a change in O(log N)
rounds instead of waiting for Raft replication to reach it) and detects unreachable nodes. But it only
ever carries *already-committed* rosters, and a `Dead` verdict is advisory input to the P0 leader.
*Advantage:* you get fast, decentralized convergence and automatic failure handling **without** ever
letting an eventually-consistent signal change who can commit. The quorum can't be corrupted by a
malicious or buggy gossip message because gossip simply isn't on the path that decides quorum.

**6. Discovery is demoted to seed contact.**
`IDiscovery` is used to find *bootstrap contact points* and to seed the *initial* greenfield roster —
not as the live membership authority.
*Advantage:* you can run on static discovery, Redis, multicast, or nothing at all (seed endpoints),
and the membership semantics are identical because none of them are trusted for live membership.

### The one-line summary of advantages

| Property | How the design delivers it |
|---|---|
| Safety (no split-brain) | Quorum reads only committed voters; single-server changes share a majority |
| Availability during joins | Learners never count toward quorum |
| No lost history from fresh nodes | Learner gate + PreVote + catch-up before promotion |
| Fast convergence | Gossip anti-entropy spreads the committed roster epidemically |
| Self-healing | SWIM detects Dead nodes; P0 leader evicts them via the committed path |
| Operational simplicity | One roster, one fence (`MembershipVersion`), single-server changes |

---

## Core concepts

### The roster is a replicated log record

Every node maintains a shared roster, `ClusterMembership`, committed on P0 via the same Raft
replication that protects your application data. Each membership change is therefore:

- **Durable** — it's in the WAL and survives restarts.
- **Serialized** — only one change is in flight at a time; concurrent requests are rejected.
- **Consistent** — no node treats the new roster as truth until a quorum of voters has acknowledged
  the entry.

```csharp
public sealed class ClusterMembership
{
    public long MembershipVersion { get; set; }   // bumped by exactly 1 on every change; the fence
    public List<ClusterMember> Members { get; set; } = [];
}

public sealed class ClusterMember
{
    public string Endpoint { get; set; } = "";     // "host:port" — the identity key
    public int NodeId { get; set; }                // descriptive node id
    public ClusterMemberRole Role { get; set; }    // Learner | Voter | Leaving  (NotMember is a query-only result)
    public long JoinedVersion { get; set; }        // the version at which this node was added as a Learner
}
```

> Note: `ClusterMember` has **no** human-readable name field — the `Endpoint` is the identity key.
> `MembershipVersion == 0` means no roster has been committed yet (the pre-seed transient on a brand-new
> cluster).

### Roles and the member lifecycle

```
   join                promote                leave / evict
 ──────────▶ Learner ──────────▶ Voter ──────────▶ Leaving ──────────▶ [removed]
              (no quorum)        (quorum)          (no longer
                                                    campaigns)
```

- **Learner** — receives replication so it can catch up; does **not** count toward quorum and may not
  start or win elections.
- **Voter** — full participant: counts toward quorum, can campaign, can be elected.
- **Leaving** — a node that has begun a graceful leave. It still counts toward quorum until its
  `RemoveMember` commits (so a leave-in-progress never shrinks the effective quorum prematurely), but
  it stops *starting* elections.
- **NotMember** — not a stored role; it's the value `RaftManager.LocalRole` returns when the local
  node isn't in the committed roster at all.

### Two layers, one strict contract

- **Layer 1 — Truth (consensus).** The committed roster on P0. Mutated only by `AddMember`,
  `PromoteMember`, `RemoveMember`, each going through the P0 leader's Raft log.
- **Layer 2 — Dissemination (gossip + SWIM).** Spreads `{MembershipVersion, roster, per-member
  liveness}` and detects failures. **It never writes the voter set.** It only (a) converges local
  caches faster and (b) feeds `Dead` verdicts to the P0 leader, which turns them into committed
  removals.

The invariant that makes the whole thing safe: **quorum is computed only from committed voters.**
Gossip, discovery, and learner presence never change quorum.

### The system partition (P0) is special

All membership mutations commit on P0 (`RaftSystemConfig.SystemPartition == 0`). User partitions
(P1, P2, …) are independent — you can keep writing to them while a join or removal is in progress,
because a user partition's quorum doesn't depend on the new member until *after* promotion.

### How the voter set is derived

`RaftManager.Nodes` (the peer set every partition uses for quorum) is a **projection of the committed
roster**: members with `Role == Voter`, excluding self. `ClusterHandler.UpdateNodes()` recomputes it
from the roster on each tick. Learners are included in the *replication* peer set (so the leader ships
them entries) but are excluded from quorum math.

---

## The API surface

All membership operations are on the `IRaft` interface (implemented by `RaftManager`).

### Read the current roster

```csharp
ClusterMembership m = raft.GetMembership();

Console.WriteLine($"Roster version: {m.MembershipVersion}");
foreach (ClusterMember member in m.Members)
    Console.WriteLine($"  {member.Endpoint}  {member.Role}");
```

`GetMembership()` returns a point-in-time snapshot. It does not update in place — call it again for
the latest.

### Check this node's role

```csharp
ClusterMemberRole role = raft.LocalRole;   // Voter | Learner | Leaving | NotMember
```

### Subscribe to roster changes

```csharp
raft.OnMembershipChanged += membership =>
{
    Console.WriteLine($"Roster → v{membership.MembershipVersion}");
    // membership.Members is the full new roster
};
```

`OnMembershipChanged` fires whenever this node's view of the committed roster advances to a strictly
higher `MembershipVersion`. That happens on the greenfield seed, on join/promotion, on graceful leave,
on failure-driven eviction, **and** when a fresher roster is adopted via gossip before the Raft append
reaches this node. Use `MembershipVersion` as a monotonic sequence number.

> ⚠️ The event fires on the system coordinator's single-consumer loop. Your handler **must not block
> or call back into the coordinator** — copy what you need and return. Events arrive in commit order,
> never reordered or duplicated.

### Join an existing cluster

Seed-based (no discovery service required):

```csharp
await raft.JoinCluster(
    seeds: ["host1:7000", "host2:7000", "host3:7000"],
    cancellationToken: cts.Token);
// Blocks until this node is a committed Voter.
```

Discovery-based (uses the `IDiscovery` registered at construction):

```csharp
await raft.JoinCluster(cancellationToken: cts.Token);
```

Both overloads end in the same state — this node is a committed Voter — and both block until that
happens or the deadline (default 60 s) trips. Pass a `CancellationToken` with a shorter deadline for
tighter control. Internally, `JoinCluster(seeds)`:

1. Contacts each seed until one (or its P0-leader hint) commits an `AddMember(Learner)` entry.
2. Waits for the P0 leader to replicate the partition map to this node (`IsInitialized`).
3. Waits for the leader to auto-promote this node Learner → Voter.

### Leave the cluster gracefully

```csharp
await raft.LeaveCluster(dispose: true);
```

`LeaveCluster` marks the node `Leaving` (so it stops campaigning immediately), commits a
`RemoveMember(self)` on P0, waits up to ~10 s for the removal to propagate back, then tears the node
down regardless. If this node is the P0 leader, it commits its own removal under the old quorum and
steps down so another node takes over.

---

## Lifecycle walkthroughs

### Joining

```
New node                    P0 leader                      Other voters
   |                            |                               |
   |--JoinRequest(endpoint)---->|                               |
   |                            |--AppendLogs(AddMember)------->|
   |                            |<--CompleteAppendLogs(ack)-----|
   |                            |   (majority ack → commit)
   |<--JoinResponse(success)----|
   |                            |
   |  [leader's UpdateNodes tick: new node now in the replication peer set]
   |<--AppendLogs(P0 catch-up)--|
   |--CompleteAppendLogs(ack)-->|
   |  [receives Partitions entry → IsInitialized = true]
   |  [leader sees lag ≤ LearnerPromotionLag for LearnerPromotionStableWindow]
   |                            |--AppendLogs(PromoteMember)--->|
   |                            |<--CompleteAppendLogs(ack)-----|
   |<--AppendLogs(PromoteMember)|
   |  LocalRole = Voter         |
```

The key invariant: **quorum is unaffected during catch-up.** The original voters keep committing
throughout; the new node is a pure receiver until promotion.

### Leaving (graceful)

```
Leaving node                P0 leader
   |                            |
   |  LocalRole = Leaving (stops campaigning, still counts to quorum)
   |--LeaveRequest(endpoint)--->|
   |                            |--AppendLogs(RemoveMember)-->voters
   |                            |<--ack majority → commit → roster shrinks
   |<--LeaveResponse(ok)--------|
   |  shutdown
```

A removal that would drop the cluster below a viable quorum (zero voters) is refused with
`InsufficientVoters` — a terminal status, so the caller gives up instead of retrying. (Going from 2
voters to 1 is allowed: single-node commit is supported.)

### Leaving (crash → automatic eviction)

If a node dies without calling `LeaveCluster`, the SWIM failure detector marks it `Suspect` → `Dead`,
and after `DeadMemberEvictionGrace` the P0 leader commits a `RemoveMember` for it automatically. See
the next section.

---

## Catch-up: how a new node gets up to date

A Learner (or any follower whose log trails the leader) is caught up by **bounded log backfill**: the
leader detects the follower's reported `MaxLogId` is below `commitIndex - BackfillThreshold` and ships
the missing **committed** entries in chunks of `MaxBackfillEntriesPerRound`, classed below client
traffic so it never starves writes. Backfill is committed-only and idempotent (entries carry id/term).

**Important limitation (today):** backfill only catches up a follower that is still **above the
compaction floor**. If a partition's WAL has compacted past the point a fresh Learner needs, tail-only
backfill can't bootstrap it — that requires a snapshot install (`ExportRange`/`ImportRange`), which is
**not yet wired into the learner path**. Until then, a Learner can only join partitions whose log
hasn't compacted past its start point.

---

## Failure detection (SWIM)

Kommander uses a SWIM-style detector to find crashed or partitioned nodes without operator action.

### How it works

1. **Direct probe.** Each node periodically sends a `Ping` to one random peer.
2. **Indirect probe.** If the direct ping times out (`PingTimeout`), the prober asks up to
   `IndirectPingFanout` other peers to relay a probe (`PingReq`) — this rules out a transient path
   failure between just those two nodes.
3. **Suspect.** If direct *and* all indirect probes fail, the target is marked `Suspect`.
4. **Dead.** A `Suspect` that stays unreachable for `SuspicionTimeout` becomes `Dead`.
5. **Evict.** Once `Dead` for `DeadMemberEvictionGrace`, the **P0 leader** (and only the P0 leader)
   commits a single `RemoveMember`. The quorum-safety guard prevents eviction from draining the voter
   set below a majority.

Liveness state (`Alive`/`Suspect`/`Dead`) and incarnation numbers live in the **gossip layer**
(`LivenessTable`), never in the committed roster — so a node flapping Alive/Suspect doesn't churn the
Raft log.

### Self-refutation (incarnation numbers)

Each node carries an **incarnation counter**. When a node learns via gossip that it's been marked
`Suspect`, it bumps its incarnation and broadcasts an `Alive` entry with the higher number. The SWIM
merge rule says higher incarnation wins, so the stale `Suspect` is overwritten everywhere. This clears
false suspicions (network blips) automatically. `Dead` is terminal locally and is *not* refutable —
a falsely-dead node is evicted and must rejoin, so keep `SuspicionTimeout` generous enough to absorb
normal blips.

---

## Configuration reference

All on `RaftConfiguration`. Defaults shown are the current code defaults.

### Catch-up / promotion

| Setting | Default | Meaning |
|---|---|---|
| `BackfillThreshold` | `10` | Leader backfills a follower whose `MaxLogId` is this many entries behind `commitIndex`. |
| `MaxBackfillEntriesPerRound` | `128` | Max committed entries shipped per backfill round (bounds the burst). |
| `LearnerPromotionLag` | `10` | A Learner within this many entries of the committed log is eligible for promotion. |
| `LearnerPromotionStableWindow` | `3 s` | A Learner must stay caught up this long before promotion (debounce). |

### Gossip (anti-entropy)

| Setting | Default | Meaning |
|---|---|---|
| `GossipInterval` | `5 s` | How often a node gossips the roster to random peers. |
| `GossipFanout` | `2` | Peers contacted per gossip round. `0` disables gossip. |

### Failure detector (SWIM)

| Setting | Default | Meaning |
|---|---|---|
| `PingInterval` | `1 s` | How often to probe a random peer. Set to `Zero` to disable the detector entirely. |
| `PingTimeout` | `500 ms` | Direct/indirect probe response deadline. |
| `IndirectPingFanout` | `2` | Number of relays used for indirect probing. |
| `SuspicionTimeout` | `5 s` | Time a node stays `Suspect` before becoming `Dead`. |
| `DeadMemberEvictionGrace` | `30 s` | Time a node stays `Dead` before the P0 leader commits `RemoveMember`. |

---

## Code map — where everything lives

When you need to change or debug membership, start here.

### Data model
- `Kommander/System/ClusterMembership.cs` — the versioned roster record.
- `Kommander/System/ClusterMember.cs` — one member entry (Endpoint/NodeId/Role/JoinedVersion).
- `Kommander/System/ClusterMemberRole.cs` — `Learner | Voter | Leaving | NotMember`.

### The commit path & drivers (the brain)
- `Kommander/System/RaftSystemCoordinator.cs`:
  - `TryAddMember` / `TryPromoteMember` / `TryRemoveMember` — the single-server mutations (validate
    version, mutate, bump, replicate). `TryRemoveMember` holds the `InsufficientVoters` guard.
  - `CheckLearnerPromotionsAsync` — the promotion driver (P0-leader-only; lag + stable-window).
  - `EvictDeadMembersAsync` — the eviction driver (P0-leader-only; reads `LivenessTable`).
  - `ApplyGossipRoster` — applies a gossiped roster to the local cache (monotonic, never to the log).
  - `TrySeedInitialMembership` — greenfield all-voters seed on first leadership.
  - `ApplyMembershipFromCache` — applies the committed `members` key on `ConfigReplicated`.
  - `RaiseMembershipChanged` callers — where `OnMembershipChanged` is fired.

### Peer-set derivation
- `Kommander/ClusterHandler.cs` — `UpdateNodes()` projects the roster's voters (+learners for
  replication) into `manager.Nodes`.

### Role gating (elections)
- `Kommander/RaftPartitionStateMachine.cs` — `StartElectionAsync`/`StartPreVoteAsync` early-return
  unless local role is `Voter`; `VoteAsync`/`ReceivedVoteAsync` reject non-roster endpoints.

### Join / leave / gossip / ping — handlers and round drivers
- `Kommander/RaftManager.cs`:
  - `JoinCluster` (both overloads), `ReceiveJoin` — join client + server.
  - `LeaveCluster`, `ReceiveLeave`, `CommitGracefulLeaveAsync` — graceful leave.
  - `GossipAsync`, `ReceiveGossip` — one gossip round + receiver.
  - `PingAsync`, `ReceivePing`, `ReceivePingReq` — one SWIM probe round + receivers.
  - `LocalRole`, `GetMembership`, `OnMembershipChanged` — the public surface.
  - `Liveness` — this node's `LivenessTable` instance.

### Gossip / SWIM types
- `Kommander/Gossip/GossipMessage.cs` — the push-pull anti-entropy message + ack (full-state push;
  see the design note in the file).
- `Kommander/Gossip/LivenessTable.cs` — thread-safe SWIM state with the merge rule + refutation.
- `Kommander/Gossip/MemberLivenessState.cs`, `MemberLivenessEntry.cs`, `PingMessage.cs`.

### Timers
- `Kommander/RaftTimerService.cs` — owns the update-nodes, gossip, and ping timers
  (`TriggerGossip`/`TriggerPing` with overlap guards; the ping timer is only created when
  `PingInterval > 0`).

### Transport
- `Kommander/Communication/ICommunication.cs` — `SendJoin`, `SendLeave`, `SendGossip`, `SendPing`,
  `SendPingReq`, `GetRemoteFollowerLag`.
- `…/Memory/InMemoryCommunication.cs` — the fully-wired implementation (tests + single-process).
- `…/Grpc/GrpcCommunication.cs`, `…/Rest/RestCommunication.cs` — see the matrix below for which calls
  are real vs. stubbed.

### Config & tests
- `Kommander/RaftConfiguration.cs` — every knob in the table above.
- `Kommander.Tests/TestMembership.cs`, `TestFourNodeJoin.cs`,
  `Kommander.Tests/Scheduler/TestRaftPartitionStateMachine.cs`.

---

## Transport support matrix

This is the most important table for avoiding surprises. "Wired" means the RPC actually crosses the
network; "stub" means it returns a default and the feature is inert (or worse) on that transport.

| Capability | InMemory | gRPC | REST |
|---|---|---|---|
| Roster commit / replication (Add/Promote/Remove) | ✅ wired | ✅ wired | ✅ wired |
| Join RPC | ✅ wired | ✅ wired | ✅ wired |
| Graceful leave RPC (`SendLeave`) | ✅ wired | ✅ wired | ✅ wired |
| Cross-partition lag (`GetRemoteFollowerLag`) | ✅ wired | ✅ wired | ✅ wired |
| Gossip anti-entropy (`SendGossip`) | ✅ wired | ✅ wired | ✅ wired |
| SWIM probes (`SendPing`/`SendPingReq`) | ✅ wired | ✅ wired | ✅ wired |
| Snapshot install for catch-up below floor | ⛔ not yet implemented | ⛔ not yet implemented | ⛔ not yet implemented |

All three transports are fully wired. Keep this matrix in sync as capabilities land — it's the first thing a confused operator checks.

---

## Operations runbook

### Add a node
1. Start the new process with seed endpoints (or a shared discovery service).
2. Call `JoinCluster(seeds, ct)` and `await` it. When it returns, the node is a committed Voter.
3. Confirm: `raft.GetMembership()` on any node shows the new endpoint with `Role == Voter`.

### Remove a node (planned)
1. Call `LeaveCluster(dispose: true)` on the node being retired. This works on every transport —
   it commits a `RemoveMember` on P0 before the node stops.
2. Confirm the roster on a surviving node no longer lists it and `MembershipVersion` advanced.
3. If a node crashes without a graceful leave, the SWIM failure detector will evict it automatically
   after `SuspicionTimeout` + `DeadMemberEvictionGrace`. You can also force removal with an explicit
   `LeaveCluster` call from any surviving node.

### Replace a dead node
1. The SWIM detector evicts the dead endpoint automatically after `SuspicionTimeout` + `DeadMemberEvictionGrace`.
2. Join the replacement with `JoinCluster`.

### What to watch
- `MembershipVersion` should be identical across healthy nodes once converged. Persistent divergence
  means a node isn't receiving replication or gossip.
- Roster size vs. expected node count — a Learner stuck not-promoting, or a zombie Learner, shows up
  here.
- Wire up `OnMembershipChanged` to your logging/metrics so every change is observable.

---

## Troubleshooting

| Symptom | Likely cause | Where to look |
|---|---|---|
| `JoinCluster` throws `TimeoutException` | Seeds unreachable, or the Learner never catches up (e.g. compacted partition, no snapshot install) | `ReceiveJoin`, backfill in `RaftPartitionStateMachine`; check the partition compaction floor |
| New node stays `Learner` forever | Lag never reaches `LearnerPromotionLag`, or it can't catch up on a partition (e.g. compacted past the floor with no snapshot install) | `CheckLearnerPromotionsAsync`; `GetRemoteFollowerLag` in `ICommunication`; partition compaction floor |
| Healthy nodes get evicted | `SuspicionTimeout` or `PingTimeout` too short for network latency; or nodes are genuinely unreachable | Increase `SuspicionTimeout`; check connectivity; set `PingInterval = Zero` to disable |
| `LeaveCluster` doesn't shrink the roster | Leaving node couldn't reach the P0 leader, or the removal would leave zero voters (`InsufficientVoters`) | `CommitGracefulLeaveAsync`, `TryRemoveMember`; check connectivity to the P0 leader |
| Roster versions differ between nodes | A node isn't getting replication/gossip; gossip not yet wired on gRPC/REST | `UpdateNodes`, `ApplyGossipRoster`, transport matrix |
| `RemoveMember` returns `InsufficientVoters` | Removal would leave zero voters | By design — terminal, don't retry |
| Commit stalls right after adding a node | (Should not happen) Learner counted toward quorum — a real bug | Quorum math in `RaftPartitionStateMachine`; the invariant below |

---

## Invariants you must not break

If you edit this subsystem, these are the load-bearing rules. Breaking one risks split-brain or lost
commits.

1. **Quorum is computed only from committed voters.** Never let Learners, `Leaving` members, gossip,
   or discovery into the quorum denominator. (This bug has happened before — Learners were briefly
   added to the quorum count. Guard it with tests.)
2. **The roster changes one member at a time, through P0's Raft log.** No bulk rewrites, no
   gossip-driven mutations to the committed roster.
3. **Gossip only ever carries already-committed rosters.** `ApplyGossipRoster` must apply only
   strictly-higher versions and must never write to the WAL/log.
4. **Only the P0 leader proposes evictions and promotions**, and only one at a time.
5. **`MembershipVersion` is monotonic** and is the fence on every mutation. Don't apply a lower or
   equal version over a higher one.
6. **A `Leaving` member still counts toward quorum** until its removal commits; it only stops
   campaigning.
7. **Don't enable a feature on a transport whose RPC is stubbed.** All built-in transports have working SWIM probes; custom `ICommunication` implementations must implement `SendPing`/`SendPingReq` before `PingInterval > 0` is safe.

---

## Testing

- Membership tests: `Kommander.Tests/TestMembership.cs` (seed, derivation, roles, graceful leave,
  quorum-not-inflated, gossip anti-entropy, SWIM eviction/refutation).
- End-to-end join: `Kommander.Tests/TestFourNodeJoin.cs`.
- Election/role-gating units: `Kommander.Tests/Scheduler/TestRaftPartitionStateMachine.cs`.

Run a focused slice while iterating:

```sh
dotnet test Kommander.Tests/Kommander.Tests.csproj --filter "FullyQualifiedName~TestMembership"
```

> **Never run multiple `dotnet test` commands in parallel or in the background.** The suite spins up
> in-process Raft clusters and timing-sensitive state machines; concurrent runs interfere and produce
> false failures. One suite at a time.

When you change shared scheduling, WAL, partition state, or cluster coordination, run the full
project, not just a filter.

---

## Glossary

- **Roster** — the committed list of cluster members (`ClusterMembership`).
- **MembershipVersion** — monotonic version of the roster; the optimistic-concurrency fence.
- **P0 / system partition** — partition 0, where the roster and partition map are committed.
- **Learner** — a non-voting member that is catching up.
- **Voter** — a full member that counts toward quorum.
- **Leaving** — a member committing a graceful departure; still a voter until removed.
- **Single-server change** — add/promote/remove exactly one member per committed step (Raft §6).
- **Backfill** — leader→follower shipping of missing committed log entries to catch a follower up.
- **Gossip / anti-entropy** — epidemic dissemination of the roster + liveness over the transport.
- **SWIM** — the failure-detection protocol (direct + indirect probes, suspicion, incarnation
  refutation).
- **Incarnation** — a per-node counter used to refute stale suspicions; higher always wins.
- **Eviction** — the P0 leader removing a `Dead` member via a committed `RemoveMember`.
</content>
</invoke>
