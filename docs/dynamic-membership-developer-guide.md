# Dynamic Cluster Membership — Developer Guide

Kommander adds and removes cluster nodes at runtime. This guide is for developers who use this feature
and for developers who change it.

---

## Table of contents

1. [Summary](#summary)
2. [Design rationale](#design-rationale)
3. [Core concepts](#core-concepts)
4. [The API surface](#the-api-surface)
5. [Lifecycle walkthroughs](#lifecycle-walkthroughs)
6. [New-node catch-up](#new-node-catch-up)
7. [Failure detection (SWIM)](#failure-detection-swim)
8. [Configuration reference](#configuration-reference)
9. [Code map](#code-map)
10. [Transport support matrix](#transport-support-matrix)
11. [Operations runbook](#operations-runbook)
12. [Troubleshooting](#troubleshooting)
13. [Invariants you must not break](#invariants-you-must-not-break)
14. [Testing](#testing)

---

## Summary

The **roster** is the authoritative list of cluster members and their roles. It is a **committed Raft
record** on the system partition (P0), versioned by `MembershipVersion`.

Membership changes one node at a time. Each change is committed like any other Raft entry. A new node
joins as a non-voting **Learner** and is automatically **promoted** to **Voter** when it has caught up. Only
voters count toward quorum.

A **gossip and SWIM** layer spreads the roster faster and detects dead nodes. SWIM is a failure-detection
protocol that uses direct and indirect probes. This layer is advisory: it can *recommend* a removal, and
only a membership change that the P0 leader commits changes who can vote.

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

## Design rationale

- **The roster is committed on P0.** Membership is the denominator of each quorum. All nodes that applied
  version *N* of the roster agree on who can vote, which prevents split-brain.
- **New nodes join as Learners.** The existing voters form the quorum while the new node catches up, and
  its join cannot stall commits. Learners cannot campaign, and a far-behind node cannot win an election and
  truncate history.
- **Changes are single-server.** Two consecutive configurations differ by one member and **always share a
  majority**. Two disjoint quorums cannot both commit.
- **`MembershipVersion` is an optimistic-concurrency fence.** Each request carries the version it was
  computed against. The P0 leader answers `StaleMembership` if the committed version moved, and
  `ConcurrentMembershipChange` if another change is in flight.
- **Gossip carries only committed rosters.** A node learns of a change in O(log N) rounds. A malicious or
  buggy gossip message cannot corrupt quorum.
- **Discovery is seed contact.** `IDiscovery` finds bootstrap contact points and seeds the *initial*
  greenfield roster. The membership semantics do not depend on which discovery you use.

---

## Core concepts

### The roster is a replicated log record

All nodes have a copy of `ClusterMembership`. Each change is durable in the WAL and serialized, and no
node treats the new roster as truth until a quorum of voters acknowledged the entry.

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

The `Endpoint` is the identity key. `MembershipVersion == 0` means that no roster is committed yet, which
is the pre-seed state of a new cluster.

### Roles and the member lifecycle

```
   join                promote                leave / evict
 ──────────▶ Learner ──────────▶ Voter ──────────▶ Leaving ──────────▶ [removed]
              (no quorum)        (quorum)          (no longer
                                                    campaigns)
```

- **Learner** — receives replication to catch up. It does **not** count toward quorum and may not start or
  win elections.
- **Voter** — counts toward quorum and can be elected.
- **Leaving** — a committed state for a node whose decommission drain is in progress (`RequestLeaveAsync`
  with replica placement active). The node leaves the roster-level quorum at the commit point and stops
  campaigning. It remains in all peer lists, because the learners that replace it catch up from it. It also
  counts toward the quorum of any *range* whose committed replica set names it, until that replica is
  dropped. The state is **reversible**: a drain that is refused or times out rolls the role back to
  `Voter`. Only one member may be `Leaving` at a time. `LeaveCluster` also sets `LocalRole == Leaving`
  locally, with no committed role change and no drain, after its attempt to commit the removal ends.
- **NotMember** — the value of `RaftManager.LocalRole` when the local node is absent from the committed
  roster. It is not stored.

### Membership commits on the system partition (P0)

All membership mutations commit on P0 (`RaftSystemConfig.SystemPartition == 0`). You can write to user
partitions (P1, P2, …) while a join or removal is in progress, because their quorum does not depend on the
new member until *after* promotion.

### How the voter set is derived

`RaftManager.Nodes` is the peer set for replication. It is a **projection of the committed roster**:
all members (`Voter`, `Learner`, `Leaving`) except self. `ClusterHandler.UpdateNodes()`
recomputes it on each tick. A `Leaving` member must be in this set, or its drain wedges: the learners that
evacuate its ranges could not catch up.

**Quorum is not derived from this list.** Voter tallies always filter through the per-range committed
replica set or the `Voter` roles of the roster.

---

## The API surface

All membership operations are on the `IRaft` interface, which `RaftManager` implements.

### Read the current roster

```csharp
ClusterMembership m = raft.GetMembership();

Console.WriteLine($"Roster version: {m.MembershipVersion}");
foreach (ClusterMember member in m.Members)
    Console.WriteLine($"  {member.Endpoint}  {member.Role}");
```

`GetMembership()` returns a point-in-time snapshot.

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

`OnMembershipChanged` fires when this node's view of the committed roster advances to a strictly higher
`MembershipVersion`. This includes a fresher roster that gossip delivers before the Raft append reaches
this node. Events arrive in commit order and are not duplicated.

> ⚠️ The event fires on the single-consumer loop of the system coordinator. Your handler **must not block
> or call back into the coordinator**. Copy what you need and return.

### Join an existing cluster

A join from seeds needs no discovery service.

```csharp
await raft.JoinCluster(
    seeds: ["host1:7000", "host2:7000", "host3:7000"],
    cancellationToken: cts.Token);
// Blocks until this node is a committed Voter.
```

The other overload uses the `IDiscovery` that was registered at construction.

```csharp
await raft.JoinCluster(cancellationToken: cts.Token);
```

Both overloads block until this node is a committed Voter or the join fails. Pass a `CancellationToken` to
stop the join earlier. `JoinCluster(seeds)` has three steps.

1. Contact each seed until one (or its P0-leader hint) commits an `AddMember(Learner)` entry.
2. Wait for the P0 leader to replicate the partition map to this node (`IsInitialized`).
3. Wait for the leader to auto-promote this node from Learner to Voter.

| Option | Default | Bounds |
|---|---|---|
| `JoinTimeout` | 60 s | Steps 1 and 2, measured from the start of the join. A cluster that cannot admit or initialize the node in this time is wedged. |
| `JoinPromotionStallTimeout` | 5 min | Step 3, by progress. The join fails if the Learner makes no catch-up progress for this long. |

Step 3 is a snapshot install plus a backfill of what was committed meanwhile. It can take many minutes on
a cluster with a lot of data or writes, and the join waits while the Learner makes progress. Any of these
local events is progress.

- The commit index of a partition moves forward, or a new partition appears.
- A snapshot chunk is received.
- A snapshot install starts, continues, or completes.

The join logs an `Information` line every 10 s while it waits.

`JoinPromotionStallTimeout` must be longer than `LearnerPromotionStableWindow` plus `UpdateNodesInterval`,
or `Validate()` throws. On an idle cluster a caught-up Learner makes no progress during the stable window,
and a shorter value would fail a healthy join. Make it longer than the longest snapshot import of one
partition that shows no progress.

If a seed join fails after the node was admitted (initialization timeout, promotion stall, or a permanent
block), the node first asks the P0 leader to remove it from the roster, and then throws. This takes up to
10 s. If no leader acknowledges the removal in time, the node logs a `Warning`, and you must remove the
member manually. A join that you cancel with the token does not withdraw the node.

### Leave the cluster gracefully

```csharp
await raft.LeaveCluster(dispose: true);
```

`LeaveCluster` commits a `RemoveMember(self)` on P0, waits up to ~10 s for the removal to propagate back,
marks the node `Leaving` locally, and tears it down regardless of the result. If this node is the P0 leader,
it commits the removal under the old quorum and steps down. `LeaveCluster` does not drain replicas.

The node campaigns until the commit attempt ends, because it can be the only node able to become P0 leader.
That is the case when the previous P0 leader left with its last entry acknowledged by this node alone. If
the node stopped campaigning first, P0 would be leaderless for the full deadline, the removal would not
commit, and the survivors would count a dead node towards quorum.

While P0 has no leader, the attempt polls for one every 200 ms. It ends with `NoLeader` when P0 has no
leader and no open voting round on this node for 5 consecutive polls, or for `EndElectionTimeout` plus
`CheckLeaderInterval` plus two polls when that is longer (24 polls, about 4.6 s, with the default timing).
A P0 that is quiet for that long has lost quorum. A poll made while this node is a P0 candidate does not
count, and the count restarts when the voting round ends. Otherwise a split vote between the last two
voters, which lasts the full `VotingTimeout`, could end the attempt and leave the survivor with a stopped
node in a quorum of two. The 10 s deadline applies in all cases. `RequestLeaveAsync` uses the same rule.

### Decommission a node (drain, then leave)

```csharp
LeaveClusterResult result = await raft.RequestLeaveAsync(ct);
if (result.Left)
    Environment.Exit(0);        // safe to stop: the roster no longer names this node
```

When per-partition replica placement is active (the committed map names this endpoint and
`EnablePlacementRebalancer` is on), `RequestLeaveAsync` drains the node before it removes the node.

1. It commits `Voter → Leaving` on the roster. The call returns `RefusedDrainInProgress` if another member
   is draining, or `RefusedInsufficientVoters` if this is the last voter. In both cases the node continues
   to serve, unchanged.
2. It waits up to `DecommissionDrainTimeout` (default 2 minutes) while the placement pass on the P0 leader
   evacuates each replica of this node onto survivors. A range moves one replica at a time (add learner,
   catch up, promote, drop the leaver's replica) and does not drop below its quorum.
3. It commits `RemoveMember(self)` after no range names the endpoint. The result has `Drained == true`.

A drain that exceeds the timeout rolls the role back to `Voter` and returns `DrainTimedOut`. The node
serves and campaigns again, and another `RequestLeaveAsync` call **resumes** the drain from the replicas
that remain. If the placement pass commits the removal while the timeout fires, removal wins and the call
returns `Committed`. If a node crashes after `Leaving` commits, the next pass on the (possibly new) P0
leader continues the evacuation and commits the final removal.

Clusters with full replication (RF 0) or with the rebalancer off skip the drain and remove the node
directly.

**An eviction voids a decommission.** SWIM can evict a node while its `RequestLeaveAsync` is in force:
mid-drain, after a `Timeout` whose `Leaving` commit landed late, or after a `Timeout` that committed no
change. Its replicas were not evacuated. The call returns `Evicted` (and not `Left`, so a caller that stops
the process on `Left` does not lose the replicas). The node drops its leave request and, with
`EnableAutoRejoin` on, re-admits itself. Retry the decommission when the node is a `Voter` again. The
committed roster records *why* each member left (`ClusterMembership.Departures`), because the member list
alone cannot tell an eviction from a requested removal. A node removed by request, through a `RemoveMember`
it sent or a drain that the pass completed, must remain out.

> ⚠️ **Rolling-upgrade hazard:** a peer with a pre-drain version drops `Leaving` members from its peer list.
> That severs replication to the draining node and wedges the drain. The role-transition RPC
> (`SetMemberRole`) does not exist on old peers and fails loudly there. Do not start a decommission drain
> against a mixed-version cluster.

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

**Quorum is unaffected during catch-up.** The original voters commit throughout.

### Leaving (graceful, shutdown-coupled — `LeaveCluster`)

```
Leaving node                P0 leader
   |                            |
   |  (still campaigning: it may have to become the P0 leader itself)
   |--LeaveRequest(endpoint)--->|
   |                            |--AppendLogs(RemoveMember)-->voters
   |                            |<--ack majority → commit → roster shrinks
   |<--LeaveResponse(ok)--------|
   |  LocalRole = Leaving (local latch: stops campaigning; no committed role change)
   |  shutdown
```

### Leaving (decommission drain — `RequestLeaveAsync` with placement active)

```
Leaving node                P0 leader                       Survivors
   |                            |                               |
   |--SetMemberRole(Leaving)--->|                               |
   |                            |--AppendLogs(members)--------->|   Voter → Leaving commits;
   |<--ok-----------------------|                               |   only one drain at a time
   |                            |                               |
   |        [placement passes: per range hosted by the leaver]  |
   |                            |--AddReplica(survivor)-------->|   learner catches up FROM the
   |<====== replication ======> |                               |   leaver (it stays in Nodes)
   |                            |--PromoteReplica-------------->|   survivor enters range quorum
   |                            |--RemoveReplica(leaver)------->|   leaver's replica shed
   |                            |                               |
   |  [map no longer names the leaver — observed locally]       |
   |--LeaveRequest(endpoint)--->|                               |
   |                            |--AppendLogs(RemoveMember)---->|   roster shrinks
   |<--LeaveResponse(ok)--------|                               |
   |  caller decides when to stop the process (Drained == true) |
```

A drain that exceeds `DecommissionDrainTimeout` sends `SetMemberRole(Voter)` to roll back. If the pass
already committed the final removal, the rollback finds the member absent and the result is `Committed`. A
P0 leader that drains **itself** steps down from all partitions it leads when its `Leaving` commits
(Raft §6: a non-voter leader must not commit). The surviving voters elect a new P0 leader, which finishes
the drain.

A removal that would leave zero voters returns `InsufficientVoters`, a terminal status that must not be
retried. A removal from 2 voters to 1 is allowed.

### Leaving (crash → automatic eviction)

If a node dies and does not call `LeaveCluster`, SWIM marks it `Suspect` and then `Dead`. After
`DeadMemberEvictionGrace` the P0 leader commits a `RemoveMember` for it. The roster that commits the
eviction records it in `Departures` (endpoint, version, `Evicted`) until the endpoint is admitted again. An
evicted node usually learns the roster through a gossip push that skips versions, and it must be able to
read the eviction from whichever roster reaches it first.

---

## New-node catch-up

A Learner, or any follower whose log trails the leader, is caught up by **bounded log backfill**. The leader
ships missing **committed** entries in chunks of `MaxBackfillEntriesPerRound`, at a lower priority than
client traffic, and each chunk is idempotent. A node below the compaction
floor needs a snapshot install. For more information, see the
[Log Catch-Up & Backfill Developer Guide](log-backfill-developer-guide.md).

---

## Failure detection (SWIM)

### How it works

1. **Direct probe.** Each node periodically sends a `Ping` to one random peer.
2. **Indirect probe.** If the ping exceeds `PingTimeout`, the prober asks up to `IndirectPingFanout` other
   peers to relay a probe (`PingReq`). This rules out a path failure between the two nodes.
3. **Suspect.** If the direct probe *and* all indirect probes fail, the target is marked `Suspect`.
4. **Dead.** A `Suspect` that is unreachable for `SuspicionTimeout` becomes `Dead`.
5. **Evict.** After `DeadMemberEvictionGrace`, the **P0 leader** (and only the P0 leader) probes the node
   one last time, and commits a single `RemoveMember` if there is no answer. At least one voter must remain.

Liveness state (`Alive`/`Suspect`/`Dead`) and incarnation numbers are in the **gossip layer**
(`LivenessTable`), and not in the committed roster. A node that flaps does not churn the Raft log.

### Self-refutation (incarnation numbers)

Each node has an **incarnation counter**. When a node learns through gossip that it is `Suspect`, it bumps
its incarnation and broadcasts an `Alive` entry. The higher incarnation wins the merge and overwrites the
stale `Suspect`. `Dead` is sticky: rumors and indirect evidence cannot downgrade it, and only a strictly
higher incarnation, from a refutation or a successful direct probe, overrides it. An evicted node must
rejoin. Set `SuspicionTimeout` long enough to absorb normal network blips.

---

## Configuration reference

All settings are on `RaftConfiguration`.

### Catch-up / promotion

| Setting | Default | Meaning |
|---|---|---|
| `BackfillThreshold` | `10` | Leader backfills a follower whose `MaxLogId` is this many entries behind `commitIndex`. |
| `MaxBackfillEntriesPerRound` | `128` | Max committed entries shipped per backfill round. |
| `LearnerPromotionLag` | `10` | A Learner within this many entries of the committed log is eligible for promotion. |
| `LearnerPromotionStableWindow` | `3 s` | A Learner must be caught up this long before promotion. |

### Gossip (anti-entropy)

| Setting | Default | Meaning |
|---|---|---|
| `GossipInterval` | `5 s` | How often a node gossips the roster to random peers. |
| `GossipFanout` | `2` | Peers contacted per gossip round. `0` disables gossip. |

### Failure detector (SWIM)

| Setting | Default | Meaning |
|---|---|---|
| `PingInterval` | `1 s` | How often to probe a random peer. `Zero` disables the detector. |
| `PingTimeout` | `500 ms` | Direct/indirect probe response deadline. |
| `IndirectPingFanout` | `2` | Number of relays used for indirect probes. |
| `SuspicionTimeout` | `5 s` | Time a node is `Suspect` before it becomes `Dead`. |
| `DeadMemberEvictionGrace` | `10 min` | Time a node is `Dead` before the P0 leader commits `RemoveMember`. Set it to the longest outage the roster should survive intact. |

An evicted member rejoins as a Learner and votes again after it catches up on all partitions. On a
three-voter cluster, eviction has no benefit and costs fault tolerance during that window. Eviction helps on
clusters with spare nodes, where the placement pass re-replicates the ranges of the evicted member. While
evicted members are out, the P0 leader logs a `Warning` once a minute.

---

## Code map

### Data model
- `Kommander/System/ClusterMembership.cs` — the versioned roster record, with `Departures`, the bounded
  record of recent removals and their reasons.
- `Kommander/System/ClusterMember.cs` — one member entry.
- `Kommander/System/ClusterMemberRole.cs` — `Learner | Voter | Leaving | NotMember`.
- `Kommander/System/ClusterDeparture.cs` — one departure record and its reason (`Requested | Evicted`).
  `TryRemoveMember` writes it from the request's `RemovalReason`, and `ClusterLeaveService` reads it to
  decide whether an exclusion voids a leave request.

### Commit path and drivers
- `Kommander/System/RaftSystemCoordinator.cs`
  - `TryAddMember` / `TryPromoteMember` / `TryRemoveMember` — the single-server mutations.
    `TryRemoveMember` has the `InsufficientVoters` guard and records the departure reason. `TryAddMember`
    drops the record of a re-admitted endpoint.
  - `TrySetMemberRole` — the committed `Voter ⇄ Leaving` transition and its guards.
  - `CheckLearnerPromotionsAsync` — the promotion driver (P0 leader only, lag and stable window).
  - `EvictDeadMembersAsync` — the eviction driver (P0 leader only, reads `LivenessTable`).
  - `ApplyGossipRoster` — applies a gossiped roster to the local cache (monotonic, no log write).
  - `TrySeedInitialMembership` — greenfield all-voters seed on first leadership.
  - `ApplyMembershipFromCache` — applies the committed `members` key on `ConfigReplicated`.
  - Callers of `RaiseMembershipChanged` — where `OnMembershipChanged` is fired.
- `ReplicaPlacementService.RunPlacementPassAsync` — evacuation, the drop of a leaver's transitional learner,
  and the sweep that removes a fully evacuated member.

### Peer-set derivation
- `Kommander/ClusterHandler.cs` — `UpdateNodes()` projects the roster into `manager.Nodes`.

### Role gating (elections)
- `Kommander/Consensus/ElectionCoordinator.cs` — `StartElectionAsync`/`StartPreVoteAsync` return early unless
  the local role is `Voter`. `VoteAsync`/`ReceivedVoteAsync` deny non-roster endpoints.

### Join / leave / gossip / ping — handlers and round drivers
- `Kommander/RaftManager.cs` — the public surface: `LocalRole`, `GetMembership`, `OnMembershipChanged`, and
  `Liveness` (this node's `LivenessTable`).
- `Kommander/ClusterJoinService.cs` — `JoinCluster` (both overloads), `ReceiveJoin`.
- `Kommander/ClusterLeaveService.cs`
  - `LeaveCluster`, `ReceiveLeave`, `CommitGracefulLeaveAsync` — graceful leave.
  - `RequestLeaveAsync`, `DrainBeforeLeaveAsync`, `RollBackDrainAsync`, `CommitRoleTransitionAsync`,
    `ReceiveSetMemberRole` — the decommission drain (client and P0-leader server).
- `Kommander/GossipService.cs`
  - `GossipAsync`, `ReceiveGossip` — one gossip round and its receiver.
  - `PingAsync`, `ReceivePing`, `ReceivePingReq` — one SWIM probe round and its receivers.

### Gossip / SWIM types
- `Kommander/Gossip/GossipMessage.cs` — the push-pull anti-entropy message and ack (full-state push).
- `Kommander/Gossip/LivenessTable.cs` — thread-safe SWIM state with the merge rule and refutation.
- `Kommander/Gossip/MemberLivenessState.cs`, `MemberLivenessEntry.cs`, `PingMessage.cs`.

### Timers
- `Kommander/RaftTimerService.cs` — the update-nodes, gossip, and ping timers (`TriggerGossip`/`TriggerPing`
  with overlap guards). The ping timer is created if `PingInterval > 0`.

### Transport
- `Kommander/Communication/ICommunication.cs` — `SendJoin`, `SendLeave`, `SendGossip`, `SendPing`,
  `SendPingReq`, `GetRemoteFollowerLag`.
- `…/Memory/InMemoryCommunication.cs`, `…/Grpc/GrpcCommunication.cs`, `…/Rest/RestCommunication.cs` — the
  built-in implementations.

---

## Transport support matrix

"Wired" means that the RPC crosses the network.

| Capability | InMemory | gRPC | REST |
|---|---|---|---|
| Roster commit / replication (Add/Promote/Remove) | ✅ wired | ✅ wired | ✅ wired |
| Join RPC | ✅ wired | ✅ wired | ✅ wired |
| Graceful leave RPC (`SendLeave`) | ✅ wired | ✅ wired | ✅ wired |
| Drain role transition (`SendSetMemberRole`) | ✅ wired | ✅ wired | ✅ wired |
| Cross-partition lag (`GetRemoteFollowerLag`) | ✅ wired | ✅ wired | ✅ wired |
| Gossip anti-entropy (`SendGossip`) | ✅ wired | ✅ wired | ✅ wired |
| SWIM probes (`SendPing`/`SendPingReq`) | ✅ wired | ✅ wired | ✅ wired |
| Snapshot install for catch-up below floor (`SendInstallSnapshot`) | ✅ wired | ✅ wired | ✅ wired |

`ICommunication.SendInstallSnapshot` has a default implementation that returns `SnapshotResponse(false)`. If
a **custom** transport does not override it, followers below the compaction floor cannot catch up. A custom
transport must also carry the typed reply, `SnapshotResponse.Outcome` (`SnapshotInstallOutcome`).

| Outcome | What the sender does |
|---|---|
| `Installed` | Advances its cursors and logs "seeded". |
| `SkippedAlreadyCovered` | Logs a skip. |
| `ChunkAccepted` for a terminal chunk | Treats the transfer as failed. |
| No outcome | Reads the reply as `Rejected`. |

---

## Operations runbook

### Add a node
1. Start the new process with seed endpoints (or a shared discovery service).
2. Call `JoinCluster(seeds, ct)` and `await` it. When it returns, the node is a committed Voter.
3. Confirm that `raft.GetMembership()` on any node shows the new endpoint with `Role == Voter`.

### Remove a node (planned)
1. Prefer `RequestLeaveAsync()` on the node to retire. Stop the process after `result.Left`, and read
   `result.Drained` to see whether replicas were evacuated. On `RefusedDrainInProgress`, retry later. On
   `DrainTimedOut`, retry to resume the drain. Decommission one node at a time.
2. `LeaveCluster(dispose: true)` is the shutdown-coupled variant. It does not drain replicas.
3. Confirm that the roster on a surviving node no longer lists it and that `MembershipVersion` advanced.

### Replace a dead node
1. SWIM evicts the dead endpoint automatically after `SuspicionTimeout` + `DeadMemberEvictionGrace`. A node
   that crashed mid-drain (role `Leaving`) is evacuated and removed by the placement pass.
2. Join the replacement with `JoinCluster`.

### What to watch
- `MembershipVersion` should be identical across healthy nodes. Persistent divergence means that a node does
  not receive replication or gossip.
- Compare the roster size with the expected node count. A Learner that is not promoted shows up here.
- Connect `OnMembershipChanged` to your logs or metrics.

---

## Troubleshooting

| Symptom | Likely cause | Where to look |
|---|---|---|
| `JoinCluster` throws `TimeoutException` with `JoinTimeout` in the message | Seeds unreachable, or the cluster does not send the partition map to the node | `ReceiveJoin`; the assembly state in the message (`systemLeader`, `p0MaxLog`, `userPartitions`) |
| `JoinCluster` throws `TimeoutException` with `JoinPromotionStallTimeout` in the message | The Learner stopped catching up: no commit index moved and no snapshot work happened for the stall timeout | The commit indexes and last progress in the message; `BackfillSender`; snapshot transfer to the node |
| New node is `Learner` indefinitely | Lag does not reach `LearnerPromotionLag`, or the node cannot catch up on a partition (e.g. below the compaction floor and no snapshot install) | `CheckLearnerPromotionsAsync`; `GetRemoteFollowerLag` in `ICommunication`; partition compaction floor |
| Healthy nodes get evicted | `SuspicionTimeout` or `PingTimeout` too short for network latency; or nodes are unreachable | Increase `SuspicionTimeout`; check connectivity; set `PingInterval = Zero` to disable |
| `LeaveCluster` doesn't shrink the roster | The node couldn't reach the P0 leader, or the removal would leave zero voters (`InsufficientVoters`) | `CommitGracefulLeaveAsync`, `TryRemoveMember`; connectivity to the P0 leader |
| Roster versions differ between nodes | A node does not receive replication or gossip | `UpdateNodes`, `ApplyGossipRoster` |
| `RemoveMember` returns `InsufficientVoters` | Removal would leave zero voters | By design. Don't retry. |
| Commit stalls right after a node is added | Learner counted toward quorum, which is a bug | Voter tallies in `Kommander/Consensus/`; invariant 1 |

---

## Invariants you must not break

A violation of these rules risks split-brain or lost commits.

1. **Quorum is computed only from committed voters.** Learners, gossip, and discovery must not enter the
   quorum denominator.
2. **The roster changes one member at a time, through P0's Raft log.** Bulk rewrites and gossip-driven
   mutations of the committed roster are not allowed.
3. **Gossip carries only already-committed rosters.** `ApplyGossipRoster` must apply strictly higher versions
   only, and must not write to the WAL or the log.
4. **Only the P0 leader proposes evictions and promotions**, and one at a time.
5. **`MembershipVersion` is monotonic** and is the fence on each mutation. Don't apply a lower or equal
   version over a higher one.
6. **A `Leaving` member is out of the roster-level quorum and stops campaigning.** It remains in all peer
   lists, and it counts toward the quorum of any range whose committed replica set names it.
7. **Don't enable a feature on a transport whose RPC is a stub.** A custom `ICommunication` must implement
   `SendPing`/`SendPingReq` before `PingInterval > 0` is safe.

---

## Testing

- Membership tests: `Kommander.Tests/TestMembership.cs` (seed, derivation, roles, graceful leave,
  quorum-not-inflated, gossip anti-entropy, SWIM eviction/refutation).
- End-to-end join: `Kommander.Tests/TestFourNodeJoin.cs`.
- Election/role-gating units: `Kommander.Tests/Scheduling/TestRaftPartitionStateMachine.cs`.

```sh
dotnet test Kommander.Tests/Kommander.Tests.csproj --filter "FullyQualifiedName~TestMembership"
```

> **Do not start multiple `dotnet test` commands in parallel or in the background.** Concurrent in-process
> Raft clusters interfere and produce false failures.

Run the full test project when you change shared code, such as scheduling or cluster coordination.
