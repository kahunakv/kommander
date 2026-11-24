# Replication Factor & Per-Partition Replica Placement — Developer Guide

This guide is for developers who use or maintain replication factor and replica placement. It
builds on the partition and system-partition concepts in the
[Architecture Overview](architecture-overview.md). Placement applies the learner lifecycle of the
[Dynamic Cluster Membership guide](dynamic-membership-developer-guide.md) and the
planner/controller pattern of the [Leader Balancer guide](leader-balancer-developer-guide.md) per
partition. Placement decides *which nodes host a range*, and the leader balancer decides *which of
those replicas leads*.

---

## Table of contents

1. [Summary](#summary)
2. [Limits of full replication](#limits-of-full-replication)
3. [Core concepts](#core-concepts)
4. [Per-partition peers and quorum](#per-partition-peers-and-quorum)
5. [Partial materialization](#partial-materialization)
6. [Replica moves](#replica-moves)
7. [Seeding a new replica: whole-partition snapshots](#seeding-a-new-replica-whole-partition-snapshots)
8. [The placement planner and the P0 controller](#the-placement-planner-and-the-p0-controller)
9. [Routing and forwarding](#routing-and-forwarding)
10. [Interplay: initial placement, split/merge, membership](#interplay-initial-placement-splitmerge-membership)
11. [Configuration](#configuration)
12. [Safety](#safety)
13. [Code map](#code-map)
14. [Known limitations](#known-limitations)

---

## Summary

With a **replication factor** (RF) configured, each partition range has a fixed number of copies,
for example 3, on a *subset* of nodes recorded in the committed partition map. Quorum for a range
is computed over *that range's* voter replicas: a 24-node cluster commits an RF=3 range with 2-of-3
acks. Nodes materialize the partitions they replicate, and an optional controller rebalances
replicas as nodes join or leave.

The feature is **off by default** (`ReplicationFactor = 0`, full replication). A range whose replica
set is empty is fully replicated, and existing clusters and maps work unchanged. When a replication
factor applies to such a range, the controller converts it to a placed range once
`EnablePlacementRebalancer` is on (see
[Interplay](#interplay-initial-placement-splitmerge-membership)). This covers a cluster that
bootstrapped at or below its factor and a factor turned on later.

```
Full replication (before):          RF = 3 on six nodes (after):

 node A: P1 P2 P3 P4                 node A: P1 P4        node D: P2 P3
 node B: P1 P2 P3 P4                 node B: P1 P4        node E: P2 P3
 node C: P1 P2 P3 P4                 node C: P1 P2        node F: P3 P4
 node D: P1 P2 P3 P4
 node E: P1 P2 P3 P4                 per range: 3 copies, quorum = 2
 node F: P1 P2 P3 P4                 per node: 2 ranges, not 4
```

---

## Limits of full replication

A full-replication cluster has three limits as it grows.

1. **Every node carries every range.** Storage and replication work scale with
   `partitions × nodes`.
2. **Quorum width grows with the cluster.** With 24 voters, a partition needs 13 acks to commit.
3. **Data cannot be placed.** Capacity, locality (rack/zone), and per-range load cannot influence
   where a range lives.

---

## Core concepts

### Replica set
Each entry in the committed partition map (`RaftPartitionRange`) can carry a list of **replicas**:
the nodes that host the range and the role of each one. The map is changed through the system
partition (P0) only.

**An empty replica list means legacy full replication**: all roster voters host the range. Old maps
load with empty lists, and `ReplicationFactor = 0` leaves them empty.

### Replica roles
A replica has one of three roles, scoped to one range.

| Role | Hosts the range? | Counts toward quorum? | Meaning |
|---|---|---|---|
| `Voter` | yes | **yes** | a full copy; the normal state |
| `Learner` | yes | no | catching up after being added; promoted once it keeps up |
| `Removing` | yes | no | marked for removal; serves until the final drop |

### Effective replication factor
`RaftConfiguration.ReplicationFactor` is the cluster-wide target, and a range may carry an
override. The effective RF is `min(target, number of live voters)`. A cluster with fewer voters
than RF is fully replicated (all voters host the range), with no loss of safety.

With the rebalancer on, the controller records that full set as an explicit replica set, and the
planner expands it as voters join or trims it once voters exceed RF. With the rebalancer off, the
set remains empty and each node that joins adds one more copy.
`IRaft.GetPartitionsAwaitingPlacement()` lists such ranges.

### Generation as the fence
A placement change bumps the range's `Generation`, as a split or merge does. A proposal fenced on an
older generation returns `PartitionMoved`. Retry it against the refreshed map.

### Single mover per range
At most **one** replica of a range is in a transitional role (`Learner` or `Removing`) at a time.
Two successive committed configurations of the range therefore overlap in a quorum, which prevents
split brain during a move.

---

## Per-partition peers and quorum

The partition state machine sees peers through its host interface.

```csharp
// Scheduling/IRaftPartitionHost.cs — the state machine's entire view of "who else is out there"
IReadOnlyList<RaftNode> Nodes { get; }
bool IsVoter(string endpoint);
```

All quorum computations (elections, commit, check-quorum, read-index confirmation) use `host.Nodes`
filtered by `host.IsVoter`, and the host adapter answers both per partition.

```csharp
// RaftPartitionHostAdapter
public IReadOnlyList<RaftNode> Nodes => manager.GetPartitionPeers(partition.PartitionId);
public bool IsVoter(string endpoint) => manager.IsPartitionVoter(partition.PartitionId, endpoint);
```

`GetPartitionPeers` returns the range's replica set (minus self) when one is assigned, and the
whole-cluster projection otherwise. P0 always uses the whole-cluster projection. `IsPartitionVoter`
returns true for the range's `Voter` replicas only. Learners and removing replicas are peers that
receive appends and do not count in the quorum denominator.

A node starts an election or pre-vote for a range only if it is a cluster-roster Voter **and** a
`Voter` replica of that range. The second check prevents a learner replica of the range from
counting itself into a quorum that the committed replica set did not grant it.

A replica-set change commits through P0's log, and P0's quorum alone would let it take effect while
the target range's own voters are unreachable. The coordinator therefore fences `AddReplica`,
`PromoteReplica`, and the first (quorum-changing) commit of `RemoveReplica` on a quorum-confirmed
leadership of the target range. The fence is a read-index round on the range's leader: local when
the P0 leader leads the range, and forwarded through `ICommunication.GetReadIndex` otherwise. If no
confirmed leader is reachable, the result is `RaftOperationStatus.LeadershipNotConfirmed`, the map
is untouched, and the placement pass retries on a later tick. The final drop of a replica already
marked `Removing` changes no quorum and is exempt.

The fence waits on the coordinator loop, between the handler's read of the map and its write. Each
forwarded `GetReadIndex` is bounded by `RaftConfiguration.RemoteProbeTimeout`, and a candidate that
does not answer in time counts as unconfirmed. If committed map entries from another P0 leader are
installed meanwhile, the write returns `RaftOperationStatus.StaleSystemConfiguration` and is not
proposed. The placement pass re-plans from the installed map on its next tick. For the general rule,
see section 14 of the [elastic partitions guide](elastic-partitions-developer-guide.md).

---

## Partial materialization

When a node applies a committed map (`RaftManager.StartUserPartitions`), it checks for each range
whether it is in the replica set.

- **In the set (any role), or the set is empty (legacy):** materialize or update the local
  `RaftPartition`.
- **Not in the set:** do not materialize it. If the node *was* hosting it, the partition leaves the
  routing dictionaries immediately. It is drained and stopped in the background, and its WAL for
  that range is reclaimed.

The reclaim is safe. A node can leave a replica set only through the final step of the two-commit
removal (next section), and no configuration after that commit can need this node's copy.

Key routing and map queries read a snapshot of the **committed map**, and a node can route a key to
a range that it does not host.

---

## Replica moves

Placement changes are committed one at a time on the P0 coordinator's single-consumer loop, the same
loop that serializes roster changes and split/merge.

### Adding a replica (range R, node X)

```
1. AddReplica(R, X)      → commit; R.Generation++       X joins as Learner
2. (time passes)           X catches up via the normal   Learner never votes,
                           backfill / snapshot path      never counts in quorum
3. PromoteReplica(R, X)  → commit; R.Generation++       X becomes Voter — THIS
                                                         commit is the quorum entry
```

In step 1, X materializes R as a learner and the range's leader starts to replicate to it. The
controller commits the promotion after X's commit lag on R is within `LearnerPromotionLag` for the
stable window.

### Removing a replica (range R, node Y)

```
1. RemoveReplica(R, Y)   → commit; R.Generation++       Y marked Removing:
                                                         out of the quorum
                                                         denominator, still serving
2. (same handler)        → commit; R.Generation++       Y dropped from the set:
                                                         Y drains the partition and
                                                         reclaims R's WAL locally
```

If Y *leads* R, its leadership is handed to another voter replica **before** step 1, and the
hand-off is confirmed.

- When the P0 leader itself leads R, the local converging transfer is used.
- Otherwise a transfer suggestion goes to the leader node. The controller waits (three election
  timeouts, between two and ten seconds) until the quorum-confirmed leader of R is someone else.

A hand-off that does not complete refuses the removal for this pass with `LeadershipNotConfirmed`,
and the next pass retries. The attempt and its outcome are logged at Information, a refusal at
Warning.

The hand-off is mandatory. Followers of a quiesced range gate elections on the SWIM liveness of the
leader's node. If the leader left the replica set, its node remains alive and an election does
*not* recover the range. As a safety net, followers also treat a leader that is no longer in the
range's replica set as vacated. For more information, see the
[partition-quiescence guide](partition-quiescence-developer-guide.md).

If the process crashes between the two commits, the map durably shows a `Removing` replica. The
controller's next pass re-issues the removal, which skips to the final drop. All handlers are
idempotent against their own retry.

### Moving a replica (Y → X)

A move adds X (learner → voter) and afterwards removes Y. The two are not in flight at once (single
mover), and the range has a full committed-voter quorum at each intermediate step.

---

## Seeding a new replica: whole-partition snapshots

In the steady state the range's WAL prefix is compacted, and a new replica **cannot** be
bootstrapped by log backfill alone. It needs a snapshot of the entire partition state from the
leader. Register the transfer that produces one.

```csharp
public interface IRaftPartitionStateTransfer
{
    // Export the WHOLE application state of the partition, reflecting AT LEAST everything
    // applied at `upToIndex`. Newer committed state may be included (an MVCC store snapshots
    // by timestamp and cannot cut an exact as-of-index export): Kommander seeds the receiver's
    // checkpoint at `upToIndex` and replays retained log entries above it, so re-applying an
    // already-reflected entry must be a no-op (idempotent apply).
    Task<Stream> ExportPartitionState(int partitionId, long upToIndex, CancellationToken ct);

    // Atomically replace this node's state for the partition with the exported blob. May run
    // more than once for the same snapshot (the sender retries when the durable boundary write
    // fails after a successful import), so repeated import must be idempotent.
    Task ImportPartitionState(int partitionId, Stream snapshot, CancellationToken ct);
}

raft.RegisterPartitionStateTransfer(new MyPartitionStateTransfer());
```

Register it on **every** node that can host or lead user partitions: the leader needs the export,
the new replica needs the import, and registration is not replicated. These transfers travel with
`SnapshotKind.PartitionState`. All nodes must use a Kommander version that understands that kind
before the transfer is registered.

If no transfer is registered, the catch-up path calls `IRaftStateMachineTransfer.ExportRange` with
a *boundless* plan (only `TargetPartitionId` set, meaning "the entire partition"). If your range
transfer cannot serve such a plan, a below-floor replica can't be seeded until the dedicated
transfer is registered. The system partition (id 0) uses `IRaftSystemStateTransfer`; see the
[system partition state snapshots guide](system-partition-state-snapshots-developer-guide.md).

### How the receiver stages a snapshot

The receiving node stages the complete snapshot before it calls `ImportPartitionState`, and gives
the importer a seekable stream it may read more than once.

| Option | Default | Meaning |
|---|---|---|
| `SnapshotMaxPendingBytes` | 512 MiB | Staged bytes across all receive sessions, including snapshots with an install in progress. A session that cannot fit, even after evicting others, is refused. |
| `SnapshotMaxPendingSessions` | 8 | Concurrent receive sessions. |
| `SnapshotStagingDirectory` | null | Node-private directory for spill files. Null: all sessions are in memory. |
| `SnapshotStagingMemoryBytes` | 64 MiB | With a staging directory, the resident budget: a session whose next chunk would exceed it moves to a spill file first. |

If no staging directory is set, `SnapshotMaxPendingBytes` is both the memory bound and the largest
snapshot the node can receive. A partition that outgrows it can't be re-seeded there. On
memory-limited nodes you should set a staging directory, size `SnapshotMaxPendingBytes` for the
largest partition (it bounds disk plus memory), and bound the resident part with
`SnapshotStagingMemoryBytes`.

Spill files are deleted when their session ends, and files left by a crashed process are removed at
startup. A failed spill write (for example a full disk) fails that session alone, and the leader
retries it.

A leader starts a new session for each transfer attempt. When one opens, the receiver drops the same
partition's older pending sessions from that leader at or below its snapshot index, and any from a
lower leader term. The counters are `raft.snapshot.receive_sessions_superseded_total` and
`raft.snapshot.receive_sessions_spilled_total` (both tagged `partition_id`).

### The install is a step of its own

The last chunk starts the install on the partition executor and is answered at once with
`InstallPending`. The leader asks for the outcome separately and waits for as long as the install
makes progress. `SnapshotChunkAckTimeout` bounds a chunk acknowledgement and each of those
questions, and `ImportPartitionState` can take as long as it needs. (For a leader from a release
that does not ask, the last chunk's call is held until the install completes.)

A node has **one install per partition** at a time. While it is queued or in progress, the receiver
stages no other snapshot for that partition. The partition's other pending sessions are dropped
when the install starts. A chunk that would open another session is answered with `InstallPending`
naming the current install, and its sender waits for that outcome. At most one staged snapshot per
partition sits beside the import in progress, even after a leader change. The counter is
`raft.snapshot.receive_sessions_refused_installing_total` (`partition_id`).

These instruments publish the install cost, one figure per process.

| Instrument | Meaning |
|---|---|
| `raft.snapshot.receive_staged_bytes` | Bytes staged by sessions whose last chunk has not arrived. |
| `raft.snapshot.receive_installing_bytes` | Staged bytes of installs that are queued or in progress. |
| `raft.snapshot.receive_in_memory_bytes` | The in-memory part of both. |
| `raft.snapshot.install_peak_heap_bytes` | Highest managed-heap size sampled during an install. A high-water mark. |
| `raft.snapshot.install_duration_ms` | Histogram: last chunk to outcome, tagged `partition_id` and `outcome`. |

Each finished install also logs one Information line with its outcome, its duration, and its memory
figures (staged and resident bytes, managed heap before and after).

---

## The placement planner and the P0 controller

A **pure planner** works over an immutable view. A **controller** on the P0 leader invokes it and
turns its output into committed mutations.

### The planner (`PlacementPlanner.Plan`)

The input is the set of placeable ranges (voters, learners, RF, current leader), the candidate nodes
(roster voters, with SWIM liveness and an optional zone tag), and the configuration. The output is a
bounded list of `AddReplica` / `RemoveReplica` moves in this priority order.

1. **Repair under-replication.** A range with fewer *healthy* voters than its RF gets a new replica
   on the least-loaded live node. Repairs bypass the deadband.
2. **Trim over-replication.** A range with more voters than RF (after a merge union, an RF
   decrease, or a completed move) sheds the replica on the most-loaded node. When zones are known,
   a voter in a zone the range doubles up on goes first. The current leader is spared only among
   equally loaded candidates. A replica stranded on a node that left the roster is shed as soon as
   the healthy voters alone satisfy RF.
3. **Balance skew.** A node with more replicas than the even-spread ceiling plus the deadband
   donates one range to the least-loaded node that lacks it. The donation is an *add*, and the
   matching trim follows on a later pass, as over-replication, after the learner promotes. The add
   is emitted only if that trim, predicted with the trim's victim order, would shed a node strictly
   more loaded than the filled target. Otherwise the trim would remove the new replica and the pass
   would loop.

Stability rules limit churn.

- One move per range per plan, and no move for a range in mid-transition.
- The current placement wins ties.
- Anti-affinity across *nodes* is absolute. Across zones, when zone hints exist, it is best-effort.
- `MaxReplicaMovesPerPass` and the remaining transfer budget cap the plan.

The planner is stateless, and a new P0 leader loses no planner state.

### The controller (`RunPlacementPass`)

The pass has a dedicated timer, `PlacementPassInterval` (default 5 s), which is active whenever
placement is in play (`ReplicationFactor > 0` or `EnablePlacementRebalancer`). It acts on the P0
leader and is independent of the leader balancer's timer: a placed cluster with
`EnableLeaderBalancer` off converges. A replication-factor override or a member removal (graceful
leave or dead-node eviction) triggers an immediate pass after its commit. Each pass has three
steps.

1. **Drive transitions to completion.** It re-issues the final drop for a `Removing` replica and
   promotes learners that were caught up for the stable window. This happens **even when the
   rebalancer is disabled**, and an interrupted move always converges.
2. **Assign initial placement to legacy ranges**, if `EnablePlacementRebalancer` is on and the
   roster is committed. Ranges with an empty replica set and an effective RF above 0 get the
   explicit set of all roster voters (a quorum no-op; see *Converting a legacy range* below),
   `MaxReplicaMovesPerPass` ranges per pass in one commit. The same pass then plans against the
   converted map.
3. **Plan new moves**, if `EnablePlacementRebalancer` is on and budget remains. **Repairs**
   (re-replication of an under-replicated range, a shed from an evicted node) draw on
   `MaxConcurrentReplicaRepairs`. **Balance** moves (trims and skew spreading) draw on
   `MaxConcurrentReplicaTransfers` and yield to any repair in flight. The larger of the two budgets
   bounds total in-flight transitions.

**Worst-case convergence bound.** One relocation costs ~3 passes (add learner → observe caught up →
promote after `LearnerPromotionStableWindow`) plus one trim pass: ~4 × `PlacementPassInterval` per
batch of concurrent moves (~20 s at the defaults). To re-replicate *N* under-replicated ranges
takes about `ceil(N / MaxConcurrentReplicaRepairs) × 4 × PlacementPassInterval` (~20 s per batch of
3 ranges at the defaults) plus the time for each learner to catch up, which dominates for large
ranges.

Learner lag comes from the range leader's follower-progress table, read remotely when the P0 leader
does not lead the range. A learner that has not acked the range counts as **not caught up**, unlike
in the cluster-membership promotion driver.

---

## Routing and forwarding

Any node can resolve a key to its partition id. When the local node does not *host* that partition,
there are two paths.

- **The consumer routes directly (the fast path).** `IRaft.GetPartitionReplicas(partitionId)`
  returns the range's committed replica set (empty = all voters). Cache it, and refresh it on
  `OnPartitionMapChanged` or on a `PartitionMoved` result.
  `IRaft.GetPartitionLeaderHint(partitionId)` names the replica that leads, also for a range this
  node does not host. The hint comes from gossiped load reports (newest fresh claim wins) and may
  be null, stale, or wrong. The receiving node re-checks leadership on each forwarded operation.
  Use the hint as the first target and the replica set on a miss. Load-report gossip is on whenever
  placement, the leader balancer, or `EnableLoadReports` is enabled
  (`RaftConfiguration.LoadReportsEnabled`).

- **Local forward fallback.** `ReplicateLogs` on a non-replica node forwards the proposal to the
  range's replicas, voters first, and tries the next replica on a `NodeIsNotLeader` answer. Leader
  checks and the generation fence apply on the remote node. This works on the in-memory transport
  and is not wired on gRPC/REST, where consumers should use the direct path.

`GetPartitionGeneration` also answers for non-hosted ranges from the committed map, which lets a
non-replica node build a fenced proposal.

---

## Interplay: initial placement, split/merge, membership

**Initial placement.** When the P0 leader bootstraps a fresh cluster with `ReplicationFactor > 0`,
each initial range gets an RF-sized, all-`Voter` replica set assigned round-robin over the nodes
visible at bootstrap. For example, six nodes with four ranges at RF=3 put each range on 3 distinct
nodes, and each node hosts exactly 2 ranges. A cluster at or below RF nodes has empty replica sets
at bootstrap. A partition created later gets the RF least-loaded voters, or an empty set while
voters do not exceed RF.

**Converting a legacy range.** On each pass the controller counts the `Active` ranges whose set is
empty while their effective RF is above 0. The `raft.placement.ranges_awaiting_initial` gauge and
`IRaft.GetPartitionsAwaitingPlacement()` expose the same count. With `EnablePlacementRebalancer` on
and a committed roster, up to `MaxReplicaMovesPerPass` of them, lowest id first, get an explicit
replica set in **one** map commit that bumps their generation. The set is **every current roster
`Voter`**, dead or alive, each as a `Voter`.

A legacy range's quorum already is the roster's voter set, and recording it changes no quorum.
Nodes that have and have not applied the new map agree on the denominator. A one-shot cut to RF
voters would be unsafe: with 5 voters and RF 3, a majority of the new set and a majority of the old
one can be disjoint, and a choice of "caught-up" voters does not close the gap under concurrent
writes. The trim path shrinks the recorded set to RF one voter per commit, behind the leadership
fence and the leader hand-off.

Roster members that are not voters (`Learner`, `Leaving`) stop hosting the converted ranges. With
the rebalancer off, no range converts, and the P0 leader logs one Warning per leadership with the
number of ranges that wait.

For example, a 3-node cluster at RF 3 converts to explicit three-voter sets, and a fourth node that
joins hosts no data range until the balancer moves one onto it. A split made while voters do not
exceed RF gives the child an empty set, which the next pass converts.

**Split.** The child range **inherits the parent's replica set**. The data already lives on those
nodes, and the child is at RF from birth with no transfer. The rebalancer may spread it later.

**Merge.** The survivor takes the **union** of both replica sets, and all nodes that held either
range's data serve it. The rebalancer trims a union that is over RF.

**Membership.** A committed member removal (graceful leave or dead-node eviction) makes each range
that had a replica there under-replicated, and the planner repairs it at top priority. A newly
promoted voter is a new target for spreading. Placement and roster changes are serialized on the
same P0 loop.

**Decommission drain (`RequestLeaveAsync`).** In a *planned* departure the node first commits the
`Leaving` roster role, and the pass evacuates its replicas **while it serves**. A `Leaving` member
drops out of the candidate set. Priority 1 therefore counts its ranges as under-replicated (the add
draws on the repair budget), and the trim of its replica counts as repair too. The controller adds
three drain rules.

- A transitional **learner on the leaver is dropped**. A promotion would make the leaver a voter
  that the next pass has to evacuate again.
- The pass **completes the removal itself**: once no non-`Removed` range names a `Leaving` member,
  it commits the final `RemoveMember`. The drain converges even if the departing node crashes after
  `Leaving` commits or P0 leadership changes.
- Only **one member may be `Leaving` at a time**, guarded on roster state. Two concurrent drains
  would shrink the voter set by two before either evacuation completes.

A drain that cannot finish within `DecommissionDrainTimeout` rolls the role back to `Voter`.
Otherwise the node could not campaign again. If the pass's removal races that rollback, the removal
wins and the leave is reported as completed.

A `Leaving` member that the SWIM detector evicts before its evacuation is recorded in the roster as
`Evicted` (`ClusterMembership.Departures`). The leave is reported as `Evicted`, and the node
re-admits itself: its replicas are on it, and the learners that replace them can be fed only once
it is a member again.

Known limitation: a range whose *only* voter is the leaver (RF 1) can drain only while the leaver
leads that range. If leadership is lost mid-drain, the range has no electable voter until the drain
times out and rolls back. Treat RF 1 decommission as unsupported.

---

## Configuration

The settings are on `RaftConfiguration`.

| Setting | Default | Meaning |
|---|---|---|
| `ReplicationFactor` | `0` | Target voter copies per range. **0 = full replication (feature off).** Prefer odd values: RF 4 needs 3-of-4 to commit, the same failure tolerance as RF 3 at the cost of an extra copy. |
| `EnablePlacementRebalancer` | `false` | Master switch for *ongoing* rebalancing (repair, trim, skew) and for the conversion of legacy ranges that a factor applies to. Initial placement at bootstrap and in-flight transitions do not depend on it. |
| `PlacementPassInterval` | `5 s` | Cadence of the controller pass on the P0 leader. Bounds convergence speed (see the worst-case bound above). Non-positive disables the timer; event-driven passes are not affected. |
| `MaxReplicaMovesPerPass` | `4` | New moves initiated per controller pass, across all priorities. Set it ≥ `MaxConcurrentReplicaRepairs` + `MaxConcurrentReplicaTransfers` or it binds before the per-class budgets. |
| `MaxConcurrentReplicaTransfers` | `1` | Balance-class moves (trims, skew spreading) allowed mid-transition at once. Caps concurrent backfill/snapshot traffic. Repairs in flight count against it too, and balance work pauses during a repair wave. |
| `MaxConcurrentReplicaRepairs` | `3` | Repair-class moves (re-replication of under-replicated ranges, sheds of replicas on evicted nodes) allowed mid-transition at once. |
| `ReplicaCountDeadband` | `1` | Per-node replica-count imbalance tolerated before balance moves are emitted. Repairs ignore it. |
| `Zone` | `null` | Optional locality hint for the local node, gossiped on its load reports. The planner and the bootstrap assignment prefer distinct zones for a range's replicas. Whitespace-only values are normalized to null at `Validate()`. |

Per-range overrides go through `IRaft.SetReplicationFactorAsync(partitionId, rf)` (P0-leader-only;
`0` clears the override). An override changes the *target*, and replicas move on later controller
passes. Routing has not changed, and the range's generation is **not** bumped.

A typical scale-out configuration sets these values.

```csharp
RaftConfiguration config = new()
{
    InitialPartitions = 16,
    ReplicationFactor = 3,
    EnablePlacementRebalancer = true,
    Zone = "rack-2"
};
```

---

## Safety

1. **Single mover per range.** The mutation handlers answer a second transitional replica with
   `ConcurrentMembershipChange`. With two at once, two committed configurations can stop
   overlapping in a quorum, which is a split brain.

2. **Quorum only over committed voters.** Learners and removing replicas are peers and not quorum
   members, which prevents quorum *inflation*. A node campaigns for a range only if the committed
   set says it is a `Voter` of that range, which prevents a non-voter from counting itself.

3. **Never below one voter.** Removal of the last voter replica of a range is refused
   (`InsufficientVoters`). The range would be permanently uncommittable.

4. **WAL reclaim only on committed absence.** A node deletes a range's WAL when the committed map
   no longer lists it as a replica, or the range itself is removed. A transient condition does not
   delete data a later configuration could need.

5. **Anti-affinity.** Replica endpoints within a range are unique.

6. **Idempotent re-drive.** Lifecycle handlers treat "already done" as success, and controller
   decisions are re-derived from the committed map. P0 leadership can move at any point in a
   sequence, and the new leader converges and applies each step once.

7. **Legacy conversion is a quorum no-op.** The first replica set a legacy range receives is all
   roster voters, the quorum it already had. A direct cut to RF voters would be a multi-member
   change whose old and new majorities need not overlap.

A misconfigured rebalancer cannot break these rules. Quorum follows the committed voter set, and a
minority cannot commit. A move cannot lose data: an add fully promotes before the matching remove
is planned. The deadband and the tie rule make the even spread a fixed point (a test asserts this),
and a balanced cluster does not thrash.

---

## Code map

| File | What it holds |
|---|---|
| `Kommander/System/RaftPartitionRange.cs` | Map entry: `Replicas` list + per-range `ReplicationFactor` |
| `Kommander/System/RaftReplica.cs`, `RaftReplicaRole.cs` | The replica record and its role enum |
| `Kommander/System/ReplicaPlacementService.cs` | Lifecycle mutations (add/promote/remove/set-RF) + the controller pass |
| `Kommander/System/Placement/PlacementPlanner.cs` | The pure planner (`Plan`, `AssignInitial`) |
| `Kommander/System/Placement/PlacementView.cs` | Planner input/output types |
| `Kommander/System/RaftSystemCoordinator.cs` | Wires the service into the serial P0 loop |
| `Kommander/System/PartitionMapService.cs` | Initial placement at bootstrap; RF assignment for created partitions |
| `Kommander/System/SplitMergeController.cs` | Child inherits replicas on split; survivor takes union on merge |
| `Kommander/PartitionRoutingTable.cs` | `GetPartitionPeers` / `IsPartitionVoter`, committed-map routing |
| `Kommander/ReplicationGateway.cs` | The local forward fallback of `ReplicateLogs` |
| `Kommander/RaftManager.cs` | Partial materialization (`StartUserPartitions`), un-host teardown (`StopUnhostedPartition`) |
| `Kommander/Scheduling/RaftPartitionHostAdapter.cs` | `Nodes` / `IsVoter` become per-partition |
| `Kommander/Consensus/ElectionCoordinator.cs` | Campaign gates also check the local node's per-range voter role |
| `Kommander/IRaft.cs` | `GetPartitionReplicas`, `GetPartitionLeaderHint`, `HostsPartition`, `GetEffectiveReplicationFactor`, `SetReplicationFactorAsync` |
| `Kommander/Communication/ICommunication.cs` | `ForwardReplicateLogs` (in-memory transport implements it) |
| `Kommander.Tests/Scheduler/TestPlacementPlanner.cs` | Pure planner tests (even spread, repair, trim, stability, zones) |
| `Kommander.Tests/Scheduler/TestReplicaPlacement.cs` | Lifecycle, partial materialization, peer/quorum seam, initial placement |

---

## Known limitations

- **Wire forwarding.** The non-replica `ReplicateLogs` fallback works on the in-memory transport
  only. gRPC/REST transports report "unsupported" and the call fails as an unknown partition. Route
  directly with `GetPartitionReplicas` on those transports.
- **Zone at bootstrap.** Zones travel on gossiped load reports, and the bootstrap assignment may
  not know all zones yet. It uses even spread for the unknown ones, and the rebalancer improves the
  zone spread on later passes.
- **Batch proposals.** `ReplicateEntries` (the heterogeneous batch API) does not forward from
  non-replica nodes. Batch producers should route directly.
