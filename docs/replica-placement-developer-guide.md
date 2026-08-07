# Replication Factor & Per-Partition Replica Placement — Developer Guide

Welcome! This guide is for the next developer who wants to **use, understand, or maintain**
Kommander's replication factor and per-partition replica placement. It assumes you know roughly what
Kommander is (a Raft-based distributed system) but **no prior knowledge of replica placement**. We
build every concept from the ground up, walk through how a replica actually moves from one node to
another, and finish with configuration, safety invariants, and the code map.

If you are brand new to Kommander, skim the [Architecture Overview](architecture-overview.md) first —
it explains partitions, leaders, and the system partition, which this guide builds on. The
[Dynamic Cluster Membership guide](dynamic-membership-developer-guide.md) and the
[Leader Balancer guide](leader-balancer-developer-guide.md) are close relatives: placement reuses
their learner/promotion lifecycle and their planner/controller pattern, applied per partition.

---

## Table of contents

1. [Summary](#summary)
2. [The problem: full replication does not scale out](#the-problem-full-replication-does-not-scale-out)
3. [Core concepts](#core-concepts)
4. [The one behavioral pivot: per-partition peers and quorum](#the-one-behavioral-pivot-per-partition-peers-and-quorum)
5. [Partial materialization: hosting only what you replicate](#partial-materialization-hosting-only-what-you-replicate)
6. [How a replica moves, step by step](#how-a-replica-moves-step-by-step)
7. [The placement planner and the P0 controller](#the-placement-planner-and-the-p0-controller)
8. [Routing and forwarding](#routing-and-forwarding)
9. [Interplay: initial placement, split/merge, membership](#interplay-initial-placement-splitmerge-membership)
10. [Using it: configuration](#using-it-configuration)
11. [Safety: invariants you must not break](#safety-invariants-you-must-not-break)
12. [Code map — where everything lives](#code-map--where-everything-lives)
13. [Known limitations](#known-limitations)
14. [FAQ](#faq)

---

## Summary

Historically, Kommander was a **full-replication** multi-Raft: every partition replicated to *every*
node in the cluster. That is simple and works well for a handful of machines, but it means adding
nodes makes the cluster *store more copies* and *commit more slowly*, instead of spreading data out.

**Replica placement** changes that. With a **replication factor** (RF) configured, each partition
range keeps a fixed number of copies — say 3 — hosted on a *subset* of nodes recorded in the
committed partition map. Quorum for a range is computed over *that range's* voter replicas only, so a
24-node cluster still commits an RF=3 range with 2-of-3 acks. Nodes materialize only the partitions
they replicate, and an optional controller continually rebalances replicas as nodes join, leave, or
die.

The feature is **off by default** (`ReplicationFactor = 0` keeps today's full replication), and a
range whose replica set is empty always behaves the legacy way, so existing clusters and maps keep
working unchanged.

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

## The problem: full replication does not scale out

Three things go wrong as a full-replication cluster grows:

1. **Every node carries every range.** A cluster of dozens of machines still stores a complete copy
   of all data on each node, and every node runs a partition executor for every Raft group. Storage,
   WAL I/O, heartbeat fan-out, and replication work scale with `partitions × nodes`, not with the
   data.

2. **Quorum width grows with the cluster.** With 24 voters, every partition needs 13 acks to commit.
   Every write is bounded by the 13th-slowest node in the *whole cluster*. Adding machines makes
   writes slower — the opposite of scale-out.

3. **No way to place data.** Capacity, locality (rack/zone), and per-range load cannot influence
   where a range lives, because a range lives *everywhere*.

A replication factor solves all three: fixed durability per range, small constant-width quorums, and
an explicit, committed record of where each range lives that a planner can optimize.

---

## Core concepts

### Replica set
Each entry in the committed partition map (`RaftPartitionRange`) can carry a list of **replicas**:
which nodes host that range, and each replica's role. The map remains the single committed, versioned
truth, mutated only through the system partition (P0) — placement rides the same record that already
carries the range's hash bounds, state, and generation.

**An empty replica list means legacy full replication** — every roster voter hosts the range. This is
both the migration story (old maps deserialize with empty lists) and the off-switch (with
`ReplicationFactor = 0` nothing ever populates them).

### Replica roles
A replica is in one of three roles, mirroring the cluster-membership lifecycle but scoped to one
range:

| Role | Hosts the range? | Counts toward quorum? | Meaning |
|---|---|---|---|
| `Voter` | yes | **yes** | a full copy; the normal state |
| `Learner` | yes | no | catching up after being added; promoted once it keeps up |
| `Removing` | yes | no | marked for removal; still serves until the final drop |

### Effective replication factor
`RaftConfiguration.ReplicationFactor` is the cluster-wide target; each range may carry an override.
The effective RF is `min(target, number of live voters)` — a cluster with fewer voters than RF simply
degrades to full replication (all voters host the range), with no loss of safety, and expands back
out as voters join.

### Generation as the fence
Every placement change bumps the range's `Generation`, exactly like a split or merge does. Consumers
that cache the map fence their proposals on the generation; a proposal racing a placement change is
rejected with `PartitionMoved` and retried against the refreshed map. No new fencing machinery
exists — placement reuses the one you already know.

### Single mover per range
At most **one** replica of a given range is in a transitional role (`Learner` or `Removing`) at a
time. This is the per-partition application of Raft's one-change-at-a-time membership rule: it
guarantees any two successive committed configurations of the range overlap in a quorum, which is
what makes replica moves split-brain-free.

---

## The one behavioral pivot: per-partition peers and quorum

The partition state machine never knew about the cluster directly — it sees peers only through its
host interface:

```csharp
// Scheduling/IRaftPartitionHost.cs — the state machine's entire view of "who else is out there"
IReadOnlyList<RaftNode> Nodes { get; }
bool IsVoter(string endpoint);
```

Every quorum computation — pre-vote, election, vote acceptance, commit — reads `host.Nodes` filtered
by `host.IsVoter`. So the entire feature pivots on re-answering those two questions per partition:

```csharp
// RaftPartitionHostAdapter
public IReadOnlyList<RaftNode> Nodes => manager.GetPartitionPeers(partition.PartitionId);
public bool IsVoter(string endpoint) => manager.IsPartitionVoter(partition.PartitionId, endpoint);
```

`GetPartitionPeers` returns the range's replica set (minus self) when one is assigned, and the
whole-cluster projection otherwise (legacy ranges, and always for P0 — the system partition
replicates everywhere by design). `IsPartitionVoter` returns true only for the range's `Voter`
replicas; learners and removing replicas are peers (they receive appends and can catch up) but never
count in the quorum denominator.

Two campaign gates complete the picture: a node will not start an election or pre-vote for a range
unless it is a cluster-roster Voter **and** a `Voter` replica of that specific range. Without the
second check, a perfectly legitimate cluster member that happens to be only a learner replica of a
range could campaign for it and count itself into a quorum the committed replica set never granted
it.

Everything else — commit counting, check-quorum, read-index confirmation — follows automatically,
because it all flows through the same two host members.

---

## Partial materialization: hosting only what you replicate

When a node applies a committed map (`RaftManager.StartUserPartitions`), it now asks per range:
*am I in this range's replica set?*

- **Yes (any role), or the set is empty (legacy):** materialize/update the local `RaftPartition` as
  before.
- **No:** do not materialize it. If the node *was* hosting it, the partition is removed from the
  routing dictionaries immediately, then drained and stopped in the background, and its WAL for that
  range is reclaimed.

Reclaiming on un-host is safe for a specific reason worth internalizing: the only committed way a
node leaves a replica set is the final step of the two-commit removal (next section). Observing
"I am absent from the committed replica set" *is* observing that commit, so no later configuration of
the range can need this node's copy.

One consequence ripples into routing: the local partition dictionary no longer contains every range,
so anything that answers "which partition owns this key?" or "what does the map look like?" now reads
a snapshot of the **committed map** rather than the hosted set. A node can route any key to any
range — including ranges it does not host.

---

## How a replica moves, step by step

Every placement change is one committed map mutation at a time, processed serially on the P0
coordinator's single-consumer loop — the same discipline as roster changes and split/merge, and on
the same loop, so they can never interleave badly.

### Adding a replica (range R, node X)

```
1. AddReplica(R, X)      → commit; R.Generation++       X joins as Learner
2. (time passes)           X catches up via the normal   Learner never votes,
                           backfill / snapshot path      never counts in quorum
3. PromoteReplica(R, X)  → commit; R.Generation++       X becomes Voter — THIS
                                                         commit is the quorum entry
```

Step 1 makes every node apply the new map; X materializes R as a follower/learner and the range's
leader starts replicating to it. The controller watches X's commit lag on R; once it stays within
`LearnerPromotionLag` for the stable window, it commits the promotion.

### Removing a replica (range R, node Y)

```
1. RemoveReplica(R, Y)   → commit; R.Generation++       Y marked Removing:
                                                         out of the quorum
                                                         denominator, still serving
2. (same handler)        → commit; R.Generation++       Y dropped from the set:
                                                         Y drains the partition and
                                                         reclaims R's WAL locally
```

If Y currently *leads* R, leadership is first transferred to another voter replica (best-effort — an
election would recover anyway; the transfer just avoids an availability blip). If the process crashes
between the two commits, the map durably shows a `Removing` replica, and the controller's next pass
re-issues the removal, which skips straight to the final drop. Every handler is idempotent against
its own retry.

### Moving a replica (Y → X)

A move is nothing new: *add X (learner → voter), then remove Y* — never both in flight at once
(single mover). The range holds a full committed-voter quorum at every intermediate step.

---

## The placement planner and the P0 controller

The design copies the leader balancer's proven shape: a **pure planner** over an immutable view,
invoked by a **controller** that runs only on the P0 leader and turns plan output into committed
mutations.

### The planner (`PlacementPlanner.Plan`)

Input: every placeable range (its voters, learners, RF, current leader), every candidate node
(roster voters, with SWIM liveness and an optional zone tag), and the knobs. Output: a bounded list
of `AddReplica` / `RemoveReplica` moves. Priorities, highest first:

1. **Repair under-replication.** A range with fewer *healthy* voters than its RF (a node died or was
   evicted) gets a new replica on the least-loaded live node. Repairs bypass the deadband — restoring
   durability always wins.
2. **Trim over-replication.** A range with more voters than RF (after a merge union, an RF decrease,
   or a completed move) sheds the replica on the most-loaded node, preferring a non-leader victim.
   A replica stranded on a node that left the roster is shed as soon as the healthy voters alone
   satisfy RF.
3. **Balance skew.** A node carrying more replicas than the even-spread ceiling plus the deadband
   donates one range to the least-loaded node that lacks it. The donation is expressed as an *add*;
   the matching trim emerges naturally on a later pass as over-replication once the learner promotes.
   That trick keeps every individual step quorum-safe without any multi-step move bookkeeping.

Stability rules: one move per range per plan, never touch a range that is mid-transition, prefer the
current placement on ties (churn costs more than perfection), spread across distinct zones when zone
hints exist (anti-affinity across *nodes* is absolute; across zones it is best-effort), and cap the
plan by `MaxReplicaMovesPerPass` and the remaining transfer budget. The planner holds no state, so it
is trivially unit-testable and a new P0 leader loses nothing.

### The controller (`RunPlacementPass`)

The pass rides the leader balancer's timer cadence and self-gates on P0 leadership. Each pass:

1. **Drives transitions to completion** — re-issues the final drop for any `Removing` replica and
   promotes learners that have stayed caught-up for the stable window. This part runs **even when
   the rebalancer is disabled**, so an interrupted move always converges.
2. **Plans new moves** — only when `EnablePlacementRebalancer` is on, and only with transfer budget
   to spare (`MaxConcurrentReplicaTransfers` minus ranges already mid-transition).

Learner lag is measured from the range leader's follower-progress table — directly when the P0 leader
happens to lead the range, remotely otherwise. Unlike the cluster-membership promotion driver, a
learner that has *never acked* the range counts as **not caught up**: under placement the learner is
explicitly expected to serve this range, so silence means replication has not reached it yet.

---

## Routing and forwarding

The committed map is global, so any node can resolve any key to its partition id. But under placement
the local node may not *host* that partition. Two paths:

- **Consumer routes directly (the fast path).** `IRaft.GetPartitionReplicas(partitionId)` returns the
  range's committed replica set (empty = every voter). Consumers cache it — refreshed on the existing
  `OnPartitionMapChanged` event or on a `PartitionMoved` rejection — and send operations straight to
  a replica, preferably the leader.

- **Local forward fallback.** `ReplicateLogs` called on a non-replica node forwards the proposal to
  the range's replicas (voters first — the leader is always a voter), trying the next replica on a
  `NodeIsNotLeader` answer. The forwarded proposal runs through the remote node's own propose path,
  so leader checks and the generation fence apply there, not here. This fallback currently works on
  the in-memory transport; on gRPC/REST it is not yet wired, and consumers should use the direct
  path.

`GetPartitionGeneration` answers for non-hosted ranges too (from the committed map), so a non-replica
node can still build a correctly-fenced proposal before forwarding it.

---

## Interplay: initial placement, split/merge, membership

**Initial placement.** When the P0 leader bootstraps a fresh cluster with `ReplicationFactor > 0`,
each initial range gets an RF-sized, all-`Voter` replica set assigned round-robin over the nodes
visible at bootstrap — a deterministic even spread (six nodes, four ranges, RF=3 ⇒ each range on 3
distinct nodes, each node hosting exactly 2 ranges). A cluster at or below RF nodes keeps empty
replica sets — full replication — and the rebalancer spreads things out later as voters join.
Dynamically created partitions get the RF least-loaded voters at creation time.

**Split.** The child range **inherits the parent's replica set**. The data already lives on those
nodes, so the child is at RF from birth with zero transfer; the rebalancer may spread it later.

**Merge.** The survivor takes the **union** of both replica sets, so every node holding either
range's data keeps serving; the union is usually over RF, which the rebalancer trims back — again as
an ordinary over-replication case, no special machinery.

**Membership.** A committed member removal (graceful leave or dead-node eviction) makes every range
that had a replica there under-replicated; the planner re-replicates them at top priority. A newly
promoted voter is a fresh target for spreading. Placement and roster changes are serialized on the
same P0 loop, so they compose without racing.

---

## Using it: configuration

All knobs live on `RaftConfiguration`:

| Setting | Default | Meaning |
|---|---|---|
| `ReplicationFactor` | `0` | Target voter copies per range. **0 = full replication (feature off).** Prefer odd values: RF 4 needs 3-of-4 to commit — the same failure tolerance as RF 3, with an extra copy's cost. |
| `EnablePlacementRebalancer` | `false` | Master switch for *ongoing* rebalancing (repair, trim, skew). Initial placement at RF applies regardless; in-flight transitions always complete regardless. |
| `MaxReplicaMovesPerPass` | `2` | New moves initiated per controller pass. Bounds the blast radius of a bad plan. |
| `MaxConcurrentReplicaTransfers` | `1` | Ranges allowed mid-transition at once. Caps concurrent backfill/snapshot traffic so rebalancing never starves client writes. |
| `ReplicaCountDeadband` | `1` | Per-node replica-count imbalance tolerated before balancing moves are emitted. Prevents ping-ponging around an already-even spread. Repairs ignore it. |
| `Zone` | `null` | Optional locality hint for the local node; the planner prefers spreading a range's replicas across distinct zones. |

Per-range overrides go through `IRaft.SetReplicationFactorAsync(partitionId, rf)` (P0-leader-only;
`0` clears the override). Changing an override adjusts the *target* only — replicas move on later
controller passes — and deliberately does **not** bump the range's generation, because routing has
not changed and consumer fences should stay valid.

A typical scale-out configuration:

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

## Safety: what can and cannot go wrong

These are the invariants the implementation enforces; break one and you reintroduce a real
distributed-systems failure mode.

1. **Single mover per range.** Every mutation handler rejects a second transitional replica
   (`ConcurrentMembershipChange`). Two transitional replicas at once is exactly the condition under
   which two committed configurations can stop overlapping in a quorum — the split-brain Raft's
   one-at-a-time membership rule exists to prevent.

2. **Quorum only over committed voters.** Learners and removing replicas are peers, never quorum
   members; a node campaigns for a range only if the committed set says it is a `Voter` of that
   range. Both directions matter: excluding transitional replicas prevents quorum *inflation*, and
   the campaign gate prevents a non-voter from counting itself.

3. **Never below one voter.** Removing the last voter replica of a range is refused
   (`InsufficientVoters`) — it would make the range permanently uncommittable.

4. **WAL reclaim only on committed absence.** A node deletes a range's WAL only when the committed
   map no longer lists it as a replica (or the range itself is removed). Transient conditions never
   delete data a later configuration could need.

5. **Anti-affinity.** Replica endpoints within a range are unique — one copy per node, always.

6. **Idempotent re-drive.** Every lifecycle handler treats "already done" as success, and every
   controller decision is re-derived from the committed map. P0 leadership can move at any point in
   any sequence and the new leader converges without double-applying anything.

What the rebalancer *cannot* do, even misconfigured: it cannot commit through a minority (quorum math
follows the committed voter set), cannot lose data by moving replicas (adds fully promote before the
matching remove is planned), and cannot thrash forever against a balanced cluster (deadband + prefer-
current-placement tiebreak make the even spread a fixed point — there is a test asserting exactly
that).

---

## Code map — where everything lives

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
| `Kommander/RaftManager.cs` | `GetPartitionPeers` / `IsPartitionVoter`, committed-map routing, partial materialization, un-host teardown, forwarding |
| `Kommander/Scheduling/RaftPartitionHostAdapter.cs` | The two-line seam: `Nodes` / `IsVoter` become per-partition |
| `Kommander/RaftPartitionStateMachine.cs` | Campaign gates also check the local node's per-range voter role |
| `Kommander/IRaft.cs` | `GetPartitionReplicas`, `GetEffectiveReplicationFactor`, `SetReplicationFactorAsync` |
| `Kommander/Communication/ICommunication.cs` | `ForwardReplicateLogs` (in-memory transport implements it) |
| `Kommander.Tests/Scheduler/TestPlacementPlanner.cs` | Pure planner tests (even spread, repair, trim, stability, zones) |
| `Kommander.Tests/Scheduler/TestReplicaPlacement.cs` | Lifecycle, partial materialization, peer/quorum seam, initial placement |

---

## Known limitations

- **Wire forwarding.** The non-replica `ReplicateLogs` fallback is implemented for the in-memory
  transport only; gRPC/REST transports report "unsupported" and the call fails as an unknown
  partition. Consumers on those transports must route directly using `GetPartitionReplicas`.
- **Remote zone hints.** Only the local node's `Zone` is known to the planner today; other nodes'
  zones are not yet gossiped, so zone-aware spread is effective mainly in embedded/in-process
  deployments until the load report carries zones.
- **Batch proposals.** `ReplicateEntries` (the heterogeneous batch API) does not forward from
  non-replica nodes; batch producers should route directly.

---

## FAQ

**Do I need to do anything to keep my existing cluster working?**
No. `ReplicationFactor` defaults to `0`, existing maps deserialize with empty replica sets, and an
empty set means "every voter" — bit-for-bit today's behavior.

**Why is an even RF discouraged?**
RF 4 commits need 3 acks — the same single-failure tolerance as RF 3's 2-of-3, but with the storage
and replication cost of a fourth copy. Go odd.

**What happens if the cluster shrinks below RF?**
The effective RF is capped at the live voter count, so the range degrades toward full replication
(all voters host it) and re-expands as voters return. Safety is never affected — only how many
copies exist.

**Can a learner replica serve reads or votes?**
No. It receives appends to catch up, but it never votes, never campaigns, and never counts in the
quorum denominator. It starts doing all of that only at the `PromoteReplica` commit.

**Who deletes the data on a node that lost a replica?**
The node itself, when it applies the committed map that no longer lists it: it drains the partition
executor, stops it, and reclaims that range's WAL. Nothing is deleted on rumor — only on the
committed map.

**What if P0 leadership changes in the middle of a move?**
Nothing is lost. All durable state is in the committed map; the new P0 leader's first controller pass
finds the `Learner` or `Removing` replica there and resumes exactly where the old leader stopped.
The handlers are idempotent, so even a duplicated step is harmless.

**How does this relate to the leader balancer?**
They are complementary and share a design. Placement decides *which nodes host a range*; the leader
balancer decides *which of those replicas leads*. Leadership can only ever land on a replica, because
only replicas participate in the range's elections.
