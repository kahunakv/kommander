# Leader Balancer — Developer Guide

This guide is for developers who **use or maintain** Kommander's leader balancer. For
the basic concepts, see the [Architecture Overview](architecture-overview.md).

---

## Table of contents

1. [Summary](#summary)
2. [Leader imbalance](#leader-imbalance)
3. [Core concepts](#core-concepts)
4. [Rebalance steps](#rebalance-steps)
5. [The two-tier balancing policy](#the-two-tier-balancing-policy)
6. [Transfer suggestions](#transfer-suggestions)
7. [Tracking moves without waiting](#tracking-moves-without-waiting)
8. [Configuration](#configuration)
9. [Observability](#observability)
10. [Safety](#safety)
11. [Code map](#code-map)
12. [Maintaining & extending](#maintaining--extending)
13. [FAQ](#faq)

---

## Summary

Each partition elects its leader independently. One node can end up with most of the leaders and become a
bottleneck while its peers are idle. The **leader balancer** detects this and asks busy nodes to hand some
leaderships to idle ones through the normal leadership handoff. It is **off by default** and advisory. It
moves leadership only, and the worst result is a pointless move.

```
Before:                          After balancing:

 node A: ████████████ (12)        node A: ████ (4)
 node B: ██ (2)                    node B: ████ (4)
 node C: ██ (2)                    node C: ████ (4)
        ^ overloaded                      ^ evenly spread
```

---

## Leader imbalance

1. **Count skew.** One node has a disproportionate *number* of leaderships. With 64 partitions and 3 nodes,
   an even spread is about 21 each, but elections can produce 40 / 12 / 12.
2. **Load skew.** The *counts* are equal, but the **hot** partitions (high write rate, deep queues) are on
   one node.

The balancer equalizes counts first and then spreads load.

---

## Core concepts

### Partition leader
The node that is currently responsible for a partition. Only the leader can hand off its leadership.

### The system partition (P0)
Partition `0`, in which all nodes participate. **Only the P0 leader executes the leader balancer.** This
guide calls that node "the controller."

### Load report
A periodic gossip message in which a node describes the partitions *it currently leads* and how busy each
one is. It is **advisory** and is not written to the Raft log.

### Load score

```
load = (ops weight) × (recent operations/sec)  +  (queue weight) × (pending work in queues)
```

The operations/sec value is an **EWMA** (exponentially weighted moving average). It follows sustained
traffic, ignores brief spikes, and decays toward zero when a partition is idle. Only *workload* operations
(client writes and replication) count. Heartbeats are excluded.

### Global view
The snapshot that the controller builds from the load reports. It shows who leads what, how many leaders
each node has, and how loaded each node is. A report past its time-to-live is dropped.

### Transfer suggestion
A request from the controller to the current leader of a partition to hand that partition to another node.
The current leader validates the request and decides.

---

## Rebalance steps

The controller does one **balancing pass** each `LeaderBalancerInterval` (default 30s).

1. **Gather.** Collect the latest unexpired load report of each node into the global view.
2. **Reconcile in-flight moves.** If the target of an earlier suggestion became the leader, mark the move
   done and put the partition on cooldown. If `SuggestionTimeout` elapsed first, clear the move.
3. **Check completeness.** If the view lacks a fresh report from any live node, **skip this pass**.
4. **Plan.** Apply the two-tier policy to produce a short list of moves.
5. **Dispatch.** Send each move as a suggestion to the partition's current leader and record it as
   "outstanding."

The controller does not block on a transfer. A later pass plans a failed or lost suggestion again.

The recipient uses the same converging handover as `IRaft.TransferLeadershipAsync`. If the target is behind,
the leader parks new proposals and continues to replicate. It hands over when the target is level with its
last index, within one election timeout. Under a steady write load, a healthy follower is behind at almost
all times. Parked proposals are answered `NodeIsNotLeader` only after the leader change is published, or are
re-admitted if the wait expires.

Each attempted move logs one `TransferSuggestion p{Partition} ... finished with {Status}` line at
Information (partition, term, from, to, status, elapsed).

---

## The two-tier balancing policy

The planner is a **pure function**. It takes the global view and the configuration, returns a list of moves,
and has no side effects.

### Tier 1 — count balance (primary)
The ideal count is `total leaders ÷ number of live nodes`. A node above it by more than `CountDeadband` is
**over-loaded**, and a node below it is **under-loaded**. The planner moves leaderships from the most
over-loaded node to the most under-loaded node. When it has a choice, it moves the **hottest** partition to
the **coolest** node.

### Tier 0 — degraded-node drain (optional, runs first)
This tier is off by default. Set `EnableSlowNodeAvoidance` to turn it on.

Each node, leader or follower, measures how long its WAL writes take to become durable and includes the
figure in its load report. When the write latency of one node is far above its peers', the balancer sends it
no new leadership and moves away the leaderships that it has. If this tier emits a move, the other tiers
emit none in that pass. A drain unbalances the counts on purpose, and Tier 1 would undo it.

These rules prevent a drain in a healthy cluster.

1. **A node is compared with the cluster median.** Under heavy write load, all disks slow down together and
   the ratio does not change.
2. **A quiet node is "unknown".** The latency estimate is updated only by writes. A node with
   too few samples or samples that are too old is not judged. This includes a freshly restarted node.
3. **Classification takes several passes.** A node must look slow for `SlowNodeEnterPasses` consecutive
   passes to be classified, and clean for `SlowNodeExitPasses` consecutive passes to be released.
4. **Half or more of the voters is a cluster-wide condition.** If that many look slow, none are classified.
5. **At least three nodes need usable measurements.** With two, the median is their mean, and a slow node
   raises the number that it is compared with.

> **This is a leadership decision only.** The replicas and the vote of a slow node do not change, and it
> is not evicted.

### Tier 2 — load balance (secondary)
If the counts are even and the load skew is above `LoadImbalanceThreshold`, the planner emits a
**count-neutral swap**. It moves a hot partition off the hot node and a cold partition back onto it. It
emits a swap only if the swap *reduces* the imbalance.

### Guard rails on every move

A move must satisfy all of these conditions.

- The partition is **Active**. A partition that is splitting or draining does not move.
- Its leader has been stable for at least `MinLeaderStabilityMs`.
- The target is a live, voting member of that partition's group.
- The target is not classified as degraded (when `EnableSlowNodeAvoidance` is on).
- The partition is not in **cooldown** from a recent move.
- The partition has no **outstanding** suggestion.

At most `MaxMovesPerPass` moves are planned per pass, and at most `MaxConcurrentTransfers` moves can be in
flight cluster-wide.

---

## Transfer suggestions

**Only the current leader of a partition can hand off that partition's leadership.** The controller usually
does not lead the partition. It sends a suggestion to the node that does.

```
   P0 controller                     Node that leads partition P
   (the decider)                     (the only one that can act)
        │                                       │
        │  "suggest: move P to node C"          │
        ├──────────────────────────────────────►
        │                                       │  validate:
        │                                       │   • do I still lead P?
        │                                       │   • is P Active?
        │                                       │   • is C a live voter?
        │                                       │
        │                                       │  if all yes → hand off P to C
        │                                       │  if any no  → silently ignore
```

The recipient is **authoritative**. If the controller's view was stale, the suggestion is dropped. The
handoff itself re-validates terms and leadership, and catches up a lagging target first. A wrong suggestion
can cause only a wasted transfer, which is logged with its outcome (`TargetNotCaughtUp`,
`NodeIsNotLeader`, ...).

> **Self-delivery:** when the controller leads the partition that must move, the suggestion is delivered
> **in-process**. The transport drops a self-addressed message, as a node is not a network peer of
> itself. If you remove this path, the balancer silently ignores the partitions that the P0 leader leads.

---

## Tracking moves without waiting

The controller has an in-memory table of **outstanding moves** (partition → intended target + deadline). On
each pass it compares that table with the global view.

- **Confirmed:** the view shows that the target leads the partition. The partition starts a `MoveCooldown`.
- **Timed out:** `SuggestionTimeout` elapsed and ownership did not change. The move is cleared, and a later
  pass can retry it after the cooldown.

There is **no durable balancer state**. When P0 leadership moves, the new controller starts fresh, and
the cost is at worst one extra pass.

---

## Configuration

Set `EnableLeaderBalancer = true` in `RaftConfiguration` on all nodes.

| Setting | Default | What it controls |
|---|---|---|
| `EnableLeaderBalancer` | `false` | Master switch. When off, there are no balancing passes and no suggestions. Load reports can flow for replica placement or `EnableLoadReports`. |
| `LeaderBalancerInterval` | `30s` | How often the controller does a balancing pass. |
| `LeaderBalancerReportInterval` | `5s` | Obsolete, no effect. A node emits its load report once per `GossipInterval`. |
| `LeaderBalancerReportTtl` | `20s` | A report older than this is ignored (the node is treated as silent). |
| `CountDeadband` | `1` | How far above/below the average a node can drift before a move is planned. |
| `LoadImbalanceThreshold` | `0.25` | How skewed load must be (when counts are even) to trigger a swap. |
| `MinLeaderStabilityMs` | `5000` | A leadership younger than this is not eligible to move. |
| `MoveCooldown` | `60s` | After a partition moves, how long before it can move again. |
| `MaxMovesPerPass` | `4` | Most moves planned in a single pass. |
| `MaxConcurrentTransfers` | `2` | Most moves in flight cluster-wide at once. |
| `SuggestionTimeout` | `15s` | How long to wait for a suggested move to show up before the controller clears it. |
| `LeaderBalancerOpsWeight` | `1.0` | Weight of throughput in the load score. |
| `LeaderBalancerQueueWeight` | `0.5` | Weight of queue depth in the load score. |
| `EnableSlowNodeAvoidance` | `false` | Master switch for Tier 0. |
| `SlowNodeMultiplier` | `3.0` | How many times the cluster median write latency makes a node a candidate. |
| `SlowNodeFloorMs` | `10.0` | Below this absolute latency, a node is not a candidate at any ratio. |
| `SlowNodeMinSamples` | `20` | Write batches a node must have observed before it is judged. |
| `SlowNodeObservationTtl` | `30s` | Older measurements count as unknown. |
| `SlowNodeEnterPasses` | `3` | Consecutive bad passes before a node is classified. |
| `SlowNodeExitPasses` | `6` | Consecutive clean passes before a node is released. |

**Tuning tips**

- For faster rebalancing, lower `LeaderBalancerInterval` or raise `MaxMovesPerPass` /
  `MaxConcurrentTransfers`. Expect more churn.
- `SuggestionTimeout` should be larger than `GossipInterval` **plus** the time that gossip takes to spread.
  If it is too small, successful moves are counted as failures and retried. Raise it on larger clusters.
- `SlowNodeEnterPasses` and `SlowNodeExitPasses` count **controller passes**. A shorter
  `LeaderBalancerInterval` shortens detection and recovery. At the defaults, a node is classified after
  90s and released after 180s.
- If the balancer oscillates, raise `CountDeadband` and `LoadImbalanceThreshold`. Lower them for tighter
  balance and more moves.

---

## Observability

The metrics are under the `Kommander` meter.

| Metric | Type | Meaning |
|---|---|---|
| `raft.balancer.moves_total` | counter | Moves tagged by outcome: `planned`, `drain`, `succeeded`, `timed_out`. |
| `raft.balancer.skipped_passes_total` | counter | Passes skipped because the view was incomplete. |
| `raft.balancer.count_imbalance` | gauge | How far the most-loaded node is above the ideal count. Trends to ~0 as it converges. |
| `raft.balancer.load_imbalance` | gauge | Fractional load skew across nodes. Also trends down. |
| `raft.balancer.slow_nodes` | gauge | Nodes currently classified degraded by Tier 0. Normally `0`. |

- A healthy cluster shows a burst of `planned` and then `succeeded`, and both imbalance gauges fall.
- A high `timed_out` rate means that recipients drop suggestions, or that `SuggestionTimeout` is too short
  for your gossip latency.
- A `drain` outcome means that Tier 0 evacuated a node. Check the disk of that node.
- If `raft.balancer.slow_nodes` alternates between `0` and `1`, raise `SlowNodeMultiplier` or
  `SlowNodeExitPasses`.
- A high `skipped_passes_total` means that the view is often incomplete. A node is silent, or the report TTL
  does not match the report interval.

> The two imbalance gauges are process-global and meaningful only on the P0 leader (other nodes report
> `0`). Do not rely on them in test harnesses that host several nodes in one process.

---

## Safety

**The balancer is advisory and cannot violate correctness.**

- Each leadership change goes through the normal transfer path, which re-validates term and leadership at the
  moment of the move.
- A stale or dropped report can cause only a **wasted or skipped** move. The partition's leader makes the
  final decision.
- The P0 leader is the single decider, and two nodes do not issue conflicting plans.

The failure modes are a missed move and a pointless move. Both self-correct on a later pass.

---

## Code map

| Concern | Where |
|---|---|
| Per-partition load measurement (the EWMA accumulator) | `Kommander/Diagnostics/PartitionLoadAccumulator.cs` |
| A node's load report | `LoadReportService.BuildLocalLoadReport()` |
| The report types | `Kommander/System/NodeLoadReport.cs`, `PartitionLoad.cs` |
| The reduced snapshot of the reports | `Kommander/System/GlobalLeadershipView.cs` |
| The planner (pure function) | `Kommander/System/LeaderBalancePlanner.cs`, `LeaderMove.cs` |
| The Tier 0 classification | `Kommander/System/SlowNodeDetector.cs` |
| The controller / balancing pass | `Kommander/System/LeaderBalancer.cs` (`RunPassAsync`), called from `RaftSystemCoordinator.RunBalancerPassAsync` |
| The suggestion message + send/receive | `Kommander/Data/TransferLeadershipSuggestionRequest.cs`, `RaftRpcRouter.SendTransferLeadershipSuggestion` / `ReceiveTransferLeadershipSuggestion` |
| The handoff primitive | `RaftManager.TransferLeadershipAsync` |
| Configuration options | `Kommander/RaftConfiguration.cs` |
| Metrics | `Kommander/Diagnostics/KommanderMetrics.cs` |

To learn the code, read `PartitionLoadAccumulator` → `NodeLoadReport` → `GlobalLeadershipView` →
`LeaderBalancePlanner` → `LeaderBalancer.RunPassAsync`.

---

## Maintaining & extending

- **The planner must be pure.** The mutable state (cooldowns, outstanding moves) belongs to the controller,
  which passes it *in*. The policy is then testable without a live cluster.
- **Read ownership and load only from the global view.** Planning and move confirmation must both use it. A
  second owner map drifts from the reduction (for example, it forgets the TTL) and confirms moves
  incorrectly.
- **Preserve in-process self-delivery** of suggestions (see the callout above). It is the common case.
- **Add a new block reason on both sides.** A new partition lifecycle state, or a new reason to block a
  move, goes in the planner's filters *and* in the recipient's validation.
- **The feature must be advisory.** Do not make this state durable, and do not let the controller drive
  transfers directly.

> **Related:** the same load report also carries a per-partition **log-replication rate** and **WAL queue
> depth** for load-based split decisions. See the
> [Per-Partition Log-Replication Rate Signal guide](partition-log-rate-signal-developer-guide.md).

---

## FAQ

**What happens during a node failure or election?**
The balancer ignores a leaderless partition in that pass, as no report contains it. The
balancer does not start or speed up elections.

**It's enabled but nothing is moving — why?**
Check `raft.balancer.skipped_passes_total` (incomplete view) and
`raft.balancer.moves_total{outcome=timed_out}` (suggestions that do not land). Also check that the imbalance
is beyond `CountDeadband`.
