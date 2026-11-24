# Per-Partition Log-Replication Rate Signal — Developer Guide

Kommander measures how hard each partition's log pipeline works. You can read the measurement from any
node and use it to decide when to split a hot partition. The signal is advisory and has no effect on Raft
correctness.

It travels in the same gossiped load report as the [Leader Balancer](leader-balancer-developer-guide.md).
For the rest of the system, see the [Architecture Overview](architecture-overview.md).

---

## Table of contents

1. [Summary](#summary)
2. [Limits of operations per second](#limits-of-operations-per-second)
3. [Core concepts](#core-concepts)
4. [Combining the three signals](#combining-the-three-signals)
5. [API](#api)
6. [Required configuration](#required-configuration)
7. [The consumer contract](#the-consumer-contract)
8. [Observability](#observability)
9. [Safety](#safety)
10. [Code map](#code-map)

---

## Summary

Each partition has one commit pipeline: the leader appends changes to a single serialized log and
replicates it. When that pipeline is the bottleneck, a split can spread the load across more log streams.
Three numbers per partition describe the pipeline.

- **`LogOpsPerSecond`** — the rate of log-replication work.
- **`WalQueueDepth`** — the backlog of the write pipeline.
- **`CommitWaitMs`** — the time from enqueue to durable for a write.

A split decision needs the rate and one saturation signal: `WalQueueDepth`, or `CommitWaitMs` when depth
is too coarse for your trigger.

---

## Limits of operations per second

A threshold on `LogOpsPerSecond` alone is a poor split trigger. For most workloads the bottleneck is the
fsync, and an fsync-bound partition commits at a fixed ceiling. A partition that is offered 1.1× its
capacity and one that is offered 10× show the same rate. The backlog tells them apart.

```
 Offered load ───►   1.1× capacity        10× capacity
 LogOpsPerSecond     ████████ (ceiling)   ████████ (ceiling)   ← identical! rate is saturated
 WalQueueDepth       ▏ (≈0, keeping up)    ████████████ (deep)  ← THIS is the difference
```

---

## Core concepts

### Log-replication operation
`LogOpsPerSecond` counts `ReplicateLogs` operations on the leader. Each one is an append that is replicated
and committed.

### Log-routed operations only
An operation counts if it goes through the log. A read that the leader serves from memory appends no entry
and does not count, and a split would not relieve it. Linearizable reads that your application routes
through the log do count. They use the same commit capacity.

Only the leader's append path is counted. The local rate on a follower is `0` under any load, so ask the
leader or read the gossiped value.

### The rate is a smoothed average (EWMA)
`LogOpsPerSecond` is an exponentially weighted moving average with a half-life of about 7 seconds. It decays
toward zero when the partition goes idle.

### WAL queue depth
`WalQueueDepth` is the number of write operations queued for the partition in the WAL scheduler. The
scheduler batches writes across partitions into shared fsync groups, and depth shows the partition's share
of the contention for them. A depth that rises while the rate is flat is the signature of an fsync-bound
partition.

### Commit-wait latency
`CommitWaitMs` is the average time from enqueue to durable, smoothed across recent batches. It resolves
fsync pressure more finely than depth does.

> **Sticky when idle.** The wait estimate does not decay. After a partition goes quiet, the last value
> remains until the next write batch. A high `CommitWaitMs` can be stale, and you should gate it on
> `LogOpsPerSecond`, which decays to ≈0.

### The load report it rides on
The numbers are attached to the partition entries in the per-node load report. A node includes only the
partitions it currently leads. The report is gossiped and is not written to the Raft log.

### The `0` sentinel
An accessor returns `0` when the partition id is unknown, or when this node is not the leader and no report
from the current leader has arrived. An idle partition also reads as `0`. For a split trigger both cases
mean "not hot". To distinguish them, check whether a report from the leader exists.

### Group-commit caveat
When several partitions have writes ready, the WAL scheduler flushes them in one fsync. For RocksDB this is
a single shared write-ahead log across column families, with batches of up to a few hundred operations.

> A split into two partitions **on the same node** adds no fsync capacity, only consensus overhead. A split
> relieves an fsync bottleneck if the new partition's leader lands on a **different node**, with a different
> disk.

---

## Combining the three signals

| Signal | What it measures | Saturates? | Idle behavior | Use it for |
|---|---|---|---|---|
| `LogOpsPerSecond` | Rate of log-routed ops on the leader | **Yes** — flat at the commit ceiling | Decays to ≈0 | Magnitude: how much load a split would redistribute |
| `WalQueueDepth` | Pending writes in the WAL pipeline | No — rises under overload | Drains to 0 | Saturation. Sufficient for most triggers. |
| `CommitWaitMs` | Enqueue→durable latency per write (ms) | No — climbs under overload | **Sticky** (last value remains) | Saturation, finer-grained. Optional companion to depth. |

| Rate | Depth | Reading |
|---|---|---|
| High | ≈0 | Busy but healthy. Don't split. |
| High | Deep and sustained | fsync-bound and overloaded. Candidate for a cross-node split. |
| Low | Any | Cold. Don't split. |

---

## API

The three accessors are on `IRaft`. You can call them on any node for any partition.

```csharp
// How much log-replication work is partition 14 doing right now? (ops/sec, smoothed)
double rate = raft.GetPartitionLogOpsPerSecond(14);

// How backed up is partition 14's write pipeline? (pending WAL operations)
int depth = raft.GetPartitionWalQueueDepth(14);

// How long is a write waiting to become durable? (enqueue→durable ms, smoothed)
double waitMs = raft.GetPartitionCommitWaitMs(14);
```

Each accessor resolves the value in this order.

1. **Local.** If this node leads the partition, the value is the live in-process measurement.
2. **Remote.** Otherwise the value comes from the gossiped load report of the node that currently leads the
   partition. If that leader is unknown, the freshest report by logical-clock time wins. During a leadership
   handoff, the partition can be in the reports of both the old and the new leader.
3. **Sentinel.** If neither applies, the result is [`0`](#the-0-sentinel).

---

## Required configuration

The local path needs no configuration. The remote path needs load-report gossip, which is off by default.
When it is off, all remote queries return `0`. This is the most common cause of a remote value that is
always zero.

> Load-report gossip is on (`LoadReportsEnabled`) when at least one of these is set:
> `EnableLeaderBalancer`, `EnablePlacementRebalancer`, `ReplicationFactor` above 0, or `EnableLoadReports`.
> It must be on for the leading nodes and for the node that reads the value.

| Setting | Default | Effect on this signal |
|---|---|---|
| `EnableLeaderBalancer` | `false` | Turns on load-report gossip, as do the other settings above. |
| `GossipInterval` | `5s` | How often a node builds its load report and gossips it to `GossipFanout` random peers. Lower = fresher, more gossip traffic. Reports are not forwarded. |
| `LeaderBalancerReportTtl` | `20s` | A balancer pass on the system-partition leader drops reports older than this. Their values then read as `0` on that node. |

A remote value is one `GossipInterval` old or more, plus the ≈7s smoothing half-life of the rate. In
practice a remote reader reacts in about 10 seconds.

---

## The consumer contract

Kommander measures the signal. A consumer, such as a split trigger in your application, must follow these
rules.

1. **Combine rate and saturation.** Trigger on high `LogOpsPerSecond` AND a sustained saturation signal.
   Prefer `WalQueueDepth`, which clears by itself. If you use `CommitWaitMs`, the rate condition protects you
   from a stale wait.
2. **Debounce.** Require the condition to hold across a window at least as long as the remote lag. Do not act
   on a single sample.
3. **Place the relief on a different node.** See the [group-commit caveat](#group-commit-caveat).
4. **Treat `0` as "not hot".** Idle and not-yet-seen both mean "no action".

---

## Observability

`LogOpsPerSecond` and `CommitWaitMs` have no dedicated metric. Read them with `GetPartitionLogOpsPerSecond`
and `GetPartitionCommitWaitMs`. The WAL backlog is published under the `Kommander` meter.

| Metric | Type | Meaning |
|---|---|---|
| `raft.wal.queue_depth` | observable gauge (per partition) | Pending-or-in-flight WAL operations per partition — the same backlog as `WalQueueDepth`. |

A partition whose gauge climbs and does not come down is fsync-bound. Compare it with `LogOpsPerSecond` to
confirm that the rate has reached its plateau.

---

## Safety

The signal is a read-only measurement and takes no part in consensus. The values are not durable. A stale or
missing value can only cause a consumer to make a suboptimal split decision, and missing data reads as
"not hot".

---

## Code map

| Concern | Where |
|---|---|
| Per-partition log-rate measurement (the EWMA accumulator) | `Kommander/Diagnostics/PartitionLoadAccumulator.cs` |
| Recording log-routed ops onto the rate | `Kommander/Scheduling/RaftPartitionExecutor.cs` |
| WAL backlog & commit-wait per partition | `Kommander/WAL/IO/FairWalScheduler.cs` (`GetPartitionDepth` / `GetPartitionCommitWaitMs`) |
| Commit-wait latency EWMA | `Kommander/Diagnostics/PartitionWaitAccumulator.cs` |
| The values carried on each partition entry | `Kommander/System/PartitionLoad.cs` (`LogOpsPerSecond`, `WalQueueDepth`, `CommitWaitMs`) |
| Building a node's own report | `LoadReportService.BuildLocalLoadReport()` |
| The accessors | `LoadReportService.GetPartitionLogOpsPerSecond` / `GetPartitionWalQueueDepth` / `GetPartitionCommitWaitMs` |
| Gossip of the report | `RaftManager` gossip path (gated on `LoadReportsEnabled`) |
| Configuration knobs | `Kommander/RaftConfiguration.cs` |
| WAL-depth metric | `Kommander/Diagnostics/KommanderMetrics.cs` (`raft.wal.queue_depth`) |

A good reading order is `PartitionLoadAccumulator` → `RaftPartitionExecutor` → `PartitionLoad` →
`BuildLocalLoadReport` → `GetPartitionLogOpsPerSecond`.
