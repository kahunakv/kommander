# Per-Partition Log-Replication Rate Signal — Developer Guide

Welcome! This guide is for the developer or operator who wants to **use or understand** Kommander's
per-partition *log-replication rate* signal — the measurement that answers *"is this partition hot
with work that splitting it could relieve?"* It assumes you know roughly what Kommander is (a
Raft-based distributed system with many partitions, each with its own leader) but **no prior knowledge
of this signal**.

This signal is a close sibling of the [Leader Balancer](leader-balancer-developer-guide.md): it rides
the same per-node load report and the same gossip path. If you haven't met those concepts yet, skim
that guide's "Core concepts" section first; this one builds on the same load-report machinery.

If you are brand new to Kommander, start with the [Architecture Overview](architecture-overview.md).

---

## Table of contents

1. [The 60-second mental model](#the-60-second-mental-model)
2. [Why "operations per second" alone misleads](#why-operations-per-second-alone-misleads)
3. [Core concepts](#core-concepts)
4. [The three signals, and how to read them together](#the-three-signals-and-how-to-read-them-together)
5. [Reading it: the API](#reading-it-the-api)
6. [Using it: what you must enable](#using-it-what-you-must-enable)
7. [The consumer contract](#the-consumer-contract)
8. [Observability](#observability)
9. [Safety: what this can and cannot do](#safety-what-this-can-and-cannot-do)
10. [Code map — where everything lives](#code-map--where-everything-lives)
11. [FAQ](#faq)

---

## The 60-second mental model

Every partition runs its own Raft group, and the **leader** appends every change to that partition's
log, then replicates it. That log is a single serialized stream: one partition, one commit pipeline.
When a partition gets hot, that one pipeline is the bottleneck — and the classic way to relieve it is
to **split** the partition so the load spreads across *more* independent log streams.

To decide whether a split is worth it, you need to measure how hard a partition's log pipeline is
working. This feature exposes three numbers per partition, readable from any node:

- **`LogOpsPerSecond`** — *how much* log-replication work the partition is doing (the rate).
- **`WalQueueDepth`** — *how backed-up* the write pipeline is (the saturation).
- **`CommitWaitMs`** — *how long* a write waits from enqueue to durable (the latency). The most direct
  fsync-pressure signal, but a finer-grained companion to depth — read it only if depth proves too
  coarse for your trigger.

Rate tells you the volume; depth and wait tell you whether the partition is keeping up or drowning. You
need **rate plus a saturation signal** to make a good decision — and this guide explains why.

The signal is **advisory**. It never affects Raft correctness; it only informs a higher-level
component (typically the embedding application's split policy) about where the heat is.

---

## Why "operations per second" alone misleads

It's tempting to trigger a split when `LogOpsPerSecond` crosses some threshold. That's a trap, because
**throughput plateaus at the bottleneck.**

For most workloads the bottleneck is the **fsync** — flushing the write to durable storage. Once a
partition is fsync-bound, it commits at a fixed ceiling. A partition offered 1.1× its capacity and one
offered 10× its capacity report the *same* `LogOpsPerSecond` — both are pinned at the ceiling. Yet the
10× partition is the one screaming for a split, and rate alone can't tell them apart.

```
 Offered load ───►   1.1× capacity        10× capacity
 LogOpsPerSecond     ████████ (ceiling)   ████████ (ceiling)   ← identical! rate is saturated
 WalQueueDepth       ▏ (≈0, keeping up)    ████████████ (deep)  ← THIS is the difference
```

What *does* diverge as a partition goes from "busy" to "overloaded" is the **backlog**: requests pile
up in the write queue and wait. That backlog is `WalQueueDepth`. So the rule is:

> **Rate is magnitude; queue depth is saturation. Trigger on rate AND depth, never rate alone.**

A high rate with a near-zero queue means a partition that is busy but comfortably keeping up — leave it
be. A high rate with a *sustained, deep* queue means a partition that can't keep up — that's the one a
split relieves.

---

## Core concepts

### Log-replication operation
An operation that flows through the leader's Raft log — an append that gets replicated and committed.
In Kommander these are `ReplicateLogs` operations. This is what `LogOpsPerSecond` counts.

### Path, not label
The signal is defined by the **path an operation takes**, not by a read/write label. Only operations
that go *through the log* count. A read served directly from the leader's in-memory state — one that
appends no log entry — does **not** count, because splitting the partition wouldn't relieve it (it
never touched the bottlenecked log pipeline in the first place). For most applications "log-routed"
means writes; for an application that routes linearizable reads *through the log*, those reads count
too — correctly, because they consume the same serialized-commit capacity a split would relieve.

A useful consequence: the signal is **leader-only by nature**. Only a leader issues `ReplicateLogs`; a
follower applying replicated entries does so through a different path that isn't counted. So a
follower's local rate for a partition it doesn't lead is naturally `0` — that's expected, not a bug.

### The rate is a smoothed average (EWMA)
`LogOpsPerSecond` is an **exponentially weighted moving average** — it reacts to sustained traffic,
ignores momentary blips, and decays toward zero on its own when a partition goes idle (half-life
≈ 7 seconds). You're seeing recent, smoothed behavior, not an instantaneous tick or a lifetime total.

### WAL queue depth
The number of write operations queued for a partition in the WAL scheduler — its backlog. Because
Kommander batches writes across partitions into shared fsync groups (see the caveat below), a
partition's depth reflects its share of the contention for the node's write pipeline. A rising depth
while the rate is flat is the textbook signature of an fsync-bound partition.

### Commit-wait latency
The average time a write spends from the moment it is enqueued to the moment it is durable
(`CommitWaitMs`), smoothed across recent batches. This is the most *direct* measure of fsync pressure:
when the write pipeline is saturated, waits climb. It overlaps with queue depth (both rise under
overload) but resolves finer — use it only if depth alone is too coarse for your trigger.

> **Sticky when idle.** Unlike rate and depth, the wait estimate does **not** fall back to zero on its
> own when a partition goes quiet — it holds its last value until the next write batch updates it. So a
> high `CommitWaitMs` on a partition that has since gone idle is stale, not current. Never act on it
> alone; always gate on `LogOpsPerSecond` (which *does* decay to ≈0 when idle) so a stale wait can't
> trigger anything.

### The load report it rides on
These numbers are attached to each partition entry in the **per-node load report** — the same small,
periodic, gossiped message the leader balancer uses. A node reports only the partitions *it currently
leads*. The report is advisory and never written to the Raft log.

### The `0` sentinel
When you ask for a partition's value and the answer is unknown — you're not the leader and no report
from the current leader has arrived yet, or the partition id is unknown — the accessor returns `0`.
This means `0` is **ambiguous**: it could be "genuinely idle" or "not seen yet." For a split trigger
that's fine, because both mean "not hot." If you need to distinguish them, check whether a report from
the leader exists at all before trusting a `0`.

### The group-commit caveat (read this before you act)
Kommander's WAL scheduler performs **cross-partition group commit**: when several partitions have
writes ready, it flushes them together in **one fsync** (for RocksDB, a single shared write-ahead log
across column families, batching up to a few hundred operations at a time). This has a critical
consequence for splitting:

> Splitting a hot partition into two partitions **on the same node** adds *no* fsync capacity — they
> still share the one fsync. It only adds consensus overhead. A split relieves an fsync bottleneck
> **only when the new partition's leader lands on a different node** (a different disk / fsync path).

So this signal answers *"is partition P hot?"* — it does **not** decide *where the relief should go.*
Whoever consumes it must place the new leader on a different node, or the split is pointless.

---

## The three signals, and how to read them together

| Signal | What it measures | Saturates? | Idle behavior | Use it for |
|---|---|---|---|---|
| `LogOpsPerSecond` | Rate of log-routed ops on the leader | **Yes** — flat at the commit ceiling | Decays to ≈0 | Magnitude: how much load exists / how much a split would redistribute |
| `WalQueueDepth` | Pending writes backed up in the WAL pipeline | No — keeps rising under overload | Drains to 0 | Saturation: is the partition keeping up, or drowning? |
| `CommitWaitMs` | Enqueue→durable latency per write (ms) | No — climbs under overload | **Sticky** (holds last value) | Saturation, finer-grained: how hard is fsync pressure? Optional companion to depth. |

Depth and commit-wait are two views of the same saturation. Depth is the simpler, self-clearing one
and is sufficient for most triggers; commit-wait is the more direct fsync measure when you need it, at
the cost of the idle-stickiness caveat above. Pick depth first; reach for commit-wait only if depth is
too coarse.

The intended reading (using depth as the saturation signal):

- **High rate, ≈0 depth** → busy but healthy. **Don't split.**
- **High rate, sustained deep queue** → fsync-bound and overloaded. **Candidate for a cross-node split.**
- **Low rate** → cold, regardless of depth or wait. **Don't split.**

---

## Reading it: the API

All three accessors live on `IRaft` and can be called on **any** node for **any** partition:

```csharp
// How much log-replication work is partition 14 doing right now? (ops/sec, smoothed)
double rate = raft.GetPartitionLogOpsPerSecond(14);

// How backed up is partition 14's write pipeline? (pending WAL operations)
int depth = raft.GetPartitionWalQueueDepth(14);

// How long is a write waiting to become durable? (enqueue→durable ms, smoothed)
double waitMs = raft.GetPartitionCommitWaitMs(14);
```

Each accessor resolves the value the same way:

1. **Local fast-path.** If *this* node currently leads the partition, the value is read directly from
   the in-process measurement — no gossip lag, always available.
2. **Remote path.** Otherwise the value comes from the most recent gossiped load report from the node
   that currently leads the partition. This trails reality by up to one report interval plus gossip
   propagation (see [Using it](#using-it-what-you-must-enable)).
3. **Sentinel.** If neither applies, you get `0` (see [the `0` sentinel](#the-0-sentinel)).

Across a leadership handoff a partition can briefly appear in two nodes' reports (the old leader's and
the new one's). The accessor resolves this by preferring the report from the node the cluster
*currently* considers the leader, falling back to the freshest report by logical-clock time — so you
get the current leader's value, not a stale predecessor's.

---

## Using it: what you must enable

The **local fast-path always works** — a node can always read the live values for partitions it leads.

The **remote path depends on the load report being gossiped**, and that only happens when the leader
balancer's reporting is turned on:

> To read a partition's signal from a node that does **not** lead it, the leading nodes must have
> **`EnableLeaderBalancer = true`** in their `RaftConfiguration`. With it off, no load reports are
> built or gossiped, so every remote query returns the `0` sentinel forever.

This is the single most common "why is it always zero?" cause. If you want cluster-wide visibility of
the signal, enable the balancer's reporting on your nodes.

Once enabled, the freshness of remote values is governed by the same knobs the balancer uses:

| Setting | Default | Effect on this signal |
|---|---|---|
| `EnableLeaderBalancer` | `false` | **Required** for the remote path. Off → remote queries return `0`. |
| `LeaderBalancerReportInterval` | `5s` | How often a leader refreshes the gossiped value. Lower = fresher, more gossip traffic. |
| `LeaderBalancerReportTtl` | `20s` | A report older than this is dropped; a silent node's values disappear (return `0`). |

Net staleness of a remote value ≈ one `LeaderBalancerReportInterval` + gossip propagation, layered on
top of the rate's ≈7s smoothing half-life. In practice a remote reader reacts on the order of ~10
seconds, not instantly. Plan your consumer's reaction window accordingly.

---

## The consumer contract

If you build something on top of this signal (for example, a split trigger in your application), follow
these rules. Kommander measures and exposes — it does **not** enforce these for you.

1. **Combine rate and saturation.** Trigger on **high `LogOpsPerSecond` AND a sustained saturation
   signal** (`WalQueueDepth`, optionally `CommitWaitMs`) — never on rate alone. Rate plateaus at the
   fsync ceiling and cannot, by itself, tell an overloaded partition from a merely busy one. Prefer
   `WalQueueDepth` (self-clearing); if you use `CommitWaitMs`, remember it is sticky when idle, so the
   `LogOpsPerSecond` half of this AND-condition is what protects you from acting on a stale wait.
2. **Debounce.** A remote value lags by up to a report interval plus gossip propagation. Require the
   condition to hold across a window **≥ that lag** before acting; never act on a single sample. A
   transient spike that clears within one report interval is not a reason to split.
3. **Place the relief on a different node.** Because fsync is shared cross-partition (group commit),
   splitting in place relieves nothing. Whatever acts on this signal must put the new leader on a
   different node than the hot one.
4. **Treat `0` as "not hot."** Idle and not-yet-seen both surface as `0`, and both correctly mean
   "no action."

---

## Observability

There is **no dedicated metric** for `LogOpsPerSecond` or `CommitWaitMs` — they are exposed through the
`IRaft` accessors and the gossiped report, not as counters or gauges. Read them programmatically via
`GetPartitionLogOpsPerSecond` / `GetPartitionCommitWaitMs`.

The WAL backlog, however, is already a published metric under the `Kommander` meter (consumable via
OpenTelemetry, `dotnet-counters`, etc.):

| Metric | Type | Meaning |
|---|---|---|
| `raft.wal.queue_depth` | observable gauge (per partition) | Pending-or-in-flight WAL operations per partition — the same backlog `WalQueueDepth` reports. |

Watching `raft.wal.queue_depth` per partition is the easiest operator-side way to spot a saturating
partition: a partition whose gauge climbs and stays high is fsync-bound. Cross-reference it with the
partition's `LogOpsPerSecond` (via the accessor) to confirm the rate has plateaued.

---

## Safety: what this can and cannot do

This signal is **purely advisory and read-only with respect to committed truth.**

- It is a *measurement*, not an actuator. Reading it changes nothing about partitions, leadership, or
  the log.
- A stale, missing, or imprecise value can at worst cause a *consumer* to make a suboptimal split
  decision (or none) — it can never violate Raft safety, because it never participates in consensus.
- The values are derived from the same advisory, gossiped load reports the balancer uses; they are
  never written to the Raft log and never durable.
- The `0` sentinel guarantees an "unknown" reads as "not hot," so missing data fails safe.

In short: the only failure modes are "didn't notice a hot partition" or "thought a partition was
hotter/cooler than it was," and both are the consumer's concern, not a correctness risk.

---

## Code map — where everything lives

| Concern | Where |
|---|---|
| Per-partition log-rate measurement (the EWMA accumulator) | `Kommander/Diagnostics/PartitionLoadAccumulator.cs` |
| Recording log-routed ops onto the rate | `Kommander/Scheduling/RaftPartitionExecutor.cs` |
| WAL backlog & commit-wait per partition | `Kommander/WAL/IO/FairWalScheduler.cs` (`GetPartitionDepth` / `GetPartitionCommitWaitMs`) |
| Commit-wait latency EWMA | `Kommander/Diagnostics/PartitionWaitAccumulator.cs` |
| The values carried on each partition entry | `Kommander/System/PartitionLoad.cs` (`LogOpsPerSecond`, `WalQueueDepth`, `CommitWaitMs`) |
| Building a node's own report | `RaftManager.BuildLocalLoadReport()` |
| The accessors | `RaftManager.GetPartitionLogOpsPerSecond` / `GetPartitionWalQueueDepth` / `GetPartitionCommitWaitMs` |
| Gossip of the report | `RaftManager` gossip path (gated on `EnableLeaderBalancer`) |
| Configuration knobs | `Kommander/RaftConfiguration.cs` |
| WAL-depth metric | `Kommander/Diagnostics/KommanderMetrics.cs` (`raft.wal.queue_depth`) |

A good reading order: `PartitionLoadAccumulator` (how a rate is measured) → `RaftPartitionExecutor`
(where ops are counted) → `PartitionLoad` (what's reported) → `BuildLocalLoadReport` →
`GetPartitionLogOpsPerSecond` (how a reader resolves a value).

---

## FAQ

**Why not just split when `LogOpsPerSecond` is high?**
Because throughput plateaus at the fsync ceiling, so a maxed-out partition and a wildly overloaded one
look identical on rate. A saturation signal (`WalQueueDepth`, or `CommitWaitMs`) is what distinguishes
them. Use rate plus one of those, never rate alone.

**Should I use `WalQueueDepth` or `CommitWaitMs`?**
Start with `WalQueueDepth` — it's simpler and self-clearing (drains to 0 when the backlog clears).
Reach for `CommitWaitMs` only if depth is too coarse for your trigger; it's the more direct fsync-
pressure measure but is **sticky when idle** (it holds its last value until the next write), so it must
always be paired with `LogOpsPerSecond` so a stale wait on a now-idle partition can't trigger anything.

**I always get `0` for partitions another node leads. Why?**
Almost certainly `EnableLeaderBalancer` is off, so no load reports are gossiped. The local fast-path
still works for partitions *this* node leads; the remote path needs reporting enabled. See
[Using it](#using-it-what-you-must-enable).

**Is `0` "idle" or "no data"?**
It can be either — the sentinel is intentionally ambiguous, and for a split decision both mean "not
hot." If you must tell them apart, check whether a report from the leader exists before trusting a `0`.

**Does reading this signal risk my data?**
No. It's a measurement only. It never participates in consensus and never mutates anything.

**Will splitting a hot partition always help?**
Only if the new leader lands on a **different node**. fsync is shared across partitions on a node
(group commit), so splitting in place relieves no capacity. The signal tells you *whether* a partition
is hot, not *where* to put the relief — that placement is the consumer's responsibility.

**Why is a follower's rate for a partition zero even under heavy load?**
The rate is leader-only by nature: only the leader's log-append path is counted. A follower applying
replicated entries doesn't increment the rate. Ask the leader (or read the gossiped value), not a
follower's local view.

**How fresh is a remote value?**
Up to one `LeaderBalancerReportInterval` (default 5s) plus gossip propagation, on top of the rate's ≈7s
smoothing. Treat it as accurate to ~10 seconds, and debounce your consumer accordingly.
