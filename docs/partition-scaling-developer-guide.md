# Partition Scaling: Shared Executor Pool & Hot-Set CheckLeader — Developer Guide

This guide explains how Kommander runs **hundreds to thousands of partitions on one node** without the
per-partition cost growing out of control. It covers two complementary mechanisms that ship together:

1. **The shared executor pool** — many partitions share a small fixed set of worker threads instead of
   each owning a dedicated OS thread.
2. **Hot-set `CheckLeader`** — the leader-check timer ticks only the *active* partitions every cycle,
   not all of them.

It is written for two readers — someone who wants to *understand* why thousands of idle partitions are
cheap, and someone who wants to *extend or debug* the scheduler. No prior Raft expertise is assumed; we
build the ideas up first, then walk the real flows and the code.

This guide is the **threads-and-CPU** half of partition scaling. Its sibling, the
[Partition Quiescence Developer Guide](partition-quiescence-developer-guide.md), is the **network**
half (stopping idle partitions from emitting heartbeats). The two are independent but designed to work
together; the hot set reuses the quiesced state that quiescence already tracks.

---

## Table of contents

1. [Summary](#summary)
2. [Why this exists (the problem)](#why-this-exists-the-problem)
3. [Core concepts](#core-concepts)
4. [Flow 1 — A partition becomes runnable and is drained](#flow-1--a-partition-becomes-runnable-and-is-drained)
5. [Flow 2 — The single-owner guarantee on a shared pool](#flow-2--the-single-owner-guarantee-on-a-shared-pool)
6. [Flow 3 — CheckLeader ticks only the hot set](#flow-3--checkleader-ticks-only-the-hot-set)
7. [Flow 4 — An idle partition leaves the hot set, and comes back](#flow-4--an-idle-partition-leaves-the-hot-set-and-comes-back)
8. [Flow 5 — Failover for a quiet partition](#flow-5--failover-for-a-quiet-partition)
9. [What this does *not* do](#what-this-does-not-do)
10. [Configuration](#configuration)
11. [Code map](#code-map)
12. [Invariants you must not break](#invariants-you-must-not-break)
13. [Testing](#testing)
14. [Glossary](#glossary)

---

## Summary

Every partition is a little serial state machine: operations for one partition must run **one at a
time, in order** (this is the *single-owner* rule — it lets the state machine run lock-free).

The simple way to guarantee that is to give each partition its own thread. That works until you have a
lot of partitions: at ~1 MB of stack per thread, **1,000 partitions ≈ 1,000 threads ≈ ~1 GB of
stacks**, and 10,000 is not viable. The thread is the hard ceiling.

Kommander breaks the ceiling with a **shared pool**: a small fixed number of worker threads
(`P`, default = CPU count) serve *all* partitions. A partition raises its hand when it has work; a free
pool thread picks it up, drains a bounded chunk, and moves on. Serial execution is preserved by a
**per-partition run-lock** — at most one pool thread works a given partition at a time.

Separately, a timer fires every 250 ms asking each partition "are you still the leader / do you need an
election?" Ticking *all* `M` partitions four times a second is wasteful when most are idle. So the
timer keeps a **hot set** — the partitions that are actually active — and ticks only those on the fast
cycle, with a slower full **safety sweep** every ~5 s as a backstop.

Result: an idle partition costs **no thread and no periodic tick**. Only the partitions doing work pay
for scheduling.

---

## Why this exists (the problem)

The target is a deployment with **many partitions, most of them idle**. Two costs used to grow linearly
with the partition count `M` and dominated before anything else:

- **One OS thread per partition.** Even a perfectly idle partition parked on a semaphore still holds a
  managed thread stack and a slot in the GC/scheduler. This is the hard ceiling described above.
- **A `CheckLeader` fan-out over every partition every 250 ms.** The leader-check timer posted a
  message into *every* partition's queue on every tick — at `M = 5,000` that is ~20,000 wakeups/sec
  doing nothing but waking a thread to immediately re-park it.

Both scaled with `M` regardless of whether a partition was doing any work. The shared pool removes the
first; hot-set `CheckLeader` removes the second.

---

## Core concepts

| Concept | What it is |
|---|---|
| **Single-owner rule** | No two operations for the *same* partition ever execute concurrently. The state machine relies on this to run without locks. |
| **Executor** | `RaftPartitionExecutor` — one per partition. Owns the partition's priority queues and drains them into the state machine. The *intake* side (`Post`/`Ask`) is unchanged from the dedicated-thread model; only *who runs the draining* changed. |
| **Shared pool** | `RaftExecutorPool` — `P` worker threads plus a global ready-queue of partitions that have pending work. |
| **Runnable / ready-queue** | A partition is "runnable" when it has queued work and is sitting in the pool's ready-queue waiting for a free worker. |
| **Run-lock** | A per-partition `0/1` gate. A pool thread must win it before draining that partition, guaranteeing single-owner across the shared pool. |
| **Bounded quantum** | A pool thread drains at most a fixed batch of each priority class, then yields the partition so others make progress (cooperative scheduling). |
| **Hot set** | The partitions that receive a `CheckLeader` tick on every fast cycle: the non-quiesced (active) partitions. |
| **Safety sweep** | A coarse, full pass over *all* partitions at the slower `UpdateNodesInterval`, catching anything that should have been in the hot set but wasn't. |

---

## Flow 1 — A partition becomes runnable and is drained

1. A producer calls `Post`/`Ask` on the executor (an inbound `AppendLogs`, a client proposal, a
   `CheckLeader` tick, …). The request lands in one of the executor's four priority queues
   (control → replication → client → maintenance).
2. The executor marks itself **runnable**: a compare-and-set claims a "I'm in the queue" flag, and if
   it wins, the executor is enqueued onto the pool's global ready-queue and one parked worker is woken.
   (The flag makes re-enqueue idempotent — a partition is never in the ready-queue twice.)
3. A free pool worker dequeues the executor, acquires its **run-lock**, and drains **one bounded
   quantum** of work in weighted-fair priority order.
4. The worker releases the run-lock. If the partition still has queued work, it re-enqueues itself;
   otherwise it clears its "in queue" flag. Either way the worker returns to the pool and picks up the
   next runnable partition.

The producer side never blocks on a thread and never knows whether a dedicated thread or a pool thread
will do the draining — only the *consumer* side changed.

---

## Flow 2 — The single-owner guarantee on a shared pool

This is the invariant that correctness depends on, so it is worth being explicit.

- A partition is enqueued onto the ready-queue **at most once** at a time (the idempotent "in queue"
  flag).
- Only one worker dequeues that single entry, and it must **win the per-partition run-lock** before
  touching the state machine.
- The worker holds the run-lock for the whole quantum, releases it, and only *then* re-enqueues.

So even though `P` threads serve `M` partitions, **no two threads ever drain the same partition at the
same time** — exactly the guarantee a dedicated thread gave. The state machine code is unchanged and
still needs no locks.

> The pool's worker threads are started when the `RaftManager` is constructed, before any partition
> executor starts. A pool-mode executor's lifecycle depends on a *running* pool (its restore is
> scheduled onto the pool, and its shutdown waits for a pool thread to drain it), so the pool must be
> live for the executor's whole life.

---

## Flow 3 — CheckLeader ticks only the hot set

A timer fires every `CheckLeaderInterval` (250 ms). On each tick:

1. The **system partition** is always ticked (it coordinates the cluster and is never idle in the same
   sense).
2. Most ticks are **hot-set ticks**: only the active (non-quiesced) partitions get a `CheckLeader`
   message. Idle/quiesced partitions are skipped entirely.
3. Every `UpdateNodesInterval / CheckLeaderInterval` ticks (with the defaults, **every 20th tick ≈ once
   every 5 s**) the tick is a **full safety sweep** over *all* partitions instead. This is the backstop
   that re-checks anything that fell out of the hot set incorrectly.

With the escape hatch off (`EnableSharedExecutorPool = false`), every tick is a full sweep — the
original behaviour.

The win: at `M = 5,000` with, say, 50 active partitions, the fast cycle ticks ~50 partitions four times
a second instead of 5,000 — and the 5,000-wide pass happens only once every 5 s.

---

## Flow 4 — An idle partition leaves the hot set, and comes back

Hot-set membership is kept in sync with the partition's **quiesced** state (see the
[Quiescence guide](partition-quiescence-developer-guide.md) for what drives quiescing):

- When a partition **quiesces** (goes idle), a callback removes it from the hot set. It no longer gets
  fast-cycle ticks — only the 5 s safety sweep.
- When a partition **un-quiesces** — a new proposal, an inbound `AppendLogs`, a vote — the same callback
  re-adds it to the hot set, so it is back on the fast cycle immediately.

Because the callback fires from inside the partition's serial execution, the hot-set bookkeeping is
race-free with respect to the partition's own transitions. The hot set is a concurrent map, so the
timer thread can read it while partitions add/remove themselves.

When a partition is **removed** (or merged away), it is evicted from the hot set in the same step it is
removed from the live partition map — otherwise a stopped executor would linger in the hot set and the
next tick would throw.

---

## Flow 5 — Failover for a quiet partition

Skipping idle partitions on the fast cycle raises a fair question: *if a quiet follower's leader dies,
who notices?*

Two mechanisms cover it:

1. **SWIM-driven wake (fast path).** When the failure detector marks a node `Suspect` or `Dead`, every
   quiet partition that believed that node was its leader is **promoted back into the hot set**
   immediately, so it gets a `CheckLeader` tick on the very next fast cycle and can start an election.
   Failover latency for a quiet partition is therefore bounded by SWIM detection, not by the slow sweep.
2. **The safety sweep (backstop).** Even with no SWIM signal, the full sweep every ~5 s visits every
   partition and re-checks leadership.

So a quiet partition is cheap *and* still fails over promptly. (For the SWIM vocabulary —
`Alive`/`Suspect`/`Dead` — see the
[Dynamic Membership guide](dynamic-membership-developer-guide.md).)

---

## What this does *not* do

To set expectations clearly:

- **It does not release per-partition memory.** A resident partition still keeps a thin in-memory
  footprint (its executor, priority queues, and state machine). The shared pool removes the *thread*
  (the dominant cost — ~1 MB of stack each); the small managed objects remain. At thousands of
  partitions this is modest; declaring tens of thousands of partitions you never touch is not free.
- **It does not close WAL handles for idle partitions.** Kommander's WAL is a **single instance shared
  by all partitions** (sharded internally), so there is no per-partition file handle to release in the
  first place — open handles scale with the number of shards, not the number of partitions. There is
  deliberately no "cold tier" that detaches the WAL.
- **It does not change Raft.** Terms, quorum, the commit rules, the log format, and the
  replication/commit path are all untouched. This is purely about *where work runs and what gets
  ticked*.

---

## Configuration

| Knob | Default | Meaning |
|---|---|---|
| `EnableSharedExecutorPool` | `true` | Master switch. `true` = many partitions share a bounded thread pool. `false` = the original one-OS-thread-per-partition model, and every `CheckLeader` tick becomes a full sweep. Both settings are Raft-safe. |
| `PartitionExecutorPoolSize` | `0` | Number of pool worker threads. `0` auto-sizes to `Environment.ProcessorCount`; values below 0 are clamped to 1. |

### The pool is fixed-size — there is no autoscaling

The pool allocates exactly `PartitionExecutorPoolSize` (or `Environment.ProcessorCount` when `0`)
threads at construction and **never grows or shrinks them**. The thread count is fixed for the life of
the `RaftManager`; changing it means changing the config and restarting. This is deliberate — the value
of the pool is that it is a small *bounded* `P ≪ M`; autoscaling toward `M` would reintroduce the very
thread-per-partition cost the pool exists to remove.

### How to size it

The pool size governs **how many partitions can be mid-drain at the same instant** — it scales with the
number of *simultaneously busy* partitions, **not** the total partition count. Ten thousand idle
partitions need zero extra threads.

The key fact for sizing: **pool threads are CPU-bound, not I/O-bound.** When the state machine writes to
the WAL it *enqueues* the write and returns immediately (`Pending`); it does not block waiting for the
disk. The actual fsync runs on a **separate** I/O pool sized by `WriteIOThreads` (default 4), and the
reply completes later via callback. So a pool thread spends its time on state-machine logic,
serialization, and draining queues — not parked on disk. That gives a clean division of dials:

| Bottleneck | Knob to turn |
|---|---|
| CPU / scheduling parallelism (executor work) | `PartitionExecutorPoolSize` |
| Disk write throughput (WAL fsync) | `WriteIOThreads` |

Practical guidance:

- **Start with the default (`0` → `ProcessorCount`).** It is right for almost every deployment, because
  the executor work is CPU-bound and WAL I/O is offloaded to `WriteIOThreads`.
- **Raise it above `ProcessorCount` only if you observe head-of-line latency while CPU is *not*
  saturated** — ops queuing behind busy partitions while cores sit idle. That means threads are stalling
  (e.g. a custom state-machine callback that blocks). Adding threads helps only when threads are
  *waiting*, never when cores are *full*.
- **Don't raise it when CPU is already saturated** — more threads just add context-switching. You're
  compute-bound; spread partitions across more nodes instead.
- **Never set it to the partition count** — that recreates the per-partition memory cost the pool exists
  to remove.
- If disk is the bottleneck (WAL writes backing up), tune **`WriteIOThreads`**, not the executor pool.

To decide empirically, watch per-dispatch latency (and the executor's slow-message warnings),
client-queue depth / rejections, and CPU utilization together: rising latency *with spare CPU* → too few
(or stalling) threads → increase; rising latency *with CPU saturated* → the pool is not the bottleneck,
leave it. The bounded drain quantum keeps even a small pool making progress across many partitions (a
busy partition yields after its quantum), so pool size affects **tail latency under contention**, not
correctness or liveness.

### Other notes

- **Leave `EnableSharedExecutorPool = true`** unless you are isolating a problem; it is what makes large
  partition counts viable. The `false` path (one OS thread per partition) exists as an escape hatch.
- Control-plane operations (heartbeats, votes) keep their **priority lane** across the shared pool, so
  client load on one partition cannot starve another partition's election traffic.
- The hot-set fast/slow cadence is derived from existing timers — `CheckLeaderInterval` (fast) and
  `UpdateNodesInterval` (safety sweep). There is no separate knob for it.

---

## Code map

| File | Responsibility |
|---|---|
| `Kommander/Scheduling/RaftExecutorPool.cs` | The shared pool: `P` worker threads, the global ready-queue, `Schedule`, and the worker loop that calls `DrainOnPool`. |
| `Kommander/Scheduling/RaftPartitionExecutor.cs` | Per-partition executor. `MarkRunnable` (idempotent enqueue), `DrainOnPool` (run-lock + bounded-quantum drain + re-enqueue), and the unchanged `Post`/`Ask` intake. Also holds the dedicated-thread path used when the pool is disabled. |
| `Kommander/RaftTimerService.cs` | `TriggerCheckLeader`: ticks the system partition always, the hot set on fast cycles, and the full set on the periodic safety sweep. |
| `Kommander/RaftManager.cs` | Owns the pool (created and started in the constructor), the hot-set map, `MarkPartitionHot`/`MarkPartitionCool`, the SWIM-driven wake, and hot-set eviction on partition removal. |
| `Kommander/RaftPartition.cs` | Wires the quiesce-state-change callback that keeps hot-set membership in sync. |
| `Kommander/RaftConfiguration.cs` | `EnableSharedExecutorPool`, `PartitionExecutorPoolSize`. |

---

## Invariants you must not break

- **Single-owner.** Whatever you change in the scheduler, no two threads may drain the same partition
  concurrently. The per-partition run-lock is the enforcement point; keep it.
- **Idempotent ready-enqueue.** A partition must never sit in the ready-queue twice, or two workers
  could pick it up. The "in queue" compare-and-set flag guarantees this — preserve the
  clear/re-check ordering at the end of a drain (it closes the race between "no more work" and a
  producer enqueuing just then).
- **Control-plane priority survives the pool.** Heartbeats and votes must not be starvable by client
  load on another partition. The bounded quantum + priority lanes provide this.
- **Hot-set entries must point at live partitions.** Add a partition to the hot set when it starts;
  remove it in the *same* step it leaves the live partition map. A stale entry pointing at a stopped
  executor makes the next tick throw and aborts the sweep for everything after it.
- **The pool outlives its executors.** A pool-mode executor's restore and shutdown depend on a running
  pool. Start the pool before executors and stop it after them.

---

## Testing

- **Single-owner under load:** drive many concurrent operations across many partitions on a small pool
  and assert no two operations for the same partition overlap (instrument the run-lock).
- **Thread-count scaling:** create many partitions with the pool on and assert the process does **not**
  spawn one `RaftPartitionExecutor` thread per partition; contrast with the pool off (one each) to prove
  the win.
- **Control-plane not starved:** saturate one partition's client queue and assert another partition's
  control op still completes within a tight bound.
- **Hot-set targeting:** assert the fast cycle ticks only the hot set and the safety sweep fires on the
  expected cadence; assert the disabled flag falls back to a full sweep every tick.
- **Quiesce/un-quiesce membership:** assert a partition leaves the hot set when it quiesces and re-enters
  when it un-quiesces, and that removing a partition evicts it so a subsequent tick does not throw.
- **No-`JoinCluster` path:** a pool-mode manager that drives partitions directly (without joining a
  cluster) must still create and remove partitions without deadlocking — the pool starts in the
  constructor precisely so this works.

Run scheduler-touching suites one at a time (the suite spins up in-process clusters and is
timing-sensitive).

---

## Glossary

| Term | Meaning |
|---|---|
| **Single-owner rule** | No two operations for one partition run concurrently; lets the state machine run lock-free. |
| **Executor** | `RaftPartitionExecutor`: per-partition priority queues + draining into the state machine. |
| **Shared pool** | `RaftExecutorPool`: `P` worker threads serving all partitions via a global ready-queue. |
| **Run-lock** | Per-partition `0/1` gate ensuring at most one pool thread drains a partition at a time. |
| **Runnable** | A partition with queued work, sitting in the pool's ready-queue. |
| **Bounded quantum** | The fixed batch a worker drains before yielding a partition to others. |
| **Hot set** | The active (non-quiesced) partitions that receive a `CheckLeader` tick every fast cycle. |
| **Safety sweep** | The periodic full pass over all partitions (≈ every 5 s) that backstops the hot set. |
| **Quiesced** | A partition that has gone idle (see the Quiescence guide); such partitions leave the hot set. |
| **SWIM wake** | Promoting a quiet partition back to the hot set when its leader's node is reported `Suspect`/`Dead`. |
| **`P` / `M`** | `P` = pool thread count (≈ CPU count); `M` = partition count. The whole point is `P ≪ M`. |
