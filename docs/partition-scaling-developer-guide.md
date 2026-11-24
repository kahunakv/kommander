# Partition Scaling: Shared Executor Pool & Hot-Set CheckLeader — Developer Guide

Kommander can host **hundreds to thousands of partitions on one node**. Two mechanisms limit the cost of an
idle partition.

1. **The shared executor pool.** The partitions share a small fixed set of worker threads.
2. **Hot-set `CheckLeader`.** The leader-check timer ticks only the *active* partitions on each fast cycle.

This guide covers threads and CPU. The
[Partition Quiescence Developer Guide](partition-quiescence-developer-guide.md) covers network traffic. The
hot set uses the quiesced state that quiescence tracks.

---

## Table of contents

1. [Summary](#summary)
2. [Core concepts](#core-concepts)
3. [Flow 1 — A partition becomes runnable and is drained](#flow-1--a-partition-becomes-runnable-and-is-drained)
4. [Flow 2 — The single-owner guarantee on a shared pool](#flow-2--the-single-owner-guarantee-on-a-shared-pool)
5. [Flow 3 — CheckLeader ticks only the hot set](#flow-3--checkleader-ticks-only-the-hot-set)
6. [Flow 4 — An idle partition leaves the hot set, and comes back](#flow-4--an-idle-partition-leaves-the-hot-set-and-comes-back)
7. [Flow 5 — Failover for a quiet partition](#flow-5--failover-for-a-quiet-partition)
8. [Scope limits](#scope-limits)
9. [Configuration](#configuration)
10. [Code map](#code-map)
11. [Invariants you must not break](#invariants-you-must-not-break)
12. [Testing](#testing)

---

## Summary

Operations for one partition must execute **one at a time, in order**. This *single-owner* rule lets the
state machine work lock-free. A dedicated thread per partition gives that order, but each thread stack is
~1 MB. This means that **1,000 partitions need ~1 GB of stacks**, and 10,000 are not viable. In the
**shared pool**, a fixed number of worker threads (`P`, default = CPU count) serve all partitions. A
**per-partition run-lock** lets at most one pool thread work on a partition at a time.

A timer also asks each partition every 250 ms whether it needs an election. At `M = 5,000` partitions, a
full pass on each tick is ~20,000 wakeups per second. The timer ticks only a **hot set** of active
partitions on the fast cycle, with a full **safety sweep** every ~5 s.

A quiesced follower partition costs **no thread and no fast-cycle tick**.

---

## Core concepts

| Concept | What it is |
|---|---|
| **Single-owner rule** | Two operations for the *same* partition do not execute concurrently. |
| **Executor** | `RaftPartitionExecutor`, one per partition. It owns the partition's priority queues and drains them into the state machine. Producers use `Post`/`Ask`. |
| **Shared pool** | `RaftExecutorPool`: `P` worker threads and a global ready-queue of partitions that have pending work. |
| **Runnable** | A partition that has queued work and is in the pool's ready-queue. |
| **Run-lock** | A per-partition `0/1` gate. A pool thread must win it before it drains that partition. |
| **Bounded quantum** | The fixed batch of each priority class that a pool thread drains before it yields the partition. |
| **Hot set** | The partitions that receive a `CheckLeader` tick on each fast cycle. |
| **Safety sweep** | A full pass over *all* partitions at the slower `UpdateNodesInterval`. |

---

## Flow 1 — A partition becomes runnable and is drained

1. A producer calls `Post`/`Ask` on the executor. The request goes into one of four priority queues
   (control → replication → client → maintenance).
2. The executor marks itself **runnable**. A compare-and-set claims an "in queue" flag. If it wins, the
   executor is enqueued onto the global ready-queue and one parked worker is woken.
3. A free worker dequeues the executor, acquires its **run-lock**, and drains **one bounded quantum** in
   weighted-fair priority order.
4. The worker releases the run-lock. If the partition has more work, it is enqueued again. Otherwise its
   "in queue" flag is cleared.

The producer side does not block on a thread.

---

## Flow 2 — The single-owner guarantee on a shared pool

- The "in queue" flag limits a partition to **at most one** ready-queue entry.
- One worker dequeues that entry, and it must **win the per-partition run-lock** before it touches the state
  machine.
- The worker has the run-lock for the full quantum. It releases the lock *before* it re-enqueues the
  partition.

Two threads therefore do not drain one partition at the same time, and the state machine needs no locks.

> The worker threads start when the `RaftManager` is constructed, before any partition executor starts. The
> restore of a pool-mode executor is scheduled onto the pool, and its shutdown waits for a pool thread to
> drain it. The pool must be live for the executor's full life.

---

## Flow 3 — CheckLeader ticks only the hot set

The timer fires on each `CheckLeaderInterval` (250 ms).

- The **system partition** is always ticked.
- A normal tick goes to the hot set only. The hot set contains the non-quiesced partitions. While
  `EnableCheckQuorum` is on (the default), it also contains the quiesced partitions that this node leads, as
  their check-quorum probe depends on the tick.
- One tick in `UpdateNodesInterval / CheckLeaderInterval` (with the defaults, **the 20th, about once every
  5 s**) is a **full safety sweep** over *all* partitions. The sweep catches a partition that left the hot
  set incorrectly.

With `EnableSharedExecutorPool = false`, each tick is a full sweep.

---

## Flow 4 — An idle partition leaves the hot set, and comes back

Hot-set membership follows the partition's **quiesced** state (see the
[Quiescence guide](partition-quiescence-developer-guide.md)).

- When a partition **quiesces**, a callback removes it from the hot set, unless the check-quorum exception in
  Flow 3 applies.
- When a partition **un-quiesces** (a new proposal, an inbound `AppendLogs`, or a vote), the callback adds it
  again at once.

The callback fires in the partition's serial execution, and the hot set is a concurrent map. This makes the
bookkeeping race-free with respect to the partition's transitions.

A partition that is **removed** or merged away is evicted from the hot set in the same step in which it
leaves the live partition map. A stopped executor in the hot set makes the next tick throw.

---

## Flow 5 — Failover for a quiet partition

1. **SWIM-driven wake (fast path).** When the failure detector marks a node `Suspect` or `Dead`, each quiet
   partition that believed that node was its leader is **promoted to the hot set** immediately. It gets a
   `CheckLeader` tick on the next fast cycle and can start an election. Failover latency for a quiet
   partition is bounded by SWIM detection.
2. **The safety sweep (backstop).** The full sweep visits all partitions every ~5 s and checks leadership
   again. It does not need a SWIM signal.

For `Alive`, `Suspect` and `Dead`, see the
[Dynamic Membership guide](dynamic-membership-developer-guide.md).

---

## Scope limits

- **Per-partition memory is not released.** A resident partition has a thin in-memory footprint (executor,
  queues, state machine). At thousands of partitions this is modest, but tens of thousands of unused
  partitions are not free.
- **WAL handles are not closed for idle partitions.** The WAL is a **single sharded instance shared by all
  partitions**. Open handles scale with the shard count.
- **Raft does not change.** Terms, quorum, commit rules, and the log format are the same.

---

## Configuration

| Option | Default | Meaning |
|---|---|---|
| `EnableSharedExecutorPool` | `true` | Master switch. `false` = one OS thread per partition, and each `CheckLeader` tick is a full sweep. Both settings are Raft-safe. Use `false` only to isolate a problem. |
| `PartitionExecutorPoolSize` | `0` | Number of pool worker threads. `0` auto-sizes to `Environment.ProcessorCount`; values below 0 are clamped to 1. |
| `IdleWorkerMaxSpinBeforePark` | `20 µs` | Upper bound of how long one idle pool thread polls the ready-queue before it parks. The same setting applies to the WAL write and read schedulers. |

The hot-set cadence comes from `CheckLeaderInterval` (fast) and `UpdateNodesInterval` (safety sweep).

Control-plane operations (heartbeats, votes) have a **priority lane** across the shared pool. Client load on
one partition cannot starve the election traffic of another partition.

### Fixed pool size

The thread count is set at construction and is fixed for the life of the `RaftManager`. To change it, change
the configuration and restart the node.

### Idle pool threads

An idle pool thread is **parked on a private event** and uses no CPU. Scheduling an executor wakes exactly
one parked thread. The pool size can therefore follow the number of busy partitions at no cost per idle
thread.

- **One spinner.** After it runs out of work, one thread (at most) polls the ready-queue for a moment before
  it parks. An executor scheduled in that moment needs no system call and has no wake-up delay. A few spins
  that find no work close the window, and work that arrives soon after a park reopens it.
  `IdleWorkerMaxSpinBeforePark` is its upper bound. `Zero` turns the spin off, which uses the least CPU and
  makes each hand-off to an idle pool a kernel wake. A larger value trades CPU for a shorter round on a host
  with idle cores.
- **No wake for a re-schedule.** A thread that drained an executor with more work puts it back in the
  ready-queue and takes it again. If it takes another partition first, it wakes a parked thread.

`RaftExecutorPool.WorkerWakeups` and `EmptyWorkerWakeups` count the hand-offs. The WAL write scheduler has
the same handshake and counters (`FairWalScheduler.WorkerWakeups`, `EmptyWorkerWakeups`).

### Pool sizing

The pool size sets **how many partitions can be mid-drain at the same instant**. Idle partitions need no
threads.

**Pool threads are CPU-bound.** The state machine *enqueues* a WAL write and returns immediately (`Pending`).
The fsync happens on a **separate** I/O pool of `WriteIOThreads` (default 4), and the reply completes later
through a callback.

| Bottleneck | Option to change |
|---|---|
| CPU / scheduling parallelism (executor work) | `PartitionExecutorPoolSize` |
| Disk write throughput (WAL fsync) | `WriteIOThreads` |

- **Start with the default (`0` → `ProcessorCount`).** It is right for almost all deployments.
- **Raise it above `ProcessorCount` only if you see head-of-line latency while CPU is *not* saturated.** In
  that case the threads stall, for example in a state-machine callback that blocks.
- **Do not raise it when CPU is saturated.** More threads add context switches. Spread the partitions across
  more nodes.
- **Do not set it to the partition count.** That brings back the per-partition memory cost.

To decide, watch three signals together.

- Per-dispatch latency and the executor's slow-message warnings
- Client-queue depth and rejections
- CPU utilization

A busy partition yields after its quantum, and a small pool makes progress across many partitions. The pool
size affects only **tail latency under contention**.

---

## Code map

| File | Responsibility |
|---|---|
| `Kommander/Scheduling/RaftExecutorPool.cs` | The shared pool: worker threads, the global ready-queue, `Schedule`, and the worker loop that calls `DrainOnPool`. |
| `Kommander/Scheduling/RaftPartitionExecutor.cs` | Per-partition executor: `MarkRunnable` (idempotent enqueue), `DrainOnPool` (run-lock, bounded-quantum drain, re-enqueue), `Post`/`Ask`, and the dedicated-thread path for a disabled pool. |
| `Kommander/RaftTimerService.cs` | `TriggerCheckLeader`: the system partition, hot-set, and safety-sweep ticks. |
| `Kommander/RaftManager.cs` | Pool ownership (created and started in the constructor), the hot-set map, `MarkPartitionHot`/`MarkPartitionCool`, the SWIM-driven wake, and hot-set eviction on partition removal. |
| `Kommander/RaftPartition.cs` | The quiesce-state-change callback that updates hot-set membership. |
| `Kommander/RaftConfiguration.cs` | `EnableSharedExecutorPool`, `PartitionExecutorPoolSize`, `IdleWorkerMaxSpinBeforePark`. |
| `Kommander/Scheduling/ParkedWorkerQueue.cs` | The ready-queue and idle-thread handshake shared by the pool and the WAL write scheduler. |

---

## Invariants you must not break

- **Single-owner.** Two threads must not drain the same partition concurrently. The per-partition run-lock
  enforces this.
- **Idempotent ready-enqueue.** If a partition is in the ready-queue twice, two workers can pick it up.
  Preserve the "in queue" compare-and-set flag and the clear/re-check order at the end of a drain. That order
  closes the race between "no more work" and a producer that enqueues at that moment.
- **Control-plane priority.** Client load on another partition must not starve heartbeats and votes. The
  bounded quantum and the priority lanes provide this.
- **Hot-set entries must point at live partitions.** Add a partition when it starts, and remove it in the
  *same* step in which it leaves the live partition map. A stale entry makes the next tick throw and aborts
  the sweep for all partitions after it.
- **The pool outlives its executors.** Start the pool before the executors and stop it after them. Restore
  and shutdown of a pool-mode executor depend on a running pool.

---

## Testing

- **Single-owner under load:** drive many concurrent operations across many partitions on a small pool, and
  assert that no two operations for the same partition overlap.
- **Thread-count scaling:** create many partitions with the pool on, and assert that the process does **not**
  start one `RaftPartitionExecutor` thread per partition. Compare with the pool off (one each).
- **Control-plane not starved:** saturate one partition's client queue, and assert that another partition's
  control operation completes within a tight bound.
- **Hot-set targeting:** assert that the fast cycle ticks only the hot set and that the safety sweep fires on
  the expected cadence. Assert that the disabled flag makes each tick a full sweep.
- **Quiesce/un-quiesce membership:** assert that a partition leaves the hot set when it quiesces and returns
  when it un-quiesces. Assert that a tick after a partition removal does not throw.
- **No-`JoinCluster` path:** a pool-mode manager that drives partitions directly, with no cluster join, must
  create and remove partitions and not deadlock. The pool starts in the constructor for this case.

Run scheduler-touching suites one at a time. The suite starts in-process clusters and is timing-sensitive.
