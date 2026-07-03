# System-Partition State Snapshots (P0 Delta Logs) — Developer Guide

This guide explains how an application stores **cluster-wide control state on the system partition
(P0)** and, in particular, how to move from **full-snapshot-per-mutation** replication to cheap
**delta** replication without ever losing state. It is written for someone integrating an application
state machine on top of Kommander. No prior Raft expertise is assumed; we build the idea up first,
then walk the contract and the flows.

---

## Table of contents

1. [Summary](#summary)
2. [Background: what the system partition is for](#background-what-the-system-partition-is-for)
3. [The problem with full-snapshot-per-mutation](#the-problem-with-full-snapshot-per-mutation)
4. [Why deltas need a repair path](#why-deltas-need-a-repair-path)
5. [The two safety nets](#the-two-safety-nets)
6. [The consumer contract (five steps)](#the-consumer-contract-five-steps)
7. [Flow 1 — Steady-state delta replication](#flow-1--steady-state-delta-replication)
8. [Flow 2 — Repairing a below-floor follower](#flow-2--repairing-a-below-floor-follower)
9. [Flow 3 — Cold restart from a local snapshot](#flow-3--cold-restart-from-a-local-snapshot)
10. [Checkpoints, compaction, and the retain floor](#checkpoints-compaction-and-the-retain-floor)
11. [Code map (public API)](#code-map-public-api)
12. [Invariants you must not break](#invariants-you-must-not-break)
13. [Testing your integration](#testing-your-integration)
14. [Glossary](#glossary)

---

## Summary

Partition 0 (**P0**, the *system partition*) carries Kommander's own coordination entries **and** any
application state you replicate there under your own log type. Applications use it for small,
cluster-wide control state — a partition/range map, a leased registry, a feature-flag table.

P0's log is **compacted**: a background pass deletes entries older than the last checkpoint. The
simplest way to survive compaction is to make **every mutation a full snapshot of your whole state** —
then any single surviving entry is complete. That is correct, but it is O(state size) **per mutation**,
which gets expensive when the state is large and mutations are frequent (e.g. lease renewals).

This feature lets you replicate **deltas** (one changed entry per mutation) instead, and stay
compaction-safe by registering a **whole-state snapshot** hook:

> You implement `IRaftSystemStateTransfer` — *export my whole state as of committed index N*, and
> *import a whole-state blob atomically*. Kommander uses it to repair any node that has fallen below
> the compaction floor, so old deltas can be safely compacted away.

```
   FULL-SNAPSHOT-PER-MUTATION            DELTA + WHOLE-STATE SNAPSHOT HOOK
   every write ships the entire state    every write ships one changed entry
   O(state) per mutation                 O(1) per mutation
   self-repairing (any entry complete)   repaired on demand via ExportPartitionState
```

The contract is five steps: **implement** the transfer, **register** it on every node, **replicate**
deltas, **persist** a local snapshot tagged with its index, and **advance the retain floor**. The rest
of this guide explains each and the flows behind them.

---

## Background: what the system partition is for

Every Kommander cluster has a system partition with id `0` (`RaftSystemConfig.SystemPartition`). It
carries two kinds of entries:

- **Reserved coordinator entries** under the log type `_RaftSystem` — Kommander's own partition maps,
  membership roster, split/merge plans. **You may not write this type**; `ReplicateLogs` rejects it on
  P0 with *"System log type is reserved on the system partition."*
- **Application entries** under **your own log type string**. These are committed on P0 and delivered
  to your `OnReplicationReceived` (live) / `OnLogRestored` (restore) callbacks, exactly like a normal
  partition.

Because P0 is replicated to every voter and is always present, it is the natural home for small state
that the whole cluster must agree on.

---

## The problem with full-snapshot-per-mutation

To survive compaction **without** any snapshot hook, the only safe pattern on P0 is:

> On every mutation, serialize your **entire** state and replicate it as one entry.

This works because compaction deletes everything below the checkpoint, and whatever single entry
survives is a complete copy — a late or restarting node converges from that one entry alone.

The cost is that a trivial change (renewing one lease out of thousands) still writes, replicates, and
fsyncs a multi-kilobyte record. Replication becomes **O(total state) per mutation**. For a hot,
frequently-touched registry this dominates WAL and network traffic.

Deltas fix the cost — one entry per change — but a delta is **not** self-contained. Once old deltas are
compacted away, a node that starts below that point cannot reconstruct the full state from the deltas
that remain. That is the gap this feature closes.

---

## Why deltas need a repair path

A node can end up **below the compaction floor** (needing entries the leader has already deleted) in
two ways:

1. **A live follower fell far behind** — it was slow or briefly partitioned, and by the time it asks
   for the missing range, the leader has compacted past it. Log backfill returns empty (it never ships
   a non-contiguous range), so it needs a snapshot.
2. **A node cold-restarts** with a locally-compacted WAL — the deltas it would replay to rebuild state
   are gone from its own disk.

With full snapshots, both cases self-heal (one entry is enough). With deltas, both cases need a way to
obtain the **whole state**. That is what `IRaftSystemStateTransfer` provides.

---

## The two safety nets

Delta safety rests on two mechanisms. You opt into both by following the contract.

**1. On-demand whole-state repair (covers the live below-floor follower).**
When the leader notices a P0 follower below the floor and a system-state transfer is registered, it
calls your `ExportPartitionState(0, checkpointIndex, ct)`, streams the blob in chunks to the follower,
and the follower calls your `ImportPartitionState(0, stream, ct)`. Kommander then seeds a
`CommittedCheckpoint` at that index so normal replication resumes from `index + 1`. You write no
transport code — Kommander drives the whole exchange.

**2. The retain floor (covers cold restart from local disk).**
`SetMinRetainIndex(0, index)` tells compaction *"never delete P0 entries below `index`."* You advance
it as you persist your own local snapshot, so the deltas you have **not yet** folded into a durable
local snapshot are always still on disk to replay after a restart.

> When the cluster is reachable, safety net #1 alone is enough — a cold-restarted node below the floor
> is repaired by the leader's snapshot. The retain floor is what lets a node **self-recover from its
> own disk** without pulling a full snapshot over the network every restart.

---

## The consumer contract (five steps)

### Step 1 — Implement `IRaftSystemStateTransfer`

```csharp
public sealed class MyP0StateTransfer : IRaftSystemStateTransfer
{
    private readonly MyStateMachine _state;

    // Export the WHOLE application state as of committed index `upToIndex`.
    // The blob must reflect exactly the state after `upToIndex` is applied — nothing above it.
    public Task<Stream> ExportPartitionState(int partitionId, long upToIndex, CancellationToken ct)
    {
        // Serialize a consistent snapshot of state at (or after) upToIndex.
        Stream blob = _state.SerializeSnapshotAsOf(upToIndex);
        return Task.FromResult(blob);   // Kommander reads it in chunks, then disposes it.
    }

    // Replace this node's whole application state with the received blob — ATOMICALLY.
    // A crash mid-import must leave the prior state intact (see the atomicity invariant).
    public async Task ImportPartitionState(int partitionId, Stream snapshot, CancellationToken ct)
    {
        await _state.InstallSnapshotAtomically(snapshot, ct);
    }
}
```

`partitionId` will be `0`. `upToIndex` is the committed index the blob must reflect — Kommander uses
it to seed the follower's checkpoint, so an inaccurate index will corrupt catch-up.

### Step 2 — Register it on **every** node

Registration is **not replicated** — each node registers independently, and you must do it on all of
them (leaders and followers, current and future), because any node may become the one that exports or
the one that imports.

```csharp
raft.RegisterSystemStateTransfer(new MyP0StateTransfer(state));
```

Registering the transfer is also the **signal** that your P0 logs may be deltas: it is what allows
Kommander to compact old P0 entries and still guarantee a below-floor node can be repaired. Pass `null`
to clear it. It is independent of `RegisterStateMachineTransfer` (which covers key-range split/merge);
you may register both.

### Step 3 — Replicate deltas

```csharp
RaftReplicationResult r = await raft.ReplicateLogs(
    partitionId: 0,
    type: "my-app-delta",      // your own type — NOT "_RaftSystem"
    data: encodedDelta,
    cancellationToken: ct);

if (r.Status == RaftOperationStatus.Success)
    long committedIndex = r.LogIndex;   // the WAL index this delta committed at
```

Each delta is a single change. `r.LogIndex` is the committed index of that entry — the same value that
arrives as `RaftLog.Id` in your replication callbacks on the followers.

### Step 4 — Persist a local snapshot tagged with its index

Periodically fold your in-memory state to durable local storage, **recording the committed index it
reflects** (call it `d`). On restart you will load this as the baseline and replay only deltas above
`d`. Kommander does not own this storage — it is yours.

### Step 5 — Advance the retain floor

After a local snapshot through index `d` is durable, tell compaction it may delete P0 deltas at or
below `d` but must keep everything above it:

```csharp
raft.SetMinRetainIndex(0, d + 1);   // keep entries with Id > d for offline replay
```

The floor is **in-memory and resets on process restart** — re-assert it every time the node starts,
after loading your local snapshot, before you rely on offline self-recovery.

---

## Flow 1 — Steady-state delta replication

```
   app → ReplicateLogs(0, "my-app-delta", data)
        → committed on P0, r.LogIndex = N
   followers: OnReplicationReceived(0, log) with log.Id == N
             → apply the single delta to local state
   periodically: persist local snapshot @ d ; SetMinRetainIndex(0, d+1)
   compaction: deletes P0 entries with Id < min(checkpoint, d+1)
```

Nobody ships the whole state on the hot path. WAL growth is bounded by compaction; the un-snapshotted
tail `(d, max]` is protected by the retain floor.

---

## Flow 2 — Repairing a below-floor follower

```
   leader notices follower F trails past the P0 compaction floor
   backfill returns empty (the needed deltas were compacted)
      │
      ├─ system-state transfer registered?  ── no ──► F cannot be caught up; a node still
      │                                               joining is rejected as permanently blocked
      │                                               (its JoinCluster fails fast) — always register it
      └─ yes
          leader:   ExportPartitionState(0, checkpointIndex) ──► stream
          chunks:   shipped as SnapshotKind.SystemState over the install-snapshot path
          F:        ImportPartitionState(0, stream)  (atomic)
          F:        WAL seeded with CommittedCheckpoint @ checkpointIndex
          F:        normal replication resumes at checkpointIndex + 1
```

You implement only the two endpoints; Kommander drives the trigger, chunking, transport, idempotency,
and checkpoint seeding.

---

## Flow 3 — Cold restart from a local snapshot

```
   node starts:
     1. load local snapshot  → in-memory state @ committed index d
     2. re-assert SetMinRetainIndex(0, d + 1)
     3. Kommander replays surviving WAL entries via OnLogRestored
          for each restored log:
              if (log.Id <= d)  → already in the local snapshot; SKIP
              else              → apply the delta
     4. state converges to the same value as its peers
```

If the local WAL was compacted below `d`'s tail (e.g. the retain floor was not asserted in time), the
node instead rejoins **below the floor** and is repaired by [Flow 2](#flow-2--repairing-a-below-floor-follower)
— the leader's snapshot. Either way it converges; the retain floor just avoids a network transfer.

The **`log.Id <= d` skip** is essential — without it you double-apply deltas already in your baseline.
`RaftLog.Id` is the committed index and is authoritative for this comparison.

---

## Checkpoints, compaction, and the retain floor

- **Checkpoint.** Compaction is checkpoint-driven: it deletes P0 entries with
  `Id < min(lastCheckpoint, retainFloor)`. Advancing the checkpoint (e.g. via `ReplicateCheckpoint(0)`)
  is what makes compaction eligible to reclaim space.
- **Retain floor.** `SetMinRetainIndex(0, d + 1)` clamps that boundary so entries above your durable
  local snapshot are never deleted. Passing `0` or a negative value means *no protection* (compaction
  is free to go up to the checkpoint) — it does **not** mean "protect everything."
- **Interaction.** The effective deletion boundary is always the **lower** of the checkpoint and the
  retain floor. You cannot accidentally retain *more* by lowering the checkpoint, nor delete *more* by
  raising the floor.

```
   Ids:  1  2  3  4  5  6  7  8  9  10        checkpoint = 10, local snapshot d = 6
                     └ retain floor = d+1 = 7
   compaction deletes Id < min(10, 7) = 7  →  1..6 removed, 7..10 kept
```

---

## Code map (public API)

Everything you need is on `IRaft` and two small types — no internal Kommander types are involved.

| Symbol | Role |
|---|---|
| `IRaftSystemStateTransfer` | The hook you implement: `ExportPartitionState(int, long, ct)` / `ImportPartitionState(int, Stream, ct)`. |
| `IRaft.RegisterSystemStateTransfer(IRaftSystemStateTransfer?)` | Register (or clear with `null`) the hook. Not replicated — call on every node. |
| `IRaft.ReplicateLogs(0, type, data, …)` | Replicate a delta under your own log type. Returns `RaftReplicationResult`. |
| `RaftReplicationResult.Status` / `.LogIndex` | Success status and the committed index of the entry. |
| `IRaft.OnReplicationReceived` / `OnLogRestored` | `Func<int, RaftLog, Task<bool>>` — live and restore delivery. |
| `RaftLog.Id` | The committed index of the delivered entry — use it to dedup against your local baseline. |
| `IRaft.ReplicateCheckpoint(0, …)` | Advance the P0 checkpoint so compaction can reclaim space. |
| `IRaft.SetMinRetainIndex(0, d + 1)` | Protect un-snapshotted deltas from compaction; in-memory, re-assert on restart. |
| `RaftSystemConfig.SystemPartition` | The system-partition id, `0`. |

---

## Invariants you must not break

1. **`ExportPartitionState` must reflect exactly `upToIndex`.** Include every committed change at or
   below it, nothing above. Kommander seeds the receiver's checkpoint at this index; an off-by-one
   here silently corrupts catch-up.
2. **`ImportPartitionState` must be atomic.** A crash or exception mid-import must leave the prior
   state intact. Kommander only seeds the checkpoint **after** import returns successfully, so a failed
   import is retried cleanly — but only if you did not leave a half-applied state behind.
3. **Register the transfer on every node.** Registration is not replicated. A node that imports without
   the hook registered rejects the snapshot; a node that joins below the compaction floor without it is
   rejected as permanently blocked — its `JoinCluster` fails fast with an `InvalidOperationException`
   rather than converging, so it can never become a voter until the transfer is registered.
4. **Never write the `_RaftSystem` type on P0.** It is reserved; `ReplicateLogs` throws.
5. **Skip restored entries at or below your local snapshot index.** Apply only `RaftLog.Id > d`, or you
   double-apply.
6. **Re-assert the retain floor after every restart.** It is in-memory and defaults to *no protection*.

---

## Testing your integration

Recommended coverage (mirrors Kommander's own tests for this path):

- **Retain floor honored.** Replicate deltas, `SetMinRetainIndex(0, d+1)`, force a compaction pass,
  assert entries `≤ d` are gone and entries `> d` (including the checkpoint) survive.
- **Callback index.** Assert the `RaftLog.Id` seen in `OnReplicationReceived` equals the `LogIndex`
  returned by `ReplicateLogs`.
- **Below-floor live catch-up.** Drive a follower below the P0 floor with the transfer registered;
  assert `ImportPartitionState` fires and the follower's WAL gains a `CommittedCheckpoint`.
- **Cold restart parity.** Persist a local snapshot, compact, restart the node, replay, and assert its
  state matches a peer.
- **No-regression.** With **no** transfer registered, full-snapshot-per-mutation P0 logs and P0
  compaction behave exactly as before — the feature is strictly opt-in.

---

## Glossary

| Term | Meaning |
|---|---|
| **P0 / system partition** | Partition id `0`, replicated to every voter; home for cluster-wide control state. |
| **Delta** | A single-change log entry, as opposed to a full-state snapshot entry. |
| **Full-snapshot-per-mutation** | The compaction-safe-without-a-hook pattern: every write is the entire state. |
| **Compaction floor** | The lowest index still retained: `min(lastCheckpoint, retainFloor)`. Entries below it are deleted. |
| **Checkpoint** | The committed index compaction may reclaim up to; advanced via `ReplicateCheckpoint`. |
| **Retain floor** | `SetMinRetainIndex(0, …)` — a lower clamp on compaction protecting un-snapshotted deltas. |
| **Whole-state transfer** | `IRaftSystemStateTransfer` — export/import the entire P0 application state. |
| **Below the floor** | A node whose needed entries have already been compacted; repairable only by a snapshot. |
| **Local snapshot index (`d`)** | The committed index your durable local snapshot reflects; the dedup boundary on restore. |
