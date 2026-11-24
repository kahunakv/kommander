# System-Partition State Snapshots (P0 Delta Logs) — Developer Guide

You can store **cluster-wide control state** on the system partition (P0) and replicate one small
**delta** per mutation. This guide shows how to do it and survive log compaction. It is for developers
who integrate an application state machine with Kommander.

---

## Table of contents

1. [Summary](#summary)
2. [Background: the system partition](#background-the-system-partition)
3. [Delta repair path](#delta-repair-path)
4. [The consumer contract (five steps)](#the-consumer-contract-five-steps)
5. [Flow 1 — Steady-state delta replication](#flow-1--steady-state-delta-replication)
6. [Flow 2 — Repairing a below-floor follower](#flow-2--repairing-a-below-floor-follower)
7. [Flow 3 — Cold restart from a local snapshot](#flow-3--cold-restart-from-a-local-snapshot)
8. [Checkpoints, compaction, and the retain floor](#checkpoints-compaction-and-the-retain-floor)
9. [Code map (public API)](#code-map-public-api)
10. [Invariants you must not break](#invariants-you-must-not-break)
11. [Testing your integration](#testing-your-integration)

---

## Summary

P0's log is **compacted**: a background pass deletes entries older than the last checkpoint. If each
mutation replicates a full snapshot of your state, any surviving entry is complete, but each mutation
costs O(state size). A frequent small change, such as a lease renewal, then dominates WAL and network
traffic.

Deltas cost one entry per change. However, a delta is **not** self-contained. A node that needs a
compacted delta cannot rebuild the state from the deltas that remain. To close that gap, you implement
`IRaftSystemStateTransfer`, which exports your full state as of a committed index and imports such a
state atomically.

```
   FULL-SNAPSHOT-PER-MUTATION            DELTA + WHOLE-STATE SNAPSHOT HOOK
   every write ships the entire state    every write ships one changed entry
   O(state) per mutation                 O(1) per mutation
   self-repairing (any entry complete)   repaired on demand via ExportPartitionState
```

The contract has five steps: **implement** the transfer, **register** it on all nodes, **replicate**
deltas, **persist** a local snapshot tagged with its index, and **advance the retain floor**.

---

## Background: the system partition

The system partition has id `0` (`RaftSystemConfig.SystemPartition`), is replicated to all voters, and
carries two kinds of entries.

- **Reserved coordinator entries** under the log type `_RaftSystem`, such as partition maps and the
  membership roster. **You may not write this type**: on P0, `ReplicateLogs` throws
  *"System log type is reserved on the system partition."*
- **Application entries** under **your log type string**. They are committed on P0 and delivered to your
  `OnReplicationReceived` (live) and `OnLogRestored` (restore) callbacks, as on a normal partition.

---

## Delta repair path

A node can fall **below the compaction floor**, which means that it needs entries that were deleted, in
two ways.

1. **A live follower fell far behind.** The leader compacted past the range that the follower asks for.
   Log backfill returns empty (it ships contiguous ranges only), and the follower needs a snapshot.
2. **A node restarts cold** with a WAL that was compacted locally. The deltas that would rebuild its
   state are gone from its disk.

Two mechanisms repair these cases.

**On-demand state repair** covers the live follower. When a system-state transfer is registered, the
leader calls your `ExportPartitionState(0, checkpointIndex, ct)` and streams the blob in chunks, and the
follower calls your `ImportPartitionState(0, stream, ct)`. Kommander then seeds a `CommittedCheckpoint`
at that index, and normal replication resumes from `index + 1`. You write no transport code.

**The retain floor** covers the cold restart. `SetMinRetainIndex(0, index)` tells compaction to keep all
P0 entries from `index` up. You advance it when you persist a local snapshot. The deltas that are
**not yet** in a durable local snapshot are then on disk for replay.

When the cluster is reachable, a cold-restarted node below the floor can be repaired by the leader's
snapshot alone. The retain floor lets a node **recover from local disk** and avoids a full snapshot
transfer on each restart.

---

## The consumer contract (five steps)

### Step 1 — Implement `IRaftSystemStateTransfer`

```csharp
public sealed class MyP0StateTransfer : IRaftSystemStateTransfer
{
    private readonly MyStateMachine _state;

    // Export the WHOLE application state as of committed index `upToIndex`.
    // The blob must reflect AT LEAST the state after `upToIndex` is applied; newer committed
    // state may be included — the receiver replays retained log entries above the boundary,
    // so re-applying an already-reflected entry must be a no-op.
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

`partitionId` will be `0`. Kommander uses `upToIndex` to seed the follower's checkpoint, and a blob that
does not reflect that committed index will corrupt catch-up.

### Step 2 — Register it on **every** node

Registration is **not replicated**. Register the transfer on all nodes, leaders and followers, current
and future, because any node can become the exporter or the importer.

```csharp
raft.RegisterSystemStateTransfer(new MyP0StateTransfer(state));
```

Registration tells Kommander that your P0 logs may be deltas, and lets it repair a node below the floor
after compaction. Pass `null` to clear it. It is independent of `RegisterStateMachineTransfer` (which
covers key-range split and merge), and you can register both.

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

`r.LogIndex` is the committed index of the entry. Followers see the same value as `RaftLog.Id` in their
replication callbacks.

### Step 4 — Persist a local snapshot tagged with its index

Periodically write your in-memory state to durable local storage, and **record the committed index it
reflects** (call it `d`). On restart, load it as the baseline and replay the deltas above `d`. You own
this storage.

### Step 5 — Advance the retain floor

After a local snapshot through index `d` is durable, allow compaction to delete P0 deltas at or below
`d`.

```csharp
raft.SetMinRetainIndex(0, d + 1);   // keep entries with Id > d for offline replay
```

The floor is **in memory and resets on process restart**. Set it again each time the node starts, after
you load the local snapshot and before you rely on recovery from local disk.

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

Compaction bounds WAL growth, and the retain floor protects the tail `(d, max]` that is not in a local
snapshot.

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

You implement the two endpoints, and Kommander drives the rest of the exchange.

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

If the local WAL was compacted above `d` (for example, the retain floor was not set in time), the node
rejoins **below the floor** and is repaired by the leader's snapshot, as in
[Flow 2](#flow-2--repairing-a-below-floor-follower).

The **`log.Id <= d` skip** is mandatory. If you omit it, you apply deltas that are already in your
baseline a second time.

---

## Checkpoints, compaction, and the retain floor

- **Checkpoint.** Compaction deletes P0 entries with `Id < min(lastCheckpoint, retainFloor)`. Advance
  the checkpoint, for example with `ReplicateCheckpoint(0)`, to make space reclaimable.
- **Retain floor.** `SetMinRetainIndex(0, d + 1)` protects entries above your durable local snapshot.
  `0` or a negative value means *no protection*: compaction can go up to the checkpoint.
- **Interaction.** The deletion boundary is always the **lower** of the checkpoint and the retain floor.
  If you raise the floor, compaction does not go past the checkpoint.
- **Composable holds.** `SetMinRetainIndex` is a single floor and the last write wins, which means that
  two independent consumers overwrite each other. When more than one consumer must protect a prefix
  (for example, a PITR-horizon updater *and* a backup driver), use `IRaft.AcquireRetentionHold(0, index)`.
  It returns an `IDisposable`, and `Dispose` releases exactly that hold. The effective retain floor is
  the **minimum across all active holds**, composed with `SetMinRetainIndex` and the checkpoint. Like the
  floor, they are in memory, and you must acquire them again after a restart.

```
   Ids:  1  2  3  4  5  6  7  8  9  10        checkpoint = 10, local snapshot d = 6
                     └ retain floor = d+1 = 7
   compaction deletes Id < min(10, 7) = 7  →  1..6 removed, 7..10 kept
```

---

## Code map (public API)

| Symbol | Role |
|---|---|
| `IRaftSystemStateTransfer` | The hook you implement: `ExportPartitionState(int, long, ct)` / `ImportPartitionState(int, Stream, ct)`. |
| `IRaft.RegisterSystemStateTransfer(IRaftSystemStateTransfer?)` | Register (or clear with `null`) the hook on each node. |
| `IRaft.ReplicateLogs(0, type, data, …)` | Replicate a delta under your log type. Returns `RaftReplicationResult`. |
| `RaftReplicationResult.Status` / `.LogIndex` | Success status and the committed index of the entry. |
| `IRaft.OnReplicationReceived` / `OnLogRestored` | `Func<int, RaftLog, Task<bool>>` — live and restore delivery. |
| `RaftLog.Id` | The committed index of the delivered entry. Compare it with `d` on restore. |
| `IRaft.ReplicateCheckpoint(0, …)` | Advance the P0 checkpoint. |
| `IRaft.SetMinRetainIndex(0, d + 1)` | Protect deltas above the local snapshot. Single floor, in memory. |
| `IRaft.AcquireRetentionHold(0, index)` | Composable retain floor for concurrent consumers. |
| `RaftSystemConfig.SystemPartition` | The system-partition id, `0`. |

---

## Invariants you must not break

1. **`ExportPartitionState` must cover `upToIndex`.** Include each committed change at or below it. A
   snapshot that reflects *less* silently corrupts catch-up. Newer committed state is allowed, because
   an MVCC store that snapshots by timestamp cannot cut an exact as-of-index export. Kommander seeds the
   checkpoint at this index and replays the retained log entries above it, and your apply must be
   idempotent for entries that the snapshot already reflects.
2. **`ImportPartitionState` must be atomic.** A crash or exception mid-import must leave the prior state
   intact. Kommander seeds the checkpoint **after** import returns successfully. A failed import can be
   retried if it did not leave a half-applied state behind.
3. **Register the transfer on every node.** A node that receives a snapshot and has not registered the
   hook does not install it. A node that joins below the compaction floor in that condition is
   permanently blocked: its `JoinCluster` fails fast with an `InvalidOperationException`, and it cannot
   become a voter until the transfer is registered.
4. **Do not write the `_RaftSystem` type on P0.** `ReplicateLogs` throws.
5. **Skip restored entries at or below your local snapshot index.** Apply only `RaftLog.Id > d`.
6. **Set the retain floor again after each restart.** It defaults to *no protection*.

---

## Testing your integration

These tests mirror Kommander's tests for this path.

- **Retain floor.** Replicate deltas, call `SetMinRetainIndex(0, d+1)`, and force a compaction pass.
  Assert that entries `≤ d` are gone and entries `> d` (including the checkpoint) survive.
- **Callback index.** Assert that the `RaftLog.Id` in `OnReplicationReceived` equals the `LogIndex` that
  `ReplicateLogs` returned.
- **Below-floor live catch-up.** Drive a follower below the P0 floor with the transfer registered. Assert
  that `ImportPartitionState` fires and the follower's WAL gains a `CommittedCheckpoint`.
- **Cold restart parity.** Persist a local snapshot, compact, restart the node, and assert that its state
  matches a peer after replay.
- **No regression.** When **no** transfer is registered, full-snapshot-per-mutation P0 logs and P0
  compaction behave as before. The feature is opt-in.
