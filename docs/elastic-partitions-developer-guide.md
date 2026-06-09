# Elastic Partitions — Developer Guide

**Audience:** contributors new to Kommander who want to understand, extend, or debug
the elastic partition system.

**Scope:** everything from the core Raft data structures through the full lifecycle of
a partition — creation, splitting, merging, and removal — including the coordinator
internals, crash recovery, snapshot transfer, and the generation fence.

---

## Table of Contents

1. [Background: What Is a Raft Partition?](#1-background-what-is-a-raft-partition)
2. [Why Elastic Partitions?](#2-why-elastic-partitions)
3. [Core Data Model](#3-core-data-model)
4. [The System Partition](#4-the-system-partition)
5. [The Coordinator: Single Consumer, All Map Mutations](#5-the-coordinator-single-consumer-all-map-mutations)
6. [Partition Lifecycle: States and Transitions](#6-partition-lifecycle-states-and-transitions)
7. [Creating and Removing Partitions](#7-creating-and-removing-partitions)
8. [Splitting a Partition (Two-Phase Protocol)](#8-splitting-a-partition-two-phase-protocol)
9. [Merging Partitions (Two-Phase Protocol)](#9-merging-partitions-two-phase-protocol)
10. [The Generation Fence](#10-the-generation-fence)
11. [Snapshot Transfer During Split](#11-snapshot-transfer-during-split)
12. [Routing: How Keys Find Their Partition](#12-routing-how-keys-find-their-partition)
13. [Crash Recovery](#13-crash-recovery)
14. [The Partition Map: Serialization and Versioning](#14-the-partition-map-serialization-and-versioning)
15. [Public API Reference](#15-public-api-reference)
16. [Key Invariants and Safety Rules](#16-key-invariants-and-safety-rules)
17. [Adding New Behavior: Where to Start](#17-adding-new-behavior-where-to-start)
18. [Test Organization](#18-test-organization)

---

## 1. Background: What Is a Raft Partition?

Raft is a distributed consensus algorithm. A group of nodes (a "cluster") elect one
**leader** and the rest are **followers**. The leader accepts writes, replicates them to
followers via a **Write-Ahead Log (WAL)**, and only confirms a write to the caller once
a majority of nodes have durably stored it.

In Kommander, a **partition** is one independent Raft group. Each partition has its own
leader, its own WAL, and its own set of replicated log entries. A cluster can run many
partitions simultaneously — each partition is completely independent from the others.

```
Cluster (3 nodes: A, B, C)

  Partition 1          Partition 2          Partition 3
  Leader: Node A       Leader: Node B       Leader: Node C
  Followers: B, C      Followers: A, C      Followers: A, B
  WAL: [1,2,3,...]    WAL: [1,2,...]       WAL: [1,...]
```

Partitions allow you to spread work across the cluster. If your cluster has 3 nodes and
3 partitions, each node can be the leader for one partition, and writes to different
partitions can proceed in parallel without one leader becoming a bottleneck.

---

## 2. Why Elastic Partitions?

A static partition count means you have to guess up front how many partitions your
workload needs. Elastic partitions let you change the partition count at runtime without
restarting any node:

- **Create** a new partition on demand for a new workload segment.
- **Split** an existing partition into two when it becomes a hotspot.
- **Merge** two lightly-loaded partitions to reclaim resources.
- **Remove** a partition that is no longer needed.

All of these operations are coordinated changes to the **partition map** — the
cluster-wide description of which partition IDs exist, what hash ranges they cover, and
what lifecycle state they are in. The map is itself stored in Raft (in the system
partition) so every node eventually learns about every change, and the change is durable
across crashes.

---

## 3. Core Data Model

### `RaftPartitionRange`

Every partition in the map is described by one `RaftPartitionRange` record:

```csharp
public sealed class RaftPartitionRange
{
    public int PartitionId { get; set; }      // unique id (1, 2, 3, ...)
    public int StartRange  { get; set; }      // inclusive lower bound of hash range
    public int EndRange    { get; set; }      // inclusive upper bound of hash range
    public long Generation { get; set; }      // incremented on every map mutation touching this entry
    public RaftPartitionState State { get; set; }       // lifecycle state
    public RaftRoutingMode    RoutingMode { get; set; } // how keys map to this partition
}
```

### `RaftPartitionState`

```
Active    — partition is live and accepting writes
Splitting — Phase 1 of a split has committed; waiting for Phase 2
Draining  — Phase 1 of a merge has committed; partition should reject new writes
Removed   — partition has been logically deleted (tombstone; WAL may be reclaimed)
```

### `RaftRoutingMode`

```
HashRange — keys are routed to this partition based on a hash of the key falling in
            [StartRange, EndRange]. GetPartitionKey() only considers HashRange entries.
Unrouted  — partition exists but is never returned by GetPartitionKey(). Used for
            application-managed partitions that the caller addresses by id directly.
```

### `RaftPartitionMap`

The whole partition map is serialized as a single JSON object:

```csharp
public sealed class RaftPartitionMap
{
    public long MapVersion { get; set; }              // bumped on every write
    public List<RaftPartitionRange> Partitions { get; set; }
}
```

`MapVersion` starts at 1 and increments with every mutation. Nodes use it to detect
stale in-memory copies.

### `Generation` on `RaftPartition`

The live `RaftPartition` object (kept in memory by each node) also carries a
`Generation` property. It is updated from the map entry every time `StartUserPartitions`
runs. The executor thread reads `Generation` for the **generation fence** (see
[Section 10](#10-the-generation-fence)). The field is backed by `Interlocked` operations
to guarantee visibility across threads on ARM64 and other weakly-ordered architectures.

---

## 4. The System Partition

Kommander reserves **partition id 0** as the **system partition**. It is an ordinary
Raft group whose log carries configuration entries instead of application data. The
partition map is one of those configuration entries, stored under the key
`RaftSystemConfigKeys.Partitions` as a JSON-serialized `RaftPartitionMap`.

When the leader receives a write to the partition map (split, merge, create, remove), it:

1. Mutates the in-memory `RaftPartitionMap`.
2. Serializes the updated map back to JSON.
3. Replicates the JSON as a `RaftSystemConfig` log entry in the system partition WAL.
4. Only after the entry is committed (majority of nodes have durably stored it) does it
   apply the change locally.

Followers receive the committed entry via the standard `OnReplicationReceived` callback
path, deserialize the JSON, and call `StartUserPartitions` to apply it.

---

## 5. The Coordinator: Single Consumer, All Map Mutations

`RaftSystemCoordinator` owns a single-consumer, unbounded `Channel<RaftSystemRequest>`.
Every partition map mutation — split, merge, create, remove — is serialized through this
channel. The background loop (`RunAsync`) reads one message at a time and dispatches it
to the appropriate handler.

```
Any thread                      Coordinator loop (single consumer)
─────────────────────────────   ──────────────────────────────────
SplitPartitionAsync()           Receive(SplitPartition)
  └─ systemCoordinator          └─ TrySplitPartition()
       .Send(request)                Phase 1: mutate map, replicate
                                     Phase 2: auto-commit if AutoCommit=true
CreatePartitionAsync()          Receive(CreatePartition)
  └─ systemCoordinator          └─ TryCreatePartition()
       .Send(request)

LeaderChanged event             Receive(LeaderChanged)
  └─ systemCoordinator          └─ TrySetInitialPartitions() / InitializePartitions()
       .Send(request)
```

Because the channel is single-consumer, there are no data races on the in-memory
`_pendingSplits`, `_pendingMerges`, or `systemConfiguration` dictionaries. You do not
need any locks inside the handler methods — the channel provides the serialization
guarantee.

**Test hooks:** `ReplicateOverride`, `StartPartitionsOverride`, and
`ReplicateCheckpointOverride` let unit tests inject fake implementations of replication
and partition activation without running a real Raft cluster. All tests in
`TestRaftSystemCoordinator`, `TestSplitPartition`, `TestMergePartition`, and
`TestPartitionLifecycle` use these hooks.

---

## 6. Partition Lifecycle: States and Transitions

```
                  CreatePartitionAsync
                         │
                         ▼
                      Active ◄──────────────────────────────────┐
                    /        \                                   │
    SplitPartitionAsync       MergePartitionsAsync (source)      │
          │                           │                         │
          ▼                           ▼                   Phase 2 commits
       Splitting                  Draining                      │
          │                           │                         │
    Phase 2 commits            Phase 2 commits                  │
          │                           │                         │
          └──────────► Active ◄───────┘ (survivor absorbs range)
                       (both halves)
                                           RemovePartitionAsync
                                                    │
                                                    ▼
                                                 Removed
                                             (tombstone + WAL reclaimed)
```

**Splitting** involves two partitions at once: the source (shrunk to its left half) and
the new target (owning the right half). Both enter `Splitting` after Phase 1 and both
return to `Active` after Phase 2.

**Draining** is the merge source. It should reject new writes (enforced by the executor
reading `partition.State == Draining` and returning `PartitionMoved`). The survivor
stays `Active` throughout. After Phase 2 the source becomes `Removed` and the survivor
absorbs its range.

**Removed** is a tombstone. It is never re-created in the `Partitions` dictionary — 
`StartUserPartitions` skips `Removed` entries. The tombstone stays in the serialized
map so nodes that missed Phase 2 can re-run WAL reclamation on restart.

---

## 7. Creating and Removing Partitions

### Creating

`CreatePartitionAsync(partitionId, mode, hashRange, ct)` — leader only.

The caller specifies an explicit partition id and routing mode. For `HashRange` mode a
`(start, end)` range is required; for `Unrouted` mode the range is ignored.

**Guards checked by the coordinator before writing:**

- The system partition map must already exist.
- The partition id must not already appear in the map (unless it is `Active` — idempotent
  return in that case).
- For `HashRange`, `start <= end` (no inverted ranges).
- For `HashRange`, the new range must not overlap any existing `HashRange` entry.

On success the coordinator appends the new entry with `Generation = 1`, `State = Active`,
replicates, updates `systemConfiguration`, calls `StartPartitions`, and resolves the
caller's `TaskCompletionSource`.

### Removing

`RemovePartitionAsync(partitionId, ct)` — leader only (requires leadership of the system
partition).

**Guards:**

- The partition must exist in the map.
- If already `Removed` — idempotent: re-attempt WAL reclamation and return `Success`.
- If `Splitting` or `Draining` — rejected with `Errored`. The caller must complete or
  abort the pending phase before removing.

On success the coordinator marks the entry `Removed`, bumps `Generation`, replicates,
drains and stops the live `RaftPartition` object, evicts it from `manager.Partitions`,
reclaims the WAL via `DeletePartitionWAL`, and then calls `StartPartitions` so the
leader fires `OnPartitionMapChanged` (matching the contract followers already satisfy
via `ConfigReplicated`).

### WAL Reclamation

`IWAL.DeletePartitionWAL(partitionId)` is implemented by all three WAL adapters:

| Adapter       | Implementation |
|---------------|----------------|
| `InMemoryWAL` | Removes the partition's `SortedDictionary` under the write lock |
| `SqliteWAL`   | `DELETE FROM logs WHERE partitionId = @id`; skips opening the DB file if it does not exist; closes and evicts the connection after deletion |
| `RocksDbWAL`  | Iterator scan + `WriteBatch` delete of all keys with the partition prefix |

All three are idempotent: calling `DeletePartitionWAL` on an already-reclaimed partition
returns `Success` without error.

---

## 8. Splitting a Partition (Two-Phase Protocol)

Splitting is the most complex operation. It uses a **two-phase commit** to ensure the
cluster never enters a state where the same key range is owned by zero partitions or by
two partitions simultaneously.

### Why Two Phases?

If the leader simply shrank partition 1 and created partition 2 in a single atomic step,
a crash between the two writes would leave the cluster with a gap in the hash ring.
Two-phase commit makes the intermediate state explicit: while both partitions are in
`Splitting`, the cluster knows a split is in progress and can resume it on restart.

### Phase 1 — Mark Both Splitting, Replicate

```
Leader calls SplitPartitionAsync(sourceId=1, plan)
                         │
                         ▼
             TrySplitPartition (coordinator)
                         │
    ┌────────────────────┴──────────────────────────┐
    │  Read partition map from systemConfiguration  │
    │  Validate: Active, no collision, valid range  │
    │  Compute split boundary (caller or midpoint)  │
    │                                               │
    │  Source: shrink EndRange, State=Splitting,    │
    │          Generation++                         │
    │  Target: new entry, State=Splitting,          │
    │          Generation=1                         │
    │  MapVersion++                                 │
    └────────────────────┬──────────────────────────┘
                         │
              Replicate to system partition WAL
              (all nodes receive ConfigReplicated)
                         │
                         ▼
    systemConfiguration updated (local cache)
    StartPartitions called (both partitions visible
    to routing, both in Splitting state)
                         │
                 (optional snapshot transfer)
                         │
                         ▼
       _pendingSplits[sourceId] = (targetId, tcs)
                         │
          AutoCommit=true? → enqueue SplitPartitionCommit
```

After Phase 1, every node knows about both partitions in `Splitting` state. The source's
hash range has been shrunk; the target owns the right half. But both are marked
`Splitting` — routing still works (keys hash to the right partition) but the application
knows data movement is in progress.

### Phase 2 — Commit Both to Active

```
Coordinator receives SplitPartitionCommit(sourceId)
                         │
              TrySplitPartitionCommit
                         │
    ┌────────────────────┴──────────────────────────┐
    │  Lookup _pendingSplits[sourceId] → targetId   │
    │  Verify both source and target are Splitting  │
    │  Set both to Active, Generation++ on each     │
    │  MapVersion++                                 │
    └────────────────────┬──────────────────────────┘
                         │
              Replicate to system partition WAL
                         │
                         ▼
    systemConfiguration updated
    StartPartitions called (both partitions Active)
    Caller's TCS resolved with (Success, generation)
```

### Guard: State Checks Prevent Double-Execution

Before Phase 1: if the source is already `Splitting`, the request is rejected with
`Errored` (duplicate split prevention).

Before Phase 2: if either source or target is not in `Splitting` state, Phase 2 is
rejected. This prevents double-execution in the crash-recovery path.

### Non-AutoCommit Path

When `AutoCommit = false` the caller controls when Phase 2 runs. This allows the
application to do work between phases — for example, copy data from the source to the
target before committing. The caller later triggers Phase 2 by:

```csharp
// Phase 2 is enqueued by re-using the same mechanism internally.
// In the current API, AutoCommit=false means the coordinator waits for
// TrySplitPartitionCommit to be sent, which happens via crash recovery or a future
// explicit commit API.
```

In practice, `SplitPartitionAsync` always sets `AutoCommit = true`. The `AutoCommit =
false` path is used internally by tests and the snapshot transfer path to control
sequencing.

### Split Boundary Rules (HashRange)

For `HashRange` splits:

```
Source owns [S, E].
SplitBoundary B must satisfy: S < B <= E
  (B > S so source keeps at least one hash value [S, B-1])
  (B <= E so target keeps at least one hash value [B, E])
Source after split:  [S, B-1]
Target after split:  [B,   E]
```

If no boundary is provided the coordinator uses the midpoint:
`B = S + (E - S) / 2`

A range of size 1 (`S == E`) cannot be split — the midpoint equals `S`, which fails the
`B > S` check.

### Unrouted Splits

An `Unrouted` split creates a second partition that also has no hash range. Both source
and target have `StartRange = EndRange = 0` and `RoutingMode = Unrouted`. Neither
appears in `GetPartitionKey` results. The application addresses them by partition id.

---

## 9. Merging Partitions (Two-Phase Protocol)

Merging absorbs a **source** partition into a **survivor** partition. The survivor's
hash range is extended to cover the source's former range; the source is removed.

### Pre-conditions

- Both partitions must be `Active`.
- Both must share the same `RoutingMode` (you cannot merge a `HashRange` partition into
  an `Unrouted` one).
- For `HashRange`, they must be **adjacent**: `source.EndRange + 1 == survivor.StartRange`
  or `survivor.EndRange + 1 == source.StartRange`. Non-adjacent merges would leave a gap
  in the hash ring.
- The caller must be leader of **both** partitions. (`MergePartitionsAsync` checks both
  before enqueuing.)

### Phase 1 — Mark Source Draining

```
Leader calls MergePartitionsAsync(survivorId, sourceId)
                         │
                   TryMergePartitions
                         │
    ┌────────────────────┴──────────────────────────┐
    │  Validate adjacency, RoutingMode match        │
    │  Source: State=Draining, Generation++         │
    │  MapVersion++                                 │
    └────────────────────┬──────────────────────────┘
                         │
              Replicate to system partition WAL
                         │
                         ▼
    systemConfiguration updated
    StartPartitions (source now Draining)
    _pendingMerges[sourceId] = (survivorId, tcs)
    Auto-enqueue MergePartitionCommit
```

A `Draining` partition rejects new write proposals with `PartitionMoved`. Existing
committed entries are still readable. In-flight proposals from before Phase 1 will
either commit normally or time out.

### Phase 2 — Absorb and Remove

```
Coordinator receives MergePartitionCommit(sourceId)
                         │
                 TryMergePartitionCommit
                         │
    ┌────────────────────┴──────────────────────────┐
    │  Verify source is still Draining              │
    │  Survivor absorbs source's range:             │
    │    survivor.StartRange = min(both.StartRange) │
    │    survivor.EndRange   = max(both.EndRange)   │
    │  Survivor: Generation++                       │
    │  Source:   State=Removed, Generation++        │
    │  MapVersion++                                 │
    └────────────────────┬──────────────────────────┘
                         │
              Replicate to system partition WAL
                         │
                         ▼
    systemConfiguration updated
    Drain and stop source RaftPartition
    Evict source from manager.Partitions
    DeletePartitionWAL(sourceId)
    StartPartitions (survivor now owns full range,
                     source is Removed tombstone)
    Caller's TCS resolved
```

---

## 10. The Generation Fence

The **generation fence** protects against write-after-move: a client that cached a
partition id before a split or merge should not silently write to the wrong partition
after the map has changed.

### How It Works

Every `ReplicateLogs` call accepts an optional `expectedGeneration` parameter:

```csharp
await raft.ReplicateLogs(
    partitionId:         2,
    type:                "myapp.event",
    data:                payload,
    expectedGeneration:  capturedGeneration);  // 0 = disabled
```

When non-zero, the executor checks the live `partition.Generation` value before batching
the log entry. If the live generation no longer matches, the call returns
`PartitionMoved` immediately without touching the WAL.

```
Caller read generation=5 for partition 2
                │
        (split happens)
        partition 2 generation → 6
                │
Caller calls ReplicateLogs(partitionId=2, expectedGeneration=5)
                │
        Executor: 5 != 6 → return PartitionMoved
```

The check happens **inside the executor** (on the executor's thread, under the serialized
drain cycle) — not on the caller's thread. This avoids the TOCTOU race that would exist
if the caller checked `GetPartitionGeneration()` in application code and then called
`ReplicateLogs` separately.

The check also runs in `ExecuteBatchAsync` (the batching path that groups consecutive
`ReplicateLogs` calls before a single WAL flush), not just in the single-item path.

### `GetPartitionGeneration(partitionId)`

Returns the live in-memory generation for a partition. Returns 0 if the partition does
not exist. Applications read this before initiating a batch of writes, capture it, and
pass it as `expectedGeneration` to each write. If any write returns `PartitionMoved`,
the client re-reads the partition map, finds the new partition(s), and retries.

---

## 11. Snapshot Transfer During Split

By default, after a split the application is responsible for moving data from the source
partition to the target partition via normal `ReplicateLogs` calls (the *log-shipping*
path). For large datasets this can be slow.

An application can register a faster path — **snapshot transfer** — by implementing
`IRaftStateMachineTransfer` and calling `RegisterStateMachineTransfer`.

### The Interface

```csharp
public interface IRaftStateMachineTransfer
{
    // Export state from the source partition up to (and including) the given log index.
    // Return a readable stream. The coordinator disposes the stream after transfer.
    Task<Stream> ExportRange(RaftSplitPlan plan, long upToIndex, CancellationToken ct);

    // Import the stream into the target partition's state machine.
    // Must be atomic (partial apply + crash = pre-import state) and idempotent.
    Task ImportRange(int targetPartitionId, Stream snapshot, CancellationToken ct);
}
```

### Transfer Sequence (runs between Phase 1 and Phase 2)

```
Phase 1 committed
(source and target both Splitting, target is empty)
         │
         ▼
  RunSnapshotTransferAsync
         │
  ExportRange(plan, snapshotIndex) ─── failure? ──► return (log-shipping fallback)
         │
         ▼
  ImportRange(targetId, snapshot)  ─── failure? ──► return (log-shipping fallback)
         │
         ▼
  ReplicateCheckpoint(targetId)    ─── failure? ──► retry once
         │                                │
         ▼                         failure again? LogError, split continues
  checkpoint committed to target WAL
  all target replicas converge on the snapshot state
         │
         ▼
  Phase 2 (AutoCommit path): both Active
```

**Why the checkpoint?** The snapshot was applied locally on the leader's state machine.
Without replicating a checkpoint entry into the target partition's WAL, follower replicas
of the target would have an empty state machine with no log entries to replay. The
checkpoint is the mechanism that makes the snapshot visible to all replicas.

**Error semantics:**

| Step | Failure effect |
|------|----------------|
| `ExportRange` throws | Transfer aborted, fallback to log-shipping. Phase 2 still completes. |
| `ImportRange` throws | Transfer aborted, **checkpoint is not attempted** (nothing was imported). Fallback to log-shipping. |
| Checkpoint fails | Retried once. Second failure logs a critical error: target replicas are diverged. Split still completes (but target data is inconsistent on followers). |

**Thread safety:** `RegisterStateMachineTransfer` uses `Volatile.Write`; the coordinator
reads it with `Volatile.Read` via the `StateMachineTransfer` property. This guarantees
visibility across threads on all architectures.

**Idempotency requirement:** `ImportRange` must be idempotent because in a future retry
scenario (e.g., an extended retry on checkpoint failure) the coordinator may re-export
and re-import. Applying the same snapshot twice must produce the same final state.

### Registering the Transfer Implementation

```csharp
raft.RegisterStateMachineTransfer(new MySnapshotTransfer());

// Deregister (revert to log-shipping):
raft.RegisterStateMachineTransfer(null);
```

Registration is **not replicated** — each node must register independently, typically at
startup before `JoinCluster()`.

---

## 12. Routing: How Keys Find Their Partition

Two routing methods exist for application code:

```csharp
// Route by the full key (uses InversePrefixedStaticHash internally)
int partitionId = raft.GetPartitionKey("orders/12345");

// Route by a prefix key (uses SimpleHash internally)
int partitionId = raft.GetPrefixPartitionKey("tenant/acme");
```

Both methods scan the live `partitions` dictionary and return the first `HashRange`
partition whose `[StartRange, EndRange]` interval contains the hash of the key.

**`Unrouted` partitions are never returned.** An application that creates an `Unrouted`
partition addresses it by its id directly, not through `GetPartitionKey`.

**What if no partition covers the hash?** Both methods throw `RaftException`. This should
not happen in a correctly maintained cluster (the initial `DivideIntoRanges` guarantees
full coverage of `[0, int.MaxValue]`), but can happen if all partitions are `Unrouted` or
the map is in a partially-applied state.

**During a split:** both the (shrunk) source and the (new) target are immediately visible
after Phase 1 and `StartUserPartitions` runs. Keys that previously hashed into the right
half of the source's range now hash into the target. Routing is correct throughout,
including during the `Splitting` intermediate state.

---

## 13. Crash Recovery

The system partition WAL is replayed on every node restart. After replay the coordinator
has `systemConfiguration` populated with the last committed partition map. Crash recovery
detects and resumes any two-phase operations that were interrupted.

### Recovery Trigger

`RestoreCompleted` is the signal that WAL replay has finished. The coordinator calls
`InitializePartitions(crashRecovery: true)`, which:

1. Calls `StartPartitions` to apply the current map to live `RaftPartition` objects.
2. Reclaims WAL for any `Removed` tombstones (in case the previous node crashed before
   `DeletePartitionWAL` ran).
3. Scans for incomplete splits (`Splitting` pairs) and incomplete merges (`Draining`
   entries) and re-enqueues their Phase 2 commits.

### Incomplete Split Recovery

A split is incomplete if both a source and target partition are still in `Splitting`
state. The coordinator re-enqueues `SplitPartitionCommit` for the source:

```
Map after crash:
  Partition 1: Splitting, Generation=2, HashRange [0,499]
  Partition 2: Splitting, Generation=1, HashRange [500,999]

Recovery:
  target P2 has Generation=1 (never committed Phase 2)
  source P1's EndRange + 1 == target P2's StartRange → pair found
  → _pendingSplits[1] = (2, null)
  → Send(SplitPartitionCommit, sourceId=1)
  → TrySplitPartitionCommit runs → both become Active
```

For `Unrouted` splits both partitions have `StartRange = EndRange = 0` so range adjacency
cannot be used. Recovery matches by routing mode instead (any other `Splitting Unrouted`
entry is the source's target).

### Incomplete Merge Recovery

A merge is incomplete if a `Draining` partition has no `_pendingMerges` entry. The
coordinator finds the adjacent `Active` survivor and re-enqueues `MergePartitionCommit`:

```
Map after crash:
  Partition 1: Draining, HashRange [0,499]
  Partition 2: Active,   HashRange [500,999]

Recovery:
  P1.EndRange + 1 == P2.StartRange → P2 is the survivor
  → _pendingMerges[1] = (2, null)
  → Send(MergePartitionCommit, sourceId=1)
  → TryMergePartitionCommit runs → P2 absorbs [0,499], P1 becomes Removed
```

**Known limitation (Unrouted merge):** with multiple `Active Unrouted` partitions, the
survivor is identified by `FirstOrDefault` in list order, which may differ from the
original intent before the crash. The correct fix is to store the survivor's id on the
`Draining` entry itself (a `MergeSurvivorId` field on `RaftPartitionRange`). Until that
field exists, `Unrouted` merge crash recovery is best-effort for multi-partition
scenarios.

### When Does Crash Recovery NOT Run?

`InitializePartitions(crashRecovery: false)` is passed in two cases:

- `ConfigReplicated` — live replication events on followers. Followers must never drive
  Phase 2 commits; only the leader does.
- `LeaderChanged` as follower — a follower learning about a new leader only needs to
  apply the partition map, not run crash recovery.

Crash recovery only runs on `RestoreCompleted` (WAL replay finished) and
`LeaderChanged` as leader (a node winning an election should complete any in-progress
operations).

---

## 14. The Partition Map: Serialization and Versioning

The partition map lives in two places simultaneously:

| Location | Who Reads It |
|----------|--------------|
| `systemConfiguration["Partitions"]` (in-memory dictionary, coordinator only) | All coordinator mutation handlers (`TrySplitPartition`, `TryCreatePartition`, etc.) |
| System partition WAL (durable) | Read on restart via `ConfigRestored`/`ConfigReplicated` events |

Every mutation handler follows the same pattern:

```csharp
// 1. Read from local cache (not WAL)
systemConfiguration.TryGetValue(RaftSystemConfigKeys.Partitions, out string? json);
RaftPartitionMap map = JsonSerializer.Deserialize<RaftPartitionMap>(json);

// 2. Mutate in-memory
map.Partitions[...].State = ...;
map.MapVersion++;

// 3. Serialize
string newJson = JsonSerializer.Serialize(map);
RaftSystemMessage message = new() { Key = ..., Value = newJson };

// 4. Replicate (durable commit)
await Replicate(RaftSystemConfig.RaftLogType, Serialize(message), true, ct);

// 5. Update local cache immediately (so the next coordinator operation sees it
//    without waiting for the ConfigReplicated callback to fire)
systemConfiguration[RaftSystemConfigKeys.Partitions] = newJson;

// 6. Apply to live partitions
StartPartitions(map.Partitions);
```

Step 5 is critical. The coordinator is a single-consumer channel: if the caller enqueues
a `CreatePartition` immediately after a `LeaderChanged`, the `CreatePartition` handler
runs before the `ConfigReplicated` event arrives (which would otherwise update
`systemConfiguration` via the replication callback). Without step 5, the second operation
would find `systemConfiguration` empty and return `Errored`.

---

## 15. Public API Reference

All methods are on `IRaft` (implemented by `RaftManager`).

### Lifecycle Operations (leader only)

```csharp
// Create a new partition. Idempotent if Active already.
Task<RaftPartitionLifecycleResult> CreatePartitionAsync(
    int partitionId,
    RaftRoutingMode mode = RaftRoutingMode.Unrouted,
    (int start, int end)? hashRange = null,
    CancellationToken ct = default);

// Remove a partition. Tombstone persists; WAL is reclaimed.
Task<RaftPartitionLifecycleResult> RemovePartitionAsync(
    int partitionId,
    CancellationToken ct = default);

// Split a partition into two. AutoCommit=true by default (Phase 2 runs automatically).
Task<RaftPartitionLifecycleResult> SplitPartitionAsync(
    int sourcePartitionId,
    int targetPartitionId = 0,     // 0 = auto-assign
    RaftSplitPlan? plan = null,
    CancellationToken ct = default);

// Merge two adjacent partitions. Caller must be leader of both.
Task<RaftPartitionLifecycleResult> MergePartitionsAsync(
    int survivorPartitionId,
    int sourcePartitionId,
    RaftMergePlan? plan = null,
    CancellationToken ct = default);
```

`RaftPartitionLifecycleResult` carries `Success`, `Status`, and `Generation`.

### Read-Only Queries

```csharp
// Current generation for a partition (0 if not found).
long GetPartitionGeneration(int partitionId);

// Snapshot of the current partition map (copy; mutations have no effect).
IReadOnlyList<RaftPartitionRange> GetPartitionMap();

// Routing — return partition id by hashing the key.
int GetPartitionKey(string partitionKey);
int GetPrefixPartitionKey(string prefixPartitionKey);
```

### Replication with Fence

```csharp
// expectedGeneration=0 disables the fence (default).
Task<RaftReplicationResult> ReplicateLogs(
    int partitionId,
    string type,
    byte[] data,
    bool autoCommit = true,
    CancellationToken cancellationToken = default,
    long expectedGeneration = 0);
```

### Events

```csharp
// Fired every time StartUserPartitions applies a new partition map.
// The argument is a point-in-time snapshot; mutating it has no effect.
// On the leader this fires from the coordinator's single-consumer loop.
// On followers it fires from whichever thread processes ConfigReplicated.
event Action<IReadOnlyList<RaftPartitionRange>>? OnPartitionMapChanged;
```

### Snapshot Transfer Registration

```csharp
// Register (or deregister with null). Not replicated — each node registers independently.
void RegisterStateMachineTransfer(IRaftStateMachineTransfer? transfer);
```

---

## 16. Key Invariants and Safety Rules

These invariants must hold at all times. Violation corrupts the partition map.

1. **Single mutation path.** All partition map mutations go through the coordinator
   channel. Never mutate `systemConfiguration` or the in-memory map from outside the
   coordinator loop.

2. **No overlapping HashRange entries.** Two `HashRange` partitions must not have
   overlapping `[StartRange, EndRange]` intervals. Validated on `CreatePartition` and
   enforced by the split boundary logic (source is shrunk before the target is added).

3. **No inverted bounds.** `StartRange <= EndRange` always, for every `HashRange` entry.
   Validated on `CreatePartition`.

4. **Splitting pairs are symmetric.** Phase 1 always marks both source and target
   `Splitting` atomically (in one replicated map write). Phase 2 marks both `Active` in
   one replicated write. There is no valid state where one is `Splitting` and the other
   is `Active`.

5. **Never remove a `Splitting` or `Draining` partition.** Removing a `Splitting`
   source orphans the target forever; removing a `Draining` source bypasses
   `MergePartitionCommit`. Both are rejected by `TryRemovePartition`.

6. **`Removed` entries are never re-added.** Once a partition id enters `Removed` state
   it is never reused or upgraded back to `Active`. The tombstone persists in the map.

7. **`OnPartitionMapChanged` fires on leader and followers.** The leader fires it from
   `StartPartitions` at the end of each successful mutation. Followers fire it via
   `ConfigReplicated → InitializePartitions(crashRecovery: false) → StartPartitions`.

8. **Crash recovery runs once, on the right node.** `InitializePartitions(crashRecovery:
   true)` is called only on `RestoreCompleted` and on `LeaderChanged` for the node that
   just became leader. Followers pass `crashRecovery: false` on `LeaderChanged` to
   prevent them from driving Phase 2 commits.

9. **`Generation` is visible across threads.** `RaftPartition.Generation` is backed by
   `Interlocked.Read`/`Interlocked.Exchange`. `StateMachineTransfer` is accessed via
   `Volatile.Read`/`Volatile.Write`. Never add cross-thread fields to `RaftPartition`
   without a thread-safety mechanism.

---

## 17. Adding New Behavior: Where to Start

### Adding a new lifecycle operation

1. Add a value to `RaftSystemRequestType`.
2. Add a message builder to `RaftSystemRequest` (constructor or property).
3. Add a `case` in `RaftSystemCoordinator.Receive`.
4. Implement the handler as a private `async Task TryXxx(...)` method. Follow the
   pattern: read map → validate → mutate → replicate → update cache → `StartPartitions`
   → resolve TCS.
5. Add a public method on `IRaft` and `RaftManager` that performs leader checks and
   enqueues the request via `systemCoordinator.Send`.
6. Add tests in `Kommander.Tests/Scheduler/` using `ReplicateOverride` and
   `StartPartitionsOverride`.

### Adding a field to `RaftPartitionRange`

1. Add the property to `RaftPartitionRange.cs`.
2. Verify JSON round-trip (the class uses standard `System.Text.Json` with no custom
   converters; adding a public property is sufficient).
3. Update `StartUserPartitions` in `RaftManager` to propagate the new field to the live
   `RaftPartition` object (if applicable).
4. Update `GetPartitionMap()` to include the field in the snapshot.
5. Add a serialization round-trip test in `TestRaftSystemCoordinator`.

### Adding a field to `RaftPartition` that is read cross-thread

Use the same pattern as `Generation`:

```csharp
private long _myField;

public long MyField
{
    get => Interlocked.Read(ref _myField);
    internal set => Interlocked.Exchange(ref _myField, value);
}
```

For reference-type fields, use `Volatile.Read`/`Volatile.Write` (as done for
`_stateMachineTransfer`).

---

## 18. Test Organization

All elastic partition tests are in `Kommander.Tests/Scheduler/`.

| File | What it covers |
|------|----------------|
| `TestRaftSystemCoordinator.cs` | Coordinator fundamentals: `ConfigReplicated`, `LeaderChanged`, `TrySetInitialPartitions`, `DivideIntoRanges`, serialization round-trip, `GetPartitionKey` routing, `TryCreatePartition`, `TryRemovePartition` |
| `TestSplitPartition.cs` | Full two-phase split, boundary arithmetic, guard conditions (Draining/Splitting rejection, collision, out-of-range boundary, single-element partition), Unrouted split, crash recovery (Phase 1 crash + RestoreCompleted), snapshot transfer (happy path, ExportRange failure, ImportRange failure), `crashRecovery=false` on ConfigReplicated and LeaderChanged-follower |
| `TestMergePartition.cs` | Full two-phase merge, adjacency requirement, RoutingMode mismatch rejection, Draining write rejection, survivor generation fence, Phase 1 crash recovery, duplicate merge rejection |
| `TestGenerationFence.cs` | Generation fence on ReplicateLogs (single and batch path), `OnPartitionMapChanged` event contract, concurrent stale-write detection |
| `TestPartitionLifecycle.cs` | Create, remove, WAL reclaim, duplicate-create idempotency — exercised against all three WAL backends (InMemory, SQLite, RocksDB) |
| `WalConformanceTests.cs` | `DeletePartitionWAL` idempotency and boundary isolation |

### Test Conventions

- All tests use `ReplicateOverride` so no real Raft cluster is needed.
- `WaitForIdleAsync` uses a drain sentinel (not `Task.Delay`) to wait for the coordinator
  queue to empty. Always prefer it over fixed delays.
- Crash recovery tests use `MakeConfigRestored` + `RaftSystemRequestType.RestoreCompleted`
  to simulate WAL replay, **not** `MakeConfigReplicated` + `LeaderChanged`. This is the
  correct sequence — `ConfigReplicated` is a live event; `ConfigRestored` is a replay
  event.
- Tests that verify a follower does NOT drive Phase 2 set `ReplicateOverride` to record
  calls and assert the call count is zero after `WaitForIdleAsync`.
