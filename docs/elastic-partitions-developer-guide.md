# Elastic Partitions — Developer Guide

This guide is for contributors who extend or debug elastic partitions. It covers the
partition lifecycle and the coordinator behind it.

---

## Table of Contents

1. [Background: Raft Partitions](#1-background-raft-partitions)
2. [Purpose of Elastic Partitions](#2-purpose-of-elastic-partitions)
3. [Core Data Model](#3-core-data-model)
4. [The System Partition](#4-the-system-partition)
5. [The Coordinator: Single Consumer, All Map Mutations](#5-the-coordinator-single-consumer-all-map-mutations)
6. [Partition Lifecycle: States and Transitions](#6-partition-lifecycle-states-and-transitions)
7. [Creating and Removing Partitions](#7-creating-and-removing-partitions)
8. [Splitting a Partition (Two-Phase Protocol)](#8-splitting-a-partition-two-phase-protocol)
9. [Merging Partitions (Two-Phase Protocol)](#9-merging-partitions-two-phase-protocol)
10. [The Generation Fence](#10-the-generation-fence)
11. [Snapshot Transfer During Split](#11-snapshot-transfer-during-split)
12. [Key-to-Partition Routing](#12-key-to-partition-routing)
13. [Crash Recovery](#13-crash-recovery)
14. [The Partition Map: Serialization and Versioning](#14-the-partition-map-serialization-and-versioning)
15. [Public API Reference](#15-public-api-reference)
16. [Key Invariants and Safety Rules](#16-key-invariants-and-safety-rules)
17. [Adding New Behavior](#17-adding-new-behavior)
18. [Test Organization](#18-test-organization)

---

## 1. Background: Raft Partitions

A **partition** is one independent Raft group with a leader, a write-ahead log (WAL), and a set of
replicated entries. Nodes can lead different partitions, and writes to different partitions proceed
in parallel.

```
Cluster (3 nodes: A, B, C)

  Partition 1          Partition 2          Partition 3
  Leader: Node A       Leader: Node B       Leader: Node C
  Followers: B, C      Followers: A, C      Followers: A, B
  WAL: [1,2,3,...]    WAL: [1,2,...]       WAL: [1,...]
```

---

## 2. Purpose of Elastic Partitions

You can change the partition count at runtime while all nodes remain in service.

- **Create** a partition for a new workload segment.
- **Split** a hot partition into two.
- **Merge** two lightly loaded partitions.
- **Remove** a partition you no longer need.

Each operation changes the **partition map**, which lists the partition ids with their hash ranges
and lifecycle states. The map is committed in the system partition's Raft log and survives crashes.

---

## 3. Core Data Model

### `RaftPartitionRange`

One `RaftPartitionRange` record describes one partition in the map.

```csharp
public sealed class RaftPartitionRange
{
    public int PartitionId { get; set; }      // unique id (1, 2, 3, ...)
    public int StartRange  { get; set; }      // inclusive lower bound of hash range
    public int EndRange    { get; set; }      // inclusive upper bound of hash range
    public long Generation { get; set; }      // incremented on every map mutation touching this entry
    public RaftPartitionState State { get; set; }       // lifecycle state
    public RaftRoutingMode    RoutingMode { get; set; } // how keys map to this partition
    public List<RaftReplica>  Replicas { get; set; }    // hosting nodes; empty = full replication
    public int ReplicationFactor { get; set; }          // voter target; 0 = RaftConfiguration value
}
```

### `RaftPartitionState`

```
Active    — partition is live and accepting writes
Splitting — Phase 1 of a split has committed; waiting for Phase 2
Draining  — Phase 1 of a merge has committed; waiting for Phase 2
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

The map is serialized as a single JSON object.

```csharp
public sealed class RaftPartitionMap
{
    public long MapVersion { get; set; }              // bumped on every write
    public List<RaftPartitionRange> Partitions { get; set; }
}
```

`MapVersion` starts at 1 and increments with each mutation. Nodes use it to detect stale in-memory
copies.

### `Generation` on `RaftPartition`

The live `RaftPartition` object on each node also carries a `Generation`. `StartUserPartitions`
updates it from the map entry, and the executor thread reads it for the
[generation fence](#10-the-generation-fence). The backing field uses `Interlocked` operations,
which makes the value visible across threads on weakly ordered architectures such as ARM64.

---

## 4. The System Partition

**Partition id 0** is the **system partition**. Its Raft log carries configuration entries, and the
partition map is one of them. The map is stored under the key `RaftSystemConfigKeys.Partitions` as a
JSON-serialized `RaftPartitionMap`.

The leader replicates a changed map as a `RaftSystemConfig` log entry in the system partition WAL
and applies it locally after the entry is committed.
[Section 14](#14-the-partition-map-serialization-and-versioning) shows the steps.

Followers receive the committed entry through `OnReplicationReceived` and apply the map with
`StartUserPartitions`.

---

## 5. The Coordinator: Single Consumer, All Map Mutations

All map mutations go through an unbounded `Channel<RaftSystemRequest>` that `RaftSystemCoordinator`
owns. The background loop (`RunAsync`) reads one message at a time and dispatches it to the
matching handler.

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

The single consumer serializes access to the coordinator's dictionaries, such as `_pendingSplits`
and `systemConfiguration`. Handlers need no locks.

**Test hooks:** unit tests fake replication with `ReplicateOverride` and
`ReplicateCheckpointOverride`, and partition activation with `StartPartitionsOverride`. They need no
running cluster.

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

**Draining** is the state of a merge source. Phase 1 bumps its generation, and a write that carries
the earlier generation returns `PartitionMoved`. The survivor remains `Active`. In Phase 2 the
source becomes `Removed` and the survivor absorbs its range.

**Removed** is a tombstone that `StartUserPartitions` skips. The tombstone remains in the serialized
map, which lets a node that missed Phase 2 re-run WAL reclamation on restart.

---

## 7. Creating and Removing Partitions

### Creating

`CreatePartitionAsync(partitionId, mode, hashRange, ct)` is leader only. `HashRange` mode requires a
`(start, end)` range. In `Unrouted` mode the range is ignored.

The coordinator checks these guards before it writes.

- The system partition map exists.
- The partition id is not in the map. An id that is already `Active` returns success (idempotent).
- The partition id is above the map's `HighestPartitionIdEver`, the highest id ever handed out. An
  id whose tombstone is missing is therefore also refused.
- For `HashRange`, `start <= end`.
- For `HashRange`, the range does not overlap an existing `HashRange` entry.
- When `RaftConfiguration.MaxPartitions` > 0, the partition count is below the bound.

On success the coordinator appends the entry with `Generation = 1` and `State = Active`, and
continues with the steps in [section 14](#14-the-partition-map-serialization-and-versioning).

### The partition-count bound

Each partition costs its replicas resources such as a WAL and file handles.
`RaftConfiguration.MaxPartitions` (default `0`, no bound) caps how many data partitions the
committed map may hold.

- The count covers all entries that are not `Removed`, excluding the system partition. `Splitting`
  and `Draining` entries count.
- At the bound, `CreatePartitionAsync` and `SplitPartitionAsync` answer
  `RaftOperationStatus.PartitionLimitReached` with `Success = false`. The check comes after the
  request is validated and before the map changes. The map version and `HighestPartitionIdEver` do
  not advance, and the same id is usable once a slot frees.
- A create of an id that is already `Active` succeeds at the bound. A removal or a merge frees a
  slot.
- The bound does not apply to the bootstrap layout at run time. An `InitialPartitions` above it
  fails `Validate()` at startup.
- The bound is per cluster and the system-partition leader checks it. Configure the same value on
  all nodes, or a leader change will change the effective bound.
- Each refusal is counted in `raft.partitions.create_refused_total{path="create"|"split"}`. The
  first refusal of an episode logs a warning. The episode ends when a later check finds room.

### Removing

`RemovePartitionAsync(partitionId, ct)` requires leadership of the system partition.

- The partition must exist in the map.
- An entry that is already `Removed` is idempotent: WAL reclamation is attempted again and the call
  returns `Success`.
- A `Splitting` or `Draining` entry returns `Errored`. Complete or abort the pending phase first.

On success the coordinator takes these steps.

1. Mark the entry `Removed` and bump `Generation`, then replicate.
2. Drain and stop the live `RaftPartition`, and evict it from `manager.Partitions`.
3. Reclaim the WAL with `DeletePartitionWAL`.
4. Call `StartPartitions`, which fires `OnPartitionMapChanged` on the leader. Followers fire it
   through `ConfigReplicated`.

### WAL Reclamation

`IWAL.DeletePartitionWAL(partitionId)` has one implementation per WAL adapter.

| Adapter       | Implementation |
|---------------|----------------|
| `InMemoryWAL` | Removes the partition's `SortedDictionary` under the write lock |
| `SqliteWAL`   | `DELETE FROM logs WHERE partitionId = @id`; skips opening the DB file if it does not exist; closes and evicts the connection after deletion |
| `RocksDbWAL`  | Iterator scan + `WriteBatch` delete of all keys with the partition prefix |

All three are idempotent and return `Success` for a partition that is already reclaimed.

---

## 8. Splitting a Partition (Two-Phase Protocol)

A split is a **two-phase commit**. At no point is a key range owned by zero partitions or by two.

### Reason for Two Phases

While both partitions are `Splitting`, the committed map records that a split is in progress. After
a crash, the cluster resumes it from that record.

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

After Phase 1 keys already hash to the right partition. The `Splitting` state tells the application
that data movement is in progress.

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

- **Phase 1:** a source that is already `Splitting` returns `Errored`, which prevents a duplicate
  split.
- **Phase 2:** the commit is rejected unless both source and target are `Splitting`, which prevents
  double execution in the crash-recovery path.

### Non-AutoCommit Path

`SplitPartitionAsync` always sets `AutoCommit = true`. `AutoCommit = false` is used internally by
tests and the snapshot transfer path to control sequencing. In that case Phase 2 waits
for `TrySplitPartitionCommit`, which crash recovery can send. There is no public commit API.

### Split Boundary Rules (HashRange)

A `HashRange` split uses these bounds.

```
Source owns [S, E].
SplitBoundary B must satisfy: S < B <= E
  (B > S so source keeps at least one hash value [S, B-1])
  (B <= E so target keeps at least one hash value [B, E])
Source after split:  [S, B-1]
Target after split:  [B,   E]
```

If the caller gives no boundary, the coordinator uses the midpoint `B = S + (E - S) / 2`. A range of
size 1 (`S == E`) cannot be split. Its midpoint equals `S`, which fails the `B > S` check.

### Unrouted Splits

In an `Unrouted` split, source and target both have `StartRange = EndRange = 0` and
`RoutingMode = Unrouted`. The application addresses them by partition id.

---

## 9. Merging Partitions (Two-Phase Protocol)

A merge extends the **survivor**'s hash range over the **source**'s range and removes the source. It
deletes the source WAL and moves no application state. Move that state before you merge.

### Pre-conditions

- Both partitions are `Active` and share the same `RoutingMode`.
- For `HashRange`, they are **adjacent**: `source.EndRange + 1 == survivor.StartRange` or
  `survivor.EndRange + 1 == source.StartRange`. A non-adjacent merge would leave a gap in the hash
  ring.
- The caller is leader of **both** partitions. `MergePartitionsAsync` checks this before it enqueues
  the request.

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

A write with the pre-merge `expectedGeneration` returns `PartitionMoved` from this point. A write
with the fence disabled is not rejected. Committed entries remain readable, and proposals in flight
from before Phase 1 either commit or time out.

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

The **generation fence** protects against write-after-move, where a client that cached a partition
id before a split or merge writes to the wrong partition.

### How It Works

`ReplicateLogs` takes an optional `expectedGeneration`.

```csharp
await raft.ReplicateLogs(
    partitionId:         2,
    type:                "myapp.event",
    data:                payload,
    expectedGeneration:  capturedGeneration);  // 0 = disabled
```

When the value is non-zero, the executor compares it with the live `partition.Generation` before it
batches the entry. On a mismatch the call returns `PartitionMoved` and the WAL is not touched.

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

The check executes on the executor thread, in the serialized drain cycle, for single items and in
`ExecuteBatchAsync`. A check in application code, with `GetPartitionGeneration()` before
`ReplicateLogs`, would have a time-of-check race.

### `GetPartitionGeneration(partitionId)`

Returns the live in-memory generation of a partition, or 0 if the partition does not exist. Read it
before a batch of writes and pass it as `expectedGeneration` to each write. On `PartitionMoved`,
re-read the partition map to find the new partition and retry.

---

## 11. Snapshot Transfer During Split

A split moves no application data. By default the application moves the data from the source to the
target with `ReplicateLogs` calls (the *log-shipping* path). The **snapshot transfer** path exports
the range and imports it once, **on the coordinator node only**. To use it, implement
`IRaftStateMachineTransfer` and call `RegisterStateMachineTransfer`.

The other replicas of the target partition do not receive the imported data. The application must
bring the state to them.

### The Interface

```csharp
public interface IRaftStateMachineTransfer
{
    // Export state for the plan's key range, reflecting AT LEAST everything applied at the
    // given log index (newer committed state may be included — the receiver replays retained
    // entries above the boundary, so re-applying a reflected entry must be a no-op).
    // Return a readable stream. The coordinator disposes the stream after transfer.
    Task<Stream> ExportRange(RaftSplitPlan plan, long upToIndex, CancellationToken ct);

    // Import the stream into the target partition's state machine.
    // Must be atomic (partial apply + crash = pre-import state) and idempotent.
    Task ImportRange(int targetPartitionId, Stream snapshot, CancellationToken ct);
}
```

> **Also outside splits:** when a follower falls below the leader's WAL compaction floor and no
> `IRaftPartitionStateTransfer` is registered, the catch-up path calls `ExportRange` with a
> *boundless* plan (only `TargetPartitionId` set, meaning "the entire partition") and installs it
> with `ImportRange`. If your range transfer cannot serve exports of the entire partition, register
> `IRaftPartitionStateTransfer` (see the replica placement guide). The catch-up path prefers it.

### Transfer Sequence (runs between Phase 1 and Phase 2)

```
Phase 1 committed
(source and target both Splitting, target is empty)
         │
         ▼
  RunSnapshotTransferAsync
         │
  ExportRange(plan, snapshotIndex) ─── failure? ──► return ExportFailed
         │
         ▼
  ImportRange(targetId, snapshot)  ─── failure? ──► return ImportFailed
         │
         ▼
  ReplicateCheckpoint(targetId)    ─── failure? ──► retry once
         │                                │
         ▼                         failure again? LogError, return CheckpointFailed
  checkpoint committed to target WAL
  return Completed (state imported on the coordinator node only)
         │
         ▼
  Phase 2 (AutoCommit path): both Active
```

The coordinator node does not have to lead the target partition. The checkpoint entry carries no
application state.

**Outcome.** A failed transfer does not fail the split, and Phase 2 completes in all cases. Read
`RaftPartitionLifecycleResult.StateTransfer`, and move the state yourself when it is not `Completed`.

| `RaftStateTransferOutcome` | Meaning |
|------|----------------|
| `NotAttempted` | The operation is not a split, or no implementation is registered on the coordinator node. |
| `Completed` | The coordinator node imported the range, and the checkpoint on the target committed. |
| `ExportFailed` | `ExportRange` threw. Nothing was imported. |
| `ImportFailed` | `ImportRange` threw, and **the checkpoint is not attempted**. The import may be partial if the implementation is not atomic. |
| `CheckpointFailed` | The import finished, but the checkpoint failed twice, for example because the coordinator node does not lead the target. An error is logged. |

**Thread safety:** `RegisterStateMachineTransfer` uses `Volatile.Write`, and the coordinator reads
the `StateMachineTransfer` property with `Volatile.Read`.

**Idempotency:** `ImportRange` must be idempotent. A later retry can export and import the same
snapshot again, and the final state must be the same.

### Registering the Transfer Implementation

```csharp
raft.RegisterStateMachineTransfer(new MySnapshotTransfer());

// Deregister:
raft.RegisterStateMachineTransfer(null);
```

Registration is **not replicated**. Register on each node at startup, before `JoinCluster()`.

---

## 12. Key-to-Partition Routing

Application code has two routing methods.

```csharp
// Route by the full key (uses InversePrefixedStaticHash internally)
int partitionId = raft.GetPartitionKey("orders/12345");

// Route by a prefix key (uses SimpleHash internally)
int partitionId = raft.GetPrefixPartitionKey("tenant/acme");
```

Both methods return the `HashRange` partition whose `[StartRange, EndRange]` interval contains the
hash of the key. They search the committed map, which also covers partitions that this node does
not host. A node that has not applied a committed map searches its hosted partitions.

`Unrouted` partitions are not returned. Address them by id.

Both methods throw `RaftException` when no partition covers the hash. This can happen if
all partitions are `Unrouted` or the map is partially applied. The initial `DivideIntoRanges` covers
all of `[0, int.MaxValue]`.

Routing is correct throughout a split. As soon as Phase 1 is applied, keys in the right half of the
source's range hash to the target.

---

## 13. Crash Recovery

System partition replay populates `systemConfiguration` with the last committed map. Crash recovery
then resumes the two-phase operations that were interrupted.

### Recovery Trigger

`RestoreCompleted` signals that WAL replay has finished. The coordinator then calls
`InitializePartitions(crashRecovery: true)`, which takes three steps.

1. Call `StartPartitions` to apply the current map to live `RaftPartition` objects.
2. Reclaim the WAL of `Removed` tombstones, in case the node crashed before `DeletePartitionWAL`
   ran.
3. Find incomplete splits (`Splitting` pairs) and incomplete merges (`Draining` entries), and
   re-enqueue their Phase 2 commits.

### Incomplete Split Recovery

A split is incomplete if a source and target are both in `Splitting`. The coordinator re-enqueues
`SplitPartitionCommit` for the source.

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

In an `Unrouted` split both partitions have `StartRange = EndRange = 0`, and adjacency cannot
identify the pair. Recovery matches any other `Splitting Unrouted` entry as the source.

### Incomplete Merge Recovery

A merge is incomplete if a `Draining` partition has no `_pendingMerges` entry. The coordinator
finds the adjacent `Active` survivor and re-enqueues `MergePartitionCommit`.

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

**Known limitation (Unrouted merge):** with more than one `Active Unrouted` partition, recovery
takes the first one in list order (`FirstOrDefault`), which can differ from the survivor chosen
before the crash. `Unrouted` merge recovery is best-effort until the `Draining` entry records its
survivor (a `MergeSurvivorId` field on `RaftPartitionRange`).

### Cases Without Crash Recovery

`InitializePartitions(crashRecovery: false)` is used in two cases.

- `ConfigReplicated`, a live replication event on a follower.
- `LeaderChanged` as follower, which applies the partition map.

Crash recovery is limited to `RestoreCompleted` and `LeaderChanged` as leader. Followers must not
drive Phase 2 commits.

---

## 14. The Partition Map: Serialization and Versioning

| Location of the map | Who reads it |
|----------|--------------|
| `systemConfiguration["Partitions"]` (in-memory dictionary, coordinator only) | The coordinator mutation handlers (`TrySplitPartition`, `TryCreatePartition`, etc.) |
| System partition WAL (durable) | Read on restart via `ConfigRestored`/`ConfigReplicated` events |

All mutation handlers follow the same pattern.

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

Step 5 is critical. A `CreatePartition` enqueued directly after a `LeaderChanged` is handled before
the `ConfigReplicated` event arrives. If step 5 did not update the cache, that handler would find
`systemConfiguration` empty and return `Errored`.

Step 4 writes the **entire** map, built in step 1 from the local copy. Another P0 leader's entries
can be installed after step 1, when the handler waited on a remote leadership confirmation and the
node lost and won P0 leadership meanwhile. The write would then overwrite them, and the in-order
install guard would refuse them as older than the write. The committed map would go back in time on
all nodes. Two rules prevent this.

- **Committed entries go first.** `ConfigReplicated`, `ConfigRestored` and
  `ConfigCheckpointRestored` deliveries are also queued apart from the channel. The loop installs
  all of them before it dispatches the next handler, whatever their channel position. A handler
  therefore starts on a map that the log has not moved past.
- **Stale-base fence.** `Replicate` first installs any delivery that arrived since the handler was
  dispatched. If it installed one, it proposes no write and returns
  `RaftOperationStatus.StaleSystemConfiguration` for the rest of that dispatch. The fence is
  complete: entries committed before this node's P0 term were all delivered by then. The promotion
  barrier publishes leadership after all prior-term entries are applied, and the applicator
  delivers each system entry before it moves the applied frontier
  (`TestSystemLeaderChangeDeliveryOrder`). Callers stop on that status. The placement pass re-plans
  on its next tick. The split and merge Phase 2 commits and `TrySetInitialPartitions` send
  themselves back through the channel and start over from the installed state.

The remote calls that a handler can wait on from the loop (`GetReadIndex`, `GetFollowerLag`) are
bounded by `RaftConfiguration.RemoteProbeTimeout` (the leadership confirmation timeout plus 3 s).
One dead connection cannot block the loop for minutes. A partition-leadership resolution probes all
its candidates at once under one such budget.

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

`RaftPartitionLifecycleResult` carries `Success`, `Status`, `Generation`, and `StateTransfer`, the
outcome of the [snapshot transfer](#11-snapshot-transfer-during-split) of a split.

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

A violation of these invariants corrupts the partition map.

1. **Single mutation path.** All partition map mutations go through the coordinator channel. Do not
   mutate `systemConfiguration` or the in-memory map from outside the coordinator loop.

2. **No overlapping HashRange entries.** `[StartRange, EndRange]` intervals do not overlap.
   `CreatePartition` validates this, and a split shrinks the source before it adds the target.

3. **No inverted bounds.** `StartRange <= EndRange` for each `HashRange` entry. `CreatePartition`
   validates this.

4. **Splitting pairs are symmetric.** Phase 1 marks both source and target `Splitting` in one
   replicated map write, and Phase 2 marks both `Active` in one. A state where one is `Splitting`
   and the other is `Active` is not valid.

5. **Never remove a `Splitting` or `Draining` partition.** To remove a `Splitting` source orphans
   the target forever. To remove a `Draining` source bypasses `MergePartitionCommit`.
   `TryRemovePartition` returns `Errored` for both.

6. **`Removed` entries are never re-added.** A `Removed` id is not reused, and its tombstone
   persists in the map. `HighestPartitionIdEver` is raised by each create and split in the same
   committed entry and is never lowered. `GetNextAvailablePartitionId` steps past it, and
   create/split refuse an id at or below it, even if the tombstone is lost. Maps written before the
   field existed load it as 0, which disables the floor until the first id is minted on the new
   version.

7. **`OnPartitionMapChanged` fires on leader and followers.** The leader fires it from
   `StartPartitions` at the end of each successful mutation. Followers fire it via
   `ConfigReplicated → InitializePartitions(crashRecovery: false) → StartPartitions`.

8. **Crash recovery runs once, on the right node.** `InitializePartitions(crashRecovery: true)` is
   called on `RestoreCompleted`, and on `LeaderChanged` for the node that just became leader. See
   [Cases Without Crash Recovery](#cases-without-crash-recovery).

9. **`Generation` is visible across threads.** `RaftPartition.Generation` is backed by
   `Interlocked.Read`/`Interlocked.Exchange`. `StateMachineTransfer` is accessed via
   `Volatile.Read`/`Volatile.Write`. A new cross-thread field on `RaftPartition` needs a
   thread-safety mechanism.

---

## 17. Adding New Behavior

### Adding a new lifecycle operation

1. Add a value to `RaftSystemRequestType`.
2. Add a message builder to `RaftSystemRequest` (constructor or property).
3. Add a `case` in `RaftSystemCoordinator.Receive`.
4. Implement the handler as an `async Task TryXxx(...)` method. Follow the pattern: read map →
   validate → mutate → replicate → update cache → `StartPartitions` → resolve TCS.
5. Add a public method on `IRaft` and `RaftManager` that performs leader checks and
   enqueues the request via `systemCoordinator.Send`.
6. Add tests in `Kommander.Tests/Scheduler/` using `ReplicateOverride` and
   `StartPartitionsOverride`.

### Adding a field to `RaftPartitionRange`

1. Add the property to `RaftPartitionRange.cs`.
2. Verify the JSON round-trip. The class uses standard `System.Text.Json`, and a public property is
   sufficient.
3. Update `StartUserPartitions` in `RaftManager` to propagate the new field to the live
   `RaftPartition` object (if applicable).
4. Update `GetPartitionMap()` to include the field in the snapshot.
5. Add a serialization round-trip test in `TestRaftSystemCoordinator`.

### Adding a field to `RaftPartition` that is read cross-thread

Use the same pattern as `Generation`.

```csharp
private long _myField;

public long MyField
{
    get => Interlocked.Read(ref _myField);
    internal set => Interlocked.Exchange(ref _myField, value);
}
```

For reference-type fields, use `Volatile.Read`/`Volatile.Write`, as `_stateMachineTransfer` does.

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

- `WaitForIdleAsync` uses a drain sentinel to wait for the coordinator queue to empty. Prefer it
  over a fixed `Task.Delay`.
- Crash recovery tests use `MakeConfigRestored` + `RaftSystemRequestType.RestoreCompleted` to
  simulate WAL replay. `ConfigReplicated` is a live event and does not start crash recovery.
- Tests that verify a follower does NOT drive Phase 2 record calls through `ReplicateOverride` and
  assert that the call count is zero after `WaitForIdleAsync`.
