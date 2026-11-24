# Heterogeneous Write Coalescing — Developer Guide

`ReplicateEntries` writes a batch of unrelated records to one partition and returns a separate result for each
record. Each entry has a type of its choice. Use it when your application would otherwise make one
`ReplicateLogs` call per record kind, for example in a key/value store or a transaction coordinator.

---

## Table of contents

1. [Summary](#summary)
2. [`ReplicateEntries` versus `ReplicateLogs`](#replicateentries-versus-replicatelogs)
3. [Batch structure](#batch-structure)
4. [The per-entry generation fence](#the-per-entry-generation-fence)
5. [Flow 1 — A heterogeneous auto-commit batch](#flow-1--a-heterogeneous-auto-commit-batch)
6. [Flow 2 — A trailing manual group](#flow-2--a-trailing-manual-group)
7. [Flow 3 — Per-entry fencing](#flow-3--per-entry-fencing)
8. [Reading the result](#reading-the-result)
9. [Rejections and errors](#rejections-and-errors)
10. [Ordering and durability guarantees](#ordering-and-durability-guarantees)
11. [API reference](#api-reference)
12. [Configuration](#configuration)
13. [Code map](#code-map)
14. [Invariants you must not break](#invariants-you-must-not-break)
15. [Testing](#testing)

---

## Summary

`ReplicateLogs` takes one type and one `autoCommit` flag and returns one ticket for the call. A burst of mixed
records costs one proposal and one replication round trip per kind. `ReplicateEntries` turns the burst into **one
proposal per commit group** and returns a **result array that is index-aligned to the input**.

```
   ReplicateLogs("kv",     [a, b, c])   ─► proposal ─► round trip ─► ticket
   ReplicateLogs("lock",   [d])         ─► proposal ─► round trip ─► ticket
   ReplicateLogs("receipt",[e])         ─► proposal ─► round trip ─► ticket
        three calls, three proposals, three round trips

   ReplicateEntries([ kv:a, kv:b, lock:d, receipt:e, kv:c ])
        one call ─► one auto-commit proposal ─► one round trip ─► per-entry results
```

A batch is a leading auto-commit group plus an optional trailing manual group.

```
   [ auto, auto, auto,  manual, manual ]
     └──── commit together ────┘  └─ one ticket you commit or roll back later ─┘
```

Each entry also has an independent generation fence. An entry that was routed on a stale partition map is dropped
with `PartitionMoved`, and the other entries proceed.

---

## `ReplicateEntries` versus `ReplicateLogs`

| You want to… | `ReplicateLogs` | `ReplicateEntries` |
|---|---|---|
| Write N payloads of the **same** type, all auto-commit | ✅ one call | ✅ (degenerate batch) |
| Write payloads of **different** types in one proposal | ❌ one call per type | ✅ one call |
| Get an **independent result per entry** (index, ticket) | ❌ one ticket for the call | ✅ per-entry array |
| Mix **auto-commit** records with a **manual** prepare in one call | ❌ two calls | ✅ auto prefix + trailing manual group |
| Fence **individual** entries by partition generation | ❌ one fence for the call | ✅ per-entry fence |

> You cannot merge two independent manual tickets, such as the prepares of two transactions, into one call. Each
> needs its own rollback decision, and the append-only log can only truncate a suffix. Use separate proposals. Their
> WAL flushes can coalesce in the scheduler.

---

## Batch structure

A batch is a list of `RaftProposalEntry`.

```csharp
public readonly record struct RaftProposalEntry(
    string Type,               // consumer log type for this entry
    byte[] Data,               // opaque payload
    bool AutoCommit = true,    // true: commits with the batch; false: trailing manual group
    long ExpectedGeneration = 0, // 0: unfenced; non-zero: fence this entry against the partition generation
    long ExpectedTerm = 0      // 0: unfenced; non-zero: the whole batch is refused (TermMismatch) unless the node serves in this term
);
```

The term fence applies to the batch. A stale term means that the caller no longer knows the node's role, and two
different non-zero terms cannot both be current. Read the term with `IRaft.GetPartitionTerm`.

Two shape rules are checked before anything is appended.

1. **All entries target the same partition.**
2. **At most one manual group, and it must be last.** After a manual (`AutoCommit == false`) entry, no later entry
   can be auto-commit. `[auto, manual]`, `[manual, manual]`, and an all-manual batch are valid;
   `[auto, manual, auto]` and `[manual, auto]` are rejected.

A manual ticket is rolled back by **suffix truncation**. If a manual group were in the middle of a batch, the
truncation would also destroy the entries after it.

```
   safe:      [ auto  auto | manual manual ]      rollback truncates only the manual suffix
                            └── truncate here ──►

   forbidden: [ auto | manual | auto ]            truncating the middle manual would take the last auto too
```

---

## The per-entry generation fence

A split or merge bumps the partition's **generation**. The fence catches writes that were routed on an old partition
map.

| `ExpectedGeneration` | Meaning |
|---|---|
| `0` | Unfenced and always admitted. Use it for hash-routed writes that do not depend on the partition map generation. |
| non-zero `N` | Admitted if the partition's current committed generation is `N`. Otherwise this entry is dropped with `PartitionMoved`, and its siblings are unaffected. |

One batch can mix unfenced entries with entries that expect different generations.

```
   partition currently at generation 7

   [ kv(gen 0)   kv(gen 7)   kv(gen 5) ]
        │            │            │
     admitted     admitted     fenced ─► PartitionMoved   (siblings still commit)
```

> **Limit of the fence.** The fence is evaluated when the batch is classified, just before the append. If all admitted
> entries are fenced, the executor checks the generation again, and a split or merge that lands in between fences the
> complete admitted group. If the admitted set also contains hash-routed entries, that second check would fence them
> too. The fence is then best-effort at classification time, as it is for `ReplicateLogs`.

---

## Flow 1 — A heterogeneous auto-commit batch

```csharp
RaftBatchReplicationResult result = await raft.ReplicateEntries(
    partitionId,
    [
        new("kv",      keyValueBytes),
        new("lock",    lockBytes),
        new("receipt", receiptBytes),
        new("kv",      anotherKeyValueBytes),
    ],
    cancellationToken);
```

1. The batch is validated and each entry is classified against the generation fence. These four are unfenced and
   admitted.
2. Kommander creates one auto-commit proposal, and each log has the type of its entry. The cost is one AppendEntries
   fan-out per follower and one group-committed WAL write.
3. When the proposal commits, the results are filled in.

```
   result.Success            = true
   result.Status             = Success
   result.Entries[0]         = { Success, LogIndex=42, Ticket=Zero }   // kv
   result.Entries[1]         = { Success, LogIndex=43, Ticket=Zero }   // lock
   result.Entries[2]         = { Success, LogIndex=44, Ticket=Zero }   // receipt
   result.Entries[3]         = { Success, LogIndex=45, Ticket=Zero }   // kv
```

`LogIndex` is the entry's position in the log. Auto-commit entries carry no ticket (`Ticket == Zero`). On a follower,
the registered restorers dispatch by `Type`, as they do for `ReplicateLogs`.

---

## Flow 2 — A trailing manual group

A batch can end with one manual group that you commit or roll back later, such as a transaction prepare.

```csharp
RaftBatchReplicationResult result = await raft.ReplicateEntries(
    partitionId,
    [
        new("kv",      kvBytes,       AutoCommit: true),
        new("lock",    lockBytes,     AutoCommit: true),
        new("prepare", prepareBytes,  AutoCommit: false),   // trailing manual group
    ],
    cancellationToken);
```

The auto prefix commits with the batch. The manual entries are **durable but not committed**. They are `Pending` and
share one ticket.

```
   result.Entries[0] = { Success, LogIndex=50, Ticket=Zero }        // kv (committed)
   result.Entries[1] = { Success, LogIndex=51, Ticket=Zero }        // lock (committed)
   result.Entries[2] = { Pending, LogIndex=52, Ticket=T }           // prepare (held)
   result.TicketId   = T                                            // the actionable ticket
```

Finish the manual group with the two-phase calls.

```csharp
HLCTimestamp ticket = result.TicketId;

// commit the prepare…
await raft.CommitLogs(partitionId, ticket, cancellationToken);

// …or abandon it — this truncates only the manual suffix, leaving the committed auto prefix intact.
await raft.RollbackLogs(partitionId, ticket, cancellationToken);
```

The manual entries sit above a committed prefix, and a rollback of the ticket cannot touch the committed auto entries.

---

## Flow 3 — Per-entry fencing

A stale-generation entry does not fail the batch.

```csharp
RaftBatchReplicationResult result = await raft.ReplicateEntries(
    partitionId,
    [
        new("kv", freshBytes, ExpectedGeneration: 0),   // unfenced → admitted
        new("kv", staleBytes, ExpectedGeneration: 5),   // partition no longer at gen 5 → fenced
    ],
    cancellationToken);

// result.Success    == true                            (a sibling was admitted)
// result.Entries[0] == { Success,       LogIndex=60, Ticket=Zero }
// result.Entries[1] == { PartitionMoved, LogIndex=-1, Ticket=Zero }
```

The fenced entry has `LogIndex == -1` in its input slot. If all entries are fenced, the batch returns
`PartitionMoved` with `Success == false`. Refresh your partition map and retry.

---

## Reading the result

```csharp
public sealed class RaftBatchReplicationResult
{
    public bool Success { get; }                       // every admitted group reached quorum
    public RaftOperationStatus Status { get; }         // overall status
    public HLCTimestamp TicketId { get; }              // the manual ticket, else the auto ticket; Zero on failure
    public IReadOnlyList<RaftEntryResult> Entries { get; } // index-aligned to the input list
}

public readonly record struct RaftEntryResult(
    RaftOperationStatus Status,
    long LogIndex,        // assigned index, or -1 on a failure status
    HLCTimestamp Ticket   // shared manual ticket for a manual entry, else Zero
);
```

| `RaftEntryResult.Status` | Meaning | `LogIndex` | `Ticket` |
|---|---|---|---|
| `Success` | Auto-commit entry, appended and committed | its index | `Zero` |
| `Pending` | Trailing manual entry, durable but not yet committed | its index | shared manual ticket |
| `PartitionMoved` | Fenced out; not appended | `-1` | `Zero` |
| any other | The batch was rejected, or the proposal failed in flight | `-1` | `Zero` |

`Success == true` means that each admitted group reached quorum. Individual entries may be `PartitionMoved`, and you
should inspect each one. After `ProposalOutcomeUnknown` or `ProposalTimeout`, the entry can be in the log and can commit
later, although its `LogIndex` is `-1`. `Entries[i]` always describes the input entry at position `i`.

---

## Rejections and errors

When a batch is rejected or all its entries are fenced out, no entry is appended and there is no partial state to clean
up.

| Condition | Result |
|---|---|
| Empty batch | `Success == true`, no entries — a no-op |
| Auto-commit entry after a manual entry (bad shape) | `Success == false`, `Status == Errored`, all slots `Errored`/`LogIndex -1` |
| Stale `ExpectedTerm`, or two different non-zero ones | `Success == false`, `Status == TermMismatch` |
| All entries fenced out | `Success == false`, `Status == PartitionMoved`, all slots `PartitionMoved` |
| Some entries fenced, some admitted | `Success == true`, fenced slots `PartitionMoved`, admitted slots `Success`/`Pending` |
| Reserved system log type on the system partition | Throws `RaftException` before any append |
| Not the leader | `Success == false`, `Status == NodeIsNotLeader`; that group was not appended |
| Lost leadership mid-flight | `Success == false`; the affected slots carry the failure status, for example `ProposalOutcomeUnknown` |
| Only the manual group fails | `Success == false`, `TicketId == Zero`; the auto slots are `Success` and committed |

---

## Ordering and durability guarantees

**Auto commits before manual is proposed.** The two groups are separate proposals, because the log commits or rolls
back a proposal as a unit. The order is a safety requirement. If the groups were posted together and the auto group
failed to commit after the manual propose was durable, a later `CommitLogs` on the manual ticket could advance the
commit index past the uncommitted auto entries.

**Flush coalescing is opportunistic.** An auto-only batch rides one group-committed write. A batch with a manual
group has two writes, and they coalesce if the scheduler merges them with concurrent traffic. `ReplicateEntries` reduces
proposals and round trips, and the scheduler's floor on syncs stays the same. For more information, see the
[WAL Write Durability guide](wal-commit-durability-developer-guide.md).

**No ordering across entries beyond "auto before manual".** Do not assume any order between two entries in the same
group.

---

## API reference

The method is on `IRaft`, and the supporting types are in `Kommander/Data/`.

```csharp
Task<RaftBatchReplicationResult> ReplicateEntries(
    int partitionId,
    IReadOnlyList<RaftProposalEntry> entries,
    CancellationToken cancellationToken = default);
```

Finish a manual group with these calls. Pass `RaftBatchReplicationResult.TicketId`, or the `RaftEntryResult.Ticket` of
any manual entry.

```csharp
Task<(bool success, RaftOperationStatus status, long commitLogId)> CommitLogs(
    int partitionId, HLCTimestamp ticketId, CancellationToken cancellationToken = default);

Task<(bool success, RaftOperationStatus status, long commitLogId)> RollbackLogs(
    int partitionId, HLCTimestamp ticketId, CancellationToken cancellationToken = default);
```

---

## Configuration

`ReplicateEntries` adds no configuration. The WAL-scheduler settings apply, and the
[WAL Write Durability guide](wal-commit-durability-developer-guide.md) explains how they interact.

| Setting | Default | Relevance to batches |
|---|---|---|
| `WalGroupCommitLingerMs` | `0` | `> 0` lets the scheduler linger to gather more ready writes into one `fsync`, raising coalescing density for a manual group riding a separate flush. |
| `MaxWalGroupBatchPartitions` | `64` | Max partitions coalesced into one `walAdapter.Write`. |
| `MaxWalBatchSize` | `256` | Max operations drained from one partition per write — bounds how large a single proposal's write can be. |

---

## Code map

```
Kommander/
├─ IRaft.cs                        ReplicateEntries declaration
├─ ReplicationGateway.cs           ReplicateEntries — the orchestration:
│     ├─ shape validation           auto-prefix + single trailing manual group; reserved-type guard
│     ├─ per-entry fence            classify each entry vs partition.Generation; PartitionMoved slots
│     ├─ auto group                 propose (autoCommit:true) + WaitForQuorum → commit
│     ├─ manual group               propose (autoCommit:false) + WaitForQuorum → propose-quorum durable
│     └─ RejectBatch / FailBatch    batch-level and in-flight failure results
├─ RaftPartition.cs                ProposeEntriesAsync(List<RaftLog>, expectedGeneration, autoCommit, expectedTerm)
│                                   — posts one ReplicateLogs executor request per group
├─ Data/
│     ├─ RaftProposalEntry.cs       input entry (Type, Data, AutoCommit, ExpectedGeneration, ExpectedTerm)
│     ├─ RaftEntryResult.cs         per-entry outcome (Status, LogIndex, Ticket)
│     └─ RaftBatchReplicationResult.cs   batch outcome (Success, Status, TicketId, Entries)
└─ Consensus/LogReplicator.cs      the proposal/commit/rollback path both groups reuse

Kommander.Tests/
└─ TestReplicateEntries.cs         heterogeneous commit + restore-by-type on a follower;
                                    contiguous per-entry indices; trailing manual commit + rollback;
                                    auto-after-manual shape rejection; per-entry fencing (mixed,
                                    interleaved with the manual split, all-fenced)
```

---

## Invariants you must not break

- **One manual group, last.** The manual group must remain a single contiguous suffix. In any other position it is not
  truncation-safe.
- **Auto commits before manual is proposed.** Never post the two groups concurrently. The sequential order stops a
  manual `CommitLogs`/`RollbackLogs` from reaching the auto entries.
- **Results stay index-aligned to the input.** All code paths must leave `Entries[i]` describing input entry `i`. Fenced
  and failed slots carry `LogIndex == -1`.
- **A batch-level rejection appends nothing.** Shape and reserved-type checks run before any append.
- **Per-entry `LogIndex` comes from the assigned log id.** It is read back off the same `RaftLog` instances after the
  propose reply resolves. Do not derive it from a single commit index.

---

## Testing

`Kommander.Tests/TestReplicateEntries.cs` uses a real two-node in-memory cluster.

- **Heterogeneous auto-commit** — a mixed-type batch commits as one proposal, the indices are contiguous, and the
  follower restores each entry under its type.
- **Trailing manual group** — the manual entries are `Pending` under one ticket. A commit commits the suffix. A rollback
  marks the suffix rolled back, and the committed auto prefix survives.
- **Shape rejection** — an auto-commit entry after a manual entry is rejected before any append.
- **Per-entry fence** — a mixed batch appends the admitted entries. A fenced entry interleaved with the manual split does
  not disturb either group. An all-fenced batch returns `PartitionMoved`.

When you change this path, run the full test project. The two-node flows are timing-sensitive, and the path is shared
with scheduling, WAL, and cluster coordination.

---

*Related: [Architecture Overview](architecture-overview.md),
[WAL Write Durability & Single-Fsync Commit Guide](wal-commit-durability-developer-guide.md), and
[Elastic Partitions Developer Guide](elastic-partitions-developer-guide.md) for how a partition's generation changes.*
