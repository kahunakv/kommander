# Heterogeneous Write Coalescing — Developer Guide

This guide explains `ReplicateEntries`, the API that lets a consumer hand Kommander a **single batch of
unrelated, per-entry-typed writes** for one partition and pay the per-proposal and per-round-trip cost once —
while still getting an **independent result for every entry**. It is written for two readers: a developer
building a consumer (a key/value store, a lock service, a transaction coordinator) who wants to stop making
one `ReplicateLogs` call per record, and a developer extending the write path who needs the exact ordering,
fencing, and durability rules. No prior Raft expertise is assumed; we build the idea up first, then walk the
flows, the result contract, and the code.

---

## Table of contents

1. [Summary](#summary)
2. [Why a second write API](#why-a-second-write-api)
3. [The shape of a batch](#the-shape-of-a-batch)
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
16. [Glossary](#glossary)

---

## Summary

The single-type write surface, `ReplicateLogs`, wraps every payload in one `type`, one `autoCommit` flag,
and returns one ticket for the whole call. A consumer that produces a burst of *unrelated* writes at once —
key/value records, lock records, receipts, plus a prepare for a two-phase transaction — must therefore make
one call per kind, each its own proposal and its own replication round trip.

`ReplicateEntries` accepts a **heterogeneous** batch: each entry carries its own type, its own
auto-commit/manual choice, and its own generation fence. Kommander turns the batch into **one proposal per
commit group** and returns a **per-entry result array, index-aligned to the input**.

```
   ReplicateLogs("kv",     [a, b, c])   ─► proposal ─► round trip ─► ticket
   ReplicateLogs("lock",   [d])         ─► proposal ─► round trip ─► ticket
   ReplicateLogs("receipt",[e])         ─► proposal ─► round trip ─► ticket
        three calls, three proposals, three round trips

   ReplicateEntries([ kv:a, kv:b, lock:d, receipt:e, kv:c ])
        one call ─► one auto-commit proposal ─► one round trip ─► per-entry results
```

A batch is a **leading auto-commit group** plus an **optional single trailing manual group**:

```
   [ auto, auto, auto,  manual, manual ]
     └──── commit together ────┘  └─ one ticket you commit or roll back later ─┘
```

Every entry also carries an independent **generation fence**: an entry routed on a stale partition map is
dropped (reported `PartitionMoved`) while its siblings proceed.

---

## Why a second write API

`ReplicateLogs` is the right tool when every payload in a call shares one type and one fate. It stays, and is
unchanged. `ReplicateEntries` exists for the case it cannot express:

| You want to… | `ReplicateLogs` | `ReplicateEntries` |
|---|---|---|
| Write N payloads of the **same** type, all auto-commit | ✅ one call | ✅ (degenerate batch) |
| Write payloads of **different** types in one proposal | ❌ one call per type | ✅ one call |
| Get an **independent result per entry** (index, ticket) | ❌ one ticket for the call | ✅ per-entry array |
| Mix **auto-commit** records with a **manual** prepare in one call | ❌ two calls | ✅ auto prefix + trailing manual group |
| Fence **individual** entries by partition generation | ❌ one fence for the call | ✅ per-entry fence |

The win is **fewer proposals and fewer replication round trips**, plus a denser group commit. It is *not*
net-new `fsync` savings the WAL scheduler cannot already achieve — see
[Ordering and durability guarantees](#ordering-and-durability-guarantees).

> `ReplicateEntries` does **not** let you merge two *independent* manual tickets (for example prepares from
> two different transactions) into one call. Each needs its own rollback decision, and the append-only log
> can only truncate a suffix — so at most one manual group, and it must come last. Those remain separate
> proposals whose WAL flushes still coalesce opportunistically in the scheduler.

---

## The shape of a batch

You describe the batch as a list of `RaftProposalEntry`:

```csharp
public readonly record struct RaftProposalEntry(
    string Type,               // consumer log type for this entry
    byte[] Data,               // opaque payload
    bool AutoCommit = true,    // true: commits with the batch; false: trailing manual group
    long ExpectedGeneration = 0 // 0: unfenced; non-zero: fence this entry against the partition generation
);
```

Two shape rules are enforced **before anything is appended** (violations append nothing — no partial state):

1. **All entries target the same partition.** The batch is per partition, like every proposal.
2. **At most one manual group, and it must be last.** Once a manual (`AutoCommit == false`) entry appears, no
   later entry may be auto-commit. `[auto, manual]`, `[manual, manual]`, and an all-manual batch are valid;
   `[auto, manual, auto]` and `[manual, auto]` are rejected.

Rule 2 exists because a manual ticket is rolled back by **suffix truncation**. If a manual group could sit in
the *middle* of a batch, truncating it would also destroy every entry appended after it. Forcing the manual
group to the end keeps it a clean, truncatable suffix.

```
   safe:      [ auto  auto | manual manual ]      rollback truncates only the manual suffix
                            └── truncate here ──►

   forbidden: [ auto | manual | auto ]            truncating the middle manual would take the last auto too
```

---

## The per-entry generation fence

When a partition splits or merges, its **generation** is bumped. A consumer that routed a write using an old
partition map is writing to a key range that may have moved. The generation fence rejects such stale writes.

Each entry sets `ExpectedGeneration` independently:

- **`0`** — unfenced. Use for hash-routed writes that do not depend on the partition map generation. Always
  admitted.
- **non-zero `N`** — fenced. Admitted only if the partition's current committed generation is still `N`;
  otherwise this entry is dropped and reported `PartitionMoved`, **and its siblings are unaffected**.

Because the fence is per entry, one batch may freely mix hash-routed (`0`) entries with fenced key-range
entries, and even entries expecting *different* non-zero generations. Only entries whose fence misses are
dropped.

```
   partition currently at generation 7

   [ kv(gen 0)   kv(gen 7)   kv(gen 5) ]
        │            │            │
     admitted     admitted     fenced ─► PartitionMoved   (siblings still commit)
```

> **Residual to know about.** The fence is evaluated against the partition generation at the moment the batch
> is classified, just before the append. For an *unambiguous key-range batch* (every admitted entry
> non-zero, none hash-routed), the classify→append window is closed with an executor-side backstop, so a
> split/merge landing mid-flight fences the whole admitted group. A **mixed** admitted set (hash-routed +
> key-range in the same batch) cannot use that backstop without also fencing the hash-routed entries, so its
> fence is best-effort at classification time. This matches the best-effort nature of the single-type fence.

---

## Flow 1 — A heterogeneous auto-commit batch

The common, highest-value case: many auto-commit records of mixed types committed together.

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

What happens:

1. The batch is validated (shape, reserved-type) and each entry is classified against the generation fence.
   All four are unfenced here, so all are admitted.
2. Kommander builds **one** `RaftLog` list — each log keeping its own type — and creates **one** auto-commit
   proposal. That is one AppendEntries fan-out per follower and one group-committed WAL write.
3. When the proposal commits, every entry's result is filled in:

```
   result.Success            = true
   result.Status             = Success
   result.Entries[0]         = { Success, LogIndex=42, Ticket=Zero }   // kv
   result.Entries[1]         = { Success, LogIndex=43, Ticket=Zero }   // lock
   result.Entries[2]         = { Success, LogIndex=44, Ticket=Zero }   // receipt
   result.Entries[3]         = { Success, LogIndex=45, Ticket=Zero }   // kv
```

Each entry's `LogIndex` is its assigned position in the log; auto-commit entries carry no caller-held ticket
(`Ticket == Zero`). On a follower, the entries are ordinary typed log records — the consumer's registered
restorers dispatch by `Type` exactly as they do for `ReplicateLogs`.

---

## Flow 2 — A trailing manual group

A batch may end with **one** manual group whose commit or rollback you control later — for example, a burst
of auto-commit records plus a single transaction prepare.

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

The auto prefix commits with the batch. The manual entries are **durable but not committed** — they report
`Pending` and share **one** ticket:

```
   result.Entries[0] = { Success, LogIndex=50, Ticket=Zero }        // kv (committed)
   result.Entries[1] = { Success, LogIndex=51, Ticket=Zero }        // lock (committed)
   result.Entries[2] = { Pending, LogIndex=52, Ticket=T }           // prepare (held)
   result.TicketId   = T                                            // the actionable ticket
```

Later, you finish the manual group with the existing two-phase calls:

```csharp
HLCTimestamp ticket = result.TicketId;

// commit the prepare…
await raft.CommitLogs(partitionId, ticket, cancellationToken);

// …or abandon it — this truncates only the manual suffix, leaving the committed auto prefix intact.
await raft.RollbackLogs(partitionId, ticket, cancellationToken);
```

Because the auto group commits **before** the manual group is proposed (see
[Ordering and durability guarantees](#ordering-and-durability-guarantees)), the manual entries always sit at
the highest indices, on top of a committed prefix. Rolling the ticket back can never touch the committed auto
entries.

---

## Flow 3 — Per-entry fencing

A batch that mixes a valid and a stale-generation entry appends the valid one and drops the stale one,
without failing the batch:

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

The fenced entry keeps its input slot, with `LogIndex == -1`, so the array stays index-aligned. If **every**
entry is fenced, nothing is appended and the whole batch reports `PartitionMoved` (overall `Success == false`)
so the caller knows to refresh its partition map and retry.

---

## Reading the result

`ReplicateEntries` returns a `RaftBatchReplicationResult`:

```csharp
public sealed class RaftBatchReplicationResult
{
    public bool Success { get; }                       // batch admitted and ≥1 entry landed
    public RaftOperationStatus Status { get; }         // overall status
    public HLCTimestamp TicketId { get; }              // the manual ticket, or the auto ticket, or Zero
    public IReadOnlyList<RaftEntryResult> Entries { get; } // index-aligned to the input list
}

public readonly record struct RaftEntryResult(
    RaftOperationStatus Status,
    long LogIndex,        // assigned index, or -1 when the entry was not appended
    HLCTimestamp Ticket   // shared manual ticket for a manual entry, else Zero
);
```

**Per-entry statuses:**

| `RaftEntryResult.Status` | Meaning | `LogIndex` | `Ticket` |
|---|---|---|---|
| `Success` | Auto-commit entry, appended and committed | its index | `Zero` |
| `Pending` | Trailing manual entry, durable but not yet committed | its index | shared manual ticket |
| `PartitionMoved` | Fenced out; not appended | `-1` | `Zero` |
| any other | Not appended (batch-level failure propagated) | `-1` | `Zero` |

**Overall result:**

- `Success == true` when the batch was admitted and at least one entry landed. Individual entries may still
  be `PartitionMoved` — always inspect per entry.
- `Success == false` only on a batch-level rejection (bad shape) or when **every** entry was fenced out
  (`Status == PartitionMoved`, nothing appended).

**Index alignment.** `Entries[i]` always describes the input entry at position `i`, regardless of how the
entries were split into auto and manual groups internally. `LogIndex` values come from the position each
entry was assigned in the log, not from a single commit index.

---

## Rejections and errors

| Condition | Result |
|---|---|
| Empty batch | `Success == true`, no entries — a no-op |
| Auto-commit entry after a manual entry (bad shape) | Batch-level rejection: `Success == false`, `Status == Errored`, every slot `Errored`/`LogIndex -1`, nothing appended |
| Every entry fenced out | `Success == false`, `Status == PartitionMoved`, every slot `PartitionMoved`, nothing appended |
| Some entries fenced, some admitted | `Success == true`, fenced slots `PartitionMoved`, admitted slots `Success`/`Pending` |
| Reserved system log type on the system partition | Throws `RaftException` before any append |
| Not the leader / lost leadership mid-flight | Admitted slots carry the failure status (e.g. `NodeIsNotLeader`); `Success == false` |

A batch-level rejection is atomic: nothing is appended, so there is no partial state to clean up.

---

## Ordering and durability guarantees

**Auto commits before manual is proposed.** The auto group and the manual group are two separate proposals
(the log commits or rolls back an entire proposal as a unit, and these two groups have different fates). They
are proposed **sequentially** — the auto group commits first, then the manual group is proposed on top of it.
This is a safety requirement, not just simplicity: if the two were posted together and the auto group failed
to commit *after* the manual propose was already durable, a later `CommitLogs` on the manual ticket could
advance the commit index past the uncommitted auto entries. Committing auto first makes the manual group a
clean uncommitted suffix on a committed prefix.

**Flush coalescing is opportunistic.** Because the two groups are proposed sequentially, there is no
guaranteed single `fsync` spanning both. Auto-commit-only batches are one proposal and ride one
group-committed write; a batch with a manual group is two proposals whose WAL writes coalesce only as the
scheduler's group commit happens to merge them with concurrent traffic. `ReplicateEntries` reduces the number
of *proposals* and *round trips*, not the scheduler's floor on syncs — see the
[WAL Write Durability guide](wal-commit-durability-developer-guide.md) for the sync mechanics and the
`WalGroupCommitLingerMs` lever that raises coalescing density.

**No ordering across entries beyond "auto before manual."** A batch is a set with one commit boundary per
group, not a sequence with cross-entry dependencies. Do not assume any order between two entries in the same
group.

---

## API reference

The surface lives on `IRaft` (implemented by `RaftManager`):

```csharp
Task<RaftBatchReplicationResult> ReplicateEntries(
    int partitionId,
    IReadOnlyList<RaftProposalEntry> entries,
    CancellationToken cancellationToken = default);
```

Supporting types (all in `Kommander/Data/`):

- **`RaftProposalEntry`** — one typed input entry: `Type`, `Data`, `AutoCommit`, `ExpectedGeneration`.
- **`RaftEntryResult`** — one per-entry outcome: `Status`, `LogIndex`, `Ticket`.
- **`RaftBatchReplicationResult`** — the batch outcome: `Success`, `Status`, `TicketId`, `Entries`.

Manual-group lifecycle (existing calls, reused unchanged):

```csharp
Task<(bool success, RaftOperationStatus status, long commitLogId)> CommitLogs(
    int partitionId, HLCTimestamp ticketId, CancellationToken cancellationToken = default);

Task<(bool success, RaftOperationStatus status, long commitLogId)> RollbackLogs(
    int partitionId, HLCTimestamp ticketId, CancellationToken cancellationToken = default);
```

Pass the `RaftBatchReplicationResult.TicketId` (or any manual entry's `RaftEntryResult.Ticket`) to
`CommitLogs`/`RollbackLogs`.

---

## Configuration

`ReplicateEntries` adds no configuration of its own. Its throughput and coalescing behaviour are governed by
the same WAL-scheduler knobs as every other write:

| Setting | Default | Relevance to batches |
|---|---|---|
| `WalGroupCommitLingerMs` | `0` | `> 0` lets the scheduler linger to gather more ready writes into one `fsync`, raising coalescing density for a manual group riding a separate flush. |
| `MaxWalGroupBatchPartitions` | `64` | Max partitions coalesced into one `walAdapter.Write`. |
| `MaxWalBatchSize` | `256` | Max operations drained from one partition per write — bounds how large a single proposal's write can be. |

See the [WAL Write Durability guide](wal-commit-durability-developer-guide.md) for how these interact.

---

## Code map

```
Kommander/
├─ IRaft.cs                        ReplicateEntries declaration
├─ RaftManager.cs                  ReplicateEntries — the orchestration:
│     ├─ shape validation           auto-prefix + single trailing manual group; reserved-type guard
│     ├─ per-entry fence            classify each entry vs partition.Generation; PartitionMoved slots
│     ├─ auto group                 propose (autoCommit:true) + WaitForQuorum → commit
│     ├─ manual group               propose (autoCommit:false) + WaitForQuorum → propose-quorum durable
│     └─ RejectBatch / FailBatch    batch-level and in-flight failure results
├─ RaftPartition.cs                ReplicateEntries(List<RaftLog>, expectedGeneration, autoCommit)
│                                   — posts one ReplicateLogs executor request per group
├─ Data/
│     ├─ RaftProposalEntry.cs       input entry (Type, Data, AutoCommit, ExpectedGeneration)
│     ├─ RaftEntryResult.cs         per-entry outcome (Status, LogIndex, Ticket)
│     └─ RaftBatchReplicationResult.cs   batch outcome (Success, Status, TicketId, Entries)
└─ RaftPartitionStateMachine.cs    the proposal/commit/rollback path both groups reuse

Kommander.Tests/
└─ TestReplicateEntries.cs         heterogeneous commit + restore-by-type on a follower;
                                    contiguous per-entry indices; trailing manual commit + rollback;
                                    auto-after-manual shape rejection; per-entry fencing (mixed,
                                    interleaved with the manual split, all-fenced)
```

---

## Invariants you must not break

- **One manual group, last.** If you change the shape validation, the manual group must remain a single
  contiguous **suffix**. A manual group anywhere but the end is not truncation-safe.
- **Auto commits before manual is proposed.** Never post the two groups concurrently. The sequential order is
  what keeps a manual `CommitLogs`/`RollbackLogs` from reaching the auto entries.
- **Results stay index-aligned to the input.** Every code path — admitted, fenced, or failed — must leave
  `Entries[i]` describing input entry `i`. Fenced and failed slots carry `LogIndex == -1`.
- **A batch-level rejection appends nothing.** Shape and reserved-type checks run before any append; they must
  never leave partial state.
- **Per-entry `LogIndex` comes from the assigned log id.** It is read back off the same `RaftLog` instances
  after the propose reply resolves — do not derive it from a single commit index.

---

## Testing

`Kommander.Tests/TestReplicateEntries.cs` runs on a real two-node in-memory cluster so the follower actually
receives each batch:

- **Heterogeneous auto-commit** — a mixed-type batch commits as one proposal; per-entry indices are
  contiguous and index-aligned; the follower restores each entry under its own type.
- **Trailing manual group** — the auto prefix commits; the manual entries are `Pending` under one shared
  ticket; committing the ticket commits the suffix, and rolling it back marks the suffix rolled back while the
  committed auto prefix survives.
- **Shape rejection** — an auto-commit entry after a manual entry is rejected before any append.
- **Per-entry fence** — a mix of admitted and fenced entries appends the admitted ones and reports
  `PartitionMoved` only in the fenced slots (overall `Success`); a fenced entry interleaved with the manual
  split does not disturb either group; an all-fenced batch reports `PartitionMoved` and appends nothing.

When you touch this path, run the full test project — it exercises shared scheduling, WAL, and cluster
coordination, and the two-node flows are timing-sensitive.

---

## Glossary

- **Heterogeneous batch** — a set of entries in one call that may each have a different type, auto-commit
  choice, and generation fence.
- **Commit group** — a set of entries that commit or roll back together as one proposal. A batch has one
  auto-commit group and at most one trailing manual group.
- **Auto-commit entry** — an entry that commits with the batch; its result carries an index and no ticket.
- **Manual entry / trailing manual group** — entries held for an explicit later `CommitLogs`/`RollbackLogs`;
  they share one ticket and must sort after every auto-commit entry.
- **Ticket** — the `HLCTimestamp` handle for a manual group, passed to `CommitLogs`/`RollbackLogs`.
- **Generation fence** — a per-entry check against the partition's committed generation; a stale entry is
  dropped with `PartitionMoved`.
- **Generation** — the partition-map version, bumped on split/merge.
- **`PartitionMoved`** — the status for an entry (or a whole all-fenced batch) whose generation fence missed;
  refresh the partition map and retry.
- **Index-aligned** — `Entries[i]` corresponds to input entry `i`, whatever internal grouping occurred.
- **Suffix truncation** — how a manual rollback removes entries: everything after a point, which is why the
  manual group must be last.

---

*For the broader system, see the [Architecture Overview](architecture-overview.md); for the sync mechanics a
batch rides, see the [WAL Write Durability & Single-Fsync Commit Guide](wal-commit-durability-developer-guide.md);
for how a partition's generation changes, see the [Elastic Partitions Developer Guide](elastic-partitions-developer-guide.md).*
