# Shared RocksDB Memory Resources for the RocksDB WAL — Developer Guide

This guide explains how to make a process that runs **both** a Kommander RocksDB WAL **and** its own
RocksDB database share one memory budget instead of paying for two. It covers what is shared (a block
cache and a write-buffer manager), what is not, how to size the budget, and the ownership and lifetime
rules you must follow to avoid leaks or crashes. It is written for two readers — an operator deciding
whether to turn this on and how to size it, and a developer wiring it into a host application. No prior
RocksDB internals are assumed; we build the idea up first, then walk the API, the sizing math, the
lifetime contract, and the code.

This feature is **opt-in and additive**. It changes only in-process memory objects — no on-disk format,
WAL semantics, recovery mode, or wire behaviour. With it unset, the RocksDB WAL behaves byte-for-byte as
before.

---

## Table of contents

1. [Summary](#summary)
2. [Why two RocksDB instances double-pay for memory](#why-two-rocksdb-instances-double-pay-for-memory)
3. [What is shared — and what is not](#what-is-shared--and-what-is-not)
4. [The unified budget](#the-unified-budget)
5. [Flow 1 — Create the bundle once, inject it everywhere](#flow-1--create-the-bundle-once-inject-it-everywhere)
6. [Flow 2 — How the WAL applies the bundle](#flow-2--how-the-wal-applies-the-bundle)
7. [Sizing the budget](#sizing-the-budget)
8. [Ownership and lifetime](#ownership-and-lifetime)
9. [API reference](#api-reference)
10. [Observability](#observability)
11. [Code map](#code-map)
12. [Invariants you must not break](#invariants-you-must-not-break)
13. [Testing](#testing)
14. [Glossary](#glossary)

---

## Summary

RocksDB's memory is dominated by two things: the **block cache** (cached SST blocks, shared across reads)
and **memtables** (in-RAM write buffers, one set per column family). A `RocksDbWAL` opens **one** RocksDB
database with **10 column families** (`default` + `metadata` + `shard0..7`), each with its own memtables.

By default the WAL configures **neither** — it uses RocksDB's small default block cache and unbounded
default memtables. If the host process also runs a *second* RocksDB instance (the application's own
database), the two never share anything: two block caches, two unbounded sets of memtables, two budgets.

`RocksDbSharedResources` lets the host create **one** block cache and **one** write-buffer manager (WBM),
then hand the same bundle to the WAL and to its own database. Both instances now draw from a single,
bounded budget:

```
   without sharing:   [ WAL: cache A + memtables A ]   [ host DB: cache B + memtables B ]
                                                         two independent budgets

   with sharing:      [ WAL ]──┐                  ┌──[ host DB ]
                               ├── one Cache ──────┤
                               └── one WBM ────────┘
                                  one unified budget
```

The host **owns** the bundle, creates it once, injects it into every database via an optional
constructor parameter, and disposes it **after** all those databases are closed.

---

## Why two RocksDB instances double-pay for memory

`RocksDbWAL` (`Kommander/WAL/RocksDbWAL.cs`) opens its database with no memory tuning:

- **No block cache configured.** The `DbOptions` set only `CreateIfMissing`,
  `CreateMissingColumnFamilies`, `AllowConcurrentMemtableWrite`, and `WalRecoveryMode` — so the database
  uses RocksDB's small default per-instance block cache.
- **No `write_buffer_size` / `max_write_buffer_number` override.** Each of the 10 column families gets
  RocksDB's default memtable sizing, with no *global* bound on total memtable memory.
- **No custom `Env`.** It uses the process-wide `Env::Default()`, whose background thread pools are
  *already* shared across every RocksDB instance in the process. The `Env` is therefore **not** a lever —
  the block cache and the memtables are.

When the host runs a second RocksDB instance with its own cache, both of these are fully duplicated. The
process pays for two block caches and two unbounded memtable footprints, even though one bounded budget
would serve both. Sharing a single `Cache` and a single `WriteBufferManager` bounds total cache+memtable
memory to **one** number without merging the databases — they keep separate data, paths, and column
families.

---

## What is shared — and what is not

| Resource | Shared? | How |
|---|---|---|
| **Block cache** | ✅ | One `Cache` (managed RocksDbSharp type) applied to every column family of every DB. |
| **Memtable budget** | ✅ | One `WriteBufferManager` cost-charged to that same cache, attached to each DB's `DbOptions`. |
| **Background thread pools (`Env`)** | already shared | Both instances use `Env::Default()` — nothing to do. |
| **On-disk data / column families** | ❌ | Each database keeps its own path, files, and CFs. Sharing is memory-only. |
| **WAL semantics, recovery mode, wire format** | ❌ unchanged | The feature touches only in-process memory objects. |

Two key facts shape the API:

- RocksDbSharp `10.10.1` exposes a **managed `Cache`** type (`Cache.CreateLru`, `Cache.GetUsage`,
  `BlockBasedTableOptions.SetBlockCache`), so the block-cache path needs no interop.
- It exposes **no** managed `WriteBufferManager` type and **no** `DbOptions.SetWriteBufferManager`. The
  WBM is therefore created and attached through a thin interop wrapper over `Native.Instance.*`, and
  `RocksDbSharedResources` holds it as a raw `IntPtr` it owns.

---

## The unified budget

`RocksDbSharedResources.CreateWithUnifiedBudget(totalBytes, memtableBudgetBytes)` builds:

1. One **soft** LRU block cache of `totalBytes` (`Cache.CreateLru`).
2. One **non-stalling** WriteBufferManager with a memtable sub-budget of `memtableBudgetBytes`,
   **cost-charged to that same cache** (`rocksdb_write_buffer_manager_create_with_cache`,
   `allow_stall = 0`).

Because the memtable budget is charged *into* the cache, cache occupancy and memtable memory share one
bound — hence the requirement `memtableBudgetBytes <= totalBytes` (the constructor throws
`ArgumentOutOfRangeException` otherwise: the sub-budget cannot be larger than the budget it lives in).

```
   ┌─────────────────────── totalBytes (LRU block cache) ───────────────────────┐
   │  cached SST blocks            │  memtable charge (<= memtableBudgetBytes)    │
   └────────────────────────────────────────────────────────────────────────────┘
```

Two deliberate choices keep this safe under write load:

- **Soft (non-strict-capacity) cache.** With memtables charged to the cache, a *hard* capacity limit
  would surface as write errors or stalls every time any sharing database flushed a memtable. A soft
  limit lets the budget be a target, not a brick wall.
- **`allow_stall = false` on the WBM.** Prevents cross-database flush coupling from turning into write
  stalls. Size conservatively instead of relying on stalls for back-pressure.

---

## Flow 1 — Create the bundle once, inject it everywhere

The host's composition root creates the bundle, then passes it to the WAL (and to its own database):

```csharp
// Host owns this. Create once, dispose last.
RocksDbSharedResources shared = RocksDbSharedResources.CreateWithUnifiedBudget(
    totalBytes:          512L * 1024 * 1024,  // 512 MiB block cache
    memtableBudgetBytes: 128L * 1024 * 1024); // 128 MiB memtable sub-budget inside it

RaftManager node = new(
    configuration,
    new StaticDiscovery(nodes),
    new RocksDbWAL(
        path: walPath, revision: walRevision, logger,
        syncWrites: true,
        sharedResources: shared),          // ← inject the bundle
    new GrpcCommunication(),
    new HybridLogicalClock(),
    logger);

// ... the host opens its own RocksDB DB with the SAME `shared.BlockCache` and
//     `shared.WriteBufferManagerHandle` (host-side wiring lives in the host repo).

// On shutdown, AFTER node and the host DB are disposed:
shared.Dispose();
```

The trailing `sharedResources:` parameter is optional. Omit it (or pass `null`) and the WAL opens exactly
as before — no cache, default buffers, default CF options.

---

## Flow 2 — How the WAL applies the bundle

When `sharedResources` is non-null, `RocksDbWAL` wires both objects in **before** `RocksDb.Open` (these
are baked into the options and CF descriptors at open time — they cannot be applied afterward):

```
  build DbOptions
     └─► rocksdb_options_set_write_buffer_manager(dbOptions.Handle, WriteBufferManagerHandle)   (interop)

  build ONE BlockBasedTableOptions().SetBlockCache(shared.BlockCache)
     └─► apply it to EVERY column family's options:
            default, metadata, shard0 … shard7   (all 10)

  RocksDb.Open(dbOptions, path, columnFamilies)
```

Applying the block cache to **every** column family matters: a CF *without* the block-cache factory keeps
RocksDB's default per-CF cache and is silently excluded from the shared budget. None can be skipped, so a
single shared `BlockBasedTableOptions` (it only references the cache) is applied to all 10 via the
`ApplyCfOptions` helper.

When `sharedResources` is null, the column families are created with plain `new()` options and no WBM is
attached — the byte-for-byte default path.

---

## Sizing the budget

The WBM bounds **total** memtable memory across *every column family of every database* sharing it. This
WAL alone contributes 10 column families. If

```
   Σ over all sharing DBs ( write_buffer_size × max_write_buffer_number × CF count )
```

greatly exceeds `memtableBudgetBytes`, ordinary write bursts trip the global budget and cause continuous
cross-CF / cross-DB flushing — the workload-coupling cost of sharing a WBM.

RocksDB's defaults are **64 MiB** `write_buffer_size` and **2** `max_write_buffer_number`. For this WAL's
10 CFs that is `10 × 64 MiB × 2 ≈ 1.28 GiB` of *potential* memtable memory — far above any typical WBM
budget. So if you share a WBM, plan for it:

- Pick `write_buffer_size` so this WAL's `cf_count × write_buffer_size × max_write_buffer_number` is a
  **small, intended fraction** of `memtableBudgetBytes`, leaving headroom for the host's own database.
- Apply matching, modest values on the host database too.
- Document the numbers you chose so the coupling is intentional, not accidental.

If write-path coupling turns out to be a problem, you have an escape hatch: **share only the `Cache`** —
the larger, workload-agnostic win — and leave the WBM unset on the host side. The block cache alone
already eliminates the duplicated read-cache footprint.

Measure consensus-path latency (WAL append / commit) under concurrent load with sharing **on vs off**
before committing to a WBM in production.

---

## Ownership and lifetime

**The host owns the bundle. `RocksDbWAL` borrows it and never disposes it.** This is the single most
important rule, and it is documented on both the type and the constructor.

The native `Cache` and WBM are reference-counted (`shared_ptr`). When a database opens with them,
`rocksdb_options_set_write_buffer_manager` copies the WBM's `shared_ptr` into the DB's options, and each
open DB holds its own reference to **both** objects. The consequences:

- **Dispose order is host last.** Dispose every database that borrowed the bundle, *then* dispose the
  bundle. `RocksDbSharedResources.Dispose()` drops only the **bundle's** reference; the open DBs keep
  their own.
- **Disposing early will not crash.** Because the DBs hold their own references, disposing the bundle
  *before* closing the databases does **not** free memory out from under them — no use-after-free. It is
  still a **usage error**, but for *budget-accounting* reasons (a charged-but-released cache leaves the
  accounting inconsistent), not crash-safety.
- **Dispose is idempotent.** A `_disposed` guard makes repeated `Dispose()` calls safe no-ops.
- **A finalizer backstops a forgotten `Dispose`.** If the host never disposes the bundle, a finalizer
  releases the raw WBM handle so it does not leak. (The managed `Cache` self-finalizes; the raw `IntPtr`
  would not, hence the backstop.) Always dispose explicitly anyway — the finalizer is a safety net, not
  the plan.

Inside `Dispose`, the order is **WBM first, then the block cache**: the WBM holds a `shared_ptr` to the
cache, so releasing it first lets the cache's refcount drop cleanly. The `Cache` has no managed `Dispose`
and frees its handle through its own finalizer; the bundle destroys the cache handle explicitly and
suppresses that finalizer so the handle is never freed twice.

---

## API reference

### `RocksDbSharedResources`

```csharp
public sealed class RocksDbSharedResources : IDisposable
{
    public Cache  BlockCache              { get; }   // apply to CFs via SetBlockCache
    public IntPtr WriteBufferManagerHandle { get; }  // raw WBM handle, owned by this type
    public long   MemtableMemoryUsage      { get; }  // live memtable bytes tracked by the WBM

    public static RocksDbSharedResources CreateWithUnifiedBudget(
        long totalBytes, long memtableBudgetBytes);  // throws if memtableBudgetBytes > totalBytes

    public void Dispose();  // host calls this, after all borrowing DBs are closed
}
```

### `RocksDbWAL` constructor

```csharp
public RocksDbWAL(
    string path,
    string revision,
    ILogger<IRaft> logger,
    bool syncWrites = true,
    RocksDbSharedResources? sharedResources = null);   // optional; null ⇒ default behaviour
```

| Parameter | Effect |
|---|---|
| `sharedResources = null` (default) | No block cache, default memtables, default CF options — byte-for-byte the prior behaviour. |
| `sharedResources != null` | Block cache applied to all 10 CFs; WBM applied to `DbOptions`; both before `RocksDb.Open`. Bundle is borrowed, never disposed by the WAL. |

There is no `RaftConfiguration` knob for this — the bundle is injected through the constructor by the host
that already constructs the `RocksDbWAL`.

---

## Observability

| Signal | Meaning | When it moves |
|---|---|---|
| `RocksDbSharedResources.MemtableMemoryUsage` | Live memtable bytes tracked by the WBM (`rocksdb_write_buffer_manager_memory_usage`). | Rises on **writes** from *any* database sharing the bundle. The primary signal that sharing is real. |
| `RocksDbSharedResources.BlockCache.GetUsage()` | Current block-cache occupancy. | Grows on **reads** (cached blocks) or via the WBM's cost-to-cache dummy entries. Do **not** rely on it alone for a write-only workload. |

To confirm two databases truly share one budget, watch `MemtableMemoryUsage` rise as either one writes.
If you assert on `GetUsage()`, issue reads on both first to populate the block cache, or the counter may
not move predictably under a pure-append workload.

---

## Code map

```
Kommander/
└─ WAL/
   ├─ RocksDbSharedResources.cs   the bundle:
   │     ├─ CreateWithUnifiedBudget   builds soft LRU cache + WBM cost-charged to it; validates budget
   │     ├─ BlockCache / WriteBufferManagerHandle / MemtableMemoryUsage
   │     ├─ Dispose                   refcount-aware release (WBM first, then cache; suppresses Cache finalizer)
   │     └─ ~RocksDbSharedResources   finalizer backstop for a forgotten Dispose (WBM handle leak guard)
   └─ RocksDbWAL.cs
         ├─ ctor(..., RocksDbSharedResources? sharedResources = null)
         │     ├─ rocksdb_options_set_write_buffer_manager(dbOptions.Handle, WBM handle)   (before Open)
         │     └─ one BlockBasedTableOptions.SetBlockCache(...) applied to every CF
         └─ ApplyCfOptions             attaches the shared table factory per CF (no-op when null)

Kommander.Tests/
└─ WAL/TestRocksDbSharedResources.cs   budget validation, backward compat, round-trip with the bundle,
                                       two-WAL sharing via MemtableMemoryUsage, both dispose orders
```

---

## Invariants you must not break

1. **The host owns the bundle; the WAL borrows it.** `RocksDbWAL` must never call `Dispose` on a
   `RocksDbSharedResources`. The host disposes it, after all borrowing databases are closed.
2. **Apply the block cache to *every* column family.** A CF without the shared table factory falls back
   to its own default cache and silently escapes the budget. All 10 CFs get the same
   `BlockBasedTableOptions`.
3. **Wire the cache and WBM before `RocksDb.Open`.** They are baked into the options and CF descriptors
   at open time and cannot be changed afterward.
4. **`memtableBudgetBytes <= totalBytes`.** The memtable sub-budget lives inside the cache budget;
   violating this throws.
5. **Soft cache + non-stalling WBM.** Never use a strict-capacity cache here — with memtables charged to
   the cache, a hard cap surfaces as write stalls/errors on every sharing database.
6. **Null path is sacred.** With `sharedResources` null, the WAL open path is byte-for-byte the prior
   default — no cache, default buffers, default CF options.
7. **Dispose order inside the bundle is WBM then cache**, and the `Cache` finalizer must be suppressed
   after the explicit cache destroy so the handle is never freed twice.

---

## Testing

`TestRocksDbSharedResources` covers the feature at the unit and integration layers:

- **Budget validation** — `CreateWithUnifiedBudget` rejects `memtableBudgetBytes > totalBytes` and
  accepts an equal budget; the resulting handles are non-null and `MemtableMemoryUsage` is readable.
- **Idempotent dispose** — `Dispose` twice does not crash.
- **Backward compatibility** — a WAL opened with `sharedResources: null` writes and reads identically to
  the default constructor.
- **Bundle applied** — a WAL opened *with* a bundle round-trips logs across several partitions/shards, so
  the shared cache + WBM do not change WAL behaviour.
- **Sharing is real** — two independent `RocksDbWAL` instances (distinct paths) opened with **one**
  bundle both drive `MemtableMemoryUsage` up, proving a single WBM tracks both databases.
- **Lifetime** — disposing the WALs then the bundle (correct order) does not crash; disposing the bundle
  *before* the WALs also does not crash (the DBs hold their own refcounts), documenting that early
  disposal is a budget-accounting error, not a crash hazard.

Reminder: **never run multiple `dotnet test` commands at once.** The suite spins up in-process Raft
clusters; concurrent runs produce false failures. Use a targeted `--filter` while iterating.

```sh
dotnet test Kommander.Tests/Kommander.Tests.csproj --filter FullyQualifiedName~TestRocksDbSharedResources
```

---

## Glossary

- **Block cache** — RocksDB's cache of decompressed SST data blocks; the dominant *read* memory cost.
  Shared here as a single `Cache`.
- **Memtable** — an in-RAM write buffer; one active (plus up to `max_write_buffer_number - 1` immutable)
  per column family. The dominant *write* memory cost.
- **WriteBufferManager (WBM)** — RocksDB's global bound on total memtable memory across column families
  and databases. Shared here, cost-charged to the block cache.
- **Column family (CF)** — an independently-keyed namespace inside one RocksDB database. The WAL uses 10:
  `default`, `metadata`, and `shard0..7`.
- **Cost-charged to a cache** — the WBM accounts its memtable memory *inside* the block cache's budget, so
  one number bounds both.
- **Soft / strict-capacity cache** — a soft cache treats its size as a target; a strict one rejects/stalls
  at the cap. This feature uses a soft cache deliberately.
- **`allow_stall`** — a WBM flag that, when true, stalls writes at the memtable budget. Set to false here
  to avoid cross-database write stalls.
- **`shared_ptr` / refcount** — the native objects are reference-counted; each open DB holds its own
  reference, so disposing the bundle only drops the bundle's reference.
- **Borrowed resource** — the WAL uses the bundle but does not own its lifetime; the host creates and
  disposes it.

---

*For the broader system, see the [Architecture Overview](architecture-overview.md); for the WAL write and
durability path, see the
[WAL Write Durability & Single-Fsync Commit Developer Guide](wal-commit-durability-developer-guide.md).*
