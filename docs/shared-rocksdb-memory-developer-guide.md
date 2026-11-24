# Shared RocksDB Memory Resources for the RocksDB WAL — Developer Guide

A process that has **both** a Kommander RocksDB WAL **and** another RocksDB database can give them one
memory budget. This guide covers what is shared, how to size the budget, and the ownership rules. It is
for operators who size the budget and for developers who wire it into a host application.

The feature is **opt-in**. It changes only in-process memory objects. The on-disk format, WAL semantics,
recovery mode, and wire behaviour are the same.

---

## Table of contents

1. [Summary](#summary)
2. [Double memory cost of two RocksDB instances](#double-memory-cost-of-two-rocksdb-instances)
3. [Shared resources](#shared-resources)
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

---

## Summary

Two things dominate RocksDB memory: the **block cache** (cached SST blocks, used by reads) and
**memtables** (in-memory write buffers, one set per column family, or CF). A `RocksDbWAL` opens **one**
database with **10 column families**: `default`, `metadata`, and `shard0..7`.

`RocksDbSharedResources` lets the host create **one** block cache and **one** write-buffer manager (WBM),
and give the same bundle to the WAL and to the host database.

```
   without sharing:   [ WAL: cache A + memtables A ]   [ host DB: cache B + memtables B ]
                                                         two independent budgets

   with sharing:      [ WAL ]──┐                  ┌──[ host DB ]
                               ├── one Cache ──────┤
                               └── one WBM ────────┘
                                  one unified budget
```

The host **owns** the bundle. It passes the bundle to each database and disposes it **after** all those
databases are closed.

---

## Double memory cost of two RocksDB instances

By default, `RocksDbWAL` configures no block cache and no global bound on memtable memory. It uses
RocksDB's small default block cache. If the host process opens a second RocksDB instance, the process
pays for two block caches and two unbounded memtable footprints.

One `Cache` and one `WriteBufferManager` bound the total to **one** number. The databases have separate
data, paths, and column families.

---

## Shared resources

| Resource | Shared? | How |
|---|---|---|
| **Block cache** | ✅ | One `Cache` (managed RocksDbSharp type) applied to each column family of each DB. |
| **Memtable budget** | ✅ | One `WriteBufferManager` charged to that same cache, attached to each DB's `DbOptions`. |
| **Background thread pools (`Env`)** | already shared | Both instances use `Env::Default()`. |
| **On-disk data / column families** | ❌ | Each database has separate files and CFs. |
| **WAL semantics, recovery mode, wire format** | ❌ unchanged | |

RocksDbSharp has a **managed `Cache`** type (`Cache.CreateLru`, `Cache.GetUsage`,
`BlockBasedTableOptions.SetBlockCache`). It has **no** managed `WriteBufferManager` type and **no**
`DbOptions.SetWriteBufferManager`. The WBM is created and attached through `Native.Instance.*`, and
`RocksDbSharedResources` owns it as a raw `IntPtr`.

---

## The unified budget

`RocksDbSharedResources.CreateWithUnifiedBudget(totalBytes, memtableBudgetBytes)` builds two objects.

1. One **soft** LRU block cache of `totalBytes` (`Cache.CreateLru`).
2. One **non-stalling** WBM with a memtable sub-budget of `memtableBudgetBytes`, **charged to that same
   cache** (`rocksdb_write_buffer_manager_create_with_cache`, `allow_stall = 0`).

Cache occupancy and memtable memory share one bound. The method throws `ArgumentOutOfRangeException`
unless `memtableBudgetBytes <= totalBytes`.

```
   ┌─────────────────────── totalBytes (LRU block cache) ───────────────────────┐
   │  cached SST blocks            │  memtable charge (<= memtableBudgetBytes)    │
   └────────────────────────────────────────────────────────────────────────────┘
```

- **The cache has a soft (non-strict) capacity.** Memtables are charged to the cache, and a *hard* limit
  would cause write errors or stalls when a sharing database flushes a memtable.
- **The WBM has `allow_stall = false`.** A flush in one database does not stall writes in another. Size
  the budget conservatively, because stalls give no back-pressure.

---

## Flow 1 — Create the bundle once, inject it everywhere

The host's composition root creates the bundle, then passes it to the WAL and to the host database.

```csharp
// Host owns this. Create once, dispose last.
RocksDbSharedResources shared = RocksDbSharedResources.CreateWithUnifiedBudget(
    totalBytes:          512L * 1024 * 1024,  // 512 MiB block cache
    memtableBudgetBytes: 256L * 1024 * 1024); // 256 MiB memtable sub-budget inside it

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

The `sharedResources:` parameter is optional.

---

## Flow 2 — How the WAL applies the bundle

`RocksDbWAL` sets both objects **before** `RocksDb.Open`. They are part of the options and CF descriptors
at open time and cannot be applied afterward.

```
  build DbOptions
     └─► rocksdb_options_set_write_buffer_manager(dbOptions.Handle, WriteBufferManagerHandle)   (interop)

  build ONE BlockBasedTableOptions().SetBlockCache(shared.BlockCache)
     └─► apply it to EVERY column family's options:
            default, metadata, shard0 … shard7   (all 10)

  RocksDb.Open(dbOptions, path, columnFamilies)
```

A CF that does not get the shared block-cache factory uses RocksDB's default per-CF cache and is silently
excluded from the budget. The `ApplyCfOptions` helper applies one `BlockBasedTableOptions` to all 10.

When `sharedResources` is null, the WAL sets no block cache and attaches no WBM.

---

## Sizing the budget

The WBM bounds **total** memtable memory across *all column families of all databases* that share it.

```
   Σ over all sharing DBs ( write_buffer_size × max_write_buffer_number × CF count )
```

If this sum greatly exceeds `memtableBudgetBytes`, ordinary write bursts trip the global budget and cause
continuous flushes across CFs and databases.

`RocksDbWalTuning` sizes the shard CFs of the WAL. Its defaults are 64 MiB for
`ShardWriteBufferSizeBytes` and 4 for `ShardMaxWriteBufferNumber`. The eight shards can use
`8 × 64 MiB × 4 = 2 GiB` of *potential* memtable memory, which is far above a typical WBM budget. Plan
for it when you share a WBM.

The budget must be at least `ShardWriteBufferSizeBytes × (ShardMinWriteBufferNumberToMerge + 1)`, which
is one flush unit plus one memtable of headroom. With the defaults this is 192 MiB. Below it, the WAL logs
a Warning at open, and RocksDB flushes the Raft log on the budget's cadence. Add the memtables of the host
database to this minimum.

- Pick values that make the envelope of the WAL a **small, intended fraction** of `memtableBudgetBytes`,
  with headroom for the host database.
- Set matching, modest values on the host database too.
- Document the numbers you chose.

If write-path coupling is a problem, **share only the `Cache`** and leave the WBM unset on the host side.
The block cache alone removes the duplicated read-cache footprint.

Before you use a WBM in production, measure consensus-path latency (WAL append and commit) under
concurrent load with sharing **on and off**.

---

## Ownership and lifetime

**The host owns the bundle. `RocksDbWAL` borrows it and must not dispose it.**

The native `Cache` and WBM are reference-counted (`shared_ptr`), and each open DB has a reference to
**both** objects.

- **Dispose the bundle last.** Dispose each database that borrowed it first.
  `RocksDbSharedResources.Dispose()` drops only the **bundle's** reference.
- **An early dispose does not crash.** The open DBs have references, and no memory is freed under them.
  However, it is a **usage error**: the budget accounting becomes inconsistent.
- **Dispose is idempotent.** Repeated `Dispose()` calls are safe.
- **A finalizer releases the raw WBM handle** if the host forgets `Dispose`. The raw `IntPtr` would leak
  otherwise. You should always dispose explicitly.

`Dispose` releases the **WBM first, then the block cache**. The WBM has a `shared_ptr` to the cache, and
this order lets the refcount drop cleanly. `Cache` has no managed `Dispose`, and its finalizer frees the
handle. The bundle destroys the cache handle explicitly and suppresses that finalizer to prevent a
double free.

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
    RocksDbSharedResources? sharedResources = null,    // optional; null ⇒ default behaviour
    IMonotonicTickSource? tickSource = null,
    RocksDbWalTuning? tuning = null);                  // null ⇒ RocksDbWalTuning.Default
```

| Parameter | Effect |
|---|---|
| `sharedResources = null` (default) | No block cache and no WBM. |
| `sharedResources != null` | Block cache applied to all 10 CFs and WBM applied to `DbOptions`, both before `RocksDb.Open`. |
| `tuning` | Sizes the shard CFs. The constructor throws `ArgumentOutOfRangeException` unless `ShardMaxWriteBufferNumber` is greater than `ShardMinWriteBufferNumberToMerge`. |

There is no `RaftConfiguration` setting for the bundle.

---

## Observability

| Signal | Meaning | When it moves |
|---|---|---|
| `RocksDbSharedResources.MemtableMemoryUsage` | Live memtable bytes tracked by the WBM (`rocksdb_write_buffer_manager_memory_usage`). | Rises on **writes** from *any* database that shares the bundle. |
| `RocksDbSharedResources.BlockCache.GetUsage()` | Current block-cache occupancy. | Grows on **reads**, or through the WBM's dummy entries. |

To confirm that two databases share one budget, check that `MemtableMemoryUsage` rises when either one
writes. Before you assert on `GetUsage()`, issue reads on both, because the counter may not move
predictably under a pure-append workload.

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

1. **The host owns the bundle and the WAL borrows it.** `RocksDbWAL` must not call `Dispose` on a
   `RocksDbSharedResources`.
2. **Apply the block cache to *every* column family.** A CF that does not get it silently escapes the
   budget.
3. **Set the cache and the WBM before `RocksDb.Open`.** They cannot be changed afterward.
4. **`memtableBudgetBytes <= totalBytes`.** A violation throws.
5. **Soft cache and non-stalling WBM.** A strict-capacity cache surfaces as write stalls or errors on all
   sharing databases.
6. **Null means unshared.** With `sharedResources` null, the WAL open path must set no block cache and no
   WBM.
7. **Dispose order in the bundle is WBM, then cache.** Suppress the `Cache` finalizer after the explicit
   destroy, or the handle is freed twice.

---

## Testing

`TestRocksDbSharedResources` covers the feature.

- **Budget validation.** `CreateWithUnifiedBudget` throws for `memtableBudgetBytes > totalBytes` and
  allows an equal budget. The handles are non-null and `MemtableMemoryUsage` is readable.
- **Idempotent dispose.** A second `Dispose` does not crash.
- **Backward compatibility.** A WAL with `sharedResources: null` writes and reads identically to the
  default constructor.
- **Bundle applied.** A WAL with a bundle round-trips logs across several partitions and shards.
- **Sharing.** Two `RocksDbWAL` instances at distinct paths with **one** bundle both drive
  `MemtableMemoryUsage` up.
- **Lifetime.** Neither dispose order (WALs first, or bundle first) crashes.

**Do not start multiple `dotnet test` commands at once.** Concurrent in-process Raft clusters produce
false failures.

```sh
dotnet test Kommander.Tests/Kommander.Tests.csproj --filter FullyQualifiedName~TestRocksDbSharedResources
```

---

*For the broader system, see the [Architecture Overview](architecture-overview.md). For the WAL write and
durability path, see the
[WAL Write Durability & Single-Fsync Commit Developer Guide](wal-commit-durability-developer-guide.md).*
