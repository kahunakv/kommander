using RocksDbSharp;

namespace Kommander.WAL;

/// <summary>
/// A disposable bundle of shared RocksDB memory objects — one LRU block cache and one
/// WriteBufferManager (WBM) — that a host application creates once and injects into one or more
/// <see cref="RocksDbWAL"/> instances (and/or its own RocksDB databases) so they all share a single
/// unified memory budget.
///
/// <para>
/// <b>Ownership and lifetime:</b> this object is owned and disposed by the <em>host application</em>,
/// after all databases that borrowed it have been closed. <see cref="RocksDbWAL"/> borrows the bundle
/// and must never call <see cref="Dispose"/> on it. Because the underlying native objects are
/// <c>shared_ptr</c> wrappers, <see cref="Dispose"/> only drops the bundle's reference — it does not
/// free memory out from under a still-open database. Disposing before closing the databases therefore
/// will not crash (the DBs hold their own shared references), but it is still a usage error: the WBM
/// and cache will no longer correctly account for memory charged by the open DBs.
/// </para>
///
/// <para>
/// <b>Why no managed <c>WriteBufferManager</c>:</b> RocksDbSharp 10.10.1 ships no managed WBM wrapper,
/// so this type holds the raw native handle and destroys it via <c>rocksdb_write_buffer_manager_destroy</c>
/// in <see cref="Dispose"/>. The block cache, by contrast, is fully covered by the managed
/// <see cref="Cache"/> API.
/// </para>
/// </summary>
public sealed class RocksDbSharedResources : IDisposable
{
    private bool _disposed;

    private RocksDbSharedResources(Cache cache, IntPtr wbm)
    {
        BlockCache = cache;
        WriteBufferManagerHandle = wbm;
    }

    /// <summary>
    /// The shared LRU block cache. Apply to every column family via
    /// <c>new BlockBasedTableOptions().SetBlockCache(BlockCache)</c> before opening a database.
    /// </summary>
    public Cache BlockCache { get; }

    /// <summary>
    /// Raw native handle to the WriteBufferManager created via
    /// <c>rocksdb_write_buffer_manager_create_with_cache</c>. Apply to a database's
    /// <see cref="DbOptions"/> via
    /// <c>Native.Instance.rocksdb_options_set_write_buffer_manager(dbOptions.Handle, WriteBufferManagerHandle)</c>
    /// before opening. This handle is valid until <see cref="Dispose"/> is called.
    /// </summary>
    public IntPtr WriteBufferManagerHandle { get; }

    /// <summary>
    /// Current bytes of memtable memory tracked by the WriteBufferManager
    /// (<c>rocksdb_write_buffer_manager_memory_usage</c>). Rises as any database sharing this
    /// bundle writes data; use this as the primary signal in sharing tests rather than
    /// <see cref="Cache.GetUsage"/>, which only grows when block-cache reads occur.
    /// </summary>
    public long MemtableMemoryUsage =>
        (long)Native.Instance.rocksdb_write_buffer_manager_memory_usage(WriteBufferManagerHandle);

    /// <summary>
    /// Creates a unified memory budget: one soft LRU block cache of <paramref name="totalBytes"/>,
    /// plus a non-stalling WriteBufferManager with a memtable sub-budget of
    /// <paramref name="memtableBudgetBytes"/> cost-charged to that same cache — so cache occupancy
    /// and memtable memory share one bound.
    ///
    /// <para>
    /// A <em>soft</em> (non-strict-capacity) cache is used deliberately: with memtables charged to
    /// the cache via the WBM, a hard capacity limit would surface as write errors or stalls whenever
    /// any database sharing the budget flushes a memtable. <c>allow_stall = false</c> on the WBM
    /// likewise prevents cross-database flush coupling from causing write stalls.
    /// </para>
    /// </summary>
    /// <param name="totalBytes">
    /// Total bytes for the shared block cache. The memtable sub-budget lives inside this budget, so
    /// this must be &gt;= <paramref name="memtableBudgetBytes"/>.
    /// </param>
    /// <param name="memtableBudgetBytes">
    /// Memtable sub-budget in bytes, cost-charged to the block cache. Must be &lt;=
    /// <paramref name="totalBytes"/>. Size conservatively: this bounds total memtable memory across
    /// <em>all</em> databases sharing this bundle; if the sum of per-CF write buffers across all
    /// sharing databases greatly exceeds this value, normal write bursts will trigger continuous
    /// cross-database flushing.
    /// </param>
    /// <exception cref="ArgumentOutOfRangeException">
    /// Thrown when <paramref name="memtableBudgetBytes"/> &gt; <paramref name="totalBytes"/>.
    /// </exception>
    public static RocksDbSharedResources CreateWithUnifiedBudget(long totalBytes, long memtableBudgetBytes)
    {
        if (memtableBudgetBytes > totalBytes)
            throw new ArgumentOutOfRangeException(
                nameof(memtableBudgetBytes),
                $"memtableBudgetBytes ({memtableBudgetBytes}) must be <= totalBytes ({totalBytes}): " +
                "the memtable sub-budget lives inside the cache budget.");

        Cache cache = Cache.CreateLru((ulong)totalBytes);
        IntPtr wbm = Native.Instance.rocksdb_write_buffer_manager_create_with_cache(
            (UIntPtr)memtableBudgetBytes,
            cache.Handle,
            allow_stall: 0);

        return new RocksDbSharedResources(cache, wbm);
    }

    /// <summary>
    /// Finalizer backstop: if the host forgets to call <see cref="Dispose"/>, the WBM native handle
    /// would otherwise leak (the <see cref="Cache"/> self-finalizes, but the raw <see cref="IntPtr"/>
    /// does not). The guard flag in <see cref="Dispose"/> makes this idempotent.
    /// </summary>
    ~RocksDbSharedResources() => Dispose();

    /// <summary>
    /// Drops the bundle's references to the native WBM and block cache. Because both objects are
    /// refcounted native <c>shared_ptr</c>s, disposal only releases this bundle's reference — open
    /// databases that received the handles at open time hold their own references and will not crash.
    /// Dispose order: WBM handle first, then the cache.
    /// </summary>
    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        GC.SuppressFinalize(this);

        // WBM first (it holds a shared_ptr to the cache; releasing it first lets the cache's refcount
        // drop cleanly in the next step).
        Native.Instance.rocksdb_write_buffer_manager_destroy(WriteBufferManagerHandle);
        Native.Instance.rocksdb_cache_destroy(BlockCache.Handle);
        GC.SuppressFinalize(BlockCache); // Cache has no Dispose; its finalizer also calls rocksdb_cache_destroy — suppress it so we don't double-free
    }
}
