
using System.Buffers;
using System.Diagnostics;
using Kommander.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text;
using Google.Protobuf;
using Kommander.Data;
using Kommander.Logging;
using Kommander.Time;
using Kommander.WAL.Protos;
using RocksDbSharp;
using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace Kommander.WAL;

/// <summary>
/// The RocksDbWAL class provides an implementation of a Write-Ahead Log (WAL) using RocksDB as the storage backend.
/// It supports functionality for reading, writing, compacting, and managing log metadata in a partitioned setup.
/// </summary>
public class RocksDbWAL : IWAL, IDisposable
{
    /// <summary>
    /// A static, reusable instance of <see cref="WriteOptions"/> configured to enable synchronous operations.
    /// This ensures data integrity by guaranteeing that write operations are fully persisted to disk
    /// before returning, at the potential cost of reduced write performance.
    /// </summary>
    private static readonly WriteOptions SynchronousWriteOptions = new WriteOptions().SetSync(true);

    private static readonly WriteOptions NonSynchronousWriteOptions = new WriteOptions().SetSync(false);

    /// <summary>
    /// Specifies the version of the format used for the RocksDb Write-Ahead Log (WAL) implementation.
    /// This constant is used to ensure compatibility by identifying the version of the metadata structure
    /// and log format maintained within the storage system.
    /// </summary>
    private const string FormatVersion = "2.0.0";

    /// <summary>
    /// Specifies the maximum number of shards supported by the Write-Ahead Log (WAL) implementation,
    /// representing the logical partitions used for segregating and managing log entries efficiently.
    ///
    /// Each shard represent a column family in RocksDB, allowing for concurrent access and
    /// concurrent compaction of the memtables in each shard.
    /// </summary>
    private const int MaxShards = 8;

    /// <summary>
    /// Specifies the fixed width, in bytes, used for encoding and storing
    /// unique identifiers within the Write-Ahead Log (WAL).
    /// This value is utilized to ensure consistent byte representation of IDs
    /// across various operations, such as reading, writing, and seeking log entries.
    ///
    /// This shouldn't be changed without a proper migration plan, as it would break the ordering of the logs.
    /// </summary>
    private const int IdWidth = 20;

    /// <summary>
    /// Specifies the fixed width, in bytes, used for encoding partition IDs into
    /// RocksDB keys. The width keeps all keys for a partition contiguous and
    /// lexicographically sorted by log ID within that partition.
    /// </summary>
    private const int PartitionIdWidth = 10;

    private const int LogKeyWidth = PartitionIdWidth + 1 + IdWidth;

    private const byte LogKeySeparator = (byte)':';

    private const byte PartitionUpperBoundSeparator = (byte)';';

    /// <summary>
    /// Represents a RocksDB instance used as the underlying storage engine
    /// for write-ahead logging (WAL). Provides efficient operations for
    /// persisting and retrieving logs with support for multiple column
    /// families and partitioning.
    ///
    /// <para>Not <c>readonly</c>: <see cref="ReopenEngine"/> replaces the instance when RocksDB latches
    /// a storage error it cannot clear on its own (see the engine-recovery region). Every access runs
    /// under the read side of <see cref="engineGuard"/>; the swap runs under its write side, which is
    /// what makes the replacement safe against in-flight iterators and writes.</para>
    /// </summary>
    private RocksDb db;

    /// <summary>
    /// Cached handle to the <c>metadata</c> column family. Resolved once at construction so the hot
    /// checkpoint-lookup / checkpoint-persist paths never re-resolve it per call. Re-resolved by
    /// <see cref="ReopenEngine"/>, because a handle belongs to the engine instance that issued it.
    /// </summary>
    private ColumnFamilyHandle metadataColumnFamily;

    private readonly string path;
    
    private readonly string revision;

    /// <summary>
    /// The options the engine was opened with, kept so that a recovery reopen uses exactly the
    /// configuration the constructor chose (shared block cache and write-buffer manager included).
    /// </summary>
    private readonly DbOptions dbOptions;

    /// <summary>Column-family descriptors the engine was opened with; see <see cref="dbOptions"/>.</summary>
    private readonly ColumnFamilies columnFamilies;

    /// <summary>The on-disk directory of this engine: <c>{path}/{revision}</c>.</summary>
    private readonly string enginePath;
    
    private readonly ConcurrentDictionary<int, Lazy<ColumnFamilyHandle>> families = new();

    /// <summary>
    /// Guards the compound read-modify-write operations against appends. The truncate/boundary ops
    /// (<c>TruncateLogsAfter</c>, <c>TruncateProposedLogsAfter</c>, <c>InstallSnapshotBoundary</c>)
    /// enumerate suffix keys and then delete them in a later batch — an append landing between the scan
    /// and the batch would survive the truncation. Those ops run on the read scheduler while appends run
    /// on the WAL write scheduler, so they are NOT otherwise mutually excluded.
    ///
    /// <para>A reader/writer lock rather than a mutex so that plain appends (the hot path) hold only the
    /// READ lock and may overlap: RocksDB is internally thread-safe per operation and its WriteThread
    /// coalesces concurrent synced writes into one write group with a single fsync — a mutex here would
    /// serialize the fsyncs and defeat group commit across scheduler workers. The scan+delete ops take the
    /// WRITE lock, preserving the original exclusion. Appends that carry a <c>CommittedCheckpoint</c> also
    /// take the WRITE lock: they read-modify-write the persisted last-checkpoint metadata
    /// (<c>GetLastCheckpointFromMeta</c> + <c>Math.Max</c> staged into the batch), and two overlapping
    /// same-partition checkpoint writers could otherwise regress the recorded id if their <c>db.Write</c>
    /// calls complete out of order. Checkpoints are rare (once per checkpoint interval), so the hot append
    /// path keeps full concurrency.</para>
    /// </summary>
    private readonly ReaderWriterLockSlim writeGuard = new(LockRecursionPolicy.NoRecursion);

    // ── Persisted compaction floor (logical deletion of the compacted prefix) ────────────────────
    //
    // Field finding (2026-09-10 write probes w2–w4 and the eight tuning arms): after the commit
    // frontier removed the second full-payload write, the Raft log still paid one full compaction
    // rewrite per flushed byte, and no memtable/L0 sizing changed it. The cause is the range
    // tombstone itself. A tombstone written by the compaction pass covers keys that live in OLDER
    // files, so every L0 file's key range starts below its own first entry and overlaps every other
    // file of the partition. RocksDB then cannot trivially move L0 files into the base level (it
    // must merge), each L0→base compaction reads and rewrites the whole live window, and
    // DeleteFilesInRange — which only drops files that lie ENTIRELY below its bound — never
    // qualifies an L0 file, because each one straddles the floor. Under a starved shared
    // WriteBufferManager (every production flush was "Write Buffer Manager"-forced at ~6 MB) the
    // entries also flushed long before their tombstone arrived, so almost nothing died in memory.
    //
    // The layout here removes the cross-file tombstone. Compaction persists a per-partition
    // COMPACTION FLOOR (metadata key raft_compaction_floor_p{id}): every row with id < floor is
    // logically deleted, and every read path clamps at it. Physical reclamation is whole-file:
    // with no cross-file tombstones, an append-only partition produces strictly non-overlapping
    // SST files, RocksDB moves them from L0 to the base level as a metadata-only trivial move, and
    // the floor-anchored DeleteFilesInRange drops each file once the floor passes its largest key.
    // No log row is ever rewritten by compaction. The pass still writes a range tombstone, but
    // CONFINED to the span of the active memtable (see ConfinedTombstoneStart): entries that die
    // while still in memory are dropped at flush and never reach disk, and because the tombstone
    // starts at or above the memtable's own first entry it never widens the flushed file's key
    // range below its first row, so the non-overlap property (and the trivial move) is preserved.
    //
    // Rules that keep the logical floor exact:
    //  * Only CompactLogsOlderThan raises it, and only to a value <= its lastCheckpoint argument, so
    //    the checkpoint row (and everything above) stays visible. It is persisted in the same batch
    //    as the confined tombstone, BEFORE DeleteFilesInRange runs, so a crash between the two
    //    leaves files that the next pass drops again — never a file dropped ahead of its floor.
    //  * TruncateLogsAfter clamps it to afterLogId + 1 and InstallSnapshotBoundary to the boundary
    //    index, so a row later written at a truncated id can never sit below the floor and read as
    //    absent. DeletePartitionWAL deletes the key; ReopenEngine clears the cache.
    //  * A CF shared by several partitions (partition ids congruent modulo MaxShards) cannot use
    //    whole-file drops for one partition's dead prefix — the files hold the other partitions'
    //    live rows — so the pass writes the full-range tombstone there, exactly as before.
    //  * An older binary ignores the key and reads the dead-but-present rows as ordinary log rows.
    //    They are genuine, applied entries below a durable checkpoint, so that is merely retention,
    //    not corruption; its own compaction pass tombstones them normally. No format bump.

    /// <summary>Cache for <see cref="CompactionFloorKey"/>; key bytes are immutable once built.</summary>
    private readonly ConcurrentDictionary<int, byte[]> compactionFloorKeys = new();

    /// <summary>
    /// In-memory copy of the persisted compaction floor per partition (0 = nothing compacted).
    /// Raised only after the advancing batch was applied; lowered exactly under the exclusive
    /// write guard by the truncation paths; cleared by <see cref="ReopenEngine"/>.
    /// </summary>
    private readonly ConcurrentDictionary<int, long> compactionFloorCache = new();

    /// <summary>
    /// Shard-row bytes written since the metadata column family was last flushed by
    /// <see cref="NoteShardBytesWritten"/>; compared against <see cref="metadataFlushEveryBytes"/>.
    /// </summary>
    private long shardBytesSinceMetadataFlush;

    /// <summary>
    /// Shard-row bytes after which the metadata CF is flushed so RocksDB can release its
    /// write-ahead log files. The metadata CF receives one tiny put per Raft batch (the commit
    /// frontier) and one per compaction pass (the floor) and never fills its memtable on its own,
    /// yet a write-ahead log stays alive until EVERY column family with data in it has flushed —
    /// so without this the metadata memtable pinned every log since its last flush (measured:
    /// 454 MB of <c>.log</c> for 3 MB of SST after 150 s locally; 2.5–3.5 GB per node against
    /// 0.2–0.9 GB live in the k175 probes), growing the on-disk log and restart replay without
    /// bound. One shard flush unit (<c>ShardWriteBufferSizeBytes × ShardMinWriteBufferNumberToMerge</c>)
    /// keeps the alive logs at about two units. The flush is asynchronous (a memtable switch plus
    /// a background job writing a few-KB SST) and touches no shard memtable.
    /// </summary>
    private readonly long metadataFlushEveryBytes;

    /// <summary>
    /// Headroom subtracted from the active-memtable span when confining a compaction tombstone.
    /// <c>rocksdb.num-entries-active-mem-table</c> counts every memtable insertion, including the
    /// tombstones and point deletes this WAL writes, so the span it implies can reach a few ids
    /// below the memtable's first row; starting the tombstone this many ids higher keeps it from
    /// covering a key that lives in an already-flushed file (which would make the next flushed
    /// file overlap that file and cost one real compaction). Entries left uncovered by the margin
    /// simply flush and are reclaimed by the whole-file drop instead.
    /// </summary>
    private const long ConfinedTombstoneMargin = 64;

    /// <summary>
    /// Test-only hook fired inside <see cref="InstallSnapshotBoundary"/> after the suffix scan and while
    /// <see cref="writeGuard"/> is held, immediately before the batch write. Lets a concurrency test prove a
    /// racing append is excluded by the guard. Null (no-op) in production.
    /// </summary>
    internal Action? OnAfterBoundaryScanForTesting;

    private readonly ILogger<IRaft> logger;

    private readonly WriteOptions writeOptions;

    private readonly bool syncWrites;

    private readonly IMonotonicTickSource tickSource;

    internal bool SyncWritesEnabled => syncWrites;

    /// <summary>For tests: number of <c>db.Write</c> calls issued by the last compaction call.</summary>
    internal int LastCompactionWriteCount { get; private set; }

    /// <summary>
    /// For tests: number of <c>Committed</c> markers this instance absorbed into the persisted
    /// commit frontier instead of rewriting the full entry. Incremented only after the absorbing
    /// batch was applied, so the count never runs ahead of durable state.
    /// </summary>
    internal long CommitMarkersAbsorbed { get; private set; }
    
    /// <summary>
    /// Opens a RocksDB WAL at <paramref name="path"/>/<paramref name="revision"/>.
    ///
    /// <para>
    /// When <paramref name="sharedResources"/> is non-null, the block cache and WriteBufferManager it
    /// carries are applied to this database before <c>RocksDb.Open</c> — the block cache to every column
    /// family, the WBM to the <see cref="DbOptions"/> — so this WAL shares a unified memory budget with
    /// any other database that received the same bundle. The bundle is <em>borrowed</em>: this constructor
    /// does not take ownership and must not dispose it; the caller owns the bundle's lifetime.
    /// </para>
    ///
    /// <para>
    /// When <paramref name="sharedResources"/> is null, behavior is byte-for-byte identical to the
    /// no-arg default: no block cache is configured and RocksDB's built-in defaults apply.
    /// </para>
    ///
    /// <para>
    /// <paramref name="tuning"/> sizes the shard column families (see <see cref="RocksDbWalTuning"/>);
    /// null applies <see cref="RocksDbWalTuning.Default"/>. Throws
    /// <see cref="ArgumentOutOfRangeException"/> when the tuning violates the stall-lock guard
    /// (<c>ShardMaxWriteBufferNumber &lt;= ShardMinWriteBufferNumberToMerge</c>) — that
    /// configuration write-stalls on every memtable rotation, which is worse than any sizing it
    /// could buy.
    /// </para>
    /// </summary>
    public RocksDbWAL(
        string path,
        string revision,
        ILogger<IRaft> logger,
        bool syncWrites = true,
        RocksDbSharedResources? sharedResources = null,
        IMonotonicTickSource? tickSource = null,
        RocksDbWalTuning? tuning = null)
    {
        // Validated before the path is composed below: the revision is a single directory name under
        // the WAL path, and a separator or relative segment would silently open the database
        // somewhere else entirely.
        WalStoragePaths.ValidateRevision(revision, nameof(revision));

        tuning ??= RocksDbWalTuning.Default;
        if (tuning.ShardMaxWriteBufferNumber <= tuning.ShardMinWriteBufferNumberToMerge)
            throw new ArgumentOutOfRangeException(nameof(tuning),
                $"ShardMaxWriteBufferNumber ({tuning.ShardMaxWriteBufferNumber}) must exceed " +
                $"ShardMinWriteBufferNumberToMerge ({tuning.ShardMinWriteBufferNumberToMerge}); " +
                "a flush waits for the merge quorum, so the writer needs one memtable above it.");

        this.path = path;
        this.revision = revision;
        this.logger = logger;
        this.syncWrites = syncWrites;
        this.metadataFlushEveryBytes = Math.Max(1, tuning.ShardWriteBufferSizeBytes * tuning.ShardMinWriteBufferNumberToMerge);
        this.writeOptions = syncWrites ? SynchronousWriteOptions : NonSynchronousWriteOptions;
        this.tickSource = tickSource ?? SystemMonotonicTickSource.Instance;

        DbOptions dbOptions = new DbOptions()
            .SetCreateIfMissing(true)
            .SetCreateMissingColumnFamilies(true)
            .SetAllowConcurrentMemtableWrite(true)
            // A torn record at the tail of the RocksDB log is the normal residue of an unclean
            // shutdown: the process died between the write syscall and its completion, so the last
            // record is short. Recovery.AbsoluteConsistency refuses to open on it, which turns a
            // single SIGKILL into a permanently unopenable data directory — the node crash-loops,
            // stays a voter that never returns, and the next fault takes the cluster below quorum.
            //
            // TolerateCorruptedTailRecords (RocksDB's own default) drops that record and opens.
            // Dropping it loses nothing that was promised: with syncWrites the fsync completes
            // before the caller is acknowledged, so a record that is short was never acknowledged.
            // Restore then rebuilds commitIndex from the contiguous committed prefix and the leader
            // re-supplies the rest — the input this mode hands it is exactly what that path expects.
            // With syncWrites off there is no such guarantee, but that configuration already accepts
            // the loss of writes the operating system had not flushed, and a WAL that cannot open at
            // all is the worse outcome there too.
            //
            // This is not a blanket "ignore corruption". A short body can only occur at the end of
            // the file. A checksum mismatch with whole records after it is real damage, and RocksDB
            // still refuses to open on it in this mode. Recovery.SkipAnyCorruptedRecords is the mode
            // that would swallow that, and it is not used here.
            .SetWalRecoveryMode(Recovery.TolerateCorruptedTailRecords);

        // When sharing resources, apply the WBM to the DbOptions before opening. The block cache is
        // applied per-CF below. Both must be set before RocksDb.Open — they cannot be changed afterward.
        if (sharedResources is not null)
            Native.Instance.rocksdb_options_set_write_buffer_manager(
                dbOptions.Handle, sharedResources.WriteBufferManagerHandle);

        // One shared BlockBasedTableOptions referencing the shared block cache, applied to every CF so
        // none are excluded from the shared budget. When sharedResources is null, CFs keep defaults.
        //
        // Write-buffer sizing when sharing: this WAL has ~10 CFs. With the WBM bounding total memtable
        // memory across all sharing databases, per-CF write_buffer_size × max_write_buffer_number × CF
        // count should be a modest fraction of memtableBudgetBytes to leave headroom for the host DB.
        // The default tuning (64 MB write_buffer_size, 4 max_write_buffer_number on the 8 shard CFs;
        // RocksDB defaults elsewhere) gives ~2.3 GB across 10 CFs on paper — far above any typical WBM
        // budget, but only actively-written CFs materialize memtables (typically 1–2 shards) and an
        // over-budget WBM flushes early, which degrades the flush-unit sizing back toward the old
        // behavior instead of growing memory. Hosts sharing a WBM should size the budget (or pass a
        // smaller RocksDbWalTuning), or accept frequent cross-CF/cross-DB flush coupling.
        BlockBasedTableOptions? sharedBbto = sharedResources is not null
            ? new BlockBasedTableOptions().SetBlockCache(sharedResources.BlockCache)
            : null;

        // More compaction/flush workers than the RocksDB default of 2: with ten column families on
        // a multi-core host, two background jobs cannot drain L0 under sustained log append, and
        // the resulting l0-file-count slowdowns land directly in the Raft quorum path (measured as
        // "Stalling writes because we have 20 level-0 files" on all three nodes of the 2026-09-09
        // write probe). Threads are lazy in RocksDB, so an idle deployment pays nothing.
        Native.Instance.rocksdb_options_set_max_background_jobs(
            dbOptions.Handle, Math.Clamp(Environment.ProcessorCount / 2, 2, 8));

        ColumnFamilies columnFamilies = new()
        {
            { "default", ApplyCfOptions(new(), sharedBbto) },
            { "metadata", ApplyCfOptions(new(), sharedBbto) }
        };

        for (int i = 0; i < MaxShards; i++)
            columnFamilies.Add("shard" + i, ApplyShardCfOptions(new(), sharedBbto, tuning));

        string completePath = $"{path}/{revision}";

        bool firstTime = !Directory.Exists(completePath);

        // Created here, restricted to the owning user, rather than left to RocksDb.Open: the engine
        // creates and rotates many files inside this directory, so restricting the directory itself
        // is what actually covers the replicated state at rest. Must run before Open, and the
        // firstTime probe above must run before this, since creating the directory would answer it.
        WalStoragePaths.EnsureDirectory(completePath);

        this.dbOptions = dbOptions;
        this.columnFamilies = columnFamilies;
        enginePath = completePath;

        db = RocksDb.Open(dbOptions, completePath, columnFamilies);

        metadataColumnFamily = db.GetColumnFamily("metadata");

        // Registered (weakly) for the raft.wal.delayed_write_rate / raft.wal.write_stalled gauges,
        // so an engine-level write stall in the Raft path is visible in ordinary node telemetry
        // rather than only inside the RocksDB LOG file.
        KommanderMetrics.RegisterWalEngine(this);

        WarnIfSharedBudgetStarvesFlushUnit(sharedResources, tuning);

        if (firstTime)
            SetMetaData("version", FormatVersion);
        else
        {
            string? currentVersion = GetMetaData("version");

            if (!string.Equals(currentVersion, FormatVersion, StringComparison.Ordinal))
            {
                db.Dispose();
                throw new InvalidOperationException(
                    $"RocksDB WAL format version '{currentVersion ?? "<missing>"}' is not compatible with '{FormatVersion}'. " +
                    "Create a fresh WAL directory or migrate the existing data before opening it."
                );
            }
        }
    }


    // ── Engine recovery after a latched storage error ────────────────────────────────────────────
    //
    // Field incident (2026-09-01): the data volume filled, the operator freed space while the process
    // kept running, and the node never recovered. Every election attempt failed inside PersistHardState
    // with the SAME RocksDB error naming the SAME WAL file, for many minutes, until a process restart.
    //
    // RocksDB keeps one background-error status per engine. A failed WAL *append* ("While appending
    // to file") is a recoverable error: the engine's built-in SstFileManager polls free space and
    // resumes the engine on its own once space is back (measured at ~5 s on this binding). A failed
    // WAL *file creation* is different. The memtable switch that every flush performs opens a fresh
    // log file, and "While open a file for appending" is that open. RocksDB classifies a failure there
    // as an unrecoverable memtable error: automatic recovery never runs for it, and even a manual
    // resume is refused. From then on every write returns the cached status without touching the
    // disk, which is why the error kept naming a log file long after the space was freed. Under a
    // full disk the switch is reached from ordinary load (a memtable fills, or the shared
    // write-buffer manager asks for a flush), so the latch is the expected end state of any
    // sustained ENOSPC episode, not a rare corner.
    //
    // The only way out is to close the engine and open it again. That is safe: every acknowledged
    // sync write is already fsynced, and un-synced records were handed to the operating system on
    // append (RocksDB flushes its writer buffer per record), so a same-process reopen replays the log
    // files from disk or page cache and loses nothing that was acknowledged. The failed batch itself
    // was already reported as Errored, and the Raft layer regressed its frontiers for it
    // (RaftWriteAhead.RegressFrontiersAfterFailedWriteAsync).
    //
    // Detection does not parse error strings. After a write-path failure the WAL asks two questions:
    //   1. Does the filesystem accept writes now?  A probe file: create, write, fsync, delete.
    //   2. Does the engine accept writes now?      A canary put into the metadata column family.
    // "Filesystem yes, engine no" is a latched engine, and only then is it reopened. A still-full
    // disk fails question 1, so the WAL keeps reporting Errored and the Raft layer keeps retrying on
    // its own cadence. An engine that resumed by itself passes question 2 and is left alone. The
    // reopen runs under the exclusive side of engineGuard, so no iterator, read, or write can touch
    // the handle being replaced. Every write-path entry point retries its operation once after a
    // successful recovery; all of them are idempotent upserts or deletes, so the retry is safe.

    /// <summary>
    /// Fences the engine handle: every public operation holds the read side for its duration, and
    /// <see cref="ReopenEngine"/> / <see cref="Dispose"/> take the write side. The policy is
    /// <see cref="LockRecursionPolicy.NoRecursion"/> — an audit of every <see cref="AcquireEngine"/>
    /// caller found no nested acquisition (compositions such as
    /// <see cref="TruncateLogsAfterAndGetMax"/> take and release the lease once per step,
    /// sequentially), so a nested acquire can only be a future bug, and this policy fails it fast
    /// with a <see cref="LockRecursionException"/> instead of silently re-entering — the safer
    /// default for a fence that guards a native handle. The policies measure within half a
    /// nanosecond of each other per enter/exit pair (see <c>WalEngineFenceBenchmarks</c>), so the
    /// choice is about the failure mode, not cost.
    /// </summary>
    private readonly ReaderWriterLockSlim engineGuard = new(LockRecursionPolicy.NoRecursion);

    /// <summary>
    /// True while <see cref="db"/> is not a usable handle: after <see cref="Dispose"/>, and between a
    /// close and a successful reopen when the reopen itself failed. Public operations throw a
    /// <see cref="RaftException"/> in that state; the next write-path failure retries the reopen.
    /// </summary>
    private volatile bool engineClosed;

    /// <summary>The exception of the most recent write-path failure; consumed by <see cref="TryRecoverEngineAfterFailure"/>.</summary>
    private Exception? lastStorageFailure;

    private long lastProbeTicks;

    /// <summary>Consecutive write-path failures since the last success; drives the sustained-failure log cadence.</summary>
    private int failureStreak;

    private long failureStreakStartTicks;

    private long lastFailureLogTicks;

    /// <summary>Number of successful engine reopens performed by recovery. Exposed for tests.</summary>
    internal int EngineReopenCount { get; private set; }

    /// <summary>Message of the most recent write-path failure, or <see langword="null"/>. Exposed for tests.</summary>
    internal string? LastStorageFailureMessage => Volatile.Read(ref lastStorageFailure)?.Message;

    /// <summary>Metadata key the engine canary writes. Its value carries no meaning.</summary>
    private static readonly byte[] EngineCanaryKey = "kommander_engine_canary"u8.ToArray();

    /// <summary>
    /// Name of the filesystem probe file, created inside the engine directory so the probe measures the
    /// volume the log actually lives on. RocksDB ignores files whose names it does not recognise, and
    /// the probe is deleted right after it is written in any case.
    /// </summary>
    private const string SpaceProbeFileName = ".kommander-space-probe";

    /// <summary>
    /// Bytes the filesystem probe writes. Large enough that a volume with a few stray free blocks does
    /// not pass, small enough that a probe is cheap; RocksDB itself needs far more than this to flush,
    /// so a passing probe followed by a failing reopen is still handled (the reopen retries later).
    /// </summary>
    private const int SpaceProbeBytes = 64 * 1024;

    /// <summary>Minimum spacing between filesystem probes, so a flood of failing writers does not flood the disk with probes.</summary>
    private static readonly TimeSpan ProbeInterval = TimeSpan.FromSeconds(1);

    /// <summary>Cadence of the "still failing" warning while writes keep failing.</summary>
    private static readonly TimeSpan SustainedFailureLogInterval = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Read-side lease on <see cref="engineGuard"/>, released by <c>using</c>. A <c>struct</c> so the hot
    /// paths pay no allocation for the fence.
    /// </summary>
    private readonly struct EngineLease : IDisposable
    {
        private readonly ReaderWriterLockSlim guard;

        public EngineLease(ReaderWriterLockSlim guard)
        {
            this.guard = guard;
            guard.EnterReadLock();
        }

        public void Dispose() => guard.ExitReadLock();
    }

    /// <summary>
    /// Takes the read-side lease and verifies the engine is open. Throws when it is closed so a caller
    /// never touches a disposed handle; write-path entry points catch this like any other failure and
    /// route it into recovery, read paths let it propagate to their caller.
    /// </summary>
    private EngineLease AcquireEngine()
    {
        EngineLease lease = new(engineGuard);

        if (!engineClosed)
            return lease;

        lease.Dispose();
        throw new RaftException(
            $"RocksDB WAL at '{enginePath}' is closed after a storage failure and has not been reopened yet; "
            + "the next write retries the reopen.");
    }

    /// <summary>
    /// Records a write-path failure. The first failure of a streak logs a warning; later ones log again
    /// every <see cref="SustainedFailureLogInterval"/> with the streak's duration so an operator can see a
    /// disk that stays full, without one line per failed batch.
    /// </summary>
    private void NoteStorageFailure(Exception ex)
    {
        Volatile.Write(ref lastStorageFailure, ex);

        long now = tickSource.GetTimestamp();
        int streak = Interlocked.Increment(ref failureStreak);

        if (streak == 1)
        {
            Volatile.Write(ref failureStreakStartTicks, now);
            Volatile.Write(ref lastFailureLogTicks, now);

            logger.LogWarning(
                "RocksDB WAL at '{Path}' rejected a write: {Message}. Raft frontiers regress to the durable log; the WAL probes the filesystem on each failure and reopens the engine once space is back",
                enginePath, ex.Message);
            return;
        }

        long lastLog = Volatile.Read(ref lastFailureLogTicks);

        if (tickSource.GetElapsedTime(lastLog, now) < SustainedFailureLogInterval)
            return;

        Volatile.Write(ref lastFailureLogTicks, now);

        logger.LogWarning(
            "RocksDB WAL at '{Path}' has rejected writes for {Duration} ({Count} failures); last error: {Message}",
            enginePath, tickSource.GetElapsedTime(Volatile.Read(ref failureStreakStartTicks), now), streak, ex.Message);
    }

    /// <summary>Closes a failure streak. One volatile read on the hot path when there is no streak.</summary>
    private void NoteStorageSuccess()
    {
        if (Volatile.Read(ref failureStreak) == 0)
            return;

        int streak = Interlocked.Exchange(ref failureStreak, 0);

        if (streak == 0)
            return;

        if (logger.IsEnabled(LogLevel.Information))
        {
            logger.LogInfoRocksDbWalAcceptsWritesAgain(
                enginePath, streak, tickSource.GetElapsedTime(Volatile.Read(ref failureStreakStartTicks), tickSource.GetTimestamp()));
        }
    }

    /// <summary>
    /// Called by every write-path entry point after its core returned <see cref="RaftOperationStatus.Errored"/>.
    /// Returns <see langword="true"/> when the caller should retry its operation once: the engine was
    /// reopened, or it turned out to accept writes again on its own. Returns <see langword="false"/> when
    /// the failure was not a storage fault, when the filesystem still refuses writes, when the probe is
    /// rate-limited, or when the reopen failed.
    /// <para>Must be called WITHOUT the engine lease held: it takes the write side of
    /// <see cref="engineGuard"/> to swap the handle.</para>
    /// </summary>
    private bool TryRecoverEngineAfterFailure()
    {
        Exception? failure = Volatile.Read(ref lastStorageFailure);

        // A managed exception (a bug, an invalid argument) is not a storage fault; reopening the engine
        // for it would hide the bug behind a recovery log line. A closed engine always qualifies.
        if (failure is not RocksDbException && !engineClosed)
            return false;

        if (!FilesystemAcceptsWrites())
            return false;

        engineGuard.EnterWriteLock();
        try
        {
            if (!engineClosed && EngineAcceptsWrites())
            {
                logger.LogInfoRocksDbWalResumedOnItsOwn(enginePath);
                return true;
            }

            return ReopenEngine(failure);
        }
        finally
        {
            engineGuard.ExitWriteLock();
        }
    }

    /// <summary>
    /// Question 1 of recovery: can the volume take a small durable write right now? Rate-limited to one
    /// probe per <see cref="ProbeInterval"/> across all failing callers; a rate-limited call answers
    /// <see langword="false"/> and the next failure asks again.
    /// </summary>
    private bool FilesystemAcceptsWrites()
    {
        long now = tickSource.GetTimestamp();
        long last = Volatile.Read(ref lastProbeTicks);

        if (last != 0 && tickSource.GetElapsedTime(last, now) < ProbeInterval)
            return false;

        if (Interlocked.CompareExchange(ref lastProbeTicks, now, last) != last)
            return false;

        string probePath = Path.Combine(enginePath, SpaceProbeFileName);

        try
        {
            using (FileStream stream = new(probePath, FileMode.Create, FileAccess.Write, FileShare.None, bufferSize: 1))
            {
                stream.Write(new byte[SpaceProbeBytes]);
                stream.Flush(flushToDisk: true);
            }

            return true;
        }
        catch (IOException)
        {
            return false;
        }
        catch (UnauthorizedAccessException)
        {
            return false;
        }
        finally
        {
            try { File.Delete(probePath); }
            catch (IOException) { }
            catch (UnauthorizedAccessException) { }
        }
    }

    /// <summary>
    /// Question 2 of recovery: does the engine accept a durable write right now? A latched engine returns
    /// its cached error without touching the disk. Must hold the write side of <see cref="engineGuard"/>.
    /// </summary>
    private bool EngineAcceptsWrites()
    {
        try
        {
            db.Put(EngineCanaryKey, BitConverter.GetBytes(tickSource.GetTimestamp()), metadataColumnFamily, SynchronousWriteOptions);
            return true;
        }
        catch (RocksDbException)
        {
            return false;
        }
    }

    /// <summary>
    /// Closes the current engine and opens a fresh one on the same directory with the same options. Must
    /// hold the write side of <see cref="engineGuard"/>. When the open fails the WAL stays closed
    /// (<see cref="engineClosed"/>): operations throw or return Errored, and the next write-path failure
    /// runs this again. There is no way to keep the old handle as a fallback, because RocksDB's directory
    /// lock permits one open engine per directory per process.
    /// </summary>
    private bool ReopenEngine(Exception? cause)
    {
        if (!engineClosed)
        {
            engineClosed = true;

            // Column-family handles belong to the engine instance that issued them.
            families.Clear();

            // The cached commit frontiers and compaction floors describe the engine instance being
            // closed; the reopened engine reloads them from its metadata CF, which reflects exactly
            // what was replayed.
            commitFrontierCache.Clear();
            compactionFloorCache.Clear();

            try
            {
                db.Dispose();
            }
            catch (Exception ex)
            {
                logger.LogWarning("RocksDB WAL at '{Path}': close before reopen reported {Message}", enginePath, ex.Message);
            }
        }

        try
        {
            db = RocksDb.Open(dbOptions, enginePath, columnFamilies);
            metadataColumnFamily = db.GetColumnFamily("metadata");
            engineClosed = false;
            EngineReopenCount++;

            KommanderMetrics.RecordWalEngineReopen(succeeded: true);

            logger.LogWarning(
                "RocksDB WAL at '{Path}' was reopened to clear a latched storage error (reopen #{Count}); the error was: {Message}",
                enginePath, EngineReopenCount, cause?.Message ?? "<engine closed>");

            return true;
        }
        catch (Exception ex)
        {
            KommanderMetrics.RecordWalEngineReopen(succeeded: false);

            logger.LogCritRocksDbWalReopenFailed(enginePath, ex.Message);

            return false;
        }
    }

    /// <summary>
    /// Bytes/second RocksDB is currently throttling writers to, or 0 when no slowdown is active.
    /// Read from the engine's <c>rocksdb.actual-delayed-write-rate</c> property for the stall
    /// gauge; never throws — a closed or failing engine reports 0 (no stall evidence, not "healthy").
    /// </summary>
    internal long GetActualDelayedWriteRate() => ReadIntegerProperty("rocksdb.actual-delayed-write-rate");

    /// <summary>1 while RocksDB has stopped writes entirely (the hard stall), else 0. See <see cref="GetActualDelayedWriteRate"/>.</summary>
    internal long GetIsWriteStopped() => ReadIntegerProperty("rocksdb.is-write-stopped");

    /// <summary>
    /// Total live SST bytes across the shard column families (the Raft-log CFs). On an
    /// append-then-delete log this should track the retained window, not grow without bound; a
    /// value far above the live-entry count means dead files are not being reclaimed — the L6
    /// non-reclaim <see cref="DropFullyDeadFiles"/> targets. Fed to <c>raft.wal.shard_live_sst_bytes</c>.
    /// </summary>
    internal long GetShardLiveSstBytes() => SumShardProperty("rocksdb.live-sst-files-size");

    /// <summary>
    /// Total SST files in the bottom level (L6) across the shard CFs, fed to
    /// <c>raft.wal.shard_l6_files</c>. On the append-then-delete log this is the count that stayed
    /// flat at ~12 files / 844 MB through write probe w3 while the data below the floor was dead —
    /// the signal that whole-file drops were not landing.
    /// </summary>
    internal long GetShardLevel6FileCount() => SumShardProperty("rocksdb.num-files-at-level6");

    /// <summary>
    /// Total SST files in L0 across the shard CFs, fed to <c>raft.wal.shard_l0_files</c>. Under the
    /// floor layout L0 files are moved to the base level (not rewritten) once
    /// <see cref="RocksDbWalTuning.ShardLevel0FileNumCompactionTrigger"/> of them accumulate and are
    /// dropped whole from there, so this should oscillate below the trigger; a value that climbs
    /// toward the slowdown trigger means moves are not happening (overlapping files — a shared
    /// shard, or a burst of truncations) and the engine is compacting for real.
    /// </summary>
    internal long GetShardLevel0FileCount() => SumShardProperty("rocksdb.num-files-at-level0");

    /// <summary>
    /// Logs once, at open, when a shared WriteBufferManager budget cannot hold the flush unit the
    /// shard tuning asks for. RocksDB flushes whichever memtable is oldest in the database that
    /// trips the budget, and this WAL trips it far more often than a co-hosted store (one write per
    /// Raft batch), so an undersized budget quietly replaces the configured 64 MB × merge-2 unit
    /// with a few-megabyte one — every flush in the 2026-09-10 write probes was budget-forced at
    /// ~6 MB. The floor layout keeps write amplification at ~1 regardless, but entries that would
    /// have died in memory then reach disk once; the operator should know which regime they run in.
    /// </summary>
    private void WarnIfSharedBudgetStarvesFlushUnit(RocksDbSharedResources? sharedResources, RocksDbWalTuning tuning)
    {
        if (sharedResources is null)
            return;

        long budget;
        try
        {
            budget = (long)(ulong)Native.Instance.rocksdb_write_buffer_manager_buffer_size(sharedResources.WriteBufferManagerHandle);
        }
        catch (Exception)
        {
            return;
        }

        // One flush unit for a single active shard plus one memtable of headroom for whatever
        // shares the budget; below this the shard memtable is flushed on the budget's cadence,
        // never on its own size.
        long flushUnit = tuning.ShardWriteBufferSizeBytes * tuning.ShardMinWriteBufferNumberToMerge;
        long needed = flushUnit + tuning.ShardWriteBufferSizeBytes;

        if (budget > 0 && budget < needed)
            logger.LogWarning(
                "RocksDB WAL at '{Path}': the shared memtable budget ({BudgetMb} MB) is below the shard flush unit plus headroom ({NeededMb} MB); RocksDB will flush the Raft log on the budget's cadence instead of at {FlushUnitMb} MB, so entries that would die in memory reach disk once. Raise the shared memtable budget or lower the shard write-buffer tuning",
                enginePath, budget >> 20, needed >> 20, flushUnit >> 20);
    }

    private long ReadIntegerProperty(string property)
    {
        if (engineClosed)
            return 0;

        try
        {
            using EngineLease lease = AcquireEngine();
            return long.TryParse(db.GetProperty(property), out long value) ? value : 0;
        }
        catch (Exception)
        {
            return 0;
        }
    }

    /// <summary>
    /// Sums a per-CF integer RocksDB property over the shard column families. A closed/failing
    /// engine reports 0 (no reclaim evidence, not "healthy"), like the stall readers above.
    /// </summary>
    private long SumShardProperty(string property)
    {
        if (engineClosed)
            return 0;

        try
        {
            using EngineLease lease = AcquireEngine();
            long sum = 0;
            for (int i = 0; i < MaxShards; i++)
            {
                ColumnFamilyHandle cf = db.GetColumnFamily("shard" + i);
                if (long.TryParse(db.GetProperty(property, cf), out long value))
                    sum += value;
            }
            return sum;
        }
        catch (Exception)
        {
            return 0;
        }
    }

    /// <summary>
    /// Test-only: forces a memtable switch on every column family that holds data, which makes RocksDB
    /// create a fresh WAL file. On a full volume that creation fails and the engine latches the
    /// unrecoverable error this recovery region exists for. Throws the engine's error to the caller.
    /// </summary>
    internal void FlushMemTablesForTesting() => FlushMemTablesForTesting(wait: false);

    /// <summary>
    /// Test-only flush with an explicit wait flag. <paramref name="wait"/> = true blocks until the
    /// memtables are written as SST files, so a test can then read file-level stats
    /// (<see cref="GetShardLiveSstBytes"/>) deterministically; the default (false) preserves the
    /// fire-and-forget switch the ENOSPC recovery tests rely on.
    /// </summary>
    internal void FlushMemTablesForTesting(bool wait)
    {
        using EngineLease lease = AcquireEngine();
        FlushOptions flushOptions = new();
        Native.Instance.rocksdb_flushoptions_set_wait(flushOptions.Handle, (byte)(wait ? 1 : 0));

        Native.Instance.rocksdb_flush_cf(db.Handle, flushOptions.Handle, metadataColumnFamily.Handle);
        Native.Instance.rocksdb_flush_cf(db.Handle, flushOptions.Handle, db.GetColumnFamily("default").Handle);

        for (int i = 0; i < MaxShards; i++)
            Native.Instance.rocksdb_flush_cf(db.Handle, flushOptions.Handle, db.GetColumnFamily("shard" + i).Handle);
    }

    /// <summary>
    /// Retrieves the column family handle for the specified partition ID from the internal collection,
    /// creating and storing it lazily if it does not already exist.
    /// </summary>
    /// <param name="partitionId">
    /// The ID of the partition for which the column family handle is to be retrieved.
    /// </param>
    /// <returns>
    /// The instance of <see cref="ColumnFamilyHandle"/> corresponding to the specified partition.
    /// </returns>
    private ColumnFamilyHandle GetColumnFamily(int partitionId)
    {
        Lazy<ColumnFamilyHandle> lazy = families.GetOrAdd(partitionId, GetColumnFamilyHandle);
        return lazy.Value;
    }

    /// <summary>
    /// Retrieves a lazy-loaded column family handle for the specified partition ID.
    /// </summary>
    /// <param name="arg">
    /// The ID of the partition for which the column family handle is to be retrieved.
    /// </param>
    /// <returns>
    /// A lazy-loaded instance of <see cref="ColumnFamilyHandle"/> corresponding to the specified partition.
    /// </returns>
    private Lazy<ColumnFamilyHandle> GetColumnFamilyHandle(int arg)
    {
        return new(() =>
        {
            int shardId = arg % MaxShards;
            return db.GetColumnFamily("shard" + shardId);
        });
    }

    /// <summary>
    /// Reads all logs from the Write-Ahead Log (WAL) for the specified partition.
    /// </summary>
    /// <param name="partitionId">
    /// The ID of the partition from which to read the logs.
    /// </param>
    /// <returns>
    /// A list of <see cref="RaftLog"/> instances representing the logs read from the specified partition.
    /// </returns>
    public List<RaftLog> ReadLogs(int partitionId)
    {
        using EngineLease lease = AcquireEngine();

        List<RaftLog> result = [];

        ColumnFamilyHandle columnFamilyHandle = GetColumnFamily(partitionId);

        // O(1) point lookup — the replay floor is persisted in the metadata CF and kept in sync with every
        // checkpoint write, so restore no longer pays a reverse scan of the post-checkpoint tail.
        long lastCheckpoint = GetLastCheckpointFromMeta(partitionId);

        long commitFrontier = GetCommitFrontier(partitionId);

        using Iterator? iterator = db.NewIterator(cf: columnFamilyHandle);

        // Rows below the compaction floor are logically deleted even where they are still physically
        // present (the floor layout reclaims whole files, not rows), so the read starts at the floor.
        long startLogId = Math.Max(Math.Max(0, lastCheckpoint), GetCompactionFloor(partitionId));
        Span<byte> seekKey = stackalloc byte[LogKeyWidth];
        BuildLogKey(seekKey, partitionId, startLogId);
        iterator.Seek(seekKey);

        Span<byte> partitionPrefix = stackalloc byte[PartitionPrefixWidth];
        BuildPartitionPrefix(partitionPrefix, partitionId);

        while (iterator.Valid())
        {
            if (!iterator.GetKeySpan().StartsWith(partitionPrefix))
                break;

            ReadLogFromWire(iterator.GetValueSpan(), out RaftLogWireView view);

            if (view.Partition != partitionId || view.Id < lastCheckpoint)
            {
                iterator.Next();
                continue;
            }

            result.Add(ToRaftLog(view, commitFrontier));

            iterator.Next();
        }

        return result;
    }

    /// <summary>
    /// Reads up to <paramref name="maxEntries"/> logs for <paramref name="partitionId"/> with id ≥
    /// <paramref name="startLogIndex"/>, sorted ascending. The iterator stops advancing once
    /// <paramref name="maxEntries"/> rows have been read so large tails are not scanned in full.
    /// </summary>
    public List<RaftLog> ReadLogsRange(int partitionId, long startLogIndex, int maxEntries = int.MaxValue)
        => ReadLogsRange(partitionId, startLogIndex, maxEntries, long.MaxValue);

    /// <summary>
    /// Byte-budgeted range read. The budget is checked against <c>view.Log.Length</c> BEFORE
    /// <see cref="ToRaftLog"/> materializes the payload, so an over-budget entry costs a wire-view
    /// decode but never a managed copy — the point of the budget is to bound that allocation.
    /// </summary>
    public List<RaftLog> ReadLogsRange(int partitionId, long startLogIndex, int maxEntries, long maxBytes)
    {
        using EngineLease lease = AcquireEngine();

        // Presize when the caller supplied a sane bound (the common case: a bounded backfill batch),
        // so a full batch does not walk the doubling sequence. An unbounded/absurd limit falls back
        // to the default growth rather than reserving for entries that may not exist.
        List<RaftLog> result = maxEntries is > 0 and <= 1024 ? new(maxEntries) : [];

        ColumnFamilyHandle columnFamilyHandle = GetColumnFamily(partitionId);

        long commitFrontier = GetCommitFrontier(partitionId);

        using Iterator? iterator = db.NewIterator(cf: columnFamilyHandle);

        // Clamped at the compaction floor: a backfill request for ids below it reads exactly what
        // it read when those rows were physically deleted — nothing — so the caller's
        // "compacted, needs a snapshot" decision is unchanged by the floor layout.
        Span<byte> seekKey = stackalloc byte[LogKeyWidth];
        BuildLogKey(seekKey, partitionId, Math.Max(Math.Max(0, startLogIndex), GetCompactionFloor(partitionId)));
        iterator.Seek(seekKey);

        Span<byte> partitionPrefix = stackalloc byte[PartitionPrefixWidth];
        BuildPartitionPrefix(partitionPrefix, partitionId);

        long payloadBytes = 0;

        while (iterator.Valid())
        {
            if (!iterator.GetKeySpan().StartsWith(partitionPrefix))
                break;

            ReadLogFromWire(iterator.GetValueSpan(), out RaftLogWireView view);

            if (view.Partition != partitionId || view.Id < startLogIndex)
            {
                iterator.Next();
                continue;
            }

            // At-least-one-entry rule: the budget stops a batch, never the first entry —
            // an entry larger than the whole budget must still ship or the follower stalls.
            if (result.Count > 0 && payloadBytes + view.Log.Length > maxBytes)
                break;

            payloadBytes += view.Log.Length;
            result.Add(ToRaftLog(view, commitFrontier));

            iterator.Next();

            if (result.Count >= maxEntries)
                break;
        }

        return result;
    }

    /// <summary>
    /// Writes a set of logs to the Write-Ahead Log (WAL) for the specified partitions.
    /// </summary>
    /// <param name="logs">
    /// A list of tuples where each tuple contains a partition ID (int) and a list of
    /// <see cref="RaftLog"/> instances to be written to that partition.
    /// </param>
    /// <returns>
    /// Returns a <see cref="RaftOperationStatus"/> indicating the status of the write operation:
    /// <see cref="RaftOperationStatus.Success"/> if the operation succeeds,
    /// <see cref="RaftOperationStatus.Errored"/> if there is an issue during the operation.
    /// </returns>
    public RaftOperationStatus Write(List<(int, List<RaftLog>)> logs) => Write(logs, sync: true);

    /// <inheritdoc/>
    /// <remarks>
    /// When <paramref name="sync"/> is <see langword="false"/> the batch is written with
    /// <see cref="NonSynchronousWriteOptions"/> (<c>SetSync(false)</c>), so RocksDB appends it to the WAL
    /// without an fsync; the next <c>SetSync(true)</c> write flushes the shared WAL up to and including it,
    /// making prior sync-off writes durable. When this instance was constructed with <c>syncWrites=false</c>
    /// every write is already non-sync, so <paramref name="sync"/> has no additional effect.
    /// </remarks>
    public RaftOperationStatus Write(List<(int, List<RaftLog>)> logs, bool sync)
    {
        RaftOperationStatus status = WriteCore(logs, sync);

        if (status == RaftOperationStatus.Errored && TryRecoverEngineAfterFailure())
            status = WriteCore(logs, sync);

        if (status == RaftOperationStatus.Success)
            NoteStorageSuccess();

        return status;
    }

    /// <summary>
    /// The body of <see cref="Write(List{ValueTuple{int, List{RaftLog}}}, bool)"/>, run under the engine
    /// lease. Never reopens the engine itself: a failure is recorded and reported as Errored, and the
    /// public wrapper decides about recovery after the lease is released.
    /// </summary>
    private RaftOperationStatus WriteCore(List<(int, List<RaftLog>)> logs, bool sync)
    {
        WriteOptions effectiveOptions = sync ? writeOptions : NonSynchronousWriteOptions;

        // Checkpoint-bearing batches read-modify-write the persisted last-checkpoint metadata and must be
        // fully exclusive; plain appends only need exclusion against the scan+delete ops, so they share
        // the read lock and keep RocksDB's group commit across workers — see writeGuard.
        bool exclusive = ContainsCommittedCheckpoint(logs);

        try
        {
            using EngineLease lease = AcquireEngine();

            if (exclusive)
                writeGuard.EnterWriteLock();
            else
                writeGuard.EnterReadLock();

            try
            {
            if (logs is [{ Item2.Count: 1 } _]) // fast path
            {
                RaftLog log = logs[0].Item2[0];
                int partitionId = logs[0].Item1;

                ColumnFamilyHandle columnFamilyHandle = GetColumnFamily(partitionId);

                if (log.Type == RaftLogType.CommittedCheckpoint)
                {
                    // Promote the single-put fast path to a 2-op batch so the log entry and the persisted
                    // last-checkpoint id land atomically (RocksDB applies a batch all-or-nothing). max() so a
                    // late/duplicate lower checkpoint never regresses the recorded id.
                    long currentFloor = GetLastCheckpointFromMeta(partitionId);
                    long newCheckpoint = Math.Max(currentFloor, log.Id);

                    using WriteBatch checkpointBatch = new();
                    PutLogToBatch(checkpointBatch, partitionId, log, columnFamilyHandle);

                    // The persisted floor is an APPLIED/COMPACTED certificate for its whole prefix (restore
                    // frontier seeding, the drain's compacted-below-floor skip, and compaction all trust it),
                    // so it may only advance when every id up to the checkpoint is durably present here. A
                    // checkpoint row broadcast over a replication gap must land as a plain row: raising the
                    // floor across the gap certified entries this node never held, the applier then skipped
                    // them as "compacted", and compaction erased them before they were ever delivered.
                    if (newCheckpoint > currentFloor)
                    {
                        if (VerifyCheckpointPrefixPresent(partitionId, columnFamilyHandle, currentFloor, log.Id,
                                                          stagedIds: null, stagedSingleId: log.Id, out long firstMissing))
                            PutLastCheckpointToBatch(checkpointBatch, partitionId, newCheckpoint);
                        else
                            logger.LogWarning(
                                "Withholding last-checkpoint advance for partition {Partition}: checkpoint {CheckpointId} landed over a gap (floor {Floor}, first missing id {MissingId}); the floor advances when a later checkpoint finds the gap backfilled",
                                partitionId, log.Id, currentFloor, firstMissing);
                    }

                    db.Write(checkpointBatch, effectiveOptions);

                    return RaftOperationStatus.Success;
                }

                if (log.Type == RaftLogType.Committed)
                {
                    long frontier = GetCommitFrontier(partitionId);

                    // Duplicate marker for an id the durable frontier already certifies: nothing to write.
                    if (log.Id <= frontier)
                        return RaftOperationStatus.Success;

                    (long advanced, bool absorbed) = TryAdvanceFrontierFor(
                        partitionId, columnFamilyHandle, frontier, log, stagedProposed: null, stagedResolved: null);

                    if (absorbed)
                    {
                        // The whole commit is one small metadata put instead of a full-payload row
                        // rewrite — the write-amplification fix this frontier exists for.
                        db.Put(CommitFrontierKey(partitionId), Encoding.UTF8.GetBytes(advanced.ToString()),
                               metadataColumnFamily, effectiveOptions);
                        RaiseCommitFrontierCache(partitionId, advanced);
                        CommitMarkersAbsorbed++;
                        return RaftOperationStatus.Success;
                    }

                    if (advanced > frontier)
                    {
                        // Partial catch-up: the marker still writes its full row, but the certified
                        // prefix advance is persisted alongside it so re-anchoring converges.
                        using WriteBatch commitBatch = new();
                        PutLogToBatch(commitBatch, partitionId, log, columnFamilyHandle);
                        PutCommitFrontierToBatch(commitBatch, partitionId, advanced);
                        db.Write(commitBatch, effectiveOptions);
                        RaiseCommitFrontierCache(partitionId, advanced);
                        return RaftOperationStatus.Success;
                    }

                    // No advance (unresolved gap / divergent term / absent row): plain full-row write.
                }

                Span<byte> buffer = stackalloc byte[LogKeyWidth];
                BuildLogKey(buffer, partitionId, log.Id);

                // RocksDB copies the value synchronously inside Put, so the rented/stack buffer is safe to
                // release as soon as the call returns. The stackalloc is evaluated only when nothing was
                // rented (?? short-circuits), so small and large messages never both reserve a buffer.
                // Serialization is hand-rolled straight from the RaftLog — see MeasureLogEntry.
                int size = MeasureLogEntry(partitionId, log, out int logTypeByteCount);
                byte[]? rented = size > StackallocThreshold ? ArrayPool<byte>.Shared.Rent(size) : null;
                Span<byte> valueBuffer = (rented ?? stackalloc byte[StackallocThreshold])[..size];
                try
                {
                    WriteLogEntry(valueBuffer, partitionId, log, logTypeByteCount);
                    db.Put(buffer, valueBuffer, columnFamilyHandle, effectiveOptions);
                }
                finally
                {
                    if (rented is not null)
                        ArrayPool<byte>.Shared.Return(rented);
                }

                NoteShardBytesWritten(size);

                return RaftOperationStatus.Success;
            }
            
            Dictionary<ColumnFamilyHandle, Dictionary<int, List<RaftLog>>> plan = new();

            // Highest CommittedCheckpoint id seen per partition in this batch; used to stage the persisted
            // last-checkpoint update into the SAME WriteBatch as the log puts (atomic).
            Dictionary<int, long> checkpointMaxByPartition = new();

            // Ids staged in THIS batch per checkpoint-bearing partition. The floor-advance contiguity
            // check below reads the CF with an iterator, which cannot see rows still staged in the
            // write batch, so staged ids count as present. Collected only on the (rare, exclusive)
            // checkpoint path to keep the plain-append path allocation-free.
            Dictionary<int, HashSet<long>>? stagedIdsByPartition = exclusive ? new() : null;

            // Copy-on-second-sight: a partition that appears exactly once (the overwhelmingly common
            // shape of a scheduler group batch) stores the CALLER'S list in the plan directly — the
            // plan is only read below, and the caller owns the batch until Write returns. Only when
            // the same partition appears again is an owned merged copy materialized; ownedLists
            // (reference-identity set, lazily allocated) records which stored lists we own.
            HashSet<List<RaftLog>>? ownedLists = null;

            foreach ((int partitionId, List<RaftLog> raftLog) log in logs)
            {
                ColumnFamilyHandle columnFamilyHandle = GetColumnFamily(log.partitionId);

                foreach (RaftLog entry in log.raftLog)
                {
                    if (entry.Type == RaftLogType.CommittedCheckpoint &&
                        entry.Id > checkpointMaxByPartition.GetValueOrDefault(log.partitionId, -1))
                        checkpointMaxByPartition[log.partitionId] = entry.Id;

                    if (stagedIdsByPartition is not null)
                    {
                        if (!stagedIdsByPartition.TryGetValue(log.partitionId, out HashSet<long>? staged))
                            stagedIdsByPartition[log.partitionId] = staged = [];
                        staged.Add(entry.Id);
                    }
                }

                if (plan.TryGetValue(columnFamilyHandle, out Dictionary<int, List<RaftLog>>? raftLogsPerPartition))
                {
                    if (raftLogsPerPartition.TryGetValue(log.partitionId, out List<RaftLog>? raftLogs))
                    {
                        ownedLists ??= [];
                        if (!ownedLists.Contains(raftLogs))
                        {
                            List<RaftLog> owned = new(raftLogs.Count + log.raftLog.Count);
                            owned.AddRange(raftLogs);
                            raftLogsPerPartition[log.partitionId] = owned;
                            ownedLists.Add(owned);
                            raftLogs = owned;
                        }

                        raftLogs.AddRange(log.raftLog);
                    }
                    else
                        raftLogsPerPartition.Add(log.partitionId, log.raftLog);
                }
                else
                {
                    raftLogsPerPartition = new() { { log.partitionId, log.raftLog } };
                    plan.Add(columnFamilyHandle, raftLogsPerPartition);
                }
            }
            
            using WriteBatch writeBatch = new();

            // Frontier advances computed while staging (absorbed commit markers + checkpoint seeds),
            // applied to the cache only after db.Write succeeds. Lazily allocated: a propose-only
            // batch (the hot leader path) never touches it.
            Dictionary<int, long>? frontierAdvances = null;
            long markersAbsorbedThisCall = 0;
            long shardBytesStaged = 0;

            foreach ((ColumnFamilyHandle key, Dictionary<int, List<RaftLog>> raftLogs) in plan)
            {
                foreach (KeyValuePair<int, List<RaftLog>> kv in raftLogs)
                {
                    int partitionId = kv.Key;
                    long frontier = GetCommitFrontier(partitionId);
                    long pendingFrontier = frontier;

                    // Rows staged in THIS batch, split by resolution state: the frontier probes read
                    // the CF, which cannot see rows still staged in the write batch, so staged rows
                    // count via these sets. A later staging of the same id replaces its membership,
                    // mirroring RocksDB's last-put-wins within a batch.
                    HashSet<long>? stagedProposed = null;
                    HashSet<long>? stagedResolved = null;

                    foreach (RaftLog log in kv.Value)
                    {
                        if (log.Type == RaftLogType.Committed)
                        {
                            // Marker already certified by the (pending) frontier: nothing to stage.
                            if (log.Id <= pendingFrontier)
                                continue;

                            (long advanced, bool absorbed) = TryAdvanceFrontierFor(
                                partitionId, key, pendingFrontier, log, stagedProposed, stagedResolved);
                            pendingFrontier = advanced;
                            if (absorbed)
                            {
                                markersAbsorbedThisCall++;
                                continue;
                            }
                        }

                        shardBytesStaged += PutLogToBatch(writeBatch, partitionId, log, key);

                        if (log.Type is RaftLogType.Proposed or RaftLogType.ProposedCheckpoint)
                        {
                            (stagedProposed ??= []).Add(log.Id);
                            stagedResolved?.Remove(log.Id);
                        }
                        else
                        {
                            (stagedResolved ??= []).Add(log.Id);
                            stagedProposed?.Remove(log.Id);
                        }
                    }

                    if (pendingFrontier > frontier)
                        (frontierAdvances ??= new())[partitionId] = pendingFrontier;
                }
            }

            // Stage the persisted last-checkpoint update for any partition that committed a checkpoint in
            // this batch, so it is durable atomically with the log entries. max() with the existing value so
            // an out-of-order lower checkpoint cannot regress the recorded id. The advance is withheld when
            // the checkpoint's prefix is not contiguously present here (see the fast-path comment): the row
            // itself still lands, and a later checkpoint re-attempts the advance once backfill closes the gap.
            foreach ((int partitionId, long batchMaxCheckpoint) in checkpointMaxByPartition)
            {
                long currentFloor = GetLastCheckpointFromMeta(partitionId);
                if (batchMaxCheckpoint <= currentFloor)
                    continue;

                HashSet<long>? staged = null;
                stagedIdsByPartition?.TryGetValue(partitionId, out staged);

                if (VerifyCheckpointPrefixPresent(partitionId, GetColumnFamily(partitionId), currentFloor,
                                                  batchMaxCheckpoint, staged, stagedSingleId: -1, out long firstMissing))
                    PutLastCheckpointToBatch(writeBatch, partitionId, batchMaxCheckpoint);
                else
                    logger.LogWarning(
                        "Withholding last-checkpoint advance for partition {Partition}: checkpoint {CheckpointId} landed over a gap (floor {Floor}, first missing id {MissingId}); the floor advances when a later checkpoint finds the gap backfilled",
                        partitionId, batchMaxCheckpoint, currentFloor, firstMissing);
            }

            if (frontierAdvances is not null)
            {
                foreach ((int partitionId, long value) in frontierAdvances)
                    PutCommitFrontierToBatch(writeBatch, partitionId, value);
            }

            db.Write(writeBatch, effectiveOptions);

            if (frontierAdvances is not null)
            {
                foreach ((int partitionId, long value) in frontierAdvances)
                    RaiseCommitFrontierCache(partitionId, value);
            }

            CommitMarkersAbsorbed += markersAbsorbedThisCall;

            NoteShardBytesWritten(shardBytesStaged);

            return RaftOperationStatus.Success;
            }
            finally
            {
                if (exclusive)
                    writeGuard.ExitWriteLock();
                else
                    writeGuard.ExitReadLock();
            }
        }
        catch (Exception ex)
        {
            logger.LogError("Error during write: {Message}\n{StackTrace}", ex.Message, ex.StackTrace);

            NoteStorageFailure(ex);
            return RaftOperationStatus.Errored;
        }
    }

    /// <summary>
    /// True when any entry in the batch is a <see cref="RaftLogType.CommittedCheckpoint"/>. Such batches
    /// read-modify-write the persisted last-checkpoint metadata inside <see cref="Write(List{ValueTuple{int, List{RaftLog}}}, bool)"/>
    /// and must therefore hold the exclusive side of <see cref="writeGuard"/>; plain appends share the read lock.
    /// </summary>
    private static bool ContainsCommittedCheckpoint(List<(int, List<RaftLog>)> logs)
    {
        foreach ((int _, List<RaftLog> raftLogs) in logs)
        {
            foreach (RaftLog log in raftLogs)
            {
                if (log.Type == RaftLogType.CommittedCheckpoint)
                    return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Adds a serialized RaftLogMessage to the write batch within a specified column family.
    /// </summary>
    /// <param name="writeBatch">
    /// The write batch object used to batch database operations.
    /// </param>
    /// <param name="message">
    /// The RaftLogMessage instance containing the details to be added to the batch.
    /// </param>
    /// <param name="columnFamilyHandle">
    /// The handle to the column family in which the record should be stored.
    /// </param>
    // ── Hand-rolled RaftLogMessage writer ─────────────────────────────────────
    //
    // The append hot paths serialize each entry directly from the RaftLog into the destination
    // span, mirroring the hand-rolled reader (ReadLogFromWire). Going through the generated
    // serializer allocated a RaftLogMessage plus a ByteString wrapper per appended entry solely to
    // flatten it. Byte identity with RaftLogMessage.WriteTo is guaranteed by construction —
    // protobuf's canonical encoding writes fields in ascending field-number order with minimal
    // varints — and pinned by RocksDbSerializeAllocationTests' byte-identity cases.
    //
    // Presence rules match the message the old code built:
    //  * proto3 scalars (1,2,3,4,7,8,9) are omitted when zero;
    //  * `optional` logType (5) / log (6) use explicit presence — written, even when empty,
    //    iff the corresponding RaftLog reference is non-null;
    //  * negative int32s (1,4,7) sign-extend to 64 bits (10-byte varints), like the generated code.

    /// <summary>
    /// Serialized size of the <c>RaftLogMessage</c> encoding of (<paramref name="partitionId"/>,
    /// <paramref name="log"/>), exactly matching the generated <c>CalculateSize</c>.
    /// <paramref name="logTypeByteCount"/> carries the UTF-8 length of <c>LogType</c> (-1 when
    /// absent) so <see cref="WriteLogEntry"/> does not measure the string twice.
    /// </summary>
    internal static int MeasureLogEntry(int partitionId, RaftLog log, out int logTypeByteCount)
    {
        int size = 0;

        if (partitionId != 0)
            size += 1 + VarintSize((ulong)(long)partitionId);
        if (log.Id != 0)
            size += 1 + VarintSize((ulong)log.Id);
        if (log.Term != 0)
            size += 1 + VarintSize((ulong)log.Term);
        if ((int)log.Type != 0)
            size += 1 + VarintSize((ulong)(long)(int)log.Type);

        logTypeByteCount = -1;
        if (log.LogType is not null)
        {
            logTypeByteCount = Encoding.UTF8.GetByteCount(log.LogType);
            size += 1 + VarintSize((ulong)logTypeByteCount) + logTypeByteCount;
        }

        if (log.LogData is not null)
            size += 1 + VarintSize((ulong)log.LogData.Length) + log.LogData.Length;

        if (log.Time.N != 0)
            size += 1 + VarintSize((ulong)(long)log.Time.N);
        if (log.Time.L != 0)
            size += 1 + VarintSize((ulong)log.Time.L);
        if (log.Time.C != 0)
            size += 1 + VarintSize(log.Time.C);

        return size;
    }

    /// <summary>
    /// Writes the canonical protobuf encoding of (<paramref name="partitionId"/>, <paramref name="log"/>)
    /// into <paramref name="destination"/>, which must be exactly <see cref="MeasureLogEntry"/> bytes.
    /// All field tags are single-byte (fields 1–9). <paramref name="logTypeByteCount"/> must be the
    /// value returned by <see cref="MeasureLogEntry"/> for the same log.
    /// </summary>
    internal static void WriteLogEntry(Span<byte> destination, int partitionId, RaftLog log, int logTypeByteCount)
    {
        int pos = 0;

        if (partitionId != 0)
        {
            destination[pos++] = 0x08; // field 1, varint
            WriteVarint(destination, ref pos, (ulong)(long)partitionId);
        }

        if (log.Id != 0)
        {
            destination[pos++] = 0x10; // field 2, varint
            WriteVarint(destination, ref pos, (ulong)log.Id);
        }

        if (log.Term != 0)
        {
            destination[pos++] = 0x18; // field 3, varint
            WriteVarint(destination, ref pos, (ulong)log.Term);
        }

        if ((int)log.Type != 0)
        {
            destination[pos++] = 0x20; // field 4, varint
            WriteVarint(destination, ref pos, (ulong)(long)(int)log.Type);
        }

        if (log.LogType is not null)
        {
            destination[pos++] = 0x2A; // field 5, length-delimited
            WriteVarint(destination, ref pos, (ulong)logTypeByteCount);
            pos += Encoding.UTF8.GetBytes(log.LogType, destination[pos..]);
        }

        if (log.LogData is not null)
        {
            destination[pos++] = 0x32; // field 6, length-delimited
            WriteVarint(destination, ref pos, (ulong)log.LogData.Length);
            log.LogData.CopyTo(destination[pos..]);
            pos += log.LogData.Length;
        }

        if (log.Time.N != 0)
        {
            destination[pos++] = 0x38; // field 7, varint
            WriteVarint(destination, ref pos, (ulong)(long)log.Time.N);
        }

        if (log.Time.L != 0)
        {
            destination[pos++] = 0x40; // field 8, varint
            WriteVarint(destination, ref pos, (ulong)log.Time.L);
        }

        if (log.Time.C != 0)
        {
            destination[pos++] = 0x48; // field 9, varint
            WriteVarint(destination, ref pos, log.Time.C);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int VarintSize(ulong value)
    {
        int size = 1;
        while (value >= 0x80)
        {
            size++;
            value >>= 7;
        }

        return size;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void WriteVarint(Span<byte> destination, ref int pos, ulong value)
    {
        while (value >= 0x80)
        {
            destination[pos++] = (byte)(value | 0x80);
            value >>= 7;
        }

        destination[pos++] = (byte)value;
    }

    /// <summary>
    /// Serializes (<paramref name="partitionId"/>, <paramref name="log"/>) via the hand-rolled
    /// writer and stages it into <paramref name="writeBatch"/> — the allocation-free counterpart of
    /// <see cref="PutToBatch"/> used by the append hot paths, so no RaftLogMessage/ByteString
    /// intermediates exist per entry. WriteBatch.Put copies the value synchronously, so the
    /// rented/stack buffer is safe to release as soon as the call returns.
    /// </summary>
    private static int PutLogToBatch(WriteBatch writeBatch, int partitionId, RaftLog log, ColumnFamilyHandle columnFamilyHandle)
    {
        Span<byte> keyBuffer = stackalloc byte[LogKeyWidth];
        BuildLogKey(keyBuffer, partitionId, log.Id);

        int size = MeasureLogEntry(partitionId, log, out int logTypeByteCount);
        byte[]? rented = size > StackallocThreshold ? ArrayPool<byte>.Shared.Rent(size) : null;
        Span<byte> valueBuffer = (rented ?? stackalloc byte[StackallocThreshold])[..size];
        try
        {
            WriteLogEntry(valueBuffer, partitionId, log, logTypeByteCount);
            writeBatch.Put(keyBuffer, valueBuffer, cf: columnFamilyHandle);
        }
        finally
        {
            if (rented is not null)
                ArrayPool<byte>.Shared.Return(rented);
        }

        return size;
    }

    /// <summary>
    /// Accounts shard-row bytes just written and, once a flush unit has accumulated, flushes the
    /// metadata column family (asynchronously) so RocksDB can release the write-ahead logs its
    /// tiny, never-full memtable would otherwise pin — see <see cref="metadataFlushEveryBytes"/>.
    /// Called with the engine lease held. A flush that cannot be scheduled is logged at Debug and
    /// retried at the next threshold; it affects disk footprint and restart replay, never data.
    /// </summary>
    private void NoteShardBytesWritten(long bytes)
    {
        if (bytes <= 0)
            return;

        long total = Interlocked.Add(ref shardBytesSinceMetadataFlush, bytes);
        if (total < metadataFlushEveryBytes)
            return;

        if (Interlocked.CompareExchange(ref shardBytesSinceMetadataFlush, 0, total) != total)
            return;

        try
        {
            FlushOptions flushOptions = new();
            Native.Instance.rocksdb_flushoptions_set_wait(flushOptions.Handle, 0);
            Native.Instance.rocksdb_flush_cf(db.Handle, flushOptions.Handle, metadataColumnFamily.Handle);
        }
        catch (Exception ex)
        {
            logger.LogDebug("RocksDB WAL at '{Path}': metadata flush to release write-ahead logs could not be scheduled: {Message}", enginePath, ex.Message);
        }
    }

    private static void PutToBatch(WriteBatch writeBatch, RaftLogMessage message, ColumnFamilyHandle columnFamilyHandle)
    {
        Span<byte> buffer = stackalloc byte[LogKeyWidth];
        BuildLogKey(buffer, message.Partition, message.Id);

        // WriteBatch.Put copies the value synchronously, so the rented/stack buffer is safe to release
        // as soon as the call returns. The stackalloc is evaluated only when nothing was rented
        // (?? short-circuits), so small and large messages never both reserve a buffer.
        int size = message.CalculateSize();
        byte[]? rented = size > StackallocThreshold ? ArrayPool<byte>.Shared.Rent(size) : null;
        Span<byte> valueBuffer = (rented ?? stackalloc byte[StackallocThreshold])[..size];
        try
        {
            SerializeInto(message, valueBuffer);
            writeBatch.Put(buffer, valueBuffer, cf: columnFamilyHandle);
        }
        finally
        {
            if (rented is not null)
                ArrayPool<byte>.Shared.Return(rented);
        }
    }

    /// <summary>
    /// Retrieves the maximum log ID from the Write-Ahead Log (WAL) for the specified partition.
    /// </summary>
    /// <param name="partitionId">
    /// The ID of the partition from which to retrieve the maximum log ID.
    /// </param>
    /// <returns>
    /// The ID of the maximum log stored in the specified partition. Returns 0 if no logs exist for the partition.
    /// </returns>
    public long GetMaxLog(int partitionId)
    {
        using EngineLease lease = AcquireEngine();

        ColumnFamilyHandle columnFamilyHandle = GetColumnFamily(partitionId);
        
        using Iterator? iterator = db.NewIterator(cf: columnFamilyHandle);
        SeekToLastPartitionKey(iterator, partitionId);

        // The key alone carries the id, so the last key in the partition answers this without reading
        // (and copying) a single value. A last key below the compaction floor means every row of the
        // partition is logically deleted — the same answer a physically emptied partition gives.
        if (iterator.Valid() && KeyBelongsToPartition(iterator.GetKeySpan(), partitionId))
        {
            long id = ParseLogIdFromKey(iterator.GetKeySpan());
            return id >= GetCompactionFloor(partitionId) ? id : 0;
        }

        return 0;
    }

    /// <summary>
    /// Retrieves the current term of the specified partition by examining the last log entry.
    /// </summary>
    /// <param name="partitionId">
    /// The ID of the partition for which to retrieve the current term.
    /// </param>
    /// <returns>
    /// The current term of the specified partition, or 0 if no logs are found.
    /// </returns>
    /// <summary>
    /// Point lookup of a single entry's term by exact id. Builds the fixed-width log key and issues a
    /// direct <c>db.Get</c> instead of creating an iterator, then parses only the protobuf message to
    /// read <c>Term</c> — it does not build a <see cref="RaftLog"/> or copy the entry's payload
    /// <c>LogData</c>. Returns -1 when the key is absent (or, defensively, when the stored message does
    /// not match the requested partition/id). A further optimization — a protobuf wire scan that reads
    /// the <c>term</c> field and skips the payload <c>ByteString</c> entirely — is left as a
    /// benchmark-gated follow-up.
    /// </summary>
    public long GetTermAt(int partitionId, long logIndex)
    {
        using EngineLease lease = AcquireEngine();

        ColumnFamilyHandle columnFamilyHandle = GetColumnFamily(partitionId);

        // Below the compaction floor the row is logically gone whether or not its file was dropped yet.
        if (logIndex < GetCompactionFloor(partitionId))
            return -1;

        Span<byte> key = stackalloc byte[LogKeyWidth];
        BuildLogKey(key, partitionId, logIndex);

        // Point lookup on the exact key: a hit is unambiguously the entry for (partitionId, logIndex),
        // so no field re-validation is needed. The span-deserializer overload hands ReadTermFromWire
        // the value directly in RocksDB's native buffer — the previous byte[] Get copied the FULL
        // serialized message (multi-megabyte payload included) into managed memory just to read the
        // `term` varint near the front. The Found flag disambiguates a missing key from a stored
        // term of 0 (the deserializer only runs on a hit; a miss yields the tuple's default).
        (bool found, long term) = db.Get(key, TermSpanDeserializer.Instance, cf: columnFamilyHandle);
        return found ? term : -1;
    }

    /// <summary>
    /// Cached singleton (allocating one per call would defeat the point) that reads only the
    /// <c>term</c> field from the value span in place via <see cref="ReadTermFromWire"/>.
    /// </summary>
    private sealed class TermSpanDeserializer : ISpanDeserializer<(bool Found, long Term)>
    {
        public static readonly TermSpanDeserializer Instance = new();

        public (bool Found, long Term) Deserialize(ReadOnlySpan<byte> buffer) => (true, ReadTermFromWire(buffer));
    }

    /// <summary>
    /// Reads only the <c>term</c> field (field 3, varint) from a serialized <c>RaftLogMessage</c>,
    /// skipping every other field — crucially the length-delimited <c>log</c> payload (field 6) —
    /// without allocating. Returns the proto3 default (<c>0</c>) when the field is absent, which is the
    /// correct value for an entry whose term is 0 (proto3 omits default-valued fields on the wire). The
    /// caller has already established, via the exact-key <c>db.Get</c> hit, that the entry exists.
    /// </summary>
    private static long ReadTermFromWire(ReadOnlySpan<byte> data)
    {
        int pos = 0;
        while (pos < data.Length)
        {
            ulong tag = ReadVarint(data, ref pos);
            int fieldNumber = (int)(tag >> 3);
            int wireType = (int)(tag & 0x7);

            if (fieldNumber == 3 && wireType == 0) // term
                return (long)ReadVarint(data, ref pos);

            switch (wireType)
            {
                case 0: ReadVarint(data, ref pos); break;                             // varint
                case 1: pos += 8; break;                                              // 64-bit
                case 2: pos += (int)ReadVarint(data, ref pos); break;                 // length-delimited (skip payload)
                case 5: pos += 4; break;                                              // 32-bit
                default: return 0;                                                    // unknown wire type — treat as default
            }
        }

        return 0; // term field omitted => proto3 default
    }

    /// <summary>
    /// Reads the scalar "header" fields of a serialized <c>RaftLogMessage</c> — <c>partition</c> (1),
    /// <c>id</c> (2), <c>term</c> (3) and <c>type</c> (4) — without allocating and without materializing
    /// the length-delimited <c>logType</c> (5) / <c>log</c> (6) fields, which are skipped by advancing
    /// past their declared length. Scan cost is therefore bounded by the number of fields, not by the
    /// payload size, so a backwards scan over a partition with multi-megabyte entries no longer allocates
    /// (and copies) one <c>ByteString</c> per entry visited.
    ///
    /// Absent fields yield the proto3 default (<c>0</c>), which is the correct value for an entry that
    /// genuinely stored 0. Use this only where the payload is not needed; callers that must return a
    /// <see cref="RaftLog"/> go through <see cref="ReadLogFromWire"/>, which decodes every field.
    /// </summary>
    private static void ReadHeaderFromWire(ReadOnlySpan<byte> data, out int partition, out long id, out long term, out int type)
    {
        partition = 0;
        id = 0;
        term = 0;
        type = 0;

        int pos = 0;
        while (pos < data.Length)
        {
            ulong tag = ReadVarint(data, ref pos);
            int fieldNumber = (int)(tag >> 3);
            int wireType = (int)(tag & 0x7);

            if (wireType == 0)
            {
                long value = (long)ReadVarint(data, ref pos);

                switch (fieldNumber)
                {
                    case 1: partition = (int)value; break;
                    case 2: id = value; break;
                    case 3: term = value; break;
                    case 4: type = (int)value; break;
                }

                continue;
            }

            switch (wireType)
            {
                case 1: pos += 8; break;                                              // 64-bit
                case 2: pos += (int)ReadVarint(data, ref pos); break;                 // length-delimited (skip payload)
                case 5: pos += 4; break;                                              // 32-bit
                default: return;                                                      // unknown wire type — stop, keep defaults
            }
        }
    }

    /// <summary>
    /// A borrowed, non-owning view over a serialized <c>RaftLogMessage</c>. The two length-delimited
    /// fields point straight into the caller's buffer (a live RocksDB iterator value), so a view is only
    /// valid until that buffer moves — it must be converted with <see cref="ToRaftLog"/>, which performs
    /// the single unavoidable payload copy, before <c>iterator.Next()</c> is called.
    /// </summary>
    private ref struct RaftLogWireView
    {
        public int Partition;
        public long Id;
        public long Term;
        public int Type;
        public ReadOnlySpan<byte> LogType;
        public ReadOnlySpan<byte> Log;
        public bool HasLog;
        public int TimeNode;
        public long TimePhysical;
        public uint TimeCounter;
    }

    /// <summary>
    /// Decodes every field of a serialized <c>RaftLogMessage</c> into a <see cref="RaftLogWireView"/>
    /// without allocating. This is the read-path counterpart to <see cref="ReadHeaderFromWire"/>: it
    /// replaces <see cref="Unserializer"/> on the hot range-scan path, where going through the generated
    /// parser allocated a <c>RaftLogMessage</c>, a <c>ByteString</c> wrapper and a fresh <c>logType</c>
    /// string for every entry visited — overhead that dwarfed the payload for small entries and was
    /// paid again on each re-read of an overlapping follower backfill range.
    ///
    /// <para>Decoding is deliberately separated from materialization so that callers can apply their
    /// partition/id filter against the view and skip non-matching entries without ever copying a
    /// payload.</para>
    ///
    /// <para>Absent fields keep their proto3 default, matching the generated parser: in particular an
    /// unset <c>logType</c> (5) yields <see cref="string.Empty"/>, not <see langword="null"/>, because
    /// the generated property substitutes the default for a missing value.</para>
    /// </summary>
    /// <exception cref="InvalidDataException">
    /// The value is truncated or otherwise malformed. The generated parser signalled this by throwing
    /// <c>InvalidProtocolBufferException</c>; corrupt entries must keep failing loudly rather than being
    /// silently decoded to defaults, which would present a torn entry as a legitimate log record.
    /// </exception>
    private static void ReadLogFromWire(ReadOnlySpan<byte> data, out RaftLogWireView view)
    {
        view = default;

        int pos = 0;
        while (pos < data.Length)
        {
            ulong tag = ReadVarint(data, ref pos);
            int fieldNumber = (int)(tag >> 3);
            int wireType = (int)(tag & 0x7);

            switch (wireType)
            {
                case 0: // varint
                {
                    ulong value = ReadVarint(data, ref pos);

                    switch (fieldNumber)
                    {
                        case 1: view.Partition = (int)value; break;
                        case 2: view.Id = (long)value; break;
                        case 3: view.Term = (long)value; break;
                        case 4: view.Type = (int)value; break;
                        case 7: view.TimeNode = (int)value; break;
                        case 8: view.TimePhysical = (long)value; break;
                        case 9: view.TimeCounter = (uint)value; break;
                    }

                    break;
                }

                case 2: // length-delimited
                {
                    int length = (int)ReadVarint(data, ref pos);
                    if (length < 0 || pos + length > data.Length)
                        throw new InvalidDataException($"Truncated RaftLogMessage: field {fieldNumber} declares {length} bytes but only {data.Length - pos} remain.");

                    switch (fieldNumber)
                    {
                        case 5: view.LogType = data.Slice(pos, length); break;
                        case 6: view.Log = data.Slice(pos, length); view.HasLog = true; break;
                    }

                    pos += length;
                    break;
                }

                case 1: // 64-bit
                    if (pos + 8 > data.Length)
                        throw new InvalidDataException("Truncated RaftLogMessage: incomplete 64-bit field.");
                    pos += 8;
                    break;

                case 5: // 32-bit
                    if (pos + 4 > data.Length)
                        throw new InvalidDataException("Truncated RaftLogMessage: incomplete 32-bit field.");
                    pos += 4;
                    break;

                default:
                    throw new InvalidDataException($"Malformed RaftLogMessage: unsupported wire type {wireType} for field {fieldNumber}.");
            }
        }
    }

    /// <summary>
    /// Materializes <paramref name="view"/> into a <see cref="RaftLog"/>, copying the payload out of the
    /// borrowed buffer. This copy is unavoidable — <see cref="RaftLog.LogData"/> is a <c>byte[]</c> that
    /// outlives the iterator position the bytes came from — and after this change it is the only
    /// per-entry allocation on the range-scan path apart from the <see cref="RaftLog"/> itself, since
    /// <c>logType</c> is served from <see cref="InternLogType"/>'s cache.
    /// <para>
    /// <paramref name="commitFrontier"/> derives the EFFECTIVE type: a <see cref="RaftLogType.Proposed"/>
    /// row at or below the frontier reads as <see cref="RaftLogType.Committed"/>, because the frontier
    /// is where the commit was persisted (see the commit-frontier region) — the row itself was
    /// deliberately not rewritten. Every other type carries its own on-disk value.
    /// </para>
    /// </summary>
    private static RaftLog ToRaftLog(in RaftLogWireView view, long commitFrontier)
    {
        RaftLogType type = (RaftLogType)view.Type;
        if (type == RaftLogType.Proposed && view.Id <= commitFrontier)
            type = RaftLogType.Committed;

        return new()
        {
            Id = view.Id,
            Term = view.Term,
            Type = type,
            Time = new(view.TimeNode, view.TimePhysical, view.TimeCounter),
            LogType = InternLogType(view.LogType),
            LogData = view.HasLog ? (view.Log.IsEmpty ? [] : view.Log.ToArray()) : null
        };
    }

    /// <summary>
    /// The per-thread cache used by <see cref="InternLogType"/>. Log types form a tiny closed set (a
    /// handful of operation names for the whole cluster), so a short linear scan comparing raw UTF-8
    /// bytes resolves nearly every entry without decoding a new string. Kept <c>[ThreadStatic]</c>
    /// rather than shared because range scans run concurrently on the read scheduler's worker threads
    /// and a shared cache would need locking to publish entries safely.
    /// </summary>
    [ThreadStatic]
    private static (byte[] Utf8, string Value)[]? logTypeCache;

    /// <summary>The number of populated slots in <see cref="logTypeCache"/>.</summary>
    [ThreadStatic]
    private static int logTypeCacheCount;

    /// <summary>The next slot <see cref="InternLogType"/> will write, wrapping round-robin once the cache is full.</summary>
    [ThreadStatic]
    private static int logTypeCacheCursor;

    /// <summary>The maximum number of distinct log types cached per thread before slots are recycled.</summary>
    private const int LogTypeCacheSize = 8;

    /// <summary>
    /// Returns the string form of a UTF-8 <c>logType</c>, reusing a previously decoded instance whenever
    /// the same bytes are seen again. A range scan over a partition is overwhelmingly homogeneous in log
    /// type, so this turns one string allocation per entry into one per distinct type per thread.
    /// A miss costs an extra <c>byte[]</c> for the cached key; with more than
    /// <see cref="LogTypeCacheSize"/> live types the oldest slot is recycled rather than growing.
    /// </summary>
    private static string InternLogType(ReadOnlySpan<byte> utf8)
    {
        if (utf8.IsEmpty)
            return "";

        (byte[] Utf8, string Value)[] cache = logTypeCache ??= new (byte[], string)[LogTypeCacheSize];

        for (int i = 0; i < logTypeCacheCount; i++)
        {
            if (utf8.SequenceEqual(cache[i].Utf8))
                return cache[i].Value;
        }

        string value = Encoding.UTF8.GetString(utf8);

        cache[logTypeCacheCursor] = (utf8.ToArray(), value);
        logTypeCacheCursor = (logTypeCacheCursor + 1) % LogTypeCacheSize;
        if (logTypeCacheCount < LogTypeCacheSize)
            logTypeCacheCount++;

        return value;
    }

    /// <summary>Decodes a base-128 varint at <paramref name="pos"/>, advancing it past the value.</summary>
    private static ulong ReadVarint(ReadOnlySpan<byte> data, ref int pos)
    {
        ulong result = 0;
        int shift = 0;
        while (pos < data.Length)
        {
            byte b = data[pos++];
            result |= (ulong)(b & 0x7F) << shift;
            if ((b & 0x80) == 0)
                break;
            shift += 7;
        }

        return result;
    }

    public long GetCurrentTerm(int partitionId)
    {
        using EngineLease lease = AcquireEngine();

        ColumnFamilyHandle columnFamilyHandle = GetColumnFamily(partitionId);

        using Iterator? iterator = db.NewIterator(cf: columnFamilyHandle);
        SeekToLastPartitionKey(iterator, partitionId);

        if (iterator.Valid() && KeyBelongsToPartition(iterator.GetKeySpan(), partitionId)
            && ParseLogIdFromKey(iterator.GetKeySpan()) >= GetCompactionFloor(partitionId))
        {
            ReadHeaderFromWire(iterator.GetValueSpan(), out _, out _, out long term, out _);
            return term;
        }

        return 0;
    }

    /// <summary>
    /// Retrieves the last checkpoint log index from the specified partition.
    /// </summary>
    /// <param name="partitionId">
    /// The ID of the partition for which to retrieve the last checkpoint.
    /// </param>
    /// <returns>
    /// The log index of the last checkpoint for the specified partition.
    /// </returns>
    public long GetLastCheckpoint(int partitionId)
    {
        using EngineLease lease = AcquireEngine();

        return GetLastCheckpointFromMeta(partitionId);
    }

    /// <inheritdoc/>
    public int CountPersistedLogs(int partitionId)
    {
        using EngineLease lease = AcquireEngine();

        ColumnFamilyHandle columnFamilyHandle = GetColumnFamily(partitionId);

        using Iterator? iterator = db.NewIterator(cf: columnFamilyHandle);
        Span<byte> seekKey = stackalloc byte[LogKeyWidth];
        BuildLogKey(seekKey, partitionId, GetCompactionFloor(partitionId));
        iterator.Seek(seekKey);

        int count = 0;

        Span<byte> partitionPrefix = stackalloc byte[PartitionPrefixWidth];
        BuildPartitionPrefix(partitionPrefix, partitionId);

        // Counting only needs partition membership, which the key prefix already establishes — no value read.
        while (iterator.Valid() && iterator.GetKeySpan().StartsWith(partitionPrefix))
        {
            count++;
            iterator.Next();
        }

        return count;
    }

    /// <inheritdoc/>
    public int CountRemovableLogs(int partitionId)
    {
        using EngineLease lease = AcquireEngine();

        long lastCheckpoint = GetLastCheckpointFromMeta(partitionId);

        if (lastCheckpoint <= 0)
            return 0;

        ColumnFamilyHandle columnFamilyHandle = GetColumnFamily(partitionId);

        using Iterator? iterator = db.NewIterator(cf: columnFamilyHandle);
        Span<byte> seekKey = stackalloc byte[LogKeyWidth];
        BuildLogKey(seekKey, partitionId, GetCompactionFloor(partitionId));
        iterator.Seek(seekKey);

        int count = 0;

        Span<byte> partitionPrefix = stackalloc byte[PartitionPrefixWidth];
        BuildPartitionPrefix(partitionPrefix, partitionId);

        while (iterator.Valid() && iterator.GetKeySpan().StartsWith(partitionPrefix))
        {
            if (ParseLogIdFromKey(iterator.GetKeySpan()) >= lastCheckpoint)
                break;

            count++;
            iterator.Next();
        }

        return count;
    }

    /// <summary>
    /// Retrieves metadata value associated with the specified key from the database.
    /// </summary>
    /// <param name="key">
    /// The key for which the metadata value is to be retrieved.
    /// </param>
    /// <returns>
    /// The metadata value as a string if the key exists; otherwise, null.
    /// </returns>
    public string? GetMetaData(string key)
    {
        using EngineLease lease = AcquireEngine();

        byte[] value = db.Get(Encoding.UTF8.GetBytes(key), cf: metadataColumnFamily);

        return value is not null ? Encoding.UTF8.GetString(value) : null;
    }

    /// <summary>
    /// Sets a metadata key-value pair in the underlying storage.
    /// </summary>
    /// <param name="key">
    /// The key of the metadata to set.
    /// </param>
    /// <param name="value">
    /// The value of the metadata to associate with the specified key.
    /// </param>
    /// <returns>
    /// A boolean indicating whether the metadata was successfully set.
    /// </returns>
    public bool SetMetaData(string key, string value)
    {
        bool ok = SetMetaDataCore(key, value);

        if (!ok && TryRecoverEngineAfterFailure())
            ok = SetMetaDataCore(key, value);

        if (ok)
            NoteStorageSuccess();

        return ok;
    }

    /// <summary>
    /// Body of <see cref="SetMetaData"/> under the engine lease. This backs Raft hard-state persistence on
    /// the election path (<see cref="IWAL.PersistHardState"/>), so a storage failure must surface as
    /// <see langword="false"/> and never as a native exception escaping into the partition executor: the
    /// election code decides what to do with a vote it could not make durable.
    /// </summary>
    private bool SetMetaDataCore(string key, string value)
    {
        try
        {
            using EngineLease lease = AcquireEngine();

            db.Put(Encoding.UTF8.GetBytes(key), Encoding.UTF8.GetBytes(value), cf: metadataColumnFamily);

            return true;
        }
        catch (Exception ex)
        {
            logger.LogError("Error during SetMetaData({Key}): {Message}", key, ex.Message);

            NoteStorageFailure(ex);
            return false;
        }
    }

    /// <summary>
    /// Builds the metadata-CF key that stores the id of the highest <see cref="RaftLogType.CommittedCheckpoint"/>
    /// entry for a partition. Mirrors the per-partition hard-state key convention
    /// (<c>raft_hardstate_p{id}</c>).
    /// </summary>
    private byte[] LastCheckpointKey(int partitionId) =>
        // Cached: this key is rebuilt (interpolated string + UTF-8 encode) on every checkpoint
        // read/write and inside every ReadLogs restore; the partition set is small and stable.
        lastCheckpointKeys.GetOrAdd(partitionId, static pid => Encoding.UTF8.GetBytes($"raft_last_checkpoint_p{pid}"));

    /// <summary>Cache for <see cref="LastCheckpointKey"/>; key bytes are immutable once built.</summary>
    private readonly ConcurrentDictionary<int, byte[]> lastCheckpointKeys = new();

    /// <summary>
    /// Reads the persisted last-checkpoint id for <paramref name="partitionId"/> as an O(1) point lookup
    /// in the metadata CF. Returns <c>-1</c> when no checkpoint has ever been recorded for the partition
    /// (fresh partition, or one whose recorded checkpoint was truncated away). This is the authoritative
    /// source — there is no reverse-scan fallback (see the feature spec: backwards compatibility is out of
    /// scope, the key is maintained atomically with every checkpoint mutation).
    /// </summary>
    private long GetLastCheckpointFromMeta(int partitionId)
    {
        byte[] value = db.Get(LastCheckpointKey(partitionId), cf: metadataColumnFamily);

        return value is not null && long.TryParse(Encoding.UTF8.GetString(value), out long id) ? id : -1;
    }

    /// <summary>
    /// Stages the persisted last-checkpoint id for <paramref name="partitionId"/> into
    /// <paramref name="writeBatch"/> so it is applied atomically with the log mutation in the same
    /// <see cref="WriteBatch"/> (RocksDB applies a batch all-or-nothing). Every checkpoint write funnels
    /// through here so the persisted value can never drift from the log.
    /// </summary>
    private void PutLastCheckpointToBatch(WriteBatch writeBatch, int partitionId, long value) =>
        writeBatch.Put(LastCheckpointKey(partitionId), Encoding.UTF8.GetBytes(value.ToString()), cf: metadataColumnFamily);

    /// <summary>
    /// Verifies that every log id in <c>(floorExclusive, checkpointId]</c> is durably present for the
    /// partition — in the column family, staged in the current write batch (<paramref name="stagedIds"/> /
    /// <paramref name="stagedSingleId"/>), or already certified by the existing floor. Any entry type
    /// counts: presence is the property being certified, resolution is the drain's concern.
    ///
    /// <para>The persisted last-checkpoint id is trusted as an applied/compacted certificate for its whole
    /// prefix by restore frontier seeding, by the apply drains' compacted-below-floor skip, and by
    /// compaction. A <see cref="RaftLogType.CommittedCheckpoint"/> row that lands OVER a replication gap
    /// (the unanchored commit broadcast on a catching-up follower) must therefore not raise the floor:
    /// doing so certified entries this node never held, the drains skipped them as compacted, compaction
    /// deleted them once backfill stored them, and the consumer permanently lost committed writes.</para>
    ///
    /// <para>A floor of <c>-1</c> (never recorded) anchors the scan at id 1. That is deliberately strict:
    /// nothing else attests where the log's compacted prefix ended, so a partition whose checkpoint
    /// metadata was truncated away withholds advances (and logs) rather than certifying an unknown prefix.
    /// Runs under the exclusive write guard (checkpoint batches only), so the scan cannot race appends.</para>
    /// </summary>
    private bool VerifyCheckpointPrefixPresent(
        int partitionId,
        ColumnFamilyHandle columnFamilyHandle,
        long floorExclusive,
        long checkpointId,
        HashSet<long>? stagedIds,
        long stagedSingleId,
        out long firstMissing)
    {
        firstMissing = -1;

        long expected = Math.Max(floorExclusive, 0) + 1;
        if (expected > checkpointId)
            return true;

        // Rows below the compaction floor are logically absent even while their file is still on
        // disk — identical to the answer the physical delete used to give for the same span.
        if (expected < GetCompactionFloor(partitionId))
        {
            firstMissing = expected;
            return false;
        }

        using Iterator iterator = db.NewIterator(cf: columnFamilyHandle);
        Span<byte> seekKey = stackalloc byte[LogKeyWidth];
        BuildLogKey(seekKey, partitionId, expected);
        iterator.Seek(seekKey);

        Span<byte> partitionPrefix = stackalloc byte[PartitionPrefixWidth];
        BuildPartitionPrefix(partitionPrefix, partitionId);

        while (expected <= checkpointId)
        {
            long present = long.MaxValue;
            if (iterator.Valid() && iterator.GetKeySpan().StartsWith(partitionPrefix))
                present = ParseLogIdFromKey(iterator.GetKeySpan());

            if (present <= expected)
            {
                if (present == expected)
                    expected++;
                iterator.Next();
                continue;
            }

            if (expected == stagedSingleId || (stagedIds is not null && stagedIds.Contains(expected)))
            {
                expected++;
                continue;
            }

            firstMissing = expected;
            return false;
        }

        return true;
    }

    /// <summary>
    /// Stages deletion of the persisted last-checkpoint id (equivalent to "no checkpoint", i.e. a future
    /// read returns <c>-1</c>) into <paramref name="writeBatch"/>. Used when a truncation removes the last
    /// remaining checkpoint entry.
    /// </summary>
    private void DeleteLastCheckpointFromBatch(WriteBatch writeBatch, int partitionId) =>
        writeBatch.Delete(LastCheckpointKey(partitionId), cf: metadataColumnFamily);

    // ── Persisted commit frontier ────────────────────────────────────────────────────────────────
    //
    // Field finding (2026-09-09 write probe): every committed entry crossed the RocksDB WAL twice —
    // once as its Proposed row, once as the byte-identical full row rewritten with Type=Committed —
    // which made the commit path responsible for roughly half of this database's WAL bytes. The
    // commit carries one bit of information, so it is persisted as one small metadata value instead:
    // a per-partition COMMIT FRONTIER, the highest id F such that every row with id <= F is
    // resolved. A Proposed row with id <= F reads as Committed (see ToRaftLog); rows above F carry
    // their own on-disk type, exactly as before.
    //
    // Safety properties, all load-bearing:
    //  * The frontier only advances over rows that are durably present with the SAME term as the
    //    commit marker (probed via a point header read, or staged as Proposed in the same
    //    WriteBatch, which lands atomically with the frontier put). A divergent-term row, an
    //    absent row (backfill first-writes), a rollback, or a commit over an unresolved gap still
    //    writes the full row, so nothing is ever representable only in memory.
    //  * Same-partition Write calls are serialized FIFO by FairWalScheduler (single writer per
    //    partition), so the read-modify-write of the frontier never races another Write. The
    //    truncation/boundary paths mutate it under the exclusive side of writeGuard, which Write's
    //    shared side already excludes.
    //  * Truncations CLAMP the frontier to the surviving prefix. Without the clamp, a fresh
    //    Proposed row later written at a truncated id would read as Committed.
    //  * The frontier NEVER certifies a Proposed row it did not row-verify: reads of rows below a
    //    checkpoint (or a snapshot boundary) keep their on-disk type exactly as before. Nodes whose
    //    history began with a snapshot or backfill re-anchor through the bounded catch-up walk in
    //    TryAdvanceFrontierFor, which passes only row-resolved entries and compacted ids the
    //    persisted checkpoint certifies — see its summary.
    //  * An older binary that ignores the frontier key reads absorbed entries as Proposed. That is
    //    the conservative direction: restore reconstructs a lower commit frontier and the leader
    //    re-supply / current-term re-commit paths deliver the entries, the same recovery the
    //    two-fsync follower crash case already relies on. No format-version bump is needed —
    //    absence of the key degrades to the pre-frontier behavior.
    //
    // The absorb fast path applies to plain Committed markers only. CommittedCheckpoint rows keep
    // the full-row path (they are rare and anchor recovery); rollbacks keep full rows (rare).

    /// <summary>
    /// Bound on the gap the frontier may verify row-by-row when a commit lands above F+1. A
    /// transient reorder of the commit broadcast leaves a short run of full-row Committed entries;
    /// probing them lets the frontier re-chain instead of stalling until the next checkpoint. A
    /// gap larger than this writes the full row (safe fallback), so the per-marker probe cost
    /// stays bounded.
    /// </summary>
    private const int FrontierCatchUpBound = 64;

    /// <summary>Cache for <see cref="CommitFrontierKey"/>; key bytes are immutable once built.</summary>
    private readonly ConcurrentDictionary<int, byte[]> commitFrontierKeys = new();

    /// <summary>
    /// In-memory copy of the persisted commit frontier per partition. Raised only AFTER the
    /// advancing batch was applied (never ahead of durable state), set exactly under the exclusive
    /// write guard by the truncation paths, and cleared by <see cref="ReopenEngine"/> so a reopened
    /// engine reloads from metadata.
    /// </summary>
    private readonly ConcurrentDictionary<int, long> commitFrontierCache = new();

    /// <summary>Builds the metadata-CF key that stores the commit frontier for a partition.</summary>
    private byte[] CommitFrontierKey(int partitionId) =>
        commitFrontierKeys.GetOrAdd(partitionId, static pid => Encoding.UTF8.GetBytes($"raft_commit_frontier_p{pid}"));

    /// <summary>
    /// Returns the commit frontier for <paramref name="partitionId"/> (0 = nothing certified),
    /// loading it from the metadata CF on first use.
    /// </summary>
    private long GetCommitFrontier(int partitionId)
    {
        if (commitFrontierCache.TryGetValue(partitionId, out long cached))
            return cached;

        byte[] value = db.Get(CommitFrontierKey(partitionId), cf: metadataColumnFamily);
        long persisted = value is not null && long.TryParse(Encoding.UTF8.GetString(value), out long id) ? id : 0;

        return commitFrontierCache.TryAdd(partitionId, persisted) ? persisted : commitFrontierCache[partitionId];
    }

    /// <summary>Stages a commit-frontier put so it lands atomically with the batch's row mutations.</summary>
    private void PutCommitFrontierToBatch(WriteBatch writeBatch, int partitionId, long value) =>
        writeBatch.Put(CommitFrontierKey(partitionId), Encoding.UTF8.GetBytes(value.ToString()), cf: metadataColumnFamily);

    /// <summary>
    /// Raises the cached frontier monotonically. Used after a successful advancing write; a raise
    /// can never certify more than the value that was just made durable.
    /// </summary>
    private void RaiseCommitFrontierCache(int partitionId, long value) =>
        commitFrontierCache.AddOrUpdate(partitionId, static (_, v) => v, static (_, old, v) => Math.Max(old, v), value);

    /// <summary>Builds the metadata-CF key that stores the compaction floor for a partition.</summary>
    private byte[] CompactionFloorKey(int partitionId) =>
        compactionFloorKeys.GetOrAdd(partitionId, static pid => Encoding.UTF8.GetBytes($"raft_compaction_floor_p{pid}"));

    /// <summary>
    /// Returns the compaction floor for <paramref name="partitionId"/> — rows with a smaller id are
    /// logically deleted (0 = nothing compacted) — loading it from the metadata CF on first use.
    /// See the persisted-compaction-floor region for the design.
    /// </summary>
    private long GetCompactionFloor(int partitionId)
    {
        if (compactionFloorCache.TryGetValue(partitionId, out long cached))
            return cached;

        byte[] value = db.Get(CompactionFloorKey(partitionId), cf: metadataColumnFamily);
        long persisted = value is not null && long.TryParse(Encoding.UTF8.GetString(value), out long id) ? id : 0;

        return compactionFloorCache.TryAdd(partitionId, persisted) ? persisted : compactionFloorCache[partitionId];
    }

    /// <summary>Stages a compaction-floor put so it lands atomically with the batch's other mutations.</summary>
    private void PutCompactionFloorToBatch(WriteBatch writeBatch, int partitionId, long value) =>
        writeBatch.Put(CompactionFloorKey(partitionId), Encoding.UTF8.GetBytes(value.ToString()), cf: metadataColumnFamily);

    /// <summary>Test-only view of the persisted compaction floor.</summary>
    internal long GetCompactionFloorForTesting(int partitionId)
    {
        using EngineLease lease = AcquireEngine();
        return GetCompactionFloor(partitionId);
    }

    /// <summary>
    /// Reads only the header of the row at (<paramref name="partitionId"/>, <paramref name="logId"/>)
    /// via <see cref="HeaderSpanDeserializer"/>. Returns <c>Found=false</c> when the key is absent.
    /// </summary>
    private (bool Found, long Term, int Type) ProbeRowHeader(int partitionId, ColumnFamilyHandle cf, long logId)
    {
        if (logId < GetCompactionFloor(partitionId))
            return default;

        Span<byte> key = stackalloc byte[LogKeyWidth];
        BuildLogKey(key, partitionId, logId);
        return db.Get(key, HeaderSpanDeserializer.Instance, cf: cf);
    }

    /// <summary>
    /// Decides how far the frontier may advance for the plain <see cref="RaftLogType.Committed"/>
    /// marker <paramref name="log"/>, walking upward from <paramref name="pendingFrontier"/>.
    /// <c>Absorbed</c> is <see langword="true"/> when the walk reached <c>log.Id</c> itself — the
    /// marker then needs no row write at all. A partial advance (the walk certified part of the
    /// gap before stopping or exhausting its budget) is still returned and persisted, so a node
    /// re-anchoring after a snapshot or backfill converges across batches instead of never.
    ///
    /// <para>The walk may pass an id only on evidence that keeps every read byte-identical:</para>
    /// <list type="bullet">
    ///   <item>A durably present row of a RESOLVED type (Committed / RolledBack / either
    ///     checkpoint), or a resolution staged in the current batch (it lands atomically with the
    ///     frontier put). Never a Proposed row — that is precisely the state the frontier would
    ///     re-type, and only the absorb of its own marker may certify it.</item>
    ///   <item>An ABSENT id at or below the persisted last-checkpoint id: the checkpoint only
    ///     advances over a verified-present prefix, so absence there means compaction removed a
    ///     certified entry. Passing an absent id transforms no row. Absent spans are crossed with
    ///     one iterator hop, so a compacted multi-million-entry prefix costs O(1), not O(n).</item>
    /// </list>
    ///
    /// <para>The target row itself must be durably present with the marker's term as
    /// Proposed/Committed (or staged as Proposed in this batch); a divergent term or an absent row
    /// (a backfill first-write) refuses the absorb and the full row is written, as before.</para>
    /// </summary>
    private (long NewFrontier, bool Absorbed) TryAdvanceFrontierFor(
        int partitionId,
        ColumnFamilyHandle cf,
        long pendingFrontier,
        RaftLog log,
        HashSet<long>? stagedProposed,
        HashSet<long>? stagedResolved)
    {
        long target = log.Id;
        long advanced = pendingFrontier;

        if (advanced < target - 1)
        {
            long certifiedFloor = GetLastCheckpointFromMeta(partitionId);
            int steps = 0;

            // Seeking at the compaction floor makes every dead-but-present row below it read as
            // absent, so the walk crosses the compacted prefix with the same one-hop rule it used
            // when those rows were physically deleted (floor <= certified checkpoint).
            using Iterator iterator = db.NewIterator(cf: cf);
            Span<byte> seekKey = stackalloc byte[LogKeyWidth];
            BuildLogKey(seekKey, partitionId, Math.Max(advanced + 1, GetCompactionFloor(partitionId)));
            iterator.Seek(seekKey);

            Span<byte> partitionPrefix = stackalloc byte[PartitionPrefixWidth];
            BuildPartitionPrefix(partitionPrefix, partitionId);

            while (advanced < target - 1 && steps < FrontierCatchUpBound)
            {
                steps++;
                long next = advanced + 1;

                // Rows staged in the current WriteBatch are invisible to the iterator.
                if (stagedResolved is not null && stagedResolved.Contains(next))
                {
                    advanced = next;
                    continue;
                }

                if (stagedProposed is not null && stagedProposed.Contains(next))
                    return (advanced, false);

                while (iterator.Valid() && iterator.GetKeySpan().StartsWith(partitionPrefix)
                       && ParseLogIdFromKey(iterator.GetKeySpan()) < next)
                    iterator.Next();

                long present = iterator.Valid() && iterator.GetKeySpan().StartsWith(partitionPrefix)
                    ? ParseLogIdFromKey(iterator.GetKeySpan())
                    : long.MaxValue;

                if (present > next)
                {
                    if (next > certifiedFloor)
                        return (advanced, false);

                    // One hop over the whole compacted-certified absent span.
                    advanced = Math.Min(present - 1, Math.Min(certifiedFloor, target - 1));
                    continue;
                }

                ReadHeaderFromWire(iterator.GetValueSpan(), out _, out _, out _, out int type);
                if (type is not ((int)RaftLogType.Committed
                                 or (int)RaftLogType.CommittedCheckpoint
                                 or (int)RaftLogType.RolledBack
                                 or (int)RaftLogType.RolledBackCheckpoint))
                    return (advanced, false);

                advanced = next;
                iterator.Next();
            }

            if (advanced < target - 1)
                return (advanced, false);
        }

        if (stagedProposed is not null && stagedProposed.Contains(target))
            return (target, true);

        (bool found, long term, int rowType) = ProbeRowHeader(partitionId, cf, target);
        if (found && term == log.Term
            && rowType is (int)RaftLogType.Proposed or (int)RaftLogType.Committed)
            return (target, true);

        return (advanced, false);
    }

    /// <summary>
    /// Test-only: forces the shard CF holding <paramref name="partitionId"/> to compact its whole
    /// key range down to the bottom level — a real merge, so a test can check what a (full-range)
    /// tombstone does to rows under compaction, or exercise <see cref="DropFullyDeadFiles"/> against
    /// bottom-level files without waiting for the background trivial move.
    /// </summary>
    internal void CompactShardToBottomForTesting(int partitionId)
    {
        using EngineLease lease = AcquireEngine();
        ColumnFamilyHandle cf = GetColumnFamily(partitionId);
        Native.Instance.rocksdb_compact_range_cf(
            db.Handle, cf.Handle, (byte[]?)null, UIntPtr.Zero, (byte[]?)null, UIntPtr.Zero);
    }

    /// <summary>
    /// Test-only: rewrites the persisted commit frontier to model a crash that lost the frontier
    /// put (the counterpart of demoting on-disk commit rows back to Proposed — under the frontier
    /// design the marker's durability lives in the metadata key, so a faithful crash simulation
    /// must regress both). A value &lt;= 0 removes the key entirely.
    /// </summary>
    internal void RegressCommitFrontierForTesting(int partitionId, long value)
    {
        using EngineLease lease = AcquireEngine();

        if (value <= 0)
            db.Remove(CommitFrontierKey(partitionId), cf: metadataColumnFamily, writeOptions: writeOptions);
        else
            db.Put(CommitFrontierKey(partitionId), Encoding.UTF8.GetBytes(value.ToString()),
                   metadataColumnFamily, writeOptions);

        commitFrontierCache[partitionId] = Math.Max(value, 0);
    }

    /// <summary>
    /// Cached singleton reading the header fields (term, type) of a row value in place, used by
    /// the frontier probes. <c>Found</c> disambiguates a missing key (the deserializer only runs
    /// on a hit).
    /// </summary>
    private sealed class HeaderSpanDeserializer : ISpanDeserializer<(bool Found, long Term, int Type)>
    {
        public static readonly HeaderSpanDeserializer Instance = new();

        public (bool Found, long Term, int Type) Deserialize(ReadOnlySpan<byte> buffer)
        {
            ReadHeaderFromWire(buffer, out _, out _, out long term, out int type);
            return (true, term, type);
        }
    }

    /// <summary>
    /// Bounded reverse scan for the highest <see cref="RaftLogType.CommittedCheckpoint"/> id whose id is
    /// <c>≤ upperIdInclusive</c>, or <c>-1</c> if none. Used ONLY on the rare truncation-adjustment path
    /// (a truncation that removes the recorded checkpoint) to recompute the surviving checkpoint — it is
    /// never on the restore/read hot path, which is the whole point of persisting the id. Cost is O(entries
    /// between <paramref name="upperIdInclusive"/> and the surviving checkpoint), same as the old
    /// GetLastCheckpoint scan but confined to an operation that essentially never fires for committed data.
    /// </summary>
    private long ScanHighestCheckpointAtMost(int partitionId, ColumnFamilyHandle columnFamilyHandle, long upperIdInclusive)
    {
        long floor = GetCompactionFloor(partitionId);

        using Iterator? iterator = db.NewIterator(cf: columnFamilyHandle);
        SeekToLastPartitionKey(iterator, partitionId);

        while (iterator.Valid() && KeyBelongsToPartition(iterator.GetKeySpan(), partitionId))
        {
            long id = ParseLogIdFromKey(iterator.GetKeySpan());
            if (id < floor)
                break;

            if (id <= upperIdInclusive)
            {
                ReadHeaderFromWire(iterator.GetValueSpan(), out _, out _, out _, out int type);
                if (type == (int)RaftLogType.CommittedCheckpoint)
                    return id;
            }

            iterator.Prev();
        }

        return -1;
    }

    /// <inheritdoc/>
    public RaftOperationStatus DeletePartitionWAL(int partitionId)
    {
        RaftOperationStatus status = DeletePartitionWALCore(partitionId);

        if (status == RaftOperationStatus.Errored && TryRecoverEngineAfterFailure())
            status = DeletePartitionWALCore(partitionId);

        if (status == RaftOperationStatus.Success)
            NoteStorageSuccess();

        return status;
    }

    private RaftOperationStatus DeletePartitionWALCore(int partitionId)
    {
        try
        {
            using EngineLease lease = AcquireEngine();

            ColumnFamilyHandle columnFamilyHandle = GetColumnFamily(partitionId);

            // One range tombstone covers the partition's entire key span instead of one point
            // tombstone per entry: a wipe of a large partition previously staged millions of
            // tombstones that each had to flush and compact. The iterator probe below only decides
            // whether any log row exists at all (to pick the batch vs standalone metadata delete).
            using WriteBatch writeBatch = new();
            int staged = 0;

            using (Iterator? iterator = db.NewIterator(cf: columnFamilyHandle))
            {
                Span<byte> seekKey = stackalloc byte[LogKeyWidth];
                BuildLogKey(seekKey, partitionId, 0);
                iterator.Seek(seekKey);

                Span<byte> partitionPrefix = stackalloc byte[PartitionPrefixWidth];
                BuildPartitionPrefix(partitionPrefix, partitionId);

                if (iterator.Valid() && iterator.GetKeySpan().StartsWith(partitionPrefix))
                {
                    byte[] rangeBegin = new byte[LogKeyWidth];
                    BuildLogKey(rangeBegin, partitionId, 0);

                    // ';' sorts above the ':' separator, so this bound is exclusive-above every
                    // possible log key of the partition and below every key of the next partition.
                    byte[] rangeEnd = new byte[PartitionIdWidth + 1];
                    BuildPartitionUpperBoundKey(rangeEnd, partitionId);

                    writeBatch.DeleteRange(rangeBegin, (ulong)rangeBegin.Length, rangeEnd, (ulong)rangeEnd.Length, columnFamilyHandle);
                    staged = 1;
                }
            }

            // Always drop the persisted last-checkpoint id and commit frontier too: wiping the partition
            // must not leave a stale replay floor — or a stale frontier that would certify rows a reused
            // partition id later writes — for the next occupant (there is no scan fallback to correct
            // either). Batch them with the log deletes when there are any, else delete them standalone.
            // The compaction floor goes too: a reused partition id may start writing from low ids
            // again, and a stale (higher) floor would hide — and leak — those new entries.
            if (staged > 0)
            {
                writeBatch.Delete(LastCheckpointKey(partitionId), cf: metadataColumnFamily);
                writeBatch.Delete(CommitFrontierKey(partitionId), cf: metadataColumnFamily);
                writeBatch.Delete(CompactionFloorKey(partitionId), cf: metadataColumnFamily);
                db.Write(writeBatch, writeOptions);
            }
            else
            {
                db.Remove(LastCheckpointKey(partitionId), cf: metadataColumnFamily, writeOptions: writeOptions);
                db.Remove(CommitFrontierKey(partitionId), cf: metadataColumnFamily, writeOptions: writeOptions);
                db.Remove(CompactionFloorKey(partitionId), cf: metadataColumnFamily, writeOptions: writeOptions);
            }

            compactionFloorCache.TryRemove(partitionId, out _);
            commitFrontierCache.TryRemove(partitionId, out _);

            return RaftOperationStatus.Success;
        }
        catch (Exception ex)
        {
            logger.LogError("Error during DeletePartitionWAL({PartitionId}): {Message}", partitionId, ex.Message);

            NoteStorageFailure(ex);
            return RaftOperationStatus.Errored;
        }
    }

    /// <inheritdoc/>
    public RaftOperationStatus TruncateLogsAfter(int partitionId, long afterLogId)
    {
        RaftOperationStatus status = TruncateLogsAfterCore(partitionId, afterLogId);

        if (status == RaftOperationStatus.Errored && TryRecoverEngineAfterFailure())
            status = TruncateLogsAfterCore(partitionId, afterLogId);

        if (status == RaftOperationStatus.Success)
            NoteStorageSuccess();

        return status;
    }

    private RaftOperationStatus TruncateLogsAfterCore(int partitionId, long afterLogId)
    {
        try
        {
            using EngineLease lease = AcquireEngine();

            writeGuard.EnterWriteLock();
            try
            {
            ColumnFamilyHandle columnFamilyHandle = GetColumnFamily(partitionId);

            // Staged directly into the batch — see DeletePartitionWAL for the rationale.
            using WriteBatch writeBatch = new();
            int staged = 0;

            using (Iterator? iterator = db.NewIterator(cf: columnFamilyHandle))
            {
                Span<byte> seekKey = stackalloc byte[LogKeyWidth];
                BuildLogKey(seekKey, partitionId, afterLogId + 1);
                iterator.Seek(seekKey);

                Span<byte> partitionPrefix = stackalloc byte[PartitionPrefixWidth];
                BuildPartitionPrefix(partitionPrefix, partitionId);

                while (iterator.Valid() && iterator.GetKeySpan().StartsWith(partitionPrefix))
                {
                    writeBatch.Delete(iterator.GetKeySpan(), cf: columnFamilyHandle);
                    staged++;
                    iterator.Next();
                }
            }

            // Clamp the commit frontier to the truncation boundary, even when no row was staged:
            // rows the frontier certified may already be gone (compaction can leave the frontier
            // above the surviving tail), and a fresh Proposed row later written at a truncated id
            // must never read as Committed through a stale frontier. Staged into the same batch so
            // the clamp and the deletes land atomically.
            long clampedFrontier = Math.Max(afterLogId, 0);
            bool clampFrontier = GetCommitFrontier(partitionId) > clampedFrontier;
            if (clampFrontier)
                PutCommitFrontierToBatch(writeBatch, partitionId, clampedFrontier);

            // Same for the compaction floor: rows written later at ids above afterLogId must not sit
            // below it and read as compacted. afterLogId + 1 keeps every surviving row's status.
            long clampedFloor = Math.Max(afterLogId + 1, 0);
            bool clampFloor = GetCompactionFloor(partitionId) > clampedFloor;
            if (clampFloor)
                PutCompactionFloorToBatch(writeBatch, partitionId, clampedFloor);

            if (staged > 0)
            {
                // If the truncation removes the recorded checkpoint (it sits above afterLogId), recompute
                // the surviving checkpoint id (highest CommittedCheckpoint ≤ afterLogId, or -1) and adjust
                // the persisted value in the SAME batch. There is no scan fallback, so this must be exact.
                // Committed checkpoints are effectively never truncated, so this reverse scan almost never
                // runs and never touches the restore hot path.
                long recorded = GetLastCheckpointFromMeta(partitionId);
                if (recorded > afterLogId)
                {
                    long surviving = ScanHighestCheckpointAtMost(partitionId, columnFamilyHandle, afterLogId);
                    if (surviving < 0)
                        DeleteLastCheckpointFromBatch(writeBatch, partitionId);
                    else
                        PutLastCheckpointToBatch(writeBatch, partitionId, surviving);
                }
            }

            if (staged > 0 || clampFrontier || clampFloor)
                db.Write(writeBatch, writeOptions);

            // Exact set (not a raise): truncation is the one path allowed to LOWER the frontier.
            // Runs under the exclusive writeGuard, so no concurrent Write can interleave a raise.
            if (clampFrontier)
                commitFrontierCache[partitionId] = clampedFrontier;

            if (clampFloor)
                compactionFloorCache[partitionId] = clampedFloor;

            return RaftOperationStatus.Success;
            }
            finally
            {
                writeGuard.ExitWriteLock();
            }
        }
        catch (Exception ex)
        {
            logger.LogError("TruncateLogsAfter({PartitionId}, {AfterLogId}): {Message}", partitionId, afterLogId, ex.Message);

            NoteStorageFailure(ex);
            return RaftOperationStatus.Errored;
        }
    }

    /// <inheritdoc/>
    public RaftOperationStatus TruncateProposedLogsAfter(int partitionId, long afterLogId)
    {
        RaftOperationStatus status = TruncateProposedLogsAfterCore(partitionId, afterLogId);

        if (status == RaftOperationStatus.Errored && TryRecoverEngineAfterFailure())
            status = TruncateProposedLogsAfterCore(partitionId, afterLogId);

        if (status == RaftOperationStatus.Success)
            NoteStorageSuccess();

        return status;
    }

    private RaftOperationStatus TruncateProposedLogsAfterCore(int partitionId, long afterLogId)
    {
        try
        {
            using EngineLease lease = AcquireEngine();

            writeGuard.EnterWriteLock();
            try
            {
                ColumnFamilyHandle columnFamilyHandle = GetColumnFamily(partitionId);

                // A Proposed row at or below the commit frontier is effectively COMMITTED (the commit
                // was persisted as the frontier instead of a row rewrite) — deleting it would silently
                // lose a committed entry. The frontier check therefore joins the type check below.
                long commitFrontier = GetCommitFrontier(partitionId);

                // Staged directly into the batch — see DeletePartitionWAL for the rationale. This runs
                // once per follower group batch in steady state, so the per-key byte[] churn mattered.
                using WriteBatch writeBatch = new();
                int staged = 0;

                using (Iterator? iterator = db.NewIterator(cf: columnFamilyHandle))
                {
                    Span<byte> seekKey = stackalloc byte[LogKeyWidth];
                    BuildLogKey(seekKey, partitionId, afterLogId + 1);
                    iterator.Seek(seekKey);

                    Span<byte> partitionPrefix = stackalloc byte[PartitionPrefixWidth];
                    BuildPartitionPrefix(partitionPrefix, partitionId);

                    while (iterator.Valid() && iterator.GetKeySpan().StartsWith(partitionPrefix))
                    {
                        // Only unresolved (Proposed / ProposedCheckpoint) entries are removable; resolved
                        // entries are quorum-agreed and load-bearing for the commit frontier.
                        ReadHeaderFromWire(iterator.GetValueSpan(), out _, out _, out _, out int type);
                        if (type is (int)RaftLogType.Proposed or (int)RaftLogType.ProposedCheckpoint
                            && ParseLogIdFromKey(iterator.GetKeySpan()) > commitFrontier)
                        {
                            writeBatch.Delete(iterator.GetKeySpan(), cf: columnFamilyHandle);
                            staged++;
                        }
                        iterator.Next();
                    }
                }

                if (staged > 0)
                    db.Write(writeBatch, writeOptions);

                // No last-checkpoint adjustment: this only removes unresolved (Proposed / ProposedCheckpoint)
                // entries, and a CommittedCheckpoint is resolved — so the recorded checkpoint is never among
                // the deleted keys.
                return RaftOperationStatus.Success;
            }
            finally
            {
                writeGuard.ExitWriteLock();
            }
        }
        catch (Exception ex)
        {
            logger.LogError("TruncateProposedLogsAfter({PartitionId}, {AfterLogId}): {Message}", partitionId, afterLogId, ex.Message);

            NoteStorageFailure(ex);
            return RaftOperationStatus.Errored;
        }
    }

    /// <inheritdoc/>
    /// <remarks>
    /// The delete and the max-read are two distinct operations. The delete (<see cref="TruncateLogsAfter"/>)
    /// is held under <see cref="writeGuard"/> so it does not interleave with a concurrent append; the max-read
    /// that follows is a consistent point read. Holes are effectively absent on the fsync-paced persistent
    /// path (the storm this repair targets is in-memory only), so this method almost never fires on RocksDB.
    /// </remarks>
    public (RaftOperationStatus Status, long MaxLogId) TruncateLogsAfterAndGetMax(int partitionId, long afterLogId)
    {
        RaftOperationStatus status = TruncateLogsAfter(partitionId, afterLogId);
        if (status != RaftOperationStatus.Success)
            return (status, 0);

        return (status, GetMaxLog(partitionId));
    }

    /// <inheritdoc/>
    /// <remarks>
    /// The suffix deletes and the checkpoint put are staged into a single <see cref="WriteBatch"/> and
    /// applied with one <c>db.Write</c>, so the boundary install is atomic (RocksDB applies a batch
    /// all-or-nothing). Durability follows <paramref name="sync"/>: <c>sync:true</c> uses the instance's
    /// sync write options, else the non-sync options. This op dispatches on the read scheduler, NOT the WAL
    /// write scheduler, so the single-writer invariant does not exclude concurrent appends; the read of the
    /// boundary term + the suffix enumeration + the batch write are therefore held under <see cref="writeGuard"/>
    /// (shared with <c>Write</c> and <c>TruncateLogsAfter</c>) so an append cannot land between the scan and
    /// the delete batch and survive the truncation.
    /// </remarks>
    public (RaftOperationStatus Status, bool SuffixTruncated) InstallSnapshotBoundary(
        int partitionId, long snapshotIndex, long lastIncludedTerm, bool sync)
    {
        (RaftOperationStatus Status, bool SuffixTruncated) result = InstallSnapshotBoundaryCore(partitionId, snapshotIndex, lastIncludedTerm, sync);

        if (result.Status == RaftOperationStatus.Errored && TryRecoverEngineAfterFailure())
            result = InstallSnapshotBoundaryCore(partitionId, snapshotIndex, lastIncludedTerm, sync);

        if (result.Status == RaftOperationStatus.Success)
            NoteStorageSuccess();

        return result;
    }

    private (RaftOperationStatus Status, bool SuffixTruncated) InstallSnapshotBoundaryCore(
        int partitionId, long snapshotIndex, long lastIncludedTerm, bool sync)
    {
        WriteOptions effectiveOptions = sync ? writeOptions : NonSynchronousWriteOptions;

        try
        {
            using EngineLease lease = AcquireEngine();

            writeGuard.EnterWriteLock();
            try
            {
            ColumnFamilyHandle columnFamilyHandle = GetColumnFamily(partitionId);

            byte[] boundaryKey = new byte[LogKeyWidth];
            BuildLogKey(boundaryKey, partitionId, snapshotIndex);
            byte[]? existing = snapshotIndex < GetCompactionFloor(partitionId) ? null : db.Get(boundaryKey, cf: columnFamilyHandle);
            long localTerm = existing is null ? -1 : ReadTermFromWire(existing);

            using WriteBatch writeBatch = new();

            bool suffixTruncated = false;
            if (localTerm != lastIncludedTerm)
            {
                // Staged directly into the batch — see DeletePartitionWAL for the rationale.
                int staged = 0;
                using (Iterator? iterator = db.NewIterator(cf: columnFamilyHandle))
                {
                    Span<byte> seekKey = stackalloc byte[LogKeyWidth];
                    BuildLogKey(seekKey, partitionId, snapshotIndex + 1);
                    iterator.Seek(seekKey);

                    Span<byte> partitionPrefix = stackalloc byte[PartitionPrefixWidth];
                    BuildPartitionPrefix(partitionPrefix, partitionId);

                    while (iterator.Valid() && iterator.GetKeySpan().StartsWith(partitionPrefix))
                    {
                        writeBatch.Delete(iterator.GetKeySpan(), cf: columnFamilyHandle);
                        staged++;
                        iterator.Next();
                    }
                }

                suffixTruncated = staged > 0;
            }

            // Upsert the checkpoint marker at the boundary index (same key overwrites any existing entry).
            RaftLogMessage checkpoint = new()
            {
                Partition = partitionId,
                Id = snapshotIndex,
                Term = lastIncludedTerm,
                Type = (int)RaftLogType.CommittedCheckpoint,
            };
            PutToBatch(writeBatch, checkpoint, columnFamilyHandle);

            // Persist the new last-checkpoint id atomically with the boundary install. When the suffix was
            // truncated, every entry above snapshotIndex (including any higher checkpoint) is gone, so the
            // new max is exactly snapshotIndex. When the suffix was retained, a higher checkpoint may still
            // exist above the boundary, so keep the greater of the existing recorded id and snapshotIndex.
            long newCheckpoint = suffixTruncated
                ? snapshotIndex
                : Math.Max(GetLastCheckpointFromMeta(partitionId), snapshotIndex);
            PutLastCheckpointToBatch(writeBatch, partitionId, newCheckpoint);

            // When the suffix was truncated, every row above the boundary is gone, so a higher
            // commit frontier must CLAMP to the boundary — a fresh Proposed row later written at a
            // truncated id must never read as Committed through the stale frontier. The frontier is
            // deliberately NOT raised to the boundary: it only ever certifies rows it row-verified,
            // and a raise would re-type any Proposed residue below the boundary on read.
            long currentFrontier = GetCommitFrontier(partitionId);
            bool clampFrontier = suffixTruncated && currentFrontier > snapshotIndex;
            if (clampFrontier)
                PutCommitFrontierToBatch(writeBatch, partitionId, snapshotIndex);

            // The boundary row itself must be readable, so a compaction floor above it clamps to the
            // boundary — whether or not a suffix was removed. The floor is deliberately not RAISED to
            // the boundary: rows below it stay for the ordinary compaction pass, exactly as before.
            bool clampFloor = GetCompactionFloor(partitionId) > snapshotIndex;
            if (clampFloor)
                PutCompactionFloorToBatch(writeBatch, partitionId, snapshotIndex);

            OnAfterBoundaryScanForTesting?.Invoke();

            db.Write(writeBatch, effectiveOptions);

            if (clampFrontier)
                commitFrontierCache[partitionId] = snapshotIndex;

            if (clampFloor)
                compactionFloorCache[partitionId] = snapshotIndex;

            return (RaftOperationStatus.Success, suffixTruncated);
            }
            finally
            {
                writeGuard.ExitWriteLock();
            }
        }
        catch (Exception ex)
        {
            logger.LogError("InstallSnapshotBoundary({PartitionId}, {SnapshotIndex}): {Message}",
                partitionId, snapshotIndex, ex.Message);

            NoteStorageFailure(ex);
            return (RaftOperationStatus.Errored, false);
        }
    }

    /// <summary>
    /// Compacts and removes logs in the Write-Ahead Log (WAL) for a specific partition that are older than the given checkpoint.
    /// </summary>
    /// <param name="partitionId">
    /// The ID of the partition whose logs are to be compacted.
    /// </param>
    /// <param name="lastCheckpoint">
    /// The log index up to which logs will be considered for compaction. Logs with an ID less than this checkpoint will be removed.
    /// </param>
    /// <param name="compactNumberEntries">
    /// The maximum number of entries removed per internal iterator batch when building the write batch.
    /// </param>
    /// <param name="maxTotalEntries">
    /// When set, multiple internal batches are accumulated into one <c>db.Write</c> so a compaction
    /// pass costs a single fsync.
    /// </param>
    /// <returns>
    /// A tuple of <see cref="RaftOperationStatus"/> and the number of entries removed.
    /// </returns>
    public (RaftOperationStatus Status, int Removed) CompactLogsOlderThan(
        int partitionId,
        long lastCheckpoint,
        int compactNumberEntries,
        int? maxTotalEntries = null)
    {
        (RaftOperationStatus Status, int Removed) result = CompactLogsOlderThanCore(partitionId, lastCheckpoint, compactNumberEntries, maxTotalEntries);

        if (result.Status == RaftOperationStatus.Errored && TryRecoverEngineAfterFailure())
            result = CompactLogsOlderThanCore(partitionId, lastCheckpoint, compactNumberEntries, maxTotalEntries);

        if (result.Status == RaftOperationStatus.Success)
            NoteStorageSuccess();

        return result;
    }

    private (RaftOperationStatus Status, int Removed) CompactLogsOlderThanCore(
        int partitionId,
        long lastCheckpoint,
        int compactNumberEntries,
        int? maxTotalEntries)
    {
        int passCap = maxTotalEntries ?? compactNumberEntries;

        try
        {
            using EngineLease lease = AcquireEngine();

            ColumnFamilyHandle columnFamilyHandle = GetColumnFamily(partitionId);

            // No last-checkpoint update: the floor only ever advances to a value <= lastCheckpoint, so
            // the recorded checkpoint id (which is >= lastCheckpoint) always stays readable.
            long floor = GetCompactionFloor(partitionId);
            int removed = 0;

            // lastCheckpoint is the caller's EFFECTIVE retention floor (checkpoint composed with the
            // durability, hold and live-replica floors), and the persisted floor advances straight to
            // it: logical deletion costs one metadata put however many rows it covers, and the whole-
            // file drop below is anchored at it, so reclaim never lags the floor by more than the
            // files still in L0. The IWAL cap therefore bounds only the WORK of the counting scan
            // here, not the deletion — at the shipped cadence (one pass per ~1,000 Raft batches of
            // ~100 entries) a cap-bounded floor fell behind ingest by ~8x on the 2026-09-10 k175
            // probe and the log grew without bound; the RocksDB conformance subclass documents the
            // relaxed cap.
            long newFloor = Math.Max(floor, lastCheckpoint);

            // Removed = rows physically present in [floor, newFloor): counted exactly up to the cap,
            // and beyond the cap by arithmetic (ids are contiguous per partition), so a large backlog
            // costs O(cap) iterator steps instead of one step per dead row.
            using Iterator? iterator = db.NewIterator(cf: columnFamilyHandle);
            Span<byte> seekKey = stackalloc byte[LogKeyWidth];
            BuildLogKey(seekKey, partitionId, floor);
            iterator.Seek(seekKey);

            Span<byte> partitionPrefix = stackalloc byte[PartitionPrefixWidth];
            BuildPartitionPrefix(partitionPrefix, partitionId);

            long lastCountedId = -1;
            while (iterator.Valid() && iterator.GetKeySpan().StartsWith(partitionPrefix) && removed < passCap)
            {
                long id = ParseLogIdFromKey(iterator.GetKeySpan());
                if (id >= newFloor)
                    break;

                lastCountedId = id;
                removed++;
                iterator.Next();
            }

            if (removed == passCap && iterator.Valid() && iterator.GetKeySpan().StartsWith(partitionPrefix)
                && ParseLogIdFromKey(iterator.GetKeySpan()) < newFloor)
            {
                long remainder = newFloor - 1 - lastCountedId;
                removed = (int)Math.Min(int.MaxValue, removed + Math.Max(0, remainder));
            }

            if (newFloor > floor)
            {
                using WriteBatch writeBatch = new();
                PutCompactionFloorToBatch(writeBatch, partitionId, newFloor);

                // The tombstone that lets rows die in memory. Confined to the active memtable's span
                // when this partition owns the column family, so the flushed file's key range never
                // reaches below its own first row and L0 files stay non-overlapping (trivial moves,
                // whole-file drops). A shared column family gets the full range instead: its files
                // can never be dropped whole for one partition, so the rows must be tombstoned to
                // die at all.
                long tombstoneStart = ShardHoldsOtherPartitions(columnFamilyHandle, partitionId)
                    ? floor
                    : Math.Max(floor, ConfinedTombstoneStart(columnFamilyHandle, partitionId));

                if (tombstoneStart < newFloor)
                {
                    byte[] rangeBegin = new byte[LogKeyWidth];
                    BuildLogKey(rangeBegin, partitionId, tombstoneStart);
                    byte[] rangeEnd = new byte[LogKeyWidth];
                    BuildLogKey(rangeEnd, partitionId, newFloor);
                    writeBatch.DeleteRange(rangeBegin, (ulong)rangeBegin.Length, rangeEnd, (ulong)rangeEnd.Length, columnFamilyHandle);
                }

                // The floor is durable before any file below it is dropped (see the region comment).
                db.Write(writeBatch, writeOptions);
                compactionFloorCache[partitionId] = newFloor;
                LastCompactionWriteCount = 1;

                if (removed > 0)
                    logger.LogDebugRemovedFromWal(removed, partitionId);
            }
            else
            {
                LastCompactionWriteCount = 0;
            }

            // Runs on every pass, not only on an advancing one: files below an unchanged floor may
            // have been moved out of L0 since the last pass and are droppable now.
            DropFullyDeadFiles(columnFamilyHandle, partitionId, newFloor);

            return (RaftOperationStatus.Success, removed);
        }
        catch (Exception ex)
        {
            logger.LogError("Error during compact: {Message}", ex.Message);

            NoteStorageFailure(ex);
            return (RaftOperationStatus.Errored, 0);
        }
    }
    
    /// <summary>
    /// Asks RocksDB to physically delete SST files that lie ENTIRELY within the dead prefix
    /// <c>[key(partition, 0), key(partition, compactionFloor − 1)]</c> — dead data reclaimed with
    /// zero compaction rewrite, which is what is meant to remove the bulk of the log's L5/L6 write
    /// amplification. The bound is the compaction FLOOR (every id below it is retention-eligible),
    /// not the last id this pass happened to tombstone: <c>DeleteFilesInRange</c> drops whole files
    /// and needs no tombstone, so anchoring at the floor reclaims files a cap-bounded pass never
    /// tombstoned. The exclusive-of-floor upper bound (<c>floor − 1</c>) keeps the file holding the
    /// first surviving entry (id == floor) untouched. Files that straddle the bound, or that also
    /// hold another partition's keys (shard CFs are shared across partitions), are left to normal
    /// compaction.
    ///
    /// <para><b>Level 0.</b> RocksDB's <c>DeleteFilesInRange</c> considers levels 1 and below only.
    /// The floor layout makes that sufficient: with no cross-file tombstones the partition's files
    /// do not overlap, so once <see cref="RocksDbWalTuning.ShardLevel0FileNumCompactionTrigger"/>
    /// of them sit in L0 RocksDB moves them to the base level as a metadata-only trivial move
    /// (measured: zero compaction bytes, files keep their numbers), and the next pass drops them
    /// here. Files that straddle the bound (the one holding the floor row) or that also hold
    /// another partition's keys are left to normal compaction.</para>
    ///
    /// <para><b>Failures are surfaced, not swallowed.</b> A native error here means the reclaim did
    /// not happen and the dead prefix stays on disk until a later pass succeeds — the rows are
    /// logically gone through the floor regardless, so the pass itself still succeeds. It is
    /// logged at Warning with the RocksDB message so the condition is visible in a run's logs.</para>
    /// </summary>
    private void DropFullyDeadFiles(ColumnFamilyHandle columnFamilyHandle, int partitionId, long compactionFloor)
    {
        // Need at least id 1 strictly below the floor for there to be anything to drop.
        if (compactionFloor < 2)
            return;

        byte[] rangeBegin = new byte[LogKeyWidth];
        BuildLogKey(rangeBegin, partitionId, 0);
        byte[] rangeEnd = new byte[LogKeyWidth];
        BuildLogKey(rangeEnd, partitionId, compactionFloor - 1);

        IntPtr errptr = IntPtr.Zero;
        Native.Instance.rocksdb_delete_file_in_range_cf(
            db.Handle, columnFamilyHandle.Handle,
            rangeBegin, (UIntPtr)rangeBegin.Length,
            rangeEnd, (UIntPtr)rangeEnd.Length,
            out errptr);

        if (errptr != IntPtr.Zero)
        {
            string message = global::System.Runtime.InteropServices.Marshal.PtrToStringAnsi(errptr) ?? "<unknown>";
            Native.Instance.rocksdb_free(errptr);
            logger.LogWarning(
                "DeleteFilesInRange for partition {PartitionId} below floor {Floor} failed: {Message}; the dead prefix stays on disk until a later pass reclaims it",
                partitionId, compactionFloor, message);
        }
    }

    /// <summary>
    /// True when the shard column family physically holds a key of any partition other than
    /// <paramref name="partitionId"/> — two seeks, no value reads. Decided from the data rather
    /// than from the partitions this process has touched, so a partition that exists on disk but
    /// was never accessed still counts. Dead-but-present rows of a wiped partition count too,
    /// which errs toward the full-range tombstone (safe, merely slower) until they compact away.
    /// </summary>
    private bool ShardHoldsOtherPartitions(ColumnFamilyHandle columnFamilyHandle, int partitionId)
    {
        using Iterator iterator = db.NewIterator(cf: columnFamilyHandle);

        Span<byte> partitionPrefix = stackalloc byte[PartitionPrefixWidth];
        BuildPartitionPrefix(partitionPrefix, partitionId);

        iterator.SeekToFirst();
        if (iterator.Valid() && !iterator.GetKeySpan().StartsWith(partitionPrefix))
            return true;

        Span<byte> upperBoundKey = stackalloc byte[PartitionIdWidth + 1];
        BuildPartitionUpperBoundKey(upperBoundKey, partitionId);
        iterator.Seek(upperBoundKey);

        return iterator.Valid();
    }

    /// <summary>
    /// Lowest id a compaction tombstone may start at without reaching below the active memtable:
    /// <c>head − entries-in-active-memtable + 1 + <see cref="ConfinedTombstoneMargin"/></c>. The
    /// partition owns the column family (checked by the caller), so the active memtable's entries
    /// are its newest rows and the arithmetic is exact up to the margin. A property read failure
    /// yields <see cref="long.MaxValue"/> — no tombstone at all, which is safe: the rows flush and
    /// the whole-file drop reclaims them — rather than an unconfined tombstone that would make the
    /// flushed file overlap its predecessors and forfeit the trivial move.
    /// </summary>
    private long ConfinedTombstoneStart(ColumnFamilyHandle columnFamilyHandle, int partitionId)
    {
        if (!long.TryParse(db.GetProperty("rocksdb.num-entries-active-mem-table", columnFamilyHandle), out long activeEntries))
            return long.MaxValue;

        using Iterator iterator = db.NewIterator(cf: columnFamilyHandle);
        SeekToLastPartitionKey(iterator, partitionId);

        if (!iterator.Valid() || !KeyBelongsToPartition(iterator.GetKeySpan(), partitionId))
            return long.MaxValue;

        long head = ParseLogIdFromKey(iterator.GetKeySpan());
        return Math.Max(0, head - activeEntries + 1 + ConfinedTombstoneMargin);
    }

    /// <summary>
    /// Test-only: the live SST files of the shard column family holding <paramref name="partitionId"/>,
    /// with the id span each covers, so a test can prove that files reach the base level with the
    /// numbers they were flushed under (moved, not rewritten) and that no two files of the partition
    /// overlap. Read through RocksDB's live-files metadata; names are relative (e.g. <c>/000012.sst</c>).
    /// </summary>
    internal List<(string Name, int Level, long SmallestId, long LargestId)> GetShardLiveFilesForTesting(int partitionId)
    {
        using EngineLease lease = AcquireEngine();

        string shardName = "shard" + (partitionId % MaxShards);
        List<(string, int, long, long)> result = [];

        IntPtr files = Native.Instance.rocksdb_livefiles(db.Handle);
        try
        {
            int count = Native.Instance.rocksdb_livefiles_count(files);
            for (int i = 0; i < count; i++)
            {
                string? cfName = global::System.Runtime.InteropServices.Marshal.PtrToStringAnsi(Native.Instance.rocksdb_livefiles_column_family_name(files, i));
                if (!string.Equals(cfName, shardName, StringComparison.Ordinal))
                    continue;

                string name = global::System.Runtime.InteropServices.Marshal.PtrToStringAnsi(Native.Instance.rocksdb_livefiles_name(files, i)) ?? "";
                int level = Native.Instance.rocksdb_livefiles_level(files, i);

                IntPtr smallestPtr = Native.Instance.rocksdb_livefiles_smallestkey(files, i, out UIntPtr smallestLen);
                IntPtr largestPtr = Native.Instance.rocksdb_livefiles_largestkey(files, i, out UIntPtr largestLen);

                byte[] smallest = new byte[(int)smallestLen];
                global::System.Runtime.InteropServices.Marshal.Copy(smallestPtr, smallest, 0, smallest.Length);
                byte[] largest = new byte[(int)largestLen];
                global::System.Runtime.InteropServices.Marshal.Copy(largestPtr, largest, 0, largest.Length);

                long smallestId = smallest.Length == LogKeyWidth && KeyBelongsToPartition(smallest, partitionId) ? ParseLogIdFromKey(smallest) : -1;
                long largestId = largest.Length == LogKeyWidth && KeyBelongsToPartition(largest, partitionId) ? ParseLogIdFromKey(largest) : -1;

                result.Add((name, level, smallestId, largestId));
            }
        }
        finally
        {
            Native.Instance.rocksdb_livefiles_destroy(files);
        }

        return result;
    }

    /// <summary>
    /// The largest serialized message size that is serialized onto a <c>stackalloc</c> buffer before
    /// falling back to an <see cref="ArrayPool{T}"/> rental. Bounds stack usage on the write path while
    /// keeping the common (small) entry copy-free of any managed allocation.
    /// </summary>
    private const int StackallocThreshold = 256;

    /// <summary>
    /// Serializes <paramref name="message"/> into <paramref name="destination"/>, which must be exactly
    /// <see cref="MessageExtensions.CalculateSize"/> bytes long (sized by the caller). The bytes produced
    /// are byte-for-byte identical to the previous <c>MemoryStream.ToArray()</c> path — Protobuf writes a
    /// canonical encoding regardless of the sink — so on-disk format is unchanged.
    /// </summary>
    private static void SerializeInto(RaftLogMessage message, Span<byte> destination)
    {
        message.WriteTo(destination);
    }

    /// <summary>
    /// Applies a shared <see cref="BlockBasedTableOptions"/> to <paramref name="cfOptions"/> when
    /// <paramref name="sharedBbto"/> is non-null, then returns <paramref name="cfOptions"/>. A no-op
    /// when <paramref name="sharedBbto"/> is null so the caller does not need a branch at each CF site.
    /// </summary>
    private static ColumnFamilyOptions ApplyCfOptions(ColumnFamilyOptions cfOptions, BlockBasedTableOptions? sharedBbto)
    {
        if (sharedBbto is not null)
            cfOptions.SetBlockBasedTableFactory(sharedBbto);
        return cfOptions;
    }

    /// <summary>
    /// Options for the shard column families, which hold the append-then-die log rows. On the
    /// RocksDB defaults (trigger 4 / slowdown 20 / stop 36) the log's write rate outran L0
    /// compaction and RocksDB throttled the Raft writer; the raised L0 triggers give the (now
    /// larger) background pool headroom before a slowdown. The write-buffer sizing widens the
    /// flush unit so an entry and the range tombstone that kills it co-reside and are dropped at
    /// flush — the CF-sizing lever against the compaction rewrite of floor-held entries measured
    /// in probe w2 (compaction wrote 3.7x the flushed bytes). All values come from
    /// <see cref="RocksDbWalTuning"/>, which documents the reasoning and the memory envelope.
    /// </summary>
    private static ColumnFamilyOptions ApplyShardCfOptions(
        ColumnFamilyOptions cfOptions, BlockBasedTableOptions? sharedBbto, RocksDbWalTuning tuning)
    {
        ApplyCfOptions(cfOptions, sharedBbto);
        cfOptions.SetWriteBufferSize((ulong)tuning.ShardWriteBufferSizeBytes);
        cfOptions.SetMinWriteBufferNumberToMerge(tuning.ShardMinWriteBufferNumberToMerge);
        cfOptions.SetMaxWriteBufferNumber(tuning.ShardMaxWriteBufferNumber);
        cfOptions.SetLevel0FileNumCompactionTrigger(tuning.ShardLevel0FileNumCompactionTrigger);
        cfOptions.SetLevel0SlowdownWritesTrigger(tuning.ShardLevel0SlowdownWritesTrigger);
        cfOptions.SetLevel0StopWritesTrigger(tuning.ShardLevel0StopWritesTrigger);

        // Layout knobs (inert at the defaults, kept for probes): the floor layout no longer needs
        // them — files move out of L0 without rewrite and are dropped whole — but a probe can still
        // change the base-level size or switch to universal to compare.
        if (tuning.ShardMaxBytesForLevelBase > 0)
            cfOptions.SetMaxBytesForLevelBase((ulong)tuning.ShardMaxBytesForLevelBase);
        if (tuning.ShardUniversalCompaction)
            cfOptions.SetCompactionStyle(Compaction.Universal);

        return cfOptions;
    }

    private static void BuildLogKey(Span<byte> result, int partitionId, long logId)
    {
        if (result.Length != LogKeyWidth)
            throw new ArgumentException($"RocksDB WAL log keys must be {LogKeyWidth} bytes.", nameof(result));

        ToDecimalBytes(result[..PartitionIdWidth], partitionId);
        result[PartitionIdWidth] = LogKeySeparator;
        ToDecimalBytes(result[(PartitionIdWidth + 1)..], logId);
    }

    private static void BuildPartitionUpperBoundKey(Span<byte> result, int partitionId)
    {
        if (result.Length != PartitionIdWidth + 1)
            throw new ArgumentException($"RocksDB WAL partition upper-bound keys must be {PartitionIdWidth + 1} bytes.", nameof(result));

        ToDecimalBytes(result[..PartitionIdWidth], partitionId);
        result[PartitionIdWidth] = PartitionUpperBoundSeparator;
    }

    /// <summary>
    /// Decodes the log id from a fixed-width WAL key without touching the stored value. The key is
    /// authoritative for both partition and id — <see cref="BuildLogKey"/> derives it from the message's
    /// own <c>Partition</c>/<c>Id</c> on every write — so scans that need only those two fields can skip
    /// reading (and copying out of native memory) the entry's value entirely.
    /// </summary>
    private static long ParseLogIdFromKey(ReadOnlySpan<byte> key)
    {
        long id = 0;

        for (int i = PartitionIdWidth + 1; i < key.Length; i++)
            id = (id * 10) + (key[i] - (byte)'0');

        return id;
    }

    /// <summary>Width of the partition prefix of a log key: the padded partition id plus separator.</summary>
    private const int PartitionPrefixWidth = PartitionIdWidth + 1;

    /// <summary>
    /// Fills <paramref name="prefix"/> (exactly <see cref="PartitionPrefixWidth"/> bytes) with the
    /// partition's log-key prefix. Per-entry scan loops build this once before the loop and compare
    /// with <c>StartsWith</c> directly — the prefix is loop-invariant, and rebuilding it per scanned
    /// entry (as <see cref="KeyBelongsToPartition"/> does) costs a 10-byte fill + digit loop each time.
    /// </summary>
    private static void BuildPartitionPrefix(Span<byte> prefix, int partitionId)
    {
        ToDecimalBytes(prefix[..PartitionIdWidth], partitionId);
        prefix[PartitionIdWidth] = LogKeySeparator;
    }

    private static bool KeyBelongsToPartition(ReadOnlySpan<byte> key, int partitionId)
    {
        Span<byte> prefix = stackalloc byte[PartitionPrefixWidth];
        BuildPartitionPrefix(prefix, partitionId);

        return key.StartsWith(prefix);
    }

    private static void SeekToLastPartitionKey(Iterator iterator, int partitionId)
    {
        Span<byte> upperBoundKey = stackalloc byte[PartitionIdWidth + 1];
        BuildPartitionUpperBoundKey(upperBoundKey, partitionId);

        iterator.Seek(upperBoundKey);

        if (iterator.Valid())
            iterator.Prev();
        else
            iterator.SeekToLast();
    }

    /// <summary>
    /// Converts the specified long value into its decimal representation as a sequence of ASCII bytes
    /// and stores it into the provided span buffer. The resulting buffer is left-padded with '0'
    /// characters to reach a fixed width.
    ///
    /// This ensures the logs will be written and ordered in lexicographical order.
    /// </summary>
    /// <param name="result">
    /// The span of bytes where the decimal ASCII representation will be stored. The span must
    /// have sufficient space to accommodate the fixed width.
    /// </param>
    /// <param name="value">
    /// The long value to be converted into its decimal ASCII byte representation.
    /// </param>
    private static void ToDecimalBytes(Span<byte> result, long value)
    {
        // 1) Pre‑fill with ASCII '0'
        for (int i = 0; i < result.Length; i++)
            result[i] = (byte)'0';

        // 2) Write digits right‑to‑left
        int pos = result.Length - 1;
        do
        {
            result[pos--] = (byte)('0' + (value % 10));
            value /= 10;
        }
        while (value > 0);        
    }

    public void Dispose()
    {
        GC.SuppressFinalize(this);

        engineGuard.EnterWriteLock();
        try
        {
            if (!engineClosed)
            {
                engineClosed = true;
                db.Dispose();
            }
        }
        finally
        {
            engineGuard.ExitWriteLock();
        }

        writeGuard.Dispose();
        engineGuard.Dispose();
    }
}
