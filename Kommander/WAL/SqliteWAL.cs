
using System.Collections.Concurrent;
using Kommander.Data;
using Kommander.Logging;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging;

namespace Kommander.WAL;

/// <summary>
/// SQLite-backed Write-Ahead Log implementation using a fixed-size shard pool.
///
/// <para><b>Shard pool layout.</b>  Instead of one database file per partition
/// (<c>raft{partitionId}_{revision}.db</c>), this implementation distributes
/// partitions across a fixed number of shard databases
/// (<c>raft_shard{shardId}_{revision}.db</c>) by mapping
/// <c>shardId = partitionId mod shardCount</c>.  All partitions in a shard share
/// one SQLite connection and are written inside a single transaction per
/// <see cref="Write"/> call, giving one fsync per shard rather than one per
/// partition.  Combined with the cross-partition group-commit in
/// <see cref="WAL.IO.FairWalScheduler"/>, a batch of P partitions across S shards
/// costs S fsyncs regardless of P.</para>
///
/// <para><b>Shard count is immutable per data directory.</b>  The first time a
/// directory is initialised the resolved count is persisted in the metadata DB
/// (<c>shard_count</c> key) and used verbatim on all subsequent opens.  Changing
/// the count for an existing directory would remap <c>partitionId → shardId</c>
/// and orphan previously-written logs; the constructor fails fast if a non-zero
/// configured value differs from the persisted one.</para>
///
/// <para><b>Concurrency.</b>  All operations on a shard (reads, writes, compaction)
/// serialize through the shard's <c>Lock</c>.  Partitions on different shards can
/// run concurrently; partitions on the same shard serialize — the deliberate
/// concurrency/amortization trade.</para>
/// </summary>
public class SqliteWAL : IWAL, IDisposable
{
    /// <summary>
    /// Serializes creation of new shard connections. Only held while a connection for a
    /// previously-unseen shard is being opened; never held during normal read/write operations.
    /// </summary>
    private readonly SemaphoreSlim semaphore = new(1, 1);

    /// <summary>
    /// Serializes access to the metadata connection and its database operations.
    /// Separate from <see cref="semaphore"/> so that metadata and shard connection creation
    /// do not contend with each other.
    /// </summary>
    private readonly object _metaDataLock = new();

    /// <summary>
    /// Per-shard SQLite state: exclusive lock, open connection, and a set of lazily-created
    /// prepared commands reused across operations on that connection.
    ///
    /// <para>Microsoft.Data.Sqlite keeps no cross-command statement cache — a fresh
    /// <see cref="SqliteCommand"/> re-compiles its SQL (<c>sqlite3_prepare_v2</c>) on first execute
    /// and finalizes it on dispose. Caching one prepared command per hot query removes that
    /// per-call compile. Every field here is only ever touched while the owning shard's
    /// <see cref="Lock"/> is held, so a single reused instance is safe — the same invariant that
    /// makes <see cref="PreparedUpsert"/> safe.</para>
    /// </summary>
    private sealed class ShardDatabase
    {
        public object Lock { get; } = new();
        public SqliteConnection Connection { get; }
        public SqliteCommand? PreparedUpsert { get; set; }

        // Prepared read commands for the hot replication/heartbeat lookups. Each is created on
        // first use and reused thereafter; all executions serialize on <see cref="Lock"/>.
        public SqliteCommand? PreparedGetTermAt { get; set; }
        public SqliteCommand? PreparedGetCurrentTerm { get; set; }
        public SqliteCommand? PreparedGetMaxLog { get; set; }
        public SqliteCommand? PreparedGetLastCheckpoint { get; set; }
        public SqliteCommand? PreparedReadLogsRangeLimited { get; set; }
        public SqliteCommand? PreparedReadLogsRangeUnlimited { get; set; }

        public ShardDatabase(SqliteConnection connection) => Connection = connection;
    }

    /// <summary>
    /// Maps shard IDs to their per-shard database state.
    /// All operations on a shard — reads and writes — serialize through
    /// <see cref="ShardDatabase.Lock"/> because <see cref="SqliteConnection"/> wraps a
    /// single <c>sqlite3*</c> handle that is not safe for concurrent command execution.
    /// </summary>
    private readonly ConcurrentDictionary<int, ShardDatabase> shards = new();

    private readonly string path;
    private readonly string revision;

    /// <summary>
    /// Connection to the metadata database. Initialised lazily under <see cref="_metaDataLock"/>.
    /// Marked <c>volatile</c> so the null check in the double-check locking fast path observes
    /// the store from any thread without an explicit memory barrier.
    /// </summary>
    private volatile SqliteConnection? metaDataConnection;

    private readonly ILogger<IRaft> logger;
    private readonly bool syncWrites;

    /// <summary>
    /// Number of shard databases across which partitions are distributed.
    /// Immutable for the lifetime of this instance (and for the data directory's lifetime).
    /// </summary>
    private readonly int shardCount;

    internal bool SyncWritesEnabled => syncWrites;

    /// <summary>For tests: number of shard transactions committed by the last <see cref="Write"/> call.</summary>
    internal int LastWriteTransactionCount { get; private set; }

    /// <summary>For tests: number of storage commits issued by the last compaction call.</summary>
    internal int LastCompactionCommitCount { get; private set; }

    // ── Construction ──────────────────────────────────────────────────────────

    /// <summary>
    /// Constructs a <see cref="SqliteWAL"/> backed by a shard pool rooted at <paramref name="path"/>.
    /// </summary>
    /// <param name="path">Directory that holds all shard and metadata database files.</param>
    /// <param name="revision">
    /// Revision token appended to file names so multiple independent WAL instances can share
    /// the same directory without conflicting.
    /// </param>
    /// <param name="logger">Logger for diagnostics and slow-write warnings.</param>
    /// <param name="syncWrites">
    /// When <see langword="true"/> (default), shard databases use <c>PRAGMA synchronous=FULL</c>
    /// so every committed transaction is durable across power loss. When <see langword="false"/>,
    /// uses <c>synchronous=NORMAL</c> in WAL journal mode: crash-safe but the last in-flight
    /// transaction may be lost on power loss.
    /// </param>
    /// <param name="shardCount">
    /// Desired number of shard databases.  This value is used <b>only</b> to seed a fresh data
    /// directory; once a directory has been initialised the persisted shard count is authoritative.
    /// <list type="bullet">
    ///   <item><c>0</c> (default) — use <see cref="Environment.ProcessorCount"/> for a new
    ///       directory, or accept the persisted value when reopening.</item>
    ///   <item>A positive value seeds a new directory and validates against the persisted value on
    ///       reopen (throws if they differ, preventing silent log orphaning).</item>
    /// </list>
    /// </param>
    /// <exception cref="InvalidOperationException">
    /// Thrown when <paramref name="shardCount"/> is non-zero and differs from the value already
    /// persisted in the metadata DB for <paramref name="path"/>.
    /// </exception>
    public SqliteWAL(string path, string revision, ILogger<IRaft> logger, bool syncWrites = true, int shardCount = 0)
    {
        this.path = path;
        this.revision = revision;
        this.logger = logger;
        this.syncWrites = syncWrites;
        this.shardCount = ResolveShardCount(shardCount);
    }

    // ── Shard routing ─────────────────────────────────────────────────────────

    /// <summary>Maps a partition to its shard using a stable modulus.</summary>
    private int ShardOf(int partitionId) => (int)((uint)partitionId % (uint)shardCount);

    /// <summary>
    /// Returns the database state for <paramref name="shardId"/>, creating and initialising
    /// the shard file on first access.
    /// </summary>
    private ShardDatabase TryOpenShard(int shardId)
    {
        if (shards.TryGetValue(shardId, out ShardDatabase? shard))
            return shard;

        semaphore.Wait();
        try
        {
            if (shards.TryGetValue(shardId, out shard))
                return shard;

            string completePath = $"{path}/raft_shard{shardId}_{revision}.db";
            // Pooling=False: ensure Dispose() physically closes the file handle instead of
            // returning it to the ADO.NET pool.  SqliteWAL manages connection lifetime
            // explicitly (shards dict), so pooling only causes FD leaks on dispose.
            SqliteConnection connection = new($"Data Source={completePath};Pooling=False");
            connection.Open();

            const string createTableQuery = """
            CREATE TABLE IF NOT EXISTS logs (
                id INT,
                partitionId INT,
                term INT,
                type INT,
                logType STRING,
                log BLOB,
                timeNode INT,
                timePhysical INT,
                timeCounter INT,
                PRIMARY KEY(partitionId, id)
            );
            """;
            using SqliteCommand createCmd = new(createTableQuery, connection);
            createCmd.ExecuteNonQuery();

            // Per-partition last-committed-checkpoint id, co-located in the SAME shard DB as `logs` so it
            // can be updated inside the same transaction as a log mutation (true atomicity). This replaces
            // the old `SELECT MAX(id) ... WHERE type=checkpoint` scan with an O(1) point lookup. Absence of
            // a row means "no checkpoint yet" → GetLastCheckpoint returns -1.
            const string createCheckpointsQuery = """
            CREATE TABLE IF NOT EXISTS checkpoints (
                partitionId INT PRIMARY KEY,
                lastCheckpoint INT NOT NULL
            );
            """;
            using SqliteCommand createCheckpointsCmd = new(createCheckpointsQuery, connection);
            createCheckpointsCmd.ExecuteNonQuery();

            string synchronousMode = syncWrites ? "FULL" : "NORMAL";
            string pragmas = $"PRAGMA journal_mode=WAL; PRAGMA synchronous={synchronousMode}; PRAGMA temp_store=MEMORY;";
            using SqliteCommand pragmaCmd = new(pragmas, connection);
            pragmaCmd.ExecuteNonQuery();

            shard = new(connection);
            shards.TryAdd(shardId, shard);
            return shard;
        }
        finally
        {
            semaphore.Release();
        }
    }

    /// <summary>
    /// Resolves the effective shard count for this data directory using a seed-once-then-pinned
    /// strategy: the configured count is persisted on first open and enforced on all subsequent opens.
    ///
    /// <list type="number">
    ///   <item>If the metadata DB already holds a <c>shard_count</c> value, that value is used.
    ///       A non-zero <paramref name="configured"/> value that differs from the persisted value
    ///       causes an <see cref="InvalidOperationException"/> (changing it would orphan logs).</item>
    ///   <item>If the directory is fresh (no persisted value), the configured value is used
    ///       (<c>0</c> resolves to <see cref="Environment.ProcessorCount"/>) and immediately
    ///       persisted so subsequent opens agree.</item>
    /// </list>
    /// </summary>
    private int ResolveShardCount(int configured)
    {
        string? persisted = GetMetaData("shard_count");

        if (persisted is not null)
        {
            if (!int.TryParse(persisted, out int persistedCount) || persistedCount < 1)
                throw new InvalidOperationException(
                    $"SqliteWAL: persisted shard_count '{persisted}' in '{path}' is not a valid positive integer.");

            if (configured != 0 && configured != persistedCount)
                throw new InvalidOperationException(
                    $"SqliteWAL: configured shardCount ({configured}) differs from the persisted " +
                    $"shard_count ({persistedCount}) for path '{path}'. Changing shard_count for an " +
                    "existing data directory would orphan previously-written logs. " +
                    "Use shardCount=0 to accept the persisted value.");

            return persistedCount;
        }

        // Fresh directory — resolve and pin.
        int resolved = configured <= 0 ? Environment.ProcessorCount : configured;
        SetMetaData("shard_count", resolved.ToString());
        return resolved;
    }

    // ── Metadata DB ───────────────────────────────────────────────────────────

    /// <summary>
    /// Returns the metadata <see cref="SqliteConnection"/>, creating it on first access.
    /// Must be called with <see cref="_metaDataLock"/> already held.
    /// </summary>
    private SqliteConnection TryOpenMetaDataDatabase()
    {
        if (metaDataConnection is not null)
            return metaDataConnection;

        string completePath = $"{path}/raft_metadata_{revision}.db";
        // Pooling=False: same as the shard connections — physical close on Dispose.
        SqliteConnection connection = new($"Data Source={completePath};Pooling=False");
        connection.Open();

        const string createTableQuery = """
        CREATE TABLE IF NOT EXISTS metadata (
            key STRING PRIMARY KEY,
            value STRING
        );
        """;
        using SqliteCommand command1 = new(createTableQuery, connection);
        command1.ExecuteNonQuery();

        const string pragmasQuery = "PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;";
        using SqliteCommand command3 = new(pragmasQuery, connection);
        command3.ExecuteNonQuery();

        metaDataConnection = connection;
        return connection;
    }

    // ── IWAL — reads ─────────────────────────────────────────────────────────

    /// <summary>
    /// Reads logs from the specified partition starting from the last checkpoint.
    /// </summary>
    public List<RaftLog> ReadLogs(int partitionId)
    {
        ShardDatabase shard = TryOpenShard(ShardOf(partitionId));

        lock (shard.Lock)
        {
            List<RaftLog> result = [];
            long lastCheckpoint = GetLastCheckpointInternal(shard, partitionId);

            const string query = """
             SELECT id, term, type, logType, log, timeNode, timePhysical, timeCounter
             FROM logs
             WHERE partitionId = @partitionId AND id >= @lastCheckpoint
             ORDER BY id ASC;
             """;

            using SqliteCommand command = new(query, shard.Connection);
            command.Parameters.AddWithValue("@partitionId", partitionId);
            command.Parameters.AddWithValue("@lastCheckpoint", lastCheckpoint);

            using SqliteDataReader reader = command.ExecuteReader();
            while (reader.Read())
                result.Add(ReadLogRow(reader));

            return result;
        }
    }

    /// <summary>
    /// Reads up to <paramref name="maxEntries"/> Raft logs for <paramref name="partitionId"/>
    /// with id ≥ <paramref name="startLogIndex"/>, sorted ascending. When
    /// <paramref name="maxEntries"/> is not <see cref="int.MaxValue"/>, the limit is pushed
    /// into SQL so the engine can stop at the boundary instead of scanning the full tail.
    /// </summary>
    public List<RaftLog> ReadLogsRange(int partitionId, long startLogIndex, int maxEntries = int.MaxValue)
    {
        ShardDatabase shard = TryOpenShard(ShardOf(partitionId));

        lock (shard.Lock)
        {
            List<RaftLog> result = [];

            bool applyLimit = maxEntries != int.MaxValue;
            SqliteCommand command = applyLimit
                ? GetOrCreateReadLogsRangeLimited(shard)
                : GetOrCreateReadLogsRangeUnlimited(shard);

            command.Parameters["@partitionId"].Value = partitionId;
            command.Parameters["@startIndex"].Value = startLogIndex;
            if (applyLimit)
                command.Parameters["@maxEntries"].Value = maxEntries;

            using SqliteDataReader reader = command.ExecuteReader();
            while (reader.Read())
                result.Add(ReadLogRow(reader));

            return result;
        }
    }

    /// <summary>
    /// Retrieves the highest log identifier from the logs for a specific partition.
    /// Returns 0 if no logs are found or an error occurs.
    /// </summary>
    public long GetMaxLog(int partitionId)
    {
        try
        {
            ShardDatabase shard = TryOpenShard(ShardOf(partitionId));
            lock (shard.Lock)
            {
                SqliteCommand command = GetOrCreateGetMaxLog(shard);
                command.Parameters["@partitionId"].Value = partitionId;
                using SqliteDataReader reader = command.ExecuteReader();
                while (reader.Read())
                    return reader.IsDBNull(0) ? 0 : reader.GetInt64(0);
                return 0;
            }
        }
        catch (Exception ex)
        {
            logger.LogError("Error during GetMaxLog: {Message}\n{StackTrace}", ex.Message, ex.StackTrace);
            return 0;
        }
    }

    /// <summary>
    /// Retrieves the current term of the Raft log for the specified partition.
    /// Returns the term of the log entry with the highest id. Returns 0 if no logs exist.
    /// </summary>
    /// <summary>
    /// Point lookup of a single entry's term by exact id. Selects only the <c>term</c> column so the
    /// engine never reads the payload/metadata columns that <see cref="ReadLogsRange"/> materializes.
    /// Returns -1 when no row with that id exists (0 for the degenerate case of a persisted NULL term,
    /// matching <see cref="GetCurrentTerm"/>).
    /// </summary>
    public long GetTermAt(int partitionId, long logIndex)
    {
        ShardDatabase shard = TryOpenShard(ShardOf(partitionId));
        lock (shard.Lock)
        {
            SqliteCommand command = GetOrCreateGetTermAt(shard);
            command.Parameters["@partitionId"].Value = partitionId;
            command.Parameters["@id"].Value = logIndex;
            using SqliteDataReader reader = command.ExecuteReader();
            while (reader.Read())
                return reader.IsDBNull(0) ? 0 : reader.GetInt64(0);
            return -1;
        }
    }

    public long GetCurrentTerm(int partitionId)
    {
        ShardDatabase shard = TryOpenShard(ShardOf(partitionId));
        lock (shard.Lock)
        {
            SqliteCommand command = GetOrCreateGetCurrentTerm(shard);
            command.Parameters["@partitionId"].Value = partitionId;
            using SqliteDataReader reader = command.ExecuteReader();
            while (reader.Read())
                return reader.IsDBNull(0) ? 0 : reader.GetInt64(0);
            return 0;
        }
    }

    /// <summary>
    /// Retrieves the last recorded checkpoint log index for the specified partition.
    /// Returns -1 if no committed checkpoint exists.
    /// </summary>
    public long GetLastCheckpoint(int partitionId)
    {
        ShardDatabase shard = TryOpenShard(ShardOf(partitionId));
        lock (shard.Lock)
            return GetLastCheckpointInternal(shard, partitionId);
    }

    /// <inheritdoc/>
    public int CountPersistedLogs(int partitionId)
    {
        ShardDatabase shard = TryOpenShard(ShardOf(partitionId));
        lock (shard.Lock)
        {
            const string query = "SELECT COUNT(*) FROM logs WHERE partitionId = @partitionId";
            using SqliteCommand command = new(query, shard.Connection);
            command.Parameters.AddWithValue("@partitionId", partitionId);
            return Convert.ToInt32(command.ExecuteScalar());
        }
    }

    /// <inheritdoc/>
    public int CountRemovableLogs(int partitionId)
    {
        ShardDatabase shard = TryOpenShard(ShardOf(partitionId));
        lock (shard.Lock)
        {
            long lastCheckpoint = GetLastCheckpointInternal(shard, partitionId);
            if (lastCheckpoint <= 0)
                return 0;

            const string query = """
             SELECT COUNT(*)
             FROM logs
             WHERE partitionId = @partitionId AND id < @lastCheckpoint;
             """;
            using SqliteCommand command = new(query, shard.Connection);
            command.Parameters.AddWithValue("@partitionId", partitionId);
            command.Parameters.AddWithValue("@lastCheckpoint", lastCheckpoint);
            return Convert.ToInt32(command.ExecuteScalar());
        }
    }

    // ── IWAL — write ─────────────────────────────────────────────────────────

    /// <summary>
    /// Writes a collection of logs, grouping them by shard to issue one SQLite transaction
    /// (one fsync) per shard rather than one per partition.
    ///
    /// <para>A batch of P partitions spanning S shards costs S fsyncs. When shardCount=1 all
    /// partitions are co-located and the entire batch costs a single fsync.</para>
    /// </summary>
    public RaftOperationStatus Write(List<(int, List<RaftLog>)> logs) => Write(logs, sync: true);

    /// <inheritdoc/>
    /// <remarks>
    /// When <paramref name="sync"/> is <see langword="false"/> and this instance is durable
    /// (<c>syncWrites=true</c>, i.e. <c>PRAGMA synchronous=FULL</c>), the shard connection is switched to
    /// <c>PRAGMA synchronous=OFF</c> for the duration of the transaction and restored to <c>FULL</c>
    /// afterward, all under the shard's write lock. In WAL journal mode this skips the per-commit fsync of
    /// the <c>-wal</c> file; the next <c>FULL</c> commit fsyncs the <c>-wal</c> and so flushes the prior
    /// sync-off frames, making them durable. The pragma is toggled <b>outside</b> the transaction (SQLite
    /// requires it) and only while the exclusive shard lock is held, so no concurrent write on the shard
    /// observes the lowered durability; reads are unaffected by the synchronous mode.
    /// </remarks>
    public RaftOperationStatus Write(List<(int, List<RaftLog>)> logs, bool sync)
    {
        bool downgradeSync = !sync && syncWrites;

        // Group by shard, then merge same-partition entries within each shard.
        Dictionary<int, Dictionary<int, List<RaftLog>>> shardPlan = new();

        foreach ((int partitionId, List<RaftLog> raftLogs) in logs)
        {
            int shardId = ShardOf(partitionId);

            if (!shardPlan.TryGetValue(shardId, out Dictionary<int, List<RaftLog>>? partitionPlan))
            {
                partitionPlan = new();
                shardPlan[shardId] = partitionPlan;
            }

            if (partitionPlan.TryGetValue(partitionId, out List<RaftLog>? existing))
                existing.AddRange(raftLogs);
            else
            {
                List<RaftLog> copy = new(raftLogs.Count);
                copy.AddRange(raftLogs);
                partitionPlan[partitionId] = copy;
            }
        }

        try
        {
            foreach (KeyValuePair<int, Dictionary<int, List<RaftLog>>> shardEntry in shardPlan)
            {
                ShardDatabase shard = TryOpenShard(shardEntry.Key);

                lock (shard.Lock)
                {
                    if (downgradeSync)
                        SetSynchronousPragma(shard.Connection, "OFF");

                    try
                    {
                        using SqliteTransaction transaction = shard.Connection.BeginTransaction();
                        SqliteCommand upsert = GetOrCreatePreparedUpsert(shard);
                        upsert.Transaction = transaction;

                        try
                        {
                            foreach (KeyValuePair<int, List<RaftLog>> kv in shardEntry.Value)
                            {
                                int partitionId = kv.Key;
                                long batchMaxCheckpoint = -1;
                                foreach (RaftLog log in kv.Value)
                                {
                                    BindAndExecUpsert(upsert, partitionId, log);
                                    if (log.Type == RaftLogType.CommittedCheckpoint && log.Id > batchMaxCheckpoint)
                                        batchMaxCheckpoint = log.Id;
                                }

                                // Persist the last-checkpoint id in the SAME transaction as the log rows so
                                // it is durable atomically. max() with the existing value so an out-of-order
                                // lower checkpoint cannot regress the recorded id.
                                if (batchMaxCheckpoint >= 0)
                                {
                                    long newCheckpoint = Math.Max(
                                        ReadCheckpointInTransaction(shard.Connection, transaction, partitionId),
                                        batchMaxCheckpoint);
                                    UpsertCheckpointInTransaction(shard.Connection, transaction, partitionId, newCheckpoint);
                                }
                            }

                            transaction.Commit();
                        }
                        catch
                        {
                            transaction.Rollback();
                            throw;
                        }
                        finally
                        {
                            upsert.Transaction = null;
                        }
                    }
                    finally
                    {
                        if (downgradeSync)
                            SetSynchronousPragma(shard.Connection, "FULL");
                    }
                }
            }

            LastWriteTransactionCount = shardPlan.Count;
        }
        catch (Exception ex)
        {
            logger.LogError("Error during write: {Message}\n{StackTrace}", ex.Message, ex.StackTrace);
            return RaftOperationStatus.Errored;
        }

        return RaftOperationStatus.Success;
    }

    /// <summary>
    /// Sets <c>PRAGMA synchronous</c> on <paramref name="connection"/> to <paramref name="mode"/>
    /// (e.g. <c>OFF</c> / <c>FULL</c>). Must be called outside a transaction and while holding the
    /// shard's write lock. Used by the sync-off branch of <see cref="Write(List{ValueTuple{int, List{RaftLog}}}, bool)"/>
    /// to skip the per-commit fsync for lazy commit-marker writes.
    /// </summary>
    private static void SetSynchronousPragma(SqliteConnection connection, string mode)
    {
        using SqliteCommand pragma = new($"PRAGMA synchronous={mode};", connection);
        pragma.ExecuteNonQuery();
    }

    // ── IWAL — partition lifecycle ────────────────────────────────────────────

    /// <summary>
    /// Deletes all logs for <paramref name="partitionId"/> from its shard database.
    ///
    /// <para>The shard connection is <b>not</b> closed after the delete because it is
    /// shared by other partitions on the same shard. The row-level <c>WHERE partitionId</c>
    /// delete ensures sibling partitions are unaffected.</para>
    /// </summary>
    public RaftOperationStatus DeletePartitionWAL(int partitionId)
    {
        int shardId = ShardOf(partitionId);

        // Fast path: shard file does not exist and no connection is open — nothing to do.
        if (!shards.TryGetValue(shardId, out ShardDatabase? shard))
        {
            string completePath = $"{path}/raft_shard{shardId}_{revision}.db";
            if (!File.Exists(completePath))
                return RaftOperationStatus.Success;

            // File exists but connection not yet open — open it to perform the delete.
            shard = TryOpenShard(shardId);
        }

        lock (shard.Lock)
        {
            try
            {
                // Drop the logs and the persisted last-checkpoint row atomically. Wiping the partition must
                // not leave a stale replay floor that a subsequently-reused partition id would inherit
                // (there is no scan fallback to correct it).
                using SqliteTransaction transaction = shard.Connection.BeginTransaction();
                try
                {
                    using (SqliteCommand command = new(
                        "DELETE FROM logs WHERE partitionId = @partitionId", shard.Connection))
                    {
                        command.Transaction = transaction;
                        command.Parameters.AddWithValue("@partitionId", partitionId);
                        command.ExecuteNonQuery();
                    }

                    DeleteCheckpointInTransaction(shard.Connection, transaction, partitionId);

                    transaction.Commit();
                }
                catch
                {
                    transaction.Rollback();
                    throw;
                }
            }
            catch (Exception ex)
            {
                logger.LogError("Error during DeletePartitionWAL({PartitionId}): {Message}", partitionId, ex.Message);
                return RaftOperationStatus.Errored;
            }

            // Do NOT close or evict the shard connection — other partitions on this shard
            // are still live and must remain accessible.
            return RaftOperationStatus.Success;
        }
    }

    /// <inheritdoc/>
    public RaftOperationStatus TruncateLogsAfter(int partitionId, long afterLogId)
    {
        ShardDatabase shard = TryOpenShard(ShardOf(partitionId));
        lock (shard.Lock)
        {
            try
            {
                // Delete + checkpoint adjustment in one transaction so the persisted last-checkpoint can
                // never disagree with the log after a crash.
                using SqliteTransaction transaction = shard.Connection.BeginTransaction();
                try
                {
                    using (SqliteCommand command = new(
                        "DELETE FROM logs WHERE partitionId = @partitionId AND id > @afterLogId",
                        shard.Connection))
                    {
                        command.Transaction = transaction;
                        command.Parameters.AddWithValue("@partitionId", partitionId);
                        command.Parameters.AddWithValue("@afterLogId", afterLogId);
                        command.ExecuteNonQuery();
                    }

                    AdjustCheckpointAfterTruncation(shard.Connection, transaction, partitionId, afterLogId);

                    transaction.Commit();
                    return RaftOperationStatus.Success;
                }
                catch
                {
                    transaction.Rollback();
                    throw;
                }
            }
            catch (Exception ex)
            {
                logger.LogError("TruncateLogsAfter({PartitionId}, {AfterLogId}): {Message}", partitionId, afterLogId, ex.Message);
                return RaftOperationStatus.Errored;
            }
        }
    }

    /// <summary>
    /// If the recorded last-checkpoint sits above <paramref name="afterLogId"/> (i.e. a truncation just
    /// removed it), recompute the surviving checkpoint (highest <see cref="RaftLogType.CommittedCheckpoint"/>
    /// with id ≤ <paramref name="afterLogId"/>, or none → delete the row) inside the same transaction.
    /// No-op on the overwhelmingly common case where the checkpoint is at or below the truncation point.
    /// </summary>
    private static void AdjustCheckpointAfterTruncation(
        SqliteConnection connection, SqliteTransaction transaction, int partitionId, long afterLogId)
    {
        long recorded = ReadCheckpointInTransaction(connection, transaction, partitionId);
        if (recorded <= afterLogId)
            return;

        long surviving = HighestCheckpointAtMostInTransaction(connection, transaction, partitionId, afterLogId);
        if (surviving < 0)
            DeleteCheckpointInTransaction(connection, transaction, partitionId);
        else
            UpsertCheckpointInTransaction(connection, transaction, partitionId, surviving);
    }

    /// <inheritdoc/>
    public RaftOperationStatus TruncateProposedLogsAfter(int partitionId, long afterLogId)
    {
        ShardDatabase shard = TryOpenShard(ShardOf(partitionId));
        lock (shard.Lock)
        {
            try
            {
                // Only unresolved (Proposed / ProposedCheckpoint) entries above the anchor are removable;
                // resolved entries are quorum-agreed and load-bearing for the commit frontier.
                using SqliteCommand command = new(
                    "DELETE FROM logs WHERE partitionId = @partitionId AND id > @afterLogId AND type IN (@proposed, @proposedCheckpoint)",
                    shard.Connection);
                command.Parameters.AddWithValue("@partitionId", partitionId);
                command.Parameters.AddWithValue("@afterLogId", afterLogId);
                command.Parameters.AddWithValue("@proposed", (int)RaftLogType.Proposed);
                command.Parameters.AddWithValue("@proposedCheckpoint", (int)RaftLogType.ProposedCheckpoint);
                command.ExecuteNonQuery();

                // No last-checkpoint adjustment: this only removes unresolved (Proposed / ProposedCheckpoint)
                // entries, and a CommittedCheckpoint is resolved — so the recorded checkpoint is never removed.
                return RaftOperationStatus.Success;
            }
            catch (Exception ex)
            {
                logger.LogError("TruncateProposedLogsAfter({PartitionId}, {AfterLogId}): {Message}", partitionId, afterLogId, ex.Message);
                return RaftOperationStatus.Errored;
            }
        }
    }

    /// <inheritdoc/>
    public (RaftOperationStatus Status, long MaxLogId) TruncateLogsAfterAndGetMax(int partitionId, long afterLogId)
    {
        ShardDatabase shard = TryOpenShard(ShardOf(partitionId));
        // Delete and read-max under one shard-lock acquisition so the pair is atomic against the
        // WAL-scheduler write path, which serializes on the same shard.Lock.
        lock (shard.Lock)
        {
            try
            {
                using SqliteTransaction transaction = shard.Connection.BeginTransaction();
                try
                {
                    using (SqliteCommand delete = new(
                        "DELETE FROM logs WHERE partitionId = @partitionId AND id > @afterLogId",
                        shard.Connection))
                    {
                        delete.Transaction = transaction;
                        delete.Parameters.AddWithValue("@partitionId", partitionId);
                        delete.Parameters.AddWithValue("@afterLogId", afterLogId);
                        delete.ExecuteNonQuery();
                    }

                    AdjustCheckpointAfterTruncation(shard.Connection, transaction, partitionId, afterLogId);

                    long maxLogId;
                    using (SqliteCommand max = new(
                        "SELECT MAX(id) AS max FROM logs WHERE partitionId = @partitionId",
                        shard.Connection))
                    {
                        max.Transaction = transaction;
                        max.Parameters.AddWithValue("@partitionId", partitionId);
                        object? result = max.ExecuteScalar();
                        maxLogId = result is null or DBNull ? 0 : Convert.ToInt64(result);
                    }

                    transaction.Commit();
                    return (RaftOperationStatus.Success, maxLogId);
                }
                catch
                {
                    transaction.Rollback();
                    throw;
                }
            }
            catch (Exception ex)
            {
                logger.LogError("TruncateLogsAfterAndGetMax({PartitionId}, {AfterLogId}): {Message}", partitionId, afterLogId, ex.Message);
                return (RaftOperationStatus.Errored, 0);
            }
        }
    }

    /// <inheritdoc/>
    /// <remarks>
    /// The term probe, suffix delete, and checkpoint upsert run inside one transaction under the shard's
    /// write lock, so the boundary is installed atomically against the WAL write path (which serializes on
    /// the same <c>shard.Lock</c>). <paramref name="sync"/> honours the same pragma-downgrade dance as
    /// <see cref="Write(List{ValueTuple{int, List{RaftLog}}}, bool)"/>: when the instance is durable and
    /// the caller opts out of fsync, <c>PRAGMA synchronous</c> is toggled OFF/FULL outside the transaction
    /// under the lock. All statements scope by <c>partitionId</c> so co-resident partitions are unaffected.
    /// </remarks>
    public (RaftOperationStatus Status, bool SuffixTruncated) InstallSnapshotBoundary(
        int partitionId, long snapshotIndex, long lastIncludedTerm, bool sync)
    {
        bool downgradeSync = !sync && syncWrites;
        ShardDatabase shard = TryOpenShard(ShardOf(partitionId));

        lock (shard.Lock)
        {
            if (downgradeSync)
                SetSynchronousPragma(shard.Connection, "OFF");

            try
            {
                using SqliteTransaction transaction = shard.Connection.BeginTransaction();

                try
                {
                    // Probe the stored term at the boundary index (no row → -1, NULL term → 0).
                    long localTerm;
                    using (SqliteCommand probe = new(
                        "SELECT term FROM logs WHERE partitionId = @partitionId AND id = @id LIMIT 1",
                        shard.Connection))
                    {
                        probe.Transaction = transaction;
                        probe.Parameters.AddWithValue("@partitionId", partitionId);
                        probe.Parameters.AddWithValue("@id", snapshotIndex);
                        object? result = probe.ExecuteScalar();
                        localTerm = result is null ? -1 : result is DBNull ? 0 : Convert.ToInt64(result);
                    }

                    bool suffixTruncated = false;
                    if (localTerm != lastIncludedTerm)
                    {
                        using SqliteCommand delete = new(
                            "DELETE FROM logs WHERE partitionId = @partitionId AND id > @snapshotIndex",
                            shard.Connection);
                        delete.Transaction = transaction;
                        delete.Parameters.AddWithValue("@partitionId", partitionId);
                        delete.Parameters.AddWithValue("@snapshotIndex", snapshotIndex);
                        int removed = delete.ExecuteNonQuery();
                        suffixTruncated = removed > 0;
                    }

                    SqliteCommand upsert = GetOrCreatePreparedUpsert(shard);
                    upsert.Transaction = transaction;
                    try
                    {
                        BindAndExecUpsert(upsert, partitionId, new RaftLog
                        {
                            Id = snapshotIndex,
                            Term = lastIncludedTerm,
                            Type = RaftLogType.CommittedCheckpoint,
                        });
                    }
                    finally
                    {
                        upsert.Transaction = null;
                    }

                    // Persist the new last-checkpoint id atomically with the boundary install. When the
                    // suffix was truncated, every entry above snapshotIndex (including any higher checkpoint)
                    // is gone, so the new max is exactly snapshotIndex. When the suffix was retained, a higher
                    // checkpoint may still exist above the boundary, so keep the greater of the two.
                    long newCheckpoint = suffixTruncated
                        ? snapshotIndex
                        : Math.Max(ReadCheckpointInTransaction(shard.Connection, transaction, partitionId), snapshotIndex);
                    UpsertCheckpointInTransaction(shard.Connection, transaction, partitionId, newCheckpoint);

                    transaction.Commit();
                    return (RaftOperationStatus.Success, suffixTruncated);
                }
                catch
                {
                    transaction.Rollback();
                    throw;
                }
            }
            catch (Exception ex)
            {
                logger.LogError("InstallSnapshotBoundary({PartitionId}, {SnapshotIndex}): {Message}",
                    partitionId, snapshotIndex, ex.Message);
                return (RaftOperationStatus.Errored, false);
            }
            finally
            {
                if (downgradeSync)
                    SetSynchronousPragma(shard.Connection, "FULL");
            }
        }
    }

    /// <summary>
    /// Compacts logs older than <paramref name="lastCheckpoint"/> for the given partition,
    /// removing up to <paramref name="compactNumberEntries"/> per internal batch, all within
    /// one SQLite transaction so the entire pass costs a single fsync.
    /// </summary>
    /// <param name="maxTotalEntries">
    /// When set, multiple internal batches of <paramref name="compactNumberEntries"/> are issued
    /// inside one SQLite transaction so a compaction pass costs a single fsync.
    /// </param>
    public (RaftOperationStatus Status, int Removed) CompactLogsOlderThan(
        int partitionId,
        long lastCheckpoint,
        int compactNumberEntries,
        int? maxTotalEntries = null)
    {
        int passCap = maxTotalEntries ?? compactNumberEntries;
        ShardDatabase shard = TryOpenShard(ShardOf(partitionId));

        try
        {
            lock (shard.Lock)
            {
                using SqliteTransaction transaction = shard.Connection.BeginTransaction();

                try
                {
                    // No last-checkpoint update: compaction only removes entries with id < lastCheckpoint, so
                    // the recorded checkpoint id (>= lastCheckpoint) can never be among the deleted rows.
                    const string deleteSql = """
                     DELETE FROM logs
                     WHERE partitionId = @partitionId
                       AND id IN (
                         SELECT id
                         FROM logs
                         WHERE partitionId = @partitionId AND id < @lastCheckpoint
                         ORDER BY id ASC
                         LIMIT @limit
                       );
                     """;

                    using SqliteCommand deleteCommand = new(deleteSql, shard.Connection);
                    deleteCommand.Transaction = transaction;
                    deleteCommand.Parameters.AddWithValue("@partitionId", partitionId);
                    deleteCommand.Parameters.AddWithValue("@lastCheckpoint", lastCheckpoint);
                    SqliteParameter limitParameter = deleteCommand.Parameters.Add("@limit", SqliteType.Integer);

                    int totalRemoved = 0;
                    while (totalRemoved < passCap)
                    {
                        int batchLimit = Math.Min(compactNumberEntries, passCap - totalRemoved);
                        limitParameter.Value = batchLimit;
                        int removed = deleteCommand.ExecuteNonQuery();
                        totalRemoved += removed;
                        if (removed < batchLimit)
                            break;
                    }

                    transaction.Commit();
                    LastCompactionCommitCount = 1;

                    if (totalRemoved > 0)
                        logger.LogDebugRemovedFromWal(totalRemoved, partitionId);

                    return (RaftOperationStatus.Success, totalRemoved);
                }
                catch
                {
                    transaction.Rollback();
                    throw;
                }
            }
        }
        catch (Exception ex)
        {
            logger.LogError("Error during compact: {Message}\n{StackTrace}", ex.Message, ex.StackTrace);
            return (RaftOperationStatus.Errored, 0);
        }
    }

    // ── IWAL — metadata ───────────────────────────────────────────────────────

    /// <summary>
    /// Retrieves a metadata value by key from the metadata database.
    /// Returns <see langword="null"/> if the key does not exist.
    /// </summary>
    public string? GetMetaData(string key)
    {
        lock (_metaDataLock)
        {
            SqliteConnection connection = TryOpenMetaDataDatabase();
            const string query = "SELECT value FROM metadata WHERE key = @key";
            using SqliteCommand command = new(query, connection);
            command.Parameters.AddWithValue("@key", key);
            using SqliteDataReader reader = command.ExecuteReader();
            while (reader.Read())
                return reader.IsDBNull(0) ? null : reader.GetString(0);
            return null;
        }
    }

    /// <summary>
    /// Upserts a metadata key/value pair into the metadata database.
    /// Returns <see langword="true"/> on success.
    /// </summary>
    public bool SetMetaData(string key, string value)
    {
        lock (_metaDataLock)
        {
            SqliteConnection connection = TryOpenMetaDataDatabase();

            const string upsertSql = """
                INSERT INTO metadata (key, value)
                VALUES (@key, @value)
                ON CONFLICT(key) DO UPDATE SET value=@value;
                """;

            using SqliteCommand command = new(upsertSql, connection);
            command.Parameters.AddWithValue("@key", key);
            command.Parameters.AddWithValue("@value", string.IsNullOrEmpty(value) ? "" : value);
            command.ExecuteNonQuery();
            return true;
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /// <summary>
    /// Reads the persisted last-checkpoint id for a partition as an O(1) point lookup in the
    /// <c>checkpoints</c> table (co-located in the shard DB). Returns <c>-1</c> when no row exists
    /// (no checkpoint ever recorded, or the recorded one was truncated away). Must be called while
    /// <see cref="ShardDatabase.Lock"/> is held and with no active transaction on the connection.
    /// The value is kept in sync with every checkpoint mutation inside the mutation's own transaction,
    /// so there is no scan fallback.
    /// </summary>
    private static long GetLastCheckpointInternal(ShardDatabase shard, int partitionId)
    {
        SqliteCommand command = GetOrCreateGetLastCheckpoint(shard);
        command.Parameters["@partitionId"].Value = partitionId;
        using SqliteDataReader reader = command.ExecuteReader();
        while (reader.Read())
            return reader.IsDBNull(0) ? -1 : reader.GetInt64(0);
        return -1;
    }

    /// <summary>
    /// Reads the persisted last-checkpoint id inside an active transaction (a plain command bound to
    /// <paramref name="transaction"/>, since a prepared command with no transaction cannot run while one
    /// is open). Returns <c>-1</c> when no row exists.
    /// </summary>
    private static long ReadCheckpointInTransaction(SqliteConnection connection, SqliteTransaction transaction, int partitionId)
    {
        using SqliteCommand command = new(
            "SELECT lastCheckpoint FROM checkpoints WHERE partitionId = @partitionId", connection);
        command.Transaction = transaction;
        command.Parameters.AddWithValue("@partitionId", partitionId);
        object? result = command.ExecuteScalar();
        return result is null or DBNull ? -1 : Convert.ToInt64(result);
    }

    /// <summary>Upserts the persisted last-checkpoint id for a partition inside <paramref name="transaction"/>.</summary>
    private static void UpsertCheckpointInTransaction(SqliteConnection connection, SqliteTransaction transaction, int partitionId, long value)
    {
        using SqliteCommand command = new(
            """
            INSERT INTO checkpoints (partitionId, lastCheckpoint)
            VALUES (@partitionId, @lastCheckpoint)
            ON CONFLICT(partitionId) DO UPDATE SET lastCheckpoint=@lastCheckpoint;
            """, connection);
        command.Transaction = transaction;
        command.Parameters.AddWithValue("@partitionId", partitionId);
        command.Parameters.AddWithValue("@lastCheckpoint", value);
        command.ExecuteNonQuery();
    }

    /// <summary>
    /// Deletes the persisted last-checkpoint row for a partition inside <paramref name="transaction"/>
    /// (equivalent to "no checkpoint" — a subsequent read returns <c>-1</c>).
    /// </summary>
    private static void DeleteCheckpointInTransaction(SqliteConnection connection, SqliteTransaction transaction, int partitionId)
    {
        using SqliteCommand command = new(
            "DELETE FROM checkpoints WHERE partitionId = @partitionId", connection);
        command.Transaction = transaction;
        command.Parameters.AddWithValue("@partitionId", partitionId);
        command.ExecuteNonQuery();
    }

    /// <summary>
    /// Highest <see cref="RaftLogType.CommittedCheckpoint"/> id whose id is <c>≤ upperIdInclusive</c>, or
    /// <c>-1</c> if none, read inside <paramref name="transaction"/>. Used ONLY on the rare truncation
    /// path that removes the recorded checkpoint, to recompute the surviving one — never on the read hot
    /// path (the whole point of the persisted value).
    /// </summary>
    private static long HighestCheckpointAtMostInTransaction(
        SqliteConnection connection, SqliteTransaction transaction, int partitionId, long upperIdInclusive)
    {
        using SqliteCommand command = new(
            "SELECT MAX(id) FROM logs WHERE partitionId = @partitionId AND type = @type AND id <= @upper",
            connection);
        command.Transaction = transaction;
        command.Parameters.AddWithValue("@partitionId", partitionId);
        command.Parameters.AddWithValue("@type", (int)RaftLogType.CommittedCheckpoint);
        command.Parameters.AddWithValue("@upper", upperIdInclusive);
        object? result = command.ExecuteScalar();
        return result is null or DBNull ? -1 : Convert.ToInt64(result);
    }

    private static RaftLog ReadLogRow(SqliteDataReader reader) => new()
    {
        Id       = reader.IsDBNull(0) ? 0 : reader.GetInt64(0),
        Term     = reader.IsDBNull(1) ? 0 : reader.GetInt64(1),
        Type     = reader.IsDBNull(2) ? RaftLogType.Proposed : (RaftLogType)reader.GetInt32(2),
        LogType  = reader.IsDBNull(3) ? "" : reader.GetString(3),
        LogData  = reader.IsDBNull(4) ? null : (byte[])reader[4],
        Time     = new(
            reader.IsDBNull(5) ? 0 : reader.GetInt32(5),
            reader.IsDBNull(6) ? 0 : reader.GetInt64(6),
            reader.IsDBNull(7) ? 0 : (uint)reader.GetInt64(7)
        )
    };

    private static SqliteCommand CreatePreparedUpsert(SqliteConnection connection)
    {
        const string sql = """
          INSERT INTO logs (id, partitionId, term, type, logType, log, timeNode, timePhysical, timeCounter)
          VALUES (@id, @partitionId, @term, @type, @logType, @log, @timeNode, @timePhysical, @timeCounter)
          ON CONFLICT(partitionId, id) DO UPDATE SET term=@term, type=@type, logType=@logType,
          log=@log, timeNode=@timeNode, timePhysical=@timePhysical, timeCounter=@timeCounter;
          """;

        SqliteCommand command = new(sql, connection);
        command.Parameters.Add("@id", SqliteType.Integer);
        command.Parameters.Add("@partitionId", SqliteType.Integer);
        command.Parameters.Add("@term", SqliteType.Integer);
        command.Parameters.Add("@type", SqliteType.Integer);
        command.Parameters.Add("@logType", SqliteType.Text);
        command.Parameters.Add("@log", SqliteType.Blob);
        command.Parameters.Add("@timeNode", SqliteType.Integer);
        command.Parameters.Add("@timePhysical", SqliteType.Integer);
        command.Parameters.Add("@timeCounter", SqliteType.Integer);
        command.Prepare();
        return command;
    }

    private static SqliteCommand GetOrCreatePreparedUpsert(ShardDatabase shard)
    {
        if (shard.PreparedUpsert is not null)
            return shard.PreparedUpsert;
        shard.PreparedUpsert = CreatePreparedUpsert(shard.Connection);
        return shard.PreparedUpsert;
    }

    /// <summary>
    /// Builds and prepares a command whose parameters are all <see cref="SqliteType.Integer"/>.
    /// All hot read lookups take only integer bind values (partitionId, id, term, limit), so this
    /// covers every prepared read command below.
    /// </summary>
    private static SqliteCommand CreatePreparedIntParamCommand(SqliteConnection connection, string sql, params string[] intParams)
    {
        SqliteCommand command = new(sql, connection);
        foreach (string parameter in intParams)
            command.Parameters.Add(parameter, SqliteType.Integer);
        command.Prepare();
        return command;
    }

    private static SqliteCommand GetOrCreateGetTermAt(ShardDatabase shard) =>
        shard.PreparedGetTermAt ??= CreatePreparedIntParamCommand(
            shard.Connection,
            "SELECT term FROM logs WHERE partitionId = @partitionId AND id = @id LIMIT 1",
            "@partitionId", "@id");

    private static SqliteCommand GetOrCreateGetCurrentTerm(ShardDatabase shard) =>
        shard.PreparedGetCurrentTerm ??= CreatePreparedIntParamCommand(
            shard.Connection,
            "SELECT term FROM logs WHERE partitionId = @partitionId ORDER BY id DESC LIMIT 1",
            "@partitionId");

    private static SqliteCommand GetOrCreateGetMaxLog(ShardDatabase shard) =>
        shard.PreparedGetMaxLog ??= CreatePreparedIntParamCommand(
            shard.Connection,
            "SELECT MAX(id) AS max FROM logs WHERE partitionId = @partitionId",
            "@partitionId");

    private static SqliteCommand GetOrCreateGetLastCheckpoint(ShardDatabase shard) =>
        shard.PreparedGetLastCheckpoint ??= CreatePreparedIntParamCommand(
            shard.Connection,
            "SELECT lastCheckpoint FROM checkpoints WHERE partitionId = @partitionId",
            "@partitionId");

    private static SqliteCommand GetOrCreateReadLogsRangeLimited(ShardDatabase shard) =>
        shard.PreparedReadLogsRangeLimited ??= CreatePreparedIntParamCommand(
            shard.Connection,
            """
             SELECT id, term, type, logType, log, timeNode, timePhysical, timeCounter
             FROM logs
             WHERE partitionId = @partitionId AND id >= @startIndex
             ORDER BY id ASC
             LIMIT @maxEntries;
             """,
            "@partitionId", "@startIndex", "@maxEntries");

    private static SqliteCommand GetOrCreateReadLogsRangeUnlimited(ShardDatabase shard) =>
        shard.PreparedReadLogsRangeUnlimited ??= CreatePreparedIntParamCommand(
            shard.Connection,
            """
             SELECT id, term, type, logType, log, timeNode, timePhysical, timeCounter
             FROM logs
             WHERE partitionId = @partitionId AND id >= @startIndex
             ORDER BY id ASC;
             """,
            "@partitionId", "@startIndex");

    private static void BindAndExecUpsert(SqliteCommand cmd, int partitionId, RaftLog log)
    {
        cmd.Parameters["@id"].Value = log.Id;
        cmd.Parameters["@partitionId"].Value = partitionId;
        cmd.Parameters["@term"].Value = log.Term;
        cmd.Parameters["@type"].Value = log.Type;
        cmd.Parameters["@logType"].Value = log.LogType is null ? DBNull.Value : (object)log.LogType;
        cmd.Parameters["@log"].Value = log.LogData is null ? DBNull.Value : (object)log.LogData;
        cmd.Parameters["@timeNode"].Value = log.Time.N;
        cmd.Parameters["@timePhysical"].Value = log.Time.L;
        cmd.Parameters["@timeCounter"].Value = log.Time.C;
        cmd.ExecuteNonQuery();
    }

    // ── IDisposable ───────────────────────────────────────────────────────────

    public void Dispose()
    {
        GC.SuppressFinalize(this);
        semaphore.Dispose();
        metaDataConnection?.Dispose();

        foreach (ShardDatabase shard in shards.Values)
        {
            shard.PreparedUpsert?.Dispose();
            shard.PreparedGetTermAt?.Dispose();
            shard.PreparedGetCurrentTerm?.Dispose();
            shard.PreparedGetMaxLog?.Dispose();
            shard.PreparedGetLastCheckpoint?.Dispose();
            shard.PreparedReadLogsRangeLimited?.Dispose();
            shard.PreparedReadLogsRangeUnlimited?.Dispose();
            shard.Connection.Dispose();
        }
    }
}
