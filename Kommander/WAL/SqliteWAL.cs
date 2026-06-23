
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
    /// Per-shard SQLite state: exclusive lock, open connection, and a lazily-created
    /// prepared upsert reused across write transactions on that connection.
    /// </summary>
    private sealed class ShardDatabase
    {
        public object Lock { get; } = new();
        public SqliteConnection Connection { get; }
        public SqliteCommand? PreparedUpsert { get; set; }
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
            long lastCheckpoint = GetLastCheckpointInternal(shard.Connection, partitionId);

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
            string query = applyLimit
                ? """
                 SELECT id, term, type, logType, log, timeNode, timePhysical, timeCounter
                 FROM logs
                 WHERE partitionId = @partitionId AND id >= @startIndex
                 ORDER BY id ASC
                 LIMIT @maxEntries;
                 """
                : """
                 SELECT id, term, type, logType, log, timeNode, timePhysical, timeCounter
                 FROM logs
                 WHERE partitionId = @partitionId AND id >= @startIndex
                 ORDER BY id ASC;
                 """;

            using SqliteCommand command = new(query, shard.Connection);
            command.Parameters.AddWithValue("@partitionId", partitionId);
            command.Parameters.AddWithValue("@startIndex", startLogIndex);
            if (applyLimit)
                command.Parameters.AddWithValue("@maxEntries", maxEntries);

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
                const string query = "SELECT MAX(id) AS max FROM logs WHERE partitionId = @partitionId";
                using SqliteCommand command = new(query, shard.Connection);
                command.Parameters.AddWithValue("@partitionId", partitionId);
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
    public long GetCurrentTerm(int partitionId)
    {
        ShardDatabase shard = TryOpenShard(ShardOf(partitionId));
        lock (shard.Lock)
        {
            const string query = "SELECT term FROM logs WHERE partitionId = @partitionId ORDER BY id DESC LIMIT 1";
            using SqliteCommand command = new(query, shard.Connection);
            command.Parameters.AddWithValue("@partitionId", partitionId);
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
            return GetLastCheckpointInternal(shard.Connection, partitionId);
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
            long lastCheckpoint = GetLastCheckpointInternal(shard.Connection, partitionId);
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
    public RaftOperationStatus Write(List<(int, List<RaftLog>)> logs)
    {
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
                    using SqliteTransaction transaction = shard.Connection.BeginTransaction();
                    SqliteCommand upsert = GetOrCreatePreparedUpsert(shard);
                    upsert.Transaction = transaction;

                    try
                    {
                        foreach (KeyValuePair<int, List<RaftLog>> kv in shardEntry.Value)
                        {
                            int partitionId = kv.Key;
                            foreach (RaftLog log in kv.Value)
                                BindAndExecUpsert(upsert, partitionId, log);
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
                using SqliteCommand command = new(
                    "DELETE FROM logs WHERE partitionId = @partitionId", shard.Connection);
                command.Parameters.AddWithValue("@partitionId", partitionId);
                command.ExecuteNonQuery();
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
                using SqliteCommand command = new(
                    "DELETE FROM logs WHERE partitionId = @partitionId AND id > @afterLogId",
                    shard.Connection);
                command.Parameters.AddWithValue("@partitionId", partitionId);
                command.Parameters.AddWithValue("@afterLogId", afterLogId);
                command.ExecuteNonQuery();
                return RaftOperationStatus.Success;
            }
            catch (Exception ex)
            {
                logger.LogError("TruncateLogsAfter({PartitionId}, {AfterLogId}): {Message}", partitionId, afterLogId, ex.Message);
                return RaftOperationStatus.Errored;
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

    private static long GetLastCheckpointInternal(SqliteConnection connection, int partitionId)
    {
        const string query = "SELECT MAX(id) AS max FROM logs WHERE partitionId = @partitionId AND type = @type";
        using SqliteCommand command = new(query, connection);
        command.Parameters.AddWithValue("@partitionId", partitionId);
        command.Parameters.AddWithValue("@type", (int)RaftLogType.CommittedCheckpoint);
        using SqliteDataReader reader = command.ExecuteReader();
        while (reader.Read())
            return reader.IsDBNull(0) ? -1 : reader.GetInt64(0);
        return -1;
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
            shard.Connection.Dispose();
        }
    }
}
