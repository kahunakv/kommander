
using Kommander.Data;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging;

namespace Kommander.WAL;

/// <summary>
/// Allows to use a SQLite database as a Write-Ahead Log (WAL) for Raft logs
/// Each partition has its own database file
/// </summary>
public class SqliteWAL : IWAL, IDisposable
{
    /// <summary>
    /// Represents the maximum number of ranged log entries that can be read in a single call.
    /// This constant is used to limit the number of Raft log entries retrieved from the SQLite Write-Ahead Log (WAL)
    /// during operations like reading a range of logs. It ensures that the operation retrieves a reasonable number
    /// of entries to prevent excessive memory usage or prolonged database operations.
    /// </summary>
    private const int MaxNumberOfRangedEntries = 100;

    /// <summary>
    /// Provides synchronization to ensure thread-safe operations when accessing or modifying the SQLite Write-Ahead Log (WAL).
    /// This semaphore is used to manage concurrent access to database resources, particularly during critical sections
    /// such as opening connections or executing commands. It enforces sequential execution in scenarios where resource
    /// contention could occur, thus preventing race conditions or data corruption.
    /// </summary>
    private readonly SemaphoreSlim semaphore = new(1, 1);

    /// <summary>
    /// Maps partition IDs to their corresponding resources,
    /// specifically a tuple containing a <see cref="ReaderWriterLock"/>
    /// for thread-safe access and a <see cref="SqliteConnection"/> for database operations.
    /// This structure is used to manage and store active connections to each partition's SQLite
    /// Write-Ahead Log (WAL) file, enabling synchronization and efficient database access.
    /// </summary>
    private readonly Dictionary<int, (ReaderWriterLock, SqliteConnection)> connections = new();

    /// <summary>
    /// Represents the base directory path used for database file storage in the SQLite Write-Ahead Log (WAL).
    /// This variable is utilized to construct the file paths for partitioned Raft log databases and metadata files.
    /// It serves as the root directory for all WAL-related database operations, ensuring proper file organization
    /// and retrieval during Raft consensus operations.
    /// </summary>
    private readonly string path;

    /// <summary>
    /// Represents the revision identifier used to differentiate database files associated with the Write-Ahead Log (WAL).
    /// This variable is part of the internal implementation of the SqliteWAL class and is used to construct file paths
    /// for Raft logs and metadata databases. The revision ensures that multiple independent instances of WAL can operate
    /// without conflicting, even if they share the same base directory.
    /// </summary>
    private readonly string revision;

    /// <summary>
    /// Represents a connection to the SQLite database used for storing metadata for the Write-Ahead Log (WAL).
    /// This connection is responsible for managing the metadata table, executing SQL commands related to metadata storage,
    /// and ensuring thread-safety when interacting with the metadata database.
    /// </summary>
    private SqliteConnection? metaDataConnection;

    /// <summary>
    /// Represents the logger instance used for recording messages, errors, and debug information
    /// related to the operation of the SQLite Write-Ahead Log (WAL) for Raft logs.
    /// This logger provides diagnostic information for tracking issues, debugging,
    /// and monitoring activities in the WAL implementation.
    /// </summary>
    private readonly ILogger<IRaft> logger;

    /// <summary>
    /// Represents a Write-Ahead Log (WAL) implementation using SQLite as the storage backend.
    /// </summary>
    public SqliteWAL(string path, string revision, ILogger<IRaft> logger)
    {
        this.path = path;
        this.revision = revision;
        this.logger = logger;
    }

    /// <summary>
    /// Attempts to open or retrieve an existing SQLite database connection and its associated reader-writer lock for the specified partition.
    /// </summary>
    /// <param name="partitionId">The unique identifier of the partition for which the database connection is to be retrieved or opened.</param>
    /// <returns>A tuple containing a <see cref="ReaderWriterLock"/> and a <see cref="SqliteConnection"/> associated with the specified partition.</returns>
    private (ReaderWriterLock, SqliteConnection) TryOpenDatabase(int partitionId)
    {
        if (connections.TryGetValue(partitionId, out (ReaderWriterLock readerWriterLock, SqliteConnection connection) connTuple))
            return connTuple;

        try
        {
            semaphore.Wait();

            if (connections.TryGetValue(partitionId, out connTuple))
                return connTuple;

            string completePath = $"{path}/raft{partitionId}_{revision}.db";

            string connectionString = $"Data Source={completePath}";
            SqliteConnection connection = new(connectionString);

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

            using SqliteCommand command1 = new(createTableQuery, connection);
            command1.ExecuteNonQuery();

            const string pragmasQuery = "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA temp_store=MEMORY;";
            using SqliteCommand command3 = new(pragmasQuery, connection);
            command3.ExecuteNonQuery();
            
            ReaderWriterLock readerWriterLock = new();

            connections.Add(partitionId, (readerWriterLock, connection));

            return (readerWriterLock, connection);
        }
        finally
        {
            semaphore.Release();
        }
    }

    /// <summary>
    /// Attempts to open and initialize the metadata database associated with the Write-Ahead Log (WAL).
    /// Ensures the database file exists, applies necessary configurations, and sets up required tables.
    /// </summary>
    /// <returns>
    /// A SqliteConnection object that represents an open connection to the metadata database.
    /// </returns>
    private SqliteConnection TryOpenMetaDataDatabase()
    {
        if (metaDataConnection is not null)
            return metaDataConnection;

        try
        {
            semaphore.Wait();

            if (metaDataConnection is not null)
                return metaDataConnection;
            
            string completePath = $"{path}/raft_metadata_{revision}.db";
            //bool firstTime = !File.Exists(completePath);

            string connectionString = $"Data Source={completePath}";
            SqliteConnection connection = new(connectionString);

            connection.Open();

            const string createTableQuery = """
            CREATE TABLE IF NOT EXISTS metadata (
                key STRING PRIMARY KEY
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
        finally
        {
            semaphore.Release();
        }
    }

    /// <summary>
    /// Reads logs from the specified partition starting from the last checkpoint.
    /// </summary>
    /// <param name="partitionId">The identifier of the partition to read logs from.</param>
    /// <returns>A list of <see cref="RaftLog"/> containing the logs retrieved from the partition.</returns>
    public List<RaftLog> ReadLogs(int partitionId)
    {
        (ReaderWriterLock readerWriterLock, SqliteConnection connection) = TryOpenDatabase(partitionId);

        try
        {
            readerWriterLock.AcquireReaderLock(TimeSpan.FromSeconds(10));

            List<RaftLog> result = [];

            long lastCheckpoint = GetLastCheckpointInternal(connection, partitionId);

            const string query = """
             SELECT id, term, type, logType, log, timeNode, timePhysical, timeCounter
             FROM logs
             WHERE partitionId = @partitionId AND id > @lastCheckpoint
             ORDER BY id ASC;
             """;

            using SqliteCommand command = new(query, connection);

            command.Parameters.AddWithValue("@partitionId", partitionId);
            command.Parameters.AddWithValue("@lastCheckpoint", lastCheckpoint);

            using SqliteDataReader reader = command.ExecuteReader();

            while (reader.Read())
            {
                result.Add(new()
                {
                    Id = reader.IsDBNull(0) ? 0 : reader.GetInt64(0),
                    Term = reader.IsDBNull(1) ? 0 : reader.GetInt64(1),
                    Type = reader.IsDBNull(2) ? RaftLogType.Proposed : (RaftLogType)reader.GetInt32(2),
                    LogType = reader.IsDBNull(3) ? "" : reader.GetString(3),
                    LogData = reader.IsDBNull(4) ? [] : (byte[])reader[4],
                    Time = new(
                        reader.IsDBNull(5) ? 0 : reader.GetInt32(5), 
                        reader.IsDBNull(6) ? 0 : reader.GetInt64(6),
                        reader.IsDBNull(7) ? 0 : (uint)reader.GetInt64(7)
                    )
                });
            }

            return result;
        }
        finally
        {
            readerWriterLock.ReleaseReaderLock();
        }
    }

    /// <summary>
    /// Reads a range of Raft logs starting from a specified log index for a given partition.
    /// </summary>
    /// <param name="partitionId">The identifier of the partition from which logs should be read.</param>
    /// <param name="startLogIndex">The starting log index from which logs will be retrieved.</param>
    /// <returns>A list of <see cref="RaftLog"/> objects representing the logs in the specified range.</returns>
    public List<RaftLog> ReadLogsRange(int partitionId, long startLogIndex)
    {
        (ReaderWriterLock readerWriterLock, SqliteConnection connection) = TryOpenDatabase(partitionId);

        try
        {
            List<RaftLog> result = [];

            int counter = 0;

            const string query = """
             SELECT id, term, type, logType, log, timeNode, timePhysical, timeCounter
             FROM logs
             WHERE partitionId = @partitionId AND id >= @startIndex
             ORDER BY id ASC;
             """;

            using SqliteCommand command = new(query, connection);

            command.Parameters.AddWithValue("@partitionId", partitionId);
            command.Parameters.AddWithValue("@startIndex", startLogIndex);

            using SqliteDataReader reader = command.ExecuteReader();

            while (reader.Read())
            {
                result.Add(new()
                {
                    Id = reader.IsDBNull(0) ? 0 : reader.GetInt64(0),
                    Term = reader.IsDBNull(1) ? 0 : reader.GetInt64(1),
                    Type = reader.IsDBNull(2) ? RaftLogType.Proposed : (RaftLogType)reader.GetInt32(2),
                    LogType = reader.IsDBNull(3) ? "" : reader.GetString(3),
                    LogData = reader.IsDBNull(4) ? [] : (byte[])reader[4],
                    Time = new(
                        reader.IsDBNull(5) ? 0 : reader.GetInt32(5), 
                        reader.IsDBNull(6) ? 0 : reader.GetInt64(6),
                        reader.IsDBNull(7) ? 0 : (uint)reader.GetInt64(7)
                    )
                });

                counter++;

                if (counter >= MaxNumberOfRangedEntries)
                    break;
            }

            return result;
        }
        finally
        {
            readerWriterLock.ReleaseReaderLock();
        }
    }

    /// <summary>
    /// Writes a collection of logs to a SQLite-backed Write-Ahead Log (WAL) system,
    /// grouping them by partition ID and ensuring efficient upserts for replication and consistency.
    /// </summary>
    /// <param name="logs">
    /// A list of tuples where each tuple contains a partition ID and a list of RaftLog entries
    /// associated with that partition.
    /// </param>
    /// <returns>
    /// Returns a <see cref="RaftOperationStatus"/> indicating the outcome of the write operation.
    /// Success is returned upon successful completion, while specific statuses convey errors or failures.
    /// </returns>
    public RaftOperationStatus Write(List<(int, List<RaftLog>)> logs)
    {
        Dictionary<int, List<RaftLog>> plan = new();

        foreach ((int partitionId, List<RaftLog> raftLogs) log in logs)
        {
            if (plan.TryGetValue(log.partitionId, out List<RaftLog>? raftLogs))
                raftLogs.AddRange(log.raftLogs);
            else
            {
                List<RaftLog> planLogs = [];
                planLogs.AddRange(log.raftLogs);
                plan.Add(log.partitionId, planLogs);
            }
        }
        
        try
        {
            const string insertOrReplaceSql = """
              INSERT INTO logs (id, partitionId, term, type, logType, log, timeNode, timePhysical, timeCounter)
              VALUES (@id, @partitionId, @term, @type, @logType, @log, @timeNode, @timePhysical, @timeCounter)
              ON CONFLICT(partitionId, id) DO UPDATE SET term=@term, type=@type, logType=@logType,
              log=@log, timeNode=@timeNode, timePhysical=@timePhysical, timeCounter=@timeCounter;
              """;
            
            foreach (KeyValuePair<int, List<RaftLog>> kv in plan)
            {
                (ReaderWriterLock readerWriterLock, SqliteConnection connection) = TryOpenDatabase(kv.Key);

                try
                {
                    readerWriterLock.AcquireWriterLock(TimeSpan.FromSeconds(10));

                    using SqliteTransaction transaction = connection.BeginTransaction();
                    
                    using SqliteCommand insertOrReplaceCommand = new(insertOrReplaceSql, connection);

                    insertOrReplaceCommand.Transaction = transaction;

                    insertOrReplaceCommand.Parameters.Add("@id", SqliteType.Integer);
                    insertOrReplaceCommand.Parameters.Add("@partitionId", SqliteType.Integer);
                    insertOrReplaceCommand.Parameters.Add("@term", SqliteType.Integer);
                    insertOrReplaceCommand.Parameters.Add("@type", SqliteType.Integer);
                    insertOrReplaceCommand.Parameters.Add("@logType", SqliteType.Text);
                    insertOrReplaceCommand.Parameters.Add("@log", SqliteType.Blob);
                    insertOrReplaceCommand.Parameters.Add("@timeNode", SqliteType.Integer);
                    insertOrReplaceCommand.Parameters.Add("@timePhysical", SqliteType.Integer);
                    insertOrReplaceCommand.Parameters.Add("@timeCounter", SqliteType.Integer);
                    
                    insertOrReplaceCommand.Prepare();

                    try
                    {
                        foreach (RaftLog log in kv.Value)
                        {
                            insertOrReplaceCommand.Parameters["@id"].Value = log.Id;
                            insertOrReplaceCommand.Parameters["@partitionId"].Value = kv.Key;
                            insertOrReplaceCommand.Parameters["@term"].Value = log.Term;
                            insertOrReplaceCommand.Parameters["@type"].Value = log.Type;

                            if (log.LogType is null)
                                insertOrReplaceCommand.Parameters["@logType"].Value = 0;
                            else
                                insertOrReplaceCommand.Parameters["@logType"].Value = log.LogType;

                            if (log.LogData is null)
                                insertOrReplaceCommand.Parameters["@log"].Value = "";
                            else
                                insertOrReplaceCommand.Parameters["@log"].Value = log.LogData;

                            insertOrReplaceCommand.Parameters["@timeNode"].Value = log.Time.N;
                            insertOrReplaceCommand.Parameters["@timePhysical"].Value = log.Time.L;
                            insertOrReplaceCommand.Parameters["@timeCounter"].Value = log.Time.C;

                            insertOrReplaceCommand.ExecuteNonQuery();
                        }

                        transaction.Commit();
                    }
                    catch
                    {
                        transaction.Rollback();
                        throw;
                    }
                }
                finally
                {
                    readerWriterLock.ReleaseWriterLock();
                }
            }
        } 
        catch (Exception ex)
        {
            logger.LogError("Error during write: {Message}\n{StackTrace}", ex.Message, ex.StackTrace);
                    
            return RaftOperationStatus.Errored;
        }

        return RaftOperationStatus.Success;
    }

    /// <summary>
    /// Retrieves the highest log identifier from the logs for a specific partition.
    /// </summary>
    /// <param name="partitionId">The unique identifier of the partition from which the maximum log identifier is retrieved.</param>
    /// <returns>The highest log identifier for the specified partition. Returns 0 if no logs are found or an error occurs.</returns>
    public long GetMaxLog(int partitionId)
    {
        try
        {
            (ReaderWriterLock readerWriterLock, SqliteConnection connection) = TryOpenDatabase(partitionId);

            try
            {
                readerWriterLock.AcquireReaderLock(TimeSpan.FromSeconds(10));

                const string query = "SELECT MAX(id) AS max FROM logs WHERE partitionId = @partitionId";
                using SqliteCommand command = new(query, connection);

                command.Parameters.AddWithValue("@partitionId", partitionId);

                using SqliteDataReader reader = command.ExecuteReader();

                while (reader.Read())
                    return reader.IsDBNull(0) ? 0 : reader.GetInt64(0);

                return 0;
            } 
            finally
            {
                readerWriterLock.ReleaseReaderLock();
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
    /// </summary>
    /// <param name="partitionId">The identifier of the partition for which the current term is to be retrieved.</param>
    /// <returns>The current term of the Raft log for the specified partition, or 0 if no logs are present.</returns>
    public long GetCurrentTerm(int partitionId)
    {
        (ReaderWriterLock readerWriterLock, SqliteConnection connection) = TryOpenDatabase(partitionId);

        try
        {
            readerWriterLock.AcquireReaderLock(TimeSpan.FromSeconds(10));

            const string query = "SELECT MAX(term) AS max FROM logs WHERE partitionId = @partitionId";
            using SqliteCommand command = new(query, connection);

            command.Parameters.AddWithValue("@partitionId", partitionId);

            using SqliteDataReader reader = command.ExecuteReader();

            while (reader.Read())
                return reader.IsDBNull(0) ? 0 : reader.GetInt64(0);

            return 0;
        }
        finally
        {
            readerWriterLock.ReleaseReaderLock();
        }
    }

    /// <summary>
    /// Retrieves the last recorded checkpoint for the specified log partition.
    /// </summary>
    /// <param name="partitionId">The ID of the log partition for which to get the last checkpoint.</param>
    /// <returns>Returns the log index of the last checkpoint within the specified partition.</returns>
    public long GetLastCheckpoint(int partitionId)
    {
        (ReaderWriterLock readerWriterLock, SqliteConnection connection) = TryOpenDatabase(partitionId);

        try
        {
            readerWriterLock.AcquireReaderLock(TimeSpan.FromSeconds(10));

            return GetLastCheckpointInternal(connection, partitionId);
        }
        finally
        {
            readerWriterLock.ReleaseReaderLock();
        }
    }

    /// <summary>
    /// Retrieves the last committed checkpoint ID from the SQLite logs table for the specified partition.
    /// </summary>
    /// <param name="connection">The SQLite database connection used to execute the query.</param>
    /// <param name="partitionId">The identifier of the partition for which the last checkpoint ID is retrieved.</param>
    /// <returns>The ID of the last committed checkpoint. Returns 0 if no committed checkpoints are found, or -1 in case of errors.</returns>
    private static long GetLastCheckpointInternal(SqliteConnection connection, int partitionId)
    {
        const string query = "SELECT MAX(id) AS max FROM logs WHERE partitionId = @partitionId AND type = @type";
        using SqliteCommand command = new(query, connection);

        command.Parameters.AddWithValue("@partitionId", partitionId);
        command.Parameters.AddWithValue("@type", (int)RaftLogType.CommittedCheckpoint);

        using SqliteDataReader reader = command.ExecuteReader();

        while (reader.Read())
            return reader.IsDBNull(0) ? 0 : reader.GetInt64(0);

        return -1;
    }

    /// <summary>
    /// Compacts logs in the SQLite database that are older than the specified checkpoint and within the specified limit.
    /// </summary>
    /// <param name="partitionId">The identifier of the partition whose logs are to be compacted.</param>
    /// <param name="lastCheckpoint">The checkpoint ID indicating the upper bound for compaction. Logs with IDs less than this value will be compacted.</param>
    /// <param name="compactNumberEntries">The maximum number of log entries to be compacted in a single operation.</param>
    /// <returns>
    /// A <see cref="RaftOperationStatus"/> value indicating the outcome of the operation:
    /// Success if the compaction succeeds, Errored if an error occurs during the operation, or other applicable status codes.
    /// </returns>
    public RaftOperationStatus CompactLogsOlderThan(int partitionId, long lastCheckpoint, int compactNumberEntries)
    {
        (ReaderWriterLock readerWriterLock, SqliteConnection connection) = TryOpenDatabase(partitionId);

        try
        {
            readerWriterLock.AcquireReaderLock(TimeSpan.FromSeconds(10));

            List<long> logs = [];

            const string query = """
             SELECT id
             FROM logs
             WHERE partitionId = @partitionId AND id < @lastCheckpoint
             ORDER BY id ASC
             LIMIT @limit;
             """;

            using SqliteCommand command = new(query, connection);

            command.Parameters.AddWithValue("@partitionId", partitionId);
            command.Parameters.AddWithValue("@lastCheckpoint", lastCheckpoint);
            command.Parameters.AddWithValue("@limit", compactNumberEntries);

            using SqliteDataReader reader = command.ExecuteReader();

            while (reader.Read())
                logs.Add(reader.IsDBNull(0) ? 0 : reader.GetInt64(0));

            if (logs.Count > 0)
            {
                using SqliteTransaction transaction = connection.BeginTransaction();

                try
                {
                    const string deleteSql = "DELETE FROM logs WHERE partitionId = @partitionId AND id = @id";

                    foreach (long log in logs)
                    {
                        using SqliteCommand deleteCommand = new(deleteSql, connection);

                        deleteCommand.Transaction = transaction;

                        deleteCommand.Parameters.AddWithValue("@partitionId", partitionId);
                        deleteCommand.Parameters.AddWithValue("@id", log);

                        deleteCommand.ExecuteNonQuery();
                    }

                    transaction.Commit();
                    
                    logger.LogDebug("Removed {Count} from WAL for partition {PartitionId}", logs.Count, partitionId);

                    return RaftOperationStatus.Success;
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
            
            return RaftOperationStatus.Errored;
        }
        finally
        {
            readerWriterLock.ReleaseReaderLock();
        }
        
        return RaftOperationStatus.Success;
    }

    /// <summary>
    /// Retrieves a metadata value associated with the specified key from the SQLite metadata store.
    /// </summary>
    /// <param name="key">The key for which the metadata value is to be retrieved.</param>
    /// <returns>The metadata value associated with the specified key, or null if the key does not exist or the value is null.</returns>
    public string? GetMetaData(string key)
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

    /// <summary>
    /// Updates or sets the metadata associated with the specified key in the SQLite database.
    /// </summary>
    /// <param name="key">The key identifying the metadata to be updated or added.</param>
    /// <param name="value">The value to be assigned to the specified key.</param>
    /// <returns>Returns <c>true</c> if the metadata update or insertion was successful.</returns>
    public bool SetMetaData(string key, string value)
    {
        SqliteConnection connection = TryOpenMetaDataDatabase();

        const string insertOrReplaceSql = """
                                          INSERT INTO metadata (key, value)
                                          VALUES (@key, @value)
                                          ON CONFLICT(key) DO UPDATE SET value=@value;
                                          """;
        
        using SqliteCommand insertOrReplaceCommand = new(insertOrReplaceSql, connection);

        insertOrReplaceCommand.Parameters.AddWithValue("@key", key);
        
        if (string.IsNullOrEmpty(value))
            insertOrReplaceCommand.Parameters.AddWithValue("@log", "");
        else
            insertOrReplaceCommand.Parameters.AddWithValue("@log", value);

        insertOrReplaceCommand.ExecuteNonQuery();

        return true;
    }

    public void Dispose()
    {
        GC.SuppressFinalize(this);
        
        semaphore.Dispose();
        metaDataConnection?.Dispose();
        
        foreach (KeyValuePair<int, (ReaderWriterLock, SqliteConnection)> conn in connections)
            conn.Value.Item2.Dispose();
    }
}