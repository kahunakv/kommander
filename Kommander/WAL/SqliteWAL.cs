
using Kommander.Data;
using Microsoft.Data.Sqlite;

namespace Kommander.WAL;

/// <summary>
/// Allows to use a SQLite database as a Write-Ahead Log (WAL) for Raft logs
/// Each partition has its own database file
/// </summary>
public class SqliteWAL : IWAL, IDisposable
{
    private const int MaxNumberOfRangedEntries = 100;

    private readonly SemaphoreSlim semaphore = new(1, 1);

    private readonly Dictionary<int, (ReaderWriterLock, SqliteConnection)> connections = new();

    private readonly string path;

    private readonly string revision;
    
    private SqliteConnection? metaDataConnection;
    
    private readonly ILogger<IRaft> logger;

    public SqliteWAL(string path, string revision, ILogger<IRaft> logger)
    {
        this.path = path;
        this.revision = revision;
        this.logger = logger;
    }

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