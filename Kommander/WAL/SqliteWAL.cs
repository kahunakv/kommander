
using Kommander.Data;
using Microsoft.Data.Sqlite;

namespace Kommander.WAL;

/// <summary>
/// Allows to use a SQLite database as a Write-Ahead Log (WAL) for Raft logs
/// Each partition has its own database file
/// </summary>
public class SqliteWAL : IWAL
{
    private const int MaxNumberOfRangedEntries = 100;

    private readonly SemaphoreSlim semaphore = new(1, 1);

    private readonly Dictionary<int, (ReaderWriterLock, SqliteConnection)> connections = new();

    private readonly string path;

    private readonly string revision;
    
    private SqliteConnection? metaDataConnection;

    public SqliteWAL(string path = ".", string revision = "1")
    {
        this.path = path;
        this.revision = revision;
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

            string connectionString = $"Data Source={path}/raft{partitionId}_{revision}.db";
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

            string connectionString = $"Data Source={path}/raft_metadata_{revision}.db";
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

            const string pragmasQuery = "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;";
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

            long lastCheckpoint = GetLastCheckpoint(connection, partitionId);

            const string query = """
             SELECT id, term, type, logType, log, timePhysical, timeCounter
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
                    Time = new(reader.IsDBNull(5) ? 0 : reader.GetInt64(5),
                        reader.IsDBNull(6) ? 0 : (uint)reader.GetInt64(6))
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
             SELECT id, term, type, logType, log, timePhysical, timeCounter
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
                    Time = new(reader.IsDBNull(5) ? 0 : reader.GetInt64(5),
                        reader.IsDBNull(6) ? 0 : (uint)reader.GetInt64(6))
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

    public RaftOperationStatus Propose(int partitionId, RaftLog log)
    {
        try
        {
            (ReaderWriterLock readerWriterLock, SqliteConnection connection) = TryOpenDatabase(partitionId);

            try
            {
                readerWriterLock.AcquireWriterLock(TimeSpan.FromSeconds(10));
                
                // @todo review

                const string insertOrReplaceQuery = """
                    INSERT INTO logs (id, partitionId, term, type, logType, log, timePhysical, timeCounter)
                    VALUES (@id, @partitionId, @term, @type, @logType, @log, @timePhysical, @timeCounter)
                    ON CONFLICT(partitionId, id) DO UPDATE SET term=@term, type=@type, logType=@logType,
                    log=@log, timePhysical=@timePhysical, timeCounter=@timeCounter;
                    """;

                using SqliteCommand insertOrReplaceCommand = new(insertOrReplaceQuery, connection);

                insertOrReplaceCommand.Parameters.AddWithValue("@id", log.Id);
                insertOrReplaceCommand.Parameters.AddWithValue("@partitionId", partitionId);
                insertOrReplaceCommand.Parameters.AddWithValue("@term", log.Term);
                insertOrReplaceCommand.Parameters.AddWithValue("@type", log.Type);

                if (log.LogType is null)
                    insertOrReplaceCommand.Parameters.AddWithValue("@logType", 0);
                else
                    insertOrReplaceCommand.Parameters.AddWithValue("@logType", log.LogType);

                if (log.LogData is null)
                    insertOrReplaceCommand.Parameters.AddWithValue("@log", "");
                else
                    insertOrReplaceCommand.Parameters.AddWithValue("@log", log.LogData);

                insertOrReplaceCommand.Parameters.AddWithValue("@timePhysical", log.Time.L);
                insertOrReplaceCommand.Parameters.AddWithValue("@timeCounter", log.Time.C);

                insertOrReplaceCommand.ExecuteNonQuery();

                return RaftOperationStatus.Success;
            }
            finally
            {
                readerWriterLock.ReleaseWriterLock();
            }
        } 
        catch (Exception ex)
        {
            Console.WriteLine($"Error during proposal: {ex.Message}\n{ex.StackTrace}");
                                        
            return RaftOperationStatus.Errored;
        }
    }

    public RaftOperationStatus Commit(int partitionId, RaftLog log)
    {
        try
        {
            (ReaderWriterLock readerWriterLock, SqliteConnection connection) = TryOpenDatabase(partitionId);

            try
            {
                readerWriterLock.AcquireWriterLock(TimeSpan.FromSeconds(10));

                const string updateQuery = "UPDATE logs SET type = @type WHERE partitionId = @partitionId AND id = @id";
                using SqliteCommand updateCommand = new(updateQuery, connection);

                updateCommand.Parameters.AddWithValue("@id", log.Id);
                updateCommand.Parameters.AddWithValue("@partitionId", partitionId);
                updateCommand.Parameters.AddWithValue("@type", log.Type);

                updateCommand.ExecuteNonQuery();

                return RaftOperationStatus.Success;
            } 
            finally
            {
                readerWriterLock.ReleaseWriterLock();
            }
        } 
        catch (Exception ex)
        {
            Console.WriteLine($"Error during commit: {ex.Message}\n{ex.StackTrace}");
                                    
            return RaftOperationStatus.Errored;
        }
    }

    public RaftOperationStatus Rollback(int partitionId, RaftLog log)
    {
        try
        {
            (ReaderWriterLock readerWriterLock, SqliteConnection connection) = TryOpenDatabase(partitionId);

            try
            {
                readerWriterLock.AcquireWriterLock(TimeSpan.FromSeconds(10));

                const string updateQuery = "UPDATE logs SET type = @type WHERE partitionId = @partitionId AND id = @id";
                using SqliteCommand updateCommand = new(updateQuery, connection);

                updateCommand.Parameters.AddWithValue("@id", log.Id);
                updateCommand.Parameters.AddWithValue("@partitionId", partitionId);
                updateCommand.Parameters.AddWithValue("@type", log.Type);

                updateCommand.ExecuteNonQuery();

                return RaftOperationStatus.Success;
            } 
            finally
            {
                readerWriterLock.ReleaseWriterLock();
            }
        } 
        catch (Exception ex)
        {
            Console.WriteLine($"Error during rollback: {ex.Message}\n{ex.StackTrace}");
                                
            return RaftOperationStatus.Errored;
        }
    }

    public RaftOperationStatus ProposeMany(int partitionId, List<RaftLog> logs)
    {
        try
        {
            (ReaderWriterLock readerWriterLock, SqliteConnection connection) = TryOpenDatabase(partitionId);

            try
            {
                readerWriterLock.AcquireWriterLock(TimeSpan.FromSeconds(10));

                const string insertOrReplaceSql = """
                   INSERT INTO logs (id, partitionId, term, type, logType, log, timePhysical, timeCounter)
                   VALUES (@id, @partitionId, @term, @type, @logType, @log, @timePhysical, @timeCounter)
                   ON CONFLICT(partitionId, id) DO UPDATE SET term=@term, type=@type, logType=@logType,
                   log=@log, timePhysical=@timePhysical, timeCounter=@timeCounter;
                   """;
                
                using SqliteTransaction transaction = connection.BeginTransaction();

                try
                {
                    foreach (RaftLog log in logs)
                    {
                        using SqliteCommand insertOrReplaceCommand = new(insertOrReplaceSql, connection);

                        insertOrReplaceCommand.Transaction = transaction;

                        insertOrReplaceCommand.Parameters.AddWithValue("@id", log.Id);
                        insertOrReplaceCommand.Parameters.AddWithValue("@partitionId", partitionId);
                        insertOrReplaceCommand.Parameters.AddWithValue("@term", log.Term);
                        insertOrReplaceCommand.Parameters.AddWithValue("@type", log.Type);
                        
                        if (log.LogType is null)
                            insertOrReplaceCommand.Parameters.AddWithValue("@logType", 0);
                        else
                            insertOrReplaceCommand.Parameters.AddWithValue("@logType", log.LogType);
                        
                        if (log.LogData is null)
                            insertOrReplaceCommand.Parameters.AddWithValue("@log", "");
                        else
                            insertOrReplaceCommand.Parameters.AddWithValue("@log", log.LogData);
                        
                        insertOrReplaceCommand.Parameters.AddWithValue("@timePhysical", log.Time.L);
                        insertOrReplaceCommand.Parameters.AddWithValue("@timeCounter", log.Time.C);

                        insertOrReplaceCommand.ExecuteNonQuery();
                    }
                    
                    transaction.Commit();

                    return RaftOperationStatus.Success;
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
        catch (Exception ex)
        {
            Console.WriteLine($"Error during propose: {ex.Message}\n{ex.StackTrace}");
                            
            return RaftOperationStatus.Errored;
        }
    }

    public RaftOperationStatus CommitMany(int partitionId, List<RaftLog> logs)
    {
        try
        {
            (ReaderWriterLock readerWriterLock, SqliteConnection connection) = TryOpenDatabase(partitionId);

            try
            {
                readerWriterLock.AcquireWriterLock(TimeSpan.FromSeconds(10));

                const string insertOrReplaceSql = """
                   INSERT INTO logs (id, partitionId, term, type, logType, log, timePhysical, timeCounter)
                   VALUES (@id, @partitionId, @term, @type, @logType, @log, @timePhysical, @timeCounter)
                   ON CONFLICT(partitionId, id) DO UPDATE SET term=@term, type=@type, logType=@logType,
                   log=@log, timePhysical=@timePhysical, timeCounter=@timeCounter;
                   """;

                using SqliteTransaction transaction = connection.BeginTransaction();

                try
                {
                    foreach (RaftLog log in logs)
                    {
                        using SqliteCommand insertOrReplaceCommand = new(insertOrReplaceSql, connection);

                        insertOrReplaceCommand.Transaction = transaction;

                        insertOrReplaceCommand.Parameters.AddWithValue("@id", log.Id);
                        insertOrReplaceCommand.Parameters.AddWithValue("@partitionId", partitionId);
                        insertOrReplaceCommand.Parameters.AddWithValue("@term", log.Term);
                        insertOrReplaceCommand.Parameters.AddWithValue("@type", log.Type);
                        
                        if (log.LogType is null)
                            insertOrReplaceCommand.Parameters.AddWithValue("@logType", 0);
                        else
                            insertOrReplaceCommand.Parameters.AddWithValue("@logType", log.LogType);
                        
                        if (log.LogData is null)
                            insertOrReplaceCommand.Parameters.AddWithValue("@log", "");
                        else
                            insertOrReplaceCommand.Parameters.AddWithValue("@log", log.LogData);
                        
                        insertOrReplaceCommand.Parameters.AddWithValue("@timePhysical", log.Time.L);
                        insertOrReplaceCommand.Parameters.AddWithValue("@timeCounter", log.Time.C);

                        insertOrReplaceCommand.ExecuteNonQuery();
                    }

                    transaction.Commit();

                    return RaftOperationStatus.Success;
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
        catch (Exception ex)
        {
            Console.WriteLine($"Error during commit: {ex.Message}\n{ex.StackTrace}");
                        
            return RaftOperationStatus.Errored;
        }
    }

    public RaftOperationStatus RollbackMany(int partitionId, List<RaftLog> logs)
    {
        try
        {
            (ReaderWriterLock readerWriterLock, SqliteConnection connection) = TryOpenDatabase(partitionId);

            try
            {
                readerWriterLock.AcquireWriterLock(TimeSpan.FromSeconds(10));
                
                const string insertOrReplaceSql = """
                  INSERT INTO logs (id, partitionId, term, type, logType, log, timePhysical, timeCounter)
                  VALUES (@id, @partitionId, @term, @type, @logType, @log, @timePhysical, @timeCounter)
                  ON CONFLICT(partitionId, id) DO UPDATE SET term=@term, type=@type, logType=@logType,
                  log=@log, timePhysical=@timePhysical, timeCounter=@timeCounter;
                  """;
                
                using SqliteTransaction transaction = connection.BeginTransaction();

                try
                {
                    foreach (RaftLog log in logs)
                    {
                        using SqliteCommand insertOrReplaceCommand = new(insertOrReplaceSql, connection);

                        insertOrReplaceCommand.Transaction = transaction;

                        insertOrReplaceCommand.Parameters.AddWithValue("@id", log.Id);
                        insertOrReplaceCommand.Parameters.AddWithValue("@partitionId", partitionId);
                        insertOrReplaceCommand.Parameters.AddWithValue("@term", log.Term);
                        insertOrReplaceCommand.Parameters.AddWithValue("@type", log.Type);
                        
                        if (log.LogType is null)
                            insertOrReplaceCommand.Parameters.AddWithValue("@logType", 0);
                        else
                            insertOrReplaceCommand.Parameters.AddWithValue("@logType", log.LogType);
                        
                        if (log.LogData is null)
                            insertOrReplaceCommand.Parameters.AddWithValue("@log", "");
                        else
                            insertOrReplaceCommand.Parameters.AddWithValue("@log", log.LogData);
                        
                        insertOrReplaceCommand.Parameters.AddWithValue("@timePhysical", log.Time.L);
                        insertOrReplaceCommand.Parameters.AddWithValue("@timeCounter", log.Time.C);

                        insertOrReplaceCommand.ExecuteNonQuery();
                    }

                    transaction.Commit();

                    return RaftOperationStatus.Success;
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
        catch (Exception ex)
        {
            Console.WriteLine($"Error during rollback: {ex.Message}");
                    
            return RaftOperationStatus.Errored;
        }
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
            Console.WriteLine($"Error during GetMaxLog: {ex.Message}\n{ex.StackTrace}");
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

    private static long GetLastCheckpoint(SqliteConnection connection, int partitionId)
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
}