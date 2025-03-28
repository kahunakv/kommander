
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

    private static readonly SemaphoreSlim semaphore = new(1, 1);

    private static readonly Dictionary<int, SqliteConnection> connections = new();

    private readonly string path;

    private readonly string revision;

    public SqliteWAL(string path = ".", string revision = "1")
    {
        this.path = path;
        this.revision = revision;
    }

    private SqliteConnection TryOpenDatabase(int partitionId)
    {
        if (connections.TryGetValue(partitionId, out SqliteConnection? connection))
            return connection;

        try
        {
            semaphore.Wait();

            if (connections.TryGetValue(partitionId, out connection))
                return connection;

            string connectionString = $"Data Source={path}/raft{partitionId}_{revision}.db";
            connection = new(connectionString);

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

            connections.Add(partitionId, connection);

            return connection;
        }
        finally
        {
            semaphore.Release();
        }
    }

    public List<RaftLog> ReadLogs(int partitionId)
    {
        List<RaftLog> result = [];

        SqliteConnection connection = TryOpenDatabase(partitionId);

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
                LogData = reader.IsDBNull(4) ? []: (byte[])reader[4],
                Time = new(reader.IsDBNull(5) ? 0 : reader.GetInt64(5), reader.IsDBNull(6) ? 0 : (uint)reader.GetInt64(6))
            });
        }

        return result;
    }

    public List<RaftLog> ReadLogsRange(int partitionId, long startLogIndex)
    {
        List<RaftLog> result = [];

        int counter = 0;

        SqliteConnection connection = TryOpenDatabase(partitionId);

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
                LogData = reader.IsDBNull(4) ? []: (byte[])reader[4],
                Time = new(reader.IsDBNull(5) ? 0 : reader.GetInt64(5), reader.IsDBNull(6) ? 0 : (uint)reader.GetInt64(6))
            });

            counter++;

            if (counter >= MaxNumberOfRangedEntries)
                break;
        }

        return result;
    }

    public RaftOperationStatus Propose(int partitionId, RaftLog log)
    {
        SqliteConnection connection = TryOpenDatabase(partitionId);

        // @todo review

        const string insertQuery = """
           INSERT INTO logs (id, partitionId, term, type, logType, log, timePhysical, timeCounter)
           VALUES (@id, @partitionId, @term, @type, @logType, @log, @timePhysical, @timeCounter)
           ON CONFLICT(partitionId, id) DO UPDATE SET term=@term, type=@type, logType=@logType,
           log=@log, timePhysical=@timePhysical, timeCounter=@timeCounter;
           """;

        using SqliteCommand insertCommand =  new(insertQuery, connection);

        insertCommand.Parameters.AddWithValue("@id", log.Id);
        insertCommand.Parameters.AddWithValue("@partitionId", partitionId);
        insertCommand.Parameters.AddWithValue("@term", log.Term);
        insertCommand.Parameters.AddWithValue("@type", log.Type);
        insertCommand.Parameters.AddWithValue("@logType", log.LogType);
        insertCommand.Parameters.AddWithValue("@log", log.LogData);
        insertCommand.Parameters.AddWithValue("@timePhysical", log.Time.L);
        insertCommand.Parameters.AddWithValue("@timeCounter", log.Time.C);

        insertCommand.ExecuteNonQuery();

        return RaftOperationStatus.Success;
    }

    public RaftOperationStatus Commit(int partitionId, RaftLog log)
    {
        SqliteConnection connection = TryOpenDatabase(partitionId);

        const string updateQuery = "UPDATE logs SET type = @type WHERE partitionId = @partitionId AND id = @id";
        using SqliteCommand updateCommand =  new(updateQuery, connection);

        updateCommand.Parameters.AddWithValue("@id", log.Id);
        updateCommand.Parameters.AddWithValue("@partitionId", partitionId);
        updateCommand.Parameters.AddWithValue("@type", log.Type);

        updateCommand.ExecuteNonQuery();

        return RaftOperationStatus.Success;
    }

    public RaftOperationStatus Rollback(int partitionId, RaftLog log)
    {
        SqliteConnection connection = TryOpenDatabase(partitionId);

        const string updateQuery = "UPDATE logs SET type = @type WHERE partitionId = @partitionId AND id = @id";
        using SqliteCommand updateCommand =  new(updateQuery, connection);

        updateCommand.Parameters.AddWithValue("@id", log.Id);
        updateCommand.Parameters.AddWithValue("@partitionId", partitionId);
        updateCommand.Parameters.AddWithValue("@type", log.Type);

        updateCommand.ExecuteNonQuery();

        return RaftOperationStatus.Success;
    }

    public RaftOperationStatus ProposeMany(int partitionId, List<RaftLog> logs)
    {
        SqliteConnection connection = TryOpenDatabase(partitionId);

        const string insertQuery = """
           INSERT INTO logs (id, partitionId, term, type, logType, log, timePhysical, timeCounter)
           VALUES (@id, @partitionId, @term, @type, @logType, @log, @timePhysical, @timeCounter)
           ON CONFLICT(partitionId, id) DO UPDATE SET term=@term, type=@type, logType=@logType,
           log=@log, timePhysical=@timePhysical, timeCounter=@timeCounter;
           """;

        foreach (RaftLog log in logs)
        {
            using SqliteCommand insertCommand = new(insertQuery, connection);

            insertCommand.Parameters.AddWithValue("@id", log.Id);
            insertCommand.Parameters.AddWithValue("@partitionId", partitionId);
            insertCommand.Parameters.AddWithValue("@term", log.Term);
            insertCommand.Parameters.AddWithValue("@type", log.Type);
            insertCommand.Parameters.AddWithValue("@logType", log.LogType);
            insertCommand.Parameters.AddWithValue("@log", log.LogData);
            insertCommand.Parameters.AddWithValue("@timePhysical", log.Time.L);
            insertCommand.Parameters.AddWithValue("@timeCounter", log.Time.C);

            insertCommand.ExecuteNonQuery();
        }

        return RaftOperationStatus.Success;
    }

    public RaftOperationStatus CommitMany(int partitionId, List<RaftLog> logs)
    {
        SqliteConnection connection = TryOpenDatabase(partitionId);

        const string insertQuery = """
           INSERT INTO logs (id, partitionId, term, type, logType, log, timePhysical, timeCounter)
           VALUES (@id, @partitionId, @term, @type, @logType, @log, @timePhysical, @timeCounter)
           ON CONFLICT(partitionId, id) DO UPDATE SET term=@term, type=@type, logType=@logType,
           log=@log, timePhysical=@timePhysical, timeCounter=@timeCounter;
           """;

        foreach (RaftLog log in logs)
        {
            using SqliteCommand insertCommand = new(insertQuery, connection);

            insertCommand.Parameters.AddWithValue("@id", log.Id);
            insertCommand.Parameters.AddWithValue("@partitionId", partitionId);
            insertCommand.Parameters.AddWithValue("@term", log.Term);
            insertCommand.Parameters.AddWithValue("@type", log.Type);
            insertCommand.Parameters.AddWithValue("@logType", log.LogType);
            insertCommand.Parameters.AddWithValue("@log", log.LogData);
            insertCommand.Parameters.AddWithValue("@timePhysical", log.Time.L);
            insertCommand.Parameters.AddWithValue("@timeCounter", log.Time.C);

            insertCommand.ExecuteNonQuery();
        }

        return RaftOperationStatus.Success;
    }

    public RaftOperationStatus RollbackMany(int partitionId, List<RaftLog> logs)
    {
        SqliteConnection connection = TryOpenDatabase(partitionId);

        const string insertQuery = """
           INSERT INTO logs (id, partitionId, term, type, logType, log, timePhysical, timeCounter)
           VALUES (@id, @partitionId, @term, @type, @logType, @log, @timePhysical, @timeCounter)
           ON CONFLICT(partitionId, id) DO UPDATE SET term=@term, type=@type, logType=@logType,
           log=@log, timePhysical=@timePhysical, timeCounter=@timeCounter;
           """;

        foreach (RaftLog log in logs)
        {
            using SqliteCommand insertCommand = new(insertQuery, connection);

            insertCommand.Parameters.AddWithValue("@id", log.Id);
            insertCommand.Parameters.AddWithValue("@partitionId", partitionId);
            insertCommand.Parameters.AddWithValue("@term", log.Term);
            insertCommand.Parameters.AddWithValue("@type", log.Type);
            insertCommand.Parameters.AddWithValue("@logType", log.LogType);
            insertCommand.Parameters.AddWithValue("@log", log.LogData);
            insertCommand.Parameters.AddWithValue("@timePhysical", log.Time.L);
            insertCommand.Parameters.AddWithValue("@timeCounter", log.Time.C);

            insertCommand.ExecuteNonQuery();
        }

        return RaftOperationStatus.Success;
    }

    public long GetMaxLog(int partitionId)
    {
        SqliteConnection connection = TryOpenDatabase(partitionId);

        const string query = "SELECT MAX(id) AS max FROM logs WHERE partitionId = @partitionId";
        using SqliteCommand command = new(query, connection);

        command.Parameters.AddWithValue("@partitionId", partitionId);

        using SqliteDataReader reader = command.ExecuteReader();

        while (reader.Read())
            return reader.IsDBNull(0) ? 0 : reader.GetInt64(0);

        return 0;
    }

    public long GetCurrentTerm(int partitionId)
    {
        SqliteConnection connection = TryOpenDatabase(partitionId);

        const string query = "SELECT MAX(term) AS max FROM logs WHERE partitionId = @partitionId";
        using SqliteCommand command = new(query, connection);

        command.Parameters.AddWithValue("@partitionId", partitionId);

        using SqliteDataReader reader = command.ExecuteReader();

        while (reader.Read())
            return reader.IsDBNull(0) ? 0 : reader.GetInt64(0);

        return 0;
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
}