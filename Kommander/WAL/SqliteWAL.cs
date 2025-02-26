
using Kommander.Data;
using Microsoft.Data.Sqlite;

namespace Kommander.WAL;

public class SqliteWAL : IWAL
{
    private static readonly SemaphoreSlim semaphore = new(1, 1);
    
    private static readonly Dictionary<int, SqliteConnection> connections = new();

    private readonly string path;
    
    public SqliteWAL(string path = ".")
    {
        this.path = path;
    }
    
    private async ValueTask<SqliteConnection> TryOpenDatabase(int partitionId)
    {
        if (connections.TryGetValue(partitionId, out SqliteConnection? connection))
            return connection;

        try
        {
            await semaphore.WaitAsync();

            if (connections.TryGetValue(partitionId, out connection))
                return connection;

            string connectionString = $"Data Source={path}/raft{partitionId}.db";
            connection = new(connectionString);

            connection.Open();

            const string createTableQuery = "CREATE TABLE IF NOT EXISTS logs (id INT, partitionId INT, term INT, type INT, log BLOB, timeLogical INT, timeCounter INT, PRIMARY KEY(partitionId, id));";
            await using SqliteCommand command1 = new(createTableQuery, connection);
            await command1.ExecuteNonQueryAsync();
            
            const string pragmasQuery = "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA temp_store=MEMORY; PRAGMA wal_checkpoint(FULL);";
            await using SqliteCommand command3 = new(pragmasQuery, connection);
            await command3.ExecuteNonQueryAsync();
            
            connections.Add(partitionId, connection);

            return connection;
        }
        finally
        {
            semaphore.Release();
        }
    }
    
    public async IAsyncEnumerable<RaftLog> ReadLogs(int partitionId)
    {
        SqliteConnection connection = await TryOpenDatabase(partitionId);
        
        const string query = "SELECT id, term, type, log, timeLogical, timeCounter FROM logs WHERE partitionId = @partitionId ORDER BY id ASC;";
        await using SqliteCommand command = new(query, connection);
        command.Parameters.AddWithValue("@partitionId", partitionId);

        await using SqliteDataReader reader = await command.ExecuteReaderAsync();
        
        while (reader.Read())
        {
            yield return new()
            {
                Id = reader.IsDBNull(0) ? 0 : reader.GetInt64(0),
                Term = reader.IsDBNull(1) ? 0 : reader.GetInt64(1),
                Type = reader.IsDBNull(2) ? RaftLogType.Regular : (RaftLogType)reader.GetInt32(2),
                Log = reader.IsDBNull(3) ? [] : reader[3] is not null ? (byte[])reader[3] : [],
                Time = new(reader.IsDBNull(4) ? 0 : reader.GetInt64(4), reader.IsDBNull(5) ? 0 : (uint)reader.GetInt64(5))
            };
        }
    }
    
    public async IAsyncEnumerable<RaftLog> ReadLogsRange(int partitionId, long startLogIndex)
    {
        SqliteConnection connection = await TryOpenDatabase(partitionId);
        
        const string query = "SELECT id, term, type, log, timeLogical, timeCounter FROM logs WHERE partitionId = @partitionId AND id >= @startIndex ORDER BY id ASC;";
        await using SqliteCommand command = new(query, connection);
        
        command.Parameters.AddWithValue("@partitionId", partitionId);
        command.Parameters.AddWithValue("@startIndex", startLogIndex);
        
        await using SqliteDataReader reader = await command.ExecuteReaderAsync();
        
        while (reader.Read())
        {
            yield return new()
            {
                Id = reader.IsDBNull(0) ? 0 : reader.GetInt64(0),
                Term = reader.IsDBNull(1) ? 0 : reader.GetInt64(1),
                Type = reader.IsDBNull(2) ? RaftLogType.Regular : (RaftLogType)reader.GetInt32(2),
                Log = reader.IsDBNull(3) ? [] : reader[3] is not null ? (byte[])reader[3] : [],
                Time = new(reader.IsDBNull(4) ? 0 : reader.GetInt64(4), reader.IsDBNull(5) ? 0 : (uint)reader.GetInt64(5))
            };
        }
    }

    public async Task Append(int partitionId, RaftLog log)
    {
        SqliteConnection connection = await TryOpenDatabase(partitionId);
        
        const string insertQuery = "INSERT INTO logs (id, partitionId, term, type, log, timeLogical, timeCounter) VALUES (@id, @partitionId, @term, @type, @log, @timeLogical, @timeCounter);";
        await using SqliteCommand insertCommand =  new(insertQuery, connection);
        
        insertCommand.Parameters.Clear();
        
        insertCommand.Parameters.AddWithValue("@id", log.Id);
        insertCommand.Parameters.AddWithValue("@partitionId", partitionId);
        insertCommand.Parameters.AddWithValue("@term", log.Term);
        insertCommand.Parameters.AddWithValue("@type", log.Type);
        insertCommand.Parameters.AddWithValue("@log", log.Log);
        insertCommand.Parameters.AddWithValue("@timeLogical", log.Time.L);
        insertCommand.Parameters.AddWithValue("@timeCounter", log.Time.C);
        
        await insertCommand.ExecuteNonQueryAsync();
    }
    
    public async Task AppendUpdate(int partitionId, RaftLog log)
    {
        await Append(partitionId, log);
    }
    
    public async Task<long> GetMaxLog(int partitionId)
    {
        SqliteConnection connection = await TryOpenDatabase(partitionId);
        
        const string query = "SELECT MAX(id) AS max FROM logs WHERE partitionId = @partitionId";
        await using SqliteCommand command = new(query, connection);
        
        command.Parameters.AddWithValue("@partitionId", partitionId);
        
        await using SqliteDataReader reader = await command.ExecuteReaderAsync();
        
        while (reader.Read())
            return reader.IsDBNull(0) ? 0 : reader.GetInt64(0);

        return 0;
    }
    
    public async Task<long> GetCurrentTerm(int partitionId)
    {
        SqliteConnection connection = await TryOpenDatabase(partitionId);
        
        const string query = "SELECT MAX(term) AS max FROM logs WHERE partitionId = @partitionId";
        await using SqliteCommand command = new(query, connection);
        
        command.Parameters.AddWithValue("@partitionId", partitionId);
        
        await using SqliteDataReader reader = await command.ExecuteReaderAsync();
        
        while (reader.Read())
            return reader.IsDBNull(0) ? 0 : reader.GetInt64(0);

        return 0;
    }
}