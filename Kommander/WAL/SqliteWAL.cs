
using Kommander.Data;
using Kommander.Time;
using Microsoft.Data.Sqlite;
// ReSharper disable MethodHasAsyncOverload
// ReSharper disable UseAwaitUsing

namespace Kommander.WAL;

public class SqliteWAL : IWAL
{
    private static readonly object _lock = new();
    
    private static SqliteConnection? connection;

    private readonly string path;
    
    public SqliteWAL(string path = ".")
    {
        this.path = path;
    }
    
    private void TryOpenDatabase()
    {
        if (connection is not null)
            return;

        lock (_lock)
        {
            if (connection is not null)
                return;
            
            string connectionString = $"Data Source={path}/database.db";
            connection = new(connectionString);

            connection.Open();

            const string createTableQuery = "CREATE TABLE IF NOT EXISTS logs (id INT, partitionId INT, term INT, type INT, message TEXT COLLATE BINARY, timeLogical INT, timeCounter INT, PRIMARY KEY(partitionId, id));";
            using SqliteCommand command1 = new(createTableQuery, connection);
            command1.ExecuteNonQuery();
            
            const string enableWalQuery = "PRAGMA journal_mode=WAL;";
            using SqliteCommand command2 = new(enableWalQuery, connection);
            command2.ExecuteNonQuery();
            
            const string enableSynchronousQuery = "PRAGMA synchronous=NORMAL;";
            using SqliteCommand command3 = new(enableSynchronousQuery, connection);
            command3.ExecuteNonQuery();
            
            const string tempStoreQuery = "PRAGMA temp_store=MEMORY;";
            using SqliteCommand command4 = new(tempStoreQuery, connection);
            command4.ExecuteNonQuery();
            
            const string checpointQuery = "PRAGMA wal_checkpoint(FULL);";
            using SqliteCommand command6 = new(checpointQuery, connection);
            command6.ExecuteNonQuery();
            
            //const string vacuumQuery = "VACUUM;";
            //using SqliteCommand command5 = new(vacuumQuery, connection);
            //command5.ExecuteNonQuery();
        }
    }
    
    public async IAsyncEnumerable<RaftLog> ReadLogs(int partitionId)
    {
        await Task.CompletedTask;
        
        TryOpenDatabase();
        
        const string query = "SELECT id, term, type, message, timeLogical, timeCounter FROM logs WHERE partitionId = @partitionId ORDER BY id ASC;";
        using SqliteCommand command = new(query, connection);
        command.Parameters.AddWithValue("@partitionId", partitionId);
        
        using SqliteDataReader reader = command.ExecuteReader();
        
        while (reader.Read())
        {
            yield return new()
            {
                Id = reader.IsDBNull(0) ? 0 : reader.GetInt64(0),
                Term = reader.IsDBNull(1) ? 0 : reader.GetInt64(1),
                Type = reader.IsDBNull(2) ? RaftLogType.Regular : (RaftLogType)reader.GetInt32(2),
                Message = reader.IsDBNull(3) ? "" : reader.GetString(3),
                Time = new(reader.IsDBNull(4) ? 0 : reader.GetInt64(4), reader.IsDBNull(5) ? 0 : (uint)reader.GetInt64(5))
            };
        }
    }
    
    public async IAsyncEnumerable<RaftLog> ReadLogsRange(int partitionId, long startLogIndex)
    {
        await Task.CompletedTask;
        
        TryOpenDatabase();
        
        const string query = "SELECT id, term, type, message, timeLogical, timeCounter FROM logs WHERE partitionId = @partitionId AND id >= @startIndex ORDER BY id ASC;";
        using SqliteCommand command = new(query, connection);
        
        command.Parameters.AddWithValue("@partitionId", partitionId);
        command.Parameters.AddWithValue("@startIndex", startLogIndex);
        
        using SqliteDataReader reader = command.ExecuteReader();
        
        while (reader.Read())
        {
            yield return new()
            {
                Id = reader.IsDBNull(0) ? 0 : reader.GetInt64(0),
                Term = reader.IsDBNull(1) ? 0 : reader.GetInt64(1),
                Type = reader.IsDBNull(2) ? RaftLogType.Regular : (RaftLogType)reader.GetInt32(2),
                Message = reader.IsDBNull(3) ? "" : reader.GetString(3),
                Time = new(reader.IsDBNull(4) ? 0 : reader.GetInt64(4), reader.IsDBNull(5) ? 0 : (uint)reader.GetInt64(5))
            };
        }
    }
    
    public async Task<bool> ExistLog(int partitionId, long id)
    {
        await Task.CompletedTask;
        
        TryOpenDatabase();
        
        const string query = "SELECT COUNT(*) AS cnt FROM logs WHERE partitionId = @partitionId AND id = @id";
        using SqliteCommand command = new(query, connection);
        
        command.Parameters.AddWithValue("@id", id);
        command.Parameters.AddWithValue("@partitionId", partitionId);
        
        using SqliteDataReader reader = command.ExecuteReader();
        
        while (reader.Read())
            return (reader.IsDBNull(0) ? 0 : reader.GetInt32(0)) > 0;

        return false;
    }

    public async Task Append(int partitionId, RaftLog log)
    {
        await Task.CompletedTask;
        
        TryOpenDatabase();
        
        const string insertQuery = "INSERT INTO logs (id, partitionId, term, type, message, timeLogical, timeCounter) VALUES (@id, @partitionId, @term, @type, @message, @timeLogical, @timeCounter);";
        using SqliteCommand insertCommand =  new(insertQuery, connection);
        
        insertCommand.Parameters.Clear();
        
        insertCommand.Parameters.AddWithValue("@id", log.Id);
        insertCommand.Parameters.AddWithValue("@partitionId", partitionId);
        insertCommand.Parameters.AddWithValue("@term", log.Term);
        insertCommand.Parameters.AddWithValue("@type", log.Type);
        insertCommand.Parameters.AddWithValue("@message", log.Message);
        insertCommand.Parameters.AddWithValue("@timeLogical", log.Time.L);
        insertCommand.Parameters.AddWithValue("@timeCounter", log.Time.C);
        
        insertCommand.ExecuteNonQuery();
    }
    
    public async Task AppendUpdate(int partitionId, RaftLog log)
    {
        await Append(partitionId, log);
    }
    
    public async Task<long> GetMaxLog(int partitionId)
    {
        await Task.CompletedTask;
        
        TryOpenDatabase();
        
        const string query = "SELECT MAX(id) AS max FROM logs WHERE partitionId = @partitionId";
        using SqliteCommand command = new(query, connection);
        
        command.Parameters.AddWithValue("@partitionId", partitionId);
        
        using SqliteDataReader reader = command.ExecuteReader();
        
        while (reader.Read())
            return reader.IsDBNull(0) ? 0 : reader.GetInt64(0);

        return 0;
    }
    
    public async Task<long> GetCurrentTerm(int partitionId)
    {
        await Task.CompletedTask;
        
        TryOpenDatabase();
        
        const string query = "SELECT MAX(term) AS max FROM logs WHERE partitionId = @partitionId";
        using SqliteCommand command = new(query, connection);
        
        command.Parameters.AddWithValue("@partitionId", partitionId);
        
        using SqliteDataReader reader = command.ExecuteReader();
        
        while (reader.Read())
            return reader.IsDBNull(0) ? 0 : reader.GetInt64(0);

        return 0;
    }
}