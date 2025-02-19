using Lux.Data;
using Microsoft.Data.Sqlite;

namespace Lux.WAL;

public class SqliteWAL : IWAL
{
    private static readonly Lock _lock = new();
    
    private static SqliteConnection? connection;
    
    private static void TryOpenDatabase()
    {
        if (connection is not null)
            return;

        lock (_lock)
        {
            if (connection is not null)
                return;

            string path = ".";
            
            string connectionString = $"Data Source={path}/database.db";
            connection = new(connectionString);

            connection.Open();

            const string createTableQuery = "CREATE TABLE IF NOT EXISTS logs (id INT, partitionId INT, type INT, message TEXT COLLATE BINARY, time INT) PRIMARY KEY(id, partitionId);";
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
        
        const string query = "SELECT id, type, message, time FROM logs WHERE partitionId = @partitionId ORDER BY id ASC;";
        using SqliteCommand command = new(query, connection);
        using SqliteDataReader reader = command.ExecuteReader();
        
        while (reader.Read())
        {
            yield return new()
            {
                Id = reader.IsDBNull(0) ? 0 : (ulong)reader.GetInt64(0),
                Type = reader.IsDBNull(1) ? RaftLogType.Regular : (RaftLogType)reader.GetInt32(1),
                Message = reader.IsDBNull(2) ? "" : reader.GetString(2),
                Time = reader.IsDBNull(3) ? 0 : reader.GetInt64(3)
            };
        }
    }
    
    public async Task<bool> ExistLog(int partitionId, ulong id)
    {
        await Task.CompletedTask;
        
        TryOpenDatabase();
        
        const string query = "SELECT COUNT(*) AS cnt FROM logs WHERE partition = @partitionId AND id = @id";
        
        using SqliteCommand command = new(query, connection);
        
        command.Parameters.AddWithValue("@id", id);
        command.Parameters.AddWithValue("@partitionId", partitionId);
        
        using SqliteDataReader reader = command.ExecuteReader();
        
        while (reader.Read())
        {
            return (reader.IsDBNull(0) ? 0 : reader.GetInt32(0)) > 0;
        }

        return false;
    }

    public async Task Append(int partitionId, RaftLog log)
    {
        await Task.CompletedTask;
        
        TryOpenDatabase();
        
        const string insertQuery = "INSERT INTO logs (id, partitionId, type, message, time) VALUES (@id, @partitionId, @type, @message, @time);";
        
        using SqliteCommand insertCommand =  new(insertQuery, connection);
        insertCommand.Parameters.Clear();
        insertCommand.Parameters.AddWithValue("@id", log.Id);
        insertCommand.Parameters.AddWithValue("@partitionId", partitionId);
        insertCommand.Parameters.AddWithValue("@type", log.Type);
        insertCommand.Parameters.AddWithValue("@message", log.Message);
        insertCommand.Parameters.AddWithValue("@time", log.Time);
        insertCommand.ExecuteNonQuery();
    }
    
    public async Task AppendUpdate(int partitionId, RaftLog log)
    {
        await Append(partitionId, log);
    }
}