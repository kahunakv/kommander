using Lux.Data;
using Microsoft.Data.Sqlite;

namespace Lux.WAL;

public class SqliteWAL : IWAL
{
    private readonly Lock _lock = new();
    
    private SqliteConnection? connection;
    
    private void TryOpenDatabase()
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

            const string createTableQuery = "CREATE TABLE IF NOT EXISTS logs (id INT PRIMARY KEY, partitionId INT, type INT, message TEXT COLLATE BINARY, time INT);";
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
        
        const string query = "SELECT id, type, message, time FROM logs ORDER BY id ASC;";
        using SqliteCommand command = new(query, connection);
        using SqliteDataReader reader = command.ExecuteReader();
        
        while (reader.Read())
        {
            yield return new RaftLog
            {
                Id = (ulong)reader.GetInt64(0),
                Type = (RaftLogType)reader.GetInt32(1),
                Message = reader.GetString(2),
                Time = reader.GetInt64(3)
            };
        }
    }

    public async Task Append(int partitionId, RaftLog log)
    {
        await Task.CompletedTask;
        
        TryOpenDatabase();
        
        const string insertQuery = "INSERT INTO storage (id, partitionId, message, time) VALUES (@id, @partitionId, @message, @time);";
        
        using SqliteCommand insertCommand =  new(insertQuery, connection);
        insertCommand.Parameters.Clear();
        insertCommand.Parameters.AddWithValue("@id", log.Id);
        insertCommand.Parameters.AddWithValue("@partitionId", partitionId);
        insertCommand.Parameters.AddWithValue("@message", log.Message);
        insertCommand.Parameters.AddWithValue("@time", log.Time);
        insertCommand.ExecuteNonQuery();
    }
    
    public void Dispose()
    {
        lock (_lock)
        {
            connection?.Dispose();
            connection = null;
        }
    }
}