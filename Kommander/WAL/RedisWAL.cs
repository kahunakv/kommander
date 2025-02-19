
using Kommander.Data;

namespace Kommander.WAL;

public sealed class RedisWAL : IWAL
{
    private const string ClusterWalKeyPrefix = "raft-wal-v2-";

    private const int MaxWalLength = 4096;
    
    public async IAsyncEnumerable<RaftLog> ReadLogs(int partitionId)
    {
        await Task.CompletedTask;
        
        yield return new RaftLog() { };
    }

    public async IAsyncEnumerable<RaftLog> ReadLogsRange(int partitionId, ulong startLogIndex, ulong endLogIndex)
    {
        await Task.CompletedTask;
        
        yield return new RaftLog() { };
    }

    public async Task Append(int partitionId, RaftLog appendLogs)
    {
        //byte[] command = MessagePackSerializer.Serialize(log);
           //string walKey = ClusterWalKeyPrefix + partition.PartitionId;

           //await connection.BasicRetry(async database => await database.ListRightPushAsync(walKey, command));

        await Task.CompletedTask;
    }
    
    public async Task AppendUpdate(int partitionId, RaftLog log)
    {
        await Task.CompletedTask; // do nothing
    }

    public Task<bool> ExistLog(int partitionId, ulong id)
    {
        throw new NotImplementedException();
    }

    private async ValueTask<ulong> Recover()
    {
        await Task.CompletedTask;
        
        /*RedisConnection connection = await GetConnection();

        RedisValue[] values = await connection.BasicRetry(async database => await database.ListRangeAsync(ClusterWalKeyPrefix + partition.PartitionId, -MaxWalLength, -1));

        foreach (RedisValue value in values)
        {
            byte[]? data = (byte[]?)value;
            if (data is null)
                continue;

            RaftLog? log = MessagePackSerializer.Deserialize<RaftLog>(data);
            if (log is null)
                continue;

            if (logs.ContainsKey(log.Id))
            {
                logger.LogInformation("[{LocalEndpoint}/{PartitionId}] Log #{Id} is already in the WAL", RaftManager.LocalEndpoint, partition.PartitionId, log.Id);
                continue;
            }

            if (log.Time == 0 || (log.Time > 0 && (currentTime - log.Time) > 1800))
            {
                nextId = log.Id + 1;
                continue;
            }

            logs.Add(log.Id, log);

            nextId = log.Id + 1;

            await manager.InvokeReplicationReceived(log.Message);
        }

        if (values.Length == 0)
            nextId = 1;

        manager.InvokeRestoreFinished();

        //logger.LogInformation("[{LocalEndpoint}/{PartitionId}] WAL restored at #{NextId} in {Elapsed}ms", RaftManager.LocalEndpoint, partition.PartitionId, nextId - 1, stopWatch.GetElapsedMilliseconds());

        //Sender.Tell(nextId);*/

        return 0; 
    }
}