
using Nixie;
using Kommander.Data;
using Kommander.WAL;

namespace Kommander;

/// <summary>
/// This actor is responsible for controlling concurrency
/// when accessing the replicated log persisted on disk.
/// </summary>
public sealed class RaftWriteAheadActor : IActorStruct<RaftWALRequest, RaftWALResponse>
{
    private const int MaxLogEntries = 10000;

    private readonly RaftManager manager;

    private readonly RaftPartition partition;

    private readonly IWAL walAdapter;

    //private readonly ILogger<IRaft> logger;

    private bool recovered;

    private long commitIndex = 1;

    private ulong operations;

    //private readonly SortedDictionary<ulong, RaftLog> logs = [];

    //private readonly List<ulong> modifications = [];

    public RaftWriteAheadActor(
        IActorContextStruct<RaftWriteAheadActor, RaftWALRequest, RaftWALResponse> _, 
        RaftManager manager, 
        RaftPartition partition,
        IWAL walAdapter
    )
    {
        this.manager = manager;
        this.partition = partition;
        this.walAdapter = walAdapter;
    }

    public async Task<RaftWALResponse> Receive(RaftWALRequest message)
    {
        try
        {
            operations++;

            switch (message.Type)
            {
                case RaftWALActionType.Append:
                    await Append(message.Term, message.Logs);
                    break;

                case RaftWALActionType.AppendCheckpoint:
                    await AppendCheckpoint(message.Term);
                    break;

                case RaftWALActionType.Update:
                    return new(await Update(message.Logs));
                
                case RaftWALActionType.GetRange:
                    return new(await GetRange(message.CurrentIndex));

                case RaftWALActionType.Recover:
                    return new(await Recover());
                
                case RaftWALActionType.GetMaxLog:
                    return new(await GetMaxLog());
                
                case RaftWALActionType.GetCurrentTerm:
                    return new(await GetCurrentTerm());
                
                default:
                    Console.WriteLine("[{0}/{1}] Unknown action type: {2}", manager.LocalEndpoint, partition.PartitionId, message.Type);
                    break;
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine("[{0}/{1}] {2}\n{3}", manager.LocalEndpoint, partition.PartitionId, ex.Message, ex.StackTrace);
        }

        return new();
    }

    private async ValueTask<long> Recover()
    {
        if (recovered)
            return 0;

        recovered = true;

        manager.InvokeRestoreStarted();

        bool found = false;

        await foreach (RaftLog log in walAdapter.ReadLogs(partition.PartitionId))
        {
            found = true;
            commitIndex = log.Id + 1;

            await manager.InvokeReplicationReceived(log.Log);
        }

        if (!found)
            commitIndex = 1;

        manager.InvokeRestoreFinished();

        return commitIndex;
    }

    private async Task Append(long term, List<RaftLog>? appendLogs)
    {
        if (appendLogs is null || appendLogs.Count == 0)
            return;

        foreach (RaftLog log in appendLogs)
        {
            log.Id = commitIndex++;
            log.Term = term;
            
            await walAdapter.Append(partition.PartitionId, log);
        }
    }
    
    private async Task<long> GetMaxLog()
    {
        return await walAdapter.GetMaxLog(partition.PartitionId);
    }
    
    private async Task<long> GetCurrentTerm()
    {
        return await walAdapter.GetCurrentTerm(partition.PartitionId);
    }

    private async Task AppendCheckpoint(long term)
    {
        /*RedisConnection connection = await GetConnection();

        await connection.BasicRetry(async database => await database.KeyDeleteAsync(ClusterWalKeyPrefix + partition.PartitionId));

        RaftLog log = new()
        {
            Id = nextId++,
            Type = RaftLogType.Checkpoint,
            Time = GetCurrentTime()
        };

        logs.Add(log.Id, log);

        AppendLogsRequest request = new(partition.PartitionId, term, RaftManager.LocalEndpoint, [log]);
        string payload = JsonSerializer.Serialize(request, RaftJsonContext.Default.AppendLogsRequest);
        await AppendLogsToNodes(payload);*/

        await Task.CompletedTask;
    }

    private async Task<long> Update(List<RaftLog>? updateLogs)
    {
        if (updateLogs is null)
            return -1;
        
        if (updateLogs.Count == 0)
            return -1;
        
        //Console.WriteLine("Got {0} logs from the leader", updateLogs.Count);

        foreach (RaftLog log in updateLogs)
        {
            if (log.Id != commitIndex)
            {
                Console.WriteLine("[{0}/{1}] Log #{2} is not the expected #{3}", manager.LocalEndpoint, partition.PartitionId, log.Id, commitIndex);
                continue;
            }
            
            //if (await walAdapter.ExistLog(partition.PartitionId, log.Id))
            //    continue;
            
            await walAdapter.AppendUpdate(partition.PartitionId, log);
            
            Console.WriteLine("[{0}/{1}] Applied log #{2}", manager.LocalEndpoint, partition.PartitionId, log.Id);

            await manager.InvokeReplicationReceived(log.Log);

            commitIndex = log.Id + 1;
        }

        //Collect(GetCurrentTime());

        return commitIndex;
    }
    
    private async Task<List<RaftLog>> GetRange(long startLogIndex)
    {
        List<RaftLog> logs = new(8);
        
        await foreach (RaftLog log in walAdapter.ReadLogsRange(partition.PartitionId, startLogIndex))
            logs.Add(log);

        return logs;
    }

    private void Collect(long currentTime)
    {
        /*if (logs.Count < MaxLogEntries)
            return;

        if (operations % 500 != 0)
            return;

        modifications.Clear();

        foreach (KeyValuePair<ulong, RaftLog> keyValue in logs)
        {
            RaftLog raftLog = keyValue.Value;

            if (raftLog.Time > 0 && (currentTime - raftLog.Time) > 1800)
                modifications.Add(keyValue.Key);
        }

        if (modifications.Count == 0)
            return;

        foreach (ulong index in modifications)
            logs.Remove(index);*/
    }

    private async Task Compact(long currentTime)
    {
        if (operations % 500 != 0)
            return;

        /*RedisConnection connection = await GetConnection();

        string key = ClusterWalKeyPrefix + partition.PartitionId;

        long length = await connection.BasicRetry(async database => await database.ListLengthAsync(key));

        if (length < MaxLogEntries)
            return;

        RedisValue[] values = await connection.BasicRetry(async database => await database.ListRangeAsync(ClusterWalKeyPrefix + partition.PartitionId, 0, 1024));

        int oldest = 0;

        foreach (RedisValue value in values)
        {
            byte[]? data = (byte[]?)value;
            if (data is null)
                continue;

            RaftLog? raftLog = MessagePackSerializer.Deserialize<RaftLog>(data);
            if (raftLog is null)
                continue;

            if (raftLog.Time > 0 && (currentTime - raftLog.Time) > 1800)
                oldest++;
        }

        if (oldest == 0)
            return;

        // logger.LogWarning("[{LocalEndpoint}/{PartitionId}] Compacting log at #{Oldest}", RaftManager.LocalEndpoint, partition.PartitionId, oldest);

        await connection.BasicRetry(async database =>
        {
            await database.ListTrimAsync(key, 0, oldest - 1);
            return Task.CompletedTask;
        });*/

        await Task.CompletedTask;
    }
}
