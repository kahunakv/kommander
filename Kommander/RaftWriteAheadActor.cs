
using Nixie;
using Kommander.Data;
using Kommander.Time;
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

    private readonly ILogger<IRaft> logger;

    private bool recovered;

    private long commitIndex = 1;

    private ulong operations;

    public RaftWriteAheadActor(
        IActorContextStruct<RaftWriteAheadActor, RaftWALRequest, RaftWALResponse> _, 
        RaftManager manager, 
        RaftPartition partition,
        IWAL walAdapter
    )
    {
        this.manager = manager;
        this.logger = manager.Logger;
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
                    return new(await Append(message.Term, message.Logs).ConfigureAwait(false));
                
                case RaftWALActionType.Update:
                    return new(await Update(message.Logs).ConfigureAwait(false));

                case RaftWALActionType.AppendCheckpoint:
                    return new(await AppendCheckpoint(message.Term, message.Timestamp).ConfigureAwait(false));
                
                case RaftWALActionType.GetRange:
                    return new(await GetRange(message.CurrentIndex).ConfigureAwait(false));

                case RaftWALActionType.Recover:
                    return new(await Recover().ConfigureAwait(false));
                
                case RaftWALActionType.GetMaxLog:
                    return new(await GetMaxLog().ConfigureAwait(false));
                
                case RaftWALActionType.GetCurrentTerm:
                    return new(await GetCurrentTerm().ConfigureAwait(false));
                
                default:
                    logger.LogError("[{Endpoint}/{PartitionId}] Unknown action type: {Type}", manager.LocalEndpoint, partition.PartitionId, message.Type);
                    break;
            }
        }
        catch (Exception ex)
        {
            logger.LogError("[{Endpoint}/{PartitionId}] {Message}\n{Stacktrace}", manager.LocalEndpoint, partition.PartitionId, ex.Message, ex.StackTrace);
        }

        return new();
    }

    private async ValueTask<long> Recover()
    {
        if (recovered)
            return -1;

        recovered = true;

        manager.InvokeRestoreStarted();

        bool found = false;

        await foreach (RaftLog log in walAdapter.ReadLogs(partition.PartitionId))
        {
            found = true;
            commitIndex = log.Id + 1;

            try
            {
                if (log.Type == RaftLogType.Checkpoint) 
                    continue;
                
                if (!await manager.InvokeReplicationReceived(log.LogType, log.LogData).ConfigureAwait(false))
                    manager.InvokeReplicationError(log);
            }
            catch (Exception ex)
            {
                manager.Logger.LogError("[{Endpoint}/{PartitionId}] {Message}\n{Stacktrace}", manager.LocalEndpoint, partition.PartitionId, ex.Message, ex.StackTrace);
                
                manager.InvokeReplicationError(log);
            }
        }

        if (!found)
            commitIndex = await GetMaxLog() + 1;

        manager.InvokeRestoreFinished();

        return commitIndex;
    }

    private async Task<long> Append(long term, List<RaftLog>? appendLogs)
    {
        if (appendLogs is null || appendLogs.Count == 0)
            return -1;

        foreach (RaftLog log in appendLogs)
        {
            log.Id = commitIndex++;
            log.Term = term;
            
            await walAdapter.Append(partition.PartitionId, log).ConfigureAwait(false);
        }

        return commitIndex;
    }
    
    private async Task<long> GetMaxLog()
    {
        return await walAdapter.GetMaxLog(partition.PartitionId).ConfigureAwait(false);
    }
    
    private async Task<long> GetCurrentTerm()
    {
        return await walAdapter.GetCurrentTerm(partition.PartitionId).ConfigureAwait(false);
    }

    private async Task<long> AppendCheckpoint(long term, HLCTimestamp timestamp)
    {
        RaftLog checkPointLog = new()
        {
            Id = commitIndex++,
            Term = term,
            Type = RaftLogType.Checkpoint,
            Time = timestamp,
            LogType = "",
            LogData = []
        };
        
        await walAdapter.Append(partition.PartitionId, checkPointLog).ConfigureAwait(false);

        return commitIndex;
    }

    private async Task<long> Update(List<RaftLog>? updateLogs)
    {
        if (updateLogs is null)
            return -1;
        
        if (updateLogs.Count == 0)
            return -1;

        foreach (RaftLog log in updateLogs)
        {
            if (log.Id != commitIndex)
            {
                logger.LogWarning("[{Endpoint}/{Partition}] Log #{Id} is not the expected #{CommitIndex}", manager.LocalEndpoint, partition.PartitionId, log.Id, commitIndex);
                continue;
            }
            
            await walAdapter.AppendUpdate(partition.PartitionId, log).ConfigureAwait(false);

            if (log.Type == RaftLogType.Regular)
            {
                logger.LogInformation("[{Endpoint}/{Partition}] Applied log #{Id}", manager.LocalEndpoint, partition.PartitionId, log.Id);
                
                if (!await manager.InvokeReplicationReceived(log.LogType, log.LogData).ConfigureAwait(false))
                    manager.InvokeReplicationError(log);
            }

            if (log.Type == RaftLogType.Checkpoint)
                logger.LogInformation("[{Endpoint}/{Partition}] Applied checkpoint log #{Id}", manager.LocalEndpoint, partition.PartitionId, log.Id);

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
