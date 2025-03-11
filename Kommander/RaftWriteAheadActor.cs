
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
    private readonly RaftManager manager;

    private readonly RaftPartition partition;

    private readonly IWAL walAdapter;

    private readonly ILogger<IRaft> logger;

    private bool recovered;
    
    private long proposeIndex = 1;

    private long commitIndex = 1;

    private long operations;

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
                case RaftWALActionType.Propose:
                {
                    (RaftOperationStatus status, long newProposedIndex) = await Propose(message.Term, message.Logs).ConfigureAwait(false);
                    return new(status, newProposedIndex);
                }

                case RaftWALActionType.Commit:
                {
                    (RaftOperationStatus status, long newCommitIndex) = await Commit(message.Logs).ConfigureAwait(false);
                    return new(status, newCommitIndex);
                }

                case RaftWALActionType.ProposeOrCommit:
                    return new(RaftOperationStatus.Success, await ProposeOrCommit(message.Logs).ConfigureAwait(false));
                
                case RaftWALActionType.GetRange:
                    return new(RaftOperationStatus.Success, await GetRange(message.CurrentIndex).ConfigureAwait(false));

                case RaftWALActionType.Recover:
                    return new(RaftOperationStatus.Success, await Recover().ConfigureAwait(false));
                
                case RaftWALActionType.GetMaxLog:
                    return new(RaftOperationStatus.Success, await GetMaxLog().ConfigureAwait(false));
                
                case RaftWALActionType.GetCurrentTerm:
                    return new(RaftOperationStatus.Success, await GetCurrentTerm().ConfigureAwait(false));
                
                default:
                    logger.LogError("[{Endpoint}/{PartitionId}] Unknown action type: {Type}", manager.LocalEndpoint, partition.PartitionId, message.Type);
                    break;
            }
        }
        catch (Exception ex)
        {
            logger.LogError("[{Endpoint}/{PartitionId}] {Message}\n{Stacktrace}", manager.LocalEndpoint, partition.PartitionId, ex.Message, ex.StackTrace);
        }

        return new(RaftOperationStatus.Errored, -1);
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

            try
            {
                switch (log.Type)
                {
                    case RaftLogType.ProposedCheckpoint:
                    case RaftLogType.Proposed:
                        continue;
                    
                    case RaftLogType.Committed:
                    case RaftLogType.CommittedCheckpoint:
                        commitIndex = log.Id + 1;
                        proposeIndex = log.Id + 1;
                        break;
                }

                if (log.Type != RaftLogType.Committed)
                    continue;
                
                if (!await manager.InvokeReplicationReceived(log).ConfigureAwait(false))
                    manager.InvokeReplicationError(log);
            }
            catch (Exception ex)
            {
                manager.Logger.LogError("[{Endpoint}/{PartitionId}] {Message}\n{Stacktrace}", manager.LocalEndpoint, partition.PartitionId, ex.Message, ex.StackTrace);
                
                manager.InvokeReplicationError(log);
            }
        }

        if (!found)
            commitIndex = await GetMaxLog().ConfigureAwait(false) + 1;

        manager.InvokeRestoreFinished();

        return commitIndex;
    }

    private async Task<(RaftOperationStatus, long)> Propose(long term, List<RaftLog>? logs)
    {
        if (logs is null || logs.Count == 0)
            return (RaftOperationStatus.Success, -1);
        
        RaftLog[] orderedLogs = logs.OrderBy(log => log.Id).ToArray();

        foreach (RaftLog log in orderedLogs)
        {
            log.Id = proposeIndex++;
            log.Term = term;
            
            await walAdapter.Propose(partition.PartitionId, log).ConfigureAwait(false);
        }

        return (RaftOperationStatus.Success, proposeIndex);
    }
    
    private async Task<(RaftOperationStatus, long)> Commit(List<RaftLog>? logs)
    {
        if (logs is null || logs.Count == 0)
            return (RaftOperationStatus.Success, -1);

        long lastCommitIndex = -1;
        RaftLog[] orderedLogs = logs.OrderBy(log => log.Id).ToArray();

        foreach (RaftLog log in orderedLogs)
        {
            switch (log.Type)
            {
                case RaftLogType.Proposed:
                    log.Type = RaftLogType.Committed;
                    commitIndex = log.Id + 1;
                    lastCommitIndex = log.Id;
                
                    await walAdapter.Commit(partition.PartitionId, log).ConfigureAwait(false);
                    break;
                
                case RaftLogType.ProposedCheckpoint:
                    log.Type = RaftLogType.CommittedCheckpoint;
                    commitIndex = log.Id + 1;
                    lastCommitIndex = log.Id;
                
                    await walAdapter.Commit(partition.PartitionId, log).ConfigureAwait(false);
                    break;
            }
        }

        return (RaftOperationStatus.Success, lastCommitIndex);
    }
    
    private async Task<long> GetMaxLog()
    {
        return await walAdapter.GetMaxLog(partition.PartitionId).ConfigureAwait(false);
    }
    
    private async Task<long> GetCurrentTerm()
    {
        return await walAdapter.GetCurrentTerm(partition.PartitionId).ConfigureAwait(false);
    }

    private async Task<long> ProposeOrCommit(List<RaftLog>? logs)
    {
        if (logs is null || logs.Count == 0)
            return -1;

        bool allOutdated = true;
        
        RaftLog[] orderedLogs = logs.OrderBy(log => log.Id).ToArray();

        foreach (RaftLog log in orderedLogs)
        {
            switch (log.Type)
            {
                case RaftLogType.Proposed or RaftLogType.ProposedCheckpoint when log.Id < (proposeIndex - 1): 
                    /*logger.LogWarning(
                        "[{Endpoint}/{Partition}] Proposed log #{Id} is not the expected #{ProposeIndex}",
                        manager.LocalEndpoint, 
                        partition.PartitionId, 
                        log.Id, 
                        proposeIndex
                    );*/
                    break;
                
                case RaftLogType.Committed or RaftLogType.CommittedCheckpoint when log.Id < (commitIndex - 1):
                    /*logger.LogWarning(
                        "[{Endpoint}/{Partition}] Committed log #{Id} is not the expected #{CommitIndex}",
                        manager.LocalEndpoint, 
                        partition.PartitionId, 
                        log.Id, 
                        commitIndex
                    );*/
                    break;
                
                default:
                    allOutdated = false;
                    break;
            }
        }

        if (allOutdated)
        {
            logger.LogWarning(
                "[{Endpoint}/{Partition}] All replicated indexes are included already in the log Min={Min} Max={Max}",
                manager.LocalEndpoint, 
                partition.PartitionId,
                logs.Min(log => log.Id),
                logs.Max(log => log.Id)
            );
            
            return Math.Max(proposeIndex, commitIndex);
        }

        foreach (RaftLog log in orderedLogs)
        {
            switch (log.Type)
            {
                case RaftLogType.Proposed when log.Id >= proposeIndex:
                    await walAdapter.Propose(partition.PartitionId, log).ConfigureAwait(false);
                
                    logger.LogDebug("[{Endpoint}/{Partition}] Proposed log #{Id}", manager.LocalEndpoint, partition.PartitionId, log.Id);

                    proposeIndex = log.Id + 1;
                    break;
                
                case RaftLogType.Committed when log.Id >= commitIndex:
                {
                    await walAdapter.Commit(partition.PartitionId, log).ConfigureAwait(false);
                
                    logger.LogDebug("[{Endpoint}/{Partition}] Committed log #{Id}", manager.LocalEndpoint, partition.PartitionId, log.Id);
                
                    if (!await manager.InvokeReplicationReceived(log).ConfigureAwait(false))
                        manager.InvokeReplicationError(log);
                    
                    commitIndex = log.Id + 1;
                    break;
                }
                
                case RaftLogType.ProposedCheckpoint when log.Id >= proposeIndex:
                    await walAdapter.Propose(partition.PartitionId, log).ConfigureAwait(false);
                
                    logger.LogDebug("[{Endpoint}/{Partition}] Proposed checkpoint log #{Id}", manager.LocalEndpoint, partition.PartitionId, log.Id);
                    
                    proposeIndex = log.Id + 1;
                    break;
                
                case RaftLogType.CommittedCheckpoint when log.Id >= commitIndex:
                    await walAdapter.Commit(partition.PartitionId, log).ConfigureAwait(false);
                
                    logger.LogDebug("[{Endpoint}/{Partition}] Committed checkpoint log #{Id}", manager.LocalEndpoint, partition.PartitionId, log.Id);
                    
                    commitIndex = log.Id + 1;
                    break;
            }
        }

        return Math.Max(proposeIndex, commitIndex);
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
