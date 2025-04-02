
using Nixie;
using Kommander.Data;
using Kommander.System;
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
    
    private readonly Dictionary<RaftLogAction, List<RaftLog>> plan = new();
    
    private readonly int compactEveryOperations;
    
    private readonly int compactNumberEntries;

    private bool recovered;
    
    private long proposeIndex = 1;

    private long commitIndex = 1;

    private int operations;

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
        
        this.compactEveryOperations = manager.Configuration.CompactEveryOperations;
        this.compactNumberEntries = manager.Configuration.CompactNumberEntries;
        this.operations = compactEveryOperations;
    }

    public async Task<RaftWALResponse> Receive(RaftWALRequest message)
    {
        try
        {
            if (--operations == 0)
            {
                await Compact();
                
                operations = compactEveryOperations;
            }

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
                
                case RaftWALActionType.Rollback:
                {
                    (RaftOperationStatus status, long newCommitIndex) = await Rollback(message.Logs).ConfigureAwait(false);
                    return new(status, newCommitIndex);
                }

                case RaftWALActionType.ProposeOrCommit:
                {
                    (RaftOperationStatus status, long newCommitIndex) = await ProposeOrCommit(message.Logs).ConfigureAwait(false);
                    return new(status, newCommitIndex);
                }
                
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

        manager.InvokeRestoreStarted(partition.PartitionId);

        bool found = false;

        List<RaftLog> logs = await manager.ReadThreadPool.EnqueueTask(() => walAdapter.ReadLogs(partition.PartitionId));

        foreach (RaftLog log in logs)
        {
            found = true;

            try
            {
                switch (log.Type)
                {
                    case RaftLogType.ProposedCheckpoint:
                    case RaftLogType.Proposed:
                    case RaftLogType.RolledBack:
                    case RaftLogType.RolledBackCheckpoint:
                        continue;
                    
                    case RaftLogType.Committed:
                    case RaftLogType.CommittedCheckpoint:
                        commitIndex = log.Id + 1;
                        proposeIndex = log.Id + 1;
                        break;
                    
                    default:
                        throw new NotImplementedException();
                }

                if (log.Type != RaftLogType.Committed)
                    continue;

                if (partition.PartitionId == RaftSystemConfig.SystemPartition)
                {
                    if (!await manager.InvokeSystemLogRestored(partition.PartitionId, log).ConfigureAwait(false))
                        manager.InvokeReplicationError(partition.PartitionId, log);
                }
                else
                {
                    if (!await manager.InvokeLogRestored(partition.PartitionId, log).ConfigureAwait(false))
                        manager.InvokeReplicationError(partition.PartitionId, log);
                }
            }
            catch (Exception ex)
            {
                manager.Logger.LogError("[{Endpoint}/{PartitionId}] {Message}\n{Stacktrace}", manager.LocalEndpoint, partition.PartitionId, ex.Message, ex.StackTrace);
                
                manager.InvokeReplicationError(partition.PartitionId, log);
            }
        }

        if (!found)
            commitIndex = await GetMaxLog().ConfigureAwait(false) + 1;

        if (partition.PartitionId == RaftSystemConfig.SystemPartition)
            manager.InvokeSystemRestoreFinished(partition.PartitionId);
        else
            manager.InvokeRestoreFinished(partition.PartitionId);

        return commitIndex;
    }

    private async Task<(RaftOperationStatus, long)> Propose(long term, List<RaftLog>? logs)
    {
        if (logs is null || logs.Count == 0)
            return (RaftOperationStatus.Success, -1);

        foreach (RaftLog log in logs.OrderBy(log => log.Id))
        {
            log.Id = proposeIndex++;
            log.Term = term;
            
            RaftOperationStatus status = await manager.WriteThreadPool.EnqueueTask(() => walAdapter.Propose(partition.PartitionId, log));
            if (status != RaftOperationStatus.Success)
                return (status, -1);
        }

        return (RaftOperationStatus.Success, proposeIndex);
    }
    
    private async Task<(RaftOperationStatus, long)> Commit(List<RaftLog>? logs)
    {
        if (logs is null || logs.Count == 0)
            return (RaftOperationStatus.Success, -1);

        long lastCommitIndex = -1;

        foreach (RaftLog log in logs.OrderBy(log => log.Id))
        {
            switch (log.Type)
            {
                case RaftLogType.Proposed:
                {
                    log.Type = RaftLogType.Committed;

                    RaftOperationStatus status = await manager.WriteThreadPool.EnqueueTask(() => walAdapter.Commit(partition.PartitionId, log));
                    if (status != RaftOperationStatus.Success)
                        return (status, -1);

                    commitIndex = log.Id + 1;
                    lastCommitIndex = log.Id;
                }
                break;

                case RaftLogType.ProposedCheckpoint:
                {
                    log.Type = RaftLogType.CommittedCheckpoint;

                    RaftOperationStatus status = await manager.WriteThreadPool.EnqueueTask(() => walAdapter.Commit(partition.PartitionId, log));
                    if (status != RaftOperationStatus.Success)
                        return (status, -1);

                    commitIndex = log.Id + 1;
                    lastCommitIndex = log.Id;
                }
                break;

                case RaftLogType.Committed:
                case RaftLogType.CommittedCheckpoint:
                case RaftLogType.RolledBack:
                case RaftLogType.RolledBackCheckpoint:
                default:
                    break;
            }
        }

        return (RaftOperationStatus.Success, lastCommitIndex);
    }
    
    private async Task<(RaftOperationStatus, long)> Rollback(List<RaftLog>? logs)
    {
        if (logs is null || logs.Count == 0)
            return (RaftOperationStatus.Success, -1);

        foreach (RaftLog log in logs.OrderBy(log => log.Id))
        {
            switch (log.Type)
            {
                case RaftLogType.Proposed:
                {
                    log.Type = RaftLogType.RolledBack;

                    RaftOperationStatus status = await manager.WriteThreadPool.EnqueueTask(() => walAdapter.Rollback(partition.PartitionId, log));
                    if (status != RaftOperationStatus.Success)
                        return (status, -1);
                }
                break;

                case RaftLogType.ProposedCheckpoint:
                {
                    log.Type = RaftLogType.RolledBackCheckpoint;

                    RaftOperationStatus status = await manager.WriteThreadPool.EnqueueTask(() => walAdapter.Rollback(partition.PartitionId, log));
                    if (status != RaftOperationStatus.Success)
                        return (status, -1);
                }
                break;
            }
        }

        return (RaftOperationStatus.Success, -1);
    }
    
    private async Task<long> GetMaxLog()
    {
        return await manager.ReadThreadPool.EnqueueTask(() => walAdapter.GetMaxLog(partition.PartitionId));
    }
    
    private async Task<long> GetCurrentTerm()
    {
        return await manager.ReadThreadPool.EnqueueTask(() => walAdapter.GetCurrentTerm(partition.PartitionId));
    }

    private async Task<(RaftOperationStatus, long)> ProposeOrCommit(List<RaftLog>? logs)
    {
        if (logs is null || logs.Count == 0)
            return (RaftOperationStatus.Success, -1);

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
            /*logger.LogWarning(
                "[{Endpoint}/{Partition}] All replicated indexes are included already in the log Min={Min} Max={Max}",
                manager.LocalEndpoint, 
                partition.PartitionId,
                logs.Min(log => log.Id),
                logs.Max(log => log.Id)
            );
            
            return (RaftOperationStatus.Success, Math.Min(proposeIndex, commitIndex));*/
        }
        
        plan.Clear();

        foreach (RaftLog log in orderedLogs)
        {
            switch (log.Type)
            {
                case RaftLogType.Proposed: /* when log.Id >= proposeIndex: */
                {
                    if (plan.TryGetValue(RaftLogAction.Propose, out List<RaftLog>? proposeActions))
                        proposeActions.Add(log);
                    else
                        plan.Add(RaftLogAction.Propose, [log]);

                    logger.LogDebug("[{Endpoint}/{Partition}] Proposed log #{Id}", manager.LocalEndpoint, partition.PartitionId, log.Id);

                    proposeIndex = log.Id + 1;
                }
                break;

                case RaftLogType.RolledBack: /* when log.Id >= proposeIndex: */
                {
                    if (plan.TryGetValue(RaftLogAction.Rollback, out List<RaftLog>? rollbackActions))
                        rollbackActions.Add(log);
                    else
                        plan.Add(RaftLogAction.Rollback, [log]);

                    logger.LogDebug("[{Endpoint}/{Partition}] Rolledback log #{Id}", manager.LocalEndpoint, partition.PartitionId, log.Id);

                    commitIndex = log.Id + 1;
                }
                break;    

                case RaftLogType.Committed: /* when log.Id >= commitIndex: */
                {
                    if (plan.TryGetValue(RaftLogAction.Commit, out List<RaftLog>? commitActions))
                        commitActions.Add(log);
                    else
                        plan.Add(RaftLogAction.Commit, [log]);
                
                    logger.LogDebug("[{Endpoint}/{Partition}] Committed log #{Id}", manager.LocalEndpoint, partition.PartitionId, log.Id);
                    
                    commitIndex = log.Id + 1;
                }
                break;    

                case RaftLogType.ProposedCheckpoint: /* when log.Id >= proposeIndex: */
                {
                    if (plan.TryGetValue(RaftLogAction.Propose, out List<RaftLog>? proposeActions))
                        proposeActions.Add(log);
                    else
                        plan.Add(RaftLogAction.Propose, [log]);

                    logger.LogDebug("[{Endpoint}/{Partition}] Proposed checkpoint log #{Id}", manager.LocalEndpoint, partition.PartitionId, log.Id);

                    proposeIndex = log.Id + 1;
                } 
                break;

                case RaftLogType.RolledBackCheckpoint: /* when log.Id >= commitIndex: */
                {
                    if (plan.TryGetValue(RaftLogAction.Rollback, out List<RaftLog>? rollbackActions))
                        rollbackActions.Add(log);
                    else
                        plan.Add(RaftLogAction.Rollback, [log]);

                    logger.LogDebug("[{Endpoint}/{Partition}] Rolled back checkpoint log #{Id}", manager.LocalEndpoint, partition.PartitionId, log.Id);

                    commitIndex = log.Id + 1;
                } 
                break;

                case RaftLogType.CommittedCheckpoint: /* when log.Id >= commitIndex:*/
                {
                    if (plan.TryGetValue(RaftLogAction.Commit, out List<RaftLog>? commitActions))
                        commitActions.Add(log);
                    else
                        plan.Add(RaftLogAction.Commit, [log]);

                    logger.LogDebug("[{Endpoint}/{Partition}] Committed checkpoint log #{Id}", manager.LocalEndpoint, partition.PartitionId, log.Id);

                    commitIndex = log.Id + 1;
                } 
                break;

                default:
                    break;
            }
        }

        RaftOperationStatus status = RaftOperationStatus.Success;
        
        foreach (KeyValuePair<RaftLogAction, List<RaftLog>> keyValue in plan)
        {
            switch (keyValue.Key)
            {
                case RaftLogAction.Propose:
                    status = await manager.WriteThreadPool.EnqueueTask(() => walAdapter.ProposeMany(partition.PartitionId, keyValue.Value)).ConfigureAwait(false);
                    break;
                
                case RaftLogAction.Commit:
                    status = await manager.WriteThreadPool.EnqueueTask(() => walAdapter.CommitMany(partition.PartitionId, keyValue.Value)).ConfigureAwait(false);
                    break;
                
                case RaftLogAction.Rollback:
                    status = await manager.WriteThreadPool.EnqueueTask(() => walAdapter.RollbackMany(partition.PartitionId, keyValue.Value)).ConfigureAwait(false);
                    break;
                
                default:
                    throw new NotImplementedException();
            }

            if (status != RaftOperationStatus.Success)
                return (status, Math.Max(proposeIndex, commitIndex));
        }

        if (plan.TryGetValue(RaftLogAction.Commit, out List<RaftLog>? actions))
        {
            foreach (RaftLog log in actions)
            {
                if (log.Type != RaftLogType.Committed)
                    continue;

                if (partition.PartitionId == RaftSystemConfig.SystemPartition)
                {
                    if (!await manager.InvokeSystemReplicationReceived(partition.PartitionId, log).ConfigureAwait(false))
                        manager.InvokeReplicationError(partition.PartitionId, log);
                }
                else
                {
                    if (!await manager.InvokeReplicationReceived(partition.PartitionId, log).ConfigureAwait(false))
                        manager.InvokeReplicationError(partition.PartitionId, log);
                }
            }
        }

        return (status, Math.Max(proposeIndex, commitIndex));
    }
    
    private async Task<List<RaftLog>> GetRange(long startLogIndex)
    {
        return await manager.ReadThreadPool.EnqueueTask(() => walAdapter.ReadLogsRange(partition.PartitionId, startLogIndex));
    }

    private async Task Compact()
    {
        long lastCheckpoint = await manager.ReadThreadPool.EnqueueTask(() => walAdapter.GetLastCheckpoint(partition.PartitionId));

        if (lastCheckpoint <= 0)
            return;
        
        logger.LogInformation("[{Endpoint}/{Partition}] Compaction process started LastCheckpoint={LastCheckpoint}", manager.LocalEndpoint, partition.PartitionId, lastCheckpoint);
        
        await manager.WriteThreadPool.EnqueueTask(() =>
            walAdapter.CompactLogsOlderThan(partition.PartitionId, lastCheckpoint, compactNumberEntries
        ));
    }
}
