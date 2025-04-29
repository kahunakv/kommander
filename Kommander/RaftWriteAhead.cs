
using System.Diagnostics;

using Kommander.Data;
using Kommander.Logging;
using Kommander.Support.Collections;
using Kommander.System;
using Kommander.WAL;
using Microsoft.Extensions.Logging;

namespace Kommander;

/// <summary>
/// Manages the write-ahead log (WAL) for a Raft partition. Provides functionality for
/// recovering logs, proposing new operations, committing or rolling back changes,
/// and managing Raft log compaction.
/// </summary>
public sealed class RaftWriteAhead
{
    private readonly RaftManager manager;

    private readonly RaftPartition partition;

    private readonly IWAL walAdapter;

    private readonly ILogger<IRaft> logger;
    
    private readonly SmallDictionary<RaftLogAction, List<RaftLog>> plan = new(3);
    
    private readonly int compactEveryOperations;
    
    private readonly int compactNumberEntries;

    private bool recovered;
    
    private long proposeIndex = 1;

    private long commitIndex = 1;

    private int operations;
        
    private readonly Stopwatch stopwatch = Stopwatch.StartNew();

    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="manager"></param>
    /// <param name="partition"></param>
    /// <param name="walAdapter"></param>
    public RaftWriteAhead(RaftManager manager, RaftPartition partition, IWAL walAdapter)
    {
        this.manager = manager;
        this.logger = manager.Logger;
        this.partition = partition;
        this.walAdapter = walAdapter;
        
        this.compactEveryOperations = manager.Configuration.CompactEveryOperations;
        this.compactNumberEntries = manager.Configuration.CompactNumberEntries;
        this.operations = compactEveryOperations;
    }

    /// <summary>
    /// Recovers the state of the Raft log for a specific partition by reading persisted logs from the Write-Ahead Log (WAL).
    /// This method ensures recovery is only executed once and updates the commit index upon completion.
    /// </summary>
    /// <returns>
    /// The updated commit index after recovery is completed. Returns -1 if the recovery has already been performed.
    /// </returns>
    /// <exception cref="Exception">
    /// Thrown if an error occurs during log recovery or processing.
    /// </exception>
    public async ValueTask<long> Recover()
    {
        if (recovered)
            return -1;

        recovered = true;

        manager.InvokeRestoreStarted(partition.PartitionId);

        bool found = false;

        List<RaftLog> logs = await manager.ReadThreadPool.EnqueueTask(() => walAdapter.ReadLogs(partition.PartitionId));
        
        if (logs.Count > 0)
            manager.Logger.LogInformation("[{Endpoint}/{Partition}] Recovered {LogsCount} logs", manager.LocalEndpoint, partition.PartitionId, logs.Count);

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

    /// <summary>
    /// Proposes a batch of logs in the current term for processing by the Raft consensus protocol.
    /// Logs are assigned unique indices and associated with the current term, then enqueued for replication.
    /// </summary>
    /// <param name="term">
    /// The current term in the Raft consensus protocol used to associate with the logs.
    /// </param>
    /// <param name="logs">
    /// A list of logs to be proposed. If the list is null or empty, the method will return immediately with a success status and no index update.
    /// </param>
    /// <returns>
    /// A tuple containing the operation status and the index of the last proposed log.
    /// If the proposal succeeds, the status will be <see cref="RaftOperationStatus.Success"/> and the index will reflect the latest proposed index.
    /// If the operation fails, the status will indicate the specific error, and the index will return as -1.
    /// </returns>
    /// <exception cref="Exception">
    /// May be thrown for unexpected errors during the proposal process or queuing for replication.
    /// </exception>
    public async Task<(RaftOperationStatus, long)> Propose(long term, List<RaftLog>? logs)
    {
        if (logs is null || logs.Count == 0)
            return (RaftOperationStatus.Success, -1);

        foreach (RaftLog log in logs.OrderBy(log => log.Id))
        {
            log.Id = proposeIndex++;
            log.Term = term;
            
            //RaftOperationStatus status = await manager.WriteThreadPool.EnqueueTask(() => walAdapter.Propose(partition.PartitionId, log));
        }
        
        RaftOperationStatus status = await manager.RaftBatcher.Enqueue((partition.PartitionId, logs)).ConfigureAwait(false);
            
        if (status != RaftOperationStatus.Success)
            return (status, -1);

        return (RaftOperationStatus.Success, proposeIndex);
    }

    /// <summary>
    /// Commits a list of Raft log entries by updating their type to indicate they are committed.
    /// Processes the logs in ascending order of their IDs and updates the commit index.
    /// </summary>
    /// <param name="logs">
    /// A list of Raft log entries to commit. If null or empty, the method returns success with a commit index of -1.
    /// </param>
    /// <returns>
    /// A tuple containing the operation status and the last committed log index.
    /// The operation status is an instance of <see cref="RaftOperationStatus"/> indicating success or failure.
    /// If the operation fails, the commit index will be -1.
    /// </returns>
    public async Task<(RaftOperationStatus, long)> Commit(List<RaftLog>? logs)
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

                    //RaftOperationStatus status = await manager.WriteThreadPool.EnqueueTask(() => walAdapter.Commit(partition.PartitionId, log));

                    commitIndex = log.Id + 1;
                    lastCommitIndex = log.Id;
                }
                break;

                case RaftLogType.ProposedCheckpoint:
                {
                    log.Type = RaftLogType.CommittedCheckpoint;

                    //RaftOperationStatus status = await manager.WriteThreadPool.EnqueueTask(() => walAdapter.Commit(partition.PartitionId, log));

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
        
        RaftOperationStatus status = await manager.RaftBatcher.Enqueue((partition.PartitionId, logs)).ConfigureAwait(false);
                    
        if (status != RaftOperationStatus.Success)
            return (status, -1);

        return (RaftOperationStatus.Success, lastCommitIndex);
    }

    /// <summary>
    /// Rolls back a list of Raft logs by updating their types to indicate a rollback operation
    /// and processing them through the Write-Ahead Log (WAL) adapter.
    /// </summary>
    /// <param name="logs">
    /// A list of Raft logs to be rolled back. If the list is null or empty, no rollback is performed.
    /// </param>
    /// <returns>
    /// A tuple containing the operation status and the index of the last processed log:
    /// - <see cref="RaftOperationStatus.Success"/> and -1 if the rollback is completed successfully or no logs were provided.
    /// - The relevant <see cref="RaftOperationStatus"/> and -1 in case of an error.
    /// </returns>
    public async Task<(RaftOperationStatus, long)> Rollback(List<RaftLog>? logs)
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

                    //RaftOperationStatus status = await manager.WriteThreadPool.EnqueueTask(() => walAdapter.Rollback(partition.PartitionId, log));
                }
                break;

                case RaftLogType.ProposedCheckpoint:
                {
                    log.Type = RaftLogType.RolledBackCheckpoint;

                    //RaftOperationStatus status = await manager.WriteThreadPool.EnqueueTask(() => walAdapter.Rollback(partition.PartitionId, log));
                }
                break;
            }
        }
        
        RaftOperationStatus status = await manager.RaftBatcher.Enqueue((partition.PartitionId, logs)).ConfigureAwait(false);
                    
        if (status != RaftOperationStatus.Success)
            return (status, -1);

        return (RaftOperationStatus.Success, -1);
    }

    /// <summary>
    /// Retrieves the highest log index recorded in the Write-Ahead Log (WAL) for a specific partition.
    /// This method queries the WAL adapter and returns the maximum log index for the partition.
    /// </summary>
    /// <returns>
    /// The maximum log index currently recorded for the specified partition.
    /// </returns>
    public async Task<long> GetMaxLog()
    {
        return await manager.ReadThreadPool.EnqueueTask(() => walAdapter.GetMaxLog(partition.PartitionId));
    }

    /// <summary>
    /// Retrieves the current term of the Raft log for the specified partition.
    /// This term represents the latest term recognized by the Write-Ahead Log (WAL).
    /// </summary>
    /// <returns>
    /// A task that represents the asynchronous operation. The task result contains the current term of the Raft log.
    /// </returns>
    public async Task<long> GetCurrentTerm()
    {
        return await manager.ReadThreadPool.EnqueueTask(() => walAdapter.GetCurrentTerm(partition.PartitionId));
    }

    /// <summary>
    /// Processes a list of Raft log entries by proposing or committing them based on their type and ID.
    /// This method validates the logs, ensures ordering, handles outdated logs, and performs necessary actions
    /// such as proposing, committing, or skipping logs as required. This is typically used by replica nodes.
    /// </summary>
    /// <param name="logs">
    /// A list of Raft log entries to be processed. The logs can be of various types, including proposed or committed logs.
    /// If the list is null or empty, no operations are performed, and a success status with an index of -1 is returned.
    /// </param>
    /// <returns>
    /// A tuple containing the operation status and the highest index reached during the process.
    /// The operation status indicates whether the process succeeded, encountered errors, or other specific conditions.
    /// The index represents the maximum of the propose or commit index after processing.
    /// </returns>
    /// <exception cref="NotImplementedException">
    /// Thrown if execution reaches functionality that has not yet been implemented.
    /// </exception>
    public async Task<(RaftOperationStatus, long)> ProposeOrCommit(List<RaftLog>? logs)
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
        
        // Reuse internal lists
        foreach (KeyValuePair<RaftLogAction, List<RaftLog>> keyValue in plan)
            keyValue.Value.Clear();

        foreach (RaftLog log in orderedLogs)
        {
            switch (log.Type)
            {
                case RaftLogType.Proposed: /* when log.Id >= proposeIndex: */
                {
                    if (plan.TryGetValue(RaftLogAction.Propose, out List<RaftLog> proposeActions))
                        proposeActions.Add(log);
                    else
                        plan.Add(RaftLogAction.Propose, [log]);

                    logger.LogDebugProposedLogs(manager.LocalEndpoint, partition.PartitionId, log.Id);

                    proposeIndex = log.Id + 1;
                }
                break;

                case RaftLogType.RolledBack: /* when log.Id >= proposeIndex: */
                {
                    if (plan.TryGetValue(RaftLogAction.Rollback, out List<RaftLog> rollbackActions))
                        rollbackActions.Add(log);
                    else
                        plan.Add(RaftLogAction.Rollback, [log]);

                    logger.LogDebug("[{Endpoint}/{Partition}] Rolledback log #{Id}", manager.LocalEndpoint, partition.PartitionId, log.Id);

                    commitIndex = log.Id + 1;
                }
                break;    

                case RaftLogType.Committed: /* when log.Id >= commitIndex: */
                {
                    if (plan.TryGetValue(RaftLogAction.Commit, out List<RaftLog> commitActions))
                        commitActions.Add(log);
                    else
                        plan.Add(RaftLogAction.Commit, [log]);
                
                    logger.LogDebugCommittedLogs(manager.LocalEndpoint, partition.PartitionId, log.Id);
                    
                    commitIndex = log.Id + 1;
                }
                break;    

                case RaftLogType.ProposedCheckpoint: /* when log.Id >= proposeIndex: */
                {
                    if (plan.TryGetValue(RaftLogAction.Propose, out List<RaftLog> proposeActions))
                        proposeActions.Add(log);
                    else
                        plan.Add(RaftLogAction.Propose, [log]);

                    logger.LogDebug("[{Endpoint}/{Partition}] Proposed checkpoint log #{Id}", manager.LocalEndpoint, partition.PartitionId, log.Id);

                    proposeIndex = log.Id + 1;
                } 
                break;

                case RaftLogType.RolledBackCheckpoint: /* when log.Id >= commitIndex: */
                {
                    if (plan.TryGetValue(RaftLogAction.Rollback, out List<RaftLog> rollbackActions))
                        rollbackActions.Add(log);
                    else
                        plan.Add(RaftLogAction.Rollback, [log]);

                    logger.LogDebug("[{Endpoint}/{Partition}] Rolled back checkpoint log #{Id}", manager.LocalEndpoint, partition.PartitionId, log.Id);

                    commitIndex = log.Id + 1;
                } 
                break;

                case RaftLogType.CommittedCheckpoint: /* when log.Id >= commitIndex:*/
                {
                    if (plan.TryGetValue(RaftLogAction.Commit, out List<RaftLog> commitActions))
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
            status = RaftOperationStatus.Success;
            
            switch (keyValue.Key)
            {
                case RaftLogAction.Propose:
                    if (keyValue.Value.Count > 0)
                    {
                        /*if (keyValue.Value.Count == 1)
                            status = await manager.WriteThreadPool.EnqueueTask(() => walAdapter.Propose(partition.PartitionId, keyValue.Value[0])).ConfigureAwait(false);
                        else
                            status = await manager.WriteThreadPool.EnqueueTask(() => walAdapter.ProposeMany(partition.PartitionId, keyValue.Value)).ConfigureAwait(false);*/
                        
                        status = await manager.RaftBatcher.Enqueue((partition.PartitionId, keyValue.Value)).ConfigureAwait(false);
                    }

                    break;
                
                case RaftLogAction.Commit:
                    if (keyValue.Value.Count > 0)
                    {
                        /*if (keyValue.Value.Count == 1)
                            status = await manager.WriteThreadPool
                                .EnqueueTask(() => walAdapter.Commit(partition.PartitionId, keyValue.Value[0]))
                                .ConfigureAwait(false);
                        else
                            status = await manager.WriteThreadPool
                                .EnqueueTask(() => walAdapter.CommitMany(partition.PartitionId, keyValue.Value))
                                .ConfigureAwait(false);*/
                        
                        status = await manager.RaftBatcher.Enqueue((partition.PartitionId, keyValue.Value)).ConfigureAwait(false);
                    }

                    break;
                
                case RaftLogAction.Rollback:
                    if (keyValue.Value.Count > 0)
                    {
                        status = await manager.RaftBatcher.Enqueue((partition.PartitionId, keyValue.Value)).ConfigureAwait(false);
                    }

                    /*if (keyValue.Value.Count == 1)
                        status = await manager.WriteThreadPool.EnqueueTask(() => walAdapter.Rollback(partition.PartitionId, keyValue.Value[0])).ConfigureAwait(false);
                    else
                        status = await manager.WriteThreadPool.EnqueueTask(() => walAdapter.RollbackMany(partition.PartitionId, keyValue.Value)).ConfigureAwait(false);*/
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

    /// <summary>
    /// Retrieves a range of log entries from the Write-Ahead Log (WAL) starting from the specified log index.
    /// </summary>
    /// <param name="startLogIndex">
    /// The index of the first log entry to be retrieved. Only log entries from this index onward will be returned.
    /// </param>
    /// <returns>
    /// A task that represents the asynchronous operation. The task result contains a list of <see cref="RaftLog"/> objects
    /// corresponding to the logs retrieved from the specified range.
    /// </returns>
    /// <exception cref="RaftException">
    /// Thrown if the thread pool is not started or disposed while attempting to retrieve logs asynchronously.
    /// </exception>
    public async Task<List<RaftLog>> GetRange(long startLogIndex)
    {
        return await manager.ReadThreadPool.EnqueueTask(() => walAdapter.ReadLogsRange(partition.PartitionId, startLogIndex)).ConfigureAwait(false);
    }

    /// <summary>
    /// Executes the log compaction process for the partition managed by this instance.
    /// This method removes logs older than a specified checkpoint to reduce
    /// storage requirements while maintaining the integrity of the state machine.
    /// </summary>
    /// <returns>
    /// A task representing the asynchronous compaction operation.
    /// The returned task completes once the compaction process has finished.
    /// </returns>
    /// <exception cref="RaftException">
    /// Thrown if the thread pool is not started or has been disposed,
    /// or if an error occurs during the log compaction process.
    /// </exception>
    public async Task Compact()
    {
        long lastCheckpoint = await manager.ReadThreadPool.EnqueueTask(() => walAdapter.GetLastCheckpoint(partition.PartitionId)).ConfigureAwait(false);

        if (lastCheckpoint <= 0)
            return;
        
        logger.LogInformation("[{Endpoint}/{Partition}] Compaction process started LastCheckpoint={LastCheckpoint}", manager.LocalEndpoint, partition.PartitionId, lastCheckpoint);
        
        await manager.WriteThreadPool.EnqueueTask(() =>
            walAdapter.CompactLogsOlderThan(partition.PartitionId, lastCheckpoint, compactNumberEntries
        )).ConfigureAwait(false);
    }
}
