
using System.Threading;
using Kommander.Data;
using Kommander.Logging;
using Kommander.Support.Collections;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL;
using Kommander.WAL.Data;
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

    private readonly Action<RaftWalCompletion> onComplete;

    private readonly RaftPartition partition;

    private readonly IWAL walAdapter;

    private readonly ILogger<IRaft> logger;

    /// <summary>
    /// A private dictionary to store and manage collections of <see cref="RaftLog"/> objects
    /// categorized by their respective actions (<see cref="RaftLogAction"/>). This dictionary
    /// is utilized to track operations such as propose, commit, and rollback for logs during
    /// the write-ahead process in the Raft consensus algorithm.
    ///
    /// By grouping logs based on their actions we can enqueue the write operations in the
    /// persistence (rocksdb or sqlite) in a more efficient way.
    /// </summary>
    private readonly SmallDictionary<RaftLogAction, List<RaftLog>> plan = new(3);
    
    private readonly int compactEveryOperations;
    
    private readonly int compactNumberEntries;

    private readonly int maxEntriesPerCompaction;

    private bool recovered;
    
    private long proposeIndex = 1;

    private long commitIndex = 1;

    private int operations;

    private long walOperationSequence;

    private int compactionInFlight;

    /// <summary>
    /// Application-supplied floor: compaction will not truncate committed entries at or above this
    /// id, even when the checkpoint has advanced past it. Used to retain a WAL tail for
    /// point-in-time recovery. Default <see cref="long.MaxValue"/> means no extra retention
    /// (truncate to checkpoint). A value &lt;= 0 is normalized to <see cref="long.MaxValue"/>
    /// (no protection), NOT to 0 — a zero floor would suppress all compaction by collapsing
    /// <c>effectiveFloor</c> to 0 and triggering the early-return, which is never the desired
    /// behaviour when a caller has not yet computed its protected index.
    /// </summary>
    private long minRetainIndex = long.MaxValue;

    // Test-only handle for WaitForCompactionIdleAsync; not a production synchronization point.
    private Task? compactionPassTask;

    private int compactionPassCount;

    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="manager"></param>
    /// <param name="onComplete">
    /// Callback invoked by the scheduler when a WAL write completes (or errors).
    /// Must not block; the owning partition executor routes the completion back
    /// to <see cref="RaftPartitionStateMachine.CompleteWalOperationAsync"/>.
    /// </param>
    /// <param name="partition"></param>
    /// <param name="walAdapter"></param>
    public RaftWriteAhead(RaftManager manager, Action<RaftWalCompletion> onComplete, RaftPartition partition, IWAL walAdapter)
    {
        this.manager = manager;
        this.onComplete = onComplete;
        this.logger = manager.Logger;
        this.partition = partition;
        this.walAdapter = walAdapter;
        
        this.compactEveryOperations = manager.Configuration.CompactEveryOperations;
        this.compactNumberEntries = manager.Configuration.GetEffectiveCompactNumberEntries();
        this.maxEntriesPerCompaction = manager.Configuration.GetEffectiveMaxEntriesPerCompaction();
        this.operations = compactEveryOperations > 0 ? compactEveryOperations : 0;
    }

    /// <summary>
    /// Called after a commit or follower-append WAL operation persists successfully.
    /// Each operation decrements the counter by one (not per log entry in the batch).
    /// When the counter reaches zero, starts a compaction pass without waiting for it.
    /// </summary>
    public void NotifyCommitted()
    {
        if (compactEveryOperations <= 0)
            return;

        int remaining = Interlocked.Decrement(ref operations);

        if (remaining <= 0)
        {
            Interlocked.Exchange(ref operations, compactEveryOperations);
            Compact();
        }
    }

    /// <summary>
    /// Phase 1 of the nonblocking restore: reads all persisted log entries from WAL
    /// storage through the I/O scheduler.  Returns the raw list so the caller can
    /// deliver it back to the partition executor for replay under the single-owner
    /// guarantee (correctness rule 1).
    /// </summary>
    public async ValueTask<IReadOnlyList<RaftLog>> LoadRestoreLogsAsync()
    {
        if (recovered)
            return Array.Empty<RaftLog>();

        List<RaftLog> logs = await manager.ReadScheduler.EnqueueTask(partition.PartitionId, () => walAdapter.ReadLogs(partition.PartitionId)).ConfigureAwait(false);

        if (logs.Count > 0)
            manager.Logger.LogInfoRecoveredLogs(manager.LocalEndpoint, partition.PartitionId, logs.Count);

        return logs;
    }

    /// <summary>
    /// Phase 2 of the nonblocking restore: replays the loaded log entries by invoking
    /// the application replication callbacks and updating the WAL commit index.
    /// Must be called on the partition executor thread (single-owner guarantee).
    /// For P0, each entry is dispatched by log type: <c>_RaftSystem</c> entries go to
    /// <c>InvokeSystemLogRestored</c>; all other types go to <c>InvokeLogRestored</c>
    /// (consumer).  After replay completes, P0 fires both <c>InvokeSystemRestoreFinished</c>
    /// and <c>InvokeRestoreFinished</c> so the coordinator and the consumer each observe
    /// restore completion; non-P0 partitions fire only <c>InvokeRestoreFinished</c>.
    /// </summary>
    public async ValueTask CompleteRestoreAsync(IReadOnlyList<RaftLog> logs)
    {
        if (recovered)
            return;

        recovered = true;

        manager.InvokeRestoreStarted(partition.PartitionId);

        bool found = false;

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

                if (partition.PartitionId == RaftSystemConfig.SystemPartition && log.LogType == RaftSystemConfig.RaftLogType)
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
        {
            // Fire both signals so the system coordinator and the consumer each learn
            // restore is complete. For non-P0 partitions only the consumer signal fires.
            manager.InvokeSystemRestoreFinished(partition.PartitionId);
            manager.InvokeRestoreFinished(partition.PartitionId);
        }
        else
            manager.InvokeRestoreFinished(partition.PartitionId);
    }

    /// <summary>
    /// Proposes a batch of logs in the current term for processing by the Raft consensus protocol.
    /// Logs are assigned unique indices and associated with the current term, then enqueued for replication.
    /// </summary>
    /// <param name="contextSelf"></param>
    /// <param name="term">
    ///     The current term in the Raft consensus protocol used to associate with the logs.
    /// </param>
    /// <param name="logs">
    ///     A list of logs to be proposed. If the list is null or empty, the method will return immediately with a success status and no index update.
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

        WALWriteOperation operation = EnqueuePropose(term, logs, default, false);

        return await Task.FromResult((RaftOperationStatus.Pending, operation.LogIndex));
    }

    public WALWriteOperation EnqueuePropose(long term, List<RaftLog> logs, HLCTimestamp timestamp, bool autoCommit)
    {
        RaftLog[] ordered = logs.OrderBy(log => log.Id).ToArray();

        // Snapshot mutable state before mutation so we can roll back atomically if
        // the scheduler rejects the operation (e.g. BackpressureExceededException).
        long savedProposeIndex = proposeIndex;
        long[] savedIds   = Array.ConvertAll(ordered, l => l.Id);
        long[] savedTerms = Array.ConvertAll(ordered, l => l.Term);

        foreach (RaftLog log in ordered)
        {
            log.Id = proposeIndex++;
            log.Term = term;
        }

        WALWriteOperation operation = new(
            onComplete,
            Interlocked.Increment(ref walOperationSequence),
            WALWriteOperationType.LeaderPropose,
            (partition.PartitionId, logs),
            timestamp,
            term: term,
            autoCommit: autoCommit,
            logIndex: proposeIndex
        );

        try
        {
            manager.WalScheduler.Enqueue(operation);
        }
        catch
        {
            // Restore all mutations so the caller observes no state change.
            proposeIndex = savedProposeIndex;
            for (int i = 0; i < ordered.Length; i++)
            {
                ordered[i].Id   = savedIds[i];
                ordered[i].Term = savedTerms[i];
            }
            throw;
        }

        return operation;
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
        long savedCommitIndex = commitIndex;
        RaftLog[] ordered = logs.OrderBy(log => log.Id).ToArray();
        RaftLogType[] savedTypes = Array.ConvertAll(ordered, l => l.Type);

        foreach (RaftLog log in ordered)
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

        try
        {
            WALWriteOperation operation = EnqueueCommitPrepared(logs, lastCommitIndex);
            return await Task.FromResult((RaftOperationStatus.Pending, operation.LogIndex));
        }
        catch
        {
            commitIndex = savedCommitIndex;
            for (int i = 0; i < ordered.Length; i++)
                ordered[i].Type = savedTypes[i];
            throw;
        }
    }

    public WALWriteOperation EnqueueCommit(List<RaftLog> logs)
    {
        long lastCommitIndex = -1;
        long savedCommitIndex = commitIndex;
        RaftLog[] ordered = logs.OrderBy(log => log.Id).ToArray();
        RaftLogType[] savedTypes = Array.ConvertAll(ordered, l => l.Type);

        foreach (RaftLog log in ordered)
        {
            switch (log.Type)
            {
                case RaftLogType.Proposed:
                    log.Type = RaftLogType.Committed;
                    commitIndex = log.Id + 1;
                    lastCommitIndex = log.Id;
                    break;

                case RaftLogType.ProposedCheckpoint:
                    log.Type = RaftLogType.CommittedCheckpoint;
                    commitIndex = log.Id + 1;
                    lastCommitIndex = log.Id;
                    break;
            }
        }

        try
        {
            return EnqueueCommitPrepared(logs, lastCommitIndex);
        }
        catch
        {
            commitIndex = savedCommitIndex;
            for (int i = 0; i < ordered.Length; i++)
                ordered[i].Type = savedTypes[i];
            throw;
        }
    }

    private WALWriteOperation EnqueueCommitPrepared(List<RaftLog> logs, long lastCommitIndex)
    {
        WALWriteOperation operation = new(
            onComplete,
            Interlocked.Increment(ref walOperationSequence),
            WALWriteOperationType.LeaderCommit,
            (partition.PartitionId, logs),
            logIndex: lastCommitIndex
        );

        manager.WalScheduler.Enqueue(operation);

        return operation;
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

        RaftLog[] ordered = logs.OrderBy(log => log.Id).ToArray();
        RaftLogType[] savedTypes = Array.ConvertAll(ordered, l => l.Type);

        foreach (RaftLog log in ordered)
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

        try
        {
            WALWriteOperation operation = EnqueueRollbackPrepared(logs);
            return await Task.FromResult((RaftOperationStatus.Pending, operation.LogIndex));
        }
        catch
        {
            for (int i = 0; i < ordered.Length; i++)
                ordered[i].Type = savedTypes[i];
            throw;
        }
    }

    public WALWriteOperation EnqueueRollback(List<RaftLog> logs)
    {
        RaftLog[] ordered = logs.OrderBy(log => log.Id).ToArray();
        RaftLogType[] savedTypes = Array.ConvertAll(ordered, l => l.Type);

        foreach (RaftLog log in ordered)
        {
            switch (log.Type)
            {
                case RaftLogType.Proposed:
                    log.Type = RaftLogType.RolledBack;
                    break;

                case RaftLogType.ProposedCheckpoint:
                    log.Type = RaftLogType.RolledBackCheckpoint;
                    break;
            }
        }

        try
        {
            return EnqueueRollbackPrepared(logs);
        }
        catch
        {
            for (int i = 0; i < ordered.Length; i++)
                ordered[i].Type = savedTypes[i];
            throw;
        }
    }

    private WALWriteOperation EnqueueRollbackPrepared(List<RaftLog> logs)
    {
        WALWriteOperation operation = new(
            onComplete,
            Interlocked.Increment(ref walOperationSequence),
            WALWriteOperationType.LeaderRollback,
            (partition.PartitionId, logs)
        );

        manager.WalScheduler.Enqueue(operation);

        return operation;
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
        return await manager.ReadScheduler.EnqueueTask(partition.PartitionId, () => walAdapter.GetMaxLog(partition.PartitionId));
    }

    /// <summary>
    /// Returns the highest log id known to be committed, read from the in-memory
    /// <c>commitIndex</c> (which is the next-commit slot, so the committed id is one less).
    /// Unlike <see cref="GetMaxLog"/> this excludes proposed-but-uncommitted tail entries,
    /// so the leader can seed its backfill cursor on election without shipping uncommitted logs.
    /// Synchronous: it reads an in-memory counter, no WAL/scheduler round-trip.
    /// </summary>
    public long GetCommitIndex() => commitIndex - 1;

    /// <summary>
    /// Returns the id of the last <c>CommittedCheckpoint</c> WAL entry for this partition, or
    /// -1 when no checkpoint exists.  Scheduled on the read thread so it does not race with WAL writes.
    /// </summary>
    public async ValueTask<long> GetLastCheckpointAsync()
    {
        return await manager.ReadScheduler.EnqueueTask(
            partition.PartitionId,
            () => walAdapter.GetLastCheckpoint(partition.PartitionId)
        ).ConfigureAwait(false);
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
        return await manager.ReadScheduler.EnqueueTask(partition.PartitionId, () => walAdapter.GetCurrentTerm(partition.PartitionId));
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
    public WALWriteOperation? EnqueueProposeOrCommit(List<RaftLog>? logs, HLCTimestamp timestamp = default, string? endpoint = null, long term = -1)
    {
        if (logs is null || logs.Count == 0)
            return null;

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
        
        // Snapshot mutable counters before the mutation loop so that a backpressure
        // rejection from WalScheduler.Enqueue can be rolled back atomically.
        long savedProposeIndex = proposeIndex;
        long savedCommitIndex  = commitIndex;

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

                    logger.LogDebugRolledbackLog(manager.LocalEndpoint, partition.PartitionId, log.Id);

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

                    logger.LogDebugProposedCheckpointLog(manager.LocalEndpoint, partition.PartitionId, log.Id);

                    proposeIndex = log.Id + 1;
                } 
                break;

                case RaftLogType.RolledBackCheckpoint: /* when log.Id >= commitIndex: */
                {
                    if (plan.TryGetValue(RaftLogAction.Rollback, out List<RaftLog> rollbackActions))
                        rollbackActions.Add(log);
                    else
                        plan.Add(RaftLogAction.Rollback, [log]);

                    logger.LogDebugRolledBackCheckpointLog(manager.LocalEndpoint, partition.PartitionId, log.Id);

                    commitIndex = log.Id + 1;
                } 
                break;

                case RaftLogType.CommittedCheckpoint: /* when log.Id >= commitIndex:*/
                {
                    if (plan.TryGetValue(RaftLogAction.Commit, out List<RaftLog> commitActions))
                        commitActions.Add(log);
                    else
                        plan.Add(RaftLogAction.Commit, [log]);

                    logger.LogDebugCommittedCheckpointLog(manager.LocalEndpoint, partition.PartitionId, log.Id);

                    commitIndex = log.Id + 1;
                } 
                break;

                default:
                    break;
            }
        }

        List<RaftLog> logsToWrite = new(orderedLogs.Length);

        foreach (KeyValuePair<RaftLogAction, List<RaftLog>> keyValue in plan)
        {
            if (keyValue.Value.Count > 0)
                logsToWrite.AddRange(keyValue.Value);
        }

        if (logsToWrite.Count == 0)
            return null;

        WALWriteOperation operation = new(
            onComplete,
            Interlocked.Increment(ref walOperationSequence),
            WALWriteOperationType.FollowerAppend,
            (partition.PartitionId, logsToWrite),
            timestamp,
            endpoint,
            term,
            logIndex: logsToWrite.Max(log => log.Id)
        );

        try
        {
            manager.WalScheduler.Enqueue(operation);
        }
        catch
        {
            proposeIndex = savedProposeIndex;
            commitIndex  = savedCommitIndex;
            throw;
        }

        return operation;
    }

    public Task<(RaftOperationStatus, long)> ProposeOrCommit(List<RaftLog>? logs)
    {
        WALWriteOperation? operation = EnqueueProposeOrCommit(logs);
        return Task.FromResult(operation is null
            ? (RaftOperationStatus.Success, Math.Max(proposeIndex, commitIndex))
            : (RaftOperationStatus.Pending, operation.LogIndex));
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
        return await manager.ReadScheduler.EnqueueTask(partition.PartitionId, () => walAdapter.ReadLogsRange(partition.PartitionId, startLogIndex)).ConfigureAwait(false);
    }

    /// <summary>
    /// Returns the term of the single entry at <paramref name="logIndex"/>, or <c>-1</c> if
    /// no entry with that id exists.  All entry types (Proposed, Committed, etc.) are included
    /// so a Log Matching Property check is correct even when the anchor entry is uncommitted.
    /// </summary>
    public async ValueTask<long> GetAnyTermAtAsync(long logIndex)
    {
        List<RaftLog> entries = await manager.ReadScheduler.EnqueueTask(
            partition.PartitionId,
            () => walAdapter.ReadLogsRange(partition.PartitionId, logIndex, 1)
        ).ConfigureAwait(false);

        return entries.Count > 0 && entries[0].Id == logIndex ? entries[0].Term : -1;
    }

    /// <summary>
    /// Reads up to <paramref name="maxEntries"/> committed log entries with id ≥
    /// <paramref name="startLogIndex"/>, sorted ascending. The bound is pushed to the storage
    /// engine so that a follower far behind the leader does not cause a full tail scan.
    /// Uncommitted (proposed/rolled-back) entries within the returned batch are filtered out.
    ///
    /// <para><b>Compaction floor handling:</b> if the leader has already compacted past
    /// <paramref name="startLogIndex"/>, the requested prefix no longer exists, so this method
    /// returns an empty list rather than a batch that would advance the follower over a gap.
    /// The leader never ships a non-contiguous range — log-shipping always starts at the
    /// follower's <c>lastCommitIndexes + 1</c>, which keeps the follower's log contiguous by
    /// construction (the follower append path does not itself enforce a prev-entry match).
    /// On the empty result, <c>SendHeartbeat</c> initiates a snapshot transfer to the follower
    /// (when a state-machine transfer is registered and a checkpoint exists); the follower then
    /// resumes normal log shipping from the snapshot index.</para>
    /// </summary>
    public async ValueTask<List<RaftLog>> GetRangeAsync(long startLogIndex, int maxEntries)
    {
        List<RaftLog> all = await manager.ReadScheduler.EnqueueTask(
            partition.PartitionId,
            () => walAdapter.ReadLogsRange(partition.PartitionId, startLogIndex, maxEntries)
        ).ConfigureAwait(false);

        // Filter out any uncommitted entries (proposed/rolled-back) within the bounded batch.
        // The storage layer already capped the row count, so no further size check is needed.
        List<RaftLog> result = [];
        foreach (RaftLog log in all)
        {
            if (log.Type != RaftLogType.Committed && log.Type != RaftLogType.CommittedCheckpoint)
                continue;
            result.Add(log);
        }
        return result;
    }

    /// <summary>
    /// Starts log compaction for this partition if no pass is already running.
    /// Returns immediately without waiting for the pass to finish.
    /// </summary>
    public void Compact()
    {
        if (Interlocked.CompareExchange(ref compactionInFlight, 1, 0) != 0)
            return;

        compactionPassTask = RunCompactionPassAsync();
    }

    /// <summary>
    /// Waits for the in-flight compaction pass to complete. For tests only.
    /// </summary>
    internal Task WaitForCompactionIdleAsync() => compactionPassTask ?? Task.CompletedTask;

    /// <summary>
    /// Number of compaction passes that actually started. For tests only.
    /// </summary>
    internal int CompactionPassCount => Volatile.Read(ref compactionPassCount);

    /// <summary>
    /// Sets the minimum WAL index that compaction must not truncate below, regardless of the
    /// checkpoint position. Used by point-in-time recovery consumers to protect a retained tail.
    /// <para>
    /// This setter is synchronous and thread-safe via a volatile write; after it returns the next
    /// compaction pass observes the new value without any scheduling round-trip.
    /// </para>
    /// <para>
    /// Values &lt;= 0 are normalized to <see cref="long.MaxValue"/> (no protection). This prevents
    /// a caller that has not yet computed its protected index (e.g. first tick, empty PITR window)
    /// from accidentally disabling compaction by passing 0.
    /// </para>
    /// <para>
    /// The floor is in-memory and resets to <see cref="long.MaxValue"/> on process restart.
    /// Consumers must re-assert it after every node start before relying on PITR for that node.
    /// </para>
    /// </summary>
    public void SetMinRetainIndex(long index) =>
        Volatile.Write(ref minRetainIndex, index <= 0 ? long.MaxValue : index);

    /// <summary>Current retention floor. Diagnostics/tests only; <see cref="long.MaxValue"/> means unset.</summary>
    internal long MinRetainIndex => Volatile.Read(ref minRetainIndex);

    private async Task RunCompactionPassAsync()
    {
        Interlocked.Increment(ref compactionPassCount);

        try
        {
            long lastCheckpoint = await manager.ReadScheduler.EnqueueTask(partition.PartitionId, () =>
                walAdapter.GetLastCheckpoint(partition.PartitionId)
            ).ConfigureAwait(false);

            if (lastCheckpoint <= 0)
                return;

            long retainFloor = Volatile.Read(ref minRetainIndex);
            long effectiveFloor = Math.Min(lastCheckpoint, retainFloor);

            if (effectiveFloor <= 0)
                return;

            logger.LogInfoCompactionStarted(manager.LocalEndpoint, partition.PartitionId, effectiveFloor);

            // Scheduled on ReadScheduler, not WalScheduler — compaction deletes must not
            // contend with the write path on the WAL scheduler.
            // All drain batches run inside a single WAL compaction call so durable backends
            // commit one transaction / db.Write per pass instead of one per batch.
            int removedTotal = await manager.ReadScheduler.EnqueueTask(partition.PartitionId, () =>
            {
                (RaftOperationStatus status, int removed) = walAdapter.CompactLogsOlderThan(
                    partition.PartitionId,
                    effectiveFloor,
                    compactNumberEntries,
                    maxEntriesPerCompaction);

                return status == RaftOperationStatus.Success ? removed : 0;
            }).ConfigureAwait(false);

            logger.LogInfoCompactionFinished(manager.LocalEndpoint, partition.PartitionId, removedTotal, effectiveFloor);
        }
        catch (Exception ex)
        {
            logger.LogError(
                ex,
                "[{Endpoint}/{Partition}] Compaction failed",
                manager.LocalEndpoint,
                partition.PartitionId);
        }
        finally
        {
            Interlocked.Exchange(ref compactionInFlight, 0);
        }
    }
}
