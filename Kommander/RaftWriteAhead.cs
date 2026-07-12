
using System.Buffers;
using Kommander.Data;
using Kommander.Diagnostics;
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

    // Out-of-order resolved (Committed/RolledBack) ids buffered until the gap below them fills,
    // so the follower's commit frontier (commitIndex) only ever advances over a contiguous prefix.
    // The unanchored live-propose broadcast delivers entries out of order under load; without this
    // buffer the frontier either overshoots a hole (applying entries before their predecessors,
    // which slows WriteOperationCompleted) or — if advanced only on an exact match — freezes at the
    // first reordered entry. Touched only on the partition's serialized executor path.
    private readonly SortedSet<long> pendingResolved = new();

    // Reused across follower appends to collect this batch's resolved ids; applied to the frontier
    // only after the WAL enqueue succeeds, so a backpressure rejection needs no frontier rollback.
    private readonly List<long> resolvedThisBatch = [];

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
            return [];

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

        // ── Reconstruct the commit frontier ───────────────────────────────────────────────
        // logs is sorted ascending by id and begins at the last durable CommittedCheckpoint.
        // We scan once to derive three quantities used below.
        long maxLogId = 0;              // highest durable id (any type) — the propose cursor floor
        long lastResolvedCommitted = 0; // id of the last Committed/CommittedCheckpoint seen (legacy path)
        long contiguousCommitted = 0;   // highest id of an unbroken committed prefix (fast path)
        bool any = false;

        foreach (RaftLog log in logs)
        {
            any = true;
            if (log.Id > maxLogId)
                maxLogId = log.Id;

            switch (log.Type)
            {
                case RaftLogType.CommittedCheckpoint:
                    lastResolvedCommitted = log.Id;
                    // A checkpoint certifies the whole prefix ≤ its id is committed (it is the durable
                    // recovery anchor), so it may jump the contiguous frontier.
                    if (log.Id > contiguousCommitted)
                        contiguousCommitted = log.Id;
                    break;

                case RaftLogType.Committed:
                    lastResolvedCommitted = log.Id;
                    // Extend the contiguous prefix only across an unbroken run; a gap — a Proposed entry
                    // whose lazy commit marker was lost on the fast path, or any missing id — stops it.
                    if (log.Id == contiguousCommitted + 1)
                        contiguousCommitted = log.Id;
                    break;

                case RaftLogType.Proposed:
                case RaftLogType.ProposedCheckpoint:
                case RaftLogType.RolledBack:
                case RaftLogType.RolledBackCheckpoint:
                    break;

                default:
                    throw new NotImplementedException();
            }
        }

        if (manager.Configuration.WalSingleFsyncCommit)
        {
            // Single-fsync fast path: the per-entry Committed marker is written lazily, so a crash can
            // leave a durable Proposed prefix whose markers were lost. The on-disk types are no longer a
            // reliable committed/uncommitted boundary, so reconstruct conservatively:
            //   commitIndex  = highest CONTIGUOUS committed prefix + 1 (the safe lower bound). Entries
            //                  above the first gap are NOT treated as committed here; the true frontier
            //                  above this is re-supplied by the leader on reconnect (follower) or by
            //                  re-commit once a current-term entry commits on election (leader).
            //   proposeIndex = maxLogId + 1, so the durable Proposed tail is PRESERVED — a later propose
            //                  never reuses its ids, which would overwrite an acked-but-lazily-committed
            //                  entry (data loss). Recovery never treats that tail as committed, so an
            //                  unacknowledged-but-not-durable write is never promoted.
            commitIndex = contiguousCommitted + 1;
            proposeIndex = maxLogId + 1;
        }
        else
        {
            // Legacy path (commit markers always durable ⇒ the committed prefix is contiguous and
            // complete). Byte-for-byte the prior behaviour: the frontier is the last committed id, and a
            // proposed-but-uncommitted tail is discarded (a later propose reuses its ids).
            commitIndex = any ? lastResolvedCommitted + 1 : await GetMaxLog().ConfigureAwait(false) + 1;
            proposeIndex = any ? lastResolvedCommitted + 1 : commitIndex;
        }

        // ── Replay the committed prefix to the application ─────────────────────────────────
        // Apply only committed data entries strictly below the reconstructed frontier. Checkpoints carry
        // no application payload (their state is restored via the snapshot transfer); entries at or above
        // the frontier are deferred to leader re-supply / re-commit.
        foreach (RaftLog log in logs)
        {
            if (log.Type != RaftLogType.Committed || log.Id >= commitIndex)
                continue;

            try
            {
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
    public Task<(RaftOperationStatus, long)> Propose(long term, List<RaftLog>? logs)
    {
        if (logs is null || logs.Count == 0)
            return Task.FromResult((RaftOperationStatus.Success, -1L));

        WALWriteOperation operation = EnqueuePropose(term, logs, default, false);

        // Synchronous: EnqueuePropose hands off to the WAL scheduler without awaiting. No async
        // state machine or extra Task.FromResult round-trip — just wrap the already-known result.
        return Task.FromResult((RaftOperationStatus.Pending, operation.LogIndex));
    }

    /// <summary>
    /// Returns <paramref name="logs"/> in ascending <see cref="RaftLog.Id"/> order. The common
    /// case — input already non-decreasing (callers build batches in id order, and freshly
    /// proposed logs share a placeholder id) — returns the original list with no allocation. Only
    /// when the input is genuinely out of order does it fall back to a stably sorted copy.
    /// <para>
    /// The fallback uses <see cref="Enumerable.OrderBy{TSource,TKey}(IEnumerable{TSource},Func{TSource,TKey})"/>,
    /// which is a stable sort: entries with equal ids keep their relative order. This is
    /// load-bearing on the propose path, where unassigned entries can share a placeholder id and
    /// must retain insertion order so they receive sequential indices deterministically. The
    /// already-sorted check uses a strict <c>&gt;</c> comparison, so equal-id runs are treated as
    /// sorted and likewise keep their original order — identical to the stable sort. The result is
    /// only read by callers, never structurally mutated.
    /// </para>
    /// <remarks>
    /// Exposed as <see langword="internal"/> (not <see langword="private"/>) so the
    /// <c>Kommander.MicroBenchmarks</c> project can measure the allocation-free fast path against
    /// the previous <c>OrderBy(...).ToArray()</c> cost on real code.
    /// </remarks>
    internal static IReadOnlyList<RaftLog> OrderById(List<RaftLog> logs)
    {
        for (int i = 1; i < logs.Count; i++)
        {
            if (logs[i - 1].Id > logs[i].Id)
                return logs.OrderBy(static log => log.Id).ToArray();
        }

        return logs;
    }

    public WALWriteOperation EnqueuePropose(long term, List<RaftLog> logs, HLCTimestamp timestamp, bool autoCommit)
    {
        IReadOnlyList<RaftLog> ordered = OrderById(logs);
        int count = ordered.Count;

        // Snapshot mutable state before mutation so we can roll back atomically if the scheduler
        // rejects the operation (e.g. BackpressureExceededException). The id/term snapshots use
        // pooled value buffers (no GC references): rented and returned within this call, never
        // exposed, so they add no steady-state allocation on the propose hot path.
        long savedProposeIndex = proposeIndex;
        long[] savedIds   = ArrayPool<long>.Shared.Rent(count);
        long[] savedTerms = ArrayPool<long>.Shared.Rent(count);

        try
        {
            for (int i = 0; i < count; i++)
            {
                RaftLog log = ordered[i];
                savedIds[i]   = log.Id;
                savedTerms[i] = log.Term;
                log.Id   = proposeIndex++;
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
                for (int i = 0; i < count; i++)
                {
                    ordered[i].Id   = savedIds[i];
                    ordered[i].Term = savedTerms[i];
                }
                throw;
            }

            // Count the durable phase from the producer side (inert unless instrumentation
            // is enabled): one LeaderPropose enqueue per single-round committed write.
            WalPhaseInstrumentation.RecordEnqueued(WALWriteOperationType.LeaderPropose);

            return operation;
        }
        finally
        {
            ArrayPool<long>.Shared.Return(savedIds);
            ArrayPool<long>.Shared.Return(savedTerms);
        }
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
    public Task<(RaftOperationStatus, long)> Commit(List<RaftLog>? logs)
    {
        if (logs is null || logs.Count == 0)
            return Task.FromResult((RaftOperationStatus.Success, -1L));

        long lastCommitIndex = -1;
        long savedCommitIndex = commitIndex;
        IReadOnlyList<RaftLog> ordered = OrderById(logs);
        int count = ordered.Count;
        RaftLogType[] savedTypes = ArrayPool<RaftLogType>.Shared.Rent(count);

        try
        {
            for (int i = 0; i < count; i++)
            {
                RaftLog log = ordered[i];
                savedTypes[i] = log.Type;

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
                return Task.FromResult((RaftOperationStatus.Pending, operation.LogIndex));
            }
            catch
            {
                commitIndex = savedCommitIndex;
                for (int i = 0; i < count; i++)
                    ordered[i].Type = savedTypes[i];
                throw;
            }
        }
        finally
        {
            ArrayPool<RaftLogType>.Shared.Return(savedTypes);
        }
    }

    public WALWriteOperation EnqueueCommit(List<RaftLog> logs)
    {
        long lastCommitIndex = -1;
        long savedCommitIndex = commitIndex;
        IReadOnlyList<RaftLog> ordered = OrderById(logs);
        int count = ordered.Count;
        RaftLogType[] savedTypes = ArrayPool<RaftLogType>.Shared.Rent(count);

        try
        {
            for (int i = 0; i < count; i++)
            {
                RaftLog log = ordered[i];
                savedTypes[i] = log.Type;

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
                for (int i = 0; i < count; i++)
                    ordered[i].Type = savedTypes[i];
                throw;
            }
        }
        finally
        {
            ArrayPool<RaftLogType>.Shared.Return(savedTypes);
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

        // The second durable phase of a committed write: pairs with the LeaderPropose
        // enqueue above, so enqueues-per-write ≈ 2 confirms the two-fsync structure.
        WalPhaseInstrumentation.RecordEnqueued(WALWriteOperationType.LeaderCommit);

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
    public Task<(RaftOperationStatus, long)> Rollback(List<RaftLog>? logs)
    {
        if (logs is null || logs.Count == 0)
            return Task.FromResult((RaftOperationStatus.Success, -1L));

        IReadOnlyList<RaftLog> ordered = OrderById(logs);
        int count = ordered.Count;
        RaftLogType[] savedTypes = ArrayPool<RaftLogType>.Shared.Rent(count);

        try
        {
            for (int i = 0; i < count; i++)
            {
                RaftLog log = ordered[i];
                savedTypes[i] = log.Type;

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
                return Task.FromResult((RaftOperationStatus.Pending, operation.LogIndex));
            }
            catch
            {
                for (int i = 0; i < count; i++)
                    ordered[i].Type = savedTypes[i];
                throw;
            }
        }
        finally
        {
            ArrayPool<RaftLogType>.Shared.Return(savedTypes);
        }
    }

    public WALWriteOperation EnqueueRollback(List<RaftLog> logs)
    {
        IReadOnlyList<RaftLog> ordered = OrderById(logs);
        int count = ordered.Count;
        RaftLogType[] savedTypes = ArrayPool<RaftLogType>.Shared.Rent(count);

        try
        {
            for (int i = 0; i < count; i++)
            {
                RaftLog log = ordered[i];
                savedTypes[i] = log.Type;

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
                for (int i = 0; i < count; i++)
                    ordered[i].Type = savedTypes[i];
                throw;
            }
        }
        finally
        {
            ArrayPool<RaftLogType>.Shared.Return(savedTypes);
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
    /// Advances the contiguous commit frontier to absorb a resolved (Committed/RolledBack) id.
    /// An id below the frontier is a duplicate replay (ignored); an id above it sits over an
    /// unfilled gap and is buffered until the gap closes; an id that fills the next slot advances
    /// the frontier and then drains any buffered successors that have now become contiguous.
    /// </summary>
    private void AdvanceCommitFrontier(long id)
    {
        if (id < commitIndex)
            return;

        if (id > commitIndex)
        {
            pendingResolved.Add(id);
            return;
        }

        commitIndex = id + 1;

        while (pendingResolved.Count > 0)
        {
            long next = pendingResolved.Min;
            if (next < commitIndex)
            {
                pendingResolved.Remove(next);   // stale duplicate already covered by the frontier
                continue;
            }
            if (next > commitIndex)
                break;                          // gap remains: stop draining

            pendingResolved.Remove(next);
            commitIndex = next + 1;
        }
    }

    /// <summary>
    /// Removes every log entry with id &gt; <paramref name="afterLogId"/> and returns the
    /// post-truncation max log id.
    /// <para>
    /// Atomicity is provided by the WAL backend, not by the scheduler: this calls the single
    /// <see cref="IWAL.TruncateLogsAfterAndGetMax"/> operation, which performs the delete and the
    /// max-read under one acquisition of the backend's per-partition write guard. Scheduling it on the
    /// <see cref="IRaftReadScheduler"/> only serializes it against other reads; it does <b>not</b>
    /// exclude the WAL-scheduler write path (a separate thread pool), so the backend-level guard is
    /// what prevents a concurrent <c>FollowerAppend</c> from re-growing the tail between the two steps.
    /// </para>
    /// <para>No-op-safe: if <paramref name="afterLogId"/> is at or above the current max, the
    /// log is unchanged and the current max is returned.</para>
    /// </summary>
    public async ValueTask<long> TruncateLogsAfterAsync(long afterLogId)
    {
        long maxLogId = await manager.ReadScheduler.EnqueueTask(partition.PartitionId, () =>
        {
            (RaftOperationStatus _, long m) = walAdapter.TruncateLogsAfterAndGetMax(partition.PartitionId, afterLogId);
            return m;
        }).ConfigureAwait(false);

        // The truncation removed every WAL entry above afterLogId, so any buffered out-of-order
        // resolution above it now points at an absent entry. Drop them; the leader's contiguous
        // backfill re-delivers (and re-buffers) the gap. Safe to mutate here without a lock: the
        // repair caller runs inside the partition's serialized executor message, so no concurrent
        // FollowerAppend is touching pendingResolved.
        while (pendingResolved.Count > 0 && pendingResolved.Max > afterLogId)
            pendingResolved.Remove(pendingResolved.Max);

        return maxLogId;
    }

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

        IReadOnlyList<RaftLog> orderedLogs = OrderById(logs);

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
        
        // Snapshot proposeIndex before the mutation loop so a backpressure rejection from
        // WalScheduler.Enqueue can be rolled back. The commit frontier is intentionally NOT
        // advanced in the loop: resolved ids are collected and applied only after a successful
        // enqueue (below), so a rejection needs no frontier rollback.
        long savedProposeIndex = proposeIndex;

        resolvedThisBatch.Clear();

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

                    resolvedThisBatch.Add(log.Id);
                }
                break;    

                case RaftLogType.Committed: /* when log.Id >= commitIndex: */
                {
                    if (plan.TryGetValue(RaftLogAction.Commit, out List<RaftLog> commitActions))
                        commitActions.Add(log);
                    else
                        plan.Add(RaftLogAction.Commit, [log]);
                
                    logger.LogDebugCommittedLogs(manager.LocalEndpoint, partition.PartitionId, log.Id);

                    resolvedThisBatch.Add(log.Id);
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

                    resolvedThisBatch.Add(log.Id);
                } 
                break;

                case RaftLogType.CommittedCheckpoint: /* when log.Id >= commitIndex:*/
                {
                    if (plan.TryGetValue(RaftLogAction.Commit, out List<RaftLog> commitActions))
                        commitActions.Add(log);
                    else
                        plan.Add(RaftLogAction.Commit, [log]);

                    logger.LogDebugCommittedCheckpointLog(manager.LocalEndpoint, partition.PartitionId, log.Id);

                    resolvedThisBatch.Add(log.Id);
                } 
                break;

                default:
                    break;
            }
        }

        List<RaftLog> logsToWrite = new(orderedLogs.Count);

        // Track the maximum id while flattening the plan groups so we avoid a second
        // LINQ Max() pass over logsToWrite. The max over the concatenated groups equals
        // the max over logsToWrite, so logIndex is identical to the previous code.
        long maxLogId = -1;

        foreach (KeyValuePair<RaftLogAction, List<RaftLog>> keyValue in plan)
        {
            List<RaftLog> group = keyValue.Value;
            for (int i = 0; i < group.Count; i++)
            {
                RaftLog log = group[i];
                logsToWrite.Add(log);
                if (log.Id > maxLogId)
                    maxLogId = log.Id;
            }
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
            logIndex: maxLogId
        );

        try
        {
            manager.WalScheduler.Enqueue(operation);
        }
        catch
        {
            proposeIndex = savedProposeIndex;
            throw;
        }

        // Enqueue succeeded: advance the commit frontier over this batch's resolved ids. Ascending
        // order (orderedLogs is sorted) keeps the drain cheap; AdvanceCommitFrontier still buffers
        // any id that sits above an unfilled gap so the frontier never overshoots a hole.
        foreach (long id in resolvedThisBatch)
            AdvanceCommitFrontier(id);

        // Follower-side durable phase. Followers fsync on the propose quorum's critical
        // path, so this phase's latency is measured symmetrically with the leader's.
        WalPhaseInstrumentation.RecordEnqueued(WALWriteOperationType.FollowerAppend);

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
        // Scalar term lookup: the backend reads a single term (point key/row) instead of
        // materializing a full RaftLog and its payload just to discard everything but Term.
        return await manager.ReadScheduler.EnqueueTask(
            partition.PartitionId,
            () => walAdapter.GetTermAt(partition.PartitionId, logIndex)
        ).ConfigureAwait(false);
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
        // `all` is a fresh list uniquely owned by this call (every backend's ReadLogsRange
        // materializes a new list, and the RaftLog references it holds are not mutated here), so
        // compact the committed entries in place with a write index and trim the tail — this
        // returns the same list and backing array instead of allocating a second List<RaftLog>.
        int write = 0;
        for (int read = 0; read < all.Count; read++)
        {
            RaftLog log = all[read];
            if (log.Type != RaftLogType.Committed && log.Type != RaftLogType.CommittedCheckpoint)
                continue;
            if (write != read)
                all[write] = log;
            write++;
        }
        if (write < all.Count)
            all.RemoveRange(write, all.Count - write);
        return all;
    }

    /// <summary>
    /// Reads up to <paramref name="maxEntries"/> log entries of ANY type (Proposed,
    /// Committed, RolledBack, etc.) with id ≥ <paramref name="startLogIndex"/>.
    /// Unlike <see cref="GetRangeAsync"/>, Proposed/RolledBack entries are not filtered out.
    /// </summary>
    public async ValueTask<List<RaftLog>> GetRangeAllTypesAsync(long startLogIndex, int maxEntries)
    {
        return await manager.ReadScheduler.EnqueueTask(
            partition.PartitionId,
            () => walAdapter.ReadLogsRange(partition.PartitionId, startLogIndex, maxEntries)
        ).ConfigureAwait(false);
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
