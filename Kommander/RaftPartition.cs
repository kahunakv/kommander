
using Kommander.Data;
using Kommander.Diagnostics;
using Kommander.Scheduling;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;
using System.Diagnostics;

namespace Kommander;

/// <summary>
/// Represents a partition in a Raft system. This class is responsible for managing
/// the lifecycle and operational aspects of a Raft partition, including log replication,
/// voting processes, and state management.
/// </summary>
public sealed class RaftPartition : IDisposable
{
    private static readonly RaftRequest RaftStateRequest = new(RaftRequestType.GetNodeState);

    // Shared singletons for the parameterless request types (RaftRequest is immutable, so one
    // instance can serve every post/ask). CheckLeaderRequest is the load-bearing one — it is
    // posted to every hot partition on every CheckLeaderInterval tick; the rest follow the same
    // pattern for consistency.
    private static readonly RaftRequest CheckLeaderRequest = new(RaftRequestType.CheckLeader);
    private static readonly RaftRequest ReplicateCheckpointRequest = new(RaftRequestType.ReplicateCheckpoint);
    private static readonly RaftRequest ForceLeaderForTestingRequest = new(RaftRequestType.ForceLeaderForTesting);
    private static readonly RaftRequest GetPartitionViewRequest = new(RaftRequestType.GetPartitionView);
    private static readonly RaftRequest StepDownRequest = new(RaftRequestType.StepDown);
    private static readonly RaftRequest ConfirmLeadershipRequest = new(RaftRequestType.ConfirmLeadership);
    private static readonly RaftRequest SuspendHeartbeatsRequest = new(RaftRequestType.SuspendHeartbeats);
    private static readonly RaftRequest ResumeHeartbeatsRequest = new(RaftRequestType.ResumeHeartbeats);

    /// <summary>
    /// Relay sink that breaks the circular dependency between
    /// <see cref="RaftPartitionStateMachine"/> (needs a sink at construction) and
    /// <see cref="RaftPartitionExecutor"/> (needs the state machine at construction).
    /// The executor reference is injected after both objects are created.
    /// </summary>
    private sealed class PartitionReplySink : IRaftOperationReplySink
    {
        internal RaftPartitionExecutor? Executor;

        public void TryComplete(ulong correlationId, RaftResponse response)
            => Executor?.DeliverReply(correlationId, response);
    }

    private readonly SemaphoreSlim semaphore = new(1, 1);

    private readonly RaftPartitionExecutor executor;

    /// <summary>
    /// The per-partition state machine. Held here only so <see cref="GetState()"/> can read the
    /// volatile role snapshot without an executor round-trip; every mutation still goes through
    /// the executor's single-writer queue.
    /// </summary>
    private readonly RaftPartitionStateMachine stateMachine;

    private readonly RaftManager manager;

    private readonly RaftWriteAhead walHandler;

    private int _disposed;

    /// <summary>
    /// Written by the executor's single-writer thread, read by arbitrary caller threads through
    /// <see cref="Leader"/> and by the <see cref="GetState()"/> snapshot path — volatile so a hot
    /// poller observes the publication instead of a cached core-local value.
    /// </summary>
    private volatile string _leader = "";

    /// <summary>
    /// Monotonic tick stamp of the last leader change, read from
    /// <see cref="RaftConfiguration.TickSource"/> rather than the process clock so that a
    /// deterministic simulation controls leader-stability windows. Seeded in the constructor,
    /// because a field initializer would run before <see cref="manager"/> is assigned.
    /// </summary>
    private long _leaderChangedTicks;

    internal string Leader
    {
        get => _leader;
        set
        {
            if (string.Equals(_leader, value, StringComparison.Ordinal))
                return;

            _leader = value;
            Interlocked.Exchange(ref _leaderChangedTicks, manager.Configuration.TickSource.GetTimestamp());
        }
    }

    internal long LeaderChangedTicks => Interlocked.Read(ref _leaderChangedTicks);

    private volatile int _startRange;
    private volatile int _endRange;
    private volatile int _routingMode;

    public int PartitionId { get; }

    public int StartRange
    {
        get => _startRange;
        internal set => _startRange = value;
    }

    public int EndRange
    {
        get => _endRange;
        internal set => _endRange = value;
    }

    /// <summary>Routing mode; updated in-place when the partition map is applied.</summary>
    public RaftRoutingMode RoutingMode
    {
        get => (RaftRoutingMode)_routingMode;
        internal set => _routingMode = (int)value;
    }

    private long _generation;

    /// <summary>
    /// Current map generation; bumped on every partition-map mutation.
    /// Written by the coordinator thread via <c>StartUserPartitions</c> and read
    /// by the executor thread's generation-fence closure, so access is
    /// interlocked to guarantee visibility on ARM64 and other weakly-ordered
    /// architectures.
    /// </summary>
    public long Generation
    {
        get => Interlocked.Read(ref _generation);
        internal set => Interlocked.Exchange(ref _generation, value);
    }

    private volatile int _state = (int)RaftPartitionState.Active;

    /// <summary>
    /// Lifecycle state of this partition as last applied by <c>StartUserPartitions</c>.
    /// Reflects the persisted partition map — <see cref="RaftPartitionState.Draining"/>
    /// means the partition is being merged and should not accept new writes.
    /// Written by the coordinator thread and read by consumers on other threads,
    /// so the backing field is volatile.
    /// </summary>
    public RaftPartitionState State
    {
        get => (RaftPartitionState)_state;
        internal set => _state = (int)value;
    }

    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="manager"></param>
    /// <param name="walAdapter"></param>
    /// <param name="partitionId"></param>
    /// <param name="startRange"></param>
    /// <param name="endRange"></param>
    /// <param name="logger"></param>
    /// <param name="pool">
    /// Optional shared executor pool.  When non-null the partition executor is driven by
    /// the pool instead of creating its own OS thread.  Must already be started.
    /// </param>
    public RaftPartition(
        RaftManager manager,
        IWAL walAdapter,
        int partitionId,
        int startRange,
        int endRange,
        ILogger<IRaft> logger,
        RaftExecutorPool? pool = null
    )
    {
        this.manager = manager;
        _leaderChangedTicks = manager.Configuration.TickSource.GetTimestamp();

        PartitionId = partitionId;
        StartRange = startRange;
        EndRange = endRange;

        // Break the circular dependency: state machine needs a reply sink; executor needs the
        // state machine. We wire the executor reference into the sink after both are created.
        PartitionReplySink replySink = new();

        // The WAL completion callback posts back to the executor. The closure captures
        // 'executor' which is assigned below, before Start() launches the worker thread,
        // so it is always non-null when any callback fires during normal operation.
        RaftPartitionExecutor? executorRef = null;
        walHandler = new(
            manager,
            completion => executorRef!.Post(new RaftRequest(RaftRequestType.WriteOperationCompleted, completion)),
            this,
            walAdapter
        );

        IRaftPartitionHost host = new RaftPartitionHostAdapter(manager, this);
        IRaftWalFacade wal = new RaftWalFacadeAdapter(walHandler);
        stateMachine = new(host, wal, replySink, logger);

        executor = new RaftPartitionExecutor(
            stateMachine,
            partitionId,
            slowThresholdMs: manager.Configuration.SlowRaftStateMachineLog,
            logger,
            maxClientQueueDepth:     manager.Configuration.MaxQueuedClientProposalsPerPartition,
            drainQuantumControl:     manager.Configuration.MaxDrainQuantumControl,
            drainQuantumReplication: manager.Configuration.MaxDrainQuantumReplication,
            drainQuantumClient:      manager.Configuration.MaxDrainQuantumClient,
            drainQuantumMaintenance: manager.Configuration.MaxDrainQuantumMaintenance,
            getGeneration:           () => Generation,
            walQueueDepthProvider:   () => manager.WalScheduler.GetPartitionDepth(partitionId),
            pool:                    pool,
            tickSource:              manager.Configuration.TickSource);
        executorRef = executor;
        replySink.Executor = executor;
        stateMachine.SetPostToExecutor(req => executorRef.Post(req));

        // Wire the hot-set callback so the manager learns immediately when this partition
        // quiesces (→ remove from hot set) or un-quiesces (→ re-add to hot set).
        // Fired under the single-owner guarantee so the ConcurrentDictionary ops are safe.
        if (manager.Configuration.EnableSharedExecutorPool)
            stateMachine.SetOnQuiesceChanged(isQuiesced =>
            {
                if (isQuiesced) manager.MarkPartitionCool(PartitionId);
                else            manager.MarkPartitionHot(PartitionId);
            });

        executor.Start();
    }

    /// <summary>
    /// Enqueues a handshake message from the partition.
    /// </summary>
    /// <param name="request"></param>
    public void Handshake(HandshakeRequest request)
    {
        executor.Post(new(
            RaftRequestType.ReceiveHandshake, 
            request.NodeId, 
            request.MaxLogId, 
            HLCTimestamp.Zero, 
            request.Endpoint
        ));
    }

    /// <summary>
    /// Posts a per-follower replication-progress reset for <paramref name="endpoint"/> to the
    /// partition's single-owner executor. Called when the committed roster (re)admits the member;
    /// see <see cref="RaftPartitionStateMachine.ResetFollowerProgress"/> for why retained progress
    /// must not survive a (re)admission.
    /// </summary>
    public void ResetFollowerProgress(string endpoint)
    {
        executor.Post(new(RaftRequestType.ResetFollowerProgress, endpoint: endpoint));
    }

    /// <summary>
    /// Sets the minimum WAL log index that compaction must not truncate below, regardless of
    /// the checkpoint position. Forwarded directly to the underlying <see cref="RaftWriteAhead"/>;
    /// the write is volatile so the next compaction pass observes it immediately with no
    /// scheduling round-trip. See <see cref="RaftWriteAhead.SetMinRetainIndex"/> for semantics.
    /// </summary>
    public void SetMinRetainIndex(long index) => walHandler.SetMinRetainIndex(index);

    /// <summary>
    /// Acquires a composable retention hold on this partition's WAL; the effective retention floor
    /// is the minimum across all active holds. Forwarded directly to the underlying
    /// <see cref="RaftWriteAhead"/>. See <see cref="RaftWriteAhead.AcquireRetentionHold"/> for semantics.
    /// </summary>
    public IDisposable AcquireRetentionHold(long index) => walHandler.AcquireRetentionHold(index);

    /// <summary>
    /// Highest log id known committed on this partition's WAL — the gap-aware contiguous commit
    /// frontier, which unlike the raw max log excludes proposed-but-uncommitted tail entries and
    /// anything above a hole. A plain in-memory read (no scheduler round-trip), safe on any path.
    /// Exposed so external observers (health checks, test harnesses) can distinguish "entries
    /// present but uncommitted" from "committed but unapplied" — the raw max log conflates them.
    /// </summary>
    public long GetCommitIndex() => walHandler.GetCommitIndex();

    /// <summary>
    /// Number of stale <c>Proposed</c> duplicates of already-resolved ids this partition has
    /// refused to write since it last started — a floor, not a lifetime total. Diagnostic only —
    /// see <see cref="RaftWriteAhead.GetStaleProposedSkippedCount"/> for what the number does and
    /// does not mean, in particular why zero is not evidence the guard never fired.
    /// </summary>
    public long GetStaleProposedSkippedCount() => walHandler.GetStaleProposedSkippedCount();

    /// <summary>
    /// Leader-side snapshot-transfer status per follower for this partition — see
    /// <see cref="IRaft.GetSnapshotStatuses"/>. Empty on a healthy partition; a plain thread-safe
    /// read, no executor round-trip.
    /// </summary>
    public IReadOnlyList<Data.RaftSnapshotStatus> GetSnapshotStatuses() => stateMachine.GetSnapshotStatuses();

    /// <summary>
    /// Leader-side non-contiguous-backfill status per follower for this partition — see
    /// <see cref="IRaft.GetBackfillStatuses"/>. Empty on a healthy partition; a plain thread-safe
    /// read, no executor round-trip.
    /// </summary>
    public IReadOnlyList<Data.RaftBackfillStatus> GetBackfillStatuses() => stateMachine.GetBackfillStatuses();

    /// <summary>
    /// Advisory composite load score for this partition, forwarded from the executor's
    /// <see cref="Scheduling.RaftPartitionExecutor.CurrentLoad"/> accumulator.
    /// </summary>
    internal double GetCurrentLoad(double wOps, double wQueue) => executor.CurrentLoad(wOps, wQueue);

    /// <summary>
    /// EWMA rate of <c>ReplicateLogs</c> operations per second on this partition.
    /// Leader-side only: reflects originating write load; returns 0 on follower nodes
    /// because followers process <c>AppendLogs</c>, not <c>ReplicateLogs</c>.
    /// </summary>
    internal double GetLogOpsPerSecond() => executor.CurrentLogOpsPerSecond();

    /// <summary>Exposes the underlying executor for targeted unit tests.</summary>
    internal RaftPartitionExecutor Executor => executor;

    /// <summary>
    /// Enqueues a "request a vote" message from the partition.
    /// </summary>
    /// <param name="request"></param>
    public void RequestVote(RequestVotesRequest request)
    {
        executor.Post(new(
            RaftRequestType.RequestVote,
            request.Term,
            request.MaxLogId,
            request.Time,
            request.Endpoint,
            preVote: request.PreVote,
            lastLogTerm: request.LastLogTerm
        ));
    }

    /// <summary>
    /// Enqueues a "vote to become leader" message in a partition.
    /// </summary>
    /// <param name="request"></param>
    public void Vote(VoteRequest request)
    {
        executor.Post(new(
            RaftRequestType.ReceiveVote,
            request.Term,
            request.MaxLogId,
            request.Time,
            request.Endpoint,
            preVote: request.PreVote,
            lastLogTerm: request.LastLogTerm
        ));
    }

    public void StepDownNotice(StepDownNoticeRequest request)
    {
        executor.Post(new(RaftRequestType.ReceiveStepDownNotice, request));
    }

    public void TransferLeadership(TransferLeadershipRequest request)
    {
        executor.Post(new(RaftRequestType.ReceiveTransferLeadership, request));
    }

    /// <summary>
    /// Append logs to the partition returning the commited index.
    /// </summary>
    /// <param name="request"></param>
    /// <returns></returns>
    public void AppendLogs(AppendLogsRequest request)
    {
        executor.Post(new(
            RaftRequestType.AppendLogs,
            request.Term,
            0,
            request.Time,
            request.Endpoint,
            RaftOperationStatus.Success,
            request.Logs,
            preVote: false,
            prevLogIndex: request.PrevLogIndex,
            prevLogTerm: request.PrevLogTerm,
            quiesce: request.Quiesce
        ));
    }
    
    /// <summary>
    /// Complete the append logs request
    /// </summary>
    /// <param name="request"></param>
    /// <returns></returns>
    public void CompleteAppendLogs(CompleteAppendLogsRequest request)
    {
        executor.Post(new(
            RaftRequestType.CompleteAppendLogs,
            request.Term,
            request.CommitIndex,
            request.Time,
            request.Endpoint,
            request.Status,
            null
        ));
    }

    /// <summary>
    /// Upper bound on how long a deferred-reply operation (propose, commit, rollback, checkpoint)
    /// may wait for its executor reply. These replies resolve through the WAL completion router,
    /// not inline in the executor, so a dropped completion would otherwise hang the caller with no
    /// response and no timeout — the Caraxes run-H shape, where callers orphaned by a term-fenced
    /// propose completion starved a closed-loop workload cluster-wide while health stayed green.
    /// The router no longer orphans replies (every fence answers before discarding); this bound is
    /// defense in depth so no future drop path can ever wedge callers permanently. Mirrors the
    /// 10 s ticket bound in <c>ReplicationGateway.WaitForQuorum</c>.
    /// </summary>
    private static readonly TimeSpan DeferredReplyTimeout = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Awaits a deferred-reply Ask under <see cref="DeferredReplyTimeout"/>, converting an elapsed
    /// bound into a <see cref="RaftOperationStatus.ProposalTimeout"/> response instead of throwing.
    /// Caller cancellation still surfaces as <see cref="OperationCanceledException"/>.
    /// </summary>
    private static async Task<RaftResponse> AskBounded(Task<RaftResponse> ask)
    {
        try
        {
            return await ask.WaitAsync(DeferredReplyTimeout).ConfigureAwait(false);
        }
        catch (TimeoutException)
        {
            return new(RaftResponseType.None, RaftOperationStatus.ProposalTimeout, 0L);
        }
    }

    /// <summary>
    /// Replicates logs to the cluster, ensuring log consistency according to the Raft consensus algorithm.
    /// (Replicates a single log to the partition)
    /// </summary>
    /// <param name="type">The type of the log entry to be replicated.</param>
    /// <param name="data">The byte array containing the data of the log entry.</param>
    /// <param name="autoCommit">A boolean value indicating whether the log should be committed automatically upon replication success.</param>
    /// <returns>A task that represents the asynchronous operation, containing a tuple with a boolean indicating success,
    /// a <see cref="RaftOperationStatus"/> indicating the result status, and an <see cref="HLCTimestamp"/> representing the ticket ID for the log entry.</returns>
    public async Task<(bool success, RaftOperationStatus status, HLCTimestamp ticketId)> ReplicateLogs(string type, byte[] data, bool autoCommit, long expectedGeneration = 0)
    {
        if (string.IsNullOrEmpty(Leader))
            return (false, RaftOperationStatus.NodeIsNotLeader, HLCTimestamp.Zero);

        if (Leader != manager.LocalEndpoint)
            return (false, RaftOperationStatus.NodeIsNotLeader, HLCTimestamp.Zero);

        List<RaftLog> logsToReplicate = [new() { Type = RaftLogType.Proposed, LogType = type, LogData = data }];

        RaftResponse response = await AskBounded(executor.Ask(new(RaftRequestType.ReplicateLogs, logsToReplicate, autoCommit, expectedGeneration))).ConfigureAwait(false);

        if (response.Status == RaftOperationStatus.Success)
            return (true, response.Status, response.TicketId);

        return (false, response.Status, HLCTimestamp.Zero);
    }

    /// <summary>
    /// Replicates logs across the Raft cluster.
    /// </summary>
    /// <param name="type">The type of the logs to be replicated, identified by a string.</param>
    /// <param name="logs">A collection of log entries, each represented as a byte array, to be replicated.</param>
    /// <param name="autoCommit">A boolean indicating whether the logs should be automatically committed after replication.</param>
    /// <returns>A tuple containing a boolean indicating success, the status of the operation as a <see cref="RaftOperationStatus"/> value, and the <see cref="HLCTimestamp"/> ticket ID of the operation.</returns>
    public async Task<(bool success, RaftOperationStatus status, HLCTimestamp ticketId)> ReplicateLogs(string type, IEnumerable<byte[]> logs, bool autoCommit = true, long expectedGeneration = 0)
    {
        // Avoid an extra copy when the caller already provides a list or array.
        IReadOnlyList<byte[]> payloads = logs as IReadOnlyList<byte[]> ?? logs.ToList();
        return await ReplicateLogs(type, payloads, autoCommit, expectedGeneration).ConfigureAwait(false);
    }

    /// <summary>
    /// Replicates logs across the Raft cluster from an already-materialized payload list,
    /// avoiding the intermediate copy incurred by the <see cref="IEnumerable{T}"/> overload
    /// when the caller holds an array or list.
    /// </summary>
    public async Task<(bool success, RaftOperationStatus status, HLCTimestamp ticketId)> ReplicateLogs(string type, IReadOnlyList<byte[]> logs, bool autoCommit = true, long expectedGeneration = 0)
    {
        if (string.IsNullOrEmpty(Leader))
            return (false, RaftOperationStatus.NodeIsNotLeader, HLCTimestamp.Zero);

        if (Leader != manager.LocalEndpoint)
            return (false, RaftOperationStatus.NodeIsNotLeader, HLCTimestamp.Zero);

        List<RaftLog> logsToReplicate = new(logs.Count);
        for (int i = 0; i < logs.Count; i++)
            logsToReplicate.Add(new() { Type = RaftLogType.Proposed, LogType = type, LogData = logs[i] });

        RaftResponse response = await AskBounded(executor.Ask(new(RaftRequestType.ReplicateLogs, logsToReplicate, autoCommit, expectedGeneration))).ConfigureAwait(false);

        if (response.Status == RaftOperationStatus.Success)
            return (true, response.Status, response.TicketId);

        return (false, response.Status, HLCTimestamp.Zero);
    }

    /// <summary>
    /// Replicates a caller-built, heterogeneous <see cref="RaftLog"/> batch as a single proposal
    /// (the <see cref="IRaft.ReplicateEntries"/> path). <paramref name="autoCommit"/> selects the commit
    /// group: <see langword="true"/> for the auto-commit portion (commits with the batch),
    /// <see langword="false"/> for a trailing manual group whose ticket the caller commits or rolls back later
    /// (slice 2).
    /// <para>
    /// Unlike the single-type overloads, the caller owns <paramref name="logs"/> and passes the same
    /// <see cref="RaftLog"/> instances that flow into the state machine's propose. <see cref="RaftWriteAhead"/>
    /// assigns each entry its log index (<see cref="RaftLog.Id"/>) in place during propose, and the executor
    /// reply resolves only once that propose is durable — so on a successful return the caller can read each
    /// entry's assigned index straight off <paramref name="logs"/> to build its per-entry results. The list is
    /// therefore not rebuilt or copied here; keep the reference stable across the call.
    /// </para>
    /// </summary>
    public async Task<(bool success, RaftOperationStatus status, HLCTimestamp ticketId)> ReplicateEntries(List<RaftLog> logs, long expectedGeneration = 0, bool autoCommit = true)
    {
        if (string.IsNullOrEmpty(Leader))
            return (false, RaftOperationStatus.NodeIsNotLeader, HLCTimestamp.Zero);

        if (Leader != manager.LocalEndpoint)
            return (false, RaftOperationStatus.NodeIsNotLeader, HLCTimestamp.Zero);

        // Reuses the existing ReplicateLogs request path: a heterogeneous List<RaftLog> with one autoCommit
        // flag and one generation fence is exactly what that path already accepts. RaftManager.ReplicateEntries
        // enforces the batch shape (single trailing manual group) before splitting into auto/manual proposals.
        RaftResponse response = await AskBounded(executor.Ask(new(RaftRequestType.ReplicateLogs, logs, autoCommit, expectedGeneration))).ConfigureAwait(false);

        if (response.Status == RaftOperationStatus.Success)
            return (true, response.Status, response.TicketId);

        return (false, response.Status, HLCTimestamp.Zero);
    }

    /// <summary>
    /// Commits logs for the specified ticket identifier if the current node is the leader and notifies followers.
    /// <para>
    /// If <paramref name="cancellationToken"/> fires while waiting on the executor, the method returns
    /// <c>(false, <see cref="RaftOperationStatus.OperationCancelled"/>, 0)</c> rather than throwing.
    /// The queued executor work may still apply after the caller reclaims control; re-issuing the same
    /// ticket is a safe idempotent no-op once the executor drains.
    /// </para>
    /// </summary>
    /// <param name="ticketId">The logical timestamp associated with the logs to be committed.</param>
    /// <param name="cancellationToken">Optional deadline; the wait is additionally bounded by <see cref="DeferredReplyTimeout"/>.</param>
    /// <returns>A tuple containing a boolean indicating success, the status of the operation as <see cref="RaftOperationStatus"/>, and the commit index of the logs.</returns>
    public async Task<(bool success, RaftOperationStatus status, long commitIndex)> CommitLogs(HLCTimestamp ticketId, CancellationToken cancellationToken = default)
    {
        // Post-propose refusal (the ticket's entries are appended already): a leadership change
        // here leaves their outcome open — the next leader's §5.4.2 inherited commit can still
        // commit them — so the answer is indeterminate, never the definite NodeIsNotLeader.
        if (string.IsNullOrEmpty(Leader))
            return (false, RaftOperationStatus.ProposalOutcomeUnknown, 0);

        if (Leader != manager.LocalEndpoint)
            return (false, RaftOperationStatus.ProposalOutcomeUnknown, 0);

        try
        {
            RaftResponse response = await AskBounded(executor.Ask(new(RaftRequestType.CommitLogs, ticketId, false), cancellationToken)).ConfigureAwait(false);

            if (response.Status == RaftOperationStatus.Success)
                return (true, response.Status, response.LogIndex);

            return (false, response.Status, 0);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            return (false, RaftOperationStatus.OperationCancelled, 0);
        }
    }

    /// <summary>
    /// Attempts to roll back logs to a specific timestamp for the current Raft partition.
    /// This operation can only be performed by the leader node of the partition.
    /// <para>
    /// If <paramref name="cancellationToken"/> fires while waiting on the executor, the method returns
    /// <c>(false, <see cref="RaftOperationStatus.OperationCancelled"/>, 0)</c> rather than throwing.
    /// The queued executor work may still apply after the caller reclaims control; re-issuing the same
    /// ticket is a safe idempotent no-op once the executor drains.
    /// </para>
    /// </summary>
    /// <param name="ticketId">The timestamp indicating the point to roll back the logs to.</param>
    /// <param name="cancellationToken">Optional deadline; the wait is additionally bounded by <see cref="DeferredReplyTimeout"/>.</param>
    /// <returns>A tuple indicating the success of the operation, the operation status, and the commit index.</returns>
    public async Task<(bool success, RaftOperationStatus status, long commitIndex)> RollbackLogs(HLCTimestamp ticketId, CancellationToken cancellationToken = default)
    {
        // Same post-propose contract as CommitLogs: a refused abort is indeterminate.
        if (string.IsNullOrEmpty(Leader))
            return (false, RaftOperationStatus.ProposalOutcomeUnknown, 0);

        if (Leader != manager.LocalEndpoint)
            return (false, RaftOperationStatus.ProposalOutcomeUnknown, 0);

        try
        {
            RaftResponse response = await AskBounded(executor.Ask(new(RaftRequestType.RollbackLogs, ticketId, false), cancellationToken)).ConfigureAwait(false);

            if (response.Status == RaftOperationStatus.Success)
                return (true, response.Status, response.LogIndex);

            return (false, response.Status, 0);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            return (false, RaftOperationStatus.OperationCancelled, 0);
        }
    }

    /// <summary>
    /// Attempts to replicate a checkpoint across the Raft cluster in the specified partition
    /// Checkpoints must be added to the Raft Log when the leader has replicated the logs to an external store.
    /// </summary>
    /// <returns>
    /// A tuple containing a boolean indicating success, the status of the operation, and the associated HLCTimestamp for the checkpoint.
    /// </returns>
    public async Task<(bool success, RaftOperationStatus status, HLCTimestamp ticketId)> ReplicateCheckpoint()
    {
        if (string.IsNullOrEmpty(Leader))
            return (false, RaftOperationStatus.NodeIsNotLeader, HLCTimestamp.Zero);
        
        if (Leader != manager.LocalEndpoint)
            return (false, RaftOperationStatus.NodeIsNotLeader, HLCTimestamp.Zero);
        
        RaftResponse response = await AskBounded(executor.Ask(ReplicateCheckpointRequest)).ConfigureAwait(false);
        
        if (response.Status == RaftOperationStatus.Success)
            return (true, response.Status, response.TicketId);

        return (false, response.Status, HLCTimestamp.Zero);
    }

    /// <summary>
    /// Installs a fully-received snapshot on this partition's single-writer executor thread and returns
    /// whether it succeeded. Asked (not posted) so the transport-side caller learns the outcome and does
    /// not dispose the staged buffer until the executor has finished importing from it. Intentionally
    /// uses the no-cancellation <c>Ask</c> so a transport-side cancellation cannot complete this task
    /// (and free the buffer) while the install is still running on the executor.
    /// </summary>
    internal async Task<bool> InstallSnapshotAsync(SnapshotInstallRequest request)
    {
        RaftResponse response = await executor.Ask(new(RaftRequestType.InstallSnapshot, request)).ConfigureAwait(false);
        return response.Status == RaftOperationStatus.Success;
    }

    public async Task<RaftOperationStatus> ForceLeaderForTestingAsync(CancellationToken cancellationToken = default)
    {
        RaftResponse response = await executor.Ask(ForceLeaderForTestingRequest, cancellationToken).ConfigureAwait(false);
        return response.Status;
    }

    /// <summary>
    /// Test-only: returns an immutable snapshot of this partition's consensus state, captured on the
    /// executor thread. Used by the chaos harness's continuous invariant checker.
    /// </summary>
    internal async Task<RaftPartitionView?> GetPartitionViewAsync(CancellationToken cancellationToken = default)
    {
        RaftResponse response = await executor.Ask(GetPartitionViewRequest, cancellationToken).ConfigureAwait(false);
        return response.PartitionView;
    }

    public async Task<RaftOperationStatus> StepDownAsync(CancellationToken cancellationToken = default)
    {
        RaftResponse response = await executor.Ask(StepDownRequest, cancellationToken).ConfigureAwait(false);
        return response.Status;
    }

    /// <summary>
    /// Read-index leadership confirmation: asks the state machine to prove leadership with a
    /// same-term quorum ack round and to wait until the applied frontier covers the commit index
    /// captured at confirmation. Returns <see langword="true"/> only when a local read served
    /// afterwards is linearizable; every non-success outcome (not leader, quorum timeout,
    /// admission-control rejection, restore in progress) maps to <see langword="false"/> so the
    /// caller retries or redirects, mirroring the write path.
    /// <para>Fast path: a leadership lease published within the last heartbeat interval (with the
    /// applied frontier caught up) confirms synchronously without touching the executor queue —
    /// the async machinery then completes with the runtime's cached <c>true</c> task, so a hit
    /// allocates nothing. A miss falls through to the full executor round; it never fails early.</para>
    /// </summary>
    public async Task<bool> ConfirmLeadershipAsync(CancellationToken cancellationToken = default)
    {
        if (stateMachine.TryConfirmLeadershipFast())
            return true;

        RaftResponse response = await executor.Ask(ConfirmLeadershipRequest, cancellationToken).ConfigureAwait(false);
        return response.Status == RaftOperationStatus.Success;
    }

    /// <summary>
    /// Leader-side answer to a non-leader's read-index fetch: runs the same confirmation as
    /// <see cref="ConfirmLeadershipAsync"/> but surfaces the quorum-confirmed commit index the
    /// round captured (carried on the reply's <see cref="RaftResponse.LogIndex"/>) so a remote
    /// follower can wait its own applied frontier up to it. Coalesces with concurrent local
    /// confirmations — remote fetches add no extra ack rounds.
    /// </summary>
    internal async Task<(RaftOperationStatus Status, long ReadIndex)> GetConfirmedReadIndexAsync(CancellationToken cancellationToken = default)
    {
        RaftResponse response = await executor.Ask(ConfirmLeadershipRequest, cancellationToken).ConfigureAwait(false);
        return (response.Status, response.LogIndex);
    }

    /// <summary>
    /// Non-leader half of <c>IRaft.ConfirmLocalApplicationAsync</c>: waits (bounded by
    /// <see cref="RaftConfiguration.LeadershipConfirmationTimeout"/>, enforced from the partition
    /// tick) until this node's applied frontier covers <paramref name="requiredIndex"/> — a commit
    /// index the partition leader confirmed via a quorum ack round. Non-success (timeout,
    /// leadership transition, restore in progress, admission control) maps to
    /// <see langword="false"/>: the caller must skip its destructive action.
    /// </summary>
    internal async Task<bool> WaitLocalApplicationAsync(long requiredIndex, CancellationToken cancellationToken = default)
    {
        RaftResponse response = await executor.Ask(
            new(RaftRequestType.WaitLocalApplication, commitIndex: requiredIndex),
            cancellationToken).ConfigureAwait(false);
        return response.Status == RaftOperationStatus.Success;
    }

    public async Task<RaftOperationStatus> TransferLeadershipAsync(
        string targetEndpoint,
        CancellationToken cancellationToken = default)
    {
        RaftResponse response = await executor.Ask(
            new(RaftRequestType.TransferLeadership, endpoint: targetEndpoint),
            cancellationToken).ConfigureAwait(false);
        return response.Status;
    }

    public async Task<RaftOperationStatus> SuspendHeartbeatsAsync(CancellationToken cancellationToken = default)
    {
        RaftResponse response = await executor.Ask(SuspendHeartbeatsRequest, cancellationToken).ConfigureAwait(false);
        return response.Status;
    }

    public async Task<RaftOperationStatus> ResumeHeartbeatsAsync(CancellationToken cancellationToken = default)
    {
        RaftResponse response = await executor.Ask(ResumeHeartbeatsRequest, cancellationToken).ConfigureAwait(false);
        return response.Status;
    }

    /// <summary>
    /// Waits until the same non-empty leader endpoint has remained stable for at least
    /// <paramref name="minStableFor"/>. <paramref name="minStableFor"/> is a required stability
    /// window, not a deadline: without <paramref name="timeout"/> the only exit besides success is
    /// the cancellation token, so a churning partition makes this wait forever. Callers must pass
    /// a bounded <paramref name="timeout"/> (throws <see cref="TimeoutException"/> on expiry) or a
    /// cancelling token.
    /// </summary>
    internal async ValueTask<string> WaitForLeaderStableAsync(
        TimeSpan minStableFor,
        TimeSpan? timeout = null,
        CancellationToken cancellationToken = default)
    {
        TimeSpan requiredStableFor = minStableFor <= TimeSpan.Zero ? TimeSpan.Zero : minStableFor;
        long startTicks = manager.Configuration.TickSource.GetTimestamp();

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (timeout.HasValue && ValueStopwatch.GetElapsedTime(startTicks, manager.Configuration.TickSource.GetTimestamp()) >= timeout.Value)
                throw new TimeoutException(
                    $"Leader for partition {PartitionId} did not remain stable for {requiredStableFor.TotalMilliseconds}ms within {timeout.Value.TotalMilliseconds}ms");

            string leader = Leader;

            if (string.IsNullOrEmpty(leader))
            {
                try
                {
                    if (await GetState(cancellationToken).ConfigureAwait(false) == RaftNodeState.Leader)
                        leader = manager.LocalEndpoint;
                }
                catch (Exception e) when (e is not OperationCanceledException)
                {
                    manager.Logger.LogWarning("WaitForLeaderStableAsync: {Message}", e.Message);
                }
            }

            if (!string.IsNullOrEmpty(leader))
            {
                if (requiredStableFor == TimeSpan.Zero)
                    return leader;

                TimeSpan stableFor = ValueStopwatch.GetElapsedTime(LeaderChangedTicks, manager.Configuration.TickSource.GetTimestamp());
                if (stableFor >= requiredStableFor)
                    return leader;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(10), cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Retrieves the current state of the Raft node.
    /// </summary>
    /// <returns>The current state of the node as <see cref="RaftNodeState"/>.</returns>
    public ValueTask<RaftNodeState> GetState() => GetState(CancellationToken.None);

    /// <summary>
    /// Reads the node's role from the volatile snapshot published by the state machine — no
    /// executor round-trip. This is deliberate: <c>GetState</c> backs
    /// <c>AmILeader</c>/<c>AmILeaderQuick</c>, and a caller polling those in a delay-free loop
    /// used to enqueue an unbounded stream of <c>GetNodeState</c> Asks whose scheduling churn
    /// starved election/heartbeat work, livelocking the very leadership convergence the caller
    /// was waiting on (observed as a permanent 500%+-CPU hang). Serving the role from a snapshot
    /// makes hot pollers cost nothing on the executor.
    /// <para>Semantics: the answer is a point-in-time snapshot, not a happens-after barrier over
    /// previously enqueued executor work. It never reports <see cref="RaftNodeState.Leader"/>
    /// earlier than the Ask-served answer would — <see cref="RaftPartitionStateMachine.NodeState"/>
    /// reports <c>Candidate</c> until leadership is published — but it may report a role one
    /// transition stale. Callers that need the queued-work ordering must use
    /// <see cref="GetStateSlow"/>.</para>
    /// </summary>
    public ValueTask<RaftNodeState> GetState(CancellationToken cancellationToken)
    {
        if (cancellationToken.IsCancellationRequested)
            return ValueTask.FromCanceled<RaftNodeState>(cancellationToken);

        if (!string.IsNullOrEmpty(Leader) && Leader == manager.LocalEndpoint)
            return new(RaftNodeState.Leader);

        return new(stateMachine.NodeState);
    }

    /// <summary>
    /// Executor-served variant of <see cref="GetState()"/>: the reply is produced on the
    /// single-writer thread, so it observes every operation enqueued before this call. Costs one
    /// round-trip and shares the client queue with proposals — never call it in a poll loop; use
    /// <see cref="GetState()"/> instead.
    /// <para>Because the request is client-class it is also subject to the executor's restore gate:
    /// until the partition finishes replaying its log the Ask is answered with
    /// <see cref="RaftOperationStatus.RestoreInProgress"/>, which surfaces here as a
    /// <see cref="RaftException"/>. The snapshot path has no such window — another reason
    /// <see cref="GetState()"/> is the right call for anything resembling a poll.</para>
    /// </summary>
    /// <exception cref="RaftException">
    /// Thrown if the response is invalid or cannot determine the node state.
    /// </exception>
    public async ValueTask<RaftNodeState> GetStateSlow(CancellationToken cancellationToken = default)
    {
        RaftResponse? response = await executor.Ask(RaftStateRequest, cancellationToken).ConfigureAwait(false);

        if (response is null)
            throw new RaftException("Unknown response (1)");

        if (response.Type != RaftResponseType.NodeState)
            throw new RaftException("Unknown response (2)");

        return response.NodeState;
    }

    /// <summary>
    /// Retrieves the state of a ticket and its associated commit ID from the Raft partition.
    /// Proposals produce a ticket ID that can be used to track the state of the proposal.
    /// </summary>
    /// <param name="ticketId">The unique identifier of the ticket based on a hybrid logical clock timestamp.</param>
    /// <param name="autoCommit">A flag indicating whether the ticket should be automatically committed if not found in a final state.</param>
    /// <returns>A tuple containing the state of the ticket (<see cref="RaftProposalTicketState"/>) and the commit ID as a long value.</returns>
    /// <exception cref="RaftException">Thrown when an unexpected or unknown response is received from the Raft actor.</exception>
    public async Task<(RaftProposalTicketState state, long commitId)> GetTicketState(HLCTimestamp ticketId, bool autoCommit)
    {
        RaftResponse? response = await executor.Ask(new(RaftRequestType.GetTicketState, ticketId, autoCommit)).ConfigureAwait(false);
        
        if (response is null)
            throw new RaftException("Unknown response (1)");
        
        if (response.Type != RaftResponseType.TicketState)
            throw new RaftException("Unknown response (2)");

        return (response.ProposalTicketState, response.LogIndex);
    }
    
    /// <summary>
    /// Returns the event-driven completion task for the active proposal identified by
    /// <paramref name="ticketId"/>, or <c>null</c> if the proposal is no longer in
    /// <c>activeProposals</c>. The returned task completes when the proposal reaches a
    /// terminal state (committed, rolled-back, or invalidated by leader loss), allowing
    /// callers to await it directly instead of polling <see cref="GetTicketState"/>.
    /// <para>
    /// One executor round-trip is incurred per write call to retrieve the task; subsequent
    /// progress is delivered without executor involvement as the state machine fires
    /// <see cref="RaftProposalQuorum.CompleteWaiter"/> on the terminal transition.
    /// </para>
    /// </summary>
    public async Task<Task<(RaftProposalTicketState, long)>?> GetTicketWaiterTaskAsync(HLCTimestamp ticketId)
    {
        RaftResponse? response = await executor.Ask(new(RaftRequestType.GetTicketWaiterTask, ticketId, false)).ConfigureAwait(false);

        if (response is null || response.Type != RaftResponseType.TicketWaiterTask)
            return null;

        return response.WaiterTask;
    }

    /// <summary>
    /// Returns the last commit index acknowledged by <paramref name="endpoint"/> on this
    /// partition, or -1 when no <c>CompleteAppendLogs</c> has been received from that endpoint.
    /// Runs on the executor thread so it is safe to read <c>lastCommitIndexes</c>.
    /// </summary>
    public async ValueTask<long> GetFollowerCommittedIndexAsync(string endpoint)
    {
        RaftResponse? response = await executor.Ask(new(RaftRequestType.GetFollowerCommittedIndex, endpoint: endpoint)).ConfigureAwait(false);
        if (response is null || response.Type != RaftResponseType.FollowerCommittedIndex)
            return -1;
        // long.MinValue is the state machine's sentinel for "never heard from"; normalize to -1.
        return response.LogIndex == long.MinValue ? -1 : response.LogIndex;
    }

    /// <summary>
    /// Nullable variant: returns <c>null</c> when the endpoint has never sent a
    /// <c>CompleteAppendLogs</c> for this partition (distinguished from −1, which means
    /// "heard from but no committed entries yet").
    /// </summary>
    public async ValueTask<long?> GetFollowerCommittedIndexNullableAsync(string endpoint)
    {
        RaftResponse? response = await executor.Ask(new(RaftRequestType.GetFollowerCommittedIndex, endpoint: endpoint)).ConfigureAwait(false);
        if (response is null || response.Type != RaftResponseType.FollowerCommittedIndex)
            return null;
        return response.LogIndex == long.MinValue ? null : response.LogIndex;
    }

    /// <summary>
    /// Sends a CheckLeader message to the raft state actor to check for leader changes
    /// </summary>
    public void CheckLeader()
    {
        executor.Post(CheckLeaderRequest);
    }

    /// <summary>
    /// Stops the partition's executor thread without releasing resources.
    /// Safe to call multiple times. Call <see cref="Dispose"/> to release all resources.
    /// </summary>
    public void Stop()
    {
        executor.Stop();
        executor.ResetTestingState();
    }

    internal Task DrainAsync(CancellationToken cancellationToken = default) =>
        executor.DrainAsync(cancellationToken);

    /// <summary>Completes when the partition's WAL restore (Phase 2) has finished.</summary>
    internal Task RestoreTask => executor.RestoreTask;

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        semaphore.Dispose();
        executor.Stop();
        executor.ResetTestingState();
        executor.Dispose();
    }
}
