
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

    private readonly RaftManager manager;

    private readonly RaftWriteAhead walHandler;

    private int _disposed;

    private string _leader = "";
    private long _leaderChangedTicks = Stopwatch.GetTimestamp();

    internal string Leader
    {
        get => _leader;
        set
        {
            if (string.Equals(_leader, value, StringComparison.Ordinal))
                return;

            _leader = value;
            Interlocked.Exchange(ref _leaderChangedTicks, Stopwatch.GetTimestamp());
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
        RaftPartitionStateMachine stateMachine = new(host, wal, replySink, logger);

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
            pool:                    pool);
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
    /// Sets the minimum WAL log index that compaction must not truncate below, regardless of
    /// the checkpoint position. Forwarded directly to the underlying <see cref="RaftWriteAhead"/>;
    /// the write is volatile so the next compaction pass observes it immediately with no
    /// scheduling round-trip. See <see cref="RaftWriteAhead.SetMinRetainIndex"/> for semantics.
    /// </summary>
    public void SetMinRetainIndex(long index) => walHandler.SetMinRetainIndex(index);

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

        RaftResponse response = await executor.Ask(new(RaftRequestType.ReplicateLogs, logsToReplicate, autoCommit, expectedGeneration)).ConfigureAwait(false);

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

        RaftResponse response = await executor.Ask(new(RaftRequestType.ReplicateLogs, logsToReplicate, autoCommit, expectedGeneration)).ConfigureAwait(false);

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
    /// <param name="cancellationToken">Optional deadline; default leaves the executor await unbounded.</param>
    /// <returns>A tuple containing a boolean indicating success, the status of the operation as <see cref="RaftOperationStatus"/>, and the commit index of the logs.</returns>
    public async Task<(bool success, RaftOperationStatus status, long commitIndex)> CommitLogs(HLCTimestamp ticketId, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrEmpty(Leader))
            return (false, RaftOperationStatus.NodeIsNotLeader, 0);

        if (Leader != manager.LocalEndpoint)
            return (false, RaftOperationStatus.NodeIsNotLeader, 0);

        try
        {
            RaftResponse response = await executor.Ask(new(RaftRequestType.CommitLogs, ticketId, false), cancellationToken).ConfigureAwait(false);

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
    /// <param name="cancellationToken">Optional deadline; default leaves the executor await unbounded.</param>
    /// <returns>A tuple indicating the success of the operation, the operation status, and the commit index.</returns>
    public async Task<(bool success, RaftOperationStatus status, long commitIndex)> RollbackLogs(HLCTimestamp ticketId, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrEmpty(Leader))
            return (false, RaftOperationStatus.NodeIsNotLeader, 0);

        if (Leader != manager.LocalEndpoint)
            return (false, RaftOperationStatus.NodeIsNotLeader, 0);

        try
        {
            RaftResponse response = await executor.Ask(new(RaftRequestType.RollbackLogs, ticketId, false), cancellationToken).ConfigureAwait(false);

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
        
        RaftResponse response = await executor.Ask(new(RaftRequestType.ReplicateCheckpoint)).ConfigureAwait(false);
        
        if (response.Status == RaftOperationStatus.Success)
            return (true, response.Status, response.TicketId);

        return (false, response.Status, HLCTimestamp.Zero);
    }

    public async Task<RaftOperationStatus> ForceLeaderForTestingAsync(CancellationToken cancellationToken = default)
    {
        RaftResponse response = await executor.Ask(new(RaftRequestType.ForceLeaderForTesting), cancellationToken).ConfigureAwait(false);
        return response.Status;
    }

    public async Task<RaftOperationStatus> StepDownAsync(CancellationToken cancellationToken = default)
    {
        RaftResponse response = await executor.Ask(new(RaftRequestType.StepDown), cancellationToken).ConfigureAwait(false);
        return response.Status;
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
        RaftResponse response = await executor.Ask(new(RaftRequestType.SuspendHeartbeats), cancellationToken).ConfigureAwait(false);
        return response.Status;
    }

    public async Task<RaftOperationStatus> ResumeHeartbeatsAsync(CancellationToken cancellationToken = default)
    {
        RaftResponse response = await executor.Ask(new(RaftRequestType.ResumeHeartbeats), cancellationToken).ConfigureAwait(false);
        return response.Status;
    }

    internal async ValueTask<string> WaitForLeaderStableAsync(
        TimeSpan minStableFor,
        CancellationToken cancellationToken = default)
    {
        TimeSpan requiredStableFor = minStableFor <= TimeSpan.Zero ? TimeSpan.Zero : minStableFor;

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

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

                TimeSpan stableFor = ValueStopwatch.GetElapsedTime(LeaderChangedTicks, Stopwatch.GetTimestamp());
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
    /// <exception cref="RaftException">
    /// Thrown if the response is invalid or cannot determine the node state.
    /// </exception>
    public ValueTask<RaftNodeState> GetState() => GetState(CancellationToken.None);

    public async ValueTask<RaftNodeState> GetState(CancellationToken cancellationToken)
    {
        if (!string.IsNullOrEmpty(Leader) && Leader == manager.LocalEndpoint)
            return RaftNodeState.Leader;

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
        executor.Post(new(RaftRequestType.CheckLeader));
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
