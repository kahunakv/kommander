using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using Kommander.Data;
using Kommander.Diagnostics;
using Kommander.Scheduling;
using Kommander.WAL;
using Kommander.WAL.Data;
using Microsoft.Extensions.Logging;

namespace Kommander.Scheduling;

/// <summary>
/// Serial executor for a single Raft partition.
///
/// <para><b>Design responsibilities:</b></para>
/// <list type="bullet">
///   <item>Guarantees exactly one concurrent state-machine transition per partition
///       (single-owner rule, invariant 1 of the non-negotiable correctness rules).</item>
///   <item>Drains queued operations in weighted-fair order across the four priority
///       classes — control (8), replication (4), client (2), maintenance (1) — so
///       heartbeats and vote traffic are never starved by client proposals.</item>
///   <item>Batches same-type replication messages into a single state-machine call
///       (mirrors the actor adapter's batch behaviour).</item>
///   <item>Provides <see cref="Post"/> (fire-and-forget) and
///       <see cref="Ask"/> (reply-awaiting) dispatch surfaces — no Nixie
///       dependency.</item>
///   <item>Stops cleanly: pending replies are cancelled and the worker thread joins
///       before <see cref="Stop"/> returns.</item>
/// </list>
///
/// <para>This class does not directly perform any WAL I/O; it delegates to
/// <see cref="RaftPartitionStateMachine"/> which drives I/O through
/// <see cref="IRaftWalFacade"/>.</para>
/// </summary>
public sealed class RaftPartitionExecutor : IDisposable
{
    // ── Operation envelope ─────────────────────────────────────────────────

    /// <summary>
    /// Wraps a <see cref="RaftRequest"/> together with an optional reply channel.
    /// </summary>
    private sealed class PendingOperation
    {
        public RaftRequest Request { get; }

        /// <summary>
        /// Non-null when the caller awaits a reply (Ask pattern).
        /// Null for fire-and-forget (Post pattern).
        /// </summary>
        public TaskCompletionSource<RaftResponse>? Reply { get; }

        public PendingOperation(RaftRequest request, TaskCompletionSource<RaftResponse>? reply)
        {
            Request = request;
            Reply = reply;
        }
    }

    // ── Priority queues ────────────────────────────────────────────────────

    // One concurrent queue per priority class. Using separate queues avoids
    // a single ConcurrentQueue that would need priority inspection on every dequeue.
    // Control, replication, and maintenance queues are unbounded; the client queue is
    // bounded to prevent proposal floods from starving control-plane traffic or
    // consuming unbounded memory under backpressure.

    private readonly ConcurrentQueue<PendingOperation> _controlQueue = new();
    private readonly ConcurrentQueue<PendingOperation> _replicationQueue = new();
    private readonly ConcurrentQueue<PendingOperation> _clientQueue = new();
    private readonly ConcurrentQueue<PendingOperation> _maintenanceQueue = new();

    // Tracks items currently in _clientQueue (pending, not yet dequeued by the worker).
    // Compared against _maxClientQueueDepth on every client enqueue for admission control.
    // Interlocked so producers can read/write concurrently without taking a lock.
    private int _clientQueueDepth;

    // 0 or negative means unbounded (not recommended for production use).
    private readonly int _maxClientQueueDepth;

    // Per-cycle drain quanta (max items drained from each priority class per wake).
    private readonly int _drainQuantumControl;
    private readonly int _drainQuantumReplication;
    private readonly int _drainQuantumClient;
    private readonly int _drainQuantumMaintenance;

    // ── Wakeup mechanism ───────────────────────────────────────────────────

    // SemaphoreSlim used to wake the single worker thread whenever new work arrives.
    // Max count is set large so producer bursts don't block.
    private readonly SemaphoreSlim _workAvailable = new(0, int.MaxValue);

    // ── Worker thread ──────────────────────────────────────────────────────

    private readonly Thread _worker;
    private readonly CancellationTokenSource _cts = new();
    private volatile bool _stopping;
    private bool _started;

    // ── Per-execution context ──────────────────────────────────────────────

    private readonly RaftPartitionStateMachine _stateMachine;
    private readonly ILogger<IRaft> _logger;
    private readonly int _partitionId;
    private readonly Func<long>? _getGeneration;

    // Slow-message threshold (ms) — same as actor adapter uses.
    private readonly int _slowThresholdMs;

    // Reply correlation: maps correlationId -> PendingOperation that expects a reply.
    private readonly Dictionary<ulong, PendingOperation> _pendingReplies = [];
    private ulong _nextCorrelationId = 1;

    // Batch scratch list to avoid per-drain allocations.
    private readonly List<PendingOperation> _drainBatch = new(256);

    // ── Restore state ──────────────────────────────────────────────────────

    // Set to true once the RestoreLogsLoaded event has been fully processed by
    // the worker thread.  Client proposals are rejected with RestoreInProgress
    // until this flag is true.  Volatile so producers see the update promptly.
    private volatile bool _restoreCompleted;

    // Completes when Phase 2 of the restore (log replay) finishes on the worker
    // thread.  Faulted if either phase fails.  Callers that need to know restore
    // is done before issuing client ops can await this task.
    private readonly TaskCompletionSource _restoreTcs =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    // ── Load tracking ─────────────────────────────────────────────────────

    private readonly PartitionLoadAccumulator _loadAccumulator;

    /// <summary>
    /// Tracks the EWMA rate of <c>ReplicateLogs</c> operations — the leader-side
    /// log-replication path only. <c>AppendLogs</c> (follower replay), checkpoints, and
    /// commits are deliberately excluded so this accumulator reflects originating write
    /// load, not protocol overhead. Leader-only by type: only leaders issue
    /// <c>ReplicateLogs</c>; followers receive <c>AppendLogs</c> instead.
    /// </summary>
    private readonly PartitionLoadAccumulator _logLoadAccumulator;

    private readonly Func<int>? _walQueueDepthProvider;

    // ── Observability ──────────────────────────────────────────────────────

    private long _totalProcessed;
    private long _totalClientRejected;
    private long _totalBatchesExecuted;

    /// <summary>Total number of operations processed by this executor since start.</summary>
    public long TotalProcessed => Interlocked.Read(ref _totalProcessed);

    /// <summary>Total number of client proposals rejected due to queue saturation.</summary>
    public long TotalClientRejected => Interlocked.Read(ref _totalClientRejected);

    /// <summary>
    /// Total number of times <c>ExecuteBatchAsync</c> ran — i.e. drain cycles where
    /// two or more contiguous <c>ReplicateLogs</c> ops were coalesced into a single
    /// state-machine call. Exposed for tests that need to verify the batch path was
    /// exercised, not just that the accumulator recorded ops.
    /// </summary>
    internal long TotalBatchesExecuted => Interlocked.Read(ref _totalBatchesExecuted);

    /// <summary>
    /// Current number of client proposals pending in the queue (not yet dequeued by
    /// the worker).  When the capacity limit is active, this value is clamped to the
    /// capacity to avoid exposing the brief transient overshoot that can occur between
    /// the atomic increment and the rejection decrement in the admission-control gate.
    /// </summary>
    public int ClientQueueDepth
    {
        get
        {
            int d = Interlocked.CompareExchange(ref _clientQueueDepth, 0, 0);
            return _maxClientQueueDepth > 0 ? Math.Min(d, _maxClientQueueDepth) : d;
        }
    }

    /// <summary>
    /// Configured maximum for the client queue.
    /// 0 or negative means the queue is unbounded.
    /// </summary>
    public int ClientQueueCapacity => _maxClientQueueDepth;

    /// <summary>
    /// Advisory composite load score for this partition, combining EWMA ops/sec and
    /// instantaneous queue backlog. See <see cref="PartitionLoadAccumulator.CurrentLoad"/>.
    /// </summary>
    public double CurrentLoad(double wOps, double wQueue) =>
        _loadAccumulator.CurrentLoad(wOps, wQueue, ClientQueueDepth, _walQueueDepthProvider?.Invoke() ?? 0);

    /// <summary>EWMA ops/sec estimate. Exposed for diagnostics and tests.</summary>
    internal double CurrentOpsPerSecond() => _loadAccumulator.CurrentOpsPerSecond();

    /// <summary>
    /// EWMA rate of <c>ReplicateLogs</c> operations per second on this partition.
    /// Reflects only the leader-side log-replication path (path-not-label: keyed on the
    /// request type, not the node role). Returns 0 on follower nodes because they process
    /// <c>AppendLogs</c>, not <c>ReplicateLogs</c>.
    /// </summary>
    internal double CurrentLogOpsPerSecond() => _logLoadAccumulator.CurrentOpsPerSecond();

    /// <summary>
    /// True once Phase 2 of the partition restore (log replay) has completed on
    /// the executor thread.  Client proposals are rejected with
    /// <see cref="RaftOperationStatus.RestoreInProgress"/> until this is true.
    /// </summary>
    public bool IsRestored => _restoreCompleted;

    /// <summary>
    /// A task that completes when the partition restore finishes successfully,
    /// or faults if either restore phase fails.  Await this before issuing client
    /// operations when you need a hard guarantee that restore is complete.
    /// </summary>
    public Task RestoreTask => _restoreTcs.Task;

    /// <summary>Id of the partition owned by this executor.</summary>
    public int PartitionId => _partitionId;

    // ── Construction ──────────────────────────────────────────────────────

    /// <summary>
    /// Creates (but does not start) a partition executor.
    /// </summary>
    /// <param name="stateMachine">The partition state machine this executor drives.</param>
    /// <param name="partitionId">Numeric id of the owned partition.</param>
    /// <param name="slowThresholdMs">
    /// Log a warning when a single state-machine dispatch takes longer than this
    /// many milliseconds.  Use 0 to disable.
    /// </param>
    /// <param name="logger">Logger.</param>
    /// <param name="maxClientQueueDepth">
    /// Maximum number of client proposals that may be queued at one time.
    /// When the limit is reached new proposals are rejected immediately with
    /// <see cref="RaftOperationStatus.ProposalQueueFull"/> rather than blocking.
    /// Use 0 or a negative value to disable the limit (not recommended in production).
    /// </param>
    /// <param name="drainQuantumControl">Max control ops drained per wake cycle. 0 = use default.</param>
    /// <param name="drainQuantumReplication">Max replication ops drained per wake cycle. 0 = use default.</param>
    /// <param name="drainQuantumClient">Max client ops drained per wake cycle. 0 = use default.</param>
    /// <param name="drainQuantumMaintenance">Max maintenance ops drained per wake cycle. 0 = use default.</param>
    /// <param name="walQueueDepthProvider">
    /// Optional delegate returning the current WAL pending-or-in-flight depth for this
    /// partition. Used in the composite load score. Null disables the WAL depth term.
    /// </param>
    public RaftPartitionExecutor(
        RaftPartitionStateMachine stateMachine,
        int partitionId,
        int slowThresholdMs,
        ILogger<IRaft> logger,
        int maxClientQueueDepth = 0,
        int drainQuantumControl = 0,
        int drainQuantumReplication = 0,
        int drainQuantumClient = 0,
        int drainQuantumMaintenance = 0,
        Func<long>? getGeneration = null,
        Func<int>? walQueueDepthProvider = null)
    {
        _stateMachine = stateMachine;
        _partitionId = partitionId;
        _slowThresholdMs = slowThresholdMs;
        _logger = logger;
        _getGeneration = getGeneration;
        _maxClientQueueDepth = maxClientQueueDepth;
        _drainQuantumControl     = drainQuantumControl     > 0 ? drainQuantumControl     : (int)RaftOperationPriority.Control;
        _drainQuantumReplication = drainQuantumReplication > 0 ? drainQuantumReplication : (int)RaftOperationPriority.Replication;
        _drainQuantumClient      = drainQuantumClient      > 0 ? drainQuantumClient      : (int)RaftOperationPriority.Client;
        _drainQuantumMaintenance = drainQuantumMaintenance > 0 ? drainQuantumMaintenance : (int)RaftOperationPriority.Maintenance;
        _walQueueDepthProvider = walQueueDepthProvider;
        _loadAccumulator = new PartitionLoadAccumulator();
        _logLoadAccumulator = new PartitionLoadAccumulator();

        _worker = new Thread(WorkerLoop)
        {
            IsBackground = true,
            Name = $"RaftPartitionExecutor-{partitionId}",
        };

        KommanderMetrics.RegisterExecutor(this);
    }

    // ── Public API ─────────────────────────────────────────────────────────

    /// <summary>Starts the background worker thread.</summary>
    public void Start()
    {
        if (_started)
            return;

        _started = true;
        _worker.Start();
    }

    /// <summary>
    /// Posts a fire-and-forget operation to the executor.
    /// Returns immediately; the operation is processed asynchronously.
    /// </summary>
    /// <exception cref="InvalidOperationException">If the executor is stopping or not started.</exception>
    public void Post(RaftRequest request)
    {
        ThrowIfNotReady();
        Enqueue(request, reply: null);
    }

    /// <summary>
    /// Posts an operation and returns a <see cref="Task{RaftResponse}"/> that completes
    /// once the state machine has processed the request and produced a reply.
    /// </summary>
    /// <exception cref="InvalidOperationException">If the executor is stopping or not started.</exception>
    public Task<RaftResponse> Ask(RaftRequest request, CancellationToken cancellationToken = default)
    {
        ThrowIfNotReady();

        TaskCompletionSource<RaftResponse> tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

        if (cancellationToken.CanBeCanceled)
            cancellationToken.Register(() => tcs.TrySetCanceled(cancellationToken));

        Enqueue(request, tcs);
        return tcs.Task;
    }

    /// <summary>
    /// Enqueues a maintenance barrier and completes when the executor has processed
    /// all currently reachable higher-priority work ahead of that barrier.
    /// </summary>
    public Task DrainAsync(CancellationToken cancellationToken = default)
    {
        if (!_started || _stopping)
            return Task.CompletedTask;

        // Guard the Ask call: a background task (e.g. WAL restore on the I/O scheduler)
        // may set _stopping between the check above and ThrowIfNotReady inside Ask — a
        // TOCTOU that is benign here because DrainAsync is already best-effort.
        try { return Ask(new RaftRequest(RaftRequestType.DrainBarrier), cancellationToken); }
        catch (InvalidOperationException) { return Task.CompletedTask; }
    }

    /// <summary>
    /// Stops the executor.
    ///
    /// <para>Any operations that are already queued will be processed up to the point
    /// where cancellation fires.  Pending <see cref="Ask"/> tasks that have not yet
    /// received a reply are cancelled.</para>
    /// </summary>
    public void Stop()
    {
        _stopping = true;
        _cts.Cancel();
        // Release the semaphore so the blocked worker wakes up and notices cancellation.
        _workAvailable.Release();
        _worker.Join();
    }

    public void ResetTestingState()
    {
        _stateMachine.ResetTestingState();
    }

    // ── Internal helpers ───────────────────────────────────────────────────

    private void ThrowIfNotReady()
    {
        if (!_started)
            throw new InvalidOperationException($"RaftPartitionExecutor({_partitionId}): call Start() before posting operations.");
        if (_stopping)
            throw new InvalidOperationException($"RaftPartitionExecutor({_partitionId}): executor is stopping; no new operations accepted.");
    }

    private void Enqueue(RaftRequest request, TaskCompletionSource<RaftResponse>? reply)
    {
        RaftOperationKind kind = RaftOperationMapper.GetKind(request.Type);

        // Restore gate: reject client proposals until Phase 2 of the partition restore
        // has completed.  Control, replication, and maintenance ops are allowed through
        // so that heartbeats, election traffic, and the RestoreLogsLoaded event itself
        // can be processed while the restore is in progress.
        if (kind == RaftOperationKind.Client && !_restoreCompleted)
        {
            Interlocked.Increment(ref _totalClientRejected);
            KommanderMetrics.ExecutorRejectionsTotal.Add(1,
                new KeyValuePair<string, object?>("partition_id", _partitionId));
            reply?.TrySetResult(RaftResponseStatic.RestoreInProgressResponse);
            return;
        }

        // Admission control: reject client proposals when the queue is full.
        // Control, replication, and maintenance queues are never bounded so
        // control-plane traffic cannot be starved (non-negotiable correctness rule 6).
        //
        // We use an increment-first pattern to avoid a read-check-increment race:
        //   1. Atomically increment the counter (reserves a slot).
        //   2. If the post-increment value exceeds the cap, release the slot and reject.
        // This guarantees depth never exceeds _maxClientQueueDepth even under heavy
        // concurrent load from many producer threads.
        if (kind == RaftOperationKind.Client && _maxClientQueueDepth > 0)
        {
            int after = Interlocked.Increment(ref _clientQueueDepth);
            if (after > _maxClientQueueDepth)
            {
                Interlocked.Decrement(ref _clientQueueDepth);
                Interlocked.Increment(ref _totalClientRejected);
                KommanderMetrics.ExecutorRejectionsTotal.Add(1,
                    new KeyValuePair<string, object?>("partition_id", _partitionId));
                _logger.LogWarning(
                    "[RaftPartitionExecutor/{PartitionId}] Client queue full ({Depth}/{Capacity}); rejecting {Type}",
                    _partitionId, after - 1, _maxClientQueueDepth, request.Type);
                reply?.TrySetResult(RaftResponseStatic.ProposalQueueFullResponse);
                return;
            }

            // Slot claimed — enqueue and wake the worker.
            _clientQueue.Enqueue(new PendingOperation(request, reply));
            _workAvailable.Release();
            return;
        }

        PendingOperation op = new(request, reply);

        switch (kind)
        {
            case RaftOperationKind.Control:
                _controlQueue.Enqueue(op);
                break;
            case RaftOperationKind.Replication:
                _replicationQueue.Enqueue(op);
                break;
            case RaftOperationKind.Client:
                // Unbounded path (maxClientQueueDepth <= 0).
                _clientQueue.Enqueue(op);
                break;
            case RaftOperationKind.Maintenance:
                _maintenanceQueue.Enqueue(op);
                break;
        }

        _workAvailable.Release();
    }

    // ── Worker loop ────────────────────────────────────────────────────────

    private void WorkerLoop()
    {
        CancellationToken token = _cts.Token;

        // Phase 1 of nonblocking restore: kick off WAL log loading on the I/O
        // scheduler (thread pool).  The task posts RestoreLogsLoaded back to this
        // executor when done so Phase 2 (replay) runs on this thread under the
        // single-owner guarantee.  The worker loop starts immediately — no blocking.
        _ = Task.Run(() => RunRestorePhase1Async(token));

        while (true)
        {
            try
            {
                _workAvailable.Wait(token);
            }
            catch (OperationCanceledException)
            {
                DrainAll();
                CancelPendingReplies();
                break;
            }

            // Drain all queues in weighted-fair order.
            DrainQueues();

            if (token.IsCancellationRequested && AreQueuesEmpty())
            {
                CancelPendingReplies();
                break;
            }
        }
    }

    private async Task RunRestorePhase1Async(CancellationToken token)
    {
        try
        {
            IReadOnlyList<RaftLog> logs = await _stateMachine.StartRestoreAsync().ConfigureAwait(false);

            // Deliver logs back to the executor for Phase 2 replay on the worker thread.
            _maintenanceQueue.Enqueue(new PendingOperation(
                new RaftRequest(RaftRequestType.RestoreLogsLoaded, logs),
                reply: null));
            _workAvailable.Release();
        }
        catch (Exception ex) when (!token.IsCancellationRequested)
        {
            _logger.LogError(
                "[RaftPartitionExecutor/{PartitionId}] WAL restore (Phase 1) failed; partition will not process operations: {Message}\n{StackTrace}",
                _partitionId, ex.Message, ex.StackTrace);

            _restoreTcs.TrySetException(ex);
            _stopping = true;
            _cts.Cancel();
            DrainAll();
            CancelPendingReplies();
        }
    }

    /// <summary>
    /// Weighted-fair drain using configurable per-class quanta.
    /// Defaults: Control×8, Replication×4, Client×2, Maintenance×1.
    /// </summary>
    private void DrainQueues()
    {
        DrainQueue(_controlQueue,     _drainQuantumControl);
        DrainQueue(_replicationQueue, _drainQuantumReplication);
        // Only decrement the depth counter when the bounded path was used in Enqueue.
        DrainQueue(_clientQueue,      _drainQuantumClient, decrementClientDepth: _maxClientQueueDepth > 0);
        DrainQueue(_maintenanceQueue, _drainQuantumMaintenance);
    }

    private void DrainQueue(ConcurrentQueue<PendingOperation> queue, int maxPerCycle, bool decrementClientDepth = false)
    {
        // Collect up to maxPerCycle items from this queue into the scratch list.
        _drainBatch.Clear();

        int taken = 0;
        while (taken < maxPerCycle && queue.TryDequeue(out PendingOperation? op))
        {
            _drainBatch.Add(op);
            taken++;
            if (decrementClientDepth)
                Interlocked.Decrement(ref _clientQueueDepth);
        }

        if (_drainBatch.Count == 0)
            return;

        // Batch only the leading contiguous run of ReplicateLogs.
        // Checking only _drainBatch[0] and sending the whole window to
        // ExecuteBatchAsync would misroute any CommitLogs / RollbackLogs /
        // other operation that trails a ReplicateLogs run in the same cycle.
        int batchEnd = 0;
        while (batchEnd < _drainBatch.Count && _drainBatch[batchEnd].Request.Type == RaftRequestType.ReplicateLogs)
            batchEnd++;

        int dispatchStart = 0;
        if (batchEnd >= 2)
        {
            ExecuteBatchAsync(_drainBatch.GetRange(0, batchEnd)).GetAwaiter().GetResult();
            dispatchStart = batchEnd;
        }

        for (int i = dispatchStart; i < _drainBatch.Count; i++)
            ExecuteOneAsync(_drainBatch[i]).GetAwaiter().GetResult();
    }

    private void DrainAll()
    {
        DrainQueue(_controlQueue,     int.MaxValue);
        DrainQueue(_replicationQueue, int.MaxValue);
        DrainQueue(_clientQueue,      int.MaxValue, decrementClientDepth: _maxClientQueueDepth > 0);
        DrainQueue(_maintenanceQueue, int.MaxValue);
    }

    private bool AreQueuesEmpty() =>
        _controlQueue.IsEmpty &&
        _replicationQueue.IsEmpty &&
        _clientQueue.IsEmpty &&
        _maintenanceQueue.IsEmpty;

    // ── Dispatch ───────────────────────────────────────────────────────────

    private async Task ExecuteOneAsync(PendingOperation op)
    {
        RaftRequest request = op.Request;
        ValueStopwatch sw = ValueStopwatch.StartNew();

        try
        {
            switch (request.Type)
            {
                case RaftRequestType.CheckLeader:
                    await _stateMachine.CheckPartitionLeadershipAsync().ConfigureAwait(false);
                    op.Reply?.TrySetResult(RaftResponseStatic.NoneResponse);
                    break;

                case RaftRequestType.ForceLeaderForTesting:
                    await _stateMachine.ForceLeaderForTestingAsync(RegisterReply(op)).ConfigureAwait(false);
                    break;

                case RaftRequestType.StepDown:
                    await _stateMachine.StepDownAsync(RegisterReply(op)).ConfigureAwait(false);
                    break;

                case RaftRequestType.TransferLeadership:
                    await _stateMachine.TransferLeadershipAsync(request.Endpoint ?? "", RegisterReply(op)).ConfigureAwait(false);
                    break;

                case RaftRequestType.SuspendHeartbeats:
                    await _stateMachine.SuspendHeartbeatsAsync(RegisterReply(op)).ConfigureAwait(false);
                    break;

                case RaftRequestType.ResumeHeartbeats:
                    await _stateMachine.ResumeHeartbeatsAsync(RegisterReply(op)).ConfigureAwait(false);
                    break;

                case RaftRequestType.ReceiveStepDownNotice:
                    await _stateMachine.ReceiveStepDownNoticeAsync(request.StepDownNotice!).ConfigureAwait(false);
                    op.Reply?.TrySetResult(RaftResponseStatic.NoneResponse);
                    break;

                case RaftRequestType.ReceiveTransferLeadership:
                    await _stateMachine.ReceiveTransferLeadershipAsync(request.TransferLeadership!).ConfigureAwait(false);
                    op.Reply?.TrySetResult(RaftResponseStatic.NoneResponse);
                    break;

                case RaftRequestType.GetNodeState:
                    op.Reply?.TrySetResult(new RaftResponse(RaftResponseType.NodeState, _stateMachine.NodeState));
                    break;

                case RaftRequestType.GetTicketState:
                {
                    (RaftProposalTicketState ticketState, long commitIndex) = _stateMachine.CheckTicketCompletion(request.Timestamp);
                    op.Reply?.TrySetResult(new RaftResponse(RaftResponseType.TicketState, ticketState, commitIndex));
                    break;
                }

                case RaftRequestType.GetFollowerCommittedIndex:
                {
                    long followerIndex = _stateMachine.GetFollowerCommittedIndex(request.Endpoint ?? "");
                    op.Reply?.TrySetResult(new RaftResponse(RaftResponseType.FollowerCommittedIndex, RaftOperationStatus.Success, followerIndex));
                    break;
                }

                case RaftRequestType.AppendLogs:
                    await _stateMachine.AppendLogsAsync(
                        request.Endpoint ?? "",
                        request.Term,
                        request.Timestamp,
                        request.Logs,
                        request.PrevLogIndex,
                        request.PrevLogTerm,
                        RegisterReply(op),
                        request.Quiesce
                    ).ConfigureAwait(false);
                    break;

                case RaftRequestType.CompleteAppendLogs:
                    await _stateMachine.CompleteAppendLogsAsync(
                        request.Endpoint ?? "",
                        request.Timestamp,
                        request.Status,
                        request.CommitIndex
                    ).ConfigureAwait(false);
                    op.Reply?.TrySetResult(RaftResponseStatic.NoneResponse);
                    break;

                case RaftRequestType.ReplicateLogs:
                    if (request.ExpectedGeneration != 0 &&
                        _getGeneration is { } getGen &&
                        getGen() != request.ExpectedGeneration)
                    {
                        op.Reply?.TrySetResult(RaftResponseStatic.PartitionMovedResponse);
                        break;
                    }
                    await _stateMachine.ReplicateLogsAsync(request.Logs, request.AutoCommit, RegisterReply(op)).ConfigureAwait(false);
                    break;

                case RaftRequestType.ReplicateCheckpoint:
                    await _stateMachine.ReplicateCheckpointAsync(RegisterReply(op)).ConfigureAwait(false);
                    break;

                case RaftRequestType.CommitLogs:
                    await _stateMachine.CommitLogsAsync(request.Timestamp, RegisterReply(op)).ConfigureAwait(false);
                    break;

                case RaftRequestType.RollbackLogs:
                    await _stateMachine.RollbackLogsAsync(request.Timestamp, RegisterReply(op)).ConfigureAwait(false);
                    break;

                case RaftRequestType.WriteOperationCompleted:
                    await _stateMachine.CompleteWalOperationAsync(request.WalCompletion).ConfigureAwait(false);
                    op.Reply?.TrySetResult(RaftResponseStatic.NoneResponse);
                    break;

                case RaftRequestType.DrainBarrier:
                    // Re-enqueue if higher-priority queues still have work so the
                    // barrier only resolves once everything queued before it is done.
                    if (!_controlQueue.IsEmpty || !_replicationQueue.IsEmpty || !_clientQueue.IsEmpty)
                    {
                        _maintenanceQueue.Enqueue(op);
                        _workAvailable.Release();
                        return;
                    }
                    Interlocked.Increment(ref _totalProcessed);
                    op.Reply?.TrySetResult(RaftResponseStatic.NoneResponse);
                    return;

                case RaftRequestType.RestoreLogsLoaded:
                    // Phase 2: replay logs and finalise restore on the executor thread.
                    await _stateMachine.CompleteRestoreAsync(request.RestoredLogs ?? []).ConfigureAwait(false);
                    _restoreCompleted = true;
                    _restoreTcs.TrySetResult();
                    op.Reply?.TrySetResult(RaftResponseStatic.NoneResponse);
                    break;

                case RaftRequestType.RequestVote:
                    await _stateMachine.VoteAsync(
                        new RaftNode(request.Endpoint ?? ""),
                        request.Term,
                        request.CommitIndex,
                        request.Timestamp,
                        request.PreVote
                    ).ConfigureAwait(false);
                    op.Reply?.TrySetResult(RaftResponseStatic.NoneResponse);
                    break;

                case RaftRequestType.ReceiveVote:
                    await _stateMachine.ReceivedVoteAsync(request.Endpoint ?? "", request.Term, request.CommitIndex, request.PreVote).ConfigureAwait(false);
                    op.Reply?.TrySetResult(RaftResponseStatic.NoneResponse);
                    break;

                case RaftRequestType.ReceiveHandshake:
                    _stateMachine.ReceiveHandshake((int)request.Term, request.Endpoint ?? "", request.CommitIndex);
                    op.Reply?.TrySetResult(RaftResponseStatic.NoneResponse);
                    break;

                case RaftRequestType.SnapshotInstalled:
                    _stateMachine.CompleteSnapshotInstalled(request.Endpoint ?? "", request.CommitIndex);
                    op.Reply?.TrySetResult(RaftResponseStatic.NoneResponse);
                    break;

                default:
                    _logger.LogError(
                        "[RaftPartitionExecutor/{PartitionId}] Unrecognised request type: {Type}",
                        _partitionId, request.Type);
                    op.Reply?.TrySetResult(RaftResponseStatic.NoneResponse);
                    break;
            }

            Interlocked.Increment(ref _totalProcessed);

            double elapsedMs = sw.GetElapsedTime().TotalMilliseconds;
            RaftOperationKind kind = RaftOperationMapper.GetKind(request.Type);
            TagList tags = new() { { "partition_id", _partitionId }, { "operation_class", kind.ToString() } };
            KommanderMetrics.ExecutorOperationsTotal.Add(1, tags);
            KommanderMetrics.ExecutorOperationDurationMs.Record(elapsedMs, tags);
            if (kind is RaftOperationKind.Client or RaftOperationKind.Replication)
                _loadAccumulator.RecordOps(1);
            if (request.Type == RaftRequestType.ReplicateLogs)
                _logLoadAccumulator.RecordOps(1);
        }
        catch (Exception ex)
        {
            _logger.LogError(
                "[RaftPartitionExecutor/{PartitionId}] {Name}: {Message}\n{StackTrace}",
                _partitionId, ex.GetType().Name, ex.Message, ex.StackTrace);

            // If restore Phase 2 itself throws, fault the restore task so waiters don't block.
            if (request.Type == RaftRequestType.RestoreLogsLoaded)
                _restoreTcs.TrySetException(ex);

            op.Reply?.TrySetException(ex);
        }
        finally
        {
            long elapsedMs = sw.GetElapsedMilliseconds();
            if (_slowThresholdMs > 0 && elapsedMs > _slowThresholdMs)
            {
                _logger.LogWarning(
                    "[RaftPartitionExecutor/{PartitionId}] Slow dispatch: {Type} took {ElapsedMs}ms",
                    _partitionId, request.Type, elapsedMs);
            }
        }
    }

    /// <summary>Batches multiple ReplicateLogs into a single state-machine call.</summary>
    private async Task ExecuteBatchAsync(List<PendingOperation> ops)
    {
        List<(List<RaftLog>? Logs, bool AutoCommit, ulong? ReplyCorrelationId)> batch = new(ops.Count);

        foreach (PendingOperation op in ops)
        {
            RaftRequest req = op.Request;

            // Mirror the generation fence in ExecuteOneAsync: a fenced write whose
            // expected generation doesn't match the current partition generation must be
            // rejected even when it lands in a batch drain cycle.
            if (req.ExpectedGeneration != 0 &&
                _getGeneration is { } getGen &&
                getGen() != req.ExpectedGeneration)
            {
                op.Reply?.TrySetResult(RaftResponseStatic.PartitionMovedResponse);
                continue;
            }

            batch.Add((req.Logs, req.AutoCommit, RegisterReply(op)));
        }

        ValueStopwatch sw = ValueStopwatch.StartNew();
        try
        {
            await _stateMachine.ReplicateLogsBatchAsync(batch).ConfigureAwait(false);
            Interlocked.Add(ref _totalProcessed, ops.Count);
            Interlocked.Increment(ref _totalBatchesExecuted);

            double elapsedMs = sw.GetElapsedTime().TotalMilliseconds;
            TagList tags = new() { { "partition_id", _partitionId }, { "operation_class", "Replication" } };
            KommanderMetrics.ExecutorOperationsTotal.Add(ops.Count, tags);
            KommanderMetrics.ExecutorOperationDurationMs.Record(elapsedMs, tags);
            _loadAccumulator.RecordOps(ops.Count);
            _logLoadAccumulator.RecordOps(ops.Count);
        }
        catch (Exception ex)
        {
            _logger.LogError(
                "[RaftPartitionExecutor/{PartitionId}] ReplicateLogsBatch error: {Message}",
                _partitionId, ex.Message);

            foreach (PendingOperation op in ops)
                op.Reply?.TrySetException(ex);
        }
    }

    // ── Reply correlation ──────────────────────────────────────────────────

    /// <summary>
    /// If the operation expects a reply, registers it and returns the correlation id.
    /// Otherwise returns null (fire-and-forget).
    /// </summary>
    private ulong? RegisterReply(PendingOperation op)
    {
        if (op.Reply is null)
            return null;

        ulong id = _nextCorrelationId++;
        _pendingReplies[id] = op;
        return id;
    }

    /// <summary>
    /// Called by the <see cref="ExecutorReplySink"/> when the state machine produces
    /// a reply for a previously registered correlation id.
    /// </summary>
    internal void DeliverReply(ulong correlationId, RaftResponse response)
    {
        if (_pendingReplies.Remove(correlationId, out PendingOperation? op))
            op.Reply?.TrySetResult(response);
    }

    private void CancelPendingReplies()
    {
        foreach (KeyValuePair<ulong, PendingOperation> kv in _pendingReplies)
            kv.Value.Reply?.TrySetCanceled();

        _pendingReplies.Clear();
    }

    // ── IDisposable ────────────────────────────────────────────────────────

    public void Dispose()
    {
        GC.SuppressFinalize(this);
        if (!_stopping)
            Stop();
        _cts.Dispose();
        _workAvailable.Dispose();
    }
}

/// <summary>
/// <see cref="IRaftOperationReplySink"/> implementation that routes replies from
/// <see cref="RaftPartitionStateMachine"/> back to the originating <see cref="RaftPartitionExecutor"/>.
/// </summary>
internal sealed class ExecutorReplySink : IRaftOperationReplySink
{
    private readonly RaftPartitionExecutor _executor;

    public ExecutorReplySink(RaftPartitionExecutor executor) => _executor = executor;

    public void TryComplete(ulong correlationId, RaftResponse response)
        => _executor.DeliverReply(correlationId, response);
}
