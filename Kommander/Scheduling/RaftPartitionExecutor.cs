using System.Collections.Concurrent;
using System.Diagnostics;
using Kommander.Data;
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

    private readonly ConcurrentQueue<PendingOperation> _controlQueue = new();
    private readonly ConcurrentQueue<PendingOperation> _replicationQueue = new();
    private readonly ConcurrentQueue<PendingOperation> _clientQueue = new();
    private readonly ConcurrentQueue<PendingOperation> _maintenanceQueue = new();

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

    // Slow-message threshold (ms) — same as actor adapter uses.
    private readonly int _slowThresholdMs;

    // Reply correlation: maps correlationId -> PendingOperation that expects a reply.
    private readonly Dictionary<ulong, PendingOperation> _pendingReplies = [];
    private ulong _nextCorrelationId = 1;

    // Batch scratch list to avoid per-drain allocations.
    private readonly List<PendingOperation> _drainBatch = new(256);

    // ── Observability ──────────────────────────────────────────────────────

    private long _totalProcessed;

    /// <summary>Total number of operations processed by this executor since start.</summary>
    public long TotalProcessed => Interlocked.Read(ref _totalProcessed);

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
    public RaftPartitionExecutor(
        RaftPartitionStateMachine stateMachine,
        int partitionId,
        int slowThresholdMs,
        ILogger<IRaft> logger)
    {
        _stateMachine = stateMachine;
        _partitionId = partitionId;
        _slowThresholdMs = slowThresholdMs;
        _logger = logger;

        _worker = new Thread(WorkerLoop)
        {
            IsBackground = true,
            Name = $"RaftPartitionExecutor-{partitionId}",
        };
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
        PendingOperation op = new(request, reply);

        RaftOperationKind kind = RaftOperationMapper.GetKind(request.Type);
        switch (kind)
        {
            case RaftOperationKind.Control:     _controlQueue.Enqueue(op); break;
            case RaftOperationKind.Replication: _replicationQueue.Enqueue(op); break;
            case RaftOperationKind.Client:      _clientQueue.Enqueue(op); break;
            case RaftOperationKind.Maintenance: _maintenanceQueue.Enqueue(op); break;
        }

        _workAvailable.Release();
    }

    // ── Worker loop ────────────────────────────────────────────────────────

    private void WorkerLoop()
    {
        CancellationToken token = _cts.Token;

        // Restore WAL on first run (mirrors actor adapter behaviour).
        // We do this synchronously on the worker thread so it blocks new operations
        // until restore completes, satisfying correctness rule 8.
        try
        {
            _stateMachine.RestoreWalAsync().AsTask().GetAwaiter().GetResult();
        }
        catch (Exception ex)
        {
            _logger.LogError(
                "[RaftPartitionExecutor/{PartitionId}] WAL restore failed; partition will not process operations: {Message}\n{StackTrace}",
                _partitionId, ex.Message, ex.StackTrace);

            // Mark the executor as stopped so Post/Ask immediately throw rather
            // than enqueuing onto a worker that has already exited.
            _stopping = true;
            _cts.Cancel();

            // Abort: cancel all already-enqueued operations and exit the worker so
            // nothing runs against unrestored state.
            DrainAll();
            CancelPendingReplies();
            return;
        }

        while (true)
        {
            try
            {
                // Wait for work (5 ms timeout so we can re-check stop on a quiet cluster).
                _workAvailable.Wait(5, token);
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

    /// <summary>
    /// Weighted-fair drain: Control×8, Replication×4, Client×2, Maintenance×1.
    /// </summary>
    private void DrainQueues()
    {
        DrainQueue(_controlQueue,     (int)RaftOperationPriority.Control);
        DrainQueue(_replicationQueue, (int)RaftOperationPriority.Replication);
        DrainQueue(_clientQueue,      (int)RaftOperationPriority.Client);
        DrainQueue(_maintenanceQueue, (int)RaftOperationPriority.Maintenance);
    }

    private void DrainQueue(ConcurrentQueue<PendingOperation> queue, int maxPerCycle)
    {
        // Collect up to maxPerCycle items from this queue into the scratch list.
        _drainBatch.Clear();

        int taken = 0;
        while (taken < maxPerCycle && queue.TryDequeue(out PendingOperation? op))
        {
            _drainBatch.Add(op);
            taken++;
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
        DrainQueue(_clientQueue,      int.MaxValue);
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
        Stopwatch sw = Stopwatch.StartNew();

        try
        {
            switch (request.Type)
            {
                case RaftRequestType.CheckLeader:
                    await _stateMachine.CheckPartitionLeadershipAsync().ConfigureAwait(false);
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

                case RaftRequestType.AppendLogs:
                    await _stateMachine.AppendLogsAsync(
                        request.Endpoint ?? "",
                        request.Term,
                        request.Timestamp,
                        request.Logs,
                        RegisterReply(op)
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

                case RaftRequestType.RequestVote:
                    await _stateMachine.VoteAsync(
                        new RaftNode(request.Endpoint ?? ""),
                        request.Term,
                        request.CommitIndex,
                        request.Timestamp
                    ).ConfigureAwait(false);
                    op.Reply?.TrySetResult(RaftResponseStatic.NoneResponse);
                    break;

                case RaftRequestType.ReceiveVote:
                    await _stateMachine.ReceivedVoteAsync(request.Endpoint ?? "", request.Term, request.CommitIndex).ConfigureAwait(false);
                    op.Reply?.TrySetResult(RaftResponseStatic.NoneResponse);
                    break;

                case RaftRequestType.ReceiveHandshake:
                    _stateMachine.ReceiveHandshake((int)request.Term, request.Endpoint ?? "", request.CommitIndex);
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
        }
        catch (Exception ex)
        {
            _logger.LogError(
                "[RaftPartitionExecutor/{PartitionId}] {Name}: {Message}\n{StackTrace}",
                _partitionId, ex.GetType().Name, ex.Message, ex.StackTrace);

            op.Reply?.TrySetException(ex);
        }
        finally
        {
            if (_slowThresholdMs > 0 && sw.ElapsedMilliseconds > _slowThresholdMs)
            {
                _logger.LogWarning(
                    "[RaftPartitionExecutor/{PartitionId}] Slow dispatch: {Type} took {ElapsedMs}ms",
                    _partitionId, request.Type, sw.ElapsedMilliseconds);
            }
        }
    }

    /// <summary>Batches multiple ReplicateLogs into a single state-machine call.</summary>
    private async Task ExecuteBatchAsync(List<PendingOperation> ops)
    {
        List<(List<RaftLog>? Logs, bool AutoCommit, ulong? ReplyCorrelationId)> batch = new(ops.Count);

        foreach (PendingOperation op in ops)
            batch.Add((op.Request.Logs, op.Request.AutoCommit, RegisterReply(op)));

        try
        {
            await _stateMachine.ReplicateLogsBatchAsync(batch).ConfigureAwait(false);
            Interlocked.Add(ref _totalProcessed, ops.Count);
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
