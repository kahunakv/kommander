using System.Collections.Concurrent;
using System.Threading.Channels;
using Kommander.Communication.Grpc;
using Kommander.Data;
using Kommander.Logging;
using Microsoft.Extensions.Logging;

namespace Kommander.Communication;

/// <summary>
/// Actor-free outbound transport dispatcher that replaces <c>RaftResponderActor</c>.
///
/// <para>One <see cref="Channel{T}"/>-backed worker task is created per remote endpoint on
/// first use.  Each worker drains its channel, groups adjacent messages into natural batches
/// (up to <see cref="MaxBatchSize"/>), and dispatches via <see cref="ICommunication"/>.
/// Per-endpoint FIFO ordering is preserved.  Batching is opportunistic — messages that
/// accumulate while a prior batch is in-flight are automatically grouped without any
/// artificial delay.</para>
///
/// <para>This class has no actor or Nixie dependency.</para>
/// </summary>
internal sealed class RaftTransportDispatcher : IDisposable
{
    private const int MaxBatchSize = 64;

    // ── Per-endpoint worker ───────────────────────────────────────────────────

    private sealed class EndpointWorker : IDisposable
    {
        private readonly Channel<RaftResponderRequest> _channel;
        private readonly CancellationTokenSource _cts = new();
        private readonly Task _loop;
        private readonly long _maxQueuedPayloadBytes;
        private readonly ILogger<IRaft> _logger;
        private readonly string _endpoint;

        /// <summary>
        /// Approximate log-payload bytes currently queued (or in the in-flight batch) for this
        /// peer. Incremented at enqueue, decremented after the batch containing the message has
        /// been handed to the transport — the queue AND the in-flight batch both retain the
        /// payload lists, so both count against the budget.
        /// </summary>
        private long _queuedPayloadBytes;

        /// <summary>Total entry-carrying messages dropped since the worker started; monotonic, for the episode log.</summary>
        private long _droppedMessages;

        /// <summary>1 while a drop episode is open, so the Warning fires once per episode, not per message.</summary>
        private int _dropEpisodeOpen;

        internal EndpointWorker(
            RaftManager manager,
            RaftNode node,
            ICommunication communication,
            ILogger<IRaft> logger,
            long maxQueuedPayloadBytes)
        {
            _maxQueuedPayloadBytes = maxQueuedPayloadBytes;
            _logger = logger;
            _endpoint = node.Endpoint;

            _channel = Channel.CreateUnbounded<RaftResponderRequest>(
                new UnboundedChannelOptions { SingleReader = true, SingleWriter = false });

            _loop = Task.Run(() =>
                RunAsync(manager, node, communication, logger, _cts.Token));

            // The loop only surfaces to a Wait in Dispose, which not every shutdown reaches; an
            // unobserved fault here would otherwise re-throw on the finalizer thread.
            Support.Parallelization.FireAndForget.Observe(_loop, logger, "RaftTransportDispatcher.Worker");
        }

        /// <summary>
        /// True when this peer's queued payload bytes are at or over the budget. Consulted by the
        /// backfill sender (via <c>RaftManager.IsOutboundQueueSaturated</c>) to skip reading a
        /// batch that would only be dropped here.
        /// </summary>
        internal bool IsSaturated =>
            _maxQueuedPayloadBytes > 0 && Interlocked.Read(ref _queuedPayloadBytes) >= _maxQueuedPayloadBytes;

        /// <summary>
        /// Sum of <see cref="RaftLog.LogData"/> lengths carried by <paramref name="request"/>, or 0
        /// for control messages and empty heartbeats. The per-entry constant approximates the
        /// <see cref="RaftLog"/> object and list-slot overhead so a flood of small entries is not
        /// accounted as free.
        /// </summary>
        private static long PayloadBytes(in RaftResponderRequest request)
        {
            List<RaftLog>? logs = request.AppendLogsRequest?.Logs;
            if (logs is null || logs.Count == 0)
                return 0;

            const int perEntryOverhead = 128;

            long total = 0;
            for (int i = 0; i < logs.Count; i++)
                total += perEntryOverhead + (logs[i].LogData?.Length ?? 0);

            return total;
        }

        internal void Enqueue(RaftResponderRequest request)
        {
            long payloadBytes = PayloadBytes(request);

            if (payloadBytes > 0 && _maxQueuedPayloadBytes > 0)
            {
                // Budget check for entry-carrying messages only: votes, handshakes, step-down
                // notices, acks, and EMPTY heartbeats always pass — they are what hold leadership
                // and elections together, and they retain no payload. Dropping an entry-carrying
                // AppendLogs is safe: the send is fire-and-forget and the heartbeat/backfill retry
                // path re-ships every unacknowledged entry once the queue drains. Buffering it
                // instead is how a paused follower made the leader retain the entire write load
                // (the Caraxes run Q memory-exhaustion abort).
                if (Interlocked.Read(ref _queuedPayloadBytes) + payloadBytes > _maxQueuedPayloadBytes)
                {
                    long dropped = Interlocked.Increment(ref _droppedMessages);

                    // One Warning per episode; the counter carries the magnitude when the episode
                    // closes and a later one re-opens. Per-message logging at saturation rates
                    // would itself be an allocation storm.
                    if (Interlocked.CompareExchange(ref _dropEpisodeOpen, 1, 0) == 0)
                        _logger.LogWarning(
                            "[RaftTransportDispatcher/{Endpoint}] Outbound queue over budget ({MaxBytes} bytes); dropping entry-carrying messages until it drains (total dropped so far: {Dropped})",
                            _endpoint, _maxQueuedPayloadBytes, dropped);

                    return;
                }

                Interlocked.Add(ref _queuedPayloadBytes, payloadBytes);
            }

            if (!_channel.Writer.TryWrite(request) && payloadBytes > 0)
                Interlocked.Add(ref _queuedPayloadBytes, -payloadBytes); // channel completed during shutdown
        }

        /// <summary>
        /// Releases the byte reservation of a dispatched (or drained) batch and closes the drop
        /// episode once the backlog has fallen to half the budget — hysteresis so a queue hovering
        /// at the boundary does not emit a Warning per message.
        /// </summary>
        private void ReleaseBatchBytes(List<RaftResponderRequest> batch)
        {
            long total = 0;
            for (int i = 0; i < batch.Count; i++)
                total += PayloadBytes(batch[i]);

            if (total == 0)
                return;

            long remaining = Interlocked.Add(ref _queuedPayloadBytes, -total);

            if (remaining <= _maxQueuedPayloadBytes / 2 && Interlocked.CompareExchange(ref _dropEpisodeOpen, 0, 1) == 1)
                _logger.LogWarning(
                    "[RaftTransportDispatcher/{Endpoint}] Outbound queue drained below half budget; resuming entry-carrying messages ({Dropped} dropped during the episode)",
                    _endpoint, Interlocked.Read(ref _droppedMessages));
        }

        internal void Stop()
        {
            // Only complete the channel writer so the worker drains remaining items
            // before exiting. Do NOT cancel _cts here — cancelling it causes
            // WaitToReadAsync to throw OperationCanceledException and the worker breaks
            // out of its loop before buffered messages are sent.
            _channel.Writer.TryComplete();
        }

        public void Dispose()
        {
            Stop(); // complete channel writer; worker will drain naturally
            try { _loop.Wait(TimeSpan.FromSeconds(5)); } catch { /* ignore shutdown races */ }
            // Hard-abort: unblock WaitToReadAsync if the worker is somehow still waiting
            // (e.g. slow drain, stuck in a long send). Worker will exit on next iteration.
            _cts.Cancel();
            _cts.Dispose();
        }

        // ── Worker loop ───────────────────────────────────────────────────────

        private async Task RunAsync(
            RaftManager manager,
            RaftNode node,
            ICommunication communication,
            ILogger<IRaft> logger,
            CancellationToken token)
        {
            ChannelReader<RaftResponderRequest> reader = _channel.Reader;
            List<RaftResponderRequest> batch = new(MaxBatchSize);

            while (true)
            {
                try
                {
                    if (!await reader.WaitToReadAsync(token).ConfigureAwait(false))
                        break; // channel completed normally
                }
                catch (OperationCanceledException)
                {
                    break; // hard abort from Dispose(); fall through to post-loop drain
                }

                // Drain whatever is immediately available to form a natural batch.
                batch.Clear();
                while (batch.Count < MaxBatchSize && reader.TryRead(out RaftResponderRequest item))
                    batch.Add(item);

                if (batch.Count == 0)
                    continue;

                try
                {
                    await Send(batch, manager, node, communication, logger).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    logger.LogError(
                        "[RaftTransportDispatcher/{Endpoint}] {Type}: {Message}\n{StackTrace}",
                        node.Endpoint, ex.GetType().Name, ex.Message, ex.StackTrace);
                }
                finally
                {
                    // Failed sends release too: the messages are gone either way, and a stuck
                    // reservation would otherwise saturate the peer forever.
                    ReleaseBatchBytes(batch);
                }
            }

            // Post-loop drain: flush any items that were buffered before the channel was
            // completed or before the hard-abort token fired. This covers the window where
            // Stop() (channel complete) races with the last Enqueue() call.
            // Clear first — batch may hold the last-processed set from the main loop;
            // the drain must only send messages that have NOT yet been dispatched.
            batch.Clear();
            while (reader.TryRead(out RaftResponderRequest remaining))
            {
                batch.Add(remaining);

                if (batch.Count >= MaxBatchSize)
                {
                    try { await Send(batch, manager, node, communication, logger).ConfigureAwait(false); }
                    catch (Exception ex)
                    {
                        logger.LogError(
                            "[RaftTransportDispatcher/{Endpoint}] drain {Type}: {Message}",
                            node.Endpoint, ex.GetType().Name, ex.Message);
                    }
                    finally { ReleaseBatchBytes(batch); }
                    batch.Clear();
                }
            }

            if (batch.Count > 0)
            {
                try { await Send(batch, manager, node, communication, logger).ConfigureAwait(false); }
                catch (Exception ex)
                {
                    logger.LogError(
                        "[RaftTransportDispatcher/{Endpoint}] drain {Type}: {Message}",
                        node.Endpoint, ex.GetType().Name, ex.Message);
                }
                finally { ReleaseBatchBytes(batch); }
            }
        }

        // ── Dispatch helpers ──────────────────────────────────────────────────

        private static async Task Send(
            List<RaftResponderRequest> messages,
            RaftManager manager,
            RaftNode node,
            ICommunication communication,
            ILogger<IRaft> logger)
        {
            if (messages.Count == 1)
            {
                await SendSingle(messages[0], manager, node, communication).ConfigureAwait(false);
                return;
            }

            logger.LogTraceSendingBatch(node.Endpoint, messages.Count);

            // Items and wrapper are rented from GrpcCommunicationPool with the same lifecycle as
            // SendWrappedSingle: everything is returned only after BatchRequests completes, since
            // the transport may reference them until serialization finishes.
            List<BatchRequestsRequestItem> items =
                GrpcCommunicationPool.RentListBatchRequestsRequestItem(messages.Count);
            BatchRequestsRequest request = GrpcCommunicationPool.RentBatchRequestsRequest();

            try
            {
                foreach (RaftResponderRequest msg in messages)
                {
                    BatchRequestsRequestItem item = GrpcCommunicationPool.RentBatchRequestsRequestItem();

                    switch (msg.Type)
                    {
                        case RaftResponderRequestType.Handshake:
                            item.Type = BatchRequestsRequestType.Handshake;
                            item.Handshake = msg.HandshakeRequest;
                            break;

                        case RaftResponderRequestType.Vote:
                            item.Type = BatchRequestsRequestType.Vote;
                            item.Vote = msg.VoteRequest;
                            break;

                        case RaftResponderRequestType.RequestVotes:
                            item.Type = BatchRequestsRequestType.RequestVote;
                            item.RequestVotes = msg.RequestVotesRequest;
                            break;

                        case RaftResponderRequestType.StepDownNotice:
                            item.Type = BatchRequestsRequestType.StepDownNotice;
                            item.StepDownNotice = msg.StepDownNoticeRequest;
                            break;

                        case RaftResponderRequestType.TransferLeadership:
                            item.Type = BatchRequestsRequestType.TransferLeadership;
                            item.TransferLeadership = msg.TransferLeadershipRequest;
                            break;

                        case RaftResponderRequestType.TransferLeadershipSuggestion:
                            item.Type = BatchRequestsRequestType.TransferLeadershipSuggestion;
                            item.TransferLeadershipSuggestion = msg.TransferLeadershipSuggestionRequest;
                            break;

                        case RaftResponderRequestType.AppendLogs:
                            item.Type = BatchRequestsRequestType.AppendLogs;
                            item.AppendLogs = msg.AppendLogsRequest;
                            break;

                        case RaftResponderRequestType.CompleteAppendLogs:
                            item.Type = BatchRequestsRequestType.CompleteAppendLogs;
                            item.CompleteAppendLogs = msg.CompleteAppendLogsRequest;
                            break;

                        default:
                            GrpcCommunicationPool.Return(item);
                            logger.LogError(
                                "[RaftTransportDispatcher/{Endpoint}] Unsupported message type {Type}",
                                node.Endpoint, msg.Type);
                            continue;
                    }

                    items.Add(item);
                }

                request.Requests = items;

                await communication.BatchRequests(manager, node, request)
                    .ConfigureAwait(false);
            }
            finally
            {
                GrpcCommunicationPool.Return(request);

                foreach (BatchRequestsRequestItem item in items)
                    GrpcCommunicationPool.Return(item);

                GrpcCommunicationPool.Return(items);
            }
        }

        private static Task SendSingle(
            RaftResponderRequest message,
            RaftManager manager,
            RaftNode node,
            ICommunication communication) =>
            message.Type switch
            {
                RaftResponderRequestType.Handshake
                    when message.Node is not null && message.HandshakeRequest is not null
                    => communication.Handshake(manager, message.Node, message.HandshakeRequest),

                RaftResponderRequestType.Vote
                    when message.Node is not null && message.VoteRequest is not null
                    => communication.Vote(manager, message.Node, message.VoteRequest),

                RaftResponderRequestType.RequestVotes
                    when message.Node is not null && message.RequestVotesRequest is not null
                    => communication.RequestVotes(manager, message.Node, message.RequestVotesRequest),

                RaftResponderRequestType.StepDownNotice
                    when message.Node is not null && message.StepDownNoticeRequest is not null
                    => SendWrappedSingle(manager, message.Node, communication, message),

                RaftResponderRequestType.TransferLeadership
                    when message.Node is not null && message.TransferLeadershipRequest is not null
                    => SendWrappedSingle(manager, message.Node, communication, message),

                RaftResponderRequestType.TransferLeadershipSuggestion
                    when message.Node is not null && message.TransferLeadershipSuggestionRequest is not null
                    => SendWrappedSingle(manager, message.Node, communication, message),

                RaftResponderRequestType.AppendLogs
                    when message.Node is not null && message.AppendLogsRequest is not null
                    => communication.AppendLogs(manager, message.Node, message.AppendLogsRequest),

                RaftResponderRequestType.CompleteAppendLogs
                    when message.Node is not null && message.CompleteAppendLogsRequest is not null
                    => communication.CompleteAppendLogs(manager, message.Node, message.CompleteAppendLogsRequest),

                _ => Task.CompletedTask
            };

        /// <summary>
        /// Sends a control message that has no dedicated single-message RPC by wrapping it in a
        /// one-item batch. The wrapper, item, and list come from <see cref="GrpcCommunicationPool"/>
        /// to keep leadership-churn traffic (step-down notices, transfer requests/suggestions)
        /// allocation-free. Pooled objects are only returned after the send completes — the
        /// transport may reference them until <c>BatchRequests</c> finishes serializing.
        /// </summary>
        private static async Task SendWrappedSingle(
            RaftManager manager,
            RaftNode node,
            ICommunication communication,
            RaftResponderRequest message)
        {
            BatchRequestsRequestItem item = GrpcCommunicationPool.RentBatchRequestsRequestItem();
            List<BatchRequestsRequestItem> items = GrpcCommunicationPool.RentListBatchRequestsRequestItem(1);
            BatchRequestsRequest request = GrpcCommunicationPool.RentBatchRequestsRequest();

            switch (message.Type)
            {
                case RaftResponderRequestType.StepDownNotice:
                    item.Type = BatchRequestsRequestType.StepDownNotice;
                    item.StepDownNotice = message.StepDownNoticeRequest;
                    break;

                case RaftResponderRequestType.TransferLeadership:
                    item.Type = BatchRequestsRequestType.TransferLeadership;
                    item.TransferLeadership = message.TransferLeadershipRequest;
                    break;

                case RaftResponderRequestType.TransferLeadershipSuggestion:
                    item.Type = BatchRequestsRequestType.TransferLeadershipSuggestion;
                    item.TransferLeadershipSuggestion = message.TransferLeadershipSuggestionRequest;
                    break;
            }

            items.Add(item);
            request.Requests = items;

            try
            {
                await communication.BatchRequests(manager, node, request).ConfigureAwait(false);
            }
            finally
            {
                GrpcCommunicationPool.Return(request);
                GrpcCommunicationPool.Return(items);
                GrpcCommunicationPool.Return(item);
            }
        }
    }

    // ── Dispatcher ────────────────────────────────────────────────────────────

    private readonly RaftManager _manager;
    private readonly ICommunication _communication;
    private readonly ILogger<IRaft> _logger;
    private readonly long _maxQueuedPayloadBytesPerPeer;
    private readonly ConcurrentDictionary<string, EndpointWorker> _workers = new();
    private volatile bool _stopped;
    private int _disposed;

    internal RaftTransportDispatcher(
        RaftManager manager,
        ICommunication communication,
        ILogger<IRaft> logger,
        long maxQueuedPayloadBytesPerPeer = 64L * 1024 * 1024)
    {
        _manager = manager;
        _communication = communication;
        _logger = logger;
        _maxQueuedPayloadBytesPerPeer = maxQueuedPayloadBytesPerPeer;
    }

    /// <summary>
    /// True when the outbound queue for <paramref name="endpoint"/> is over its payload-byte
    /// budget, meaning an entry-carrying message enqueued now would be dropped. A peer with no
    /// worker yet has an empty queue and is never saturated.
    /// </summary>
    internal bool IsOutboundQueueSaturated(string endpoint) =>
        _workers.TryGetValue(endpoint, out EndpointWorker? worker) && worker.IsSaturated;

    /// <summary>
    /// Enqueues an outbound message for delivery to the given remote endpoint.
    /// Thread-safe.  No-op if the dispatcher has been stopped or disposed.
    /// </summary>
    internal void Enqueue(string endpoint, RaftResponderRequest request)
    {
        if (_stopped)
            return;

        // TryGetValue fast path: this runs once per outbound message and the worker
        // always exists after the first send to a peer; calling GetOrAdd directly would
        // allocate the capturing factory closure on every enqueue.
        if (!_workers.TryGetValue(endpoint, out EndpointWorker? worker))
            worker = _workers.GetOrAdd(
                endpoint,
                ep => new EndpointWorker(_manager, new RaftNode(ep), _communication, _logger, _maxQueuedPayloadBytesPerPeer));

        worker.Enqueue(request);
    }

    /// <summary>
    /// Signals all worker channels to complete.  Workers finish in-flight sends
    /// before exiting.  Safe to call multiple times.
    /// </summary>
    internal void Stop()
    {
        _stopped = true;
        foreach (EndpointWorker worker in _workers.Values)
            worker.Stop();
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        _stopped = true;
        foreach (EndpointWorker worker in _workers.Values)
            worker.Dispose();
    }
}
