
using Kommander.Communication;
using Kommander.Data;

namespace Kommander.Tests.Scheduler;

/// <summary>
/// Deterministic in-memory transport for use in scheduler harness tests.
///
/// <para>All outbound messages are captured in <see cref="SentMessages"/> rather than
/// being delivered immediately.  Tests control delivery explicitly via
/// <see cref="DeliverAll"/>, <see cref="DeliverNext"/>, or per-message callbacks so
/// that message ordering, partitions, and dropped messages can be asserted without
/// real networking or wall-clock delays.</para>
///
/// <para>Network partition simulation is per-endpoint pair.  Call
/// <see cref="PartitionLink"/> to block messages from a source endpoint to a
/// destination, and <see cref="HealLink"/> to restore it.  Blocked messages are
/// silently dropped from the delivery queue.</para>
/// </summary>
public sealed class FakeTransport : ICommunication
{
    // ── Captured message model ─────────────────────────────────────────────

    /// <summary>The type of an in-flight Raft message.</summary>
    public enum MessageKind
    {
        Handshake,
        RequestVotes,
        Vote,
        AppendLogs,
        CompleteAppendLogs,
        BatchRequests,
    }

    /// <summary>An outbound message that has been captured but not yet delivered.</summary>
    public sealed class CapturedMessage
    {
        /// <summary>Source endpoint (the node that sent the message).</summary>
        public string SourceEndpoint { get; init; } = "";

        /// <summary>Destination node endpoint.</summary>
        public string DestinationEndpoint { get; init; } = "";

        /// <summary>Message type discriminator.</summary>
        public MessageKind Kind { get; init; }

        /// <summary>The underlying request payload (cast to the concrete type as needed).</summary>
        public object Payload { get; init; } = null!;

        /// <summary>
        /// Completion source that the transport resolves when the test delivers this message.
        /// </summary>
        internal TaskCompletionSource<object?> Reply { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    // ── State ─────────────────────────────────────────────────────────────

    private readonly Queue<CapturedMessage> _queue = new();
    private readonly HashSet<(string from, string to)> _partitioned = [];
    private readonly object _lock = new();

    /// <summary>All messages that have been sent (including delivered and dropped ones).</summary>
    public List<CapturedMessage> SentMessages { get; } = [];

    /// <summary>Messages that were dropped due to a simulated network partition.</summary>
    public List<CapturedMessage> DroppedMessages { get; } = [];

    /// <summary>Messages that have been delivered to their destination.</summary>
    public List<CapturedMessage> DeliveredMessages { get; } = [];

    // ── Network partition control ─────────────────────────────────────────

    /// <summary>
    /// Blocks all future messages from <paramref name="fromEndpoint"/> to
    /// <paramref name="toEndpoint"/>.  Messages currently in the queue from that
    /// link are dropped on the next delivery attempt.
    /// </summary>
    public void PartitionLink(string fromEndpoint, string toEndpoint)
    {
        lock (_lock) _partitioned.Add((fromEndpoint, toEndpoint));
    }

    /// <summary>Removes the block between the two endpoints.</summary>
    public void HealLink(string fromEndpoint, string toEndpoint)
    {
        lock (_lock) _partitioned.Remove((fromEndpoint, toEndpoint));
    }

    /// <summary>Removes all network partition rules.</summary>
    public void HealAll()
    {
        lock (_lock) _partitioned.Clear();
    }

    private bool IsPartitioned(string from, string to)
    {
        lock (_lock) return _partitioned.Contains((from, to));
    }

    // ── Delivery helpers ──────────────────────────────────────────────────

    /// <summary>
    /// Delivers the oldest pending message and returns it, or <c>null</c> when the
    /// queue is empty.  Messages on a partitioned link are dropped instead.
    /// </summary>
    /// <param name="defaultResponse">
    /// Object returned to the sender as the reply.  Pass <c>null</c> to use the
    /// empty-response singleton appropriate to the message kind.
    /// </param>
    public CapturedMessage? DeliverNext(object? defaultResponse = null)
    {
        CapturedMessage? msg;
        lock (_lock)
        {
            if (!_queue.TryDequeue(out msg))
                return null;
        }

        if (IsPartitioned(msg.SourceEndpoint, msg.DestinationEndpoint))
        {
            DroppedMessages.Add(msg);
            msg.Reply.TrySetException(new IOException(
                $"FakeTransport: link {msg.SourceEndpoint} → {msg.DestinationEndpoint} is partitioned."));
            return msg;
        }

        DeliveredMessages.Add(msg);
        msg.Reply.TrySetResult(defaultResponse ?? DefaultReply(msg.Kind));
        return msg;
    }

    /// <summary>Delivers all pending messages in FIFO order and returns the count.</summary>
    public int DeliverAll()
    {
        int count = 0;
        while (DeliverNext() is not null)
            count++;
        return count;
    }

    /// <summary>
    /// Drops the oldest pending message without delivering it (simulates a lost packet).
    /// Returns the dropped message or <c>null</c> when the queue is empty.
    /// </summary>
    public CapturedMessage? DropNext()
    {
        CapturedMessage? msg;
        lock (_lock)
        {
            if (!_queue.TryDequeue(out msg))
                return null;
        }

        DroppedMessages.Add(msg);
        msg.Reply.TrySetException(new IOException("FakeTransport: message explicitly dropped by test."));
        return msg;
    }

    /// <summary>Number of messages currently waiting for delivery.</summary>
    public int PendingCount { get { lock (_lock) return _queue.Count; } }

    // ── ICommunication implementation ─────────────────────────────────────

    /// <inheritdoc/>
    public Task<HandshakeResponse> Handshake(RaftManager manager, RaftNode node, HandshakeRequest request)
        => EnqueueAndWait<HandshakeResponse>(manager.GetLocalEndpoint(), node.Endpoint, MessageKind.Handshake, request);

    /// <inheritdoc/>
    public Task<RequestVotesResponse> RequestVotes(RaftManager manager, RaftNode node, RequestVotesRequest request)
        => EnqueueAndWait<RequestVotesResponse>(manager.GetLocalEndpoint(), node.Endpoint, MessageKind.RequestVotes, request);

    /// <inheritdoc/>
    public Task<VoteResponse> Vote(RaftManager manager, RaftNode node, VoteRequest request)
        => EnqueueAndWait<VoteResponse>(manager.GetLocalEndpoint(), node.Endpoint, MessageKind.Vote, request);

    /// <inheritdoc/>
    public Task<AppendLogsResponse> AppendLogs(RaftManager manager, RaftNode node, AppendLogsRequest request)
        => EnqueueAndWait<AppendLogsResponse>(manager.GetLocalEndpoint(), node.Endpoint, MessageKind.AppendLogs, request);

    /// <inheritdoc/>
    public Task<CompleteAppendLogsResponse> CompleteAppendLogs(RaftManager manager, RaftNode node, CompleteAppendLogsRequest request)
        => EnqueueAndWait<CompleteAppendLogsResponse>(manager.GetLocalEndpoint(), node.Endpoint, MessageKind.CompleteAppendLogs, request);

    /// <inheritdoc/>
    public Task<BatchRequestsResponse> BatchRequests(RaftManager manager, RaftNode node, BatchRequestsRequest request)
        => EnqueueAndWait<BatchRequestsResponse>(manager.GetLocalEndpoint(), node.Endpoint, MessageKind.BatchRequests, request);

    public Task<JoinResponse> SendJoin(RaftManager manager, RaftNode node, JoinRequest request)
        => Task.FromResult(new JoinResponse(false));

    public Task<LeaveResponse> SendLeave(RaftManager manager, RaftNode node, LeaveRequest request)
        => Task.FromResult(new LeaveResponse(false));

    // ── Internal ──────────────────────────────────────────────────────────

    private Task<TResponse> EnqueueAndWait<TResponse>(string source, string dest, MessageKind kind, object payload)
        where TResponse : new()
    {
        CapturedMessage msg = new()
        {
            SourceEndpoint = source,
            DestinationEndpoint = dest,
            Kind = kind,
            Payload = payload,
        };

        lock (_lock)
        {
            SentMessages.Add(msg);
            _queue.Enqueue(msg);
        }

        return msg.Reply.Task.ContinueWith(
            t => t.IsFaulted ? throw t.Exception!.InnerException! : (TResponse)t.Result!,
            TaskContinuationOptions.ExecuteSynchronously);
    }

    private static object DefaultReply(MessageKind kind) => kind switch
    {
        MessageKind.Handshake          => new HandshakeResponse(),
        MessageKind.RequestVotes       => new RequestVotesResponse(),
        MessageKind.Vote               => new VoteResponse(),
        MessageKind.AppendLogs         => new AppendLogsResponse(),
        MessageKind.CompleteAppendLogs => new CompleteAppendLogsResponse(),
        MessageKind.BatchRequests      => new BatchRequestsResponse(),
        _ => new object(),
    };
}
