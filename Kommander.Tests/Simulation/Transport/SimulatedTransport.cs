using Kommander.Communication;
using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Gossip;

namespace Kommander.Tests.Simulation.Transport;

/// <summary>
/// A cluster-aware network model for deterministic simulation runs.
///
/// <para><b>It wraps the production transport, it does not replace it.</b> Every delivery ends
/// in <see cref="InMemoryCommunication"/>, which is library code. The wrapper adds only the
/// three things a simulation needs and the production transport has no reason to carry: a
/// hold queue, a per-message identity, and an explicit drop. The tested surface therefore stays
/// the real one.</para>
///
/// <para><b>Hold queue.</b> While <see cref="HoldMessages"/> is true, a consensus RPC is not
/// delivered. It is recorded with a stable id and the caller receives the same empty response
/// the in-memory transport already returns. The harness then chooses which message is delivered
/// next, in which order, and which is dropped. That choice is the event a replay log records.</para>
///
/// <para><b>Why the empty response is correct.</b> The held RPCs are one-way in the in-memory
/// transport: <c>RequestVotes</c>, <c>Vote</c>, <c>AppendLogs</c>, <c>CompleteAppendLogs</c> and
/// <c>BatchRequests</c> all return a constant empty response, and the real reply travels back as
/// its own RPC. Holding one therefore delays a message without inventing an answer the caller
/// would not otherwise get.</para>
///
/// <para><b>What is never held.</b> <c>Handshake</c>, alone among the Raft RPCs, answers
/// synchronously with the peer's real state. Holding it would hand the caller a fabricated empty
/// handshake, which is a different protocol rather than a delayed one, so it passes straight
/// through. The control-plane RPCs (join, leave, gossip, ping, snapshot, forwarding) also pass
/// through: deferring them would stall a scenario without adding coverage.</para>
///
/// <para><b>Ordering.</b> Ids are allocated under one lock, so the queue preserves the order in
/// which the calls arrived. Delivery order is the harness's choice, not the arrival order, so
/// reordering needs no separate feature: hold the wire, then deliver by id in any order.</para>
///
/// <para><b>Link faults.</b> A one-way block and a duplication factor are set per ordered pair of
/// endpoints. One-way matters on its own: a link that fails in one direction only is what
/// separates a genuinely partitioned leader from one that can still hear its followers, and the
/// two produce very different elections. Duplication matters because Raft claims idempotence for
/// its RPCs, and a claim of idempotence is worth testing.</para>
/// </summary>
public sealed class SimulatedTransport : ICommunication
{
    private readonly InMemoryCommunication inner = new();
    private readonly object gate = new();
    private readonly List<PendingMessage> pending = [];

    private readonly Dictionary<(string From, string To), LinkFault> linkFaults = [];

    private long nextMessageId = 1;
    private long deliveredCount;
    private long droppedCount;
    private long duplicatedCount;

    /// <summary>Fault settings for one ordered pair of endpoints.</summary>
    /// <param name="Blocked">Drop every message on this link, in this direction only.</param>
    /// <param name="Copies">How many times a delivered message arrives. 1 is normal.</param>
    private readonly record struct LinkFault(bool Blocked, int Copies);

    /// <summary>
    /// When true, consensus RPCs are queued instead of delivered. The harness releases them
    /// with <see cref="DeliverNext"/>, <see cref="Deliver"/>, or <see cref="DeliverAll"/>.
    /// Default false, so a cluster built on this transport behaves exactly like one built on
    /// <see cref="InMemoryCommunication"/> until a scenario asks for control.
    /// </summary>
    public bool HoldMessages { get; set; }

    /// <summary>Total messages delivered through this transport, held or not.</summary>
    public long DeliveredCount => Interlocked.Read(ref deliveredCount);

    /// <summary>Total messages the harness dropped without delivering.</summary>
    public long DroppedCount => Interlocked.Read(ref droppedCount);

    /// <summary>Total extra copies delivered because a link duplicates.</summary>
    public long DuplicatedCount => Interlocked.Read(ref duplicatedCount);

    /// <summary>
    /// Drops every message from <paramref name="from"/> to <paramref name="to"/>, and only in that
    /// direction. Call it twice, once each way, for a symmetric partition; or use
    /// <see cref="PartitionNode"/>, which cuts a node off in both directions.
    /// </summary>
    public void BlockLink(string from, string to) => UpdateLink(from, to, fault => fault with { Blocked = true });

    /// <summary>Restores a link blocked by <see cref="BlockLink"/>.</summary>
    public void UnblockLink(string from, string to) => UpdateLink(from, to, fault => fault with { Blocked = false });

    /// <summary>
    /// Makes every message on this link arrive <paramref name="copies"/> times.
    ///
    /// <para>Raft's RPCs are meant to be idempotent, so a duplicate must change nothing. A value
    /// of 1 restores normal delivery; 0 and negatives are treated as 1, because "deliver zero
    /// copies" is a block and belongs in <see cref="BlockLink"/> where it reads as one.</para>
    /// </summary>
    public void SetLinkDuplication(string from, string to, int copies) =>
        UpdateLink(from, to, fault => fault with { Copies = copies < 1 ? 1 : copies });

    /// <summary>Clears every link fault. The wire returns to perfect.</summary>
    public void ClearLinkFaults()
    {
        lock (gate)
            linkFaults.Clear();
    }

    /// <summary>Registers the cluster routing table. Delegates to the wrapped transport.</summary>
    public void SetNodes(Dictionary<string, IRaft> nodes) => inner.SetNodes(nodes);

    /// <summary>Drops all traffic to and from <paramref name="endpoint"/> until healed.</summary>
    public void PartitionNode(string endpoint) => inner.PartitionNode(endpoint);

    /// <summary>Restores traffic to and from <paramref name="endpoint"/>.</summary>
    public void HealPartition(string endpoint) => inner.HealPartition(endpoint);

    /// <summary>Number of messages currently waiting in the hold queue.</summary>
    public int PendingCount
    {
        get { lock (gate) return pending.Count; }
    }

    /// <summary>
    /// Snapshot of the hold queue for <see cref="SimulationSnapshot"/> and failure reports.
    /// Returned in queue order, so the first entry is the oldest held message.
    /// </summary>
    public IReadOnlyList<SimulationPendingMessageSnapshot> GetPendingSnapshots()
    {
        lock (gate)
            return pending
                .Select(message => new SimulationPendingMessageSnapshot
                {
                    MessageId = message.Id,
                    FromNode = message.From,
                    ToNode = message.To,
                    MessageType = message.MessageType,
                    ScheduledDeliveryTime = message.EnqueuedLogicalTime,
                })
                .ToList();
    }

    /// <summary>
    /// Delivers the message with <paramref name="messageId"/> and returns true, or returns false
    /// when no such message is queued. Delivery runs the wrapped transport's real call.
    /// </summary>
    public async Task<bool> Deliver(long messageId)
    {
        PendingMessage? message = Take(messageId);
        if (message is null)
            return false;

        await message.Send().ConfigureAwait(false);
        Interlocked.Increment(ref deliveredCount);
        return true;
    }

    /// <summary>Delivers the oldest queued message. Returns false when the queue is empty.</summary>
    public async Task<bool> DeliverNext()
    {
        PendingMessage? message;
        lock (gate)
        {
            if (pending.Count == 0)
                return false;

            message = pending[0];
            pending.RemoveAt(0);
        }

        await message.Send().ConfigureAwait(false);
        Interlocked.Increment(ref deliveredCount);
        return true;
    }

    /// <summary>
    /// Delivers every message currently queued, oldest first, and returns how many were sent.
    /// Messages enqueued by the deliveries themselves stay queued for the next call, so one
    /// call is one round of the network rather than an unbounded cascade.
    /// </summary>
    public async Task<int> DeliverAll()
    {
        List<PendingMessage> batch;
        lock (gate)
        {
            batch = [.. pending];
            pending.Clear();
        }

        foreach (PendingMessage message in batch)
        {
            await message.Send().ConfigureAwait(false);
            Interlocked.Increment(ref deliveredCount);
        }

        return batch.Count;
    }

    /// <summary>
    /// Discards the message with <paramref name="messageId"/> without delivering it, which models
    /// a lost packet. Returns false when no such message is queued.
    /// </summary>
    public bool Drop(long messageId)
    {
        if (Take(messageId) is null)
            return false;

        Interlocked.Increment(ref droppedCount);
        return true;
    }

    /// <summary>Discards every queued message and returns how many were dropped.</summary>
    public int DropAll()
    {
        int count;
        lock (gate)
        {
            count = pending.Count;
            pending.Clear();
        }

        Interlocked.Add(ref droppedCount, count);
        return count;
    }

    // ── ICommunication: consensus RPCs (interceptable) ─────────────────────

    private static readonly Task<RequestVotesResponse> EmptyRequestVotes = Task.FromResult(new RequestVotesResponse());
    private static readonly Task<VoteResponse> EmptyVote = Task.FromResult(new VoteResponse());
    private static readonly Task<AppendLogsResponse> EmptyAppendLogs = Task.FromResult(new AppendLogsResponse());
    private static readonly Task<CompleteAppendLogsResponse> EmptyCompleteAppendLogs =
        Task.FromResult(new CompleteAppendLogsResponse());
    private static readonly Task<BatchRequestsResponse> EmptyBatchRequests = Task.FromResult(new BatchRequestsResponse());

    /// <summary>
    /// Always delivered inline. See "What is never held": the handshake reply is synchronous and
    /// carries the peer's real state, so a held handshake would be a fabricated answer.
    /// </summary>
    public Task<HandshakeResponse> Handshake(RaftManager manager, RaftNode node, HandshakeRequest request)
    {
        Interlocked.Increment(ref deliveredCount);
        return inner.Handshake(manager, node, request);
    }

    public Task<RequestVotesResponse> RequestVotes(RaftManager manager, RaftNode node, RequestVotesRequest request) =>
        Intercept(manager, node, "RequestVotes", () => inner.RequestVotes(manager, node, request), EmptyRequestVotes);

    public Task<VoteResponse> Vote(RaftManager manager, RaftNode node, VoteRequest request) =>
        Intercept(manager, node, "Vote", () => inner.Vote(manager, node, request), EmptyVote);

    public Task<AppendLogsResponse> AppendLogs(RaftManager manager, RaftNode node, AppendLogsRequest request) =>
        Intercept(manager, node, "AppendLogs", () => inner.AppendLogs(manager, node, request), EmptyAppendLogs);

    public Task<CompleteAppendLogsResponse> CompleteAppendLogs(
        RaftManager manager, RaftNode node, CompleteAppendLogsRequest request) =>
        Intercept(
            manager,
            node,
            "CompleteAppendLogs",
            () => inner.CompleteAppendLogs(manager, node, request),
            EmptyCompleteAppendLogs);

    public Task<BatchRequestsResponse> BatchRequests(RaftManager manager, RaftNode node, BatchRequestsRequest request) =>
        Intercept(manager, node, "BatchRequests", () => inner.BatchRequests(manager, node, request), EmptyBatchRequests);

    // ── ICommunication: control-plane RPCs (always inline) ─────────────────

    public Task<JoinResponse> SendJoin(RaftManager manager, RaftNode node, JoinRequest request) =>
        inner.SendJoin(manager, node, request);

    public Task<LeaveResponse> SendLeave(
        RaftManager manager, RaftNode node, LeaveRequest request, CancellationToken cancellationToken = default) =>
        inner.SendLeave(manager, node, request, cancellationToken);

    public Task<SetMemberRoleResponse> SendSetMemberRole(
        RaftManager manager, RaftNode node, SetMemberRoleRequest request, CancellationToken cancellationToken = default) =>
        inner.SendSetMemberRole(manager, node, request, cancellationToken);

    public Task<GossipAck> SendGossip(
        RaftManager manager, RaftNode node, GossipMessage digest, CancellationToken cancellationToken = default) =>
        inner.SendGossip(manager, node, digest, cancellationToken);

    public Task<Gossip.PingResponse> SendPing(
        RaftManager manager, RaftNode node, Gossip.PingRequest request, CancellationToken cancellationToken = default) =>
        inner.SendPing(manager, node, request, cancellationToken);

    public Task<Gossip.PingReqResponse> SendPingReq(
        RaftManager manager, RaftNode node, Gossip.PingReqRequest request, CancellationToken cancellationToken = default) =>
        inner.SendPingReq(manager, node, request, cancellationToken);

    public Task<long?> GetRemoteFollowerLag(
        RaftManager manager, RaftNode node, int partitionId, string followerEndpoint) =>
        inner.GetRemoteFollowerLag(manager, node, partitionId, followerEndpoint);

    public Task<SnapshotResponse> SendInstallSnapshot(
        RaftManager manager, RaftNode node, SnapshotRequest request, CancellationToken cancellationToken = default) =>
        inner.SendInstallSnapshot(manager, node, request, cancellationToken);

    public Task NotifyJoinBlocked(
        RaftManager manager, string targetEndpoint, string reason, CancellationToken cancellationToken = default) =>
        inner.NotifyJoinBlocked(manager, targetEndpoint, reason, cancellationToken);

    public Task<GetReadIndexResponse> GetReadIndex(
        RaftManager manager, RaftNode node, GetReadIndexRequest request, CancellationToken cancellationToken = default) =>
        inner.GetReadIndex(manager, node, request, cancellationToken);

    public Task<RaftReplicationResult?> ForwardReplicateLogs(
        RaftManager manager, RaftNode node, int partitionId, string type,
        IReadOnlyList<byte[]> logs, bool autoCommit, long expectedGeneration,
        CancellationToken cancellationToken = default) =>
        inner.ForwardReplicateLogs(
            manager, node, partitionId, type, logs, autoCommit, expectedGeneration, cancellationToken);

    // ── Internals ─────────────────────────────────────────────────────────

    /// <summary>
    /// Delivers immediately when the hold queue is off, otherwise records the call and answers
    /// with the transport's own empty response.
    /// </summary>
    private Task<TResponse> Intercept<TResponse>(
        RaftManager manager,
        RaftNode node,
        string messageType,
        Func<Task<TResponse>> send,
        Task<TResponse> heldResponse)
    {
        string from = manager.LocalEndpoint;
        string to = node.Endpoint;
        LinkFault fault = GetLinkFault(from, to);

        // A blocked link drops before anything else. The caller still gets the transport's empty
        // response, which is what it would get from a real send into a black hole.
        if (fault.Blocked)
        {
            Interlocked.Increment(ref droppedCount);
            return heldResponse;
        }

        int copies = fault.Copies < 1 ? 1 : fault.Copies;

        if (!HoldMessages)
        {
            Interlocked.Increment(ref deliveredCount);

            // The extra copies are fire-and-forget, exactly as a duplicating network would deliver
            // them: the caller waits for one send and never learns the others happened.
            for (int copy = 1; copy < copies; copy++)
            {
                Interlocked.Increment(ref duplicatedCount);
                _ = send();
            }

            return send();
        }

        lock (gate)
        {
            for (int copy = 0; copy < copies; copy++)
            {
                if (copy > 0)
                    Interlocked.Increment(ref duplicatedCount);

                pending.Add(new PendingMessage(
                    nextMessageId++,
                    from,
                    to,
                    messageType,
                    Environment.TickCount64,
                    async () => await send().ConfigureAwait(false)));
            }
        }

        return heldResponse;
    }

    private LinkFault GetLinkFault(string from, string to)
    {
        lock (gate)
            return linkFaults.TryGetValue((from, to), out LinkFault fault) ? fault : new LinkFault(false, 1);
    }

    private void UpdateLink(string from, string to, Func<LinkFault, LinkFault> update)
    {
        lock (gate)
        {
            LinkFault current = linkFaults.TryGetValue((from, to), out LinkFault existing)
                ? existing
                : new LinkFault(false, 1);

            linkFaults[(from, to)] = update(current);
        }
    }

    private PendingMessage? Take(long messageId)
    {
        lock (gate)
        {
            int index = pending.FindIndex(candidate => candidate.Id == messageId);
            if (index < 0)
                return null;

            PendingMessage message = pending[index];
            pending.RemoveAt(index);
            return message;
        }
    }

    /// <summary>One held RPC, replayable by invoking <see cref="Send"/>.</summary>
    private sealed record PendingMessage(
        long Id,
        string From,
        string To,
        string MessageType,
        long EnqueuedLogicalTime,
        Func<Task> Send);
}
