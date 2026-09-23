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
/// <para><b>Frozen endpoints.</b> <see cref="FreezeEndpoint"/> models a stopped process rather than
/// a broken link. Every consensus RPC addressed to a frozen endpoint queues, whatever
/// <see cref="HoldMessages"/> says, and no delivery method will release one while it stays frozen.
/// <see cref="ThawEndpoint"/> lets the whole backlog through on the next delivery round, in arrival
/// order — the burst a real <c>SIGSTOP</c> and <c>SIGCONT</c> pair produces, and a different failure
/// from a blocked link, which loses the traffic instead of storing it.</para>
///
/// <para><b>What a freeze does not cover.</b> <c>Handshake</c> and the control-plane RPCs still
/// answer, for the reason given above: their replies are synchronous and carry real state, so
/// holding one fabricates an answer rather than delaying it. A frozen node is therefore silent on
/// the consensus path and still responsive on the control plane. That is a modelled limitation, not
/// a property of a stopped process.</para>
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

    /// <summary>Endpoints whose process is modelled as stopped. Messages to them queue and stay queued.</summary>
    private readonly HashSet<string> frozenEndpoints = [];

    /// <summary>Endpoints whose process is modelled as dead. Messages to them are refused at once.</summary>
    private readonly HashSet<string> downEndpoints = [];

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

    /// <summary>Endpoints isolated by <see cref="PartitionNode"/>, mirrored from the inner transport.</summary>
    private readonly HashSet<string> partitionedEndpoints = [];

    /// <summary>Drops all traffic to and from <paramref name="endpoint"/> until healed.</summary>
    public void PartitionNode(string endpoint)
    {
        lock (gate)
            partitionedEndpoints.Add(endpoint);

        inner.PartitionNode(endpoint);
    }

    /// <summary>Restores traffic to and from <paramref name="endpoint"/>.</summary>
    public void HealPartition(string endpoint)
    {
        lock (gate)
            partitionedEndpoints.Remove(endpoint);

        inner.HealPartition(endpoint);
    }

    /// <summary>
    /// True when no link fault, isolation, frozen or dead endpoint, or duplication is set: the wire is
    /// perfect. The held-message switch is not a fault.
    /// </summary>
    public bool IsHealthy
    {
        get
        {
            lock (gate)
                return partitionedEndpoints.Count == 0
                       && frozenEndpoints.Count == 0
                       && downEndpoints.Count == 0
                       && linkFaults.Values.All(fault => !fault.Blocked && fault.Copies <= 1);
        }
    }

    /// <summary>
    /// True when a message from <paramref name="from"/> would reach a running process at
    /// <paramref name="to"/> now: neither end is isolated, the link is not blocked in that direction,
    /// and the receiver is neither dead nor stopped.
    ///
    /// <para>For the harness's failure-detector model, which asks what a SWIM probe would see. A
    /// stopped process counts as unreachable here, although the transport still lets its control
    /// plane answer (see "What a freeze does not cover"): a real stopped process answers no probe,
    /// and the model is about the real one.</para>
    /// </summary>
    public bool CanDeliver(string from, string to)
    {
        lock (gate)
        {
            if (partitionedEndpoints.Contains(from) || partitionedEndpoints.Contains(to))
                return false;

            if (downEndpoints.Contains(to) || frozenEndpoints.Contains(to))
                return false;

            return !(linkFaults.TryGetValue((from, to), out LinkFault fault) && fault.Blocked);
        }
    }

    /// <summary>
    /// Stops <paramref name="endpoint"/> receiving consensus traffic and starts storing it.
    ///
    /// <para>This is a stopped process, not a cut cable. The difference is what happens on
    /// recovery: a blocked link has already lost its traffic, while a frozen endpoint still holds
    /// every message and takes them all at once. Several defects only appear in that burst.</para>
    /// </summary>
    public void FreezeEndpoint(string endpoint)
    {
        lock (gate)
            frozenEndpoints.Add(endpoint);
    }

    /// <summary>
    /// Lets <paramref name="endpoint"/> receive again. The stored backlog goes out on the next
    /// delivery round, oldest first.
    /// </summary>
    public void ThawEndpoint(string endpoint)
    {
        lock (gate)
            frozenEndpoints.Remove(endpoint);
    }

    /// <summary>True while <paramref name="endpoint"/> is modelled as a stopped process.</summary>
    public bool IsFrozen(string endpoint)
    {
        lock (gate)
            return frozenEndpoints.Contains(endpoint);
    }

    /// <summary>
    /// Models the death of the process behind <paramref name="endpoint"/>: whatever was waiting for
    /// it is lost, and nothing new is stored until it comes back. Returns how many stored messages
    /// the death took.
    ///
    /// <para><b>Why a crash is not a deep pause.</b> A stopped process still owns its socket, so
    /// its peers' traffic waits and arrives in one burst when it wakes. A dead process owns
    /// nothing: the connection is reset, the sender learns at once, and the messages are gone. The
    /// difference is not cosmetic. Storing traffic across a crash delivers pre-crash messages to a
    /// manager built after it, carrying terms and indices from a life the new process never had —
    /// something no real network does, and a failure the harness would then report as the
    /// library's.</para>
    ///
    /// <para>Found by the random search on its first outing, which is the argument for the random
    /// search in one sentence: the scripted scenarios all crash a node whose peers happen to have
    /// nothing stored for it.</para>
    /// </summary>
    public int MarkDown(string endpoint)
    {
        lock (gate)
        {
            downEndpoints.Add(endpoint);

            int lost = pending.RemoveAll(message => message.To == endpoint);
            droppedCount += lost;

            return lost;
        }
    }

    /// <summary>Lets a restarted process receive again.</summary>
    public void MarkUp(string endpoint)
    {
        lock (gate)
            downEndpoints.Remove(endpoint);
    }

    /// <summary>True while the process behind <paramref name="endpoint"/> is modelled as dead.</summary>
    public bool IsDown(string endpoint)
    {
        lock (gate)
            return downEndpoints.Contains(endpoint);
    }

    /// <summary>Messages stored for <paramref name="endpoint"/> while it is frozen.</summary>
    public int FrozenBacklog(string endpoint)
    {
        lock (gate)
            return pending.Count(message => message.To == endpoint);
    }

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
        PendingMessage? message = Take(messageId, respectFreeze: true);
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
            int index = pending.FindIndex(candidate => !frozenEndpoints.Contains(candidate.To));
            if (index < 0)
                return false;

            message = pending[index];
            pending.RemoveAt(index);
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
            // A frozen endpoint keeps its messages. They are not lost and not delivered: they wait,
            // which is the whole difference between a stopped process and a cut link.
            batch = pending.Where(message => !frozenEndpoints.Contains(message.To)).ToList();
            pending.RemoveAll(message => !frozenEndpoints.Contains(message.To));
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
        if (Take(messageId, respectFreeze: false) is null)
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

    /// <summary>
    /// Consensus traffic that carries log entries, copied the way a wire copies it.
    ///
    /// <para>See <see cref="Serialize(AppendLogsRequest, string)"/> for when the copy is taken, and
    /// why every delivery gets its own.</para>
    /// </summary>
    public Task<AppendLogsResponse> AppendLogs(RaftManager manager, RaftNode node, AppendLogsRequest request)
    {
        string from = manager.LocalEndpoint;
        string to = node.Endpoint;

        if (IsOversized(Estimate(request)))
            return RefuseOversized(EmptyAppendLogs);

        Func<AppendLogsRequest> wire = Serialize(request, to);

        return Intercept(
            manager,
            node,
            "AppendLogs",
            () =>
            {
                AppendLogsRequest received = wire();
                ObserveDelivery(from, to, received);
                return inner.AppendLogs(manager, node, received);
            },
            EmptyAppendLogs);
    }

    public Task<CompleteAppendLogsResponse> CompleteAppendLogs(
        RaftManager manager, RaftNode node, CompleteAppendLogsRequest request) =>
        Intercept(
            manager,
            node,
            "CompleteAppendLogs",
            () => inner.CompleteAppendLogs(manager, node, request),
            EmptyCompleteAppendLogs);

    /// <summary>
    /// Batched consensus traffic, copied before it is intercepted.
    ///
    /// <para><b>The copy is required, not defensive.</b> <c>RaftTransportDispatcher</c> rents the
    /// batch wrapper, its item list, and each item from a pool and returns them to it as soon as
    /// the send completes — its own summary says the transport may reference them only until the
    /// call finishes. This transport breaks that contract on purpose: a held message answers at
    /// once and is actually sent later, and a duplicated one is sent again without anybody waiting.
    /// By then the pooled objects belong to a different batch, and the receiver enumerating the
    /// list sees it change underneath it.</para>
    ///
    /// <para>The containers are copied at once. The log entries inside are deep-copied too, for a
    /// different reason: see <see cref="Serialize(BatchRequestsRequest, string)"/>.</para>
    ///
    /// <para>Found by the random search on its first outing. It needed a message stored while a
    /// node was paused and released in the burst after it woke — a delay long enough for the pool
    /// to hand the same list to somebody else. No scripted scenario had held one that long.</para>
    /// </summary>
    public Task<BatchRequestsResponse> BatchRequests(RaftManager manager, RaftNode node, BatchRequestsRequest request)
    {
        string from = manager.LocalEndpoint;
        string to = node.Endpoint;

        if (IsOversized(Estimate(request)))
            return RefuseOversized(EmptyBatchRequests);

        Func<BatchRequestsRequest> wire = Serialize(request, to);

        return Intercept(
            manager,
            node,
            "BatchRequests",
            () =>
            {
                BatchRequestsRequest received = wire();

                foreach (BatchRequestsRequestItem item in received.Requests ?? [])
                {
                    if (item.AppendLogs is not null)
                        ObserveDelivery(from, to, item.AppendLogs);
                }

                return inner.BatchRequests(manager, node, received);
            },
            EmptyBatchRequests);
    }

    // ── The wire copy ─────────────────────────────────────────────────────

    /// <summary>Endpoints whose inbound traffic is copied at delivery rather than at send.</summary>
    private readonly HashSet<string> lateSerialization = [];

    /// <summary>
    /// Largest message, in estimated bytes, this transport delivers. Null, the default, delivers
    /// everything.
    ///
    /// <para>The in-memory transport has no size limit, and production does: gRPC refuses a frame
    /// above <see cref="RaftConfiguration.GrpcMaxMessageBytes"/>, and every entry on the refused frame
    /// goes back to the retry path (<c>2ec4f92</c>). A simulation with no limit can never reach that
    /// path. Set this to the same value as the nodes' <c>GrpcMaxMessageBytes</c>. A message over the
    /// limit is dropped and counted in <see cref="OversizedCount"/>.</para>
    ///
    /// <para><b>The size is an estimate.</b> It adds each entry's payload and type name to a fixed
    /// cost for each entry and each request. The estimate is close to the protobuf size for the
    /// payloads Kommander ships, and it never under-counts the payload itself, which is the part
    /// that decides whether a frame fits.</para>
    /// </summary>
    public long? MaxMessageBytes { get; set; }

    /// <summary>Messages dropped because they were larger than <see cref="MaxMessageBytes"/>.</summary>
    public long OversizedCount => Interlocked.Read(ref oversizedCount);

    private long oversizedCount;

    /// <summary>
    /// Called on every delivery of log entries, with the entries exactly as the receiver gets them.
    ///
    /// <para>For a scenario that must prove it reached a state rather than assume it. Example: a
    /// follower whose first sight of an entry is the committed row. That state depends on when the
    /// wire copy was taken, and a scenario that does not check it can pass without the state.</para>
    /// </summary>
    public event Action<string, string, IReadOnlyList<RaftLog>>? AppendLogsDelivered;

    /// <summary>
    /// Takes the wire copy of traffic to <paramref name="endpoint"/> at delivery, not at send, while
    /// <paramref name="enabled"/> is true.
    ///
    /// <para><b>What this models.</b> In production the leader does not serialize a message when it
    /// decides to send it. The responder serializes it later, when it takes the message off its
    /// queue. The leader's commit path changes <c>log.Type</c> in place on the same objects, so a
    /// responder that runs late sends an entry typed <c>Committed</c> that the follower never saw
    /// as proposed (<c>367eac9</c>). A held message with a late copy is that responder delay: the
    /// message waits with its references, and the copy happens when it leaves.</para>
    ///
    /// <para>Off by default, because the ordinary copy at send is the ordinary case. It changes
    /// nothing for a message that is delivered at once.</para>
    /// </summary>
    public void SetLateSerialization(string endpoint, bool enabled)
    {
        lock (gate)
        {
            if (enabled)
                lateSerialization.Add(endpoint);
            else
                lateSerialization.Remove(endpoint);
        }
    }

    /// <summary>
    /// Returns a function that produces what the receiver gets: a new, deep copy of the message on
    /// each call.
    ///
    /// <para><b>Why a deep copy.</b> Over gRPC a receiver deserializes new objects, and nothing the
    /// sender does later can reach them. The in-memory transport passes references, so the leader's
    /// in-place <c>log.Type = Committed</c> reached every follower's store directly, and a follower
    /// seemed to hold a committed marker it never wrote. A crash could then never lose a marker,
    /// and the random search could not find <c>367eac9</c>. A node must never hold an object that
    /// another node can change.</para>
    ///
    /// <para><b>Why a copy for each delivery.</b> A duplicated message is deserialized once for each
    /// copy in production. The receivers must not share one object, because a receiver can change
    /// what it received.</para>
    ///
    /// <para><b>When the first copy is taken.</b> At send, which is when a real frame leaves the
    /// sender. With <see cref="SetLateSerialization"/> on for the receiver, at delivery instead.</para>
    /// </summary>
    private Func<AppendLogsRequest> Serialize(AppendLogsRequest request, string to)
    {
        if (IsLateSerialized(to))
            return () => DeepCopy(request);

        AppendLogsRequest sent = DeepCopy(request);
        return () => DeepCopy(sent);
    }

    /// <inheritdoc cref="Serialize(AppendLogsRequest, string)"/>
    private Func<BatchRequestsRequest> Serialize(BatchRequestsRequest request, string to)
    {
        // The pooled containers are always copied at once: the pool takes them back when the send
        // returns, whatever the receiver. Only the entries inside may wait for a late copy.
        BatchRequestsRequest containers = CopyContainers(request);

        if (IsLateSerialized(to))
            return () => DeepCopy(containers);

        BatchRequestsRequest sent = DeepCopy(containers);
        return () => DeepCopy(sent);
    }

    private bool IsLateSerialized(string endpoint)
    {
        lock (gate)
            return lateSerialization.Contains(endpoint);
    }

    private void ObserveDelivery(string from, string to, AppendLogsRequest request)
    {
        if (request.Logs is { Count: > 0 } logs)
            AppendLogsDelivered?.Invoke(from, to, logs);
    }

    /// <summary>
    /// Copies a batch far enough that the pool cannot take it back.
    ///
    /// <para><b>The copy is required, not defensive.</b> <c>RaftTransportDispatcher</c> rents the
    /// batch wrapper, its item list, and each item from a pool and returns them as soon as the send
    /// completes. A held message is sent later, and by then the pooled objects belong to a different
    /// batch. Found by the random search on its first outing.</para>
    /// </summary>
    private static BatchRequestsRequest CopyContainers(BatchRequestsRequest request)
    {
        if (request.Requests is null)
            return new BatchRequestsRequest();

        List<BatchRequestsRequestItem> items = new(request.Requests.Count);

        foreach (BatchRequestsRequestItem item in request.Requests)
            items.Add(CopyItem(item, item.AppendLogs));

        return new BatchRequestsRequest { Requests = items };
    }

    private static BatchRequestsRequest DeepCopy(BatchRequestsRequest request)
    {
        List<BatchRequestsRequestItem> items = new(request.Requests?.Count ?? 0);

        foreach (BatchRequestsRequestItem item in request.Requests ?? [])
            items.Add(CopyItem(item, item.AppendLogs is null ? null : DeepCopy(item.AppendLogs)));

        return new BatchRequestsRequest { Requests = items };
    }

    /// <summary>
    /// Copies one batch item. Only <c>AppendLogs</c> carries objects a sender changes after the
    /// send; the other requests hold values only, and the sender never changes them.
    /// </summary>
    private static BatchRequestsRequestItem CopyItem(BatchRequestsRequestItem item, AppendLogsRequest? appendLogs) =>
        new()
        {
            Type = item.Type,
            Handshake = item.Handshake,
            Vote = item.Vote,
            RequestVotes = item.RequestVotes,
            StepDownNotice = item.StepDownNotice,
            TransferLeadership = item.TransferLeadership,
            AppendLogs = appendLogs,
            CompleteAppendLogs = item.CompleteAppendLogs,
            TransferLeadershipSuggestion = item.TransferLeadershipSuggestion,
        };

    private static AppendLogsRequest DeepCopy(AppendLogsRequest request) =>
        new(
            request.Partition,
            request.Term,
            request.Time,
            request.Endpoint,
            request.Logs?.Select(DeepCopy).ToList(),
            request.PrevLogIndex,
            request.PrevLogTerm)
        {
            Quiesce = request.Quiesce,
        };

    private static RaftLog DeepCopy(RaftLog log) => new()
    {
        Id = log.Id,
        Type = log.Type,
        Term = log.Term,
        Time = log.Time,
        LogType = log.LogType,
        LogData = log.LogData is null ? null : (byte[])log.LogData.Clone(),
    };

    // ── The size limit ────────────────────────────────────────────────────

    /// <summary>Fixed cost of one request: partition, term, time, endpoint and anchors.</summary>
    private const long RequestOverheadBytes = 64;

    /// <summary>Fixed cost of one entry: id, type, term, time and framing.</summary>
    private const long EntryOverheadBytes = 32;

    private static long Estimate(AppendLogsRequest request)
    {
        long bytes = RequestOverheadBytes + (request.Endpoint?.Length ?? 0);

        foreach (RaftLog log in request.Logs ?? [])
            bytes += EntryOverheadBytes + (log.LogData?.Length ?? 0) + (log.LogType?.Length ?? 0);

        return bytes;
    }

    private static long Estimate(BatchRequestsRequest request)
    {
        long bytes = 0;

        foreach (BatchRequestsRequestItem item in request.Requests ?? [])
            bytes += item.AppendLogs is null ? RequestOverheadBytes : Estimate(item.AppendLogs);

        return bytes;
    }

    private bool IsOversized(long estimatedBytes) =>
        MaxMessageBytes is { } limit && estimatedBytes > limit;

    /// <summary>
    /// Drops an oversized message. The sender gets the empty response, as for any lost message:
    /// the entries on it go back to the retry path, which is what a refused gRPC frame causes.
    /// </summary>
    private Task<TResponse> RefuseOversized<TResponse>(Task<TResponse> emptyResponse)
    {
        Interlocked.Increment(ref oversizedCount);
        Interlocked.Increment(ref droppedCount);
        return emptyResponse;
    }

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
        IReadOnlyList<byte[]> logs, bool autoCommit, long expectedGeneration, long expectedTerm,
        CancellationToken cancellationToken = default) =>
        inner.ForwardReplicateLogs(
            manager, node, partitionId, type, logs, autoCommit, expectedGeneration, expectedTerm, cancellationToken);

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

        // A dead process refuses before anything is stored for it. See MarkDown: storing traffic
        // across a crash would hand a message written for one process to the one that replaced it.
        if (IsDown(to))
        {
            Interlocked.Increment(ref droppedCount);
            return heldResponse;
        }

        int copies = fault.Copies < 1 ? 1 : fault.Copies;

        // A frozen endpoint queues regardless of the hold switch. Its process is stopped, so the
        // message reaches its socket and sits there; the scenario did not have to hold the wire to
        // ask for that.
        if (!HoldMessages && !IsFrozen(to))
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

    /// <param name="respectFreeze">
    /// True for delivery, which must not reach a frozen endpoint. False for a drop, which models a
    /// lost packet and may discard a message a stopped process was never going to read anyway.
    /// </param>
    private PendingMessage? Take(long messageId, bool respectFreeze)
    {
        lock (gate)
        {
            int index = pending.FindIndex(candidate => candidate.Id == messageId);
            if (index < 0)
                return null;

            if (respectFreeze && frozenEndpoints.Contains(pending[index].To))
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
