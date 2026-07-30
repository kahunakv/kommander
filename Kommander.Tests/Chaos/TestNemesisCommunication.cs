
using Kommander;
using Kommander.Communication;
using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Diagnostics;
using Kommander.Discovery;
using Kommander.Gossip;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.Chaos;

/// <summary>
/// Unit tests for <see cref="NemesisCommunication"/> covering the increment-D gate: every interface
/// method and batch item type is classified; asymmetric and symmetric link policies; held-release order;
/// cancellation cleanup of held/delayed calls; deep-copy of snapshot bytes across a deferred delivery;
/// deterministic decisions for a fixed envelope order + seed; unique sequence allocation under
/// concurrency; and that no lock is held while a blocking inner transport runs.
/// </summary>
public sealed class TestNemesisCommunication : IDisposable
{
    private const string Src = "localhost:9500";
    private const string Dst = "localhost:9600";

    private readonly RaftManager _manager;
    private readonly RaftNode _node = new(Dst);

    public TestNemesisCommunication()
    {
        RaftConfiguration cfg = new() { NodeId = 1, Host = "localhost", Port = 9500, InitialPartitions = 1 };
        _manager = new RaftManager(cfg, new StaticDiscovery([]), new InMemoryWAL(NullLogger<IRaft>.Instance),
            new InMemoryCommunication(), new HybridLogicalClock(), NullLogger<IRaft>.Instance);
    }

    public void Dispose() => _manager.Dispose();

    // ── recording inner transport ──────────────────────────────────────────────────

    private sealed class RecordingComm : ICommunication
    {
        private readonly object _l = new();
        public List<(NemesisVerb Verb, string Endpoint)> Calls { get; } = [];
        public List<byte[]> SnapshotBytes { get; } = [];
        public List<long> AppendLogsPrev { get; } = [];
        public SemaphoreSlim? Gate;
        private int _inFlight;
        public int MaxInFlight;

        private void Record(NemesisVerb v, string ep) { lock (_l) Calls.Add((v, ep)); }
        private int Count(NemesisVerb v) { lock (_l) return Calls.Count(c => c.Verb == v); }
        public int CountOf(NemesisVerb v) => Count(v);

        private async Task GateAsync()
        {
            int now = Interlocked.Increment(ref _inFlight);
            lock (_l) MaxInFlight = Math.Max(MaxInFlight, now);
            if (Gate is not null) await Gate.WaitAsync().ConfigureAwait(false);
            Interlocked.Decrement(ref _inFlight);
        }

        public async Task<AppendLogsResponse> AppendLogs(RaftManager m, RaftNode n, AppendLogsRequest r)
        {
            Record(NemesisVerb.AppendLogs, n.Endpoint);
            lock (_l) AppendLogsPrev.Add(r.PrevLogIndex);
            await GateAsync().ConfigureAwait(false);
            return new AppendLogsResponse();
        }

        public Task<HandshakeResponse> Handshake(RaftManager m, RaftNode n, HandshakeRequest r) { Record(NemesisVerb.Handshake, n.Endpoint); return Task.FromResult(new HandshakeResponse()); }
        public Task<RequestVotesResponse> RequestVotes(RaftManager m, RaftNode n, RequestVotesRequest r) { Record(NemesisVerb.RequestVotes, n.Endpoint); return Task.FromResult(new RequestVotesResponse()); }
        public Task<VoteResponse> Vote(RaftManager m, RaftNode n, VoteRequest r) { Record(NemesisVerb.Vote, n.Endpoint); return Task.FromResult(new VoteResponse()); }
        public Task<CompleteAppendLogsResponse> CompleteAppendLogs(RaftManager m, RaftNode n, CompleteAppendLogsRequest r) { Record(NemesisVerb.CompleteAppendLogs, n.Endpoint); return Task.FromResult(new CompleteAppendLogsResponse()); }
        public Task<BatchRequestsResponse> BatchRequests(RaftManager m, RaftNode n, BatchRequestsRequest r)
        {
            lock (_l) foreach (BatchRequestsRequestItem it in r.Requests ?? []) Calls.Add((VerbOf(it.Type), n.Endpoint));
            return Task.FromResult(new BatchRequestsResponse());
        }
        public Task<JoinResponse> SendJoin(RaftManager m, RaftNode n, JoinRequest r) { Record(NemesisVerb.Join, n.Endpoint); return Task.FromResult(new JoinResponse(true)); }
        public Task<LeaveResponse> SendLeave(RaftManager m, RaftNode n, LeaveRequest r, CancellationToken ct = default) { Record(NemesisVerb.Leave, n.Endpoint); return Task.FromResult(new LeaveResponse(true)); }
        public Task<GossipAck> SendGossip(RaftManager m, RaftNode n, GossipMessage d, CancellationToken ct = default) { Record(NemesisVerb.Gossip, n.Endpoint); return Task.FromResult(new GossipAck(1, null)); }
        public Task<Gossip.PingResponse> SendPing(RaftManager m, RaftNode n, Gossip.PingRequest r, CancellationToken ct = default) { Record(NemesisVerb.Ping, n.Endpoint); return Task.FromResult(new Gossip.PingResponse(true, 1)); }
        public Task<Gossip.PingReqResponse> SendPingReq(RaftManager m, RaftNode n, Gossip.PingReqRequest r, CancellationToken ct = default) { Record(NemesisVerb.PingReq, n.Endpoint); return Task.FromResult(new Gossip.PingReqResponse(true)); }
        public Task<long?> GetRemoteFollowerLag(RaftManager m, RaftNode n, int p, string fe) { Record(NemesisVerb.GetFollowerLag, n.Endpoint); return Task.FromResult<long?>(7); }
        public Task<SnapshotResponse> SendInstallSnapshot(RaftManager m, RaftNode n, SnapshotRequest r, CancellationToken ct = default)
        {
            lock (_l) { Calls.Add((NemesisVerb.InstallSnapshot, n.Endpoint)); SnapshotBytes.Add(r.Data.ToArray()); }
            return Task.FromResult(new SnapshotResponse(true));
        }
        public Task NotifyJoinBlocked(RaftManager m, string targetEndpoint, string reason, CancellationToken ct = default) { Record(NemesisVerb.NotifyJoinBlocked, targetEndpoint); return Task.CompletedTask; }

        private static NemesisVerb VerbOf(BatchRequestsRequestType t) => t switch
        {
            BatchRequestsRequestType.Handshake => NemesisVerb.Handshake,
            BatchRequestsRequestType.Vote => NemesisVerb.Vote,
            BatchRequestsRequestType.RequestVote => NemesisVerb.RequestVotes,
            BatchRequestsRequestType.AppendLogs => NemesisVerb.AppendLogs,
            BatchRequestsRequestType.CompleteAppendLogs => NemesisVerb.CompleteAppendLogs,
            BatchRequestsRequestType.StepDownNotice => NemesisVerb.StepDownNotice,
            BatchRequestsRequestType.TransferLeadership => NemesisVerb.TransferLeadership,
            BatchRequestsRequestType.TransferLeadershipSuggestion => NemesisVerb.TransferLeadershipSuggestion,
            _ => NemesisVerb.Ping,
        };
    }

    // ── invoking every method ────────────────────────────────────────────────────

    private async Task InvokeAll(NemesisCommunication nem)
    {
        await nem.Handshake(_manager, _node, new HandshakeRequest(1, 0, 0, Src));
        await nem.RequestVotes(_manager, _node, new RequestVotesRequest(0, 1, 0, 0, default, Src));
        await nem.Vote(_manager, _node, new VoteRequest(0, 1, 0, 0, default, Src));
        await nem.AppendLogs(_manager, _node, new AppendLogsRequest(0, 1, default, Src));
        await nem.CompleteAppendLogs(_manager, _node, new CompleteAppendLogsRequest(0, 1, default, Src, RaftOperationStatus.Success, 0));
        await nem.SendJoin(_manager, _node, new JoinRequest(Src, 1));
        await nem.SendLeave(_manager, _node, new LeaveRequest(Src, 1));
        await nem.SendGossip(_manager, _node, new GossipMessage(Src, 0, null));
        await nem.SendPing(_manager, _node, new Gossip.PingRequest(Src));
        await nem.SendPingReq(_manager, _node, new Gossip.PingReqRequest(Src, Dst));
        await nem.GetRemoteFollowerLag(_manager, _node, 0, Dst);
        await nem.SendInstallSnapshot(_manager, _node, new SnapshotRequest { SessionId = "s", PartitionId = 1, SnapshotIndex = 1, IsLast = true, Data = new byte[] { 1 } });
        await nem.NotifyJoinBlocked(_manager, Dst, "reason");
    }

    [Fact]
    public async Task AllInterfaceMethods_AreClassified()
    {
        RecordingComm inner = new();
        NemesisCommunication nem = new(inner);

        await InvokeAll(nem);

        NemesisVerb[] expected =
        [
            NemesisVerb.Handshake, NemesisVerb.RequestVotes, NemesisVerb.Vote, NemesisVerb.AppendLogs,
            NemesisVerb.CompleteAppendLogs, NemesisVerb.Join, NemesisVerb.Leave, NemesisVerb.Gossip,
            NemesisVerb.Ping, NemesisVerb.PingReq, NemesisVerb.GetFollowerLag, NemesisVerb.InstallSnapshot,
            NemesisVerb.NotifyJoinBlocked,
        ];

        IReadOnlyList<NemesisEvent> events = nem.AllEvents();
        foreach (NemesisVerb v in expected)
            Assert.Contains(events, e => e.Kind == NemesisEventKind.Decision && e.Envelope.Verb == v);
    }

    [Fact]
    public async Task AllBatchItemTypes_AreClassifiedIndependently()
    {
        RecordingComm inner = new();
        NemesisCommunication nem = new(inner);

        BatchRequestsRequest batch = new()
        {
            Requests =
            [
                new() { Type = BatchRequestsRequestType.Handshake, Handshake = new HandshakeRequest(1, 1, 0, Src) },
                new() { Type = BatchRequestsRequestType.Vote, Vote = new VoteRequest(1, 1, 0, 0, default, Src) },
                new() { Type = BatchRequestsRequestType.RequestVote, RequestVotes = new RequestVotesRequest(1, 1, 0, 0, default, Src) },
                new() { Type = BatchRequestsRequestType.AppendLogs, AppendLogs = new AppendLogsRequest(1, 1, default, Src) },
                new() { Type = BatchRequestsRequestType.CompleteAppendLogs, CompleteAppendLogs = new CompleteAppendLogsRequest(1, 1, default, Src, RaftOperationStatus.Success, 0) },
                new() { Type = BatchRequestsRequestType.StepDownNotice, StepDownNotice = new StepDownNoticeRequest(1, 1, default, Src) },
                new() { Type = BatchRequestsRequestType.TransferLeadership, TransferLeadership = new TransferLeadershipRequest(1, 1, default, Src, Dst) },
                new() { Type = BatchRequestsRequestType.TransferLeadershipSuggestion, TransferLeadershipSuggestion = new TransferLeadershipSuggestionRequest { Partition = 1 } },
            ],
        };

        await nem.BatchRequests(_manager, _node, batch);

        NemesisVerb[] expected =
        [
            NemesisVerb.Handshake, NemesisVerb.Vote, NemesisVerb.RequestVotes, NemesisVerb.AppendLogs,
            NemesisVerb.CompleteAppendLogs, NemesisVerb.StepDownNotice, NemesisVerb.TransferLeadership,
            NemesisVerb.TransferLeadershipSuggestion,
        ];
        IReadOnlyList<NemesisEvent> events = nem.AllEvents();
        foreach (NemesisVerb v in expected)
            Assert.Contains(events, e => e.Kind == NemesisEventKind.Decision && e.Envelope.Verb == v);
    }

    [Fact]
    public async Task WhollyDroppedBatch_StillNotifiesInvariantChecker()
    {
        RecordingComm inner = new();
        NemesisCommunication nem = new(inner);
        nem.Drop(Src, Dst); // every item dropped → no delivery events, but the checker must still be woken

        int notifications = 0;
        nem.OnEvent = () => Interlocked.Increment(ref notifications);

        await nem.BatchRequests(_manager, _node, new BatchRequestsRequest
        {
            Requests = [new() { Type = BatchRequestsRequestType.AppendLogs, AppendLogs = new AppendLogsRequest(1, 1, default, Src) }],
        });

        Assert.True(notifications > 0, "a wholly-dropped batch must notify the invariant checker of the transport state change");
    }

    [Fact]
    public async Task BatchItem_SatisfiesEventBarrier_AndDeliveryPreservesDecisionSequence()
    {
        RecordingComm inner = new();
        NemesisCommunication nem = new(inner);

        // Register a barrier on the AppendLogs batch item BEFORE the batch is sent.
        Task barrier = nem.WaitForEventAsync(e => e.Verb == NemesisVerb.AppendLogs, TestContext.Current.CancellationToken);

        await nem.BatchRequests(_manager, _node, new BatchRequestsRequest
        {
            Requests = [new() { Type = BatchRequestsRequestType.AppendLogs, AppendLogs = new AppendLogsRequest(1, 1, default, Src) }],
        });

        await barrier.WaitAsync(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken); // must complete — barriers see batch-item events

        IReadOnlyList<NemesisEvent> events = nem.AllEvents();
        NemesisEvent decision = events.First(e => e.Kind == NemesisEventKind.Decision && e.Envelope.Verb == NemesisVerb.AppendLogs);
        NemesisEvent delivery = events.First(e => e.Kind == NemesisEventKind.Delivery && e.Envelope.Verb == NemesisVerb.AppendLogs);
        Assert.NotEqual(0, delivery.Envelope.Sequence);                       // not the old hardcoded 0
        Assert.Equal(decision.Envelope.Sequence, delivery.Envelope.Sequence); // decision↔delivery correlate
    }

    [Fact]
    public async Task DelayedBatchDelivery_IsOwned_CanceledOnHeal_AndNeverDeliversAfter()
    {
        RecordingComm inner = new();
        NemesisCommunication nem = new(inner);
        // A long delay so the delivery is still pending when we heal.
        nem.Delay(Src, Dst, TimeSpan.FromSeconds(30), verb: NemesisVerb.AppendLogs);

        await nem.BatchRequests(_manager, _node, new BatchRequestsRequest
        {
            Requests = [new() { Type = BatchRequestsRequestType.AppendLogs, AppendLogs = new AppendLogsRequest(1, 1, default, Src) }],
        });

        Assert.Equal(1, nem.DelayedDeliveryCount);   // tracked, not fire-and-forget
        Assert.Empty(inner.Calls);                    // not delivered yet (still delayed)

        // Heal + drain: the delayed delivery is canceled and never reaches the inner transport.
        int canceled = await nem.CancelDelayedDeliveriesAsync();

        Assert.Equal(1, canceled);
        Assert.Equal(0, nem.DelayedDeliveryCount);
        Assert.Empty(inner.Calls);                    // canceled before delivery — nothing lands after heal
    }

    // ── drop / neutral response shapes ───────────────────────────────────────────

    [Fact]
    public async Task Drop_ReturnsNeutralResponses_AndDoesNotDeliver()
    {
        RecordingComm inner = new();
        NemesisCommunication nem = new(inner);
        nem.Drop(Src, Dst); // drop everything on this link

        Assert.False((await nem.SendJoin(_manager, _node, new JoinRequest(Src, 1))).Success);
        Assert.False((await nem.SendInstallSnapshot(_manager, _node, new SnapshotRequest { SessionId = "s", PartitionId = 1, SnapshotIndex = 1, IsLast = true, Data = new byte[] { 1 } }, TestContext.Current.CancellationToken)).Success);
        Assert.Null(await nem.GetRemoteFollowerLag(_manager, _node, 0, Dst));

        Assert.Empty(inner.Calls);  // nothing reached the inner transport
        Assert.Contains(nem.AllEvents(), e => e.Kind == NemesisEventKind.Drop);
    }

    // ── partitions ───────────────────────────────────────────────────────────────

    [Fact]
    public async Task AsymmetricPartition_DropsOneDirectionOnly()
    {
        RecordingComm inner = new();
        NemesisCommunication nem = new(inner);
        nem.Partition(Src, Dst, "cut"); // only Src->Dst

        await nem.AppendLogs(_manager, _node, new AppendLogsRequest(0, 1, default, Src)); // dropped
        Assert.Equal(0, inner.CountOf(NemesisVerb.AppendLogs));

        // Dst->Src is not cut: a message whose source is Dst passes. (Simulate by classifying.)
        Assert.Equal(FaultAction.Pass, nem.ClassifyForTesting(Dst, Src, NemesisVerb.AppendLogs));
        Assert.Equal(FaultAction.Drop, nem.ClassifyForTesting(Src, Dst, NemesisVerb.AppendLogs));

        nem.Heal("cut");
        Assert.Equal(FaultAction.Pass, nem.ClassifyForTesting(Src, Dst, NemesisVerb.AppendLogs));
    }

    [Fact]
    public void SymmetricPartition_DropsBothDirections_HealRemovesBoth()
    {
        NemesisCommunication nem = new(new RecordingComm());
        nem.PartitionSymmetric(Src, Dst, "split");

        Assert.Equal(FaultAction.Drop, nem.ClassifyForTesting(Src, Dst, NemesisVerb.Vote));
        Assert.Equal(FaultAction.Drop, nem.ClassifyForTesting(Dst, Src, NemesisVerb.Vote));

        Assert.Equal(2, nem.Heal("split"));
        Assert.Equal(FaultAction.Pass, nem.ClassifyForTesting(Src, Dst, NemesisVerb.Vote));
        Assert.Equal(FaultAction.Pass, nem.ClassifyForTesting(Dst, Src, NemesisVerb.Vote));
    }

    // ── hold / release order ──────────────────────────────────────────────────────

    [Fact]
    public async Task HeldMessages_ReleaseInRequestedOrder()
    {
        RecordingComm inner = new();
        NemesisCommunication nem = new(inner);
        nem.Hold(Src, Dst, NemesisVerb.AppendLogs);

        // Three holds, distinguished by PrevLogIndex, fired in order (each returns a pending task).
        Task t1 = nem.AppendLogs(_manager, _node, new AppendLogsRequest(0, 1, default, Src, prevLogIndex: 1));
        Task t2 = nem.AppendLogs(_manager, _node, new AppendLogsRequest(0, 1, default, Src, prevLogIndex: 2));
        Task t3 = nem.AppendLogs(_manager, _node, new AppendLogsRequest(0, 1, default, Src, prevLogIndex: 3));

        Assert.Equal(3, nem.HeldCount);
        Assert.Empty(inner.AppendLogsPrev);
        Assert.All(new[] { t1, t2, t3 }, t => Assert.False(t.IsCompleted));

        int released = await nem.ReleaseHeldAsync(verb: NemesisVerb.AppendLogs);
        await Task.WhenAll(t1, t2, t3);

        Assert.Equal(3, released);
        Assert.Equal([1L, 2L, 3L], inner.AppendLogsPrev);
        Assert.Equal(0, nem.HeldCount);
    }

    // ── cancellation ──────────────────────────────────────────────────────────────

    [Fact]
    public async Task Cancellation_RemovesHeldWaiter_AndCancelsCall()
    {
        RecordingComm inner = new();
        NemesisCommunication nem = new(inner);
        nem.Hold(Src, Dst, NemesisVerb.Leave);

        using CancellationTokenSource cts = new();
        Task<LeaveResponse> held = nem.SendLeave(_manager, _node, new LeaveRequest(Src, 1), cts.Token);
        Assert.Equal(1, nem.HeldCount);

        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => held);
        Assert.Equal(0, nem.HeldCount);
        Assert.Empty(inner.Calls);
        Assert.Contains(nem.AllEvents(), e => e.Kind == NemesisEventKind.Cancellation);
    }

    [Fact]
    public async Task Cancellation_DuringDelay_CancelsWithoutDelivering()
    {
        RecordingComm inner = new();
        NemesisCommunication nem = new(inner);
        nem.Delay(Src, Dst, TimeSpan.FromSeconds(30), NemesisVerb.Leave);

        using CancellationTokenSource cts = new();
        Task<LeaveResponse> delayed = nem.SendLeave(_manager, _node, new LeaveRequest(Src, 1), cts.Token);
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => delayed);
        Assert.Empty(inner.Calls);
        Assert.Contains(nem.AllEvents(), e => e.Kind == NemesisEventKind.Cancellation);
    }

    // ── deep copy of buffers ──────────────────────────────────────────────────────

    [Fact]
    public async Task HeldSnapshot_RetainsOriginalBytes_AfterSenderMutatesBuffer()
    {
        RecordingComm inner = new();
        NemesisCommunication nem = new(inner);
        nem.Hold(Src, Dst, NemesisVerb.InstallSnapshot);

        byte[] pooled = [1, 2, 3, 4]; // stands in for the sender's reused pooled buffer
        SnapshotRequest req = new() { SessionId = "s", PartitionId = 1, SnapshotIndex = 1, IsLast = true, Data = pooled };

        Task<SnapshotResponse> held = nem.SendInstallSnapshot(_manager, _node, req, TestContext.Current.CancellationToken);
        Assert.Equal(1, nem.HeldCount);

        // Sender reuses the pooled buffer while the message is held.
        pooled[0] = 99; pooled[1] = 88;

        await nem.ReleaseHeldAsync(verb: NemesisVerb.InstallSnapshot);
        await held;

        // The deferred delivery carried the DEEP COPY taken at intercept time, not the mutated buffer.
        Assert.Single(inner.SnapshotBytes);
        Assert.Equal([1, 2, 3, 4], inner.SnapshotBytes[0]);
    }

    [Fact]
    public async Task Duplicate_DeliversTwice_WithCorrectBytes()
    {
        RecordingComm inner = new();
        NemesisCommunication nem = new(inner);
        nem.Duplicate(Src, Dst, NemesisVerb.InstallSnapshot);

        SnapshotRequest req = new() { SessionId = "s", PartitionId = 1, SnapshotIndex = 1, IsLast = true, Data = new byte[] { 5, 6, 7 } };
        await nem.SendInstallSnapshot(_manager, _node, req, TestContext.Current.CancellationToken);

        Assert.Equal(2, inner.SnapshotBytes.Count);
        Assert.Equal([5, 6, 7], inner.SnapshotBytes[0]);
        Assert.Equal([5, 6, 7], inner.SnapshotBytes[1]);
    }

    // ── determinism ────────────────────────────────────────────────────────────────

    [Fact]
    public void FixedEnvelopeList_PlusSeed_YieldsSameDecisions()
    {
        NemesisRandomProfile profile = new() { Drop = 0.3, Fail = 0.1, Delay = 0.1, Duplicate = 0.1 };

        NemesisCommunication a = new(new RecordingComm(), seed: 12345);
        NemesisCommunication b = new(new RecordingComm(), seed: 12345);
        a.SetRandomProfile(profile);
        b.SetRandomProfile(profile);

        (string, string, NemesisVerb)[] envelopes =
        [
            (Src, Dst, NemesisVerb.AppendLogs), (Dst, Src, NemesisVerb.Vote), (Src, Dst, NemesisVerb.InstallSnapshot),
            (Src, Dst, NemesisVerb.Ping), (Dst, Src, NemesisVerb.AppendLogs), (Src, Dst, NemesisVerb.Gossip),
            (Src, Dst, NemesisVerb.CompleteAppendLogs), (Dst, Src, NemesisVerb.Handshake),
        ];

        // The random profile targets user-partition traffic only (system partition 0 and null-partition control/
        // gossip are protected), so classify against a user partition here.
        List<FaultAction> da = envelopes.Select(e => a.ClassifyForTesting(e.Item1, e.Item2, e.Item3, partition: 1)).ToList();
        List<FaultAction> db = envelopes.Select(e => b.ClassifyForTesting(e.Item1, e.Item2, e.Item3, partition: 1)).ToList();

        Assert.Equal(da, db);
        Assert.Contains(da, x => x != FaultAction.Pass); // profile actually injected something
    }

    [Fact]
    public void RandomProfile_TargetsUserPartitionsOnly_ProtectsSystemAndControl()
    {
        NemesisRandomProfile profile = new() { Drop = 1.0 }; // would drop everything it applies to
        NemesisCommunication n = new(new RecordingComm(), seed: 1);
        n.SetRandomProfile(profile);

        // System partition (0) and null-partition control/gossip are protected from the random profile.
        Assert.Equal(FaultAction.Pass, n.ClassifyForTesting(Src, Dst, NemesisVerb.AppendLogs, partition: 0));
        Assert.Equal(FaultAction.Pass, n.ClassifyForTesting(Src, Dst, NemesisVerb.Ping, partition: null));
        // A user partition is subject to the profile.
        Assert.Equal(FaultAction.Drop, n.ClassifyForTesting(Src, Dst, NemesisVerb.AppendLogs, partition: 1));
    }

    // ── concurrency ────────────────────────────────────────────────────────────────

    [Fact]
    public async Task ConcurrentCalls_AllocateUniqueSequences()
    {
        RecordingComm inner = new();
        NemesisCommunication nem = new(inner);

        const int n = 200;
        await Task.WhenAll(Enumerable.Range(0, n).Select(_ =>
            Task.Run(() => nem.AppendLogs(_manager, _node, new AppendLogsRequest(0, 1, default, Src)))));

        long[] seqs = nem.AllEvents()
            .Where(e => e.Kind == NemesisEventKind.Decision)
            .Select(e => e.Envelope.Sequence)
            .ToArray();

        Assert.Equal(n, seqs.Length);
        Assert.Equal(n, seqs.Distinct().Count());          // all unique
        Assert.Equal(Enumerable.Range(1, n).Select(i => (long)i).OrderBy(x => x), seqs.OrderBy(x => x));
    }

    [Fact]
    public async Task NoLockHeld_WhileBlockingInnerTransportRuns()
    {
        RecordingComm inner = new() { Gate = new SemaphoreSlim(0) };
        NemesisCommunication nem = new(inner);

        // Two concurrent AppendLogs, both Pass → both must reach the (blocking) inner transport at once.
        Task a = nem.AppendLogs(_manager, _node, new AppendLogsRequest(0, 1, default, Src));
        Task b = nem.AppendLogs(_manager, _node, new AppendLogsRequest(0, 1, default, Src));

        ValueStopwatchWait(() => inner.MaxInFlight >= 2, 5000);
        Assert.True(inner.MaxInFlight >= 2, $"inner in-flight peaked at {inner.MaxInFlight}; the nemesis lock must not be held during delivery");

        inner.Gate!.Release(2);
        await Task.WhenAll(a, b);
    }

    // ── fail / delay / occurrence ────────────────────────────────────────────────────

    [Fact]
    public async Task Fail_ThrowsConfiguredException()
    {
        RecordingComm inner = new();
        NemesisCommunication nem = new(inner);
        nem.Fail(Src, Dst, () => new NemesisTransportException("boom"), NemesisVerb.Vote);

        NemesisTransportException ex = await Assert.ThrowsAsync<NemesisTransportException>(
            () => nem.Vote(_manager, _node, new VoteRequest(0, 1, 0, 0, default, Src)));
        Assert.Equal("boom", ex.Message);
        Assert.Empty(inner.Calls);
    }

    [Fact]
    public async Task Occurrence_AppliesOnlyToNthMatch()
    {
        RecordingComm inner = new();
        NemesisCommunication nem = new(inner);
        nem.Hold(Src, Dst, NemesisVerb.AppendLogs, occurrence: 2); // hold only the 2nd

        Task first = nem.AppendLogs(_manager, _node, new AppendLogsRequest(0, 1, default, Src, prevLogIndex: 1));
        await first; // 1st passes immediately
        Assert.Equal(0, nem.HeldCount);
        Assert.Equal([1L], inner.AppendLogsPrev);

        Task second = nem.AppendLogs(_manager, _node, new AppendLogsRequest(0, 1, default, Src, prevLogIndex: 2));
        Assert.Equal(1, nem.HeldCount); // 2nd is held
        Assert.False(second.IsCompleted);

        await nem.ReleaseHeldAsync(verb: NemesisVerb.AppendLogs);
        await second;
        Assert.Equal([1L, 2L], inner.AppendLogsPrev);
    }

    private static void ValueStopwatchWait(Func<bool> cond, int timeoutMs)
    {
        ValueStopwatch sw = ValueStopwatch.StartNew();
        while (!cond() && sw.GetElapsedMilliseconds() < timeoutMs)
            Thread.Sleep(10);
    }
}
