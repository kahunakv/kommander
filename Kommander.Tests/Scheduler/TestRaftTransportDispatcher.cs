using System.Collections.Concurrent;
using Kommander.Communication;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.Scheduler;

/// <summary>
/// Unit tests for <see cref="RaftTransportDispatcher"/> (Task 12).
///
/// Covers:
/// - Single-message path: one message dispatched via the per-type ICommunication method.
/// - Batching: messages that accumulate while a prior send is in-flight are grouped.
/// - FIFO: per-endpoint delivery order matches submission order.
/// - Shutdown drain: messages buffered before Stop() are fully delivered.
/// - Multi-endpoint isolation: messages to different endpoints use separate workers.
/// - Payload conversion: each of the 5 request types routes through the correct method.
/// - Enqueue after Stop: enqueuing after Stop() is a no-op.
/// </summary>
public sealed class TestRaftTransportDispatcher
{
    // ── Capturing ICommunication stub ─────────────────────────────────────────

    private sealed class CapturingCommunication : ICommunication
    {
        public sealed class Call
        {
            public string Kind { get; init; } = "";
            public string DestinationEndpoint { get; init; } = "";
            public object Payload { get; init; } = null!;
        }

        private readonly ConcurrentQueue<Call> _calls = new();

        // When set, the NEXT call blocks inside AwaitGate before returning.
        private volatile TaskCompletionSource<bool>? _gate;

        // Signalled as soon as a call enters the gate (i.e. the worker is blocked).
        private volatile TaskCompletionSource<bool>? _gateEntered;

        public IEnumerable<Call> Calls => _calls;

        /// <summary>
        /// Causes the very next ICommunication call to block until
        /// <paramref name="gate"/> is set. If <paramref name="entered"/> is
        /// supplied it is signalled once the call has entered the gate, allowing
        /// tests to synchronise deterministically.
        /// </summary>
        public void SetGate(TaskCompletionSource<bool> gate, TaskCompletionSource<bool>? entered = null)
        {
            _gateEntered = entered;
            _gate = gate; // set last so the gate is visible before entered
        }

        private async Task AwaitGate()
        {
            if (_gate is not { } gate) return;
            _gate = null;
            _gateEntered?.TrySetResult(true);
            _gateEntered = null;
            await gate.Task.ConfigureAwait(false);
        }

        private void Record(string kind, string endpoint, object payload) =>
            _calls.Enqueue(new() { Kind = kind, DestinationEndpoint = endpoint, Payload = payload });

        public async Task<HandshakeResponse> Handshake(RaftManager manager, RaftNode node, HandshakeRequest request)
        {
            await AwaitGate().ConfigureAwait(false);
            Record("Handshake", node.Endpoint, request);
            return new();
        }

        public async Task<RequestVotesResponse> RequestVotes(RaftManager manager, RaftNode node, RequestVotesRequest request)
        {
            await AwaitGate().ConfigureAwait(false);
            Record("RequestVotes", node.Endpoint, request);
            return new();
        }

        public async Task<VoteResponse> Vote(RaftManager manager, RaftNode node, VoteRequest request)
        {
            await AwaitGate().ConfigureAwait(false);
            Record("Vote", node.Endpoint, request);
            return new();
        }

        public async Task<AppendLogsResponse> AppendLogs(RaftManager manager, RaftNode node, AppendLogsRequest request)
        {
            await AwaitGate().ConfigureAwait(false);
            Record("AppendLogs", node.Endpoint, request);
            return new();
        }

        public async Task<CompleteAppendLogsResponse> CompleteAppendLogs(RaftManager manager, RaftNode node, CompleteAppendLogsRequest request)
        {
            await AwaitGate().ConfigureAwait(false);
            Record("CompleteAppendLogs", node.Endpoint, request);
            return new();
        }

        public async Task<BatchRequestsResponse> BatchRequests(RaftManager manager, RaftNode node, BatchRequestsRequest request)
        {
            await AwaitGate().ConfigureAwait(false);
            // The dispatcher returns the pooled Requests list to GrpcCommunicationPool after this
            // method returns, which clears the list.  Snapshot it now so test assertions can
            // inspect the items after Dispose() completes.
            BatchRequestsRequest snapshot = new()
            {
                Requests = request.Requests?.Select(r => new BatchRequestsRequestItem
                {
                    Type = r.Type,
                    Handshake = r.Handshake,
                    Vote = r.Vote,
                    RequestVotes = r.RequestVotes,
                    AppendLogs = r.AppendLogs,
                    CompleteAppendLogs = r.CompleteAppendLogs,
                }).ToList()
            };
            Record("BatchRequests", node.Endpoint, snapshot);
            return new();
        }
    }

    // ── Test helpers ──────────────────────────────────────────────────────────

    /// <summary>
    /// Builds a minimal <see cref="RaftManager"/> that is wired to an *isolated*
    /// <see cref="CapturingCommunication"/> instance so that any timer-driven
    /// background sends (heartbeats, etc.) never land in the test's capturing
    /// <paramref name="testComm"/> instance.
    /// </summary>
    private static (RaftManager manager, RaftTransportDispatcher dispatcher)
        Build(CapturingCommunication testComm)
    {
        RaftConfiguration config = new()
        {
            Host = "localhost",
            Port = 9000,
            // Zero partitions: no executor threads or WAL restores start up.
            InitialPartitions = 0,
        };

        RaftManager manager = new(
            config,
            new StaticDiscovery([]),
            new InMemoryWAL(NullLogger<IRaft>.Instance),
            new CapturingCommunication(), // isolated — manager traffic stays here
            new HybridLogicalClock(),
            NullLogger<IRaft>.Instance
        );

        RaftTransportDispatcher dispatcher = new(manager, testComm, NullLogger<IRaft>.Instance);
        return (manager, dispatcher);
    }

    // ── Request factories ─────────────────────────────────────────────────────

    private static RaftNode Node(string ep) => new(ep);

    private static RaftResponderRequest MakeVote(string ep, long term = 0) =>
        new(RaftResponderRequestType.Vote,
            Node(ep),
            new VoteRequest(0, term, 0, HLCTimestamp.Zero, ep));

    private static RaftResponderRequest MakeAppendLogs(string ep, long term) =>
        new(RaftResponderRequestType.AppendLogs,
            Node(ep),
            new AppendLogsRequest(0, term, HLCTimestamp.Zero, ep));

    private static RaftResponderRequest MakeHandshake(string ep) =>
        new(RaftResponderRequestType.Handshake,
            Node(ep),
            new HandshakeRequest(0, 0, 0, ep));

    private static RaftResponderRequest MakeRequestVotes(string ep) =>
        new(RaftResponderRequestType.RequestVotes,
            Node(ep),
            new RequestVotesRequest(0, 0, 0, HLCTimestamp.Zero, ep));

    private static RaftResponderRequest MakeCompleteAppendLogs(string ep) =>
        new(RaftResponderRequestType.CompleteAppendLogs,
            Node(ep),
            new CompleteAppendLogsRequest(0, 0, HLCTimestamp.Zero, ep, RaftOperationStatus.Errored, 0));

    // ── Utility: total message count across mixed single + batch calls ─────────

    private static int CountMessages(IEnumerable<CapturingCommunication.Call> calls) =>
        calls.Sum(c =>
            c.Kind == "BatchRequests"
                ? ((BatchRequestsRequest)c.Payload).Requests!.Count
                : 1);

    // ── Tests ─────────────────────────────────────────────────────────────────

    [Fact]
    public void Enqueue_AfterStop_IsNoOp()
    {
        CapturingCommunication comm = new();
        var (manager, dispatcher) = Build(comm);
        using (manager)
        {
            dispatcher.Stop();
            dispatcher.Enqueue("node-a:8001", MakeVote("node-a:8001"));
            dispatcher.Dispose();

            Assert.Empty(comm.Calls);
        }
    }

    [Fact]
    public void SingleMessage_DispatchesViaSingleTypeMethod_NotBatch()
    {
        CapturingCommunication comm = new();
        var (manager, dispatcher) = Build(comm);
        using (manager)
        {
            dispatcher.Enqueue("node-a:8001", MakeVote("node-a:8001"));
            dispatcher.Dispose();

            CapturingCommunication.Call call = Assert.Single(comm.Calls);
            Assert.Equal("Vote", call.Kind);
            Assert.Equal("node-a:8001", call.DestinationEndpoint);
        }
    }

    [Fact]
    public async Task Messages_ThatAccumulateDuringSend_AreBatchedTogether()
    {
        CapturingCommunication comm = new();
        var (manager, dispatcher) = Build(comm);
        using (manager)
        {
            // Gate the first send so additional messages accumulate in the channel.
            TaskCompletionSource<bool> gate = new(TaskCreationOptions.RunContinuationsAsynchronously);
            TaskCompletionSource<bool> entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
            comm.SetGate(gate, entered);

            // The worker reads message 1 and blocks on the gate.
            dispatcher.Enqueue("node-a:8001", MakeVote("node-a:8001"));

            // Wait until the worker is confirmed blocked — deterministic synchronisation.
            await entered.Task.WaitAsync(TimeSpan.FromSeconds(5));

            // Enqueue more messages while the worker is blocked: they accumulate.
            dispatcher.Enqueue("node-a:8001", MakeVote("node-a:8001"));
            dispatcher.Enqueue("node-a:8001", MakeVote("node-a:8001"));
            dispatcher.Enqueue("node-a:8001", MakeVote("node-a:8001"));

            // Unblock the first send.
            gate.TrySetResult(true);
            dispatcher.Dispose();

            List<CapturingCommunication.Call> calls = comm.Calls.ToList();

            // First call: single send (direct path, 1 message).
            Assert.Equal("Vote", calls[0].Kind);

            // Second call: BatchRequests for the 3 accumulated messages.
            Assert.Equal(2, calls.Count);
            Assert.Equal("BatchRequests", calls[1].Kind);
            BatchRequestsRequest batch = (BatchRequestsRequest)calls[1].Payload;
            Assert.Equal(3, batch.Requests!.Count);
        }
    }

    [Fact]
    public void FifoOrdering_PerEndpoint_IsPreserved()
    {
        CapturingCommunication comm = new();
        var (manager, dispatcher) = Build(comm);
        using (manager)
        {
            // Embed ascending sequence numbers in Term so delivery order is observable.
            for (long seq = 1; seq <= 5; seq++)
                dispatcher.Enqueue("node-a:8001", MakeAppendLogs("node-a:8001", seq));

            dispatcher.Dispose();

            // Extract Term values from however the messages were delivered
            // (singles, batches, or a mix) and assert the original submission order.
            List<long> terms = [];
            foreach (CapturingCommunication.Call call in comm.Calls)
            {
                if (call.Kind == "AppendLogs")
                    terms.Add(((AppendLogsRequest)call.Payload).Term);
                else if (call.Kind == "BatchRequests")
                    foreach (BatchRequestsRequestItem item in ((BatchRequestsRequest)call.Payload).Requests!)
                        if (item.AppendLogs is { } r) terms.Add(r.Term);
            }

            Assert.Equal(new long[] { 1, 2, 3, 4, 5 }, terms);
        }
    }

    [Fact]
    public void ShutdownDrain_DeliversAllBufferedMessages()
    {
        CapturingCommunication comm = new();
        var (manager, dispatcher) = Build(comm);
        using (manager)
        {
            const int Count = 20;
            for (int i = 0; i < Count; i++)
                dispatcher.Enqueue("node-a:8001", MakeVote("node-a:8001"));

            // Stop() completes the channel writer; Dispose() waits for the worker to drain.
            dispatcher.Stop();
            dispatcher.Dispose();

            Assert.Equal(Count, CountMessages(comm.Calls));
        }
    }

    [Fact]
    public void MultipleEndpoints_UseIsolatedWorkers_MessagesDoNotCross()
    {
        CapturingCommunication comm = new();
        var (manager, dispatcher) = Build(comm);
        using (manager)
        {
            const string A = "node-a:8001";
            const string B = "node-b:8002";

            dispatcher.Enqueue(A, MakeVote(A));
            dispatcher.Enqueue(B, MakeVote(B));
            dispatcher.Enqueue(A, MakeVote(A));
            dispatcher.Enqueue(B, MakeVote(B));

            dispatcher.Dispose();

            // Expand all calls to a flat list of (endpoint, message) pairs.
            List<string> endpoints = [];
            foreach (CapturingCommunication.Call call in comm.Calls)
            {
                if (call.Kind == "BatchRequests")
                    for (int i = 0; i < ((BatchRequestsRequest)call.Payload).Requests!.Count; i++)
                        endpoints.Add(call.DestinationEndpoint);
                else
                    endpoints.Add(call.DestinationEndpoint);
            }

            // Each send goes to exactly one endpoint (no cross-contamination).
            Assert.All(comm.Calls, c => Assert.NotEqual("", c.DestinationEndpoint));

            // Both endpoints received messages.
            Assert.Contains(A, endpoints);
            Assert.Contains(B, endpoints);

            // Every call that targets A never has B's endpoint, and vice versa.
            Assert.All(comm.Calls.Where(c => c.DestinationEndpoint == A),
                c => Assert.Equal(A, c.DestinationEndpoint));
            Assert.All(comm.Calls.Where(c => c.DestinationEndpoint == B),
                c => Assert.Equal(B, c.DestinationEndpoint));

            // Total delivered messages = 4 (2 per endpoint).
            Assert.Equal(4, CountMessages(comm.Calls));
        }
    }

    [Theory]
    [InlineData("Handshake")]
    [InlineData("Vote")]
    [InlineData("RequestVotes")]
    [InlineData("AppendLogs")]
    [InlineData("CompleteAppendLogs")]
    public void PayloadConversion_EachTypeRoutesToCorrectCommunicationMethod(string typeName)
    {
        CapturingCommunication comm = new();
        var (manager, dispatcher) = Build(comm);
        using (manager)
        {
            const string Ep = "node-a:8001";

            RaftResponderRequest req = typeName switch
            {
                "Handshake"          => MakeHandshake(Ep),
                "Vote"               => MakeVote(Ep),
                "RequestVotes"       => MakeRequestVotes(Ep),
                "AppendLogs"         => MakeAppendLogs(Ep, 1),
                "CompleteAppendLogs" => MakeCompleteAppendLogs(Ep),
                _                    => throw new InvalidOperationException(),
            };

            dispatcher.Enqueue(Ep, req);
            dispatcher.Dispose();

            // One direct-method call with the expected method name and endpoint.
            CapturingCommunication.Call call = Assert.Single(comm.Calls);
            Assert.Equal(typeName, call.Kind);
            Assert.Equal(Ep, call.DestinationEndpoint);
        }
    }
}
