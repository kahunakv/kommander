
using System.Diagnostics.CodeAnalysis;
using Kommander.Communication;
using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;

namespace Kommander.Tests;

/// <summary>
/// Regression tests for single-node election when two phantom witnesses are present.
///
/// Context
/// -------
/// Kahuna's <c>EmbeddedKahunaNode</c> uses an <c>EmbeddedRaftCommunication</c> that lists two
/// static witnesses so that <c>host.Nodes.Count == 2</c>. The zero-peer fast-path inside
/// <see cref="RaftPartitionStateMachine"/> (<c>if (host.Nodes.Count == 0)</c>) is therefore
/// bypassed and a full pre-vote round is launched.
///
/// Bug (Kahuna 0.3.5 / Kommander 0.11.2)
/// --------------------------------------
/// <c>EmbeddedRaftCommunication.RequestVotes</c> responds to pre-vote probes by calling
/// <c>manager.Vote(new(..., /* preVote not passed */ ))</c>, which routes the reply into the
/// <em>real-election</em> tally path (<c>ReceivedVoteAsync(preVote: false)</c>). Because the
/// requesting node is still a Follower (not yet a Candidate), that tally is silently ignored.
/// Pre-vote quorum is never reached; <c>JoinCluster</c> times out after 60 s.
///
/// Fix
/// ---
/// Pass <c>preVote: request.PreVote</c> when constructing the <see cref="VoteRequest"/> inside
/// the <c>RequestVotes</c> handler so that <c>ReceivedVoteAsync(preVote: true)</c> is invoked
/// and the pre-vote grant is correctly tallied.
/// </summary>
[SuppressMessage("Performance", "CA1859:Use concrete types when possible for improved performance")]
[Collection(ClusterIntegrationCollection.Name)]
public sealed class TestPhantomPeerPreVote
{
    // Two phantom witnesses: present in StaticDiscovery so Nodes.Count == 2,
    // but not backed by real RaftManager instances.
    private static readonly List<RaftNode> PhantomWitnesses =
    [
        new("phantom-witness-1:0"),
        new("phantom-witness-2:0"),
    ];

    private readonly ILogger<IRaft> logger;

    public TestPhantomPeerPreVote()
    {
        ILoggerFactory lf = LoggerFactory.Create(b => b.SetMinimumLevel(LogLevel.Warning));
        logger = lf.CreateLogger<IRaft>();
    }

    // -----------------------------------------------------------------------
    // Communication stubs
    // -----------------------------------------------------------------------

    /// <summary>
    /// Mirrors the bug in Kahuna's <c>EmbeddedRaftCommunication</c> before the fix:
    /// <see cref="RequestVotesRequest.PreVote"/> is silently dropped so pre-vote grants
    /// are routed to the real-vote tally and ignored.
    /// </summary>
    private sealed class BrokenWitnessCommunication : ICommunication
    {
        private static readonly Task<HandshakeResponse> s_handshake = Task.FromResult(new HandshakeResponse());
        private static readonly Task<RequestVotesResponse> s_requestVotes = Task.FromResult(new RequestVotesResponse());
        private static readonly Task<VoteResponse> s_vote = Task.FromResult(new VoteResponse());
        private static readonly Task<AppendLogsResponse> s_appendLogs = Task.FromResult(new AppendLogsResponse());
        private static readonly Task<CompleteAppendLogsResponse> s_completeAppendLogs = Task.FromResult(new CompleteAppendLogsResponse());

        public Task<HandshakeResponse> Handshake(RaftManager manager, RaftNode node, HandshakeRequest request) => s_handshake;

        public Task<RequestVotesResponse> RequestVotes(RaftManager manager, RaftNode node, RequestVotesRequest request)
        {
            // BUG: request.PreVote is not forwarded → ReceivedVoteAsync(preVote: false) is called
            // → tally is ignored because the requesting node is a Follower, not a Candidate
            // → pre-vote quorum is never reached → election never starts → JoinCluster times out.
            manager.Vote(new(request.Partition, request.Term, request.MaxLogId, request.Time, node.Endpoint));
            return s_requestVotes;
        }

        public Task<VoteResponse> Vote(RaftManager manager, RaftNode node, VoteRequest request) => s_vote;
        public Task<AppendLogsResponse> AppendLogs(RaftManager manager, RaftNode node, AppendLogsRequest request) => s_appendLogs;
        public Task<CompleteAppendLogsResponse> CompleteAppendLogs(RaftManager manager, RaftNode node, CompleteAppendLogsRequest request) => s_completeAppendLogs;
        public Task<BatchRequestsResponse> BatchRequests(RaftManager manager, RaftNode node, BatchRequestsRequest request) => Task.FromResult(new BatchRequestsResponse());
        public Task<JoinResponse> SendJoin(RaftManager manager, RaftNode node, JoinRequest request) => Task.FromResult(new JoinResponse(false));
    }

    /// <summary>
    /// The correct implementation, mirroring what Kahuna's <c>EmbeddedRaftCommunication</c> should
    /// do after the fix. Two behaviours are required:
    /// <list type="bullet">
    ///   <item><see cref="RequestVotes"/> propagates <see cref="RequestVotesRequest.PreVote"/> so
    ///   pre-vote grants reach <c>ReceivedVoteAsync(preVote: true)</c> and are tallied.</item>
    ///   <item><see cref="AppendLogs"/> calls <c>manager.CompleteAppendLogs</c> to ACK log entries
    ///   so that the leader can reach commit quorum for the initial partition-map proposal.</item>
    /// </list>
    /// </summary>
    private sealed class FixedWitnessCommunication : ICommunication
    {
        private static readonly Task<HandshakeResponse> s_handshake = Task.FromResult(new HandshakeResponse());
        private static readonly Task<RequestVotesResponse> s_requestVotes = Task.FromResult(new RequestVotesResponse());
        private static readonly Task<VoteResponse> s_vote = Task.FromResult(new VoteResponse());
        private static readonly Task<AppendLogsResponse> s_appendLogs = Task.FromResult(new AppendLogsResponse());
        private static readonly Task<CompleteAppendLogsResponse> s_completeAppendLogs = Task.FromResult(new CompleteAppendLogsResponse());

        public Task<HandshakeResponse> Handshake(RaftManager manager, RaftNode node, HandshakeRequest request) => s_handshake;

        public Task<RequestVotesResponse> RequestVotes(RaftManager manager, RaftNode node, RequestVotesRequest request)
        {
            // FIX: propagate request.PreVote → ReceivedVoteAsync(preVote: true) tallies the grant
            // → pre-vote quorum is reached → real election starts → node becomes leader.
            manager.Vote(new(request.Partition, request.Term, request.MaxLogId, request.Time, node.Endpoint, preVote: request.PreVote));
            return s_requestVotes;
        }

        public Task<VoteResponse> Vote(RaftManager manager, RaftNode node, VoteRequest request) => s_vote;

        public Task<AppendLogsResponse> AppendLogs(RaftManager manager, RaftNode node, AppendLogsRequest request)
        {
            // Simulate peer ACK so the leader can advance its commit index and replicate the
            // initial partition-map proposal. Without this CompleteAppendLogs call the proposal
            // never commits and IsInitialized is never set.
            long commitIndex = request.Logs is null || request.Logs.Count == 0
                ? 0
                : request.Logs.Max(log => log.Id);

            manager.CompleteAppendLogs(new(
                request.Partition,
                request.Term,
                request.Time,
                node.Endpoint,
                RaftOperationStatus.Success,
                commitIndex));

            return s_appendLogs;
        }

        public Task<CompleteAppendLogsResponse> CompleteAppendLogs(RaftManager manager, RaftNode node, CompleteAppendLogsRequest request) => s_completeAppendLogs;
        public Task<BatchRequestsResponse> BatchRequests(RaftManager manager, RaftNode node, BatchRequestsRequest request) => Task.FromResult(new BatchRequestsResponse());
        public Task<JoinResponse> SendJoin(RaftManager manager, RaftNode node, JoinRequest request) => Task.FromResult(new JoinResponse(false));
    }

    // -----------------------------------------------------------------------
    // Tests
    // -----------------------------------------------------------------------

    /// <summary>
    /// Reproduces the bug: two phantom witnesses bypass the zero-peer fast-path and the broken
    /// communication drops the PreVote flag, so <c>JoinCluster</c> times out waiting for the
    /// system partition to elect a leader.
    /// </summary>
    [Theory]
    [InlineData(1)]
    [InlineData(3)]
    public async Task SingleNode_BrokenPhantomPeers_PreVoteNeverGranted_JoinClusterTimesOut(int partitions)
    {
        IRaft node = BuildNode(partitions, new BrokenWitnessCommunication());

        using CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(8));

        // JoinCluster must NOT complete: it is stuck in the pre-vote phase because no grants arrive.
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => node.JoinCluster(cts.Token));

        Assert.False(node.IsInitialized, "node must NOT be initialized when pre-vote grants are dropped");

        await node.LeaveCluster(true);
    }

    /// <summary>
    /// Documents the fix: once <see cref="RequestVotesRequest.PreVote"/> is correctly propagated,
    /// the phantom witnesses grant the pre-vote, quorum is reached, a real election starts, the
    /// node elects itself, and <c>JoinCluster</c> completes well within the timeout.
    ///
    /// This test also serves as a regression guard: if the pre-vote grant path is broken again,
    /// this test will time out and fail.
    /// </summary>
    [Theory]
    [InlineData(1)]
    [InlineData(3)]
    public async Task SingleNode_FixedPhantomPeers_PreVoteGranted_JoinClusterInitializes(int partitions)
    {
        IRaft node = BuildNode(partitions, new FixedWitnessCommunication());

        using CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(10));

        await node.JoinCluster(cts.Token);

        Assert.True(node.IsInitialized, "node must be initialized once pre-vote grants are correctly tallied");

        await node.LeaveCluster(true);
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private IRaft BuildNode(int partitions, ICommunication communication)
    {
        RaftConfiguration config = new()
        {
            NodeName = "node1",
            NodeId = 1,
            Host = "localhost",
            Port = 8001,
            InitialPartitions = partitions,
            CompactEveryOperations = 100,
            CompactNumberEntries = 50,
            HeartbeatInterval = TimeSpan.FromMilliseconds(50),
            RecentHeartbeat = TimeSpan.FromMilliseconds(25),
            VotingTimeout = TimeSpan.FromMilliseconds(250),
            CheckLeaderInterval = TimeSpan.FromMilliseconds(25),
            UpdateNodesInterval = TimeSpan.FromMilliseconds(100),
            TimerInitialDelay = TimeSpan.FromMilliseconds(25),
            StartElectionTimeout = 100,
            EndElectionTimeout = 250,
        };

        return new RaftManager(
            config,
            new StaticDiscovery(PhantomWitnesses),
            new InMemoryWAL(logger),
            communication,
            new HybridLogicalClock(),
            logger);
    }
}
