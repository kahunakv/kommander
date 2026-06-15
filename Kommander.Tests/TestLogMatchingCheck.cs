
using Kommander.Communication;
using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.Gossip;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests;

/// <summary>
/// Tests for the Log Matching Property check introduced in AppendLogsCoreAsync.
/// A follower must reject any AppendEntries whose prevLogIndex/prevLogTerm anchor
/// does not match its local log, and must not write any entries in that case.
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
public sealed class TestLogMatchingCheck
{
    private readonly ILogger<IRaft> logger = NullLoggerFactory.Instance.CreateLogger<IRaft>();

    // ── reply-capturing communication wrapper ─────────────────────────────────

    /// <summary>
    /// Wraps <see cref="InMemoryCommunication"/> and records every
    /// <see cref="CompleteAppendLogsRequest"/> that passes through, allowing tests to assert
    /// on the status and commit-index the follower returned.
    /// </summary>
    private sealed class CapturingCommunication : ICommunication
    {
        private readonly InMemoryCommunication _inner;
        private readonly List<CompleteAppendLogsRequest> _captured = [];
        private readonly object _lock = new();

        public CapturingCommunication(InMemoryCommunication inner) => _inner = inner;

        public IReadOnlyList<CompleteAppendLogsRequest> Captured
        {
            get { lock (_lock) return _captured.ToList(); }
        }

        public void SetNodes(Dictionary<string, IRaft> nodes) => _inner.SetNodes(nodes);

        public Task<HandshakeResponse> Handshake(RaftManager manager, RaftNode node, HandshakeRequest request)
            => _inner.Handshake(manager, node, request);

        public Task<RequestVotesResponse> RequestVotes(RaftManager manager, RaftNode node, RequestVotesRequest request)
            => _inner.RequestVotes(manager, node, request);

        public Task<VoteResponse> Vote(RaftManager manager, RaftNode node, VoteRequest request)
            => _inner.Vote(manager, node, request);

        public Task<AppendLogsResponse> AppendLogs(RaftManager manager, RaftNode node, AppendLogsRequest request)
            => _inner.AppendLogs(manager, node, request);

        public Task<CompleteAppendLogsResponse> CompleteAppendLogs(RaftManager manager, RaftNode node, CompleteAppendLogsRequest request)
        {
            lock (_lock) _captured.Add(request);
            return _inner.CompleteAppendLogs(manager, node, request);
        }

        public Task<BatchRequestsResponse> BatchRequests(RaftManager manager, RaftNode node, BatchRequestsRequest request)
        {
            if (request.Requests is not null)
            {
                foreach (BatchRequestsRequestItem item in request.Requests)
                {
                    if (item.Type == BatchRequestsRequestType.CompleteAppendLogs && item.CompleteAppendLogs is not null)
                        lock (_lock) _captured.Add(item.CompleteAppendLogs);
                }
            }
            return _inner.BatchRequests(manager, node, request);
        }

        public Task<JoinResponse> SendJoin(RaftManager manager, RaftNode node, JoinRequest request)
            => _inner.SendJoin(manager, node, request);

        public Task<LeaveResponse> SendLeave(RaftManager manager, RaftNode node, LeaveRequest request, CancellationToken cancellationToken = default)
            => _inner.SendLeave(manager, node, request, cancellationToken);

        public Task<GossipAck> SendGossip(RaftManager manager, RaftNode node, GossipMessage digest, CancellationToken cancellationToken = default)
            => _inner.SendGossip(manager, node, digest, cancellationToken);

        public Task<Gossip.PingResponse> SendPing(RaftManager manager, RaftNode node, Gossip.PingRequest request, CancellationToken cancellationToken = default)
            => _inner.SendPing(manager, node, request, cancellationToken);

        public Task<Gossip.PingReqResponse> SendPingReq(RaftManager manager, RaftNode node, Gossip.PingReqRequest request, CancellationToken cancellationToken = default)
            => _inner.SendPingReq(manager, node, request, cancellationToken);
    }

    // ── tests ─────────────────────────────────────────────────────────────────

    /// <summary>
    /// Follower at index 3 receiving an AppendLogs with prevLogIndex=5 must reject with
    /// LogMismatch (its local max log 3 is less than the requested anchor 5) and must not
    /// write any new entries.
    /// </summary>
    [Fact]
    public async Task Follower_RejectsAppend_WhenPrevLogIndexExceedsLocalMax()
    {
        (IRaft leader, IRaft follower, CapturingCommunication comm) = await BuildTwoNodeCluster();

        await CommitEntries(leader, partition: 1, count: 3);

        await WaitForCondition(
            () => follower.WalAdapter.GetMaxLog(1) >= 3,
            TimeSpan.FromSeconds(10),
            TestContext.Current.CancellationToken);

        long followerMaxBefore = follower.WalAdapter.GetMaxLog(1);
        Assert.True(followerMaxBefore >= 3);

        long leaderTerm = leader.WalAdapter.GetCurrentTerm(1);

        follower.AppendLogs(new AppendLogsRequest(
            partition: 1,
            term: leaderTerm,
            time: leader.HybridLogicalClock.TrySendOrLocalEvent(1),
            endpoint: leader.GetLocalEndpoint(),
            logs: [new RaftLog { Id = 6, Term = leaderTerm, Type = RaftLogType.Committed, LogType = "test" }],
            prevLogIndex: 5,
            prevLogTerm: leaderTerm
        ));

        await WaitForCondition(
            () => comm.Captured.Any(r => r.Status == RaftOperationStatus.LogMismatch),
            TimeSpan.FromSeconds(5),
            TestContext.Current.CancellationToken);

        // Follower must not have written any new entries.
        Assert.Equal(followerMaxBefore, follower.WalAdapter.GetMaxLog(1));

        // Follower must have replied with LogMismatch and its local max log as CommitIndex.
        CompleteAppendLogsRequest reply = comm.Captured.First(r => r.Status == RaftOperationStatus.LogMismatch);
        Assert.Equal(RaftOperationStatus.LogMismatch, reply.Status);
        Assert.Equal(followerMaxBefore, reply.CommitIndex);

        await leader.LeaveCluster(true);
        await follower.LeaveCluster(true);
    }

    /// <summary>
    /// Follower at index 3 receiving an AppendLogs with prevLogIndex=3 but a wrong
    /// prevLogTerm must reject with LogMismatch and not write any new entries.
    /// </summary>
    [Fact]
    public async Task Follower_RejectsAppend_WhenPrevLogTermMismatches()
    {
        (IRaft leader, IRaft follower, CapturingCommunication comm) = await BuildTwoNodeCluster();

        await CommitEntries(leader, partition: 1, count: 3);

        await WaitForCondition(
            () => follower.WalAdapter.GetMaxLog(1) >= 3,
            TimeSpan.FromSeconds(10),
            TestContext.Current.CancellationToken);

        long followerMaxBefore = follower.WalAdapter.GetMaxLog(1);
        Assert.True(followerMaxBefore >= 3);

        long leaderTerm = leader.WalAdapter.GetCurrentTerm(1);
        const long wrongPrevLogTerm = 999;

        follower.AppendLogs(new AppendLogsRequest(
            partition: 1,
            term: leaderTerm,
            time: leader.HybridLogicalClock.TrySendOrLocalEvent(1),
            endpoint: leader.GetLocalEndpoint(),
            logs: [new RaftLog { Id = 4, Term = leaderTerm, Type = RaftLogType.Committed, LogType = "test" }],
            prevLogIndex: 3,
            prevLogTerm: wrongPrevLogTerm
        ));

        await WaitForCondition(
            () => comm.Captured.Any(r => r.Status == RaftOperationStatus.LogMismatch),
            TimeSpan.FromSeconds(5),
            TestContext.Current.CancellationToken);

        Assert.Equal(followerMaxBefore, follower.WalAdapter.GetMaxLog(1));

        CompleteAppendLogsRequest reply2 = comm.Captured.First(r => r.Status == RaftOperationStatus.LogMismatch);
        Assert.Equal(RaftOperationStatus.LogMismatch, reply2.Status);
        Assert.Equal(followerMaxBefore, reply2.CommitIndex);

        await leader.LeaveCluster(true);
        await follower.LeaveCluster(true);
    }

    /// <summary>
    /// Follower at index 3 receiving an AppendLogs with prevLogIndex=3 and the correct
    /// prevLogTerm must accept the entry and advance its log to index 4.
    /// </summary>
    [Fact]
    public async Task Follower_AcceptsAppend_WhenAnchorMatches()
    {
        (IRaft leader, IRaft follower, CapturingCommunication comm) = await BuildTwoNodeCluster();

        await CommitEntries(leader, partition: 1, count: 3);

        await WaitForCondition(
            () => follower.WalAdapter.GetMaxLog(1) >= 3,
            TimeSpan.FromSeconds(10),
            TestContext.Current.CancellationToken);

        long followerMaxBefore = follower.WalAdapter.GetMaxLog(1);
        Assert.True(followerMaxBefore >= 3);

        long leaderTerm = leader.WalAdapter.GetCurrentTerm(1);
        // The term at index 3 is the current term (all 3 entries were committed in one election).
        long termAtIndex3 = follower.WalAdapter.GetCurrentTerm(1);

        follower.AppendLogs(new AppendLogsRequest(
            partition: 1,
            term: leaderTerm,
            time: leader.HybridLogicalClock.TrySendOrLocalEvent(1),
            endpoint: leader.GetLocalEndpoint(),
            logs: [new RaftLog { Id = 4, Term = leaderTerm, Type = RaftLogType.Committed, LogType = "test" }],
            prevLogIndex: followerMaxBefore,
            prevLogTerm: termAtIndex3
        ));

        await WaitForCondition(
            () => follower.WalAdapter.GetMaxLog(1) >= 4,
            TimeSpan.FromSeconds(5),
            TestContext.Current.CancellationToken);

        Assert.True(follower.WalAdapter.GetMaxLog(1) >= 4, "Follower must have accepted the matching append and advanced its log.");

        await leader.LeaveCluster(true);
        await follower.LeaveCluster(true);
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    private async Task<(IRaft leader, IRaft follower, CapturingCommunication comm)> BuildTwoNodeCluster()
    {
        InMemoryCommunication inner = new();
        CapturingCommunication comm = new(inner);

        IRaft n1 = MakeNode(1, "localhost:8001", ["localhost:8002"], comm);
        IRaft n2 = MakeNode(2, "localhost:8002", ["localhost:8001"], comm);

        inner.SetNodes(new Dictionary<string, IRaft>
        {
            { "localhost:8001", n1 },
            { "localhost:8002", n2 }
        });

        await n1.UpdateNodes();
        await n2.UpdateNodes();

        await Task.WhenAll(
            n1.JoinCluster(TestContext.Current.CancellationToken),
            n2.JoinCluster(TestContext.Current.CancellationToken));

        string leaderEndpoint = await n1.WaitForLeaderStableAsync(
            1,
            TimeSpan.FromMilliseconds(150),
            TestContext.Current.CancellationToken);

        IRaft leader   = leaderEndpoint == n1.GetLocalEndpoint() ? n1 : n2;
        IRaft follower = leaderEndpoint == n1.GetLocalEndpoint() ? n2 : n1;

        return (leader, follower, comm);
    }

    private IRaft MakeNode(int id, string endpoint, string[] peers, ICommunication comm)
    {
        string host = endpoint.Split(':')[0];
        int port    = int.Parse(endpoint.Split(':')[1]);

        RaftConfiguration config = new()
        {
            NodeId   = id,
            Host     = host,
            Port     = port,
            InitialPartitions    = 1,
            PingInterval         = TimeSpan.Zero,
            HeartbeatInterval    = TimeSpan.FromMilliseconds(50),
            RecentHeartbeat      = TimeSpan.FromMilliseconds(25),
            VotingTimeout        = TimeSpan.FromMilliseconds(250),
            CheckLeaderInterval  = TimeSpan.FromMilliseconds(25),
            UpdateNodesInterval  = TimeSpan.FromMilliseconds(100),
            TimerInitialDelay    = TimeSpan.FromMilliseconds(25),
            StartElectionTimeout = 100,
            EndElectionTimeout   = 250,
        };

        return new RaftManager(
            config,
            new StaticDiscovery(peers.Select(p => new RaftNode(p)).ToList()),
            new InMemoryWAL(logger),
            comm,
            new HybridLogicalClock(),
            logger);
    }

    private static async Task CommitEntries(IRaft leader, int partition, int count)
    {
        byte[] data = "test"u8.ToArray();
        for (int i = 0; i < count; i++)
        {
            RaftReplicationResult result = await leader.ReplicateLogs(
                partition, "LogMatchingTest", data,
                cancellationToken: TestContext.Current.CancellationToken);
            Assert.Equal(RaftOperationStatus.Success, result.Status);
        }
    }

    private static async Task WaitForCondition(Func<bool> condition, TimeSpan timeout, CancellationToken ct)
    {
        using CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(timeout);
        while (!cts.Token.IsCancellationRequested)
        {
            if (condition()) return;
            await Task.Delay(100, cts.Token).ConfigureAwait(false);
        }
        Assert.Fail("Condition not met within timeout.");
    }
}
