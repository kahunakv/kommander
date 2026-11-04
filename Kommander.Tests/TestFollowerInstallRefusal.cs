using Kommander.Communication;
using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests;

/// <summary>
/// A follower whose partition executor is held by a whole-partition snapshot install refuses
/// entry-carrying appends at its receive path — answering the leader
/// <see cref="RaftOperationStatus.FollowerInstallingSnapshot"/> without queueing anything — and takes
/// them again the moment the install ends. Before this, every append the leader streamed during an
/// install waited behind it in the executor's unbounded queue: two million entries during one 130 s
/// install in a CamusDB fault soak, on a node that then ran out of memory.
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
public sealed class TestFollowerInstallRefusal
{
    private readonly ILogger<IRaft> logger = NullLoggerFactory.Instance.CreateLogger<IRaft>();

    [Fact]
    public async Task EntryCarryingAppend_IsRefusedWhileTheInstallRuns_AndAcceptedAfterIt()
    {
        (IRaft leader, IRaft follower, CapturingCommunication comm) = await BuildTwoNodeCluster();

        await CommitEntries(leader, partition: 1, count: 3);
        await WaitForCondition(() => follower.WalAdapter.GetMaxLog(1) >= 3, TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);

        long term = leader.WalAdapter.GetCurrentTerm(1);
        long termAtIndex3 = follower.WalAdapter.GetCurrentTerm(1);

        // The install the test holds open at its first gate, after every validation passed.
        TaskCompletionSource gateReached = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        follower.RegisterPartitionStateTransfer(new NoopTransfer());
        using IDisposable gate = follower.SetSnapshotInstallGateForTesting(1, SnapshotInstallPhase.BeforeImport, _ =>
        {
            gateReached.TrySetResult();
            return new ValueTask(release.Task);
        });

        RaftPartition partition = ((RaftManager)follower).Partitions[1];

        Task<SnapshotInstallOutcome> install = partition.InstallSnapshotAsync(new SnapshotInstallRequest
        {
            PartitionId = 1,
            SnapshotIndex = 3,
            LastIncludedTerm = termAtIndex3,
            LeaderTerm = term,
            LeaderEndpoint = leader.GetLocalEndpoint(),
            Kind = SnapshotKind.PartitionState,
            Snapshot = new MemoryStream([1, 2, 3]),
        });

        await gateReached.Task.WaitAsync(TestTimeouts.Scale(TimeSpan.FromSeconds(10)), TestContext.Current.CancellationToken);
        Assert.True(partition.IsInstallingSnapshot);

        int capturedBefore = comm.Captured.Count;

        // An entry-carrying append from the leader while the install holds the executor.
        follower.AppendLogs(Append(leader, term, id: 4, prevLogIndex: 3, prevLogTerm: termAtIndex3));

        await WaitForCondition(
            () => comm.Captured.Skip(capturedBefore).Any(r => r.Status == RaftOperationStatus.FollowerInstallingSnapshot),
            TimeSpan.FromSeconds(5),
            TestContext.Current.CancellationToken);

        CompleteAppendLogsRequest refusal = comm.Captured.Skip(capturedBefore).First(r => r.Status == RaftOperationStatus.FollowerInstallingSnapshot);
        Assert.Equal(-1, refusal.CommitIndex);
        Assert.Equal(-1, refusal.DurableIndex);
        Assert.Equal(term, refusal.Term);
        Assert.Equal(1, partition.AppendsRefusedWhileInstalling);

        // Nothing was queued or written: the follower's log is where the install found it.
        Assert.Equal(3, follower.WalAdapter.GetMaxLog(1));

        // A heartbeat is still taken (it queues behind the install), never refused.
        follower.AppendLogs(new AppendLogsRequest(partition: 1, term: term, time: leader.HybridLogicalClock.TrySendOrLocalEvent(1), endpoint: leader.GetLocalEndpoint()));
        await Task.Delay(100, TestContext.Current.CancellationToken);
        Assert.Equal(1, partition.AppendsRefusedWhileInstalling);

        release.TrySetResult();
        SnapshotInstallOutcome outcome = await install.WaitAsync(TestTimeouts.Scale(TimeSpan.FromSeconds(10)), TestContext.Current.CancellationToken);

        Assert.Equal(SnapshotInstallOutcome.Installed, outcome);
        Assert.False(partition.IsInstallingSnapshot);
        Assert.Equal(0, partition.AppendsRefusedWhileInstalling);

        // Entries are taken again now that the install is over: a write through the leader reaches the
        // follower's log over the ordinary replication path. The hold starved the leader of this
        // follower's acks for longer than the election timeout, so leadership is re-resolved first.
        string leaderNow = await leader.WaitForLeaderStableAsync(1, TimeSpan.FromMilliseconds(150), TestContext.Current.CancellationToken);
        IRaft writer = leaderNow == leader.GetLocalEndpoint() ? leader : follower;

        await CommitEntries(writer, partition: 1, count: 1);
        await WaitForCondition(() => follower.WalAdapter.GetMaxLog(1) >= 4, TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
        Assert.Equal(0, partition.AppendsRefusedWhileInstalling);

        await leader.LeaveCluster(true, CancellationToken.None);
        await follower.LeaveCluster(true, CancellationToken.None);
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    private static AppendLogsRequest Append(IRaft leader, long term, long id, long prevLogIndex, long prevLogTerm) =>
        new(
            partition: 1,
            term: term,
            time: leader.HybridLogicalClock.TrySendOrLocalEvent(1),
            endpoint: leader.GetLocalEndpoint(),
            logs: [new RaftLog { Id = id, Term = term, Type = RaftLogType.Committed, LogType = "test" }],
            prevLogIndex: prevLogIndex,
            prevLogTerm: prevLogTerm);

    private sealed class NoopTransfer : IRaftPartitionStateTransfer
    {
        public Task<Stream> ExportPartitionState(int partitionId, long upToIndex, CancellationToken ct) =>
            Task.FromResult<Stream>(new MemoryStream([1]));

        public Task ImportPartitionState(int partitionId, Stream snapshot, CancellationToken ct) =>
            snapshot.CopyToAsync(Stream.Null, ct);
    }

    private async Task<(IRaft leader, IRaft follower, CapturingCommunication comm)> BuildTwoNodeCluster()
    {
        InMemoryCommunication inner = new();
        CapturingCommunication comm = new(inner);

        IRaft n1 = MakeNode(1, "localhost:8101", ["localhost:8102"], comm);
        IRaft n2 = MakeNode(2, "localhost:8102", ["localhost:8101"], comm);

        inner.SetNodes(new Dictionary<string, IRaft>
        {
            { "localhost:8101", n1 },
            { "localhost:8102", n2 }
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

        IRaft leader = leaderEndpoint == n1.GetLocalEndpoint() ? n1 : n2;
        IRaft follower = leaderEndpoint == n1.GetLocalEndpoint() ? n2 : n1;

        return (leader, follower, comm);
    }

    private IRaft MakeNode(int id, string endpoint, string[] peers, ICommunication comm)
    {
        string host = endpoint.Split(':')[0];
        int port = int.Parse(endpoint.Split(':')[1]);

        RaftConfiguration config = new()
        {
            NodeId = id,
            Host = host,
            Port = port,
            InitialPartitions = 1,
            PingInterval = TimeSpan.Zero,
            HeartbeatInterval = TimeSpan.FromMilliseconds(50),
            RecentHeartbeat = TimeSpan.FromMilliseconds(25),
            VotingTimeout = TimeSpan.FromMilliseconds(250),
            CheckLeaderInterval = TimeSpan.FromMilliseconds(25),
            UpdateNodesInterval = TimeSpan.FromMilliseconds(100),
            TimerInitialDelay = TimeSpan.FromMilliseconds(25),
            StartElectionTimeout = 100,
            EnableQuiescence = false,
            EndElectionTimeout = 250,
            // The test holds the follower's executor for longer than the election timeout; the leader
            // must not step down as isolated for that.
            EnableCheckQuorum = false,
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
                partition, "InstallRefusalTest", data,
                cancellationToken: TestContext.Current.CancellationToken);
            Assert.Equal(RaftOperationStatus.Success, result.Status);
        }
    }

    private static async Task WaitForCondition(Func<bool> condition, TimeSpan timeout, CancellationToken ct)
    {
        timeout = TestTimeouts.Scale(timeout);
        using CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(timeout);
        while (!cts.Token.IsCancellationRequested)
        {
            if (condition()) return;
            await Task.Delay(50, cts.Token).ConfigureAwait(false);
        }
        Assert.Fail("Condition not met within timeout.");
    }

    /// <summary>Wraps the in-memory transport and records every append acknowledgement that passes through.</summary>
    private sealed class CapturingCommunication : ICommunication
    {
        private readonly InMemoryCommunication inner;
        private readonly List<CompleteAppendLogsRequest> captured = [];
        private readonly object gate = new();

        public CapturingCommunication(InMemoryCommunication inner) => this.inner = inner;

        public IReadOnlyList<CompleteAppendLogsRequest> Captured
        {
            get { lock (gate) return captured.ToList(); }
        }

        public Task<HandshakeResponse> Handshake(RaftManager manager, RaftNode node, HandshakeRequest request) => inner.Handshake(manager, node, request);
        public Task<RequestVotesResponse> RequestVotes(RaftManager manager, RaftNode node, RequestVotesRequest request) => inner.RequestVotes(manager, node, request);
        public Task<VoteResponse> Vote(RaftManager manager, RaftNode node, VoteRequest request) => inner.Vote(manager, node, request);
        public Task<AppendLogsResponse> AppendLogs(RaftManager manager, RaftNode node, AppendLogsRequest request) => inner.AppendLogs(manager, node, request);

        public Task<CompleteAppendLogsResponse> CompleteAppendLogs(RaftManager manager, RaftNode node, CompleteAppendLogsRequest request)
        {
            lock (gate) captured.Add(request);
            return inner.CompleteAppendLogs(manager, node, request);
        }

        public Task<BatchRequestsResponse> BatchRequests(RaftManager manager, RaftNode node, BatchRequestsRequest request)
        {
            if (request.Requests is not null)
            {
                foreach (BatchRequestsRequestItem item in request.Requests)
                {
                    if (item.Type == BatchRequestsRequestType.CompleteAppendLogs && item.CompleteAppendLogs is not null)
                        lock (gate) captured.Add(item.CompleteAppendLogs);
                }
            }

            return inner.BatchRequests(manager, node, request);
        }

        public Task<JoinResponse> SendJoin(RaftManager manager, RaftNode node, JoinRequest request) => inner.SendJoin(manager, node, request);
        public Task<LeaveResponse> SendLeave(RaftManager manager, RaftNode node, LeaveRequest request, CancellationToken cancellationToken = default) => inner.SendLeave(manager, node, request, cancellationToken);
        public Task<SetMemberRoleResponse> SendSetMemberRole(RaftManager manager, RaftNode node, SetMemberRoleRequest request, CancellationToken cancellationToken = default) => inner.SendSetMemberRole(manager, node, request, cancellationToken);
        public Task<Gossip.GossipAck> SendGossip(RaftManager manager, RaftNode node, Gossip.GossipMessage digest, CancellationToken cancellationToken = default) => inner.SendGossip(manager, node, digest, cancellationToken);
        public Task<Gossip.PingResponse> SendPing(RaftManager manager, RaftNode node, Gossip.PingRequest request, CancellationToken cancellationToken = default) => inner.SendPing(manager, node, request, cancellationToken);
        public Task<Gossip.PingReqResponse> SendPingReq(RaftManager manager, RaftNode node, Gossip.PingReqRequest request, CancellationToken cancellationToken = default) => inner.SendPingReq(manager, node, request, cancellationToken);
    }
}
