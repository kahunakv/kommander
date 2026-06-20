
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
/// Integration test: a 4th node contacts a seed, is added as a Learner,
/// catches up via bounded backfill, and is auto-promoted to Voter.
/// Verifies the full join RPC + JoinCluster(seeds) + promotion-driver path.
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
public sealed class TestFourNodeJoin
{
    private readonly ILogger<IRaft> logger = NullLoggerFactory.Instance.CreateLogger<IRaft>();

    private static async Task WaitForConditionAsync(Func<bool> condition, CancellationToken ct, int timeoutMs = 30_000)
    {
        long deadline = Environment.TickCount64 + timeoutMs;
        while (Environment.TickCount64 < deadline)
        {
            ct.ThrowIfCancellationRequested();
            if (condition()) return;
            await Task.Delay(50, ct).ConfigureAwait(false);
        }
        throw new TimeoutException($"Condition not met within {timeoutMs} ms.");
    }

    [Fact]
    public async Task FourthNode_JoinClusterWithSeeds_BecomesVoter()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        InMemoryCommunication communication = new();

        RaftManager node1 = BuildNode("node1", 1, 8201, ["localhost:8202", "localhost:8203"], communication, logger);
        RaftManager node2 = BuildNode("node2", 2, 8202, ["localhost:8201", "localhost:8203"], communication, logger);
        RaftManager node3 = BuildNode("node3", 3, 8203, ["localhost:8201", "localhost:8202"], communication, logger);
        // Node 4 has no initial partitions. Its discovery seeds include nodes 1-3 so that before
        // the committed roster populates its Nodes list it can still route CompleteAppendLogs ACKs
        // back to the P0 leader (required by InMemoryCommunication's IsNode guard).
        RaftManager node4 = BuildNode("node4", 4, 8204, ["localhost:8201", "localhost:8202", "localhost:8203"], communication, logger, initialPartitions: 0);

        // Register all 4 nodes in the in-memory routing table up-front so the P0 leader can
        // deliver AppendLogs to node4 once it is added to the roster as a Learner.
        communication.SetNodes(new Dictionary<string, IRaft>
        {
            ["localhost:8201"] = node1,
            ["localhost:8202"] = node2,
            ["localhost:8203"] = node3,
            ["localhost:8204"] = node4
        });

        try
        {
            // ── Step 1: start 3-node cluster ────────────────────────────────────────
            await Task.WhenAll(
                node1.JoinCluster(ct),
                node2.JoinCluster(ct),
                node3.JoinCluster(ct)
            );

            await WaitForConditionAsync(
                () => node1.IsInitialized && node2.IsInitialized && node3.IsInitialized,
                ct);

            // ── Step 2: commit entries so node4 has log to backfill ──────────────────
            RaftManager leader = await FindLeaderAsync([node1, node2, node3], ct);
            int userPartitionId = leader.Partitions.Keys.FirstOrDefault();
            if (userPartitionId != 0)
            {
                for (int i = 0; i < 10; i++)
                {
                    await leader.ReplicateLogs(userPartitionId, "test", [1, 2, 3], cancellationToken: ct);
                }
            }

            // ── Step 3: node4 joins via seeds ────────────────────────────────────────
            // JoinCluster(seeds) contacts a seed, is admitted as Learner, waits for
            // IsInitialized, then waits for promotion to Voter — all within the 60 s deadline.
            await node4.JoinCluster(["localhost:8201"], ct);

            // ── Step 4: verify node4 is now a committed Voter ─────────────────────────
            Assert.Equal(System.ClusterMemberRole.Voter, node4.LocalRole);

            // All original nodes should also see node4 as Voter in the committed roster.
            await WaitForConditionAsync(
                () => node1.SystemCoordinator.GetMembership().Members
                    .Any(m => m.Endpoint == "localhost:8204" && m.Role == System.ClusterMemberRole.Voter),
                ct);
        }
        finally
        {
            await node4.LeaveCluster(dispose: true);
            await node3.LeaveCluster(dispose: true);
            await node2.LeaveCluster(dispose: true);
            await node1.LeaveCluster(dispose: true);
        }
    }

    private static async Task<RaftManager> FindLeaderAsync(RaftManager[] nodes, CancellationToken ct)
    {
        long deadline = Environment.TickCount64 + 15_000;
        while (Environment.TickCount64 < deadline)
        {
            ct.ThrowIfCancellationRequested();
            foreach (RaftManager node in nodes)
            {
                if (await node.AmILeaderQuick(0).ConfigureAwait(false))
                    return node;
            }
            await Task.Delay(100, ct).ConfigureAwait(false);
        }
        return nodes[0];
    }

    private static async Task<RaftManager> FindLeaderForPartitionAsync(RaftManager[] nodes, int partitionId, CancellationToken ct)
    {
        long deadline = Environment.TickCount64 + 15_000;
        while (Environment.TickCount64 < deadline)
        {
            ct.ThrowIfCancellationRequested();
            foreach (RaftManager node in nodes)
            {
                if (await node.AmILeaderQuick(partitionId).ConfigureAwait(false))
                    return node;
            }
            await Task.Delay(100, ct).ConfigureAwait(false);
        }
        throw new TimeoutException($"No leader for partition {partitionId} within 15 s.");
    }

    /// <summary>
    /// Regression for the quorum-inflation bug: a Learner is in the leader's peer set (so it
    /// receives replication) but must NOT count toward quorum. With one of three voters down and
    /// a Learner present, a commit must still succeed at 2-of-3 voters. If the Learner inflated the
    /// denominator to 3-of-4, the leader plus one surviving voter (2 acks) could never reach quorum
    /// and this commit would stall.
    /// </summary>
    [Fact]
    public async Task LearnerPresent_DoesNotInflateQuorum_CommitSucceedsWithOneVoterDown()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        InMemoryCommunication communication = new();

        // Long stable window so the partitioned Learner is never auto-promoted during the test.
        TimeSpan longWindow = TimeSpan.FromSeconds(60);

        RaftManager node1 = BuildNode("node1", 1, 8221, ["localhost:8222", "localhost:8223"], communication, logger, promotionStableWindow: longWindow);
        RaftManager node2 = BuildNode("node2", 2, 8222, ["localhost:8221", "localhost:8223"], communication, logger, promotionStableWindow: longWindow);
        RaftManager node3 = BuildNode("node3", 3, 8223, ["localhost:8221", "localhost:8222"], communication, logger, promotionStableWindow: longWindow);
        RaftManager node4 = BuildNode("node4", 4, 8224, ["localhost:8221", "localhost:8222", "localhost:8223"], communication, logger, promotionStableWindow: longWindow);

        communication.SetNodes(new Dictionary<string, IRaft>
        {
            ["localhost:8221"] = node1,
            ["localhost:8222"] = node2,
            ["localhost:8223"] = node3,
            ["localhost:8224"] = node4
        });

        try
        {
            await Task.WhenAll(node1.JoinCluster(ct), node2.JoinCluster(ct), node3.JoinCluster(ct));
            await WaitForConditionAsync(
                () => node1.IsInitialized && node2.IsInitialized && node3.IsInitialized, ct);

            RaftManager[] voters = [node1, node2, node3];

            // Partition node4 up-front: it never acks, so it stays a Learner (the long stable
            // window also blocks the never-acked promotion path from promoting it mid-test).
            communication.PartitionNode("localhost:8224");

            // Admit node4 as a Learner via the P0 leader. We do NOT use JoinCluster(seeds) here
            // because that blocks until promotion to Voter — node4 must remain a Learner.
            RaftManager p0Leader = await FindLeaderForPartitionAsync(voters, 0, ct);
            TaskCompletionSource<(RaftOperationStatus Status, long Generation)> tcs =
                new(TaskCreationOptions.RunContinuationsAsynchronously);
            p0Leader.SystemCoordinator.Send(new System.RaftSystemRequest(
                System.RaftSystemRequestType.AddMember, "localhost:8224", 4,
                p0Leader.SystemCoordinator.GetMembership().MembershipVersion, tcs));
            (RaftOperationStatus addStatus, _) = await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5), ct);
            Assert.Equal(RaftOperationStatus.Success, addStatus);

            // Wait until every voter's derived peer set includes the Learner — whichever voter is
            // the user-partition leader must have node4 in host.Nodes for the test to be meaningful.
            await WaitForConditionAsync(
                () => voters.All(v => v.GetNodes().Any(n => n.Endpoint == "localhost:8224")), ct);
            Assert.Equal(System.ClusterMemberRole.Learner,
                p0Leader.SystemCoordinator.GetMembership().Members.First(m => m.Endpoint == "localhost:8224").Role);

            // Take down one voter that is not the user-partition leader (and, if possible, not the
            // P0 leader either, to avoid incidental P0 re-election churn).
            int userPartition = p0Leader.Partitions.Keys.First(k => k != 0);
            RaftManager p1Leader = await FindLeaderForPartitionAsync(voters, userPartition, ct);

            RaftManager voterToKill =
                voters.FirstOrDefault(v => v.GetLocalEndpoint() != p1Leader.GetLocalEndpoint()
                                        && v.GetLocalEndpoint() != p0Leader.GetLocalEndpoint())
                ?? voters.First(v => v.GetLocalEndpoint() != p1Leader.GetLocalEndpoint());

            communication.PartitionNode(voterToKill.GetLocalEndpoint());

            // Two of three voters remain (leader + one follower). The commit must succeed at
            // 2-of-3 — the Learner must not have inflated the quorum to 3-of-4.
            RaftReplicationResult result = await p1Leader.ReplicateLogs(
                userPartition, "test", [9, 9, 9], cancellationToken: ct);

            Assert.Equal(RaftOperationStatus.Success, result.Status);

            // The Learner must still be a Learner — never promoted.
            Assert.Equal(System.ClusterMemberRole.Learner,
                p0Leader.SystemCoordinator.GetMembership().Members.First(m => m.Endpoint == "localhost:8224").Role);

            communication.HealPartition(voterToKill.GetLocalEndpoint());
            communication.HealPartition("localhost:8224");
        }
        finally
        {
            await node4.LeaveCluster(dispose: true);
            await node3.LeaveCluster(dispose: true);
            await node2.LeaveCluster(dispose: true);
            await node1.LeaveCluster(dispose: true);
        }
    }

    /// <summary>
    /// Verifies the cross-partition promotion gate: a Learner that is caught up on the
    /// P0-led partition but behind on a partition led by a different node must stay a
    /// Learner until it catches up on that remote partition.
    ///
    /// The gate queries <c>ICommunication.GetRemoteFollowerLag</c> for partitions the P0 leader
    /// does not lead locally.  This test intercepts that call via <see cref="RemoteLagBlocker"/>
    /// to inject fake lag for the learner on the test partition, confirms promotion is withheld
    /// for the full stable window, then removes the block and waits for actual promotion.
    /// </summary>
    [Fact]
    public async Task LearnerOnRemotelyLedPartition_IsBlockedUntilCaughtUp()
    {
        const string n4Endpoint = "localhost:8574";
        const int lagThreshold = 5;
        const int promotionStableWindowMs = 1000;
        const int holdMs = promotionStableWindowMs + 500;

        CancellationToken ct = TestContext.Current.CancellationToken;
        InMemoryCommunication innerComm = new();
        RemoteLagBlocker blocker = new(innerComm);

        static RaftManager Make(ICommunication comm, string name, int id, int port, string[] peers, int initialPartitions, int stableWindowMs)
        {
            RaftConfiguration config = new()
            {
                NodeName = name, NodeId = id, Host = "localhost", Port = port,
                InitialPartitions = initialPartitions,
                HeartbeatInterval = TimeSpan.FromMilliseconds(50),
                RecentHeartbeat = TimeSpan.FromMilliseconds(25),
                VotingTimeout = TimeSpan.FromMilliseconds(500),
                CheckLeaderInterval = TimeSpan.FromMilliseconds(25),
                UpdateNodesInterval = TimeSpan.FromMilliseconds(200),
                TimerInitialDelay = TimeSpan.FromMilliseconds(25),
                StartElectionTimeout = 100, EndElectionTimeout = 300,
                EnableQuiescence = false,
                BackfillThreshold = 0, MaxBackfillEntriesPerRound = 128,
                LearnerPromotionLag = lagThreshold,
                LearnerPromotionStableWindow = TimeSpan.FromMilliseconds(stableWindowMs),
            };
            return new RaftManager(config,
                new StaticDiscovery(peers.Select(p => new RaftNode(p)).ToList()),
                new InMemoryWAL(NullLoggerFactory.Instance.CreateLogger<IRaft>()),
                comm, new HybridLogicalClock(),
                NullLoggerFactory.Instance.CreateLogger<IRaft>());
        }

        RaftManager n1 = Make(blocker, "node1", 1, 8571, ["localhost:8572", "localhost:8573"], 2, promotionStableWindowMs);
        RaftManager n2 = Make(blocker, "node2", 2, 8572, ["localhost:8571", "localhost:8573"], 2, promotionStableWindowMs);
        RaftManager n3 = Make(blocker, "node3", 3, 8573, ["localhost:8571", "localhost:8572"], 2, promotionStableWindowMs);
        RaftManager n4 = Make(blocker, "node4", 4, 8574, ["localhost:8571", "localhost:8572", "localhost:8573"], 0, promotionStableWindowMs);

        // Register all 4 in the in-memory routing table before starting so that the P0 leader can
        // route AppendLogs to n4 once it is committed to the roster.
        innerComm.SetNodes(new Dictionary<string, IRaft>
        {
            ["localhost:8571"] = n1,
            ["localhost:8572"] = n2,
            ["localhost:8573"] = n3,
            ["localhost:8574"] = n4,
        });

        try
        {
            // ── Phase 1: boot 3-node cluster ──────────────────────────────────────────
            await Task.WhenAll(n1.JoinCluster(ct), n2.JoinCluster(ct), n3.JoinCluster(ct));
            await WaitForConditionAsync(() => n1.IsInitialized && n2.IsInitialized && n3.IsInitialized, ct);

            RaftManager[] voters = [n1, n2, n3];
            RaftManager p0Leader = await FindLeaderForPartitionAsync(voters, 0, ct);
            string p0Endpoint = p0Leader.GetLocalEndpoint();

            // Pick one user partition to use as the "remote" test partition.
            int testPartitionId = p0Leader.Partitions.Keys.First();

            // Ensure testPartition is led by a node OTHER than the P0 leader.
            // This is required so the gate uses GetRemoteFollowerLag (the code under test)
            // rather than the local committed-index path.
            if (await p0Leader.AmILeaderQuick(testPartitionId))
            {
                RaftManager transferTarget = voters.First(v => v.GetLocalEndpoint() != p0Endpoint);
                // TransferLeadershipAsync can return ReplicationFailed transiently if the target
                // hasn't replicated the leader's no-op yet (race between user-partition heartbeat
                // and this call completing before the first replication round). Retry until the
                // transfer is initiated or another node has already won the partition.
                long transferDeadline = Environment.TickCount64 + 10_000;
                while (Environment.TickCount64 < transferDeadline)
                {
                    RaftOperationStatus st = await p0Leader.TransferLeadershipAsync(
                        testPartitionId, transferTarget.GetLocalEndpoint(), ct);
                    if (st != RaftOperationStatus.ReplicationFailed)
                        break;
                    await Task.Delay(200, ct);
                }
            }

            // Wait until a leader is established for testPartition that is NOT the P0 leader.
            RaftManager testPartitionLeader = await WaitForLeaderExcludingAsync(voters, testPartitionId, p0Endpoint, ct);

            // ── Phase 2: admit n4 as Learner ─────────────────────────────────────────
            // Start n4's runtime (fire-and-forget; it blocks on IsInitialized which
            // resolves once P0 replicates the partition map to it after AddMember).
            Task n4Runtime = n4.JoinCluster(ct);

            TaskCompletionSource<(RaftOperationStatus, long)> addTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
            p0Leader.SystemCoordinator.Send(new System.RaftSystemRequest(
                System.RaftSystemRequestType.AddMember, n4Endpoint, 4,
                p0Leader.SystemCoordinator.GetMembership().MembershipVersion, addTcs));
            (RaftOperationStatus addStatus, _) = await addTcs.Task.WaitAsync(TimeSpan.FromSeconds(10), ct);
            Assert.Equal(RaftOperationStatus.Success, addStatus);

            // n4 is now in the roster as Learner and the P0 leader will start replicating to it.
            await WaitForConditionAsync(
                () => p0Leader.SystemCoordinator.GetMembership().Members
                    .Any(m => m.Endpoint == n4Endpoint && m.Role == System.ClusterMemberRole.Learner),
                ct);

            // Let n4 actually initialize (receives partition map from P0).
            await WaitForConditionAsync(() => n4.IsInitialized, ct, timeoutMs: 20_000);

            // Let all voters pick up n4 in their peer lists so replication to n4 starts.
            await WaitForConditionAsync(
                () => voters.All(v => v.GetNodes().Any(n => n.Endpoint == n4Endpoint)), ct);

            // ── Phase 3: build genuine committed-log depth on testPartition ───────────
            // Commit more than LearnerPromotionLag entries on testPartition so the leader's
            // committed index is well above the threshold.
            for (int i = 0; i < lagThreshold * 4; i++)
                await testPartitionLeader.ReplicateLogs(testPartitionId, "test", [1, 2, 3], cancellationToken: ct);

            // ── Phase 4: inject fake lag and confirm promotion is withheld ────────────
            // Block GetRemoteFollowerLag for (testPartitionId, n4) — forces the gate to see
            // learnerCommitted = 0, so lag = leaderCommitted >> LearnerPromotionLag.
            blocker.Block(testPartitionId, n4Endpoint);

            long holdDeadline = Environment.TickCount64 + holdMs;
            while (Environment.TickCount64 < holdDeadline)
            {
                ct.ThrowIfCancellationRequested();
                System.ClusterMemberRole role = p0Leader.SystemCoordinator.GetMembership().Members
                    .FirstOrDefault(m => m.Endpoint == n4Endpoint)?.Role
                    ?? System.ClusterMemberRole.NotMember;
                Assert.Equal(System.ClusterMemberRole.Learner, role);
                await Task.Delay(50, ct);
            }

            // ── Phase 5: remove the block and wait for promotion ──────────────────────
            // n4 has been receiving all entries throughout (only the lag query was intercepted),
            // so once unblocked the real committed-index is returned, lag falls to ~0,
            // and the stable window can accumulate.
            blocker.Unblock();

            await WaitForConditionAsync(
                () => p0Leader.SystemCoordinator.GetMembership().Members
                    .Any(m => m.Endpoint == n4Endpoint && m.Role == System.ClusterMemberRole.Voter),
                ct, timeoutMs: 15_000);

            // Confirm from n4's own view.
            Assert.Equal(System.ClusterMemberRole.Voter, n4.LocalRole);

            // Clean up n4Runtime — it should have completed once n4 was initialized.
            await n4Runtime.WaitAsync(TimeSpan.FromSeconds(5), ct);
        }
        finally
        {
            await n4.LeaveCluster(dispose: true);
            await n3.LeaveCluster(dispose: true);
            await n2.LeaveCluster(dispose: true);
            await n1.LeaveCluster(dispose: true);
        }
    }

    private static async Task<RaftManager> WaitForLeaderExcludingAsync(
        RaftManager[] nodes, int partitionId, string excludedEndpoint, CancellationToken ct)
    {
        long deadline = Environment.TickCount64 + 15_000;
        while (Environment.TickCount64 < deadline)
        {
            ct.ThrowIfCancellationRequested();
            foreach (RaftManager node in nodes)
            {
                if (node.GetLocalEndpoint() == excludedEndpoint)
                    continue;
                if (await node.AmILeaderQuick(partitionId).ConfigureAwait(false))
                    return node;
            }
            await Task.Delay(50, ct).ConfigureAwait(false);
        }
        throw new TimeoutException($"No leader (excluding {excludedEndpoint}) elected for partition {partitionId} within 15 s.");
    }

    /// <summary>
    /// Intercepts <see cref="ICommunication.GetRemoteFollowerLag"/> for a specific
    /// (partitionId, followerEndpoint) pair and returns 0, simulating a learner that appears
    /// far behind on that partition without actually pausing in-process replication.
    /// All other ICommunication calls are forwarded to the wrapped <see cref="InMemoryCommunication"/>.
    /// </summary>
    private sealed class RemoteLagBlocker(InMemoryCommunication inner) : ICommunication
    {
        private volatile bool _active;
        private int _blockedPartition;
        private string _blockedFollower = "";

        public void Block(int partitionId, string follower)
        {
            _blockedPartition = partitionId;
            _blockedFollower = follower;
            _active = true;
        }

        public void Unblock() => _active = false;

        public Task<long?> GetRemoteFollowerLag(RaftManager m, RaftNode n, int partitionId, string followerEndpoint)
        {
            if (_active && partitionId == _blockedPartition && followerEndpoint == _blockedFollower)
                return Task.FromResult<long?>(0L);
            return inner.GetRemoteFollowerLag(m, n, partitionId, followerEndpoint);
        }

        public Task<HandshakeResponse> Handshake(RaftManager m, RaftNode n, HandshakeRequest r) => inner.Handshake(m, n, r);
        public Task<RequestVotesResponse> RequestVotes(RaftManager m, RaftNode n, RequestVotesRequest r) => inner.RequestVotes(m, n, r);
        public Task<VoteResponse> Vote(RaftManager m, RaftNode n, VoteRequest r) => inner.Vote(m, n, r);
        public Task<AppendLogsResponse> AppendLogs(RaftManager m, RaftNode n, AppendLogsRequest r) => inner.AppendLogs(m, n, r);
        public Task<CompleteAppendLogsResponse> CompleteAppendLogs(RaftManager m, RaftNode n, CompleteAppendLogsRequest r) => inner.CompleteAppendLogs(m, n, r);
        public Task<BatchRequestsResponse> BatchRequests(RaftManager m, RaftNode n, BatchRequestsRequest r) => inner.BatchRequests(m, n, r);
        public Task<JoinResponse> SendJoin(RaftManager m, RaftNode n, JoinRequest r) => inner.SendJoin(m, n, r);
        public Task<LeaveResponse> SendLeave(RaftManager m, RaftNode n, LeaveRequest r, CancellationToken ct) => inner.SendLeave(m, n, r, ct);
        public Task<Gossip.GossipAck> SendGossip(RaftManager m, RaftNode n, Gossip.GossipMessage r, CancellationToken ct) => inner.SendGossip(m, n, r, ct);
        public Task<Gossip.PingResponse> SendPing(RaftManager m, RaftNode n, Gossip.PingRequest r, CancellationToken ct) => inner.SendPing(m, n, r, ct);
        public Task<Gossip.PingReqResponse> SendPingReq(RaftManager m, RaftNode n, Gossip.PingReqRequest r, CancellationToken ct) => inner.SendPingReq(m, n, r, ct);
    }

    private RaftManager BuildNode(
        string name, int id, int port,
        string[] peers,
        InMemoryCommunication communication,
        ILogger<IRaft> logger,
        int initialPartitions = 1,
        TimeSpan? promotionStableWindow = null)
    {
        RaftConfiguration config = new()
        {
            NodeName = name,
            NodeId = id,
            Host = "localhost",
            Port = port,
            InitialPartitions = initialPartitions,
            HeartbeatInterval = TimeSpan.FromMilliseconds(50),
            RecentHeartbeat = TimeSpan.FromMilliseconds(25),
            VotingTimeout = TimeSpan.FromMilliseconds(500),
            CheckLeaderInterval = TimeSpan.FromMilliseconds(25),
            UpdateNodesInterval = TimeSpan.FromMilliseconds(200),
            TimerInitialDelay = TimeSpan.FromMilliseconds(25),
            StartElectionTimeout = 100,
            EnableQuiescence = false,
            EndElectionTimeout = 300,
            BackfillThreshold = 0,
            MaxBackfillEntriesPerRound = 128,
            LearnerPromotionLag = 5,
            LearnerPromotionStableWindow = promotionStableWindow ?? TimeSpan.FromMilliseconds(500),
        };

        return new RaftManager(
            config,
            new StaticDiscovery(peers.Select(p => new RaftNode(p)).ToList()),
            new InMemoryWAL(logger),
            communication,
            new HybridLogicalClock(),
            logger);
    }
}
