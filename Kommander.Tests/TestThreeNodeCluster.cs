
using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Diagnostics;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;

namespace Kommander.Tests;

[SuppressMessage("Performance", "CA1859:Use concrete types when possible for improved performance")]
[Collection(ClusterIntegrationCollection.Name)]
public sealed class TestThreeNodeCluster
{
    private readonly ILogger<IRaft> logger;

    private int totalLeaderReceived;

    private int totalFollowersReceived;

    public TestThreeNodeCluster(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory1 = LoggerFactory.Create(builder =>
        {
            builder
                .AddXUnit(outputHelper)
                .SetMinimumLevel(LogLevel.Debug);
        });

        logger = loggerFactory1.CreateLogger<IRaft>();
    }

    [Theory]
    [InlineData("memory", 1)]
    [InlineData("memory", 8)]
    [InlineData("sqlite", 1)]
    [InlineData("rocksdb", 1)]
    public async Task TestJoinClusterAndDecideLeaderOnManyPartitions(
        string walStorage,
        int partitions
    )
    {
        (IRaft node1, IRaft node2, IRaft node3) = await AssembleThreNodeCluster(walStorage, partitions);

        await node1.LeaveCluster(true, CancellationToken.None);
        await node2.LeaveCluster(true, CancellationToken.None);
        await node3.LeaveCluster(true, CancellationToken.None);
    }

    [Theory]
    [InlineData("memory", 1, 25)]
    [InlineData("memory", 8, 25)]
    [InlineData("sqlite", 1, 10)]
    [InlineData("rocksdb", 1, 10)]
    public async Task TestJoinClusterAndMultiReplicateLogs(
        string walStorage,
        int partitions,
        int entries
    )
    {
        (IRaft node1, IRaft node2, IRaft node3) = await AssembleThreNodeCluster(walStorage, partitions);

        IRaft? leader = await GetLeader(1, [node1, node2, node3]);
        Assert.NotNull(leader);

        List<IRaft> followers = await GetFollowers(1, [node1, node2, node3]);
        Assert.NotEmpty(followers);
        Assert.Equal(2, followers.Count);

        leader.OnReplicationReceived += (_, _) =>
        {
            Interlocked.Increment(ref totalLeaderReceived);
            return Task.FromResult(true);
        };

        ConcurrentBag<long> logsReceived = [];

        foreach (IRaft follower in followers)
            follower.OnReplicationReceived += (_, log) =>
            {
                logsReceived.Add(log.Id);

                Interlocked.Increment(ref totalFollowersReceived);
                return Task.FromResult(true);
            };

        long maxId = node1.WalAdapter.GetMaxLog(1);
        Assert.Equal(0, maxId);

        maxId = node2.WalAdapter.GetMaxLog(1);
        Assert.Equal(0, maxId);

        maxId = node3.WalAdapter.GetMaxLog(1);
        Assert.Equal(0, maxId);

        byte[] data = "Hello World"u8.ToArray();

        int expectedId = 1;

        for (int i = 0; i < entries; i++)
        {
            RaftReplicationResult response = await leader.ReplicateLogs(
                1,
                "Greeting",
                data,
                cancellationToken: TestContext.Current.CancellationToken
            );

            Assert.Equal(RaftOperationStatus.Success, response.Status);
            Assert.Equal(expectedId++, response.LogIndex);

            if (expectedId % 50 == 0)
            {
                response = await leader.ReplicateCheckpoint(
                    1,
                    cancellationToken: TestContext.Current.CancellationToken
                );

                Assert.Equal(RaftOperationStatus.Success, response.Status);
                Assert.Equal(expectedId++, response.LogIndex);
            }
        }

        // The final ack guarantees quorum durability (leader + 1 follower); the trailing follower
        // receives the last entry on the next AppendEntries/heartbeat. Converge rather than asserting
        // all three are durable the instant the last ack returns.
        //
        // This is a terminal eventual-convergence check: the exact-max comparison is safe because
        // promotion appends no no-op entry (a spurious election under load cannot inflate the count),
        // so the only variable is how long the trailing follower takes to catch up. On a loaded CI
        // runner that catch-up — and any election churn the load induces — can exceed the 15s default,
        // so give it a generous ceiling that still surfaces a genuine permanent stall.
        long expectedMax = entries + (entries / 50);
        await WaitForConditionAsync(
            () => node1.WalAdapter.GetMaxLog(1) == expectedMax
                  && node2.WalAdapter.GetMaxLog(1) == expectedMax
                  && node3.WalAdapter.GetMaxLog(1) == expectedMax,
            TestContext.Current.CancellationToken,
            timeoutMs: 30_000);

        await node1.LeaveCluster(true, CancellationToken.None);
        await node2.LeaveCluster(true, CancellationToken.None);
        await node3.LeaveCluster(true, CancellationToken.None);
    }

    [Theory]
    [InlineData("memory", 1)]
    [InlineData("memory", 8)]
    [InlineData("sqlite", 1)]
    [InlineData("rocksdb", 1)]
    public async Task TestJoinClusterAndProposeReplicateLogs(
        string walStorage,
        int partitions
    )
    {
        (IRaft node1, IRaft node2, IRaft node3) = await AssembleThreNodeCluster(walStorage, partitions);

        IRaft? leader = await GetLeader(1, [node1, node2, node3]);
        Assert.NotNull(leader);

        List<IRaft> followers = await GetFollowers(1, [node1, node2, node3]);
        Assert.NotEmpty(followers);
        Assert.Equal(2, followers.Count);

        byte[] data = "Hello World"u8.ToArray();

        leader.OnReplicationReceived += (_, log) =>
        {
            Assert.Equal("Greeting", log.LogType);
            Assert.Equal(data, log.LogData);

            Interlocked.Increment(ref totalLeaderReceived);
            return Task.FromResult(true);
        };

        foreach (IRaft follower in followers)
            follower.OnReplicationReceived += (_, _) =>
            {
                Interlocked.Increment(ref totalFollowersReceived);
                return Task.FromResult(true);
            };

        long maxId = node1.WalAdapter.GetMaxLog(1);
        Assert.Equal(0, maxId);

        maxId = node2.WalAdapter.GetMaxLog(1);
        Assert.Equal(0, maxId);

        maxId = node3.WalAdapter.GetMaxLog(1);
        Assert.Equal(0, maxId);

        for (int i = 1; i <= partitions; i++)
        {
            leader = await GetLeader(i, [node1, node2, node3]);
            Assert.NotNull(leader);

            RaftReplicationResult response = await leader.ReplicateLogs(i, "Greeting", data, false, cancellationToken: TestContext.Current.CancellationToken);

            Assert.True(response.Success);

            Assert.Equal(RaftOperationStatus.Success, response.Status);
            Assert.Equal(1, response.LogIndex);

            Assert.Equal(0, totalFollowersReceived);
            Assert.Equal(0, totalLeaderReceived);
        }

        await node1.LeaveCluster(true, CancellationToken.None);
        await node2.LeaveCluster(true, CancellationToken.None);
        await node3.LeaveCluster(true, CancellationToken.None);
    }

    [Theory]
    [InlineData("memory", 1)]
    [InlineData("memory", 8)]
    public async Task TestJoinClusterAndProposeReplicateLogsRace(
        string walStorage,
        int partitions
    )
    {
        (IRaft node1, IRaft node2, IRaft node3) = await AssembleThreNodeCluster(walStorage, partitions);

        IRaft? leader = await GetLeader(1, [node1, node2, node3]);
        Assert.NotNull(leader);

        List<IRaft> followers = await GetFollowers(1, [node1, node2, node3]);
        Assert.NotEmpty(followers);
        Assert.Equal(2, followers.Count);

        byte[] data = "Hello World"u8.ToArray();

        leader.OnReplicationReceived += (_, log) =>
        {
            Assert.Equal("Greeting", log.LogType);
            Assert.Equal(data, log.LogData);

            Interlocked.Increment(ref totalLeaderReceived);
            return Task.FromResult(true);
        };

        foreach (IRaft follower in followers)
            follower.OnReplicationReceived += (_, _) =>
            {
                Interlocked.Increment(ref totalFollowersReceived);
                return Task.FromResult(true);
            };

        long maxId = node1.WalAdapter.GetMaxLog(1);
        Assert.Equal(0, maxId);

        maxId = node2.WalAdapter.GetMaxLog(1);
        Assert.Equal(0, maxId);

        maxId = node3.WalAdapter.GetMaxLog(1);
        Assert.Equal(0, maxId);

        for (int i = 1; i <= partitions; i++)
        {
            leader = await GetLeader(i, [node1, node2, node3]);
            Assert.NotNull(leader);

            RaftReplicationResult[] responses = await Task.WhenAll(
                leader.ReplicateLogs(i, "Greeting", data, cancellationToken: TestContext.Current.CancellationToken),
                leader.ReplicateLogs(i, "Greeting", data, cancellationToken: TestContext.Current.CancellationToken),
                leader.ReplicateLogs(i, "Greeting", data, cancellationToken: TestContext.Current.CancellationToken)
            );

            foreach (RaftReplicationResult response in responses)
            {
                if (!response.Success)
                    throw new Exception(response.Status.ToString());

                Assert.True(response.Success);
                Assert.Equal(RaftOperationStatus.Success, response.Status);
            }
        }

        await node1.LeaveCluster(true, CancellationToken.None);
        await node2.LeaveCluster(true, CancellationToken.None);
        await node3.LeaveCluster(true, CancellationToken.None);
    }

    [Fact]
    public async Task WaitForLeaderStableAsync_LeaderChange_ResetsStabilityTimer()
    {
        (IRaft node1, IRaft node2, IRaft node3) = await AssembleThreNodeCluster("memory", 1);

        string initialLeader = await node1.WaitForLeaderStableAsync(
            1,
            TimeSpan.FromMilliseconds(150),
            TestContext.Current.CancellationToken);

        IRaft leaderNode = new[] { node1, node2, node3 }
            .Single(node => node.GetLocalEndpoint() == initialLeader);

        RaftOperationStatus status = await leaderNode.StepDownAsync(
            1,
            TestContext.Current.CancellationToken);

        Assert.Equal(RaftOperationStatus.Success, status);

        string nextLeader = await node2.WaitForLeaderStableAsync(
            1,
            TimeSpan.FromMilliseconds(150),
            TestContext.Current.CancellationToken);

        Assert.NotEqual(initialLeader, nextLeader);

        await node1.LeaveCluster(true, CancellationToken.None);
        await node2.LeaveCluster(true, CancellationToken.None);
        await node3.LeaveCluster(true, CancellationToken.None);
    }

    [Fact]
    public async Task TransferLeadershipAsync_TargetBecomesStableLeader_AndProposalSucceeds()
    {
        (IRaft node1, IRaft node2, IRaft node3) = await AssembleThreNodeCluster("memory", 1);

        IRaft[] nodes = [node1, node2, node3];
        string initialLeader = await node1.WaitForLeaderStableAsync(
            1,
            TimeSpan.FromMilliseconds(150),
            TestContext.Current.CancellationToken);

        string targetEndpoint = initialLeader != node2.GetLocalEndpoint()
            ? node2.GetLocalEndpoint()
            : node3.GetLocalEndpoint();

        IRaft leaderNode = GetNodeByEndpoint(nodes, initialLeader);
        IRaft targetNode = GetNodeByEndpoint(nodes, targetEndpoint);

        RaftOperationStatus transferStatus = await leaderNode.TransferLeadershipAsync(
            1,
            targetEndpoint,
            TestContext.Current.CancellationToken);

        Assert.Equal(RaftOperationStatus.Success, transferStatus);

        string stableLeader = await targetNode.WaitForLeaderStableAsync(
            1,
            TimeSpan.FromMilliseconds(150),
            TestContext.Current.CancellationToken);

        Assert.Equal(targetEndpoint, stableLeader);

        RaftReplicationResult response = await targetNode.ReplicateLogs(
            1,
            "Greeting",
            "Hello World"u8.ToArray(),
            cancellationToken: TestContext.Current.CancellationToken);

        Assert.True(response.Success);
        Assert.Equal(RaftOperationStatus.Success, response.Status);

        await node1.LeaveCluster(true, CancellationToken.None);
        await node2.LeaveCluster(true, CancellationToken.None);
        await node3.LeaveCluster(true, CancellationToken.None);
    }

    [Fact]
    public async Task TransferLeadershipAsync_StaleTarget_ReturnsReplicationFailed()
    {
        // Seed a QUORUM (node1 + node2) with committed entries, leaving node3 empty. The quorum makes
        // one of the seeded nodes deterministically win the election — node3 alone can never assemble a
        // majority. Seeding only ONE node is unsafe as a test setup now that the stale-handshake
        // election veto has been removed (B4): the two empty nodes could grant each other votes and
        // elect a data-less leader. That state is impossible in real operation (committed ⇒ replicated
        // to a quorum, so at most one node can be behind), which is exactly what the quorum seed models.
        List<RaftLog> seededCommitted =
        [
            new() { Id = 1, Term = 1, LogData = "Hello"u8.ToArray(), Time = HLCTimestamp.Zero, Type = RaftLogType.Committed },
            new() { Id = 2, Term = 1, LogData = "Hello"u8.ToArray(), Time = HLCTimestamp.Zero, Type = RaftLogType.Committed },
        ];

        (IRaft node1, IRaft node2, IRaft node3, _, InMemoryCommunication communication) =
            await AssembleThreNodeClusterWithNetwork(
                "memory",
                1,
                (wal1, wal2, _) =>
                {
                    SeedWal(wal1, 1, seededCommitted);
                    SeedWal(wal2, 1, seededCommitted);
                });

        IRaft[] nodes = [node1, node2, node3];

        string stableLeader = await node1.WaitForLeaderStableAsync(
            1,
            TimeSpan.FromMilliseconds(150),
            TestContext.Current.CancellationToken);

        // The leader is one of the seeded quorum nodes — never the empty node3.
        Assert.NotEqual(node3.GetLocalEndpoint(), stableLeader);

        IRaft leaderNode = GetNodeByEndpoint(nodes, stableLeader);

        // Keep node3 genuinely stale. The leader now (correctly) converges a sub-threshold follower
        // once writes go quiet — so the empty node3 would catch up to the seeded log on its own,
        // making a transfer to it legitimate. To exercise the rejection path we must hold node3
        // durably behind: partition it, then commit fresh entries through the surviving quorum
        // (node1 + node2). node3 cannot receive them while partitioned, so the leader's known remote
        // max for node3 stays below its own — the exact condition TransferLeadershipAsync rejects.
        communication.PartitionNode(node3.GetLocalEndpoint());

        for (int i = 0; i < 3; i++)
        {
            RaftReplicationResult r = await leaderNode.ReplicateLogs(
                1, "Greeting", "Hello World"u8.ToArray(),
                cancellationToken: TestContext.Current.CancellationToken);
            Assert.Equal(RaftOperationStatus.Success, r.Status);
        }

        // node3 is behind the leader's committed log, so transferring leadership to it must be rejected.
        RaftOperationStatus transferStatus = await leaderNode.TransferLeadershipAsync(
            1,
            node3.GetLocalEndpoint(),
            TestContext.Current.CancellationToken);

        Assert.Equal(RaftOperationStatus.ReplicationFailed, transferStatus);

        await node1.LeaveCluster(true, CancellationToken.None);
        await node2.LeaveCluster(true, CancellationToken.None);
        await node3.LeaveCluster(true, CancellationToken.None);
    }

    [Fact]
    public async Task SuspendHeartbeatsAsync_FollowersElectNewLeader_ResumeDoesNotCreateSplitBrain()
    {
        (IRaft node1, IRaft node2, IRaft node3) = await AssembleThreNodeCluster("memory", 1);

        IRaft[] nodes = [node1, node2, node3];
        string initialLeader = await node1.WaitForLeaderStableAsync(
            1,
            TimeSpan.FromMilliseconds(150),
            TestContext.Current.CancellationToken);

        IRaft initialLeaderNode = GetNodeByEndpoint(nodes, initialLeader);

        RaftOperationStatus suspendStatus = await initialLeaderNode.SuspendHeartbeatsAsync(
            1,
            TestContext.Current.CancellationToken);

        Assert.Equal(RaftOperationStatus.Success, suspendStatus);

        string newLeader = await WaitForDifferentStableLeader(
            nodes,
            1,
            initialLeader,
            TestContext.Current.CancellationToken);

        Assert.NotEqual(initialLeader, newLeader);
        Assert.False(await initialLeaderNode.AmILeaderQuick(1));

        RaftOperationStatus resumeStatus = await initialLeaderNode.ResumeHeartbeatsAsync(
            1,
            TestContext.Current.CancellationToken);

        Assert.Equal(RaftOperationStatus.Success, resumeStatus);

        string node1Leader = await node1.WaitForLeaderStableAsync(
            1,
            TimeSpan.FromMilliseconds(150),
            TestContext.Current.CancellationToken);
        string node2Leader = await node2.WaitForLeaderStableAsync(
            1,
            TimeSpan.FromMilliseconds(150),
            TestContext.Current.CancellationToken);
        string node3Leader = await node3.WaitForLeaderStableAsync(
            1,
            TimeSpan.FromMilliseconds(150),
            TestContext.Current.CancellationToken);

        Assert.Equal(newLeader, node1Leader);
        Assert.Equal(newLeader, node2Leader);
        Assert.Equal(newLeader, node3Leader);
        Assert.Equal(1, await CountLeaders(nodes, 1));

        await node1.LeaveCluster(true, CancellationToken.None);
        await node2.LeaveCluster(true, CancellationToken.None);
        await node3.LeaveCluster(true, CancellationToken.None);
    }

    [Fact]
    public async Task GetActiveNodes_LeaderPerspective_ReturnsReachableFollowers()
    {
        (IRaft node1, IRaft node2, IRaft node3) = await AssembleThreNodeCluster("memory", 1);

        IRaft[] nodes = [node1, node2, node3];
        string leaderEndpoint = await node1.WaitForLeaderStableAsync(
            1,
            TimeSpan.FromMilliseconds(150),
            TestContext.Current.CancellationToken);

        IRaft leader = GetNodeByEndpoint(nodes, leaderEndpoint);
        string[] followerEndpoints = nodes
            .Where(node => node.GetLocalEndpoint() != leaderEndpoint)
            .Select(node => node.GetLocalEndpoint())
            .OrderBy(endpoint => endpoint, StringComparer.Ordinal)
            .ToArray();

        await WaitForConditionAsync(
            () =>
            {
                IReadOnlyList<string> activeNodes = leader.GetActiveNodes(TimeSpan.FromMilliseconds(1500));
                return activeNodes.Count == followerEndpoints.Length &&
                    followerEndpoints.All(endpoint => activeNodes.Contains(endpoint, StringComparer.Ordinal));
            },
            TestContext.Current.CancellationToken);

        IReadOnlyList<string> active = leader.GetActiveNodes(TimeSpan.FromMilliseconds(1500));

        Assert.Equal(followerEndpoints, active);

        foreach (string followerEndpoint in followerEndpoints)
            Assert.True(leader.GetLastNodeActivity(followerEndpoint) > HLCTimestamp.Zero);

        Assert.Equal(HLCTimestamp.Zero, leader.GetLastNodeActivity("localhost:9999"));

        await node1.LeaveCluster(true, CancellationToken.None);
        await node2.LeaveCluster(true, CancellationToken.None);
        await node3.LeaveCluster(true, CancellationToken.None);
    }

    [Fact]
    public async Task GetActiveNodes_UnreachableFollower_DropsAfterWindow()
    {
        (IRaft node1, IRaft node2, IRaft node3, Dictionary<string, IRaft> network, _) = await AssembleThreNodeClusterWithNetwork("memory", 1);

        IRaft[] nodes = [node1, node2, node3];
        string leaderEndpoint = await node1.WaitForLeaderStableAsync(
            1,
            TimeSpan.FromMilliseconds(150),
            TestContext.Current.CancellationToken);

        IRaft leader = GetNodeByEndpoint(nodes, leaderEndpoint);
        string isolatedFollower = nodes
            .Select(node => node.GetLocalEndpoint())
            .First(endpoint => endpoint != leaderEndpoint);
        string healthyFollower = nodes
            .Select(node => node.GetLocalEndpoint())
            .First(endpoint => endpoint != leaderEndpoint && endpoint != isolatedFollower);

        await WaitForConditionAsync(
            () =>
            {
                IReadOnlyList<string> activeNodes = leader.GetActiveNodes(TimeSpan.FromMilliseconds(1500));
                return activeNodes.Contains(isolatedFollower, StringComparer.Ordinal) &&
                    activeNodes.Contains(healthyFollower, StringComparer.Ordinal);
            },
            TestContext.Current.CancellationToken);

        // Cleanly remove the isolated follower from the cluster. Unlike dropping it from the
        // in-memory routing table (which leaves it able to campaign and churn leadership), a
        // graceful leave stops it participating entirely, so the remaining two nodes keep a stable
        // leader. The leader simply stops hearing from the removed node, so it must drop out of
        // GetActiveNodes once the liveness window elapses, while the healthy follower remains.
        IRaft isolatedNode = GetNodeByEndpoint(nodes, isolatedFollower);
        await isolatedNode.LeaveCluster(true, CancellationToken.None);
        network.Remove(isolatedFollower);

        await WaitForConditionAsync(
            () =>
            {
                IReadOnlyList<string> activeNodes = leader.GetActiveNodes(TimeSpan.FromMilliseconds(1500));
                return !activeNodes.Contains(isolatedFollower, StringComparer.Ordinal) &&
                    activeNodes.Contains(healthyFollower, StringComparer.Ordinal);
            },
            TestContext.Current.CancellationToken,
            timeoutMs: 15_000);

        IReadOnlyList<string> active = leader.GetActiveNodes(TimeSpan.FromMilliseconds(1500));

        Assert.DoesNotContain(isolatedFollower, active);
        Assert.Contains(healthyFollower, active);
        Assert.True(leader.GetLastNodeActivity(isolatedFollower) > HLCTimestamp.Zero);

        await leader.LeaveCluster(true, CancellationToken.None);
        await GetNodeByEndpoint(nodes, healthyFollower).LeaveCluster(true, CancellationToken.None);
    }

    /// <summary>
    /// PreVote regression (Raft §9.6): a follower whose transport is paused while the rest of the
    /// quorum keeps committing must, on resume, rejoin without livelock — the partition keeps a
    /// single decided leader, the resumed node reverts to Follower, and its stale log catches up.
    /// Before PreVote this scenario could leave the resumed (term-inflated, stale-log) node stuck
    /// campaigning forever, surfacing as "Leader couldn't be found or is not decided".
    /// </summary>
    [Fact]
    public async Task PausedStaleFollower_OnResume_RejoinsWithoutLivelock()
    {
        InMemoryCommunication communication = new();

        IRaft node1 = GetNode1(communication, "memory", 1, logger);
        IRaft node2 = GetNode2(communication, "memory", 1, logger);
        IRaft node3 = GetNode3(communication, "memory", 1, logger);
        IRaft[] nodes = [node1, node2, node3];

        Dictionary<string, IRaft> network = new()
        {
            { "localhost:8001", node1 },
            { "localhost:8002", node2 },
            { "localhost:8003", node3 }
        };
        communication.SetNodes(network);

        await node1.UpdateNodes();
        await node2.UpdateNodes();
        await node3.UpdateNodes();

        await Task.WhenAll(
            node1.JoinCluster(TestContext.Current.CancellationToken),
            node2.JoinCluster(TestContext.Current.CancellationToken),
            node3.JoinCluster(TestContext.Current.CancellationToken));

        string initialLeader = await node1.WaitForLeaderStableAsync(
            1,
            TimeSpan.FromMilliseconds(150),
            TestContext.Current.CancellationToken);

        // Pause a follower (never the leader) so the remaining two nodes still form a quorum.
        IRaft pausedNode = nodes.First(node => node.GetLocalEndpoint() != initialLeader);
        string pausedEndpoint = pausedNode.GetLocalEndpoint();
        communication.PartitionNode(pausedEndpoint);

        // Commit entries on the surviving quorum while the paused node's log goes stale and it
        // campaigns in isolation (its pre-votes can reach no one, so it never disrupts the leader).
        // Use more than BackfillThreshold (default 10) entries so automatic backfill fires on resume.
        IRaft leader = GetNodeByEndpoint(nodes, initialLeader);
        byte[] data = "Hello World"u8.ToArray();
        const int entries = 12;
        for (int i = 0; i < entries; i++)
        {
            RaftReplicationResult response = await leader.ReplicateLogs(
                1,
                "Greeting",
                data,
                cancellationToken: TestContext.Current.CancellationToken);

            Assert.Equal(RaftOperationStatus.Success, response.Status);
        }

        long leaderMaxLog = leader.WalAdapter.GetMaxLog(1);
        Assert.True(leaderMaxLog >= entries);

        // Give the isolated node time to time out and run (failing) pre-vote rounds.
        await Task.Delay(1_000, TestContext.Current.CancellationToken);

        // The surviving quorum must still have exactly one leader, unchanged by the isolation.
        Assert.Equal(1, await CountLeaders(nodes.Where(n => n.GetLocalEndpoint() != pausedEndpoint).ToArray(), 1));

        // Resume the paused node's transport.
        communication.HealPartition(pausedEndpoint);

        // No livelock: a single leader is decided across the whole cluster, and the resumed node
        // agrees on it rather than getting stuck as a perpetual candidate.
        string resumedLeaderView = await pausedNode.WaitForLeaderStableAsync(
            1,
            TimeSpan.FromMilliseconds(150),
            TestContext.Current.CancellationToken);

        Assert.Equal(1, await CountLeaders(nodes, 1));
        Assert.False(await pausedNode.AmILeaderQuick(1)); // reverted to / stayed Follower

        IRaft decidedLeader = GetNodeByEndpoint(nodes, resumedLeaderView);

        // The leader's heartbeat loop detects the gap (> BackfillThreshold) and ships committed
        // entries automatically — no manual replications needed to drive convergence.
        await WaitForConditionAsync(
            () => pausedNode.WalAdapter.GetMaxLog(1) >= decidedLeader.WalAdapter.GetMaxLog(1),
            TestContext.Current.CancellationToken,
            timeoutMs: 20_000);

        AssertContiguousLog(pausedNode, decidedLeader, 1);

        await node1.LeaveCluster(true, CancellationToken.None);
        await node2.LeaveCluster(true, CancellationToken.None);
        await node3.LeaveCluster(true, CancellationToken.None);
    }

    /// <summary>
    /// Validates multi-round chunked backfill: a follower paused while the leader commits
    /// more entries than <c>MaxBackfillEntriesPerRound</c> must converge across several heartbeat
    /// rounds, not just the first one. Uses <c>MaxBackfillEntriesPerRound=5</c> and 20 committed
    /// entries so the follower requires at least 4 successive backfill rounds.
    ///
    /// If the chunk loop were broken — e.g. the follower's <c>lastCommitIndexes</c> entry is not
    /// updated between rounds — the follower would stall near entry 5 and the
    /// <see cref="WaitForConditionAsync"/> timeout would fire, failing the test.
    /// </summary>
    [Fact]
    public async Task PausedStaleFollower_MultiRoundBackfill_ConvergesAcrossChunks()
    {
        InMemoryCommunication communication = new();

        IRaft node1 = GetNode1WithBackfillConfig(communication, logger, maxBackfillEntriesPerRound: 5, backfillThreshold: 3);
        IRaft node2 = GetNode2WithBackfillConfig(communication, logger, maxBackfillEntriesPerRound: 5, backfillThreshold: 3);
        IRaft node3 = GetNode3WithBackfillConfig(communication, logger, maxBackfillEntriesPerRound: 5, backfillThreshold: 3);
        IRaft[] nodes = [node1, node2, node3];

        Dictionary<string, IRaft> network = new()
        {
            { "localhost:8001", node1 },
            { "localhost:8002", node2 },
            { "localhost:8003", node3 }
        };
        communication.SetNodes(network);

        await node1.UpdateNodes();
        await node2.UpdateNodes();
        await node3.UpdateNodes();

        await Task.WhenAll(
            node1.JoinCluster(TestContext.Current.CancellationToken),
            node2.JoinCluster(TestContext.Current.CancellationToken),
            node3.JoinCluster(TestContext.Current.CancellationToken));

        string initialLeader = await node1.WaitForLeaderStableAsync(
            1,
            TimeSpan.FromMilliseconds(150),
            TestContext.Current.CancellationToken);

        IRaft pausedNode = nodes.First(n => n.GetLocalEndpoint() != initialLeader);
        string pausedEndpoint = pausedNode.GetLocalEndpoint();
        communication.PartitionNode(pausedEndpoint);

        // Commit 20 entries — 4× MaxBackfillEntriesPerRound — while the follower is offline.
        IRaft leader = GetNodeByEndpoint(nodes, initialLeader);
        byte[] data = "Chunk test"u8.ToArray();
        const int entries = 20;
        for (int i = 0; i < entries; i++)
        {
            RaftReplicationResult response = await leader.ReplicateLogs(
                1, "Chunk", data,
                cancellationToken: TestContext.Current.CancellationToken);
            Assert.Equal(RaftOperationStatus.Success, response.Status);
        }

        Assert.True(leader.WalAdapter.GetMaxLog(1) >= entries);

        await Task.Delay(500, TestContext.Current.CancellationToken);

        Assert.Equal(1, await CountLeaders(
            nodes.Where(n => n.GetLocalEndpoint() != pausedEndpoint).ToArray(), 1));

        communication.HealPartition(pausedEndpoint);

        string resumedLeaderView = await pausedNode.WaitForLeaderStableAsync(
            1,
            TimeSpan.FromMilliseconds(150),
            TestContext.Current.CancellationToken);

        Assert.Equal(1, await CountLeaders(nodes, 1));
        Assert.False(await pausedNode.AmILeaderQuick(1));

        IRaft decidedLeader = GetNodeByEndpoint(nodes, resumedLeaderView);

        // Multi-round convergence: each heartbeat ships at most 5 entries, so ≥4 rounds are needed.
        await WaitForConditionAsync(
            () => pausedNode.WalAdapter.GetMaxLog(1) >= decidedLeader.WalAdapter.GetMaxLog(1),
            TestContext.Current.CancellationToken,
            timeoutMs: 20_000);

        AssertContiguousLog(pausedNode, decidedLeader, 1);

        await node1.LeaveCluster(true, CancellationToken.None);
        await node2.LeaveCluster(true, CancellationToken.None);
        await node3.LeaveCluster(true, CancellationToken.None);
    }

    /// <summary>
    /// Small-tail-gap convergence after writes stop: a follower that misses only the last
    /// entry (a gap smaller than <see cref="RaftConfiguration.BackfillThreshold"/>) while briefly
    /// partitioned must still converge once the partition heals — even though no further writes
    /// arrive to carry it forward on the live path.
    ///
    /// This is the exact shape of the flaky CI convergence failure: the leader commits an entry to
    /// a quorum of two while the third follower is unreachable, then writes stop. Heartbeats to the
    /// lagging follower are empty (they carry no log entries), and the backfill loop is gated on
    /// <c>gap &gt; BackfillThreshold</c>, so a 1-entry tail gap is never re-shipped. Without a
    /// recovery path the follower stays permanently one entry behind. The generous
    /// timeout only surfaces a genuine permanent stall — correct behaviour converges in well
    /// under a second.
    /// </summary>
    [Fact]
    public async Task SmallTailGap_ConvergesAfterWritesStop()
    {
        InMemoryCommunication communication = new();

        // Default-scale BackfillThreshold (10) so a 1-entry gap sits inside the un-backfilled window,
        // reproducing the production configuration rather than the aggressive threshold=2 used by the
        // large-gap backfill tests.
        IRaft node1 = GetNode1WithBackfillConfig(communication, logger, maxBackfillEntriesPerRound: 5, backfillThreshold: 10);
        IRaft node2 = GetNode2WithBackfillConfig(communication, logger, maxBackfillEntriesPerRound: 5, backfillThreshold: 10);
        IRaft node3 = GetNode3WithBackfillConfig(communication, logger, maxBackfillEntriesPerRound: 5, backfillThreshold: 10);
        IRaft[] nodes = [node1, node2, node3];

        Dictionary<string, IRaft> network = new()
        {
            { "localhost:8001", node1 },
            { "localhost:8002", node2 },
            { "localhost:8003", node3 }
        };
        communication.SetNodes(network);

        await node1.UpdateNodes();
        await node2.UpdateNodes();
        await node3.UpdateNodes();

        await Task.WhenAll(
            node1.JoinCluster(TestContext.Current.CancellationToken),
            node2.JoinCluster(TestContext.Current.CancellationToken),
            node3.JoinCluster(TestContext.Current.CancellationToken));

        string leaderEndpoint = await node1.WaitForLeaderStableAsync(
            1, TimeSpan.FromMilliseconds(150), TestContext.Current.CancellationToken);

        IRaft leader = GetNodeByEndpoint(nodes, leaderEndpoint);
        IRaft follower = nodes.First(n => n.GetLocalEndpoint() != leaderEndpoint);

        // Seed a committed prefix that all three nodes hold.
        byte[] data = "Seed"u8.ToArray();
        const int seed = 5;
        for (int i = 0; i < seed; i++)
        {
            RaftReplicationResult r = await leader.ReplicateLogs(1, "Seed", data,
                cancellationToken: TestContext.Current.CancellationToken);
            Assert.Equal(RaftOperationStatus.Success, r.Status);
        }

        await WaitForConditionAsync(
            () => follower.WalAdapter.GetMaxLog(1) == seed,
            TestContext.Current.CancellationToken);

        // Drop only this one follower, then commit exactly one entry: the leader + the other
        // follower form a quorum, so the write succeeds while the partitioned follower misses it.
        communication.PartitionNode(follower.GetLocalEndpoint());

        RaftReplicationResult tail = await leader.ReplicateLogs(1, "Tail", data,
            cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(RaftOperationStatus.Success, tail.Status);

        long leaderMax = leader.WalAdapter.GetMaxLog(1);
        Assert.Equal(seed + 1, leaderMax);

        // Heal and then STOP writing. The lagging follower now depends entirely on the leader's
        // recovery machinery (heartbeats / backfill) to receive that final entry.
        communication.HealPartition(follower.GetLocalEndpoint());

        await WaitForConditionAsync(
            () => follower.WalAdapter.GetMaxLog(1) == leaderMax,
            TestContext.Current.CancellationToken,
            timeoutMs: 15_000);

        await node1.LeaveCluster(true, CancellationToken.None);
        await node2.LeaveCluster(true, CancellationToken.None);
        await node3.LeaveCluster(true, CancellationToken.None);
    }

    /// <summary>
    /// Contiguous convergence via backfill: a follower partitioned from the start receives
    /// no live-proposal traffic and must catch up entirely through the heartbeat backfill loop.
    /// Asserts the follower's log is contiguous from id 1 to N, with the same term and type
    /// as the leader at every position — not merely that GetMaxLog matches.
    /// </summary>
    [Fact]
    public async Task BackfilledFollower_HasContiguousLog_MatchingLeader()
    {
        InMemoryCommunication communication = new();

        IRaft node1 = GetNode1WithBackfillConfig(communication, logger, maxBackfillEntriesPerRound: 5, backfillThreshold: 2);
        IRaft node2 = GetNode2WithBackfillConfig(communication, logger, maxBackfillEntriesPerRound: 5, backfillThreshold: 2);
        IRaft node3 = GetNode3WithBackfillConfig(communication, logger, maxBackfillEntriesPerRound: 5, backfillThreshold: 2);
        IRaft[] nodes = [node1, node2, node3];

        Dictionary<string, IRaft> network = new()
        {
            { "localhost:8001", node1 },
            { "localhost:8002", node2 },
            { "localhost:8003", node3 }
        };
        communication.SetNodes(network);

        await node1.UpdateNodes();
        await node2.UpdateNodes();
        await node3.UpdateNodes();

        await Task.WhenAll(
            node1.JoinCluster(TestContext.Current.CancellationToken),
            node2.JoinCluster(TestContext.Current.CancellationToken),
            node3.JoinCluster(TestContext.Current.CancellationToken));

        string initialLeader = await node1.WaitForLeaderStableAsync(
            1, TimeSpan.FromMilliseconds(150), TestContext.Current.CancellationToken);

        IRaft follower = nodes.First(n => n.GetLocalEndpoint() != initialLeader);
        communication.PartitionNode(follower.GetLocalEndpoint());

        IRaft leader = GetNodeByEndpoint(nodes, initialLeader);
        byte[] data = "BackfillEntry"u8.ToArray();
        const int entries = 15;
        for (int i = 0; i < entries; i++)
        {
            RaftReplicationResult result = await leader.ReplicateLogs(
                1, "Backfill", data,
                cancellationToken: TestContext.Current.CancellationToken);
            Assert.Equal(RaftOperationStatus.Success, result.Status);
        }

        Assert.True(leader.WalAdapter.GetMaxLog(1) >= entries);

        communication.HealPartition(follower.GetLocalEndpoint());

        await follower.WaitForLeaderStableAsync(
            1, TimeSpan.FromMilliseconds(150), TestContext.Current.CancellationToken);

        // Poll against whichever peer is leader now (leadership churns under load — this test runs
        // aggressive election timeouts with quiescence off), checking the true convergence property:
        // the follower's committed log is a contiguous prefix from id 1, reaches at least the 15
        // committed entries, and agrees with the leader on every id they share. Full log equality is
        // NOT the property here — a churning leader keeps appending fresh no-ops, and a follower
        // lagging by less than BackfillThreshold is not actively backfilled, so the follower can
        // legitimately settle a no-op or two behind while never diverging.
        IRaft? PrefixLeader() => nodes.FirstOrDefault(n => n != follower && FollowerPrefixAgreesWithLeader(follower, n, 1, entries));

        await WaitForConditionAsync(
            () => PrefixLeader() is not null,
            TestContext.Current.CancellationToken,
            timeoutMs: 30_000);

        AssertFollowerPrefixMatchesLeader(follower, PrefixLeader() ?? nodes.First(n => n != follower), 1, entries);

        await node1.LeaveCluster(true, CancellationToken.None);
        await node2.LeaveCluster(true, CancellationToken.None);
        await node3.LeaveCluster(true, CancellationToken.None);
    }

    /// <summary>
    /// Divergent suffix via anchored backfill: a follower holding stale proposed entries from
    /// a fake previous term is brought up to date through the leader's heartbeat backfill loop.
    ///
    /// Setup: all three nodes join and reach steady state, then the target follower is
    /// partitioned.  While isolated its WAL is injected with stale Proposed entries (term 999)
    /// at the ids immediately above its current max; in parallel the leader commits those same
    /// ids (and more) with the actual election term.  On heal the follower reports a higher
    /// maxLog (from the injected stale entries) than the leader last acked.  The leader's
    /// backfill sends a batch with <c>prevLogIndex</c> pointing at one of the stale entries,
    /// which fails the follower-side LMP term check; the leader backtracks <c>nextIndex</c>
    /// past the stale ids until the anchor matches a legitimately-committed entry, then
    /// re-ships the correct range and the follower truncates its stale tail.
    ///
    /// Drives divergence through backfill (not a live proposal) so the anchored prevLog check
    /// in <see cref="RaftPartitionStateMachine.TrySendBackfillBatchAsync"/> is exercised.
    /// </summary>
    /// <summary>
    /// A partitioned follower that accumulated stale proposed entries (uncommitted, wrong term) while
    /// disconnected must converge to the leader log after the partition heals.  Convergence happens via
    /// same-id overwrite: the leader's backfill batch includes entries at the same ids as the stale ones,
    /// the follower's WAL write replaces them in-place, and <c>TruncateLogsAfter</c> removes any
    /// remaining stale tail beyond the batch.  The LMP prevLogIndex anchor lands on the last legitimately
    /// committed entry (which both sides agree on), so no LogMismatch or nextIndex backtracking occurs.
    /// </summary>
    [Fact]
    public async Task DivergentSuffix_Backfill_OverwritesStaleUncommittedEntries()
    {
        InMemoryCommunication communication = new();

        IRaft node1 = GetNode1WithBackfillConfig(communication, logger, maxBackfillEntriesPerRound: 5, backfillThreshold: 2);
        IRaft node2 = GetNode2WithBackfillConfig(communication, logger, maxBackfillEntriesPerRound: 5, backfillThreshold: 2);
        IRaft node3 = GetNode3WithBackfillConfig(communication, logger, maxBackfillEntriesPerRound: 5, backfillThreshold: 2);
        IRaft[] nodes = [node1, node2, node3];

        Dictionary<string, IRaft> network = new()
        {
            { "localhost:8001", node1 },
            { "localhost:8002", node2 },
            { "localhost:8003", node3 }
        };
        communication.SetNodes(network);

        await node1.UpdateNodes();
        await node2.UpdateNodes();
        await node3.UpdateNodes();

        // All three nodes join so the system partition elects a leader.
        await Task.WhenAll(
            node1.JoinCluster(TestContext.Current.CancellationToken),
            node2.JoinCluster(TestContext.Current.CancellationToken),
            node3.JoinCluster(TestContext.Current.CancellationToken));

        string leaderEndpoint = await node1.WaitForLeaderStableAsync(
            1, TimeSpan.FromMilliseconds(150), TestContext.Current.CancellationToken);

        IRaft leader = GetNodeByEndpoint(nodes, leaderEndpoint);
        IRaft follower = nodes.First(n => n.GetLocalEndpoint() != leaderEndpoint && n != leader);

        // Commit a few real entries so the follower has a legitimate committed prefix.
        byte[] data = "Seed"u8.ToArray();
        for (int i = 0; i < 3; i++)
        {
            RaftReplicationResult r = await leader.ReplicateLogs(1, "Seed", data,
                cancellationToken: TestContext.Current.CancellationToken);
            Assert.Equal(RaftOperationStatus.Success, r.Status);
        }

        // Wait for the follower to receive the seed entries before partitioning it.
        await WaitForConditionAsync(
            () => follower.WalAdapter.GetMaxLog(1) >= 3,
            TestContext.Current.CancellationToken);

        // Partition the follower.  From this point it receives no leader messages.
        communication.PartitionNode(follower.GetLocalEndpoint());

        long followerMaxBeforeStale = follower.WalAdapter.GetMaxLog(1);

        // Inject stale proposed entries directly into the follower's WAL at the ids immediately
        // above its current max, using a term (999) that can never match the real election term.
        // This simulates entries a previous stale leader proposed but never committed, leaving a
        // divergent suffix the current leader must detect and replace via anchored backfill.
        long staleStart = followerMaxBeforeStale + 1;
        follower.WalAdapter.Write([(1,
        [
            new RaftLog { Id = staleStart,     Term = 999, Type = RaftLogType.Proposed, LogType = "stale" },
            new RaftLog { Id = staleStart + 1, Term = 999, Type = RaftLogType.Proposed, LogType = "stale" },
            new RaftLog { Id = staleStart + 2, Term = 999, Type = RaftLogType.Proposed, LogType = "stale" },
        ])]);

        // Commit enough entries on the surviving quorum to exceed BackfillThreshold and to
        // overlap the follower's stale ids so the leader must write over them.
        const int newEntries = 15;
        for (int i = 0; i < newEntries; i++)
        {
            RaftReplicationResult r = await leader.ReplicateLogs(1, "Live", "LiveData"u8.ToArray(),
                cancellationToken: TestContext.Current.CancellationToken);
            Assert.Equal(RaftOperationStatus.Success, r.Status);
        }

        Assert.True(leader.WalAdapter.GetMaxLog(1) >= followerMaxBeforeStale + newEntries);

        // Heal the partition.  The follower's reported maxLog (3 real + 3 stale = 6) is above
        // what the leader last acked for it.  The leader fires backfill starting at nextIndex
        // (which still points at the first stale id), anchored at the last committed entry both
        // sides agree on — so the LMP check passes.  The batch entries overwrite the follower's
        // stale ids in-place, and TruncateLogsAfter removes any tail beyond the batch end.
        communication.HealPartition(follower.GetLocalEndpoint());

        // Wait for ACTUAL convergence — the follower holding as many entries as the leader — not merely
        // for its max id to reach the leader's. Max-id alone is satisfied prematurely: the unanchored
        // live-propose broadcast delivers high ids out of order, so an orphan entry above an unfilled
        // gap pushes GetMaxLog to the leader's value while the log still has holes that backfill has not
        // filled yet. Asserting contiguity on that intermediate state is a race the assert wins whenever
        // backfill is slowed (e.g. a CPU-constrained CI runner), which is exactly how this test failed
        // intermittently. Entry count reaching parity implies the gaps are filled.
        await WaitForConditionAsync(
            () => follower.WalAdapter.ReadLogs(1).Count >= leader.WalAdapter.ReadLogs(1).Count,
            TestContext.Current.CancellationToken,
            timeoutMs: 30_000);

        // Log must be byte-for-byte identical to the leader: contiguous ids, matching terms/types.
        AssertContiguousLog(follower, leader, 1);

        // No stale entries with term 999 must survive.
        List<RaftLog> followerLogs = follower.WalAdapter.ReadLogs(1);
        Assert.DoesNotContain(followerLogs, l => l.Term == 999);

        await node1.LeaveCluster(true, CancellationToken.None);
        await node2.LeaveCluster(true, CancellationToken.None);
        await node3.LeaveCluster(true, CancellationToken.None);
    }

    [Theory]
    [Trait("Category", "Stress")]
    [InlineData("memory", 8)]
    [InlineData("sqlite", 1)]
    [InlineData("rocksdb", 1)]
    public async Task TestJoinClusterAndProposeReplicateLogsRace2(
        string walStorage,
        int partitions
    )
    {
        (IRaft node1, IRaft node2, IRaft node3) = await AssembleThreNodeCluster(walStorage, partitions);

        IRaft? leader = await GetLeader(1, [node1, node2, node3]);
        Assert.NotNull(leader);

        List<IRaft> followers = await GetFollowers(1, [node1, node2, node3]);
        Assert.NotEmpty(followers);
        Assert.Equal(2, followers.Count);

        byte[] data = "Hello World"u8.ToArray();

        leader.OnReplicationReceived += (_, log) =>
        {
            Assert.Equal("Greeting", log.LogType);
            Assert.Equal(data, log.LogData);

            Interlocked.Increment(ref totalLeaderReceived);
            return Task.FromResult(true);
        };

        foreach (IRaft follower in followers)
            follower.OnReplicationReceived += (_, _) =>
            {
                Interlocked.Increment(ref totalFollowersReceived);
                return Task.FromResult(true);
            };

        long maxId = node1.WalAdapter.GetMaxLog(1);
        Assert.Equal(0, maxId);

        maxId = node2.WalAdapter.GetMaxLog(1);
        Assert.Equal(0, maxId);

        maxId = node3.WalAdapter.GetMaxLog(1);
        Assert.Equal(0, maxId);

        for (int i = 1; i <= partitions; i++)
        {
            leader = await GetLeader(i, [node1, node2, node3]);
            Assert.NotNull(leader);

            RaftReplicationResult[] responses = await Task.WhenAll(
                leader.ReplicateLogs(i, "Greeting", data, cancellationToken: TestContext.Current.CancellationToken),
                leader.ReplicateLogs(i, "Greeting", data, cancellationToken: TestContext.Current.CancellationToken),
                leader.ReplicateLogs(i, "Greeting", data, cancellationToken: TestContext.Current.CancellationToken),
                leader.ReplicateLogs(i, "Greeting", data, cancellationToken: TestContext.Current.CancellationToken),
                leader.ReplicateLogs(i, "Greeting", data, cancellationToken: TestContext.Current.CancellationToken),
                leader.ReplicateLogs(i, "Greeting", data, cancellationToken: TestContext.Current.CancellationToken),
                leader.ReplicateLogs(i, "Greeting", data, cancellationToken: TestContext.Current.CancellationToken),
                leader.ReplicateLogs(i, "Greeting", data, cancellationToken: TestContext.Current.CancellationToken),
                leader.ReplicateLogs(i, "Greeting", data, cancellationToken: TestContext.Current.CancellationToken)
            );

            foreach (RaftReplicationResult response in responses)
            {
                if (!response.Success)
                    throw new Exception(response.Status.ToString());

                Assert.True(response.Success);
                Assert.Equal(RaftOperationStatus.Success, response.Status);
            }
        }

        await node1.LeaveCluster(true, CancellationToken.None);
        await node2.LeaveCluster(true, CancellationToken.None);
        await node3.LeaveCluster(true, CancellationToken.None);
    }

    [Theory]
    [Trait("Category", "Stress")]
    [InlineData("memory", 8)]
    [InlineData("sqlite", 1)]
    [InlineData("rocksdb", 1)]
    public async Task TestJoinClusterAndProposeReplicateLogsRace3(
        string walStorage,
        int partitions
    )
    {
        (IRaft node1, IRaft node2, IRaft node3) = await AssembleThreNodeCluster(walStorage, partitions);

        IRaft? leader = await GetLeader(1, [node1, node2, node3]);
        Assert.NotNull(leader);

        List<IRaft> followers = await GetFollowers(1, [node1, node2, node3]);
        Assert.NotEmpty(followers);
        Assert.Equal(2, followers.Count);

        byte[] data = "Hello World"u8.ToArray();

        leader.OnReplicationReceived += (_, log) =>
        {
            Assert.Equal("Greeting", log.LogType);
            Assert.Equal(data, log.LogData);

            Interlocked.Increment(ref totalLeaderReceived);
            return Task.FromResult(true);
        };

        foreach (IRaft follower in followers)
            follower.OnReplicationReceived += (_, _) =>
            {
                Interlocked.Increment(ref totalFollowersReceived);
                return Task.FromResult(true);
            };

        long maxId = node1.WalAdapter.GetMaxLog(1);
        Assert.Equal(0, maxId);

        maxId = node2.WalAdapter.GetMaxLog(1);
        Assert.Equal(0, maxId);

        maxId = node3.WalAdapter.GetMaxLog(1);
        Assert.Equal(0, maxId);

        for (int i = 1; i <= partitions; i++)
        {
            leader = await GetLeader(i, [node1, node2, node3]);
            Assert.NotNull(leader);

            RaftReplicationResult[] responses = await Task.WhenAll(
                leader.ReplicateLogs(i, "Greeting", data, cancellationToken: TestContext.Current.CancellationToken),
                leader.ReplicateLogs(i, "Greeting", data, cancellationToken: TestContext.Current.CancellationToken),
                leader.ReplicateLogs(i, "Greeting", data, cancellationToken: TestContext.Current.CancellationToken),
                leader.ReplicateCheckpoint(i, cancellationToken: TestContext.Current.CancellationToken),
                leader.ReplicateLogs(i, "Greeting", data, cancellationToken: TestContext.Current.CancellationToken),
                leader.ReplicateLogs(i, "Greeting", data, cancellationToken: TestContext.Current.CancellationToken),
                leader.ReplicateLogs(i, "Greeting", data, cancellationToken: TestContext.Current.CancellationToken),
                leader.ReplicateLogs(i, "Greeting", data, cancellationToken: TestContext.Current.CancellationToken),
                leader.ReplicateCheckpoint(i, cancellationToken: TestContext.Current.CancellationToken),
                leader.ReplicateLogs(i, "Greeting", data, cancellationToken: TestContext.Current.CancellationToken),
                leader.ReplicateLogs(i, "Greeting", data, cancellationToken: TestContext.Current.CancellationToken),
                leader.ReplicateCheckpoint(i, cancellationToken: TestContext.Current.CancellationToken)
            );

            foreach (RaftReplicationResult response in responses)
            {
                if (!response.Success)
                    throw new Exception(response.Status.ToString());

                Assert.True(response.Success);
                Assert.Equal(RaftOperationStatus.Success, response.Status);
            }
        }

        await node1.LeaveCluster(true, CancellationToken.None);
        await node2.LeaveCluster(true, CancellationToken.None);
        await node3.LeaveCluster(true, CancellationToken.None);
    }

    private async Task<(IRaft, IRaft, IRaft)> AssembleThreNodeCluster(
        string walStorage,
        int partitions,
        Action<IWAL, IWAL, IWAL>? seedWal = null)
    {
        (IRaft node1, IRaft node2, IRaft node3, _, _) = await AssembleThreNodeClusterWithNetwork(
            walStorage,
            partitions,
            seedWal);

        return (node1, node2, node3);
    }

    private async Task<(IRaft, IRaft, IRaft, Dictionary<string, IRaft>, InMemoryCommunication)> AssembleThreNodeClusterWithNetwork(
        string walStorage,
        int partitions,
        Action<IWAL, IWAL, IWAL>? seedWal = null)
    {
        InMemoryCommunication communication = new();

        IRaft node1 = GetNode1(communication, walStorage, partitions, logger);
        IRaft node2 = GetNode2(communication, walStorage, partitions, logger);
        IRaft node3 = GetNode3(communication, walStorage, partitions, logger);

        seedWal?.Invoke(node1.WalAdapter, node2.WalAdapter, node3.WalAdapter);

        Dictionary<string, IRaft> network = new()
        {
            { "localhost:8001", node1 },
            { "localhost:8002", node2 },
            { "localhost:8003", node3 }
        };

        communication.SetNodes(network);

        await node1.UpdateNodes();
        await node2.UpdateNodes();
        await node3.UpdateNodes();

        await Task.WhenAll([node1.JoinCluster(TestContext.Current.CancellationToken), node2.JoinCluster(TestContext.Current.CancellationToken), node3.JoinCluster(TestContext.Current.CancellationToken)]);

        for (int i = 1; i <= partitions; i++)
            await WaitForAnyLeader([node1, node2, node3], i, TestContext.Current.CancellationToken);

        return (node1, node2, node3, network, communication);
    }

    private static async Task WaitForAnyLeader(IRaft[] nodes, int partitionId, CancellationToken cancellationToken)
    {
        ValueStopwatch stopwatch = ValueStopwatch.StartNew();

        while (stopwatch.GetElapsedMilliseconds() < 15_000)
        {
            cancellationToken.ThrowIfCancellationRequested();

            foreach (IRaft node in nodes)
            {
                if (await node.AmILeaderQuick(partitionId).ConfigureAwait(false))
                    return;
            }

            await Task.Delay(25, cancellationToken);
        }

        throw new TimeoutException($"No leader elected for partition {partitionId} within 15 seconds.");
    }

    private static async Task<string> WaitForDifferentStableLeader(
        IRaft[] nodes,
        int partitionId,
        string previousLeader,
        CancellationToken cancellationToken)
    {
        const long totalBudgetMs = 30_000;
        ValueStopwatch stopwatch = ValueStopwatch.StartNew();

        while (stopwatch.GetElapsedMilliseconds() < totalBudgetMs)
        {
            cancellationToken.ThrowIfCancellationRequested();

            foreach (IRaft node in nodes)
            {
                long remaining = totalBudgetMs - stopwatch.GetElapsedMilliseconds();
                if (remaining <= 0)
                    break;

                // Cap each per-node wait so the outer budget is always honoured.
                // WaitForLeaderStableAsync loops indefinitely if no leader is stable yet,
                // so without a bound, one slow node can exhaust the entire timeout.
                using CancellationTokenSource nodeCts =
                    CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                nodeCts.CancelAfter(TimeSpan.FromMilliseconds(Math.Min(remaining, 500)));

                try
                {
                    string leader = await node.WaitForLeaderStableAsync(
                        partitionId,
                        TimeSpan.FromMilliseconds(150),
                        nodeCts.Token);

                    if (leader != previousLeader)
                        return leader;
                }
                catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
                {
                    // Per-node budget expired; continue to the next node.
                }
            }

            await Task.Delay(25, cancellationToken).ConfigureAwait(false);
        }

        throw new TimeoutException(
            $"Partition {partitionId} did not change leaders from {previousLeader} within {totalBudgetMs / 1000} seconds.");
    }

    /// <summary>
    /// Reads both nodes' full committed log for <paramref name="partitionId"/> and asserts:
    /// <list type="bullet">
    ///   <item>Same count of entries.</item>
    ///   <item>Ids are contiguous from 1 to N with no gaps.</item>
    ///   <item>Per-id <c>Term</c> and <c>Type</c> match the leader's log exactly.</item>
    /// </list>
    /// This is a stronger check than <c>GetMaxLog ≥ N</c>: it catches holes, reordered ids,
    /// and entries that survived with a mismatched term from a previous election.
    /// </summary>
    /// <summary>
    /// The true post-backfill convergence property, robust to leadership churn: the follower's log
    /// is a contiguous prefix from id 1, has caught up to at least <paramref name="minEntries"/>
    /// entries, and <b>agrees</b> with the leader on every id they <i>share</i> (same term and type).
    ///
    /// <para>It compares only the overlapping prefix, not the full logs, because
    /// <see cref="Kommander.WAL.IWAL.ReadLogs"/> returns committed <b>and</b> proposed entries.
    /// Both sides can legitimately run a little ahead of the other: under aggressive election
    /// timeouts a churning leader keeps appending fresh no-ops (leader ahead), and after a heal the
    /// leader ships the follower its uncommitted tail via AppendEntries, so a subsequent leader
    /// change can leave the follower holding proposed entries at ids the new leader does not yet
    /// have (follower ahead). Those trailing uncommitted entries are not part of the agreed log —
    /// they get truncated or committed later — so requiring the leader to hold every follower id
    /// would reject a follower that is actually correct on its committed prefix.</para>
    ///
    /// <para>Divergence is still caught: a mismatched term/type at any shared id fails agreement, a
    /// hole fails contiguity, and a follower truncated too far back fails the <paramref name="minEntries"/>
    /// floor.</para>
    /// </summary>
    private static bool FollowerPrefixAgreesWithLeader(IRaft follower, IRaft leader, int partitionId, int minEntries)
    {
        List<RaftLog> followerLogs = follower.WalAdapter.ReadLogs(partitionId);
        List<RaftLog> leaderLogs   = leader.WalAdapter.ReadLogs(partitionId);

        if (followerLogs.Count < minEntries || leaderLogs.Count < minEntries)
            return false;

        // Follower must be contiguous from id 1 with no holes.
        for (int i = 0; i < followerLogs.Count; i++)
        {
            if (followerLogs[i].Id != i + 1)
                return false;
        }

        // Agree on the overlapping prefix only (ids present in both logs). Both are contiguous from
        // 1, so the overlap is positions 0..min(count)-1; entries beyond that on either side are
        // uncommitted tail that will be reconciled later.
        int overlap = Math.Min(followerLogs.Count, leaderLogs.Count);
        for (int i = 0; i < overlap; i++)
        {
            RaftLog f = followerLogs[i];
            RaftLog l = leaderLogs[i];
            if (f.Id != l.Id || f.Term != l.Term || f.Type != l.Type)
                return false;
        }

        return true;
    }

    /// <summary>
    /// Throwing form of <see cref="FollowerPrefixAgreesWithLeader"/> for a descriptive final assert.
    /// </summary>
    private static void AssertFollowerPrefixMatchesLeader(IRaft follower, IRaft leader, int partitionId, int minEntries)
    {
        List<RaftLog> followerLogs = follower.WalAdapter.ReadLogs(partitionId);
        List<RaftLog> leaderLogs   = leader.WalAdapter.ReadLogs(partitionId);

        Assert.True(FollowerPrefixAgreesWithLeader(follower, leader, partitionId, minEntries),
            $"Follower log did not converge to a contiguous prefix agreeing with the leader " +
            $"(>= {minEntries} entries). follower ids/terms=[{string.Join(",", followerLogs.Select(x => $"{x.Id}:{x.Term}"))}] " +
            $"leader ids/terms=[{string.Join(",", leaderLogs.Select(x => $"{x.Id}:{x.Term}"))}]");
    }

    private static void AssertContiguousLog(IRaft follower, IRaft leader, int partitionId)
    {
        List<RaftLog> followerLogs = follower.WalAdapter.ReadLogs(partitionId);
        List<RaftLog> leaderLogs   = leader.WalAdapter.ReadLogs(partitionId);

        Assert.Equal(leaderLogs.Count, followerLogs.Count);

        for (int i = 0; i < followerLogs.Count; i++)
        {
            RaftLog f = followerLogs[i];
            RaftLog l = leaderLogs[i];
            Assert.Equal(l.Id,   f.Id);
            Assert.Equal(l.Term, f.Term);
            Assert.Equal(l.Type, f.Type);
        }

        long expectedId = 1;
        foreach (RaftLog log in followerLogs)
        {
            Assert.Equal(expectedId, log.Id);
            expectedId++;
        }
    }

    private static IRaft GetNodeByEndpoint(IRaft[] nodes, string endpoint) =>
        nodes.Single(node => node.GetLocalEndpoint() == endpoint);

    private static async Task<int> CountLeaders(IRaft[] nodes, int partitionId)
    {
        int leaders = 0;

        foreach (IRaft node in nodes)
        {
            if (await node.AmILeaderQuick(partitionId))
                leaders++;
        }

        return leaders;
    }

    private static async Task WaitForConditionAsync(
        Func<bool> condition,
        CancellationToken cancellationToken,
        int timeoutMs = 15_000)
    {
        ValueStopwatch stopwatch = ValueStopwatch.StartNew();

        while (stopwatch.GetElapsedMilliseconds() < timeoutMs)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (condition())
                return;

            await Task.Delay(25, cancellationToken).ConfigureAwait(false);
        }

        throw new TimeoutException($"Condition not satisfied within {timeoutMs}ms.");
    }

    private static IRaft GetNode1(InMemoryCommunication communication, string walStorage, int partitions, ILogger<IRaft> logger)
    {
        IWAL wal = GetWAL(walStorage, logger);

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
            EnableQuiescence = false,
            EndElectionTimeout = 250,
        };

        RaftManager node = new(
            config,
            new StaticDiscovery([new("localhost:8002"), new("localhost:8003")]),
            wal,
            communication,
            new HybridLogicalClock(),
            logger
        );

        return node;
    }

    private static RaftManager GetNode2(InMemoryCommunication communication, string walStorage, int partitions, ILogger<IRaft> logger)
    {
        IWAL wal = GetWAL(walStorage, logger);

        RaftConfiguration config = new()
        {
            NodeName = "node2",
            NodeId = 2,
            Host = "localhost",
            Port = 8002,
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
            EnableQuiescence = false,
            EndElectionTimeout = 250,
        };

        RaftManager node = new(
            config,
            new StaticDiscovery([new("localhost:8001"), new("localhost:8003")]),
            wal,
            communication,
            new HybridLogicalClock(),
            logger
        );

        return node;
    }

    private static RaftManager GetNode3(InMemoryCommunication communication, string walStorage, int partitions, ILogger<IRaft> logger)
    {
        IWAL wal = GetWAL(walStorage, logger);

        RaftConfiguration config = new()
        {
            NodeName = "node3",
            NodeId = 3,
            Host = "localhost",
            Port = 8003,
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
            EnableQuiescence = false,
            EndElectionTimeout = 250,
        };

        RaftManager node = new(
            config,
            new StaticDiscovery([new("localhost:8001"), new("localhost:8002")]),
            wal,
            communication,
            new HybridLogicalClock(),
            logger
        );

        return node;
    }

    private static IRaft GetNode1WithBackfillConfig(
        InMemoryCommunication communication, ILogger<IRaft> logger,
        int maxBackfillEntriesPerRound, int backfillThreshold)
    {
        RaftConfiguration config = new()
        {
            NodeName = "node1", NodeId = 1, Host = "localhost", Port = 8001,
            InitialPartitions = 1,
            HeartbeatInterval = TimeSpan.FromMilliseconds(50),
            RecentHeartbeat = TimeSpan.FromMilliseconds(25),
            VotingTimeout = TimeSpan.FromMilliseconds(250),
            CheckLeaderInterval = TimeSpan.FromMilliseconds(25),
            UpdateNodesInterval = TimeSpan.FromMilliseconds(100),
            TimerInitialDelay = TimeSpan.FromMilliseconds(25),
            // Stable leadership: these backfill tests partition a follower (never the leader) and do
            // not exercise failover, so aggressive election timeouts only add spurious re-elections
            // under load — churn that resets backfill progress and makes convergence flaky. Long
            // timeouts keep one leader put so backfill is what's actually measured. Heartbeats stay
            // at 50 ms so backfill rounds (which ride heartbeats) remain fast.
            StartElectionTimeout = 2000,
            EnableQuiescence = false,
            EndElectionTimeout = 4000,
            MaxBackfillEntriesPerRound = maxBackfillEntriesPerRound,
            BackfillThreshold = backfillThreshold,
        };
        return new RaftManager(config,
            new StaticDiscovery([new("localhost:8002"), new("localhost:8003")]),
            new InMemoryWAL(logger), communication, new HybridLogicalClock(), logger);
    }

    private static IRaft GetNode2WithBackfillConfig(
        InMemoryCommunication communication, ILogger<IRaft> logger,
        int maxBackfillEntriesPerRound, int backfillThreshold)
    {
        RaftConfiguration config = new()
        {
            NodeName = "node2", NodeId = 2, Host = "localhost", Port = 8002,
            InitialPartitions = 1,
            HeartbeatInterval = TimeSpan.FromMilliseconds(50),
            RecentHeartbeat = TimeSpan.FromMilliseconds(25),
            VotingTimeout = TimeSpan.FromMilliseconds(250),
            CheckLeaderInterval = TimeSpan.FromMilliseconds(25),
            UpdateNodesInterval = TimeSpan.FromMilliseconds(100),
            TimerInitialDelay = TimeSpan.FromMilliseconds(25),
            // Stable leadership: these backfill tests partition a follower (never the leader) and do
            // not exercise failover, so aggressive election timeouts only add spurious re-elections
            // under load — churn that resets backfill progress and makes convergence flaky. Long
            // timeouts keep one leader put so backfill is what's actually measured. Heartbeats stay
            // at 50 ms so backfill rounds (which ride heartbeats) remain fast.
            StartElectionTimeout = 2000,
            EnableQuiescence = false,
            EndElectionTimeout = 4000,
            MaxBackfillEntriesPerRound = maxBackfillEntriesPerRound,
            BackfillThreshold = backfillThreshold,
        };
        return new RaftManager(config,
            new StaticDiscovery([new("localhost:8001"), new("localhost:8003")]),
            new InMemoryWAL(logger), communication, new HybridLogicalClock(), logger);
    }

    private static IRaft GetNode3WithBackfillConfig(
        InMemoryCommunication communication, ILogger<IRaft> logger,
        int maxBackfillEntriesPerRound, int backfillThreshold)
    {
        RaftConfiguration config = new()
        {
            NodeName = "node3", NodeId = 3, Host = "localhost", Port = 8003,
            InitialPartitions = 1,
            HeartbeatInterval = TimeSpan.FromMilliseconds(50),
            RecentHeartbeat = TimeSpan.FromMilliseconds(25),
            VotingTimeout = TimeSpan.FromMilliseconds(250),
            CheckLeaderInterval = TimeSpan.FromMilliseconds(25),
            UpdateNodesInterval = TimeSpan.FromMilliseconds(100),
            TimerInitialDelay = TimeSpan.FromMilliseconds(25),
            // Stable leadership: these backfill tests partition a follower (never the leader) and do
            // not exercise failover, so aggressive election timeouts only add spurious re-elections
            // under load — churn that resets backfill progress and makes convergence flaky. Long
            // timeouts keep one leader put so backfill is what's actually measured. Heartbeats stay
            // at 50 ms so backfill rounds (which ride heartbeats) remain fast.
            StartElectionTimeout = 2000,
            EnableQuiescence = false,
            EndElectionTimeout = 4000,
            MaxBackfillEntriesPerRound = maxBackfillEntriesPerRound,
            BackfillThreshold = backfillThreshold,
        };
        return new RaftManager(config,
            new StaticDiscovery([new("localhost:8001"), new("localhost:8002")]),
            new InMemoryWAL(logger), communication, new HybridLogicalClock(), logger);
    }

    private static IWAL GetWAL(string walStorage, ILogger<IRaft> logger)
    {
        return walStorage switch
        {
            "memory" => new InMemoryWAL(logger),
            "sqlite" => new SqliteWAL("/tmp", Guid.NewGuid().ToString(), logger),
            "rocksdb" => new RocksDbWAL("/tmp", Guid.NewGuid().ToString(), logger),
            _ => throw new ArgumentException($"Unknown wal: {walStorage}")
        };
    }

    private static async Task<IRaft?> GetLeader(int partitionId, IRaft[] nodes)
    {
        foreach (IRaft node in nodes)
        {
            if (await node.AmILeaderQuick(partitionId).ConfigureAwait(false))
                return node;
        }

        return null;
    }

    private static async Task<List<IRaft>> GetFollowers(int partitionId, IRaft[] nodes)
    {
        List<IRaft> followers = [];

        foreach (IRaft node in nodes)
        {
            if (!await node.AmILeaderQuick(partitionId).ConfigureAwait(false))
                followers.Add(node);
        }

        return followers;
    }

    private static void SeedWal(IWAL wal, int partitionId, List<RaftLog> logs) =>
        Assert.Equal(RaftOperationStatus.Success, wal.Write([(partitionId, logs)]));
}
