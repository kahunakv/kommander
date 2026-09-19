
using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;

namespace Kommander.Tests;

/// <summary>
/// Tests for the pairwise delivery filters of <see cref="InMemoryCommunication"/>
/// (<see cref="InMemoryCommunication.BlockLink"/> and related methods).
/// </summary>
/// <remarks>
/// <para>
/// <see cref="InMemoryCommunication.PartitionNode"/> isolates a whole node. The link filters cut only
/// the link between two given nodes, so an in-process cluster can simulate a network partition in
/// which each side still reaches some peers. Embedded hosts (for example the browser demo of a
/// minority that cannot commit) and test harnesses use them.
/// </para>
/// <para>
/// The cluster tests use three voters and one partition, with short timers. Quiescence is off, so
/// followers detect a lost leader through the heartbeat timer.
/// </para>
/// </remarks>
[Collection(ClusterIntegrationCollection.Name)]
public sealed class TestInMemoryLinkBlocks
{
    private const int Partition = 1;

    private readonly ILogger<IRaft> logger;

    public TestInMemoryLinkBlocks(ITestOutputHelper outputHelper)
    {
        ILoggerFactory factory = LoggerFactory.Create(b => b.AddXUnit(outputHelper).SetMinimumLevel(LogLevel.Information));
        logger = factory.CreateLogger<IRaft>();
    }

    // ── Filter semantics ──────────────────────────────────────────────────────

    [Fact]
    public void BlockLink_DropsOnlyTheBlockedDirection()
    {
        InMemoryCommunication communication = new();

        Assert.False(communication.IsDeliveryBlocked("a", "b"));

        communication.BlockLink("a", "b");

        Assert.True(communication.IsDeliveryBlocked("a", "b"));
        Assert.False(communication.IsDeliveryBlocked("b", "a"));
        Assert.False(communication.IsDeliveryBlocked("a", "c"));
        Assert.False(communication.IsDeliveryBlocked("c", "b"));

        communication.UnblockLink("a", "b");

        Assert.False(communication.IsDeliveryBlocked("a", "b"));
    }

    [Fact]
    public void BlockLinkBothWays_DropsBothDirections_AndUnblockRestoresBoth()
    {
        InMemoryCommunication communication = new();

        communication.BlockLinkBothWays("a", "b");

        Assert.True(communication.IsDeliveryBlocked("a", "b"));
        Assert.True(communication.IsDeliveryBlocked("b", "a"));
        Assert.False(communication.IsDeliveryBlocked("a", "c"));

        communication.UnblockLinkBothWays("b", "a");

        Assert.False(communication.IsDeliveryBlocked("a", "b"));
        Assert.False(communication.IsDeliveryBlocked("b", "a"));
    }

    /// <summary>
    /// Node isolation and link blocks are independent filters. Healing one must not remove the other,
    /// and <see cref="InMemoryCommunication.HealAll"/> removes both.
    /// </summary>
    [Fact]
    public void NodeIsolation_AndLinkBlocks_Combine()
    {
        InMemoryCommunication communication = new();

        communication.PartitionNode("a");
        communication.BlockLink("b", "c");

        Assert.True(communication.IsDeliveryBlocked("a", "b"));
        Assert.True(communication.IsDeliveryBlocked("c", "a"));
        Assert.True(communication.IsDeliveryBlocked("b", "c"));

        communication.HealPartition("a");

        Assert.False(communication.IsDeliveryBlocked("a", "b"));
        Assert.True(communication.IsDeliveryBlocked("b", "c"));

        communication.PartitionNode("a");
        communication.HealAll();

        Assert.False(communication.IsDeliveryBlocked("a", "b"));
        Assert.False(communication.IsDeliveryBlocked("b", "c"));
    }

    /// <summary>
    /// Writers publish with a compare-and-swap loop, so concurrent writers must not lose each
    /// other's changes.
    /// </summary>
    [Fact]
    public async Task ConcurrentWriters_KeepEveryBlock()
    {
        InMemoryCommunication communication = new();
        const int writers = 8;
        const int linksPerWriter = 200;

        await Task.WhenAll(Enumerable.Range(0, writers).Select(w => Task.Run(() =>
        {
            for (int i = 0; i < linksPerWriter; i++)
                communication.BlockLink($"w{w}", $"t{i}");
        }, TestContext.Current.CancellationToken)));

        for (int w = 0; w < writers; w++)
            for (int i = 0; i < linksPerWriter; i++)
                Assert.True(communication.IsDeliveryBlocked($"w{w}", $"t{i}"));
    }

    // ── Cluster behavior ──────────────────────────────────────────────────────

    /// <summary>
    /// Cutting the link between the two followers only: each follower still reaches the leader, so
    /// the leader stays, and writes still reach both followers.
    /// </summary>
    [Fact]
    public async Task FollowerToFollowerCut_LeaderStays_AndWritesReachBothFollowers()
    {
        (InMemoryCommunication communication, IRaft[] nodes) = await StartClusterAsync();

        try
        {
            IRaft leader = await GetStableLeaderAsync(nodes);
            IRaft[] followers = nodes.Where(n => n != leader).ToArray();

            communication.BlockLinkBothWays(followers[0].GetLocalEndpoint(), followers[1].GetLocalEndpoint());

            for (int i = 0; i < 5; i++)
            {
                RaftReplicationResult result = await leader.ReplicateLogs(Partition, "Cut", "x"u8.ToArray(),
                    cancellationToken: TestContext.Current.CancellationToken);
                Assert.Equal(RaftOperationStatus.Success, result.Status);
            }

            long leaderMax = leader.WalAdapter.GetMaxLog(Partition);

            await WaitForConditionAsync(
                () => followers.All(f => f.WalAdapter.GetMaxLog(Partition) == leaderMax),
                TestContext.Current.CancellationToken);

            // Several election timeouts later, the same node still leads.
            await Task.Delay(1_500, TestContext.Current.CancellationToken);

            Assert.True(await leader.AmILeaderQuick(Partition));
            Assert.Equal(1, await CountLeadersAsync(nodes));
        }
        finally
        {
            await StopClusterAsync(communication, nodes);
        }
    }

    /// <summary>
    /// Link blocks that cut the leader from both followers: the majority elects a new leader, and the
    /// old leader cannot commit. After the links heal, the old leader steps down and catches up.
    /// </summary>
    [Fact]
    public async Task LeaderCutFromBothFollowers_MajorityElects_OldLeaderCannotCommit_ThenCatchesUp()
    {
        (InMemoryCommunication communication, IRaft[] nodes) = await StartClusterAsync();

        try
        {
            IRaft oldLeader = await GetStableLeaderAsync(nodes);
            IRaft[] majority = nodes.Where(n => n != oldLeader).ToArray();

            RaftReplicationResult seed = await oldLeader.ReplicateLogs(Partition, "Seed", "x"u8.ToArray(),
                cancellationToken: TestContext.Current.CancellationToken);
            Assert.Equal(RaftOperationStatus.Success, seed.Status);

            foreach (IRaft follower in majority)
                communication.BlockLinkBothWays(oldLeader.GetLocalEndpoint(), follower.GetLocalEndpoint());

            // The two followers still reach each other, so they elect one of them.
            IRaft? newLeader = null;
            await WaitForConditionAsync(async () =>
            {
                newLeader = await GetLeaderAsync(majority);
                return newLeader is not null;
            }, TestContext.Current.CancellationToken);

            Assert.NotNull(newLeader);
            Assert.NotEqual(oldLeader.GetLocalEndpoint(), newLeader.GetLocalEndpoint());

            // The old leader is alone: a write on it must not commit.
            long oldLeaderCommitBefore = oldLeader.GetCommitIndex(Partition);
            RaftOperationStatus minorityStatus = await TryReplicateAsync(oldLeader, TimeSpan.FromSeconds(2));
            Assert.NotEqual(RaftOperationStatus.Success, minorityStatus);
            Assert.Equal(oldLeaderCommitBefore, oldLeader.GetCommitIndex(Partition));

            // The majority side commits.
            for (int i = 0; i < 3; i++)
            {
                RaftReplicationResult result = await newLeader.ReplicateLogs(Partition, "Majority", "y"u8.ToArray(),
                    cancellationToken: TestContext.Current.CancellationToken);
                Assert.Equal(RaftOperationStatus.Success, result.Status);
            }

            communication.HealAll();

            // The old leader learns the higher term, steps down, and receives the majority's writes.
            await WaitForConditionAsync(
                async () => !await oldLeader.AmILeaderQuick(Partition),
                TestContext.Current.CancellationToken);

            await WaitForConditionAsync(async () =>
            {
                IRaft? current = await GetLeaderAsync(nodes);
                if (current is null)
                    return false;

                long leaderCommit = current.GetCommitIndex(Partition);
                return oldLeader.GetCommitIndex(Partition) >= leaderCommit
                    && oldLeader.WalAdapter.GetMaxLog(Partition) >= leaderCommit;
            }, TestContext.Current.CancellationToken, timeoutMs: 20_000);

            Assert.Equal(1, await CountLeadersAsync(nodes));
        }
        finally
        {
            await StopClusterAsync(communication, nodes);
        }
    }

    /// <summary>
    /// A follower that hears the leader but cannot answer it: it stays a follower, and the leader
    /// commits with the other follower.
    /// </summary>
    [Fact]
    public async Task OneWay_FollowerCannotReachLeader_LeaderStaysAndCommits()
    {
        (InMemoryCommunication communication, IRaft[] nodes) = await StartClusterAsync();

        try
        {
            IRaft leader = await GetStableLeaderAsync(nodes);
            IRaft muted = nodes.First(n => n != leader);
            IRaft other = nodes.First(n => n != leader && n != muted);

            communication.BlockLink(muted.GetLocalEndpoint(), leader.GetLocalEndpoint());

            for (int i = 0; i < 5; i++)
            {
                RaftReplicationResult result = await leader.ReplicateLogs(Partition, "OneWay", "x"u8.ToArray(),
                    cancellationToken: TestContext.Current.CancellationToken);
                Assert.Equal(RaftOperationStatus.Success, result.Status);
            }

            long leaderMax = leader.WalAdapter.GetMaxLog(Partition);

            await WaitForConditionAsync(
                () => other.WalAdapter.GetMaxLog(Partition) == leaderMax,
                TestContext.Current.CancellationToken);

            await Task.Delay(1_500, TestContext.Current.CancellationToken);

            Assert.True(await leader.AmILeaderQuick(Partition));
            Assert.False(await muted.AmILeaderQuick(Partition));

            communication.UnblockLink(muted.GetLocalEndpoint(), leader.GetLocalEndpoint());

            await WaitForConditionAsync(
                () => muted.WalAdapter.GetMaxLog(Partition) == leaderMax,
                TestContext.Current.CancellationToken);
        }
        finally
        {
            await StopClusterAsync(communication, nodes);
        }
    }

    /// <summary>
    /// A follower that cannot hear the leader but can still send to it. It starts pre-vote rounds,
    /// but the other voter still hears the leader and refuses them, so the leader stays. The follower
    /// gets no new entries while blocked, and catches up after the block is removed. See the liveness
    /// note on <see cref="InMemoryCommunication.BlockLink"/>.
    /// </summary>
    [Fact]
    public async Task OneWay_LeaderCannotReachFollower_PreVoteKeepsLeader_FollowerCatchesUpAfterUnblock()
    {
        (InMemoryCommunication communication, IRaft[] nodes) = await StartClusterAsync();

        try
        {
            IRaft leader = await GetStableLeaderAsync(nodes);
            IRaft deaf = nodes.First(n => n != leader);

            communication.BlockLink(leader.GetLocalEndpoint(), deaf.GetLocalEndpoint());

            for (int i = 0; i < 5; i++)
            {
                RaftReplicationResult result = await leader.ReplicateLogs(Partition, "Deaf", "x"u8.ToArray(),
                    cancellationToken: TestContext.Current.CancellationToken);
                Assert.Equal(RaftOperationStatus.Success, result.Status);
            }

            long leaderMax = leader.WalAdapter.GetMaxLog(Partition);

            // Several election timeouts: the deaf follower campaigns (pre-vote) and loses every round.
            long deadline = Environment.TickCount64 + 3_000;
            while (Environment.TickCount64 < deadline)
            {
                IRaft? current = await GetLeaderAsync(nodes);
                Assert.True(
                    current is null || current == leader,
                    $"leadership moved to {current?.GetLocalEndpoint()} while only {leader.GetLocalEndpoint()} -> {deaf.GetLocalEndpoint()} was blocked");

                await Task.Delay(50, TestContext.Current.CancellationToken);
            }

            Assert.True(await leader.AmILeaderQuick(Partition));
            Assert.True(deaf.WalAdapter.GetMaxLog(Partition) < leaderMax);

            communication.UnblockLink(leader.GetLocalEndpoint(), deaf.GetLocalEndpoint());

            await WaitForConditionAsync(
                () => deaf.WalAdapter.GetMaxLog(Partition) == leaderMax,
                TestContext.Current.CancellationToken);

            Assert.True(await leader.AmILeaderQuick(Partition));
        }
        finally
        {
            await StopClusterAsync(communication, nodes);
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private async Task<(InMemoryCommunication, IRaft[])> StartClusterAsync()
    {
        InMemoryCommunication communication = new();

        IRaft node1 = MakeNode(communication, 1, [new("localhost:8602"), new("localhost:8603")]);
        IRaft node2 = MakeNode(communication, 2, [new("localhost:8601"), new("localhost:8603")]);
        IRaft node3 = MakeNode(communication, 3, [new("localhost:8601"), new("localhost:8602")]);
        IRaft[] nodes = [node1, node2, node3];

        communication.SetNodes(new Dictionary<string, IRaft>
        {
            { "localhost:8601", node1 },
            { "localhost:8602", node2 },
            { "localhost:8603", node3 }
        });

        await Task.WhenAll(nodes.Select(n => n.UpdateNodes()));

        await Task.WhenAll(nodes.Select(n => n.JoinCluster(TestContext.Current.CancellationToken)));

        return (communication, nodes);
    }

    private static async Task StopClusterAsync(InMemoryCommunication communication, IRaft[] nodes)
    {
        communication.HealAll();

        foreach (IRaft node in nodes)
            await node.LeaveCluster(true, CancellationToken.None);
    }

    private IRaft MakeNode(InMemoryCommunication communication, int nodeId, IEnumerable<RaftNode> peers)
    {
        RaftConfiguration config = new()
        {
            NodeName = $"node{nodeId}",
            NodeId = nodeId,
            Host = "localhost",
            Port = 8600 + nodeId,
            InitialPartitions = 1,
            HeartbeatInterval = TimeSpan.FromMilliseconds(50),
            RecentHeartbeat = TimeSpan.FromMilliseconds(25),
            VotingTimeout = TimeSpan.FromMilliseconds(250),
            CheckLeaderInterval = TimeSpan.FromMilliseconds(25),
            UpdateNodesInterval = TimeSpan.FromMilliseconds(100),
            TimerInitialDelay = TimeSpan.FromMilliseconds(25),
            StartElectionTimeout = 500,
            EndElectionTimeout = 1000,
            EnableQuiescence = false,
        };

        return new RaftManager(
            config,
            new StaticDiscovery(peers.ToList()),
            new InMemoryWAL(logger),
            communication,
            new HybridLogicalClock(),
            logger);
    }

    private static async Task<IRaft> GetStableLeaderAsync(IRaft[] nodes)
    {
        string endpoint = await nodes[0].WaitForLeaderStableAsync(
            Partition, TimeSpan.FromMilliseconds(300), TestContext.Current.CancellationToken);

        IRaft? leader = null;
        await WaitForConditionAsync(async () =>
        {
            leader = await GetLeaderAsync(nodes);
            return leader is not null && leader.GetLocalEndpoint() == endpoint;
        }, TestContext.Current.CancellationToken);

        return leader!;
    }

    /// <summary>
    /// Tries one write and returns its status. A write that cannot reach a quorum may fail or may
    /// wait until the timeout; both count as "did not commit".
    /// </summary>
    private static async Task<RaftOperationStatus> TryReplicateAsync(IRaft node, TimeSpan timeout)
    {
        using CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        cts.CancelAfter(timeout);

        try
        {
            RaftReplicationResult result = await node.ReplicateLogs(Partition, "Minority", "z"u8.ToArray(),
                cancellationToken: cts.Token);
            return result.Status;
        }
        catch (OperationCanceledException) when (!TestContext.Current.CancellationToken.IsCancellationRequested)
        {
            return RaftOperationStatus.Errored;
        }
    }

    private static async Task<IRaft?> GetLeaderAsync(IRaft[] nodes)
    {
        foreach (IRaft node in nodes)
        {
            if (await node.AmILeaderQuick(Partition).ConfigureAwait(false))
                return node;
        }

        return null;
    }

    private static async Task<int> CountLeadersAsync(IRaft[] nodes)
    {
        int count = 0;
        foreach (IRaft node in nodes)
        {
            if (await node.AmILeaderQuick(Partition).ConfigureAwait(false))
                count++;
        }

        return count;
    }

    private static async Task WaitForConditionAsync(Func<bool> condition, CancellationToken cancellationToken, int timeoutMs = 15_000)
    {
        await WaitForConditionAsync(() => Task.FromResult(condition()), cancellationToken, timeoutMs).ConfigureAwait(false);
    }

    private static async Task WaitForConditionAsync(Func<Task<bool>> condition, CancellationToken cancellationToken, int timeoutMs = 15_000)
    {
        timeoutMs = TestTimeouts.Scale(timeoutMs);
        long startMs = Environment.TickCount64;

        while (Environment.TickCount64 - startMs < timeoutMs)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (await condition().ConfigureAwait(false))
                return;

            await Task.Delay(25, cancellationToken).ConfigureAwait(false);
        }

        Assert.Fail($"Condition was not met within {timeoutMs} ms.");
    }
}
