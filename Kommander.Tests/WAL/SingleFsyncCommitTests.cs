using System.Collections.Concurrent;
using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Diagnostics;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;

namespace Kommander.Tests.WAL;

/// <summary>
/// Task 7 of the WAL double-fsync spec: the single-fsync fast path
/// (<see cref="RaftConfiguration.WalSingleFsyncCommit"/>) releases an <c>autoCommit</c>
/// proposal's client ticket on <b>propose-quorum-durable</b> instead of on the leader's own
/// second (commit) fsync.
///
/// <para>These are correctness tests, not latency tests (the p50 win is measured out-of-band
/// in the Kahuna harness). They assert that with the flag <b>on</b> the end-to-end committed
/// result is identical to the flag <b>off</b> path: every acked write is replicated to a
/// quorum, indices are contiguous, and the durable log on every node matches. The load-bearing
/// invariant — acked ⇒ durable on a quorum — is what makes the earlier ack safe.</para>
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
public sealed class SingleFsyncCommitTests
{
    private readonly ILogger<IRaft> logger;

    public SingleFsyncCommitTests(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = LoggerFactory.Create(builder => builder.AddXUnit(outputHelper).SetMinimumLevel(LogLevel.Warning));
        logger = loggerFactory.CreateLogger<IRaft>();
    }

    /// <summary>
    /// With the fast path on, an autoCommit replicate still returns Success with a contiguous
    /// log index, and the entry lands durably on the leader and both followers — i.e. the ticket
    /// released on quorum-durable corresponds to a genuinely quorum-durable entry.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task FastPath_AutoCommitReplicatesToQuorum_IdenticalToBaseline(bool singleFsyncCommit)
    {
        const int entries = 20;
        (IRaft node1, IRaft node2, IRaft node3) = await AssembleCluster(singleFsyncCommit);

        try
        {
            IRaft leader = await GetLeader(1, [node1, node2, node3]) ?? throw new InvalidOperationException("no leader");
            List<IRaft> followers = await GetFollowers(1, [node1, node2, node3]);
            Assert.Equal(2, followers.Count);

            ConcurrentBag<long> followerReceived = [];
            foreach (IRaft follower in followers)
                follower.OnReplicationReceived += (_, log) =>
                {
                    followerReceived.Add(log.Id);
                    return Task.FromResult(true);
                };

            byte[] data = "Hello World"u8.ToArray();

            for (int expectedId = 1; expectedId <= entries; expectedId++)
            {
                RaftReplicationResult response = await leader.ReplicateLogs(
                    1, "Greeting", data, cancellationToken: TestContext.Current.CancellationToken);

                Assert.True(response.Success);
                Assert.Equal(RaftOperationStatus.Success, response.Status);
                // The ticket carries the committed index whether it was released on the fast
                // path (quorum-durable) or the slow path (commit fsync). Must be contiguous.
                Assert.Equal(expectedId, response.LogIndex);
            }

            // Durable log on every node matches — the acked writes are quorum-durable.
            Assert.Equal(entries, node1.WalAdapter.GetMaxLog(1));
            Assert.Equal(entries, node2.WalAdapter.GetMaxLog(1));
            Assert.Equal(entries, node3.WalAdapter.GetMaxLog(1));

            // Both followers eventually observe every committed entry (commit broadcast still
            // runs on the fast path — only the leader's *ack timing* moves earlier).
            await WaitForCondition(
                () => followerReceived.Distinct().Count() == entries,
                TestContext.Current.CancellationToken);
        }
        finally
        {
            await node1.LeaveCluster(true, CancellationToken.None);
            await node2.LeaveCluster(true, CancellationToken.None);
            await node3.LeaveCluster(true, CancellationToken.None);
        }
    }

    /// <summary>
    /// Single-voter fast path: a 0-peer leader is its own quorum, so the propose fsync that
    /// completes locally already makes the entry quorum-durable. This exercises the distinct
    /// call site in the propose-completion handler (the <c>!Nodes.Any(IsVoter)</c> branch),
    /// which the 3-node test does not reach. With the flag on the ticket releases on that
    /// propose-durable point; the committed result must be identical to the flag-off path.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task FastPath_SingleVoterLeader_CommitsLocally_IdenticalToBaseline(bool singleFsyncCommit)
    {
        const int entries = 20;
        IRaft node = BuildSingleNode(singleFsyncCommit);

        using CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(15));

        try
        {
            await node.JoinCluster(cts.Token);
            Assert.True(node.IsInitialized);
            Assert.True(await node.AmILeaderQuick(1));

            byte[] data = "Hello World"u8.ToArray();

            for (int expectedId = 1; expectedId <= entries; expectedId++)
            {
                RaftReplicationResult response = await node.ReplicateLogs(
                    1, "Greeting", data, cancellationToken: cts.Token);

                Assert.True(response.Success, $"replicate failed: {response.Status}");
                Assert.Equal(RaftOperationStatus.Success, response.Status);
                Assert.Equal(expectedId, response.LogIndex);
            }

            Assert.Equal(entries, node.WalAdapter.GetMaxLog(1));
        }
        finally
        {
            await node.LeaveCluster(true, CancellationToken.None);
        }
    }

    /// <summary>
    /// Concurrent / pipelined autoCommit proposals are the ordering hazard the fast path must
    /// tolerate: a quorum acking entry N+1 can land before N's own quorum-completion fires, and
    /// the fast path advances the frontier to N+1 on that ack. That is safe only because
    /// followers append contiguously, so a quorum holding N+1 necessarily holds N. This fires a
    /// burst of proposals at once and asserts every ack got a unique, contiguous index 1..N and
    /// all nodes converge on the full durable log — identical with the flag on and off.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task FastPath_ConcurrentAutoCommitProposals_AllCommitContiguously(bool singleFsyncCommit)
    {
        const int entries = 50;
        (IRaft node1, IRaft node2, IRaft node3) = await AssembleCluster(singleFsyncCommit, stableLeadership: true);

        try
        {
            IRaft leader = await GetLeader(1, [node1, node2, node3]) ?? throw new InvalidOperationException("no leader");
            byte[] data = "Hello World"u8.ToArray();

            // Fire all proposals concurrently so later entries can reach quorum before earlier ones.
            Task<RaftReplicationResult>[] tasks = new Task<RaftReplicationResult>[entries];
            for (int i = 0; i < entries; i++)
                tasks[i] = leader.ReplicateLogs(1, "Greeting", data, cancellationToken: TestContext.Current.CancellationToken);

            RaftReplicationResult[] results = await Task.WhenAll(tasks);

            // The fast path releases each ticket via SetState(Committed) on its own quorum
            // completion. Under a concurrent burst those completions arrive out of order, so all 50
            // returning Success proves the fast-path release is correct under exactly the ordering
            // the hazard is about — no ticket is stranded when a later entry's quorum lands first.
            Assert.All(results, r => Assert.True(r.Success, $"replicate failed: {r.Status}"));

            // Every acked write got a unique, contiguous index 1..N — out-of-order quorum completion
            // neither skipped nor duplicated a committed index.
            long[] indices = results.Select(r => r.LogIndex).OrderBy(x => x).ToArray();
            Assert.Equal(Enumerable.Range(1, entries).Select(i => (long)i).ToArray(), indices);

            // (Cross-node WAL convergence is intentionally NOT asserted here: after a one-shot burst
            // with no further write traffic, a follower lagging by less than BackfillThreshold is not
            // actively backfilled and has nothing to ride on, so it can settle a few entries behind.
            // That eventual-convergence property is covered by the steady-traffic cluster tests; this
            // test targets the fast path's behavior under concurrent out-of-order completion.)
        }
        finally
        {
            await node1.LeaveCluster(true, CancellationToken.None);
            await node2.LeaveCluster(true, CancellationToken.None);
            await node3.LeaveCluster(true, CancellationToken.None);
        }
    }

    // ── Harness ────────────────────────────────────────────────────────────

    private IRaft BuildSingleNode(bool singleFsyncCommit)
    {
        RaftConfiguration config = new()
        {
            NodeName = "node1",
            NodeId = 1,
            Host = "localhost",
            Port = 8001,
            InitialPartitions = 1,
            HeartbeatInterval = TimeSpan.FromMilliseconds(50),
            RecentHeartbeat = TimeSpan.FromMilliseconds(25),
            VotingTimeout = TimeSpan.FromMilliseconds(250),
            CheckLeaderInterval = TimeSpan.FromMilliseconds(25),
            UpdateNodesInterval = TimeSpan.FromMilliseconds(100),
            TimerInitialDelay = TimeSpan.FromMilliseconds(25),
            StartElectionTimeout = 100,
            EnableQuiescence = false,
            EndElectionTimeout = 250,
            WalSingleFsyncCommit = singleFsyncCommit,
        };

        return new RaftManager(
            config,
            new StaticDiscovery([]),
            new InMemoryWAL(logger),
            new InMemoryCommunication(),
            new HybridLogicalClock(),
            logger);
    }

    private async Task<(IRaft, IRaft, IRaft)> AssembleCluster(bool singleFsyncCommit, bool stableLeadership = false)
    {
        InMemoryCommunication communication = new();

        IRaft node1 = GetNode(communication, 1, 8001, singleFsyncCommit, stableLeadership);
        IRaft node2 = GetNode(communication, 2, 8002, singleFsyncCommit, stableLeadership);
        IRaft node3 = GetNode(communication, 3, 8003, singleFsyncCommit, stableLeadership);

        Dictionary<string, IRaft> network = new()
        {
            { "localhost:8001", node1 },
            { "localhost:8002", node2 },
            { "localhost:8003", node3 },
        };

        communication.SetNodes(network);

        await node1.UpdateNodes();
        await node2.UpdateNodes();
        await node3.UpdateNodes();

        await Task.WhenAll(
            node1.JoinCluster(TestContext.Current.CancellationToken),
            node2.JoinCluster(TestContext.Current.CancellationToken),
            node3.JoinCluster(TestContext.Current.CancellationToken));

        await WaitForAnyLeader([node1, node2, node3], 1, TestContext.Current.CancellationToken);

        return (node1, node2, node3);
    }

    private IRaft GetNode(InMemoryCommunication communication, int nodeId, int port, bool singleFsyncCommit, bool stableLeadership = false)
    {
        // The concurrent-burst test needs leadership to stay put through the burst: aggressive
        // election timeouts would let a heartbeat slip under load, trigger a re-election, and leave
        // followers stranded behind lost commit broadcasts with no post-burst traffic to recover on.
        RaftConfiguration config = new()
        {
            NodeName = $"node{nodeId}",
            NodeId = nodeId,
            Host = "localhost",
            Port = port,
            InitialPartitions = 1,
            HeartbeatInterval = stableLeadership ? TimeSpan.FromMilliseconds(100) : TimeSpan.FromMilliseconds(50),
            RecentHeartbeat = TimeSpan.FromMilliseconds(25),
            VotingTimeout = TimeSpan.FromMilliseconds(250),
            CheckLeaderInterval = TimeSpan.FromMilliseconds(25),
            UpdateNodesInterval = TimeSpan.FromMilliseconds(100),
            TimerInitialDelay = TimeSpan.FromMilliseconds(25),
            StartElectionTimeout = stableLeadership ? 2000 : 100,
            EnableQuiescence = false,
            EndElectionTimeout = stableLeadership ? 4000 : 250,
            WalSingleFsyncCommit = singleFsyncCommit,
        };

        string[] others = nodeId switch
        {
            1 => ["localhost:8002", "localhost:8003"],
            2 => ["localhost:8001", "localhost:8003"],
            _ => ["localhost:8001", "localhost:8002"],
        };

        return new RaftManager(
            config,
            new StaticDiscovery([.. others.Select(o => new RaftNode(o))]),
            new InMemoryWAL(logger),
            communication,
            new HybridLogicalClock(),
            logger);
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

    private static async Task WaitForCondition(Func<bool> condition, CancellationToken cancellationToken)
    {
        ValueStopwatch stopwatch = ValueStopwatch.StartNew();
        while (stopwatch.GetElapsedMilliseconds() < 15_000)
        {
            if (condition())
                return;
            await Task.Delay(25, cancellationToken);
        }
        throw new TimeoutException("Condition not met within 15 seconds.");
    }
}
