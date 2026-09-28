using System.Diagnostics;
using Kommander.Communication.Memory;
using Kommander.Consensus;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.Tests.Communication;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.Scheduling;

/// <summary>
/// The ack fast-path re-supply must not re-ship what a live commit broadcast already carries.
///
/// <para>Under write load the slower follower's propose ack reports a commit frontier one batch
/// below the leader's just-advanced <c>LocalCommittedIndex</c>: the other follower's ack made the
/// quorum while this ack was on its way, and the commit broadcast follows the leader's commit-marker
/// write. The gate counts that broadcast from the moment the marker is queued. A batch larger than
/// <c>BackfillThreshold</c> made both re-supply branches ship a backfill batch per proposal, a
/// third <c>AppendLogs</c> per follower on top of the propose and commit broadcasts (3.1-3.2 frames
/// per proposal per follower in Kommander.Benchmark, three follower appends per proposal on the
/// CamusDB cluster).</para>
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
public class TestResupplyInFlightGate
{
    private const string Peer = "peer:9001";
    private static readonly TimeSpan Freshness = TimeSpan.FromMilliseconds(500);

    // ── ReplicationTracker.AreResolutionsInFlight ─────────────────────────────

    [Fact]
    public void Gap_CoveredByOneFreshBroadcast_IsInFlight()
    {
        ReplicationTracker tracker = new(null!);
        long now = Stopwatch.GetTimestamp();

        tracker.RecordResolutionShipped(Peer, 101, 250, now);

        Assert.True(tracker.AreResolutionsInFlight(Peer, 100, 250, now, Freshness));
    }

    [Fact]
    public void Gap_CoveredByConsecutiveBroadcasts_InAnyShipOrder_IsInFlight()
    {
        ReplicationTracker tracker = new(null!);
        long now = Stopwatch.GetTimestamp();

        // Pipelined proposals can reach quorum, and so commit, out of order.
        tracker.RecordResolutionShipped(Peer, 151, 200, now);
        tracker.RecordResolutionShipped(Peer, 101, 150, now);

        Assert.True(tracker.AreResolutionsInFlight(Peer, 100, 200, now, Freshness));
    }

    [Fact]
    public void Gap_WithAnUnshippedRange_IsNotInFlight()
    {
        ReplicationTracker tracker = new(null!);
        long now = Stopwatch.GetTimestamp();

        // 151..160 was never broadcast to this peer (withheld while it reported saturation, or
        // committed by an inherited re-commit that broadcasts nothing): only a backfill carries it.
        tracker.RecordResolutionShipped(Peer, 101, 150, now);
        tracker.RecordResolutionShipped(Peer, 161, 200, now);

        Assert.False(tracker.AreResolutionsInFlight(Peer, 100, 200, now, Freshness));
    }

    [Fact]
    public void Gap_BelowTheFirstBroadcast_IsNotInFlight()
    {
        ReplicationTracker tracker = new(null!);
        long now = Stopwatch.GetTimestamp();

        tracker.RecordResolutionShipped(Peer, 101, 150, now);

        // A peer that stalled at 80 needs 81..100, which no live broadcast carries.
        Assert.False(tracker.AreResolutionsInFlight(Peer, 80, 150, now, Freshness));
    }

    [Fact]
    public void Gap_BeyondTheLastBroadcast_IsNotInFlight()
    {
        ReplicationTracker tracker = new(null!);
        long now = Stopwatch.GetTimestamp();

        tracker.RecordResolutionShipped(Peer, 101, 150, now);

        Assert.False(tracker.AreResolutionsInFlight(Peer, 100, 160, now, Freshness));
    }

    [Fact]
    public void Broadcast_OlderThanTheFreshnessWindow_NoLongerCounts()
    {
        ReplicationTracker tracker = new(null!);
        long shipped = Stopwatch.GetTimestamp();
        long later = shipped + (long)(Freshness.TotalSeconds * Stopwatch.Frequency) + 1;

        // The peer should have answered a broadcast this old: it was dropped at the outbound byte
        // cap or lost in a restart, and the re-supply is the repair.
        tracker.RecordResolutionShipped(Peer, 101, 150, shipped);

        Assert.True(tracker.AreResolutionsInFlight(Peer, 100, 150, shipped, Freshness));
        Assert.False(tracker.AreResolutionsInFlight(Peer, 100, 150, later, Freshness));
    }

    [Fact]
    public void Ack_PrunesCoveredBroadcasts_AndAPeerWithNoGapIsTriviallyCovered()
    {
        ReplicationTracker tracker = new(null!);
        long now = Stopwatch.GetTimestamp();

        tracker.RecordResolutionShipped(Peer, 101, 150, now);
        tracker.PruneResolutionShipments(Peer, 150);

        Assert.False(tracker.AreResolutionsInFlight(Peer, 100, 150, now, Freshness));
        Assert.True(tracker.AreResolutionsInFlight(Peer, 150, 150, now, Freshness));
    }

    [Fact]
    public void LeaderChange_ClearsBroadcasts()
    {
        ReplicationTracker tracker = new(null!);
        long now = Stopwatch.GetTimestamp();

        tracker.RecordResolutionShipped(Peer, 101, 150, now);
        tracker.ClearProgressKeepingCommitFrontiers();

        Assert.False(tracker.AreResolutionsInFlight(Peer, 100, 150, now, Freshness));
    }

    // ── end to end ────────────────────────────────────────────────────────────

    /// <summary>
    /// Serial proposals of 16 entries on a three-voter in-memory cluster: each follower gets the
    /// propose and the commit broadcast, and nothing else in steady state. Before the gate, the
    /// slower follower's propose ack re-supplied the batch on nearly every proposal (≈ 3.2 frames).
    /// </summary>
    [Fact]
    public async Task SteadyStateProposals_ShipTwoFramesPerProposalPerFollower()
    {
        const string ep1 = "localhost:9311";
        const string ep2 = "localhost:9312";
        const string ep3 = "localhost:9313";
        const int partitionId = 1;
        const int proposalCount = 200;
        const int batchSize = 16;

        CancellationToken ct = TestContext.Current.CancellationToken;
        ILogger<IRaft> logger = NullLoggerFactory.Instance.CreateLogger<IRaft>();

        InMemoryCommunication inner = new();
        AppendCountingCommunication counting = new(inner);

        RaftManager n1 = MakeNode(1, ep1, [ep2, ep3], counting, logger);
        RaftManager n2 = MakeNode(2, ep2, [ep1, ep3], counting, logger);
        RaftManager n3 = MakeNode(3, ep3, [ep1, ep2], counting, logger);
        RaftManager[] nodes = [n1, n2, n3];

        try
        {
            counting.SetNodes(new() { { ep1, n1 }, { ep2, n2 }, { ep3, n3 } });

            await Task.WhenAll(n1.UpdateNodes(), n2.UpdateNodes(), n3.UpdateNodes());
            await Task.WhenAll(n1.JoinCluster(ct), n2.JoinCluster(ct), n3.JoinCluster(ct));

            RaftManager leader = await GetLeaderAsync(partitionId, nodes, ct);
            string[] followers = nodes.Where(n => n != leader).Select(n => n.GetLocalEndpoint()).ToArray();

            byte[][] batch = Enumerable.Range(0, batchSize).Select(_ => new byte[280]).ToArray();

            // Warm-up: the leadership barrier and first catch-up ride backfill legitimately.
            for (int i = 0; i < 20; i++)
                Assert.True((await leader.ReplicateLogs(partitionId, "test", batch, cancellationToken: ct)).Success);

            await Task.Delay(100, ct);

            long before = followers.Sum(f => counting.FramesTo(partitionId, f));

            for (int i = 0; i < proposalCount; i++)
                Assert.True((await leader.ReplicateLogs(partitionId, "test", batch, cancellationToken: ct)).Success);

            // Let the last commit broadcasts and their acks land.
            await Task.Delay(100, ct);

            long frames = followers.Sum(f => counting.FramesTo(partitionId, f)) - before;
            double perProposalPerFollower = frames / (double)(proposalCount * followers.Length);

            Assert.True(perProposalPerFollower <= 2.2,
                $"{perProposalPerFollower:F2} entry-carrying frames per proposal per follower; the steady state is the " +
                "propose and the commit broadcast (2.0). More means the ack fast path re-supplied batches the commit " +
                "broadcast already carried.");
        }
        finally
        {
            foreach (RaftManager node in nodes)
                await node.LeaveCluster(true, CancellationToken.None);
        }
    }

    private static RaftManager MakeNode(int id, string endpoint, string[] peers, AppendCountingCommunication comm, ILogger<IRaft> logger)
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
        };

        return new RaftManager(
            config,
            new StaticDiscovery(peers.Select(p => new RaftNode(p)).ToList()),
            new InMemoryWAL(logger),
            comm,
            new HybridLogicalClock(),
            logger);
    }

    private static async Task<RaftManager> GetLeaderAsync(int partitionId, RaftManager[] nodes, CancellationToken ct)
    {
        for (int attempt = 0; attempt < 500; attempt++)
        {
            foreach (RaftManager node in nodes)
            {
                if (await node.AmILeaderQuick(partitionId))
                    return node;
            }

            await Task.Delay(10, ct);
        }

        throw new InvalidOperationException($"No leader elected for partition {partitionId}");
    }
}
