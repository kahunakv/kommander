using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Diagnostics;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests;

/// <summary>
/// The pure decision of <see cref="JoinCatchUpProgress"/>: what counts as catch-up progress for a
/// seed-joining learner, and when a lack of it is a stall. No cluster, so no collection.
/// </summary>
public sealed class TestJoinCatchUpProgress
{
    [Fact]
    public void FirstSample_IsTheBaseline_AndStartsTheStallClock()
    {
        JoinCatchUpProgress progress = new();

        Assert.True(progress.Observe([(0, 5), (1, 10)], 0, nowMs: 1_000));
        Assert.Equal(1_000, progress.LastProgressMs);
        Assert.Equal(0, progress.MillisecondsSinceProgress(1_000));
        Assert.Equal(4_000, progress.MillisecondsSinceProgress(5_000));
    }

    [Fact]
    public void UnchangedSamples_AreNotProgress()
    {
        JoinCatchUpProgress progress = new();
        progress.Observe([(0, 5), (1, 10)], 7, nowMs: 0);

        Assert.False(progress.Observe([(0, 5), (1, 10)], 7, nowMs: 500));
        Assert.False(progress.Observe([(0, 5), (1, 10)], 7, nowMs: 1_000));
        Assert.Equal(1_000, progress.MillisecondsSinceProgress(1_000));
    }

    [Fact]
    public void RisingCommitFrontier_IsProgress()
    {
        JoinCatchUpProgress progress = new();
        progress.Observe([(0, 5), (1, 10)], 0, nowMs: 0);

        Assert.True(progress.Observe([(0, 5), (1, 11)], 0, nowMs: 2_000));
        Assert.Equal(2_000, progress.LastProgressMs);
        Assert.Contains("partition 1", progress.LastProgress);
    }

    /// <summary>
    /// A frontier that dips and comes back to where it was is not new progress: frontiers are
    /// compared with the best value seen, so oscillation cannot keep a stalled join alive.
    /// </summary>
    [Fact]
    public void FrontierThatDipsAndRecovers_IsNotProgress()
    {
        JoinCatchUpProgress progress = new();
        progress.Observe([(1, 10)], 0, nowMs: 0);

        Assert.False(progress.Observe([(1, 8)], 0, nowMs: 500));
        Assert.False(progress.Observe([(1, 10)], 0, nowMs: 1_000));
        Assert.Equal(0, progress.LastProgressMs);
    }

    [Fact]
    public void NewPartition_IsProgress()
    {
        JoinCatchUpProgress progress = new();
        progress.Observe([(0, 5)], 0, nowMs: 0);

        Assert.True(progress.Observe([(0, 5), (3, -1)], 0, nowMs: 700));
        Assert.Contains("partition 3 appeared", progress.LastProgress);
    }

    /// <summary>
    /// A snapshot transfer and its import move no commit frontier until the install completes, so
    /// the receiver's marker must count on its own — in either direction, because the marker falls
    /// when an install completes and its consumed bytes leave the sum.
    /// </summary>
    [Fact]
    public void SnapshotMarkerChange_IsProgress_InEitherDirection()
    {
        JoinCatchUpProgress progress = new();
        progress.Observe([(1, -1)], 100, nowMs: 0);

        Assert.True(progress.Observe([(1, -1)], 4_096, nowMs: 1_000));
        Assert.True(progress.Observe([(1, -1)], 101, nowMs: 2_000));
        Assert.False(progress.Observe([(1, -1)], 101, nowMs: 3_000));
        Assert.Equal(2_000, progress.LastProgressMs);
    }

    /// <summary>
    /// An unreadable marker (null) is neither progress nor a new reference value; the next readable
    /// sample is compared with the last value that was actually read.
    /// </summary>
    [Fact]
    public void UnreadableSnapshotMarker_IsIgnored()
    {
        JoinCatchUpProgress progress = new();
        progress.Observe([(1, 3)], 50, nowMs: 0);

        Assert.False(progress.Observe([(1, 3)], null, nowMs: 500));
        Assert.False(progress.Observe([(1, 3)], 50, nowMs: 1_000));
        Assert.True(progress.Observe([(1, 3)], 51, nowMs: 1_500));
    }

    [Fact]
    public void DescribeFrontiers_ListsTheBestValuePerPartition()
    {
        JoinCatchUpProgress progress = new();
        progress.Observe([(2, 7), (0, 3)], 0, nowMs: 0);
        progress.Observe([(2, 6), (0, 4)], 0, nowMs: 10);

        Assert.Equal("p0=4 p2=7", progress.DescribeFrontiers());
    }
}

/// <summary>
/// Startup validation of the two join bounds.
/// </summary>
public sealed class TestJoinTimeoutValidation
{
    [Fact]
    public void JoinTimeout_MustBePositive()
    {
        RaftConfiguration config = new() { JoinTimeout = TimeSpan.Zero };

        RaftException ex = Assert.Throws<RaftException>(config.Validate);
        Assert.Contains("JoinTimeout", ex.Message);
    }

    /// <summary>
    /// A stall timeout not longer than the stable window plus one promotion tick would fail a
    /// healthy join on an idle cluster, where a caught-up learner shows no progress at all while the
    /// leader observes it. Refused at startup.
    /// </summary>
    [Fact]
    public void JoinPromotionStallTimeout_MustExceedStableWindowPlusPromotionTick()
    {
        RaftConfiguration config = new()
        {
            LearnerPromotionStableWindow = TimeSpan.FromSeconds(3),
            UpdateNodesInterval = TimeSpan.FromSeconds(5),
            JoinPromotionStallTimeout = TimeSpan.FromSeconds(8),
        };

        RaftException ex = Assert.Throws<RaftException>(config.Validate);
        Assert.Contains("JoinPromotionStallTimeout", ex.Message);

        config.JoinPromotionStallTimeout = TimeSpan.FromSeconds(9);
        config.Validate();
    }

    [Fact]
    public void Defaults_AreValid()
    {
        RaftConfiguration config = new();

        Assert.Equal(TimeSpan.FromSeconds(60), config.JoinTimeout);
        Assert.Equal(TimeSpan.FromMinutes(5), config.JoinPromotionStallTimeout);
        config.Validate();
    }
}

/// <summary>
/// The seed join's promotion wait against a real in-process cluster: it is bounded by catch-up
/// progress, not by <see cref="RaftConfiguration.JoinTimeout"/>, and a join that stalls withdraws the
/// node from the roster before it throws (Caraxes bank-backup-replace-node-soak, 2026-10-14: a
/// replacement node aborted on a fixed 60 s bound while still catching up, and stayed in the roster
/// as a dead Learner).
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
public sealed class TestJoinPromotionWait
{
    /// <summary>
    /// The learner receives every entry but none of its acknowledgements reaches the partition
    /// leader, so the leader sees it fall further behind on every write and cannot promote it. Its
    /// local commit frontier still moves, which is catch-up progress. The block is held well past
    /// both <see cref="RaftConfiguration.JoinTimeout"/> and
    /// <see cref="RaftConfiguration.JoinPromotionStallTimeout"/>; the join must neither time out nor
    /// stall, and must finish as a Voter once the acknowledgements flow again.
    /// <para>
    /// Before the fix every wait of the join shared one fixed bound from the start of the join, so a
    /// promotion that took longer than it threw however fast the learner was moving.
    /// </para>
    /// </summary>
    [Fact]
    public async Task LearnerThatKeepsProgressing_IsPromoted_AfterLongerThanBothBounds()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        InMemoryCommunication comm = new();

        TimeSpan joinTimeout = TimeSpan.FromSeconds(3);
        TimeSpan stallTimeout = TimeSpan.FromSeconds(4);

        void Configure(RaftConfiguration cfg)
        {
            // Lag 0: any write the leader has and the learner has not acknowledged blocks promotion.
            cfg.LearnerPromotionLag = 0;
            // Long enough to cover the gap before the learner's first acknowledgement of the user
            // partition: until then the promotion check skips that partition (a known gap in
            // CheckLearnerPromotionsAsync) and only this window keeps the learner from promotion.
            cfg.LearnerPromotionStableWindow = TimeSpan.FromSeconds(3);
            cfg.JoinTimeout = joinTimeout;
            cfg.JoinPromotionStallTimeout = stallTimeout;
        }

        RaftManager n1 = BuildNode(comm, 8761, 1, ["localhost:8762", "localhost:8763"], configure: Configure);
        RaftManager n2 = BuildNode(comm, 8762, 2, ["localhost:8761", "localhost:8763"], configure: Configure);
        RaftManager n3 = BuildNode(comm, 8763, 3, ["localhost:8761", "localhost:8762"], configure: Configure);
        RaftManager n4 = BuildNode(comm, 8764, 4, ["localhost:8761", "localhost:8762", "localhost:8763"], initialPartitions: 0, configure: Configure);
        const string joiner = "localhost:8764";

        comm.SetNodes(new Dictionary<string, IRaft>
        {
            ["localhost:8761"] = n1,
            ["localhost:8762"] = n2,
            ["localhost:8763"] = n3,
            [joiner] = n4,
        });

        using CancellationTokenSource writerCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        Task? writer = null;

        try
        {
            await Task.WhenAll(n1.JoinCluster(ct), n2.JoinCluster(ct), n3.JoinCluster(ct));
            await WaitForAsync(() => n1.IsInitialized && n2.IsInitialized && n3.IsInitialized, ct);

            RaftManager[] voters = [n1, n2, n3];
            int partitionId = (await FindLeaderAsync(voters, 0, ct)).Partitions.Keys.First(k => k != 0);
            RaftManager partitionLeader = await FindLeaderAsync(voters, partitionId, ct);

            // A steady write stream keeps the leader ahead of anything the learner acknowledged.
            writer = Task.Run(() => WriteContinuouslyAsync(partitionLeader, partitionId, writerCts.Token), CancellationToken.None);

            ValueStopwatch joinStopwatch = ValueStopwatch.StartNew();
            Task join = n4.JoinCluster(["localhost:8761"], ct);

            // The learner must have acknowledged the partition at least once before the block: a
            // learner that never acknowledged a partition is skipped by the promotion check.
            await WaitForAsync(async () =>
                await partitionLeader.GetFollowerCommittedIndexNullableAsync(partitionId, joiner) is not null, ct);

            comm.BlockLink(joiner, partitionLeader.LocalEndpoint);
            long frontierAtBlock = n4.GetCommitIndex(partitionId);

            // Hold the block past both bounds plus a margin.
            TimeSpan hold = joinTimeout + stallTimeout + TimeSpan.FromSeconds(2);
            await Task.Delay(hold, ct);

            Assert.False(join.IsCompleted, $"The join ended while the learner was still catching up: {join.Exception?.GetBaseException().Message}");
            Assert.Equal(System.ClusterMemberRole.Learner, n4.LocalRole);
            Assert.True(n4.GetCommitIndex(partitionId) > frontierAtBlock,
                $"The learner's commit frontier did not move during the block ({frontierAtBlock}); the test did not model a progressing catch-up");

            // Let the acknowledgements through and the leader settle: promotion follows.
            comm.UnblockLink(joiner, partitionLeader.LocalEndpoint);
            await writerCts.CancelAsync();

            await join.WaitAsync(TestTimeouts.Scale(TimeSpan.FromSeconds(30)), ct);

            Assert.Equal(System.ClusterMemberRole.Voter, n4.LocalRole);
            Assert.True(joinStopwatch.GetElapsedMilliseconds() > (long)(joinTimeout + stallTimeout).TotalMilliseconds);
        }
        finally
        {
            await writerCts.CancelAsync();
            if (writer is not null)
            {
                try
                {
                    await writer;
                }
                catch (OperationCanceledException)
                {
                    // The writer ends on cancellation.
                }
            }

            n1.Dispose(); n2.Dispose(); n3.Dispose(); n4.Dispose();
        }
    }

    /// <summary>
    /// The learner is cut off from every voter after it has acknowledged the partition, then falls
    /// behind by more than <see cref="RaftConfiguration.LearnerPromotionLag"/>. It can make no
    /// progress, so the join must fail with the stall reason after
    /// <see cref="RaftConfiguration.JoinPromotionStallTimeout"/> — and must take the node back out
    /// of the committed roster first, so the survivors do not keep a Learner that will never be
    /// promoted.
    /// <para>
    /// The withdrawal request reaches the leader through the cut (only the voters → joiner direction
    /// is blocked), but its reply cannot come back. The test heals the links once the leader has
    /// removed the node, which lets a retry confirm it, exactly as a reply lost in transit would be.
    /// </para>
    /// </summary>
    [Fact]
    public async Task LearnerThatStopsProgressing_FailsWithStallReason_AndIsWithdrawnFromRoster()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        InMemoryCommunication comm = new();

        void Configure(RaftConfiguration cfg)
        {
            cfg.LearnerPromotionLag = 5;
            // Covers the gap before the first acknowledgement, as in the progressing-learner test.
            cfg.LearnerPromotionStableWindow = TimeSpan.FromSeconds(3);
            cfg.JoinTimeout = TimeSpan.FromSeconds(30);
            cfg.JoinPromotionStallTimeout = TimeSpan.FromSeconds(4);
        }

        RaftManager n1 = BuildNode(comm, 8771, 1, ["localhost:8772", "localhost:8773"], configure: Configure);
        RaftManager n2 = BuildNode(comm, 8772, 2, ["localhost:8771", "localhost:8773"], configure: Configure);
        RaftManager n3 = BuildNode(comm, 8773, 3, ["localhost:8771", "localhost:8772"], configure: Configure);
        RaftManager n4 = BuildNode(comm, 8774, 4, ["localhost:8771", "localhost:8772", "localhost:8773"], initialPartitions: 0, configure: Configure);
        const string joiner = "localhost:8774";

        comm.SetNodes(new Dictionary<string, IRaft>
        {
            ["localhost:8771"] = n1,
            ["localhost:8772"] = n2,
            ["localhost:8773"] = n3,
            [joiner] = n4,
        });

        RaftManager[] voters = [n1, n2, n3];

        try
        {
            await Task.WhenAll(n1.JoinCluster(ct), n2.JoinCluster(ct), n3.JoinCluster(ct));
            await WaitForAsync(() => n1.IsInitialized && n2.IsInitialized && n3.IsInitialized, ct);

            int partitionId = (await FindLeaderAsync(voters, 0, ct)).Partitions.Keys.First(k => k != 0);
            RaftManager partitionLeader = await FindLeaderAsync(voters, partitionId, ct);

            for (int i = 0; i < 5; i++)
                await partitionLeader.ReplicateLogs(partitionId, "test", [1, 2, 3], cancellationToken: ct);

            Task join = n4.JoinCluster(["localhost:8771"], ct);

            // Caught up and acknowledged once, so the promotion check measures this partition.
            await WaitForAsync(async () =>
                await partitionLeader.GetFollowerCommittedIndexNullableAsync(partitionId, joiner) >= partitionLeader.GetCommitIndex(partitionId), ct);

            foreach (RaftManager voter in voters)
                comm.BlockLink(voter.LocalEndpoint, joiner);

            for (int i = 0; i < 20; i++)
                await partitionLeader.ReplicateLogs(partitionId, "test", [1, 2, 3], cancellationToken: ct);

            // Heal once the leader has removed the joiner, so the withdrawal's retry is answered.
            Task healer = Task.Run(async () =>
            {
                await WaitForAsync(() => voters.All(v => !InRoster(v, joiner)), ct, timeoutMs: 60_000);
                comm.HealAll();
            }, ct);

            TimeoutException ex = await Assert.ThrowsAsync<TimeoutException>(() => join.WaitAsync(TestTimeouts.Scale(TimeSpan.FromSeconds(60)), ct));

            Assert.Contains("no catch-up progress", ex.Message);
            Assert.Contains("JoinPromotionStallTimeout", ex.Message);
            Assert.NotEqual(System.ClusterMemberRole.Voter, n4.LocalRole);

            await healer;
            Assert.All(voters, v => Assert.False(InRoster(v, joiner)));
        }
        finally
        {
            comm.HealAll();
            n1.Dispose(); n2.Dispose(); n3.Dispose(); n4.Dispose();
        }
    }

    // ── helpers ────────────────────────────────────────────────────────────────

    private static bool InRoster(RaftManager node, string endpoint) =>
        node.SystemCoordinator.GetMembership().Members.Any(m => m.Endpoint == endpoint);

    private static async Task WriteContinuouslyAsync(RaftManager leader, int partitionId, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await leader.ReplicateLogs(partitionId, "test", [1, 2, 3], cancellationToken: ct);
                await Task.Delay(20, ct);
            }
            catch (OperationCanceledException)
            {
                return;
            }
        }
    }

    private static Task WaitForAsync(Func<bool> condition, CancellationToken ct, int timeoutMs = 30_000) =>
        WaitForAsync(() => ValueTask.FromResult(condition()), ct, timeoutMs);

    private static async Task WaitForAsync(Func<ValueTask<bool>> condition, CancellationToken ct, int timeoutMs = 30_000)
    {
        timeoutMs = TestTimeouts.Scale(timeoutMs);
        long deadline = Environment.TickCount64 + timeoutMs;
        while (Environment.TickCount64 < deadline)
        {
            ct.ThrowIfCancellationRequested();
            if (await condition())
                return;
            await Task.Delay(50, ct);
        }
        throw new TimeoutException($"Condition not met within {timeoutMs} ms.");
    }

    private static async Task<RaftManager> FindLeaderAsync(RaftManager[] nodes, int partitionId, CancellationToken ct)
    {
        long deadline = Environment.TickCount64 + TestTimeouts.Scale(15_000);
        while (Environment.TickCount64 < deadline)
        {
            ct.ThrowIfCancellationRequested();
            foreach (RaftManager node in nodes)
            {
                if (await node.AmILeaderQuick(partitionId))
                    return node;
            }
            await Task.Delay(50, ct);
        }
        throw new TimeoutException($"No leader for partition {partitionId} within the deadline.");
    }

    private static RaftManager BuildNode(
        InMemoryCommunication comm,
        int port,
        int nodeId,
        string[] peers,
        int initialPartitions = 1,
        Action<RaftConfiguration>? configure = null)
    {
        RaftConfiguration cfg = new()
        {
            NodeId = nodeId,
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
            EndElectionTimeout = 300,
            EnableQuiescence = false,
            BackfillThreshold = 0,
            MaxBackfillEntriesPerRound = 128,
        };

        configure?.Invoke(cfg);

        return new RaftManager(
            cfg,
            new StaticDiscovery(peers.Select(e => new RaftNode(e)).ToList()),
            new InMemoryWAL(NullLogger<IRaft>.Instance),
            comm,
            new HybridLogicalClock(),
            NullLogger<IRaft>.Instance);
    }
}
