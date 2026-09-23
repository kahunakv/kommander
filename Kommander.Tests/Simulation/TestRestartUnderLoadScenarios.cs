using Kommander.Data;
using Kommander.Tests.Simulation.Cluster;
using Kommander.Tests.Simulation.Invariants;
using Kommander.Tests.Simulation.WAL;
using Microsoft.Extensions.Logging;

namespace Kommander.Tests.Simulation;

/// <summary>
/// A node killed under load and restarted over its own store, against a real three-node cluster
/// whose leader has a compaction floor.
///
/// <para><b>Why these exist.</b> The Caraxes bank-leader-kill run (vorpal <c>b24b3891</c>). A
/// restarted node answers AppendEntries while Phase 1 of its restore is still reading the log, and
/// every frontier those acks read is at its init. The restarted leader reported a contiguous
/// frontier of 0 with a log that covered the leader's floor by millions of entries; the leader
/// anchored a backfill at 1, could not serve it below its floor, and re-seeded the node with a
/// 38-second snapshot three seconds before the restore finished. Below the floor a snapshot is the
/// only repair, and every existing rescue scenario puts its follower there on purpose — so nothing
/// asserted that a node ABOVE the floor is repaired by backfill, and the run reported success.</para>
///
/// <para><b>What makes a pass mean something.</b> The scenario asserts that the window was
/// reached — the leader's traffic really arrived before the restore landed — and that the node's
/// store really covered the floor. A restart that misses the window proves only that backfill
/// works, which other scenarios already prove.</para>
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
[Trait("Category", "DSTSmoke")]
public sealed class TestRestartUnderLoadScenarios
{
    private const int PartitionId = 1;

    /// <summary>The compaction cadence of the frequent-compaction sweep, so a short scenario compacts.</summary>
    private const int CompactEveryOperations = 8;

    /// <summary>Entries the leader retains below its checkpoint for a lagging follower it counts as alive.</summary>
    private const long LiveReplicaLagBudget = 4;

    /// <summary>
    /// The budget for the silent-peer scenario: wide enough that an outage spanning a whole
    /// compaction cadence (two batches of <see cref="CompactEveryOperations"/> plus a checkpoint)
    /// stays inside it, so the hold — not the budget — decides whether the node is compacted past.
    /// </summary>
    private const long SilentPeerLagBudget = 32;

    private readonly ILogger<IRaft> logger;

    public TestRestartUnderLoadScenarios(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = LoggerFactory.Create(builder =>
            builder.AddXUnit(outputHelper).SetMinimumLevel(LogLevel.Warning));

        logger = loggerFactory.CreateLogger<IRaft>();
    }

    /// <summary>
    /// A killed follower whose store still covers the leader's compaction floor rejoins by backfill,
    /// not by snapshot, even though the leader reaches it before its restore has landed.
    ///
    /// <para><b>The shape.</b> The leader writes and checkpoints until it has compacted a prefix,
    /// with every node caught up, so every store covers the floor. A follower is killed and the
    /// survivors commit a few entries without a checkpoint, so the floor does not move. The follower
    /// restarts with its restore read held open, into a leader that is heartbeating and re-supplying
    /// at it: the acks it sends before its restore completes must carry no position, and the leader
    /// must wait for the first real one rather than anchor a backfill at 1.</para>
    ///
    /// <para><b>Two defects, one shape.</b> The pre-restore ack above is the Caraxes mechanism. The
    /// same scenario also found the restart-initialisation form the report first suspected: the
    /// crash reverts the last checkpoint's commit marker inside the fsync window, the compaction
    /// that trusted it has already removed every earlier checkpoint, and the restore — reading only
    /// the rows — rebuilt a frontier of 0 for a log the node held through the floor. The restore now
    /// seeds its scan at the compaction floor, and the no-unnecessary-snapshot invariant is what
    /// caught it.</para>
    /// </summary>
    [Fact]
    public async Task AKilledFollowerAboveTheFloor_RejoinsByBackfill_WhenTheLeaderReachesItBeforeItsRestoreLands()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;

        await using SimulationCluster cluster = await SimulationCluster.StartAsync(
            new SimulationClusterOptions
            {
                NodeCount = 3,
                PartitionCount = 1,
                Seed = 20260915,
                ConfigureNode = configuration =>
                {
                    configuration.CompactEveryOperations = CompactEveryOperations;
                    configuration.CompactionLiveReplicaLagBudget = LiveReplicaLagBudget;
                },
            },
            logger,
            cancellationToken);

        ClusterInvariantRunner invariants = new();

        SimulationNode leader = await ElectAsync(cluster, cancellationToken);
        SimulationNode victim = cluster.Nodes.First(node => node != leader);

        // Build a compaction floor while everyone is caught up. A loop, because compaction runs on
        // its own cadence and the retention hold paces it behind the slowest live follower.
        for (int round = 0; round < 12 && FirstRetained(leader) <= 1; round++)
        {
            await ProposeAsync(leader, count: CompactEveryOperations, cancellationToken);
            await CheckpointAsync(cluster, leader, cancellationToken);
            await ConvergeAsync(cluster, invariants, await CommitIndexAsync(leader, cancellationToken), cancellationToken);
        }

        long floor = FirstRetained(leader);
        Assert.True(floor > 1, "The leader never compacted, so there is no floor for a restarted node to be judged against.");

        // Slack above the floor, so the fsync window a crash takes cannot drop the victim below it.
        await ProposeAsync(leader, count: 3, cancellationToken);
        await ConvergeAsync(cluster, invariants, await CommitIndexAsync(leader, cancellationToken), cancellationToken);

        await cluster.CrashNodeAsync(victim, cancellationToken);

        // The entries the victim misses. No checkpoint, so the floor stays where it is and backfill
        // remains the right repair.
        await ProposeAsync(leader, count: 2, cancellationToken);
        long duringOutage = await CommitIndexAsync(leader, cancellationToken);

        Assert.True(
            await cluster.RunUntilAsync(
                async () =>
                {
                    await invariants.CheckAsync(cluster, PartitionId, cancellationToken);
                    return cluster.Nodes.Where(node => node != victim)
                        .All(node => node.Wal.GetMaxLog(PartitionId) >= duringOutage);
                },
                stepCount: 300,
                advanceMilliseconds: 50,
                cancellationToken),
            "The surviving majority did not commit while one node was down.");

        // The preconditions, asserted rather than assumed: the floor did not move, and the victim's
        // own store covers it, so a backfill anchored at its prefix + 1 can be served.
        Assert.Equal(floor, FirstRetained(leader));
        long heldThrough = HeldThrough(victim);
        Assert.True(
            heldThrough >= floor - 1,
            $"The crashed node holds a resolved prefix only through {heldThrough} against a floor of {floor}: " +
            "it is below the floor, and a snapshot would be the correct repair. The scenario did not reach its shape.");

        // The window is the point of the scenario, so it is held open rather than raced for: the
        // partition's restore read waits a quarter of a simulated second, and the leader heartbeats and
        // re-supplies the node from the moment its endpoint is back — every one of those is
        // answered before the restore has read the store.
        victim.SimulatedWal!.HoldRestoreReads(PartitionId, forMilliseconds: 250);

        await cluster.RestartNodeAsync(victim, cancellationToken);

        await ProposeAsync(leader, count: 1, cancellationToken);
        long target = await CommitIndexAsync(leader, cancellationToken);

        bool converged = await cluster.RunUntilAsync(
            async () =>
            {
                await invariants.CheckAsync(cluster, PartitionId, cancellationToken);
                return await CommitIndexAsync(victim, cancellationToken) >= target;
            },
            stepCount: 400,
            advanceMilliseconds: 50,
            cancellationToken);

        Assert.True(
            converged,
            $"The restarted node never caught up: it is at {await CommitIndexAsync(victim, cancellationToken)} " +
            $"against the leader's {target}; the leader's log starts at {FirstRetained(leader)}.");

        // Read after the repair: the join returns when the partition map is applied, which is
        // before the held restore read and before the leader's first batch reaches the partition.
        long answeredBeforeRestore = victim.AppendsAnsweredBeforeRestore(PartitionId);
        Assert.True(
            answeredBeforeRestore > 0,
            "The leader's traffic never reached the node before its restore landed, so the pre-restore " +
            "window was not exercised and this run proves only that backfill works.");

        // Repaired by backfill. A snapshot here is the Caraxes shape: a node re-seeded for a
        // position it never had.
        Assert.Equal(0, cluster.Nodes.Sum(node => node.StateTransfer.ExportsServed));
        Assert.Empty(cluster.UnnecessarySnapshotImports);

        await invariants.CheckConvergedAsync(cluster, PartitionId, cancellationToken);
    }

    /// <summary>
    /// A killed follower whose range the leader compacts past DURING the outage is still repaired
    /// by backfill: the leader holds retention for a silent peer for the silent-peer window, so a
    /// restart inside it is served from the log. The Caraxes bank-leader-kill residue: the floor
    /// ran past the restarting node twice inside one 30-second outage and it was re-seeded by a
    /// full snapshot each time, then ran at half speed while its replica recovered.
    ///
    /// <para>Two runs of one shape. With the window on, no snapshot is served; with it off, the
    /// same outage puts the node below the floor and the (then legitimate) snapshot is served —
    /// which proves the hold is what makes the difference, and that the writes during the outage
    /// really did compact past the node.</para>
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task AKilledFollowerCompactedPastDuringItsOutage_IsHeldForTheWindow_AndRejoinsByBackfill(bool windowEnabled)
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;

        await using SimulationCluster cluster = await SimulationCluster.StartAsync(
            new SimulationClusterOptions
            {
                NodeCount = 3,
                PartitionCount = 1,
                Seed = 20260916,
                ConfigureNode = configuration =>
                {
                    configuration.CompactEveryOperations = CompactEveryOperations;
                    configuration.CompactionLiveReplicaLagBudget = SilentPeerLagBudget;
                    configuration.CompactionSilentPeerRetentionWindow = windowEnabled ? TimeSpan.FromMinutes(2) : TimeSpan.Zero;
                },
            },
            logger,
            cancellationToken);

        ClusterInvariantRunner invariants = new();

        SimulationNode leader = await ElectAsync(cluster, cancellationToken);
        SimulationNode victim = cluster.Nodes.First(node => node != leader);

        for (int round = 0; round < 12 && FirstRetained(leader) <= 1; round++)
        {
            await ProposeAsync(leader, count: CompactEveryOperations, cancellationToken);
            await CheckpointAsync(cluster, leader, cancellationToken);
            await ConvergeAsync(cluster, invariants, await CommitIndexAsync(leader, cancellationToken), cancellationToken);
        }

        Assert.True(FirstRetained(leader) > 1, "The leader never compacted, so there is no floor to run past.");

        await ProposeAsync(leader, count: 3, cancellationToken);
        await ConvergeAsync(cluster, invariants, await CommitIndexAsync(leader, cancellationToken), cancellationToken);

        await cluster.CrashNodeAsync(victim, cancellationToken);
        long victimHeld = HeldThrough(victim);

        // A whole compaction cadence during the outage — a batch, a checkpoint, and the batch that
        // triggers the pass against it — inside the lag budget, so the hold alone decides whether
        // the node's range survives.
        await ProposeAsync(leader, count: CompactEveryOperations, cancellationToken);
        await CheckpointAsync(cluster, leader, cancellationToken);
        await ProposeAsync(leader, count: CompactEveryOperations, cancellationToken);

        long floorTarget = await CommitIndexAsync(leader, cancellationToken);
        await cluster.RunUntilAsync(
            async () =>
            {
                await invariants.CheckAsync(cluster, PartitionId, cancellationToken);
                return !windowEnabled && FirstRetained(leader) > victimHeld + 1;
            },
            stepCount: 60,
            advanceMilliseconds: 50,
            cancellationToken);

        long firstRetained = FirstRetained(leader);
        if (windowEnabled)
            Assert.True(
                firstRetained <= victimHeld + 1,
                $"The leader compacted through {firstRetained - 1} past the silent node's prefix {victimHeld} inside the window.");
        else
            Assert.True(
                firstRetained > victimHeld + 1,
                $"The leader's log starts at {firstRetained} while the crashed node holds through {victimHeld}: " +
                "the outage never compacted past it, so this run tests nothing.");

        await cluster.RestartNodeAsync(victim, cancellationToken);

        await ProposeAsync(leader, count: 1, cancellationToken);
        long target = await CommitIndexAsync(leader, cancellationToken);

        Assert.True(
            await cluster.RunUntilAsync(
                async () =>
                {
                    await invariants.CheckAsync(cluster, PartitionId, cancellationToken);
                    return await CommitIndexAsync(victim, cancellationToken) >= target;
                },
                stepCount: 400,
                advanceMilliseconds: 50,
                cancellationToken),
            $"The restarted node never caught up: it is at {await CommitIndexAsync(victim, cancellationToken)} " +
            $"against the leader's {target}; the leader's log starts at {FirstRetained(leader)} (floor target {floorTarget}).");

        Assert.Equal(windowEnabled ? 0 : 1, cluster.Nodes.Sum(node => node.StateTransfer.ExportsServed));
        Assert.Empty(cluster.UnnecessarySnapshotImports);

        await invariants.CheckConvergedAsync(cluster, PartitionId, cancellationToken);
    }

    /// <summary>
    /// A follower whose FIRST sight of an entry is the committed row keeps that entry across a
    /// crash, and the leader's silent-peer hold therefore matches what the node really holds.
    ///
    /// <para><b>The defect this pins.</b> The leader ships its own log instances through the
    /// responder queue and its commit path retypes them in place, so a propose broadcast that is
    /// delivered after the leader committed the entry (through the other follower) arrives typed
    /// <c>Committed</c>. The follower planned it as a commit marker, the scheduler's single-fsync
    /// fast path wrote the all-<c>Committed</c> batch without an fsync, the follower reported the
    /// entry durably present, and the leader held retention one above it. The crash then took the
    /// row itself — it had no earlier durable version — and the node held one entry less than the
    /// leader believed: "compacted through 21 past the silent node's prefix 20", the GA flake of
    /// the scenario above, which reached this ordering only when the victim's executor lost the
    /// race against the leader's commit. This scenario forces the ordering by holding the victim's
    /// traffic across the commit, so the shape is exercised on every run.</para>
    ///
    /// <para><b>How the state is built.</b> The transport gives every receiver its own copy of a
    /// message, so the leader's later retype cannot reach a copy that already left. The late type
    /// comes from the responder: it serializes a message when it takes it off its queue, which can be
    /// after the commit. <see cref="Transport.SimulatedTransport.SetLateSerialization"/> models that
    /// delay for the victim while its traffic is held. Measured: at this seed the responder already
    /// sends after the commit, so the state appears without the late copy too. The late copy makes it
    /// certain on a machine where the responder runs sooner. The delivery observer below is what
    /// proves the state, whichever path produced it.</para>
    ///
    /// <para><b>What proves the run was real.</b> The leader must have committed the entry while
    /// the victim's traffic was held, the victim's first delivery of the entry must carry the type
    /// <c>Committed</c> (else the victim just saw an ordinary propose), and the leader's recorded
    /// durable frontier for the victim must not exceed what the victim's store kept — the invariant
    /// the retention hold is built on.</para>
    /// </summary>
    [Fact]
    public async Task AFollowerThatFirstSeesAnEntryCommitted_KeepsItAcrossACrash_AndTheHoldMatchesItsPrefix()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;

        await using SimulationCluster cluster = await SimulationCluster.StartAsync(
            new SimulationClusterOptions
            {
                NodeCount = 3,
                PartitionCount = 1,
                Seed = 20260916,
                ConfigureNode = configuration =>
                {
                    configuration.CompactEveryOperations = CompactEveryOperations;
                    configuration.CompactionLiveReplicaLagBudget = SilentPeerLagBudget;
                    configuration.CompactionSilentPeerRetentionWindow = TimeSpan.FromMinutes(2);
                },
            },
            logger,
            cancellationToken);

        ClusterInvariantRunner invariants = new();

        SimulationNode leader = await ElectAsync(cluster, cancellationToken);
        SimulationNode victim = cluster.Nodes.First(node => node != leader);

        for (int round = 0; round < 12 && FirstRetained(leader) <= 1; round++)
        {
            await ProposeAsync(leader, count: CompactEveryOperations, cancellationToken);
            await CheckpointAsync(cluster, leader, cancellationToken);
            await ConvergeAsync(cluster, invariants, await CommitIndexAsync(leader, cancellationToken), cancellationToken);
        }

        Assert.True(FirstRetained(leader) > 1, "The leader never compacted, so there is no floor to run past.");

        await ProposeAsync(leader, count: 2, cancellationToken);
        await ConvergeAsync(cluster, invariants, await CommitIndexAsync(leader, cancellationToken), cancellationToken);

        // The first type each id carried when it reached the victim.
        Dictionary<long, RaftLogType> firstSight = [];
        cluster.Transport.AppendLogsDelivered += (_, to, logs) =>
        {
            if (to != victim.Endpoint)
                return;

            lock (firstSight)
            {
                foreach (RaftLog log in logs)
                    firstSight.TryAdd(log.Id, log.Type);
            }
        };

        // Hold the victim's traffic across the commit of one entry: the leader commits it with the
        // other follower, and the victim's propose broadcast is serialized and delivered only after
        // that — the lagging responder.
        long beforeHold = await CommitIndexAsync(victim, cancellationToken);
        cluster.Transport.SetLateSerialization(victim.Endpoint, true);
        cluster.Transport.FreezeEndpoint(victim.Endpoint);

        await ProposeAsync(leader, count: 1, cancellationToken);
        long lastId = await CommitIndexAsync(leader, cancellationToken);
        await cluster.RunUntilAsync(() => Task.FromResult(false), stepCount: 2, advanceMilliseconds: 50, cancellationToken);

        Assert.Equal(beforeHold + 1, lastId);
        Assert.True(
            await CommitIndexAsync(victim, cancellationToken) < lastId,
            "The victim learned of the entry while its traffic was held, so the run does not exercise a late propose broadcast.");

        cluster.Transport.ThawEndpoint(victim.Endpoint);
        cluster.Transport.SetLateSerialization(victim.Endpoint, false);
        await ConvergeAsync(cluster, invariants, lastId, cancellationToken);

        RaftLogType sight;
        lock (firstSight)
            Assert.True(firstSight.TryGetValue(lastId, out sight), $"Entry {lastId} never reached the victim.");

        Assert.True(
            sight == RaftLogType.Committed,
            $"The victim first saw entry {lastId} as {sight}, so the run does not exercise a committed first sight.");

        await cluster.CrashNodeAsync(victim, cancellationToken);
        long victimHeld = HeldThrough(victim);

        Assert.True(
            victimHeld >= lastId,
            $"The crashed node holds through {victimHeld} but had reported {lastId}: the entry it first saw as " +
            "committed was written without an fsync and the crash took it.");

        RaftFollowerProgress? progress = leader.Manager.GetFollowerProgress(PartitionId, victim.Endpoint);
        Assert.NotNull(progress);
        Assert.True(
            progress.DurableFrontier <= victimHeld,
            $"The leader recorded the node's durable frontier at {progress.DurableFrontier}, above the {victimHeld} its store kept.");

        // The same outage cadence as the window scenario: the hold, not the budget, decides.
        await ProposeAsync(leader, count: CompactEveryOperations, cancellationToken);
        await CheckpointAsync(cluster, leader, cancellationToken);
        await ProposeAsync(leader, count: CompactEveryOperations, cancellationToken);

        await cluster.RunUntilAsync(
            async () =>
            {
                await invariants.CheckAsync(cluster, PartitionId, cancellationToken);
                return false;
            },
            stepCount: 60,
            advanceMilliseconds: 50,
            cancellationToken);

        long firstRetained = FirstRetained(leader);
        Assert.True(
            firstRetained <= victimHeld + 1,
            $"The leader compacted through {firstRetained - 1} past the silent node's prefix {victimHeld} inside the window.");

        await cluster.RestartNodeAsync(victim, cancellationToken);

        await ProposeAsync(leader, count: 1, cancellationToken);
        long target = await CommitIndexAsync(leader, cancellationToken);

        Assert.True(
            await cluster.RunUntilAsync(
                async () =>
                {
                    await invariants.CheckAsync(cluster, PartitionId, cancellationToken);
                    return await CommitIndexAsync(victim, cancellationToken) >= target;
                },
                stepCount: 400,
                advanceMilliseconds: 50,
                cancellationToken),
            $"The restarted node never caught up: it is at {await CommitIndexAsync(victim, cancellationToken)} " +
            $"against the leader's {target}; the leader's log starts at {FirstRetained(leader)}.");

        Assert.Equal(0, cluster.Nodes.Sum(node => node.StateTransfer.ExportsServed));
        Assert.Empty(cluster.UnnecessarySnapshotImports);

        await invariants.CheckConvergedAsync(cluster, PartitionId, cancellationToken);
    }

    /// <summary>
    /// A follower that restarts holding, as proposed, an entry it had reported as committed and
    /// durable is repaired by a snapshot, and the leader does not re-send a refused batch forever.
    /// The DST FINDING 7 liveness backstop.
    ///
    /// <para><b>The state.</b> The leader compacts through the follower's reported durable frontier
    /// N while the follower is down (the silent-peer window holds retention at N + 1, exactly as
    /// designed). The follower's disk lied about the fsync of its last commit marker, so it restarts
    /// holding N as <c>Proposed</c>, with a commit frontier of N - 1. The leader's backfill starts at
    /// N + 1 and is anchored on N, which it compacted, so it carries <c>prevLogTerm = -1</c>. The
    /// follower accepts such an anchor only inside its committed prefix (FINDING 6), and N is not in
    /// it.</para>
    ///
    /// <para><b>What went wrong before the backstop.</b> The rejection took the ordinary backtrack,
    /// the anchored-repair note re-sent the same batch on the next heartbeat, and the no-progress
    /// streak never grew because only <c>Success</c> acks feed it: 800 identical rejections in 400
    /// steps and no snapshot. That is how FINDING 7 wedged. Its fix removed the library's own way into
    /// this state; the lying disk is another way in, and a real one.</para>
    ///
    /// <para><b>What proves the run was real.</b> The fault reverted a row, the leader compacted
    /// through that row, and the repair was a snapshot — backfill cannot repair this state.</para>
    /// </summary>
    [Fact]
    public async Task AFollowerWhoseDiskLostAReportedCommitMarker_IsRepairedByASnapshot()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;

        await using SimulationCluster cluster = await SimulationCluster.StartAsync(
            new SimulationClusterOptions
            {
                NodeCount = 3,
                PartitionCount = 1,
                Seed = 20260916,
                ConfigureNode = configuration =>
                {
                    configuration.CompactEveryOperations = CompactEveryOperations;
                    configuration.CompactionLiveReplicaLagBudget = SilentPeerLagBudget;
                    configuration.CompactionSilentPeerRetentionWindow = TimeSpan.FromMinutes(2);
                },
            },
            logger,
            cancellationToken);

        ClusterInvariantRunner invariants = new();

        SimulationNode leader = await ElectAsync(cluster, cancellationToken);
        SimulationNode victim = cluster.Nodes.First(node => node != leader);

        for (int round = 0; round < 12 && FirstRetained(leader) <= 1; round++)
        {
            await ProposeAsync(leader, count: CompactEveryOperations, cancellationToken);
            await CheckpointAsync(cluster, leader, cancellationToken);
            await ConvergeAsync(cluster, invariants, await CommitIndexAsync(leader, cancellationToken), cancellationToken);
        }

        await ProposeAsync(leader, count: 3, cancellationToken);

        // A checkpoint last: its commit row is always written with an fsync, which carries every
        // marker before it, so the victim reports everything it has committed as durable.
        await CheckpointAsync(cluster, leader, cancellationToken);
        await ConvergeAsync(cluster, invariants, await CommitIndexAsync(leader, cancellationToken), cancellationToken);

        // Wait for the report, not only the commit index. The view's commit index follows the
        // victim's durable presence, while the report also needs its resolutions on disk, and under
        // load the last markers can still be queued. The state under test is a follower that
        // REPORTED the entry as committed and durable, so the report is what must be in place.
        long leaderCommit = await CommitIndexAsync(leader, cancellationToken);

        Assert.True(
            await cluster.RunUntilAsync(
                () => Task.FromResult(
                    leader.Manager.GetFollowerProgress(PartitionId, victim.Endpoint) is { } progress
                    && progress.DurableFrontier >= leaderCommit),
                stepCount: 100,
                advanceMilliseconds: 50,
                cancellationToken),
            $"The victim never reported its committed prefix through {leaderCommit} as durable: " +
            $"{leader.Manager.GetFollowerProgress(PartitionId, victim.Endpoint)}.");

        victim.SimulatedWal!.LoseResolutionsOnNextCrash(PartitionId, count: 1);
        await cluster.CrashNodeAsync(victim, cancellationToken);

        Assert.Equal(1, victim.SimulatedWal.ResolutionsLostByLyingDisk);

        long reverted = victim.Wal.ReadLogsRange(PartitionId, 0)
            .Where(log => log.Type == RaftLogType.Proposed)
            .Select(log => log.Id)
            .DefaultIfEmpty(-1)
            .Max();

        Assert.True(reverted > 0, "The lying disk reverted no committed row.");

        // A whole compaction cadence during the outage, as in the window scenario: the hold keeps the
        // leader's log from the reported frontier + 1, so it compacts the reverted entry itself.
        await ProposeAsync(leader, count: CompactEveryOperations, cancellationToken);
        await CheckpointAsync(cluster, leader, cancellationToken);
        await ProposeAsync(leader, count: CompactEveryOperations, cancellationToken);

        await cluster.RunUntilAsync(
            async () =>
            {
                await invariants.CheckAsync(cluster, PartitionId, cancellationToken);
                return FirstRetained(leader) > reverted;
            },
            stepCount: 60,
            advanceMilliseconds: 50,
            cancellationToken);

        Assert.True(
            FirstRetained(leader) > reverted,
            $"The leader's log starts at {FirstRetained(leader)}, so it did not compact the reverted entry {reverted}, " +
            "and the batch is not anchored on a compacted entry: this run tests nothing.");

        await cluster.RestartNodeAsync(victim, cancellationToken);

        await ProposeAsync(leader, count: 1, cancellationToken);
        long target = await CommitIndexAsync(leader, cancellationToken);

        bool repaired = await cluster.RunUntilAsync(
            async () =>
            {
                await invariants.CheckAsync(cluster, PartitionId, cancellationToken);
                return await CommitIndexAsync(victim, cancellationToken) >= target;
            },
            stepCount: 400,
            advanceMilliseconds: 50,
            cancellationToken);

        Assert.True(
            repaired,
            $"The follower was never repaired: it is at {await CommitIndexAsync(victim, cancellationToken)} against " +
            $"{target}; the reverted entry is {reverted} and the leader's log starts at {FirstRetained(leader)}. " +
            $"Exports served: {leader.StateTransfer.ExportsServed}. " +
            $"Snapshot statuses: [{string.Join(" | ", leader.Manager.GetSnapshotStatuses(PartitionId))}].");

        Assert.True(
            leader.StateTransfer.ExportsServed >= 1,
            "The follower converged without a snapshot, so the refused -1 anchor was never reached.");

        await invariants.CheckConvergedAsync(cluster, PartitionId, cancellationToken);
        await AppliedStateRule.CheckAsync(cluster, PartitionId, advanceMilliseconds: 50, cancellationToken);
    }

    // ── Helpers ───────────────────────────────────────────────────────────

    private static long FirstRetained(SimulationNode node) =>
        node.SimulatedWal?.Snapshot().Partition(PartitionId)?.FirstLogId ?? -1;

    /// <summary>
    /// Highest id the node holds resolved with no hole below it, above whatever it is no longer
    /// required to hold — the position backfill would anchor above.
    /// </summary>
    private static long HeldThrough(SimulationNode node)
    {
        SimulatedWalPartitionSnapshot? store = node.SimulatedWal?.Snapshot().Partition(PartitionId);
        if (store is null)
            return -1;

        long held = store.CoveredThrough;
        foreach (RaftLog log in node.Wal.ReadLogsRange(PartitionId, held + 1))
        {
            if (log.Id != held + 1 || log.Type is not (RaftLogType.Committed or RaftLogType.CommittedCheckpoint))
                break;
            held = log.Id;
        }

        return held;
    }

    private static async Task<long> CommitIndexAsync(SimulationNode node, CancellationToken cancellationToken)
    {
        RaftPartitionView? view = await node.GetPartitionViewAsync(PartitionId, cancellationToken);
        return view?.CommitIndex ?? -1;
    }

    private static async Task ProposeAsync(SimulationNode leader, int count, CancellationToken cancellationToken)
    {
        for (int index = 0; index < count; index++)
        {
            RaftReplicationResult result = await leader.Manager.ReplicateLogs(
                PartitionId, "Greeting", "Hello World"u8.ToArray(), cancellationToken: cancellationToken);

            Assert.Equal(RaftOperationStatus.Success, result.Status);
        }
    }

    private static async Task CheckpointAsync(
        SimulationCluster cluster,
        SimulationNode leader,
        CancellationToken cancellationToken)
    {
        Task<RaftReplicationResult> checkpoint = leader.Manager.ReplicateCheckpoint(PartitionId, cancellationToken);

        await cluster.RunUntilAsync(
            () => Task.FromResult(checkpoint.IsCompleted),
            stepCount: 40,
            advanceMilliseconds: 50,
            cancellationToken);

        Assert.Equal(RaftOperationStatus.Success, (await checkpoint).Status);
    }

    private static async Task ConvergeAsync(
        SimulationCluster cluster,
        ClusterInvariantRunner invariants,
        long index,
        CancellationToken cancellationToken)
    {
        Assert.True(
            await cluster.RunUntilAsync(
                async () =>
                {
                    await invariants.CheckAsync(cluster, PartitionId, cancellationToken);

                    IReadOnlyList<RaftPartitionView> views =
                        await cluster.GetPartitionViewsAsync(PartitionId, cancellationToken);

                    return views.Count == cluster.Nodes.Count(node => node.HasLiveManager)
                           && views.All(view => view.CommitIndex >= index);
                },
                stepCount: 300,
                advanceMilliseconds: 50,
                cancellationToken),
            $"The running nodes did not all commit up to {index}.");
    }

    private static async Task<SimulationNode> ElectAsync(
        SimulationCluster cluster,
        CancellationToken cancellationToken)
    {
        bool elected = await cluster.RunUntilAsync(
            async () =>
            {
                IReadOnlyList<RaftPartitionView> views =
                    await cluster.GetPartitionViewsAsync(PartitionId, cancellationToken);

                return views.Count(view => view.Role == RaftNodeState.Leader) == 1;
            },
            stepCount: 300,
            advanceMilliseconds: 50,
            cancellationToken);

        Assert.True(elected, "No single leader was elected within the step budget.");

        foreach (SimulationNode node in cluster.Nodes)
        {
            RaftPartitionView? view = await node.GetPartitionViewAsync(PartitionId, cancellationToken);
            if (view?.Role == RaftNodeState.Leader)
                return node;
        }

        throw new InvalidOperationException("No running leader is present.");
    }
}
