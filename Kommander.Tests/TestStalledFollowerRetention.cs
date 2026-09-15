
using Kommander.Communication.Memory;
using Kommander.Diagnostics;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;
using Kommander.Tests.Chaos;

namespace Kommander.Tests;

/// <summary>
/// A follower whose disk pauses under load must be ridden out from the leader's log, not by a snapshot
/// (CamusDB slow-disk run sd8: the paused follower fell 205,000 entries below the WAL compaction floor in
/// 16 s, the refused backfill escalated to a snapshot transfer, and the follower was OOM-killed with the
/// chunks buffered six seconds after its heal). Two leader-side facts make that hold, both carried in the
/// follower's acks: the durable contiguous frontier, on which WAL retention is held, and the pending-write
/// age, on which entry-carrying backfill and snapshot transfers are deferred. The stall is injected with the
/// gated WAL of <see cref="TestWalStallStepDown"/> on a FOLLOWER, so the leader keeps writing with the other
/// replica and compaction keeps running against a checkpoint.
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
public sealed class TestStalledFollowerRetention
{
    private const int Partition = 1;

    private static string Endpoint(int index) => $"localhost:{8300 + index}";

    private static RaftManager BuildNode(int index, InMemoryCommunication communication, TestWalStallStepDown.GatedWal wal, ILogger<IRaft> logger)
    {
        List<RaftNode> peers = [];
        for (int i = 1; i <= 3; i++)
            if (i != index)
                peers.Add(new(Endpoint(i)));

        RaftConfiguration config = new()
        {
            NodeName = $"retention-node{index}",
            NodeId = index,
            Host = "localhost",
            Port = 8300 + index,
            InitialPartitions = Partition,
            HeartbeatInterval = TimeSpan.FromMilliseconds(50),
            RecentHeartbeat = TimeSpan.FromMilliseconds(25),
            VotingTimeout = TimeSpan.FromMilliseconds(250),
            CheckLeaderInterval = TimeSpan.FromMilliseconds(25),
            UpdateNodesInterval = TimeSpan.FromMilliseconds(100),
            TimerInitialDelay = TimeSpan.FromMilliseconds(25),
            StartElectionTimeout = 100,
            EndElectionTimeout = 250,
            EnableQuiescence = false,
            // Compaction runs every 10 committed operations against the checkpoint the test writes.
            CompactEveryOperations = 10,
            CompactNumberEntries = 5,
            MaxEntriesPerCompaction = 100_000,
            // A 100 ms pending write counts as a stall, on the follower's log and in its acks.
            WalStallWarnThreshold = TimeSpan.FromMilliseconds(100),
        };

        return new RaftManager(config, new StaticDiscovery(peers), wal, communication, new HybridLogicalClock(), logger);
    }

    private static async Task<(IRaft[] Nodes, Dictionary<IRaft, TestWalStallStepDown.GatedWal> Wals)> AssembleAsync(ILogger<IRaft> logger, CancellationToken ct)
    {
        InMemoryCommunication communication = new();
        Dictionary<IRaft, TestWalStallStepDown.GatedWal> wals = [];
        Dictionary<string, IRaft> network = [];
        IRaft[] nodes = new IRaft[3];

        for (int i = 1; i <= 3; i++)
        {
            TestWalStallStepDown.GatedWal wal = new(new InMemoryWAL(logger));
            RaftManager node = BuildNode(i, communication, wal, logger);
            nodes[i - 1] = node;
            wals[node] = wal;
            network[Endpoint(i)] = node;
        }

        communication.SetNodes(network);

        foreach (IRaft node in nodes)
            await node.UpdateNodes();

        await Task.WhenAll(nodes.Select(n => n.JoinCluster(ct)));

        return (nodes, wals);
    }

    private static async Task<IRaft> WaitForLeaderAsync(IRaft[] nodes, CancellationToken ct, int timeoutMs = 15_000)
    {
        ValueStopwatch stopwatch = ValueStopwatch.StartNew();

        while (stopwatch.GetElapsedMilliseconds() < timeoutMs)
        {
            ct.ThrowIfCancellationRequested();

            foreach (IRaft node in nodes)
            {
                if (await node.AmILeaderQuick(Partition).ConfigureAwait(false))
                    return node;
            }

            await Task.Delay(25, ct);
        }

        throw new TimeoutException($"No leader for partition {Partition} within {timeoutMs} ms.");
    }

    private static async Task WaitUntilAsync(Func<bool> predicate, int timeoutMs, Func<string> what, CancellationToken ct)
    {
        ValueStopwatch stopwatch = ValueStopwatch.StartNew();

        while (stopwatch.GetElapsedMilliseconds() < timeoutMs)
        {
            if (predicate())
                return;

            await Task.Delay(25, ct);
        }

        Assert.True(predicate(), what());
    }

    private static async Task ReplicateAsync(IRaft leader, int count, string tag, CancellationToken ct)
    {
        for (int i = 0; i < count; i++)
        {
            RaftReplicationResult result = await leader.ReplicateLogs(Partition, "Greeting", global::System.Text.Encoding.UTF8.GetBytes($"{tag}-{i}"), cancellationToken: ct);
            Assert.True(result.Success, $"replication {tag}-{i} failed: {result.Status}");
        }
    }

    private static long FirstRetainedId(IRaft node)
    {
        List<RaftLog> head = node.WalAdapter.ReadLogsRange(Partition, 0, 1);
        return head.Count == 0 ? -1 : head[0].Id;
    }

    /// <summary>
    /// While a follower's disk is paused the leader compacts up to, and never past, the follower's
    /// durable frontier — although the follower keeps acking every heartbeat with a protocol frontier
    /// at the leader's commit — ships it no snapshot, records no refused backfill, and once the disk
    /// answers the follower converges from the retained log.
    /// </summary>
    [Fact]
    public async Task PausedFollower_IsNotCompactedPastItsDurableFrontier_AndCatchesUpFromTheLog()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (IRaft[] nodes, Dictionary<IRaft, TestWalStallStepDown.GatedWal> wals) = await AssembleAsync(new TempFileLogger<IRaft>(), ct);

        try
        {
            IRaft leader = await WaitForLeaderAsync(nodes, ct);
            IRaft follower = nodes.First(n => !ReferenceEquals(n, leader));
            TestWalStallStepDown.GatedWal followerWal = wals[follower];

            // A baseline every replica holds durably, so the follower has a position to be held at.
            await ReplicateAsync(leader, 20, "baseline", ct);
            long baselineMax = leader.WalAdapter.GetMaxLog(Partition);
            await WaitUntilAsync(
                () => nodes.All(n => n.WalAdapter.GetMaxLog(Partition) >= baselineMax),
                timeoutMs: 15_000,
                () => $"every replica must hold the baseline through {baselineMax}; got [{string.Join(",", nodes.Select(n => n.WalAdapter.GetMaxLog(Partition)))}]",
                ct);

            // The follower's disk stops answering. Its durable position freezes here; everything the
            // leader ships from now on only queues in its WAL scheduler. The in-memory transport moves
            // a proposal in well under a millisecond, so the phases below are paced with the heartbeat
            // interval (50 ms) and the stall threshold (100 ms): the leader learns the follower's
            // durable frontier and its stall from heartbeat acks, and a real pause outlasts hundreds
            // of heartbeat rounds — this test must give it at least a few.
            followerWal.Stall();
            long durableAtStall = follower.WalAdapter.GetMaxLog(Partition);
            await ReplicateAsync(leader, 5, "into-the-stall", ct);
            await Task.Delay(400, ct);

            // The cluster keeps writing on the two healthy replicas, checkpoints, and keeps writing so
            // that compaction runs several times against that checkpoint while the follower is paused.
            await ReplicateAsync(leader, 60, "during", ct);
            await Task.Delay(150, ct);
            RaftReplicationResult checkpoint = await leader.ReplicateCheckpoint(Partition, ct);
            Assert.True(checkpoint.Success, $"checkpoint failed: {checkpoint.Status}");
            await ReplicateAsync(leader, 60, "after-checkpoint", ct);
            await Task.Delay(150, ct);
            await ReplicateAsync(leader, 30, "after-checkpoint-2", ct);

            Assert.True(followerWal.BlockedWrites > 0, "the follower's engine must be holding a write");

            // Compaction ran (the prefix below the follower's durable position is gone) and stopped
            // exactly at the follower's durable frontier: the retention floor followed the durable
            // report, not the protocol frontier the follower's heartbeat acks kept advancing.
            await WaitUntilAsync(
                () => FirstRetainedId(leader) > 1,
                timeoutMs: 15_000,
                () => $"the leader must have compacted its prefix; first retained is {FirstRetainedId(leader)} with checkpoint {leader.WalAdapter.GetLastCheckpoint(Partition)}",
                ct);

            for (int i = 0; i < 10; i++)
            {
                long firstRetained = FirstRetainedId(leader);
                Assert.True(firstRetained <= durableAtStall + 1,
                    $"the leader compacted past the paused follower: first retained {firstRetained}, follower durable {durableAtStall}, checkpoint {leader.WalAdapter.GetLastCheckpoint(Partition)}");
                Assert.Empty(leader.GetBackfillStatuses(Partition));
                Assert.Empty(leader.GetSnapshotStatuses(Partition));
                await Task.Delay(50, ct);
            }

            Assert.True(leader.WalAdapter.GetLastCheckpoint(Partition) > durableAtStall + 1,
                "the checkpoint must sit above the follower's position, or the floor was never what held retention");
            Assert.True(followerWal.BlockedWrites > 0, "the follower must still be paused while retention is checked");

            // The disk answers. The follower drains its queue and is topped up from the retained log —
            // never by a snapshot and never through a refused batch.
            followerWal.Release();

            long expectedMax = leader.WalAdapter.GetMaxLog(Partition);
            await WaitUntilAsync(
                () => follower.WalAdapter.GetMaxLog(Partition) >= expectedMax && follower.GetCommitIndex(Partition) >= leader.GetCommitIndex(Partition),
                timeoutMs: 20_000,
                () => $"the follower must converge to max {expectedMax} / commit {leader.GetCommitIndex(Partition)}; it is at {follower.WalAdapter.GetMaxLog(Partition)} / {follower.GetCommitIndex(Partition)}",
                ct);

            Assert.Empty(leader.GetBackfillStatuses(Partition));
            Assert.Empty(leader.GetSnapshotStatuses(Partition));
            Assert.Equal(0, followerWal.BlockedWrites);
        }
        finally
        {
            foreach (TestWalStallStepDown.GatedWal wal in wals.Values)
                wal.Release();

            foreach (IRaft node in nodes)
            {
                try
                {
                    await node.LeaveCluster(true);
                }
                catch
                {
                    // best effort teardown
                }
            }

            foreach (TestWalStallStepDown.GatedWal wal in wals.Values)
                wal.Dispose();
        }
    }
}
