
using System.Diagnostics.Metrics;
using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Diagnostics;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;

namespace Kommander.Tests;

/// <summary>
/// Integration tests for partition quiescence.
///
/// Quiescence lets an idle leader suppress per-partition heartbeats; followers switch to
/// SWIM-based election gating instead of the heartbeat timer so they do not start spurious
/// elections.  New writes un-quiesce the leader, resuming normal heartbeating and replication.
///
/// All tests use <see cref="InMemoryCommunication"/> with timing parameters tuned so quiescence
/// fires in well under 500 ms:
///   HeartbeatInterval = 50 ms, QuiesceAfter = 100 ms, CheckLeaderInterval = 25 ms.
///
/// The constraint <c>PingInterval (200 ms) &lt; StartElectionTimeout (500 ms)</c> is satisfied
/// as required by <see cref="RaftConfiguration.Validate"/>.
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
public sealed class TestQuiescence
{
    private readonly ILogger<IRaft> logger;

    public TestQuiescence(ITestOutputHelper outputHelper)
    {
        ILoggerFactory factory = LoggerFactory.Create(b => b.AddXUnit(outputHelper).SetMinimumLevel(LogLevel.Debug));
        logger = factory.CreateLogger<IRaft>();
    }

    // ── Config guard ──────────────────────────────────────────────────────────

    /// <summary>
    /// <see cref="RaftConfiguration.Validate"/> must throw when
    /// <c>EnableQuiescence = true</c> and <c>PingInterval ≥ StartElectionTimeout</c>.
    /// </summary>
    [Fact]
    public void QuiescenceConfig_PingIntervalAtOrAboveElectionTimeout_ThrowsOnValidate()
    {
        RaftConfiguration config = new()
        {
            Host = "localhost",
            Port = 8001,
            InitialPartitions = 1,
            EnableQuiescence = true,
            PingInterval = TimeSpan.FromMilliseconds(1000),
            StartElectionTimeout = 1000,
        };

        Assert.Throws<RaftException>(config.Validate);
    }

    // ── Idle cluster stays stable ─────────────────────────────────────────────

    /// <summary>
    /// After the idle window elapses the leader sends a quiesce marker and suppresses
    /// per-partition heartbeats.  Followers gate future elections on SWIM liveness rather
    /// than the heartbeat timer, so the cluster must remain stable with exactly one leader
    /// for a window of several election-timeout lengths without any writes.
    /// </summary>
    [Fact]
    public async Task IdleCluster_WithQuiescence_KeepsStableLeaderWithoutWrites()
    {
        InMemoryCommunication communication = new();
        IRaft node1 = MakeNode(communication, 1, [new("localhost:8002"), new("localhost:8003")]);
        IRaft node2 = MakeNode(communication, 2, [new("localhost:8001"), new("localhost:8003")]);
        IRaft node3 = MakeNode(communication, 3, [new("localhost:8001"), new("localhost:8002")]);
        IRaft[] nodes = [node1, node2, node3];

        communication.SetNodes(new Dictionary<string, IRaft>
        {
            { "localhost:8001", node1 },
            { "localhost:8002", node2 },
            { "localhost:8003", node3 }
        });

        await Task.WhenAll(
            node1.UpdateNodes(),
            node2.UpdateNodes(),
            node3.UpdateNodes());

        await Task.WhenAll(
            node1.JoinCluster(TestContext.Current.CancellationToken),
            node2.JoinCluster(TestContext.Current.CancellationToken),
            node3.JoinCluster(TestContext.Current.CancellationToken));

        // Wait for a stable leader on partition 1.
        string initialLeader = await node1.WaitForLeaderStableAsync(
            1, TimeSpan.FromMilliseconds(150), TestContext.Current.CancellationToken);

        // Let the idle window elapse so the leader quiesces (HeartbeatInterval + QuiesceAfter = 150 ms;
        // add a generous margin for scheduler jitter).
        await Task.Delay(600, TestContext.Current.CancellationToken);

        // Wait for 3 × StartElectionTimeout without writes.  If quiescence is broken and followers
        // still run the heartbeat timer, the missing heartbeats would trigger a spurious election.
        await Task.Delay(3 * 500, TestContext.Current.CancellationToken);

        // Exactly one leader must remain on each partition, unchanged.
        Assert.Equal(1, await CountLeadersAsync(nodes, 1));
        string currentLeader = await node1.WaitForLeaderStableAsync(
            1, TimeSpan.FromMilliseconds(150), TestContext.Current.CancellationToken);
        Assert.Equal(initialLeader, currentLeader);

        await node1.LeaveCluster(true);
        await node2.LeaveCluster(true);
        await node3.LeaveCluster(true);
    }

    // ── Write wakes quiesced partition ────────────────────────────────────────

    /// <summary>
    /// A write proposed after the partition quiesces must un-quiesce the leader, replicate
    /// successfully, and commit to all three nodes.  The proposal must not time out or return
    /// an error status.
    /// </summary>
    [Fact]
    public async Task IdleCluster_AfterQuiesce_WriteSucceedsAndReplicates()
    {
        InMemoryCommunication communication = new();
        IRaft node1 = MakeNode(communication, 1, [new("localhost:8002"), new("localhost:8003")]);
        IRaft node2 = MakeNode(communication, 2, [new("localhost:8001"), new("localhost:8003")]);
        IRaft node3 = MakeNode(communication, 3, [new("localhost:8001"), new("localhost:8002")]);
        IRaft[] nodes = [node1, node2, node3];

        communication.SetNodes(new Dictionary<string, IRaft>
        {
            { "localhost:8001", node1 },
            { "localhost:8002", node2 },
            { "localhost:8003", node3 }
        });

        await Task.WhenAll(
            node1.UpdateNodes(),
            node2.UpdateNodes(),
            node3.UpdateNodes());

        await Task.WhenAll(
            node1.JoinCluster(TestContext.Current.CancellationToken),
            node2.JoinCluster(TestContext.Current.CancellationToken),
            node3.JoinCluster(TestContext.Current.CancellationToken));

        await node1.WaitForLeaderStableAsync(
            1, TimeSpan.FromMilliseconds(150), TestContext.Current.CancellationToken);

        IRaft? leader = await GetLeaderAsync(1, nodes);
        Assert.NotNull(leader);

        // Wait for quiescence to take effect.
        await Task.Delay(600, TestContext.Current.CancellationToken);

        // Propose after quiesce; the leader must un-quiesce and replicate.
        RaftReplicationResult result = await leader.ReplicateLogs(
            1, "WakeTest", "hello"u8.ToArray(),
            cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal(RaftOperationStatus.Success, result.Status);
        Assert.Equal(1, result.LogIndex);

        // All replicas must have received the entry (backfill will catch any laggard).
        await WaitForConditionAsync(
            () => node1.WalAdapter.GetMaxLog(1) >= 1 &&
                  node2.WalAdapter.GetMaxLog(1) >= 1 &&
                  node3.WalAdapter.GetMaxLog(1) >= 1,
            TestContext.Current.CancellationToken);

        await node1.LeaveCluster(true);
        await node2.LeaveCluster(true);
        await node3.LeaveCluster(true);
    }

    // ── Heartbeat counter stops after quiesce ─────────────────────────────────

    /// <summary>
    /// With <c>EnableQuiescence = true</c>, once the idle window elapses the leader must
    /// stop calling <c>SendHeartbeat</c>.  This is observed via the <c>raft.heartbeats_sent_total</c>
    /// metric: the counter must not advance during a measurement window that starts after the
    /// expected quiesce time.
    /// </summary>
    [Fact]
    public async Task IdleLeader_WithQuiescence_HeartbeatCounterStopsAfterIdle()
    {
        long heartbeatCount = 0;
        using MeterListener listener = new();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == KommanderMetrics.MeterName &&
                instrument.Name == "raft.heartbeats_sent_total")
                l.EnableMeasurementEvents(instrument);
        };
        // Only track heartbeats for partition 1 (the data partition).  The system partition (0)
        // has ongoing membership writes that prevent quiescence, so it keeps heartbeating.
        listener.SetMeasurementEventCallback<long>((_, measurement, tags, _) =>
        {
            foreach (KeyValuePair<string, object?> tag in tags)
            {
                if (tag.Key == "partition_id" && tag.Value is int id && id == 1)
                {
                    Interlocked.Add(ref heartbeatCount, measurement);
                    break;
                }
            }
        });
        listener.Start();

        InMemoryCommunication communication = new();
        IRaft node1 = MakeNode(communication, 1, [new("localhost:8002"), new("localhost:8003")]);
        IRaft node2 = MakeNode(communication, 2, [new("localhost:8001"), new("localhost:8003")]);
        IRaft node3 = MakeNode(communication, 3, [new("localhost:8001"), new("localhost:8002")]);
        IRaft[] nodes = [node1, node2, node3];

        communication.SetNodes(new Dictionary<string, IRaft>
        {
            { "localhost:8001", node1 },
            { "localhost:8002", node2 },
            { "localhost:8003", node3 }
        });

        await Task.WhenAll(
            node1.UpdateNodes(),
            node2.UpdateNodes(),
            node3.UpdateNodes());

        await Task.WhenAll(
            node1.JoinCluster(TestContext.Current.CancellationToken),
            node2.JoinCluster(TestContext.Current.CancellationToken),
            node3.JoinCluster(TestContext.Current.CancellationToken));

        await node1.WaitForLeaderStableAsync(
            1, TimeSpan.FromMilliseconds(150), TestContext.Current.CancellationToken);

        // Wait until the leader is expected to have quiesced.
        await Task.Delay(600, TestContext.Current.CancellationToken);

        // Snapshot counter at start of measurement window, then wait.
        long countBefore = Interlocked.Read(ref heartbeatCount);
        await Task.Delay(350, TestContext.Current.CancellationToken);
        long countAfter = Interlocked.Read(ref heartbeatCount);

        // If quiescence is working, no heartbeats should be sent after the quiesce marker.
        Assert.Equal(0, countAfter - countBefore);

        await node1.LeaveCluster(true);
        await node2.LeaveCluster(true);
        await node3.LeaveCluster(true);
    }

    // ── Feature-flag parity ───────────────────────────────────────────────────

    /// <summary>
    /// With <c>EnableQuiescence = false</c> the leader must continue sending regular heartbeats
    /// even after the idle window elapses, matching pre-quiescence behaviour.  The
    /// <c>raft.heartbeats_sent_total</c> counter must keep advancing throughout a measurement
    /// window that is long after the point where quiescence would have fired.
    /// </summary>
    [Fact]
    public async Task IdleLeader_QuiescenceDisabled_HeartbeatsKeepComing()
    {
        long heartbeatCount = 0;
        using MeterListener listener = new();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == KommanderMetrics.MeterName &&
                instrument.Name == "raft.heartbeats_sent_total")
                l.EnableMeasurementEvents(instrument);
        };
        // Only track heartbeats for partition 1 (the data partition) so system-partition activity
        // does not pollute the measurement regardless of whether it quiesces.
        listener.SetMeasurementEventCallback<long>((_, measurement, tags, _) =>
        {
            foreach (KeyValuePair<string, object?> tag in tags)
            {
                if (tag.Key == "partition_id" && tag.Value is int id && id == 1)
                {
                    Interlocked.Add(ref heartbeatCount, measurement);
                    break;
                }
            }
        });
        listener.Start();

        InMemoryCommunication communication = new();
        IRaft node1 = MakeNode(communication, 1, [new("localhost:8002"), new("localhost:8003")], enableQuiescence: false);
        IRaft node2 = MakeNode(communication, 2, [new("localhost:8001"), new("localhost:8003")], enableQuiescence: false);
        IRaft node3 = MakeNode(communication, 3, [new("localhost:8001"), new("localhost:8002")], enableQuiescence: false);
        IRaft[] nodes = [node1, node2, node3];

        communication.SetNodes(new Dictionary<string, IRaft>
        {
            { "localhost:8001", node1 },
            { "localhost:8002", node2 },
            { "localhost:8003", node3 }
        });

        await Task.WhenAll(
            node1.UpdateNodes(),
            node2.UpdateNodes(),
            node3.UpdateNodes());

        await Task.WhenAll(
            node1.JoinCluster(TestContext.Current.CancellationToken),
            node2.JoinCluster(TestContext.Current.CancellationToken),
            node3.JoinCluster(TestContext.Current.CancellationToken));

        await node1.WaitForLeaderStableAsync(
            1, TimeSpan.FromMilliseconds(150), TestContext.Current.CancellationToken);

        // Wait past the window where quiescence would have fired if it were enabled.
        await Task.Delay(600, TestContext.Current.CancellationToken);

        long countBefore = Interlocked.Read(ref heartbeatCount);
        await Task.Delay(350, TestContext.Current.CancellationToken);
        long countAfter = Interlocked.Read(ref heartbeatCount);

        // Without quiescence, the leader keeps heartbeating at HeartbeatInterval (50 ms).
        // Over a 350 ms window we expect at least 3 heartbeat rounds.
        Assert.True(countAfter - countBefore >= 3,
            $"Expected heartbeat counter to advance by at least 3 but got {countAfter - countBefore}.");

        await node1.LeaveCluster(true);
        await node2.LeaveCluster(true);
        await node3.LeaveCluster(true);
    }

    // ── Failover from quiesced partition ──────────────────────────────────────

    /// <summary>
    /// When a quiesced leader's transport is cut (all Raft and SWIM traffic drops), followers
    /// must elect a replacement promptly.  With <c>EnableQuiescence = true</c>, quiesced
    /// followers gate elections on SWIM Suspect state (fires after approximately one
    /// <c>PingInterval</c>) rather than the heartbeat timer, so failover must complete well
    /// within <c>SuspicionTimeout</c>.
    /// </summary>
    [Fact]
    public async Task QuiescedLeader_WhenPartitioned_NewLeaderElectedBeforeSuspicionTimeout()
    {
        InMemoryCommunication communication = new();
        IRaft node1 = MakeNode(communication, 1, [new("localhost:8002"), new("localhost:8003")]);
        IRaft node2 = MakeNode(communication, 2, [new("localhost:8001"), new("localhost:8003")]);
        IRaft node3 = MakeNode(communication, 3, [new("localhost:8001"), new("localhost:8002")]);
        IRaft[] nodes = [node1, node2, node3];

        communication.SetNodes(new Dictionary<string, IRaft>
        {
            { "localhost:8001", node1 },
            { "localhost:8002", node2 },
            { "localhost:8003", node3 }
        });

        await Task.WhenAll(
            node1.UpdateNodes(),
            node2.UpdateNodes(),
            node3.UpdateNodes());

        await Task.WhenAll(
            node1.JoinCluster(TestContext.Current.CancellationToken),
            node2.JoinCluster(TestContext.Current.CancellationToken),
            node3.JoinCluster(TestContext.Current.CancellationToken));

        string initialLeader = await node1.WaitForLeaderStableAsync(
            1, TimeSpan.FromMilliseconds(150), TestContext.Current.CancellationToken);

        // Let the leader quiesce.
        await Task.Delay(600, TestContext.Current.CancellationToken);

        // Cut the leader's transport.  SWIM probes from followers will fail and mark
        // the leader Suspect after approximately one PingInterval (200 ms), after which
        // quiesced followers start an election.
        communication.PartitionNode(initialLeader);

        IRaft[] survivors = nodes.Where(n => n.GetLocalEndpoint() != initialLeader).ToArray();

        // Failover must complete well within SuspicionTimeout (5 s default).
        // We use 3 s as the ceiling to give a meaningful gap from Dead promotion.
        await WaitForConditionAsync(
            async () => await CountLeadersAsync(survivors, 1).ConfigureAwait(false) == 1,
            TestContext.Current.CancellationToken,
            timeoutMs: 3_000);

        string newLeader = await survivors[0].WaitForLeaderStableAsync(
            1, TimeSpan.FromMilliseconds(150), TestContext.Current.CancellationToken);

        Assert.NotEqual(initialLeader, newLeader);

        communication.HealPartition(initialLeader);

        await node1.LeaveCluster(true);
        await node2.LeaveCluster(true);
        await node3.LeaveCluster(true);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private IRaft MakeNode(
        InMemoryCommunication communication,
        int nodeId,
        IEnumerable<RaftNode> peers,
        bool enableQuiescence = true)
    {
        RaftConfiguration config = new()
        {
            NodeName = $"node{nodeId}",
            NodeId = nodeId,
            Host = "localhost",
            Port = 8000 + nodeId,
            InitialPartitions = 1,
            HeartbeatInterval = TimeSpan.FromMilliseconds(50),
            RecentHeartbeat = TimeSpan.FromMilliseconds(25),
            VotingTimeout = TimeSpan.FromMilliseconds(250),
            CheckLeaderInterval = TimeSpan.FromMilliseconds(25),
            UpdateNodesInterval = TimeSpan.FromMilliseconds(100),
            TimerInitialDelay = TimeSpan.FromMilliseconds(25),
            StartElectionTimeout = 500,
            EndElectionTimeout = 1000,
            EnableQuiescence = enableQuiescence,
            QuiesceAfter = TimeSpan.FromMilliseconds(100),
            PingInterval = TimeSpan.FromMilliseconds(200),
            SuspicionTimeout = TimeSpan.FromSeconds(5),
        };

        return new RaftManager(
            config,
            new StaticDiscovery(peers.ToList()),
            new InMemoryWAL(logger),
            communication,
            new HybridLogicalClock(),
            logger);
    }

    private static async Task<IRaft?> GetLeaderAsync(int partitionId, IRaft[] nodes)
    {
        foreach (IRaft node in nodes)
        {
            if (await node.AmILeaderQuick(partitionId).ConfigureAwait(false))
                return node;
        }
        return null;
    }

    private static async Task<int> CountLeadersAsync(IRaft[] nodes, int partitionId)
    {
        int count = 0;
        foreach (IRaft node in nodes)
        {
            if (await node.AmILeaderQuick(partitionId).ConfigureAwait(false))
                count++;
        }
        return count;
    }

    private static async Task WaitForConditionAsync(
        Func<bool> condition,
        CancellationToken cancellationToken,
        int timeoutMs = 15_000)
    {
        long startMs = Environment.TickCount64;
        while (Environment.TickCount64 - startMs < timeoutMs)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (condition())
                return;
            await Task.Delay(25, cancellationToken).ConfigureAwait(false);
        }
        throw new TimeoutException($"Condition not satisfied within {timeoutMs} ms.");
    }

    private static async Task WaitForConditionAsync(
        Func<Task<bool>> condition,
        CancellationToken cancellationToken,
        int timeoutMs = 15_000)
    {
        long startMs = Environment.TickCount64;
        while (Environment.TickCount64 - startMs < timeoutMs)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (await condition().ConfigureAwait(false))
                return;
            await Task.Delay(25, cancellationToken).ConfigureAwait(false);
        }
        throw new TimeoutException($"Condition not satisfied within {timeoutMs} ms.");
    }
}
