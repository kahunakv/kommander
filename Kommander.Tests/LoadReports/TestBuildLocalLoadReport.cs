
using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.Scheduling;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL;
using Kommander.WAL.IO;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.LoadReports;

/// <summary>
/// Unit tests for <see cref="RaftManager.BuildLocalLoadReport"/>: verifies that the
/// builder enumerates only partitions this node currently leads, stamps monotonically
/// increasing <c>ReportVersion</c> values, and populates the expected fields.
/// </summary>
public sealed class TestBuildLocalLoadReport
{
    [Fact]
    public void BuildLocalLoadReport_ListsOnlyLedPartitions()
    {
        using InMemoryWAL wal = new(NullLogger<IRaft>.Instance);

        RaftConfiguration config = new()
        {
            Host = "localhost",
            Port = 9000,
            InitialPartitions = 0,
            EnableLeaderBalancer = true,
            LeaderBalancerOpsWeight = 1.0,
            LeaderBalancerQueueWeight = 0.5,
        };

        using RaftManager manager = new(
            config,
            new StaticDiscovery([]),
            wal,
            new InMemoryCommunication(),
            new HybridLogicalClock(),
            NullLogger<IRaft>.Instance);

        ((FairReadScheduler)manager.ReadScheduler).Start();

        // This node leads partitions 1 and 3; partition 2 is led by a peer.
        RaftPartition p1 = new(manager, wal, 1, 0, 0, NullLogger<IRaft>.Instance);
        RaftPartition p2 = new(manager, wal, 2, 0, 0, NullLogger<IRaft>.Instance);
        RaftPartition p3 = new(manager, wal, 3, 0, 0, NullLogger<IRaft>.Instance);

        try
        {
            manager.Partitions[1] = p1;
            manager.Partitions[2] = p2;
            manager.Partitions[3] = p3;

            p1.Leader = manager.LocalEndpoint;
            p2.Leader = "peer:9001";
            p3.Leader = manager.LocalEndpoint;

            NodeLoadReport report = manager.BuildLocalLoadReport();

            Assert.Equal(manager.LocalEndpoint, report.Endpoint);
            Assert.Equal(1L, report.ReportVersion);
            Assert.Equal(2, report.Leaderships.Count);

            global::System.Collections.Generic.HashSet<int> led =
                [.. report.Leaderships.Select(l => l.PartitionId)];

            Assert.Contains(1, led);
            Assert.Contains(3, led);
            Assert.DoesNotContain(2, led);
        }
        finally
        {
            p1.Dispose();
            p2.Dispose();
            p3.Dispose();
        }
    }

    [Fact]
    public void BuildLocalLoadReport_ReportVersion_IsMonotonicallyIncreasing()
    {
        using InMemoryWAL wal = new(NullLogger<IRaft>.Instance);

        RaftConfiguration config = new()
        {
            Host = "localhost",
            Port = 9000,
            InitialPartitions = 0,
        };

        using RaftManager manager = new(
            config,
            new StaticDiscovery([]),
            wal,
            new InMemoryCommunication(),
            new HybridLogicalClock(),
            NullLogger<IRaft>.Instance);

        ((FairReadScheduler)manager.ReadScheduler).Start();

        NodeLoadReport r1 = manager.BuildLocalLoadReport();
        NodeLoadReport r2 = manager.BuildLocalLoadReport();
        NodeLoadReport r3 = manager.BuildLocalLoadReport();

        Assert.True(r1.ReportVersion < r2.ReportVersion);
        Assert.True(r2.ReportVersion < r3.ReportVersion);
        Assert.Equal(r1.ReportVersion + 1, r2.ReportVersion);
        Assert.Equal(r2.ReportVersion + 1, r3.ReportVersion);
    }

    [Fact]
    public void BuildLocalLoadReport_NoLedPartitions_YieldsEmptyLeaderships()
    {
        using InMemoryWAL wal = new(NullLogger<IRaft>.Instance);

        RaftConfiguration config = new()
        {
            Host = "localhost",
            Port = 9000,
            InitialPartitions = 0,
        };

        using RaftManager manager = new(
            config,
            new StaticDiscovery([]),
            wal,
            new InMemoryCommunication(),
            new HybridLogicalClock(),
            NullLogger<IRaft>.Instance);

        ((FairReadScheduler)manager.ReadScheduler).Start();

        RaftPartition p1 = new(manager, wal, 1, 0, 0, NullLogger<IRaft>.Instance);

        try
        {
            manager.Partitions[1] = p1;
            p1.Leader = "other:9000"; // this node doesn't lead it

            NodeLoadReport report = manager.BuildLocalLoadReport();

            Assert.Equal(manager.LocalEndpoint, report.Endpoint);
            Assert.Empty(report.Leaderships);
        }
        finally
        {
            p1.Dispose();
        }
    }

    [Fact]
    public void BuildLocalLoadReport_LedPartition_HasNonNegativeLeaderSinceMs()
    {
        using InMemoryWAL wal = new(NullLogger<IRaft>.Instance);

        RaftConfiguration config = new()
        {
            Host = "localhost",
            Port = 9000,
            InitialPartitions = 0,
        };

        using RaftManager manager = new(
            config,
            new StaticDiscovery([]),
            wal,
            new InMemoryCommunication(),
            new HybridLogicalClock(),
            NullLogger<IRaft>.Instance);

        ((FairReadScheduler)manager.ReadScheduler).Start();

        RaftPartition p1 = new(manager, wal, 1, 0, 0, NullLogger<IRaft>.Instance);

        try
        {
            manager.Partitions[1] = p1;
            p1.Leader = manager.LocalEndpoint;

            NodeLoadReport report = manager.BuildLocalLoadReport();

            Assert.Single(report.Leaderships);
            Assert.True(report.Leaderships[0].LeaderSinceMs >= 0,
                "LeaderSinceMs must never be negative");
        }
        finally
        {
            p1.Dispose();
        }
    }

    /// <summary>
    /// A partition this node leads, with no <c>ReplicateLogs</c> activity, must emit
    /// <c>LogOpsPerSecond == 0</c> in the load report.
    /// </summary>
    [Fact]
    public void BuildLocalLoadReport_IdleLedPartition_LogOpsPerSecondIsZero()
    {
        using InMemoryWAL wal = new(NullLogger<IRaft>.Instance);

        RaftConfiguration config = new()
        {
            Host = "localhost",
            Port = 9000,
            InitialPartitions = 0,
        };

        using RaftManager manager = new(
            config,
            new StaticDiscovery([]),
            wal,
            new InMemoryCommunication(),
            new HybridLogicalClock(),
            NullLogger<IRaft>.Instance);

        ((FairReadScheduler)manager.ReadScheduler).Start();

        RaftPartition p1 = new(manager, wal, 1, 0, 0, NullLogger<IRaft>.Instance);

        try
        {
            manager.Partitions[1] = p1;
            p1.Leader = manager.LocalEndpoint;

            NodeLoadReport report = manager.BuildLocalLoadReport();

            Assert.Single(report.Leaderships);
            Assert.Equal(0.0, report.Leaderships[0].LogOpsPerSecond);
        }
        finally
        {
            p1.Dispose();
        }
    }

    /// <summary>
    /// After dispatching <c>ReplicateLogs</c> ops through the partition executor,
    /// <c>BuildLocalLoadReport</c> must emit a non-zero <c>LogOpsPerSecond</c> for
    /// the led partition and 0 for a peer-led partition.
    /// </summary>
    [Fact]
    public async Task BuildLocalLoadReport_AfterReplicateLogs_LogOpsPerSecondIsNonZero()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        using InMemoryWAL wal = new(NullLogger<IRaft>.Instance);

        RaftConfiguration config = new()
        {
            Host = "localhost",
            Port = 9000,
            InitialPartitions = 0,
        };

        using RaftManager manager = new(
            config,
            new StaticDiscovery([]),
            wal,
            new InMemoryCommunication(),
            new HybridLogicalClock(),
            NullLogger<IRaft>.Instance);

        ((FairReadScheduler)manager.ReadScheduler).Start();

        // p1 is led locally; p2 is led by a peer and must stay at 0.
        RaftPartition p1 = new(manager, wal, 1, 0, 0, NullLogger<IRaft>.Instance);
        RaftPartition p2 = new(manager, wal, 2, 0, 0, NullLogger<IRaft>.Instance);

        try
        {
            manager.Partitions[1] = p1;
            manager.Partitions[2] = p2;
            p1.Leader = manager.LocalEndpoint;
            p2.Leader = "peer:9001";

            await p1.Executor.RestoreTask;

            // Drive ReplicateLogs load. Empty-log requests return Success immediately
            // (no WAL touch) but still record to the log accumulator.
            for (int i = 0; i < 5; i++)
                p1.Executor.Post(new RaftRequest(
                    RaftRequestType.ReplicateLogs, new List<RaftLog>(), autoCommit: true));

            await p1.Executor.DrainAsync(ct)
                .WaitAsync(TimeSpan.FromSeconds(5), ct);

            NodeLoadReport report = manager.BuildLocalLoadReport();

            PartitionLoad led  = report.Leaderships.Single(l => l.PartitionId == 1);
            Assert.True(led.LogOpsPerSecond > 0.0,
                $"Expected LogOpsPerSecond > 0 for led partition after ReplicateLogs load, got {led.LogOpsPerSecond}.");

            // p2 is peer-led and must not appear in the report at all.
            Assert.DoesNotContain(report.Leaderships, l => l.PartitionId == 2);
        }
        finally
        {
            p1.Dispose();
            p2.Dispose();
        }
    }

    [Fact]
    public void BuildLocalLoadReport_EndpointMatchesManagerLocalEndpoint()
    {
        using InMemoryWAL wal = new(NullLogger<IRaft>.Instance);

        RaftConfiguration config = new()
        {
            Host = "mynode",
            Port = 7777,
            InitialPartitions = 0,
        };

        using RaftManager manager = new(
            config,
            new StaticDiscovery([]),
            wal,
            new InMemoryCommunication(),
            new HybridLogicalClock(),
            NullLogger<IRaft>.Instance);

        ((FairReadScheduler)manager.ReadScheduler).Start();

        NodeLoadReport report = manager.BuildLocalLoadReport();

        Assert.Equal("mynode:7777", report.Endpoint);
    }

    // ── Stability gate wiring ────────────────────────────────────────────────

    /// <summary>
    /// A freshly constructed <see cref="RaftPartition"/> (as created by
    /// <c>StartUserPartitions</c> / <c>CreatePartitionAsync</c>) must emit a
    /// <c>LeaderSinceMs</c> close to zero in the next load report.
    /// <para>
    /// The invariant: <c>_leaderChangedTicks</c> initializes at construction time,
    /// then resets when the <c>Leader</c> property is first set (initial value is <c>""</c>,
    /// so the first non-empty assignment always fires the setter and resets the clock).
    /// <c>BuildLocalLoadReport</c> then computes elapsed ms from that tick, which must be
    /// far below any practical <c>MinLeaderStabilityMs</c>.
    /// </para>
    /// </summary>
    [Fact]
    public void BuildLocalLoadReport_NewlyLedPartition_HasNearZeroLeaderSinceMs()
    {
        using InMemoryWAL wal = new(NullLogger<IRaft>.Instance);

        RaftConfiguration config = new()
        {
            Host = "localhost",
            Port = 9000,
            InitialPartitions = 0,
            MinLeaderStabilityMs = 5_000,
        };

        using RaftManager manager = new(
            config,
            new StaticDiscovery([]),
            wal,
            new InMemoryCommunication(),
            new HybridLogicalClock(),
            NullLogger<IRaft>.Instance);

        ((FairReadScheduler)manager.ReadScheduler).Start();

        RaftPartition p1 = new(manager, wal, 1, 0, 0, NullLogger<IRaft>.Instance);

        try
        {
            manager.Partitions[1] = p1;
            // Setting Leader from "" to a non-empty endpoint resets _leaderChangedTicks,
            // just as the first election does on a real partition.
            p1.Leader = manager.LocalEndpoint;

            NodeLoadReport report = manager.BuildLocalLoadReport();

            Assert.Single(report.Leaderships);
            long sinceMs = report.Leaderships[0].LeaderSinceMs;

            // Must be far below MinLeaderStabilityMs — we just set the leader.
            Assert.True(sinceMs < config.MinLeaderStabilityMs,
                $"Expected LeaderSinceMs ({sinceMs} ms) < MinLeaderStabilityMs " +
                $"({config.MinLeaderStabilityMs} ms) for a freshly led partition.");
        }
        finally
        {
            p1.Dispose();
        }
    }

    /// <summary>
    /// End-to-end: the load report emitted by a freshly led partition feeds a
    /// <see cref="LeaderBalancePlanner"/> pass that returns no moves — confirming the
    /// stability gate covers split-created and newly created partitions without any
    /// special-casing in the planner.
    /// </summary>
    [Fact]
    public void BuildLocalLoadReport_NewlyLedPartition_IsExcludedByStabilityGate()
    {
        using InMemoryWAL wal = new(NullLogger<IRaft>.Instance);

        RaftConfiguration config = new()
        {
            Host = "localhost",
            Port = 9000,
            InitialPartitions = 0,
            EnableLeaderBalancer = true,
            MinLeaderStabilityMs = 5_000,
            CountDeadband = 0,
            MaxMovesPerPass = 8,
            MoveCooldown = TimeSpan.FromSeconds(60),
            LeaderBalancerReportTtl = TimeSpan.FromSeconds(60),
        };

        using RaftManager manager = new(
            config,
            new StaticDiscovery([]),
            wal,
            new InMemoryCommunication(),
            new HybridLogicalClock(),
            NullLogger<IRaft>.Instance);

        ((FairReadScheduler)manager.ReadScheduler).Start();

        // Local node leads 5 partitions (all just created — LeaderSinceMs ≈ 0).
        // Peer leads 1 partition that has been stable. Without the stability gate this
        // imbalance would trigger moves; the gate must suppress them all.
        for (int id = 1; id <= 5; id++)
        {
            RaftPartition p = new(manager, wal, id, 0, 0, NullLogger<IRaft>.Instance);
            manager.Partitions[id] = p;
            p.Leader = manager.LocalEndpoint; // fresh leader — resets LeaderChangedTicks
        }

        NodeLoadReport localReport = manager.BuildLocalLoadReport();

        // Peer has one long-stable partition.
        NodeLoadReport peerReport = new()
        {
            Endpoint = "peer:9001",
            ReportVersion = 1,
            Time = manager.HybridLogicalClock.SendOrLocalEvent(0),
            Leaderships = [new PartitionLoad { PartitionId = 6, Load = 1.0, LeaderSinceMs = 60_000 }],
        };

        IReadOnlyList<NodeLoadReport> allReports = [localReport, peerReport];
        IReadOnlyList<ClusterMember> members =
        [
            new ClusterMember { Endpoint = manager.LocalEndpoint, Role = ClusterMemberRole.Voter },
            new ClusterMember { Endpoint = "peer:9001",           Role = ClusterMemberRole.Voter },
        ];

        GlobalLeadershipView view = GlobalLeadershipView.Build(
            allReports,
            members,
            aliveEndpoints: new global::System.Collections.Generic.HashSet<string>(members.Select(m => m.Endpoint)),
            config.LeaderBalancerReportTtl,
            localReport.Time + TimeSpan.FromMilliseconds(100));

        RaftPartitionMap map = new()
        {
            MapVersion = 1,
            Partitions = Enumerable.Range(1, 6)
                .Select(id => new RaftPartitionRange { PartitionId = id, State = RaftPartitionState.Active })
                .ToList(),
        };

        IReadOnlyList<LeaderMove> moves = LeaderBalancePlanner.Plan(
            view, map, config,
            cooldownState: new global::System.Collections.Generic.Dictionary<int, global::System.DateTimeOffset>(),
            now: global::System.DateTimeOffset.UtcNow);

        Assert.Empty(moves);

        // Cleanup
        foreach (RaftPartition p in manager.Partitions.Values)
            p.Dispose();
    }
}
