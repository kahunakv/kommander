using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests;

/// <summary>
/// The per-partition retention and diagnostic calls of <see cref="IRaft"/> must reach the system
/// partition as well as user partitions. Kommander holds partition 0 apart from the user partition
/// table, and a lookup in that table alone turned these calls into silent no-ops on partition 0:
/// compaction could then remove application deltas that the application had protected.
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
public sealed class TestSystemPartitionRetentionApi
{
    private static async Task<(RaftManager Manager, InMemoryWAL Wal)> JoinSingleNode(CancellationToken ct)
    {
        InMemoryWAL wal = new(NullLogger<IRaft>.Instance);

        RaftManager manager = new(
            new RaftConfiguration { Host = "localhost", Port = 19910, InitialPartitions = 1 },
            new StaticDiscovery([]),
            wal,
            new InMemoryCommunication(),
            new HybridLogicalClock(),
            NullLogger<IRaft>.Instance);

        await manager.JoinCluster(ct);
        await manager.WaitForLeader(RaftSystemConfig.SystemPartition, ct);
        return (manager, wal);
    }

    [Fact]
    public async Task SetMinRetainIndex_OnSystemPartition_SetsTheFloor()
    {
        (RaftManager manager, _) = await JoinSingleNode(TestContext.Current.CancellationToken);
        using RaftManager owned = manager;

        manager.SetMinRetainIndex(RaftSystemConfig.SystemPartition, 42);
        Assert.Equal(42, manager.SystemPartition!.EffectiveRetentionFloor);

        manager.SetMinRetainIndex(RaftSystemConfig.SystemPartition, 0);
        Assert.Equal(long.MaxValue, manager.SystemPartition!.EffectiveRetentionFloor);
    }

    [Fact]
    public async Task AcquireRetentionHold_OnSystemPartition_HoldsUntilDisposed()
    {
        (RaftManager manager, _) = await JoinSingleNode(TestContext.Current.CancellationToken);
        using RaftManager owned = manager;

        IDisposable hold = manager.AcquireRetentionHold(RaftSystemConfig.SystemPartition, 10);
        Assert.Equal(10, manager.SystemPartition!.EffectiveRetentionFloor);

        hold.Dispose();
        Assert.Equal(long.MaxValue, manager.SystemPartition!.EffectiveRetentionFloor);
    }

    [Fact]
    public async Task Diagnostics_OnSystemPartition_ReportTheHostedPartition()
    {
        (RaftManager manager, _) = await JoinSingleNode(TestContext.Current.CancellationToken);
        using RaftManager owned = manager;

        // -1 means "not hosted", and this node hosts partition 0.
        Assert.True(manager.GetStaleProposedSkippedCount(RaftSystemConfig.SystemPartition) >= 0);
    }

    /// <summary>
    /// The contract that the floor exists for: application deltas at or above the floor survive a
    /// compaction pass through a checkpoint that sits above them.
    /// </summary>
    [Fact]
    public async Task Compaction_OnSystemPartition_KeepsDeltasProtectedThroughThePublicApi()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (RaftManager manager, InMemoryWAL wal) = await JoinSingleNode(ct);
        using RaftManager owned = manager;

        const int partition = RaftSystemConfig.SystemPartition;

        long protectedDelta = -1;
        for (int i = 0; i < 4; i++)
        {
            RaftReplicationResult write = await manager.ReplicateLogs(partition, "delta", [1], cancellationToken: ct);
            Assert.True(write.Success, $"Delta write failed: {write.Status}");

            if (i == 1)
                protectedDelta = write.LogIndex;
        }

        RaftReplicationResult checkpoint = await manager.ReplicateCheckpoint(partition, ct);
        Assert.True(checkpoint.Success, $"Checkpoint failed: {checkpoint.Status}");
        Assert.True(wal.GetLastCheckpoint(partition) > protectedDelta);

        manager.SetMinRetainIndex(partition, protectedDelta);

        await manager.SystemPartition!.CompactForTestingAsync();

        List<RaftLog> remaining = wal.ReadLogsRange(partition, 1, 1000);
        Assert.Contains(remaining, log => log.Id == protectedDelta);
        Assert.DoesNotContain(remaining, log => log.Id < protectedDelta);
    }
}
