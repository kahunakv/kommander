using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.Gossip;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL;
using Kommander.WAL.IO;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.LoadReports;

/// <summary>
/// Unit tests for <see cref="RaftManager.ReceiveTransferLeadershipSuggestion"/>.
/// Validates the guard logic: only acts when this node leads the partition,
/// the partition is Active, the target is a live voter, and ignores suspects/dead nodes.
/// </summary>
public sealed class TestTransferLeadershipSuggestion
{
    private static RaftManager MakeManager(string host = "localhost", int port = 9000)
    {
        RaftConfiguration config = new()
        {
            Host = host,
            Port = port,
            InitialPartitions = 0,
            EnableLeaderBalancer = true,
        };
        InMemoryWAL wal = new(NullLogger<IRaft>.Instance);
        return new RaftManager(
            config,
            new StaticDiscovery([]),
            wal,
            new InMemoryCommunication(),
            new HybridLogicalClock(),
            NullLogger<IRaft>.Instance
        );
    }

    private static TransferLeadershipSuggestionRequest MakeSuggestion(
        int partition, string suggestedBy, string target, long term = 1) =>
        new(partition, term, HLCTimestamp.Zero, suggestedBy, target);

    [Fact]
    public void ReceiveSuggestion_NoPartition_DropsGracefully()
    {
        using RaftManager manager = MakeManager();

        // Partition 99 was never registered — must not throw.
        manager.ReceiveTransferLeadershipSuggestion(
            MakeSuggestion(99, "p0:9000", "peer:9001"));
    }

    [Fact]
    public void ReceiveSuggestion_NotLeader_Drops()
    {
        using RaftManager manager = MakeManager();

        InMemoryWAL wal = new(NullLogger<IRaft>.Instance);
        RaftPartition partition = new(manager, wal, 1, 0, 0, NullLogger<IRaft>.Instance);
        manager.Partitions[1] = partition;
        partition.Leader = "someone-else:9001"; // not us

        manager.ReceiveTransferLeadershipSuggestion(
            MakeSuggestion(1, "p0:9000", "peer:9001"));

        // The partition leader must remain unchanged.
        Assert.Equal("someone-else:9001", partition.Leader);
    }

    [Fact]
    public void ReceiveSuggestion_TargetNotVoter_Drops()
    {
        using RaftManager manager = MakeManager();

        InMemoryWAL wal = new(NullLogger<IRaft>.Instance);
        RaftPartition partition = new(manager, wal, 1, 0, 0, NullLogger<IRaft>.Instance);
        manager.Partitions[1] = partition;
        partition.Leader = manager.LocalEndpoint;
        partition.State = RaftPartitionState.Active;

        // No membership committed — peer:9001 is not a voter.
        manager.ReceiveTransferLeadershipSuggestion(
            MakeSuggestion(1, "p0:9000", "peer:9001"));

        // Partition still led by us (transfer was not fired).
        Assert.Equal(manager.LocalEndpoint, partition.Leader);
    }

    [Fact]
    public async Task ReceiveSuggestion_TargetSuspect_Drops()
    {
        using RaftManager manager = MakeManager();

        // Commit a minimal membership so peer:9001 appears as voter.
        await manager.SystemCoordinator.DrainAsync();

        // Mark peer suspect in the liveness table.
        manager.Liveness.MarkSuspect("peer:9001");

        InMemoryWAL wal = new(NullLogger<IRaft>.Instance);
        RaftPartition partition = new(manager, wal, 1, 0, 0, NullLogger<IRaft>.Instance);
        manager.Partitions[1] = partition;
        partition.Leader = manager.LocalEndpoint;
        partition.State = RaftPartitionState.Active;

        manager.ReceiveTransferLeadershipSuggestion(
            MakeSuggestion(1, "p0:9000", "peer:9001"));

        // Target was suspect — must have been dropped.
        Assert.Equal(manager.LocalEndpoint, partition.Leader);
    }

    [Fact]
    public void ReceiveSuggestion_PartitionNotActive_Drops()
    {
        using RaftManager manager = MakeManager();

        InMemoryWAL wal = new(NullLogger<IRaft>.Instance);
        RaftPartition partition = new(manager, wal, 1, 0, 0, NullLogger<IRaft>.Instance);
        manager.Partitions[1] = partition;
        partition.Leader = manager.LocalEndpoint;
        partition.State = RaftPartitionState.Draining; // not Active

        manager.ReceiveTransferLeadershipSuggestion(
            MakeSuggestion(1, "p0:9000", "peer:9001"));

        Assert.Equal(manager.LocalEndpoint, partition.Leader);
    }
}
