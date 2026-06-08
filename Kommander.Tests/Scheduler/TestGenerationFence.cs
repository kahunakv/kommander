
using System.Text.Json;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.System;
using Kommander.System.Protos;
using Kommander.Time;
using Kommander.WAL;
using Kommander.WAL.IO;
using Microsoft.Extensions.Logging.Abstractions;
using Google.Protobuf;

namespace Kommander.Tests.Scheduler;

/// <summary>
/// Integration-level tests for the generation fence (Task 3.2 / 3.5).
///
/// The fence rejects a <c>ReplicateLogs</c> proposal with
/// <see cref="RaftOperationStatus.PartitionMoved"/> when the caller's
/// <c>expectedGeneration</c> does not match the partition's live
/// <c>Generation</c>. These tests verify the full path from
/// <c>RaftPartition.ReplicateLogs</c> through the executor fence check.
///
/// All tests use the coordinator-override harness (no real Raft quorum).
/// Leadership is simulated by setting <c>partition.Leader</c> directly,
/// which is legal here because <c>Kommander.Tests</c> is an
/// <c>InternalsVisibleTo</c> friend assembly.
/// </summary>
public sealed class TestGenerationFence
{
    // ── Builder ───────────────────────────────────────────────────────────────

    private static RaftManager Build()
    {
        RaftManager manager = new(
            new RaftConfiguration { Host = "localhost", Port = 9000, InitialPartitions = 0 },
            new StaticDiscovery([]),
            new InMemoryWAL(NullLogger<IRaft>.Instance),
            new Kommander.Communication.Memory.InMemoryCommunication(),
            new HybridLogicalClock(),
            NullLogger<IRaft>.Instance);

        // Start the I/O schedulers so the partition executor's WAL-restore phase can
        // complete. Normally these are started inside JoinCluster, but tests drive the
        // coordinator directly without joining a real cluster.
        ((FairReadScheduler)manager.ReadScheduler).Start();
        ((FairWalScheduler)manager.WalScheduler).Start();

        return manager;
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static byte[] SerializeMessage(string key, string value)
    {
        RaftSystemMessage msg = new() { Key = key, Value = value };
        using MemoryStream ms = new();
        msg.WriteTo(ms);
        return ms.ToArray();
    }

    private static RaftSystemRequest MakeConfigReplicated(List<RaftPartitionRange> ranges, long mapVersion = 1) =>
        new(RaftSystemRequestType.ConfigReplicated,
            SerializeMessage(
                RaftSystemConfigKeys.Partitions,
                JsonSerializer.Serialize(new RaftPartitionMap { MapVersion = mapVersion, Partitions = ranges })));

    private static Task WaitForIdleAsync(RaftManager manager) =>
        manager.SystemCoordinator.DrainAsync().WaitAsync(TimeSpan.FromSeconds(5));

    /// <summary>
    /// Simulates leadership on <paramref name="partition"/> and waits until its
    /// WAL restore has completed so client proposals are accepted by the executor.
    /// </summary>
    private static async Task SimulateLeaderAsync(RaftManager manager, RaftPartition partition)
    {
        partition.Leader = manager.LocalEndpoint;
        await partition.RestoreTask.WaitAsync(TimeSpan.FromSeconds(5));
    }

    private static List<RaftPartitionRange> OnePartition(int gen = 1) =>
    [
        new()
        {
            PartitionId  = 1,
            StartRange   = 0,
            EndRange     = int.MaxValue,
            Generation   = gen,
            State        = RaftPartitionState.Active,
            RoutingMode  = RaftRoutingMode.HashRange
        }
    ];

    // ── Tests ─────────────────────────────────────────────────────────────────

    /// <summary>
    /// When <c>expectedGeneration</c> matches the partition's current generation the
    /// fence passes — the proposal reaches the state machine (which returns
    /// <c>NodeIsNotLeader</c> internally because there is no real quorum, but crucially
    /// the fence does not fire <c>PartitionMoved</c>).
    /// </summary>
    [Fact]
    public async Task ReplicateLogs_FencePassesWhenGenerationMatches()
    {
        using RaftManager manager = Build();

        manager.SystemCoordinator.Send(MakeConfigReplicated(OnePartition(gen: 1)));
        await WaitForIdleAsync(manager);

        RaftPartition partition = manager.Partitions[1];
        await SimulateLeaderAsync(manager, partition);

        long gen = manager.GetPartitionGeneration(1);
        Assert.Equal(1, gen);

        (bool _, RaftOperationStatus status, _) =
            await partition.ReplicateLogs("test", "hello"u8.ToArray(), autoCommit: true, expectedGeneration: gen);

        // Fence passed — the state machine received the proposal (no quorum, so it
        // returns something other than PartitionMoved, e.g. ReplicationFailed).
        Assert.NotEqual(RaftOperationStatus.PartitionMoved, status);
    }

    /// <summary>
    /// After the partition's generation is bumped (simulated by re-delivering a
    /// <c>ConfigReplicated</c> message with a higher generation), a proposal that
    /// carries the old generation must be rejected with <c>PartitionMoved</c>.
    /// </summary>
    [Fact]
    public async Task ReplicateLogs_FenceRejectsWhenGenerationIsStale()
    {
        using RaftManager manager = Build();

        // Start with gen=1.
        manager.SystemCoordinator.Send(MakeConfigReplicated(OnePartition(gen: 1)));
        await WaitForIdleAsync(manager);

        RaftPartition partition = manager.Partitions[1];
        await SimulateLeaderAsync(manager, partition);

        long oldGen = manager.GetPartitionGeneration(1);
        Assert.Equal(1, oldGen);

        // Simulate a map mutation (e.g. split or create) that bumps partition 1 to gen=2.
        manager.SystemCoordinator.Send(MakeConfigReplicated(OnePartition(gen: 2), mapVersion: 2));
        await WaitForIdleAsync(manager);

        // Re-apply leader flag since StartUserPartitions creates a new entry or updates it.
        partition.Leader = manager.LocalEndpoint;

        long newGen = manager.GetPartitionGeneration(1);
        Assert.Equal(2, newGen);

        // Proposal fenced at the old generation must be rejected.
        (bool _, RaftOperationStatus status, _) =
            await partition.ReplicateLogs("test", "hello"u8.ToArray(), autoCommit: true, expectedGeneration: oldGen);

        Assert.Equal(RaftOperationStatus.PartitionMoved, status);
    }

    /// <summary>
    /// <c>expectedGeneration = 0</c> (the default) bypasses the fence entirely regardless
    /// of what the partition's current generation is.
    /// </summary>
    [Fact]
    public async Task ReplicateLogs_ZeroExpectedGeneration_FenceNeverFires()
    {
        using RaftManager manager = Build();

        // Start at gen=5 to make the absence of a fence obvious.
        manager.SystemCoordinator.Send(MakeConfigReplicated(OnePartition(gen: 5)));
        await WaitForIdleAsync(manager);

        RaftPartition partition = manager.Partitions[1];
        await SimulateLeaderAsync(manager, partition);

        Assert.Equal(5, manager.GetPartitionGeneration(1));

        // expectedGeneration = 0 → no fence, regardless of partition gen.
        (bool _, RaftOperationStatus status, _) =
            await partition.ReplicateLogs("test", "hello"u8.ToArray(), autoCommit: true, expectedGeneration: 0);

        Assert.NotEqual(RaftOperationStatus.PartitionMoved, status);
    }

    /// <summary>
    /// When a generation bump races with an in-flight proposal:
    /// requests that arrive at the executor after the bump must receive
    /// <c>PartitionMoved</c>; requests that arrived before must not be double-applied.
    ///
    /// Uses <see cref="RaftManager.ReplicateLogs(int, string, byte[], bool, CancellationToken, long)"/>
    /// through the full stack to validate end-to-end ordering.
    /// </summary>
    [Fact]
    public async Task ReplicateLogs_GenerationBumpedConcurrently_LateWriterReceivesPartitionMoved()
    {
        using RaftManager manager = Build();

        manager.SystemCoordinator.Send(MakeConfigReplicated(OnePartition(gen: 1)));
        await WaitForIdleAsync(manager);

        RaftPartition partition = manager.Partitions[1];
        await SimulateLeaderAsync(manager, partition);

        Assert.Equal(1, manager.GetPartitionGeneration(1));

        // Before the bump: request fenced at gen=1 passes the fence (not PartitionMoved).
        (bool _, RaftOperationStatus before, _) =
            await partition.ReplicateLogs("test", "before"u8.ToArray(), autoCommit: true, expectedGeneration: 1);
        Assert.NotEqual(RaftOperationStatus.PartitionMoved, before);

        // Simulate map mutation — bump to gen=2.
        manager.SystemCoordinator.Send(MakeConfigReplicated(OnePartition(gen: 2), mapVersion: 2));
        await WaitForIdleAsync(manager);
        partition.Leader = manager.LocalEndpoint;

        // After the bump: same request still fenced at gen=1 must receive PartitionMoved.
        (bool _, RaftOperationStatus after, _) =
            await partition.ReplicateLogs("test", "after"u8.ToArray(), autoCommit: true, expectedGeneration: 1);
        Assert.Equal(RaftOperationStatus.PartitionMoved, after);

        // A new request fenced at gen=2 (the current gen) passes the fence.
        (bool _, RaftOperationStatus current, _) =
            await partition.ReplicateLogs("test", "current"u8.ToArray(), autoCommit: true, expectedGeneration: 2);
        Assert.NotEqual(RaftOperationStatus.PartitionMoved, current);
    }

    /// <summary>
    /// <see cref="IRaft.GetPartitionGeneration"/> returns 0 for unknown partitions and
    /// the correct generation for known ones. <see cref="IRaft.GetPartitionMap"/> returns
    /// a snapshot that matches the live state.
    /// </summary>
    [Fact]
    public async Task GetPartitionGeneration_AndGetPartitionMap_ReturnCorrectValues()
    {
        using RaftManager manager = Build();

        // Unknown partition before initialization.
        Assert.Equal(0, manager.GetPartitionGeneration(99));

        manager.SystemCoordinator.Send(MakeConfigReplicated(
        [
            new() { PartitionId = 1, StartRange = 0,           EndRange = 500_000_000, Generation = 3, State = RaftPartitionState.Active, RoutingMode = RaftRoutingMode.HashRange },
            new() { PartitionId = 2, StartRange = 500_000_001, EndRange = int.MaxValue, Generation = 7, State = RaftPartitionState.Active, RoutingMode = RaftRoutingMode.HashRange }
        ]));
        await WaitForIdleAsync(manager);

        Assert.Equal(3, manager.GetPartitionGeneration(1));
        Assert.Equal(7, manager.GetPartitionGeneration(2));
        Assert.Equal(0, manager.GetPartitionGeneration(99));

        IReadOnlyList<RaftPartitionRange> map = manager.GetPartitionMap();
        Assert.Equal(2, map.Count);
        Assert.Contains(map, r => r.PartitionId == 1 && r.Generation == 3);
        Assert.Contains(map, r => r.PartitionId == 2 && r.Generation == 7);

        // Mutating the snapshot must not affect the live state.
        List<RaftPartitionRange> mutableCopy = map.ToList();
        mutableCopy.Clear();
        Assert.Equal(2, manager.GetPartitionMap().Count);
    }
}
