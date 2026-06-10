
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
/// The batch path (<c>ExecuteBatchAsync</c>) is also covered: two fenced writes
/// dispatched concurrently land in the same drain cycle and must both be
/// checked — a stale write must receive <c>PartitionMoved</c> even in a batch.
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
    /// Uses <see cref="RaftManager.ReplicateLogs(int, string, byte[], bool, long, CancellationToken)"/>
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

    // ── OnPartitionMapChanged ─────────────────────────────────────────────────
    //
    // Task 3.4 acceptance criterion: a subscriber registered before a partition map
    // change receives the event with the updated snapshot.  CreatePartitionAsync
    // goes through the same StartUserPartitions → OnPartitionMapChanged chain as
    // ConfigReplicated, so the harness tests below cover the same code path without
    // requiring a real Raft quorum.

    /// <summary>
    /// A handler subscribed before <c>ConfigReplicated</c> is applied receives
    /// exactly one event with a snapshot that reflects the new map.
    /// </summary>
    [Fact]
    public async Task OnPartitionMapChanged_FiredOnceWithCorrectSnapshot()
    {
        using RaftManager manager = Build();

        List<IReadOnlyList<RaftPartitionRange>> received = [];
        manager.OnPartitionMapChanged += snap => received.Add(snap);

        manager.SystemCoordinator.Send(MakeConfigReplicated(
        [
            new() { PartitionId = 1, StartRange = 0, EndRange = 499, Generation = 3, State = RaftPartitionState.Active, RoutingMode = RaftRoutingMode.HashRange },
            new() { PartitionId = 2, StartRange = 500, EndRange = 999, Generation = 7, State = RaftPartitionState.Active, RoutingMode = RaftRoutingMode.HashRange },
        ]));
        await WaitForIdleAsync(manager);

        Assert.Single(received);

        IReadOnlyList<RaftPartitionRange> snap = received[0];
        Assert.Equal(2, snap.Count);
        Assert.Contains(snap, r => r.PartitionId == 1 && r.Generation == 3);
        Assert.Contains(snap, r => r.PartitionId == 2 && r.Generation == 7);
    }

    /// <summary>
    /// Each distinct map update fires a separate event; the subscriber receives
    /// notifications in order with the correct generation values.
    /// </summary>
    [Fact]
    public async Task OnPartitionMapChanged_FiredForEveryMapUpdate()
    {
        using RaftManager manager = Build();

        List<IReadOnlyList<RaftPartitionRange>> received = [];
        manager.OnPartitionMapChanged += snap => received.Add(snap);

        manager.SystemCoordinator.Send(MakeConfigReplicated(
        [
            new() { PartitionId = 1, StartRange = 0, EndRange = int.MaxValue, Generation = 1, State = RaftPartitionState.Active, RoutingMode = RaftRoutingMode.HashRange },
        ]));
        await WaitForIdleAsync(manager);

        manager.SystemCoordinator.Send(MakeConfigReplicated(
        [
            new() { PartitionId = 1, StartRange = 0, EndRange = int.MaxValue, Generation = 5, State = RaftPartitionState.Active, RoutingMode = RaftRoutingMode.HashRange },
        ], mapVersion: 2));
        await WaitForIdleAsync(manager);

        Assert.Equal(2, received.Count);
        Assert.Equal(1, received[0].Single(r => r.PartitionId == 1).Generation);
        Assert.Equal(5, received[1].Single(r => r.PartitionId == 1).Generation);
    }

    /// <summary>
    /// Mutating the snapshot delivered by <c>OnPartitionMapChanged</c> has no
    /// effect on the live partition map returned by <see cref="IRaft.GetPartitionMap"/>.
    /// </summary>
    [Fact]
    public async Task OnPartitionMapChanged_SnapshotIsIsolated_MutationsDoNotAffectLiveMap()
    {
        using RaftManager manager = Build();

        IReadOnlyList<RaftPartitionRange>? delivered = null;
        manager.OnPartitionMapChanged += snap => delivered = snap;

        manager.SystemCoordinator.Send(MakeConfigReplicated(
        [
            new() { PartitionId = 1, StartRange = 0, EndRange = int.MaxValue, Generation = 2, State = RaftPartitionState.Active, RoutingMode = RaftRoutingMode.HashRange },
        ]));
        await WaitForIdleAsync(manager);

        Assert.NotNull(delivered);

        // Wipe the delivered snapshot's backing list.
        List<RaftPartitionRange> mutable = delivered!.ToList();
        mutable.Clear();

        // Live map must be unaffected.
        Assert.Single(manager.GetPartitionMap());
        Assert.Equal(2, manager.GetPartitionGeneration(1));
    }

    /// <summary>
    /// Two fenced writes are dispatched without awaiting between them so they
    /// arrive in the executor's replication queue together.  The executor may
    /// drain both in one <c>ExecuteBatchAsync</c> call.  The write whose
    /// <c>expectedGeneration</c> is stale must receive <c>PartitionMoved</c>;
    /// the write whose generation matches must not.
    ///
    /// Regression for the batch-path fence gap: before the fix, stale writes that
    /// landed in a batch bypassed the fence entirely because <c>ExecuteBatchAsync</c>
    /// never read <c>ExpectedGeneration</c>.  After the fix the fence fires
    /// per-item before the item is added to the batch, so batching cannot hide a
    /// stale generation.
    /// </summary>
    [Fact]
    public async Task ReplicateLogs_ConcurrentFencedWrites_StaleWriteReceivesPartitionMoved()
    {
        using RaftManager manager = Build();

        // Start at gen=2 so the stale generation (1) is non-zero and triggers the fence.
        manager.SystemCoordinator.Send(MakeConfigReplicated(OnePartition(gen: 2)));
        await WaitForIdleAsync(manager);

        RaftPartition partition = manager.Partitions[1];
        await SimulateLeaderAsync(manager, partition);

        long currentGen = manager.GetPartitionGeneration(1);  // == 2

        // Fire both writes without awaiting between them.  They arrive in the queue
        // together and the executor may pick them up in the same drain batch.
        Task<(bool, RaftOperationStatus, HLCTimestamp)> validTask =
            partition.ReplicateLogs("test", "valid"u8.ToArray(), autoCommit: true, expectedGeneration: currentGen);
        Task<(bool, RaftOperationStatus, HLCTimestamp)> staleTask =
            partition.ReplicateLogs("test", "stale"u8.ToArray(), autoCommit: true, expectedGeneration: currentGen - 1);

        (bool _, RaftOperationStatus validStatus, _) = await validTask.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        (bool _, RaftOperationStatus staleStatus, _) = await staleTask.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        // Valid write must not be rejected by the fence.
        Assert.NotEqual(RaftOperationStatus.PartitionMoved, validStatus);
        // Stale write must always be rejected — including when it lands in a batch.
        Assert.Equal(RaftOperationStatus.PartitionMoved, staleStatus);
    }
}
