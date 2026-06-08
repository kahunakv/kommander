
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
/// Functional tests for the two-phase split protocol (Task 4.4).
///
/// Tests cover:
///   1. Basic split correctness (ranges, generations, state)
///   2. No-gap / no-overlap invariant for HashRange partitions
///   3. Unfenced reads during Splitting state are not blocked
///   4. Fenced writes with a stale generation receive PartitionMoved during Phase 1
///   5. Generation fence holds after full cutover (Phase 2)
///   6. Crash mid-split: coordinator detects Splitting pair and resumes Phase 2
///   7. Idempotency: a second split on an already-Splitting source is rejected
/// </summary>
public sealed class TestSplitPartition
{
    // ── Builders ───────────────────────────────────────────────────────────────

    private static RaftManager Build() => new(
        new RaftConfiguration { Host = "localhost", Port = 9000, InitialPartitions = 0 },
        new StaticDiscovery([]),
        new InMemoryWAL(NullLogger<IRaft>.Instance),
        new Kommander.Communication.Memory.InMemoryCommunication(),
        new HybridLogicalClock(),
        NullLogger<IRaft>.Instance);

    /// <summary>
    /// Builds a manager with the I/O schedulers started, which is required for
    /// partition executor operations (ReplicateLogs, fence check).
    /// Normally the schedulers are started inside JoinCluster; tests drive the
    /// coordinator directly so they must start the schedulers manually.
    /// </summary>
    private static RaftManager BuildWithSchedulers()
    {
        RaftManager manager = Build();
        ((FairReadScheduler)manager.ReadScheduler).Start();
        ((FairWalScheduler)manager.WalScheduler).Start();
        return manager;
    }

    // ── Helpers ────────────────────────────────────────────────────────────────

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

    private static async Task SimulateLeaderAsync(RaftManager manager, RaftPartition partition)
    {
        partition.Leader = manager.LocalEndpoint;
        await partition.RestoreTask.WaitAsync(TimeSpan.FromSeconds(5));
    }

    private static Func<string, byte[], bool, CancellationToken, Task<RaftReplicationResult>> AlwaysSucceed() =>
        (_, _, _, _) => Task.FromResult(new RaftReplicationResult(true, RaftOperationStatus.Success, HLCTimestamp.Zero, 1));

    // ── Test 1: Basic split correctness ───────────────────────────────────────

    [Fact]
    public async Task Split_BothHalvesActiveWithCorrectRangesAndGenerations()
    {
        using RaftManager manager = Build();

        manager.SystemCoordinator.Send(MakeConfigReplicated(
        [
            new() { PartitionId = 1, StartRange = 0, EndRange = 999, Generation = 1, State = RaftPartitionState.Active, RoutingMode = RaftRoutingMode.HashRange }
        ]));
        await WaitForIdleAsync(manager);

        manager.SystemCoordinator.ReplicateOverride = AlwaysSucceed();

        TaskCompletionSource done = new(TaskCreationOptions.RunContinuationsAsynchronously);
        int startCount = 0;
        List<RaftPartitionRange>? finalRanges = null;
        manager.SystemCoordinator.StartPartitionsOverride = ranges =>
        {
            manager.StartUserPartitions(ranges);
            if (++startCount == 2)
            {
                finalRanges = [..ranges];
                done.TrySetResult();
            }
        };

        manager.SystemCoordinator.Send(new RaftSystemRequest(
            1, new RaftSplitPlan { TargetRoutingMode = RaftRoutingMode.HashRange, AutoCommit = true }));
        await done.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        Assert.NotNull(finalRanges);
        Assert.Equal(2, finalRanges.Count);

        List<RaftPartitionRange> sorted = [..finalRanges.OrderBy(r => r.StartRange)];

        // Both halves are Active with HashRange routing after Phase 2.
        Assert.All(sorted, r => Assert.Equal(RaftPartitionState.Active, r.State));
        Assert.All(sorted, r => Assert.Equal(RaftRoutingMode.HashRange, r.RoutingMode));

        // Original bounds are preserved.
        Assert.Equal(0, sorted[0].StartRange);
        Assert.Equal(999, sorted[1].EndRange);

        // No gap between the two halves.
        Assert.Equal(sorted[0].EndRange + 1, sorted[1].StartRange);

        // Source: baseline=1, Phase1→2, Phase2→3. Target: Phase1→1, Phase2→2.
        Assert.Equal(3, sorted.Single(r => r.PartitionId == 1).Generation);
        Assert.Equal(2, sorted.Single(r => r.PartitionId != 1).Generation);

        // Live partition dict contains both.
        Assert.Equal(2, manager.Partitions.Count);
    }

    // ── Test 2: No-gap / no-overlap invariant ─────────────────────────────────

    [Fact]
    public async Task Split_NoGapNoOverlap_HashRangeCoversFullDomain()
    {
        using RaftManager manager = Build();

        manager.SystemCoordinator.Send(MakeConfigReplicated(
        [
            new() { PartitionId = 1, StartRange = 0, EndRange = int.MaxValue, Generation = 1, State = RaftPartitionState.Active, RoutingMode = RaftRoutingMode.HashRange }
        ]));
        await WaitForIdleAsync(manager);

        manager.SystemCoordinator.ReplicateOverride = AlwaysSucceed();

        TaskCompletionSource done = new(TaskCreationOptions.RunContinuationsAsynchronously);
        int startCount = 0;
        List<RaftPartitionRange>? hashRanges = null;
        manager.SystemCoordinator.StartPartitionsOverride = ranges =>
        {
            manager.StartUserPartitions(ranges);
            if (++startCount == 2)
            {
                hashRanges = [..ranges.Where(r => r.RoutingMode == RaftRoutingMode.HashRange)];
                done.TrySetResult();
            }
        };

        manager.SystemCoordinator.Send(new RaftSystemRequest(
            1, new RaftSplitPlan { TargetRoutingMode = RaftRoutingMode.HashRange, AutoCommit = true }));
        await done.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        Assert.NotNull(hashRanges);
        List<RaftPartitionRange> sorted = [..hashRanges.OrderBy(r => r.StartRange)];

        // Cover starts at 0 and ends at int.MaxValue.
        Assert.Equal(0, sorted[0].StartRange);
        Assert.Equal(int.MaxValue, sorted[^1].EndRange);

        // No gaps or overlaps.
        for (int i = 0; i < sorted.Count - 1; i++)
            Assert.Equal(sorted[i].EndRange + 1, sorted[i + 1].StartRange);
    }

    // ── Test 3: Reads during Splitting state are not frozen ───────────────────

    [Fact]
    public async Task Split_UnfencedReadDuringSplitting_IsNotFrozen()
    {
        using RaftManager manager = BuildWithSchedulers();

        manager.SystemCoordinator.Send(MakeConfigReplicated(
        [
            new() { PartitionId = 1, StartRange = 0, EndRange = int.MaxValue, Generation = 1, State = RaftPartitionState.Active, RoutingMode = RaftRoutingMode.HashRange }
        ]));
        await WaitForIdleAsync(manager);

        RaftPartition partition = manager.Partitions[1];
        await SimulateLeaderAsync(manager, partition);

        // Block Phase 2's replication so the partition stays in Splitting state long
        // enough to issue operations against it.
        int replicateCallCount = 0;
        TaskCompletionSource phase2Arrived = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource phase2Release = new(TaskCreationOptions.RunContinuationsAsynchronously);

        manager.SystemCoordinator.ReplicateOverride = async (_, _, _, _) =>
        {
            int n = Interlocked.Increment(ref replicateCallCount);
            if (n == 2) { phase2Arrived.TrySetResult(); await phase2Release.Task; }
            return new RaftReplicationResult(true, RaftOperationStatus.Success, HLCTimestamp.Zero, n);
        };

        TaskCompletionSource done = new(TaskCreationOptions.RunContinuationsAsynchronously);
        int startCount = 0;
        manager.SystemCoordinator.StartPartitionsOverride = ranges =>
        {
            manager.StartUserPartitions(ranges);
            if (++startCount == 2) done.TrySetResult();
        };

        manager.SystemCoordinator.Send(new RaftSystemRequest(
            1, new RaftSplitPlan { TargetRoutingMode = RaftRoutingMode.HashRange, AutoCommit = true }));

        // Wait until Phase 1 has committed and Phase 2 is blocked in Replicate.
        await phase2Arrived.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        // Re-acquire partition (StartUserPartitions may have updated the entry) and re-set leader.
        partition = manager.Partitions[1];
        partition.Leader = manager.LocalEndpoint;

        // Unfenced read (expectedGeneration=0) must not be rejected with PartitionMoved.
        (bool _, RaftOperationStatus status, _) =
            await partition.ReplicateLogs("test", "read-during-split"u8.ToArray(), autoCommit: true, expectedGeneration: 0);

        Assert.NotEqual(RaftOperationStatus.PartitionMoved, status);

        // Release Phase 2 and wait for full split completion.
        phase2Release.TrySetResult();
        await done.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
    }

    // ── Test 4: Fenced write with stale generation is rejected during Phase 1 ─

    [Fact]
    public async Task Split_FencedWriteWithStaleGeneration_ReceivesPartitionMoved_DuringPhase1()
    {
        using RaftManager manager = BuildWithSchedulers();

        manager.SystemCoordinator.Send(MakeConfigReplicated(
        [
            new() { PartitionId = 1, StartRange = 0, EndRange = int.MaxValue, Generation = 1, State = RaftPartitionState.Active, RoutingMode = RaftRoutingMode.HashRange }
        ]));
        await WaitForIdleAsync(manager);

        RaftPartition partition = manager.Partitions[1];
        await SimulateLeaderAsync(manager, partition);

        long preSplitGen = manager.GetPartitionGeneration(1);

        int replicateCallCount = 0;
        TaskCompletionSource phase2Arrived = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource phase2Release = new(TaskCreationOptions.RunContinuationsAsynchronously);

        manager.SystemCoordinator.ReplicateOverride = async (_, _, _, _) =>
        {
            int n = Interlocked.Increment(ref replicateCallCount);
            if (n == 2) { phase2Arrived.TrySetResult(); await phase2Release.Task; }
            return new RaftReplicationResult(true, RaftOperationStatus.Success, HLCTimestamp.Zero, n);
        };

        TaskCompletionSource done = new(TaskCreationOptions.RunContinuationsAsynchronously);
        int startCount = 0;
        manager.SystemCoordinator.StartPartitionsOverride = ranges =>
        {
            manager.StartUserPartitions(ranges);
            if (++startCount == 2) done.TrySetResult();
        };

        manager.SystemCoordinator.Send(new RaftSystemRequest(
            1, new RaftSplitPlan { TargetRoutingMode = RaftRoutingMode.HashRange, AutoCommit = true }));

        // Wait until Phase 1 committed: source is now Splitting at preSplitGen+1.
        await phase2Arrived.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        partition = manager.Partitions[1];
        partition.Leader = manager.LocalEndpoint;

        // Write fenced at the pre-Phase-1 generation must be rejected.
        (bool _, RaftOperationStatus status, _) =
            await partition.ReplicateLogs("test", "stale-write"u8.ToArray(), autoCommit: true, expectedGeneration: preSplitGen);

        Assert.Equal(RaftOperationStatus.PartitionMoved, status);

        phase2Release.TrySetResult();
        await done.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
    }

    // ── Test 5: Generation fence holds after full cutover (Phase 2) ───────────

    [Fact]
    public async Task Split_GenerationFenceAfterFullCutover_RejectsPreSplitGeneration()
    {
        using RaftManager manager = BuildWithSchedulers();

        manager.SystemCoordinator.Send(MakeConfigReplicated(
        [
            new() { PartitionId = 1, StartRange = 0, EndRange = int.MaxValue, Generation = 1, State = RaftPartitionState.Active, RoutingMode = RaftRoutingMode.HashRange }
        ]));
        await WaitForIdleAsync(manager);

        RaftPartition partition = manager.Partitions[1];
        await SimulateLeaderAsync(manager, partition);

        long preSplitGen = manager.GetPartitionGeneration(1);

        manager.SystemCoordinator.ReplicateOverride = AlwaysSucceed();

        TaskCompletionSource done = new(TaskCreationOptions.RunContinuationsAsynchronously);
        int startCount = 0;
        manager.SystemCoordinator.StartPartitionsOverride = ranges =>
        {
            manager.StartUserPartitions(ranges);
            if (++startCount == 2) done.TrySetResult();
        };

        manager.SystemCoordinator.Send(new RaftSystemRequest(
            1, new RaftSplitPlan { TargetRoutingMode = RaftRoutingMode.HashRange, AutoCommit = true }));
        await done.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        long postSplitGen = manager.GetPartitionGeneration(1);
        // Two phases each bump source generation by 1.
        Assert.Equal(preSplitGen + 2, postSplitGen);

        // Re-acquire post-split partition and simulate leader.
        partition = manager.Partitions[1];
        partition.Leader = manager.LocalEndpoint;

        // Pre-split generation must be rejected.
        (bool _, RaftOperationStatus staleStatus, _) =
            await partition.ReplicateLogs("test", "stale"u8.ToArray(), autoCommit: true, expectedGeneration: preSplitGen);
        Assert.Equal(RaftOperationStatus.PartitionMoved, staleStatus);

        // Current (post-split) generation must pass the fence.
        (bool _, RaftOperationStatus currentStatus, _) =
            await partition.ReplicateLogs("test", "current"u8.ToArray(), autoCommit: true, expectedGeneration: postSplitGen);
        Assert.NotEqual(RaftOperationStatus.PartitionMoved, currentStatus);
    }

    // ── Test 6: Crash after Phase 1 — coordinator resumes Phase 2 on restart ──

    [Fact]
    public async Task Split_CrashAfterPhase1_InitializePartitionsResumesPhase2()
    {
        using RaftManager manager = Build();

        // Simulate the persisted state after a crash mid-split:
        // Phase 1 was committed but Phase 2 was not.
        // source.EndRange + 1 == target.StartRange  →  crash-recovery pairs them.
        List<RaftPartitionRange> splittingMap =
        [
            new() { PartitionId = 1, StartRange = 0, EndRange = 499, Generation = 2, State = RaftPartitionState.Splitting, RoutingMode = RaftRoutingMode.HashRange },
            new() { PartitionId = 2, StartRange = 500, EndRange = 999, Generation = 1, State = RaftPartitionState.Splitting, RoutingMode = RaftRoutingMode.HashRange }
        ];

        manager.SystemCoordinator.ReplicateOverride = AlwaysSucceed();

        TaskCompletionSource done = new(TaskCreationOptions.RunContinuationsAsynchronously);
        List<RaftPartitionRange>? recoveredRanges = null;
        manager.SystemCoordinator.StartPartitionsOverride = ranges =>
        {
            manager.StartUserPartitions(ranges);
            // Fire done when Phase 2 completes: all ranges in map are Active.
            if (ranges.Count > 0 && ranges.All(r => r.State == RaftPartitionState.Active))
            {
                recoveredRanges = [..ranges];
                done.TrySetResult();
            }
        };

        // Delivering the Splitting map triggers InitializePartitions which detects the
        // Splitting pair and re-enqueues SplitPartitionCommit to complete Phase 2.
        manager.SystemCoordinator.Send(MakeConfigReplicated(splittingMap, mapVersion: 2));
        await done.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        Assert.NotNull(recoveredRanges);
        Assert.Equal(2, recoveredRanges.Count);
        Assert.All(recoveredRanges, r => Assert.Equal(RaftPartitionState.Active, r.State));

        // Phase 2 bumps both generations: source 2→3, target 1→2.
        Assert.Equal(3, recoveredRanges.Single(r => r.PartitionId == 1).Generation);
        Assert.Equal(2, recoveredRanges.Single(r => r.PartitionId == 2).Generation);

        // Both halves are live.
        Assert.True(manager.Partitions.ContainsKey(1));
        Assert.True(manager.Partitions.ContainsKey(2));
    }

    // ── Test 7: Idempotency — duplicate split while Splitting is rejected ─────

    [Fact]
    public async Task Split_DuplicateRequestWhileSplitting_IsRejectedWithError()
    {
        using RaftManager manager = Build();

        manager.SystemCoordinator.Send(MakeConfigReplicated(
        [
            new() { PartitionId = 1, StartRange = 0, EndRange = 999, Generation = 1, State = RaftPartitionState.Active, RoutingMode = RaftRoutingMode.HashRange }
        ]));
        await WaitForIdleAsync(manager);

        manager.SystemCoordinator.ReplicateOverride = AlwaysSucceed();
        manager.SystemCoordinator.StartPartitionsOverride = ranges => manager.StartUserPartitions(ranges);

        // Phase 1 without AutoCommit: source enters Splitting, no Phase 2 queued.
        manager.SystemCoordinator.Send(new RaftSystemRequest(
            1, new RaftSplitPlan { TargetRoutingMode = RaftRoutingMode.HashRange, AutoCommit = false }));
        await WaitForIdleAsync(manager);

        // Source generation must have been bumped to 2 (Phase 1 committed).
        Assert.Equal(2, manager.Partitions[1].Generation);

        // Attempt a second split while source is still in Splitting state.
        TaskCompletionSource<(RaftOperationStatus Status, long Generation)> tcs =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        manager.SystemCoordinator.Send(new RaftSystemRequest(
            1, new RaftSplitPlan { TargetRoutingMode = RaftRoutingMode.HashRange, AutoCommit = false }, tcs));
        await WaitForIdleAsync(manager);

        (RaftOperationStatus status, long gen) =
            await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        // Guard must have rejected the duplicate: Errored with the current (Splitting) generation.
        Assert.Equal(RaftOperationStatus.Errored, status);
        Assert.Equal(2, gen);
    }
}
