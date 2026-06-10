
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
///   8. Splitting a Draining partition (mid-merge) is rejected with Errored
///   9. Explicit target partition id that collides with an existing entry is rejected with Errored
///  10. Out-of-range HashBoundary (too low, too high) is rejected with Errored
///  11. Single-element partition (StartRange == EndRange) produces an auto-boundary that is rejected
///  12. Crash after Phase 1 of an Unrouted split: InitializePartitions resumes Phase 2
///  13. Unrouted split: both partitions end up Active with Unrouted routing and zero ranges
///  14. Explicit HashBoundary is honoured: ranges reflect the caller-specified split point
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

    /// <summary>
    /// Simulates a WAL-restore entry (as opposed to a live replication event).
    /// ConfigRestored populates systemConfiguration but does NOT call StartPartitions or
    /// trigger crash recovery — that happens via the subsequent RestoreCompleted message.
    /// </summary>
    private static RaftSystemRequest MakeConfigRestored(List<RaftPartitionRange> ranges, long mapVersion = 1) =>
        new(RaftSystemRequestType.ConfigRestored,
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

        // ConfigRestored replays the WAL entry into systemConfiguration without calling
        // StartPartitions or crash recovery (mirrors real WAL-replay behaviour).
        // RestoreCompleted then fires InitializePartitions(crashRecovery: true), which
        // applies the map, detects the Splitting pair, and re-enqueues SplitPartitionCommit.
        // Crash recovery is the responsibility of the node that discovers the persisted
        // Splitting state via WAL replay — not of every follower on every LeaderChanged.
        manager.SystemCoordinator.Send(MakeConfigRestored(splittingMap, mapVersion: 2));
        manager.SystemCoordinator.Send(new RaftSystemRequest(RaftSystemRequestType.RestoreCompleted));
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

    // ── Test 6b: ConfigReplicated must NOT trigger crash-recovery re-enqueuing ──

    /// <summary>
    /// Followers must never drive Phase 2 commits. ConfigReplicated is a live replication
    /// event and must call InitializePartitions(crashRecovery: false). Verifies that
    /// delivering a Splitting map via ConfigReplicated alone does NOT enqueue
    /// SplitPartitionCommit (which would cause a follower to attempt replication).
    /// See also Split_LeaderChangedFollower_DoesNotTriggerCrashRecoveryReEnqueue for
    /// the LeaderChanged (follower) variant.
    /// </summary>
    [Fact]
    public async Task Split_ConfigReplicated_DoesNotTriggerCrashRecoveryReEnqueue()
    {
        using RaftManager manager = Build();

        List<RaftPartitionRange> splittingMap =
        [
            new() { PartitionId = 1, StartRange = 0, EndRange = 499, Generation = 2, State = RaftPartitionState.Splitting, RoutingMode = RaftRoutingMode.HashRange },
            new() { PartitionId = 2, StartRange = 500, EndRange = 999, Generation = 1, State = RaftPartitionState.Splitting, RoutingMode = RaftRoutingMode.HashRange }
        ];

        bool replicateCalled = false;
        manager.SystemCoordinator.ReplicateOverride = (_, _, _, _) =>
        {
            replicateCalled = true;
            return Task.FromResult(new RaftReplicationResult(true, RaftOperationStatus.Success, HLCTimestamp.Zero, 1));
        };
        manager.SystemCoordinator.StartPartitionsOverride = ranges => manager.StartUserPartitions(ranges);

        manager.SystemCoordinator.Send(MakeConfigReplicated(splittingMap, mapVersion: 2));
        await WaitForIdleAsync(manager);

        // No SplitPartitionCommit was enqueued — replication must not have been called.
        Assert.False(replicateCalled);
        // Partitions are present and still in Splitting state.
        Assert.True(manager.Partitions.ContainsKey(1));
        Assert.Equal(RaftPartitionState.Splitting, manager.Partitions[1].State);
    }

    // ── Test 6c: LeaderChanged (follower) must NOT trigger crash-recovery re-enqueuing ─

    /// <summary>
    /// Followers receive LeaderChanged whenever leadership changes, which happens frequently
    /// (elections, transfers). InitializePartitions must NOT run crash recovery on that path:
    /// if it did, every leadership change would register stale _pendingSplits entries that
    /// cause double-commits when the follower later wins leadership.
    /// Crash recovery is the restarting node's responsibility, triggered via RestoreCompleted.
    /// </summary>
    [Fact]
    public async Task Split_LeaderChangedFollower_DoesNotTriggerCrashRecoveryReEnqueue()
    {
        using RaftManager manager = Build();

        List<RaftPartitionRange> splittingMap =
        [
            new() { PartitionId = 1, StartRange = 0, EndRange = 499, Generation = 2, State = RaftPartitionState.Splitting, RoutingMode = RaftRoutingMode.HashRange },
            new() { PartitionId = 2, StartRange = 500, EndRange = 999, Generation = 1, State = RaftPartitionState.Splitting, RoutingMode = RaftRoutingMode.HashRange }
        ];

        bool replicateCalled = false;
        manager.SystemCoordinator.ReplicateOverride = (_, _, _, _) =>
        {
            replicateCalled = true;
            return Task.FromResult(new RaftReplicationResult(true, RaftOperationStatus.Success, HLCTimestamp.Zero, 1));
        };
        manager.SystemCoordinator.StartPartitionsOverride = ranges => manager.StartUserPartitions(ranges);

        // Populate systemConfiguration via ConfigReplicated (follower received live replication).
        manager.SystemCoordinator.Send(MakeConfigReplicated(splittingMap, mapVersion: 2));
        // Follower path: LeaderChanged with a remote endpoint must NOT re-enqueue Phase 2.
        manager.SystemCoordinator.Send(
            new RaftSystemRequest(RaftSystemRequestType.LeaderChanged, "other-node:9001"));
        await WaitForIdleAsync(manager);

        // No SplitPartitionCommit was enqueued by the LeaderChanged (follower) path.
        Assert.False(replicateCalled);
        // Partitions remain in Splitting state — no Phase 2 was driven.
        Assert.True(manager.Partitions.ContainsKey(1));
        Assert.Equal(RaftPartitionState.Splitting, manager.Partitions[1].State);
    }

    // ── Test 6.2b: ExportRange failure — split still completes via log-shipping fallback ─

    /// <summary>
    /// When ExportRange throws, RunSnapshotTransferAsync logs the error and returns without
    /// aborting the split.  AutoCommit must still enqueue SplitPartitionCommit so both
    /// partitions reach Active state.  A faulting transfer must never prevent Phase 2.
    /// </summary>
    [Fact]
    public async Task Split_ExportRangeFails_SplitStillCompletesViaLogShipping()
    {
        using RaftManager manager = Build();

        manager.SystemCoordinator.Send(MakeConfigReplicated(
        [
            new() { PartitionId = 1, StartRange = 0, EndRange = 999, Generation = 1, State = RaftPartitionState.Active, RoutingMode = RaftRoutingMode.HashRange }
        ]));
        await WaitForIdleAsync(manager);

        manager.SystemCoordinator.ReplicateOverride = AlwaysSucceed();

        // Register a transfer whose ExportRange always throws.
        manager.RegisterStateMachineTransfer(new ThrowingExportTransfer());

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

        // Phase 2 must have completed: both partitions Active.
        Assert.NotNull(finalRanges);
        Assert.Equal(2, finalRanges.Count);
        Assert.All(finalRanges, r => Assert.Equal(RaftPartitionState.Active, r.State));
    }

    private sealed class ThrowingExportTransfer : IRaftStateMachineTransfer
    {
        public Task<Stream> ExportRange(RaftSplitPlan plan, long upToIndex, CancellationToken ct) =>
            throw new InvalidOperationException("simulated ExportRange failure");

        public Task ImportRange(int targetPartitionId, Stream snapshot, CancellationToken ct) =>
            Task.CompletedTask;
    }

    // ── Test 6.2c: ImportRange failure — split still completes via log-shipping fallback ─

    /// <summary>
    /// When ExportRange succeeds but ImportRange throws, the import did not happen so no
    /// state was applied to the target partition.  RunSnapshotTransferAsync must log the
    /// error, skip the checkpoint step entirely, and return — leaving AutoCommit to enqueue
    /// SplitPartitionCommit so both partitions still reach Active state.
    /// </summary>
    [Fact]
    public async Task Split_ImportRangeFails_SplitStillCompletesViaLogShipping()
    {
        using RaftManager manager = Build();

        manager.SystemCoordinator.Send(MakeConfigReplicated(
        [
            new() { PartitionId = 1, StartRange = 0, EndRange = 999, Generation = 1, State = RaftPartitionState.Active, RoutingMode = RaftRoutingMode.HashRange }
        ]));
        await WaitForIdleAsync(manager);

        manager.SystemCoordinator.ReplicateOverride = AlwaysSucceed();

        // Register a transfer whose ExportRange returns a valid stream but ImportRange throws.
        manager.RegisterStateMachineTransfer(new ThrowingImportTransfer());

        // Verify ReplicateCheckpoint is never called when ImportRange fails — the coordinator
        // must short-circuit before attempting to replicate a checkpoint for a state that was
        // never applied.
        bool checkpointCalled = false;
        manager.SystemCoordinator.ReplicateCheckpointOverride = (_, _) =>
        {
            checkpointCalled = true;
            return Task.FromResult(new RaftReplicationResult(true, RaftOperationStatus.Success, HLCTimestamp.Zero, 1));
        };

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

        // Phase 2 must have completed: both partitions Active.
        Assert.NotNull(finalRanges);
        Assert.Equal(2, finalRanges.Count);
        Assert.All(finalRanges, r => Assert.Equal(RaftPartitionState.Active, r.State));

        // No checkpoint was attempted — the import never happened.
        Assert.False(checkpointCalled);
    }

    private sealed class ThrowingImportTransfer : IRaftStateMachineTransfer
    {
        public Task<Stream> ExportRange(RaftSplitPlan plan, long upToIndex, CancellationToken ct) =>
            Task.FromResult<Stream>(new MemoryStream("export-payload"u8.ToArray()));

        public Task ImportRange(int targetPartitionId, Stream snapshot, CancellationToken ct) =>
            throw new InvalidOperationException("simulated ImportRange failure");
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

    // ── Test 8: Split a Draining partition is rejected ────────────────────────

    [Fact]
    public async Task Split_Draining_IsRejectedWithError()
    {
        using RaftManager manager = Build();

        // P1 is Draining with no adjacent Active neighbor.  InitializePartitions crash
        // recovery will log a warning and skip it (no survivor to pair with), leaving
        // P1 in Draining state — which lets the split guard fire cleanly.
        manager.SystemCoordinator.Send(MakeConfigReplicated(
        [
            new() { PartitionId = 1, StartRange = 0, EndRange = 999, Generation = 2, State = RaftPartitionState.Draining, RoutingMode = RaftRoutingMode.HashRange },
        ]));
        await WaitForIdleAsync(manager);

        manager.SystemCoordinator.ReplicateOverride = AlwaysSucceed();
        manager.SystemCoordinator.StartPartitionsOverride = ranges => manager.StartUserPartitions(ranges);

        TaskCompletionSource<(RaftOperationStatus Status, long Generation)> tcs =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        manager.SystemCoordinator.Send(new RaftSystemRequest(
            1, new RaftSplitPlan { TargetRoutingMode = RaftRoutingMode.HashRange, AutoCommit = false }, tcs));
        await WaitForIdleAsync(manager);

        (RaftOperationStatus status, long gen) =
            await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        Assert.Equal(RaftOperationStatus.Errored, status);
        // Generation must be returned as-is; no mutation took place.
        Assert.Equal(2, gen);
        Assert.Equal(2, manager.GetPartitionGeneration(1));
    }

    // ── Test 9: Explicit target id that collides with an existing partition is rejected ─

    [Fact]
    public async Task Split_ExplicitTargetIdCollision_IsRejectedWithError()
    {
        using RaftManager manager = Build();

        manager.SystemCoordinator.Send(MakeConfigReplicated(
        [
            new() { PartitionId = 1, StartRange = 0,   EndRange = 499, Generation = 1, State = RaftPartitionState.Active, RoutingMode = RaftRoutingMode.HashRange },
            new() { PartitionId = 2, StartRange = 500, EndRange = 999, Generation = 1, State = RaftPartitionState.Active, RoutingMode = RaftRoutingMode.HashRange },
        ]));
        await WaitForIdleAsync(manager);

        manager.SystemCoordinator.ReplicateOverride = AlwaysSucceed();
        manager.SystemCoordinator.StartPartitionsOverride = ranges => manager.StartUserPartitions(ranges);

        // Attempt to split P1 with an explicit target id that already belongs to P2.
        TaskCompletionSource<(RaftOperationStatus Status, long Generation)> tcs =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        manager.SystemCoordinator.Send(new RaftSystemRequest(
            1, new RaftSplitPlan { TargetPartitionId = 2, TargetRoutingMode = RaftRoutingMode.HashRange, AutoCommit = false }, tcs));
        await WaitForIdleAsync(manager);

        (RaftOperationStatus status, long gen) =
            await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        Assert.Equal(RaftOperationStatus.Errored, status);

        // Source partition must be unmodified — no generation bump, no state change.
        Assert.Equal(1, manager.GetPartitionGeneration(1));
        Assert.Equal(1, manager.GetPartitionGeneration(2));
        Assert.Equal(2, manager.Partitions.Count);
    }

    // ── Test 10: Out-of-range HashBoundary is rejected ────────────────────────

    [Theory]
    [InlineData(0)]       // boundary == StartRange → source would get EndRange = -1
    [InlineData(-1)]      // boundary < StartRange
    [InlineData(1000)]    // boundary > EndRange
    [InlineData(1001)]    // boundary well beyond EndRange
    public async Task Split_OutOfRangeHashBoundary_IsRejectedWithError(int boundary)
    {
        using RaftManager manager = Build();

        manager.SystemCoordinator.Send(MakeConfigReplicated(
        [
            new() { PartitionId = 1, StartRange = 0, EndRange = 999, Generation = 1, State = RaftPartitionState.Active, RoutingMode = RaftRoutingMode.HashRange },
        ]));
        await WaitForIdleAsync(manager);

        manager.SystemCoordinator.ReplicateOverride = AlwaysSucceed();
        manager.SystemCoordinator.StartPartitionsOverride = ranges => manager.StartUserPartitions(ranges);

        TaskCompletionSource<(RaftOperationStatus Status, long Generation)> tcs =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        manager.SystemCoordinator.Send(new RaftSystemRequest(
            1, new RaftSplitPlan { HashBoundary = boundary, TargetRoutingMode = RaftRoutingMode.HashRange, AutoCommit = false }, tcs));
        await WaitForIdleAsync(manager);

        (RaftOperationStatus status, long gen) =
            await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        Assert.Equal(RaftOperationStatus.Errored, status);
        // Source partition must be completely unmodified.
        Assert.Equal(1, gen);
        Assert.Equal(1, manager.GetPartitionGeneration(1));
        Assert.Single(manager.Partitions);
    }

    // ── Test 11: Single-element partition cannot be split (auto-boundary) ─────

    [Fact]
    public async Task Split_SingleElementPartition_IsRejectedWithError()
    {
        using RaftManager manager = Build();

        // StartRange == EndRange: midpoint auto-boundary == StartRange, which fails
        // the splitBoundary <= StartRange check.
        manager.SystemCoordinator.Send(MakeConfigReplicated(
        [
            new() { PartitionId = 1, StartRange = 42, EndRange = 42, Generation = 1, State = RaftPartitionState.Active, RoutingMode = RaftRoutingMode.HashRange },
        ]));
        await WaitForIdleAsync(manager);

        manager.SystemCoordinator.ReplicateOverride = AlwaysSucceed();
        manager.SystemCoordinator.StartPartitionsOverride = ranges => manager.StartUserPartitions(ranges);

        TaskCompletionSource<(RaftOperationStatus Status, long Generation)> tcs =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        manager.SystemCoordinator.Send(new RaftSystemRequest(
            1, new RaftSplitPlan { TargetRoutingMode = RaftRoutingMode.HashRange, AutoCommit = false }, tcs));
        await WaitForIdleAsync(manager);

        (RaftOperationStatus status, long gen) =
            await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        Assert.Equal(RaftOperationStatus.Errored, status);
        Assert.Equal(1, gen);
        Assert.Equal(1, manager.GetPartitionGeneration(1));
        Assert.Single(manager.Partitions);
    }

    // ── Test 12: Crash after Phase 1 of Unrouted split — coordinator resumes Phase 2 ─

    [Fact]
    public async Task Split_Unrouted_CrashAfterPhase1_InitializePartitionsResumesPhase2()
    {
        using RaftManager manager = Build();

        // Simulate persisted state after a crash mid-split of an Unrouted partition.
        // Both source and target have StartRange = EndRange = 0, so range-adjacency
        // matching cannot pair them — the fix uses routing-mode matching instead.
        List<RaftPartitionRange> splittingMap =
        [
            new() { PartitionId = 1, StartRange = 0, EndRange = 0, Generation = 2, State = RaftPartitionState.Splitting, RoutingMode = RaftRoutingMode.Unrouted },
            new() { PartitionId = 2, StartRange = 0, EndRange = 0, Generation = 1, State = RaftPartitionState.Splitting, RoutingMode = RaftRoutingMode.Unrouted },
        ];

        manager.SystemCoordinator.ReplicateOverride = AlwaysSucceed();

        TaskCompletionSource done = new(TaskCreationOptions.RunContinuationsAsynchronously);
        List<RaftPartitionRange>? recoveredRanges = null;
        manager.SystemCoordinator.StartPartitionsOverride = ranges =>
        {
            manager.StartUserPartitions(ranges);
            if (ranges.Count > 0 && ranges.All(r => r.State == RaftPartitionState.Active))
            {
                recoveredRanges = [..ranges];
                done.TrySetResult();
            }
        };

        manager.SystemCoordinator.Send(MakeConfigRestored(splittingMap, mapVersion: 2));
        manager.SystemCoordinator.Send(new RaftSystemRequest(RaftSystemRequestType.RestoreCompleted));
        await done.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        Assert.NotNull(recoveredRanges);
        Assert.Equal(2, recoveredRanges.Count);
        Assert.All(recoveredRanges, r => Assert.Equal(RaftPartitionState.Active, r.State));
        Assert.All(recoveredRanges, r => Assert.Equal(RaftRoutingMode.Unrouted, r.RoutingMode));

        // Phase 2 bumps both generations: source 2→3, target 1→2.
        Assert.Equal(3, recoveredRanges.Single(r => r.PartitionId == 1).Generation);
        Assert.Equal(2, recoveredRanges.Single(r => r.PartitionId == 2).Generation);

        Assert.True(manager.Partitions.ContainsKey(1));
        Assert.True(manager.Partitions.ContainsKey(2));
    }

    // ── Test 13: Unrouted split — full happy-path ──────────────────────────────

    [Fact]
    public async Task Split_Unrouted_BothPartitionsActiveWithUnroutedRoutingMode()
    {
        using RaftManager manager = Build();

        manager.SystemCoordinator.Send(MakeConfigReplicated(
        [
            new() { PartitionId = 1, StartRange = 0, EndRange = 0, Generation = 1, State = RaftPartitionState.Active, RoutingMode = RaftRoutingMode.Unrouted },
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
            1, new RaftSplitPlan { TargetRoutingMode = RaftRoutingMode.Unrouted, AutoCommit = true }));
        await done.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        Assert.NotNull(finalRanges);
        Assert.Equal(2, finalRanges.Count);

        // Both partitions are Active after Phase 2.
        Assert.All(finalRanges, r => Assert.Equal(RaftPartitionState.Active, r.State));
        Assert.All(finalRanges, r => Assert.Equal(RaftRoutingMode.Unrouted, r.RoutingMode));

        // Unrouted partitions have no meaningful hash range — both start/end at zero.
        Assert.All(finalRanges, r => Assert.Equal(0, r.StartRange));
        Assert.All(finalRanges, r => Assert.Equal(0, r.EndRange));

        // Source keeps its original range (no shrink in Unrouted split).
        RaftPartitionRange src = finalRanges.Single(r => r.PartitionId == 1);
        Assert.Equal(3, src.Generation); // Phase1: 1→2, Phase2: 2→3

        // Target starts at generation 2 after Phase 2.
        RaftPartitionRange tgt = finalRanges.Single(r => r.PartitionId != 1);
        Assert.Equal(2, tgt.Generation); // Phase1: gen=1, Phase2: 1→2

        Assert.Equal(2, manager.Partitions.Count);
    }

    // ── Test 6.2: Snapshot transfer — ExportRange/ImportRange/Checkpoint are invoked ─

    private sealed class FakeTransfer : IRaftStateMachineTransfer
    {
        private static readonly byte[] ExportPayload = "snapshot-payload"u8.ToArray();

        public int ExportCallCount;
        public int ImportCallCount;
        public RaftSplitPlan? LastExportPlan;
        public long LastExportIndex;
        public byte[]? ImportedBytes;

        public Task<Stream> ExportRange(RaftSplitPlan plan, long upToIndex, CancellationToken ct)
        {
            ExportCallCount++;
            LastExportPlan  = plan;
            LastExportIndex = upToIndex;
            return Task.FromResult<Stream>(new MemoryStream(ExportPayload));
        }

        public async Task ImportRange(int targetPartitionId, Stream snapshot, CancellationToken ct)
        {
            ImportCallCount++;
            using MemoryStream ms = new();
            await snapshot.CopyToAsync(ms, ct);
            ImportedBytes = ms.ToArray();
        }
    }

    [Fact]
    public async Task Split_WithSnapshotTransfer_ExportsImportsAndReplicatesCheckpoint()
    {
        using RaftManager manager = Build();

        manager.SystemCoordinator.Send(MakeConfigReplicated(
        [
            new() { PartitionId = 1, StartRange = 0, EndRange = 999, Generation = 1, State = RaftPartitionState.Active, RoutingMode = RaftRoutingMode.HashRange }
        ]));
        await WaitForIdleAsync(manager);

        manager.SystemCoordinator.ReplicateOverride = AlwaysSucceed();

        // Register a fake snapshot transfer.
        FakeTransfer fakeTransfer = new();
        manager.RegisterStateMachineTransfer(fakeTransfer);

        // Capture ReplicateCheckpoint calls so we know which partition was checkpointed.
        int checkpointedPartitionId = -1;
        manager.SystemCoordinator.ReplicateCheckpointOverride = (partitionId, _) =>
        {
            checkpointedPartitionId = partitionId;
            return Task.FromResult(new RaftReplicationResult(true, RaftOperationStatus.Success, HLCTimestamp.Zero, 1));
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
        await done.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        // ExportRange called exactly once, with the resolved target partition id.
        Assert.Equal(1, fakeTransfer.ExportCallCount);
        Assert.NotNull(fakeTransfer.LastExportPlan);
        Assert.Equal(2, fakeTransfer.LastExportPlan.TargetPartitionId);  // auto-assigned: max(1)+1
        Assert.Equal(RaftRoutingMode.HashRange, fakeTransfer.LastExportPlan.TargetRoutingMode);
        // upToIndex must be GetMaxLog(sourcePartitionId). No WAL entries were written to
        // partition 1 in this test, so the expected value is 0.  Any future change that
        // passes a different index (e.g. a hardcoded constant or the wrong partition id)
        // will be caught here.
        Assert.Equal(0L, fakeTransfer.LastExportIndex);

        // ImportRange received the exact bytes exported.
        Assert.Equal(1, fakeTransfer.ImportCallCount);
        Assert.Equal("snapshot-payload"u8.ToArray(), fakeTransfer.ImportedBytes);

        // Checkpoint replicated for the target partition (id=2).
        Assert.Equal(2, checkpointedPartitionId);

        // Split completed: both partitions are Active.
        Assert.Equal(2, manager.Partitions.Count);
        Assert.All(manager.Partitions.Values, p => Assert.Equal(RaftPartitionState.Active, p.State));
    }

    // ── Test 14: Explicit HashBoundary is honoured ─────────────────────────────

    [Fact]
    public async Task Split_ExplicitHashBoundary_RangesReflectCallerSpecifiedSplitPoint()
    {
        using RaftManager manager = Build();

        manager.SystemCoordinator.Send(MakeConfigReplicated(
        [
            new() { PartitionId = 1, StartRange = 0, EndRange = 999, Generation = 1, State = RaftPartitionState.Active, RoutingMode = RaftRoutingMode.HashRange },
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

        // Split at boundary 200: source keeps [0,199], target gets [200,999].
        manager.SystemCoordinator.Send(new RaftSystemRequest(
            1, new RaftSplitPlan { HashBoundary = 200, TargetRoutingMode = RaftRoutingMode.HashRange, AutoCommit = true }));
        await done.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        Assert.NotNull(finalRanges);
        Assert.Equal(2, finalRanges.Count);

        List<RaftPartitionRange> sorted = [..finalRanges.OrderBy(r => r.StartRange)];

        // Source (P1) owns exactly [0, 199].
        Assert.Equal(0,   sorted[0].StartRange);
        Assert.Equal(199, sorted[0].EndRange);
        Assert.Equal(1,   sorted[0].PartitionId);

        // Target owns exactly [200, 999].
        Assert.Equal(200, sorted[1].StartRange);
        Assert.Equal(999, sorted[1].EndRange);

        // No gap between the two halves.
        Assert.Equal(sorted[0].EndRange + 1, sorted[1].StartRange);

        // Both Active after Phase 2.
        Assert.All(sorted, r => Assert.Equal(RaftPartitionState.Active, r.State));
        Assert.All(sorted, r => Assert.Equal(RaftRoutingMode.HashRange, r.RoutingMode));
    }
}
