
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
/// Functional tests for the two-phase merge protocol (Task 5.4).
///
/// Tests cover:
///   1. Basic merge: survivor absorbs source range, source is Removed, WAL reclaimed
///   2. No-gap / no-overlap invariant: merged range is contiguous and covers the full domain
///   3. Write to draining source partition with stale generation receives PartitionMoved
///   4. Crash after Phase 1: InitializePartitions detects Draining partition and resumes Phase 2
///   5. Generation fence on survivor after full merge rejects pre-merge generation
///   6. Non-adjacent HashRange partitions are rejected with Errored
///   7. Cross-RoutingMode merge (HashRange + Unrouted) is rejected with Errored
///   8. Idempotency: a duplicate merge while source is Draining is rejected with Errored + current generation
/// </summary>
public sealed class TestMergePartition
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
    /// Builds a manager with the I/O schedulers started, required for partition executor
    /// operations (ReplicateLogs, fence check) that tests drive directly.
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

    // ── Test 1: Basic merge correctness ───────────────────────────────────────

    [Fact]
    public async Task Merge_SurvivorAbsorbsSourceRange_SourceRemovedFromPartitions()
    {
        using RaftManager manager = Build();

        // Two adjacent HashRange partitions covering 0–999.
        manager.SystemCoordinator.Send(MakeConfigReplicated(
        [
            new() { PartitionId = 1, StartRange = 0,   EndRange = 499, Generation = 1, State = RaftPartitionState.Active, RoutingMode = RaftRoutingMode.HashRange },
            new() { PartitionId = 2, StartRange = 500, EndRange = 999, Generation = 1, State = RaftPartitionState.Active, RoutingMode = RaftRoutingMode.HashRange },
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

        // Merge P2 (source) into P1 (survivor).
        manager.SystemCoordinator.Send(new RaftSystemRequest(
            new RaftMergePlan { SurvivorPartitionId = 1, SourcePartitionId = 2 }));
        await done.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        Assert.NotNull(finalRanges);

        RaftPartitionRange? survivor = finalRanges.FirstOrDefault(r => r.PartitionId == 1);
        RaftPartitionRange? source   = finalRanges.FirstOrDefault(r => r.PartitionId == 2);

        Assert.NotNull(survivor);
        Assert.NotNull(source);

        // Survivor is Active and absorbed the full range.
        Assert.Equal(RaftPartitionState.Active,  survivor.State);
        Assert.Equal(RaftRoutingMode.HashRange, survivor.RoutingMode);
        Assert.Equal(0,   survivor.StartRange);
        Assert.Equal(999, survivor.EndRange);

        // Source is Removed.
        Assert.Equal(RaftPartitionState.Removed, source.State);

        // Survivor generation: Phase 2 bump only (1 → 2).
        Assert.Equal(2, survivor.Generation);

        // Source generation: Phase 1 bump (1→2, Draining) + Phase 2 bump (2→3, Removed).
        Assert.Equal(3, source.Generation);

        // Source evicted from live partitions; survivor still present.
        Assert.False(manager.Partitions.ContainsKey(2));
        Assert.True(manager.Partitions.ContainsKey(1));
    }

    // ── Test 2: No-gap / no-overlap invariant ─────────────────────────────────

    [Fact]
    public async Task Merge_NoGapNoOverlap_SurvivorCoversFullOriginalDomain()
    {
        using RaftManager manager = Build();

        // Two partitions together covering 0–int.MaxValue.
        manager.SystemCoordinator.Send(MakeConfigReplicated(
        [
            new() { PartitionId = 1, StartRange = 0,                      EndRange = int.MaxValue / 2,     Generation = 1, State = RaftPartitionState.Active, RoutingMode = RaftRoutingMode.HashRange },
            new() { PartitionId = 2, StartRange = int.MaxValue / 2 + 1,   EndRange = int.MaxValue,         Generation = 1, State = RaftPartitionState.Active, RoutingMode = RaftRoutingMode.HashRange },
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
            new RaftMergePlan { SurvivorPartitionId = 1, SourcePartitionId = 2 }));
        await done.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        Assert.NotNull(finalRanges);

        RaftPartitionRange? survivor = finalRanges.FirstOrDefault(r => r.PartitionId == 1 && r.State == RaftPartitionState.Active);
        Assert.NotNull(survivor);

        // After merge the survivor must cover the full original domain with no gap.
        Assert.Equal(0, survivor.StartRange);
        Assert.Equal(int.MaxValue, survivor.EndRange);

        // No other Active partition exists; the domain is fully covered by P1 alone.
        Assert.DoesNotContain(finalRanges, r => r.PartitionId != 1 && r.State == RaftPartitionState.Active);
    }

    // ── Test 3: Writes to draining partition receive PartitionMoved ───────────

    [Fact]
    public async Task Merge_WriteToGrainingPartition_ReceivesPartitionMoved()
    {
        using RaftManager manager = BuildWithSchedulers();

        manager.SystemCoordinator.Send(MakeConfigReplicated(
        [
            new() { PartitionId = 1, StartRange = 0,   EndRange = 499, Generation = 1, State = RaftPartitionState.Active, RoutingMode = RaftRoutingMode.HashRange },
            new() { PartitionId = 2, StartRange = 500, EndRange = 999, Generation = 1, State = RaftPartitionState.Active, RoutingMode = RaftRoutingMode.HashRange },
        ]));
        await WaitForIdleAsync(manager);

        RaftPartition srcPartition = manager.Partitions[2];
        await SimulateLeaderAsync(manager, srcPartition);

        long preMergeGen = manager.GetPartitionGeneration(2);

        // Block Phase 2 replication so the source stays Draining long enough to test.
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
            new RaftMergePlan { SurvivorPartitionId = 1, SourcePartitionId = 2 }));

        // Wait until Phase 1 is committed: P2 is now Draining at generation preMergeGen+1.
        await phase2Arrived.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        // Re-acquire partition and re-set leader (StartPartitions may have updated the entry).
        srcPartition = manager.Partitions[2];
        srcPartition.Leader = manager.LocalEndpoint;

        // A fenced write with the pre-Draining generation must be rejected with PartitionMoved.
        (bool _, RaftOperationStatus status, _) =
            await srcPartition.ReplicateLogs("test", "drain-write"u8.ToArray(), autoCommit: true, expectedGeneration: preMergeGen);

        Assert.Equal(RaftOperationStatus.PartitionMoved, status);

        phase2Release.TrySetResult();
        await done.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
    }

    // ── Test 4: Crash after Phase 1 — coordinator resumes Phase 2 on restart ──

    [Fact]
    public async Task Merge_CrashAfterPhase1_InitializePartitionsResumesMerge()
    {
        using RaftManager manager = Build();

        // Simulate the persisted state after a crash mid-merge:
        // Phase 1 committed (source is Draining, gen bumped), Phase 2 was not.
        List<RaftPartitionRange> drainingMap =
        [
            new() { PartitionId = 1, StartRange = 0,   EndRange = 499, Generation = 1, State = RaftPartitionState.Active,   RoutingMode = RaftRoutingMode.HashRange },
            new() { PartitionId = 2, StartRange = 500, EndRange = 999, Generation = 2, State = RaftPartitionState.Draining, RoutingMode = RaftRoutingMode.HashRange },
        ];

        manager.SystemCoordinator.ReplicateOverride = AlwaysSucceed();

        TaskCompletionSource done = new(TaskCreationOptions.RunContinuationsAsynchronously);
        List<RaftPartitionRange>? recoveredRanges = null;
        manager.SystemCoordinator.StartPartitionsOverride = ranges =>
        {
            manager.StartUserPartitions(ranges);
            // Fire done when Phase 2 completes: only the survivor is Active.
            if (ranges.Any(r => r.State == RaftPartitionState.Removed))
            {
                recoveredRanges = [..ranges];
                done.TrySetResult();
            }
        };

        // ConfigRestored replays the WAL entry into systemConfiguration without calling
        // StartPartitions or crash recovery. RestoreCompleted then fires
        // InitializePartitions(crashRecovery: true), which detects the Draining partition
        // and re-enqueues MergePartitionCommit to resume Phase 2.
        manager.SystemCoordinator.Send(MakeConfigRestored(drainingMap, mapVersion: 2));
        manager.SystemCoordinator.Send(new RaftSystemRequest(RaftSystemRequestType.RestoreCompleted));
        await done.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        Assert.NotNull(recoveredRanges);

        RaftPartitionRange? survivor = recoveredRanges.FirstOrDefault(r => r.PartitionId == 1);
        RaftPartitionRange? source   = recoveredRanges.FirstOrDefault(r => r.PartitionId == 2);

        Assert.NotNull(survivor);
        Assert.NotNull(source);

        // Survivor absorbed the full range.
        Assert.Equal(RaftPartitionState.Active, survivor.State);
        Assert.Equal(0,   survivor.StartRange);
        Assert.Equal(999, survivor.EndRange);

        // Source is Removed; Phase 2 bumped its generation: 2 → 3.
        Assert.Equal(RaftPartitionState.Removed, source.State);
        Assert.Equal(3, source.Generation);

        // Survivor generation bumped once in Phase 2: 1 → 2.
        Assert.Equal(2, survivor.Generation);

        // Source evicted from live partitions; survivor present.
        Assert.False(manager.Partitions.ContainsKey(2));
        Assert.True(manager.Partitions.ContainsKey(1));
    }

    // ── Test 5: Generation fence on survivor after full merge ─────────────────

    [Fact]
    public async Task Merge_SurvivorGenerationFenceAfterMerge_RejectsPreMergeGeneration()
    {
        using RaftManager manager = BuildWithSchedulers();

        manager.SystemCoordinator.Send(MakeConfigReplicated(
        [
            new() { PartitionId = 1, StartRange = 0,   EndRange = 499, Generation = 1, State = RaftPartitionState.Active, RoutingMode = RaftRoutingMode.HashRange },
            new() { PartitionId = 2, StartRange = 500, EndRange = 999, Generation = 1, State = RaftPartitionState.Active, RoutingMode = RaftRoutingMode.HashRange },
        ]));
        await WaitForIdleAsync(manager);

        RaftPartition survPartition = manager.Partitions[1];
        await SimulateLeaderAsync(manager, survPartition);

        long preMergeGen = manager.GetPartitionGeneration(1);

        manager.SystemCoordinator.ReplicateOverride = AlwaysSucceed();

        TaskCompletionSource done = new(TaskCreationOptions.RunContinuationsAsynchronously);
        int startCount = 0;
        manager.SystemCoordinator.StartPartitionsOverride = ranges =>
        {
            manager.StartUserPartitions(ranges);
            if (++startCount == 2) done.TrySetResult();
        };

        manager.SystemCoordinator.Send(new RaftSystemRequest(
            new RaftMergePlan { SurvivorPartitionId = 1, SourcePartitionId = 2 }));
        await done.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        long postMergeGen = manager.GetPartitionGeneration(1);
        // Survivor generation bumped once in Phase 2 only: 1 → 2.
        Assert.Equal(preMergeGen + 1, postMergeGen);

        // Re-acquire post-merge survivor and simulate leader.
        survPartition = manager.Partitions[1];
        survPartition.Leader = manager.LocalEndpoint;

        // Pre-merge generation must be rejected.
        (bool _, RaftOperationStatus staleStatus, _) =
            await survPartition.ReplicateLogs("test", "stale"u8.ToArray(), autoCommit: true, expectedGeneration: preMergeGen);
        Assert.Equal(RaftOperationStatus.PartitionMoved, staleStatus);

        // Current (post-merge) generation must pass the fence.
        (bool _, RaftOperationStatus currentStatus, _) =
            await survPartition.ReplicateLogs("test", "current"u8.ToArray(), autoCommit: true, expectedGeneration: postMergeGen);
        Assert.NotEqual(RaftOperationStatus.PartitionMoved, currentStatus);
    }

    // ── Test 6: Non-adjacent partitions are rejected ──────────────────────────

    [Fact]
    public async Task Merge_NonAdjacentPartitions_IsRejectedWithError()
    {
        using RaftManager manager = Build();

        // Three partitions: P1 [0–199], P2 [200–499], P3 [500–999].
        // P1 and P3 are not adjacent — merging them must be rejected.
        manager.SystemCoordinator.Send(MakeConfigReplicated(
        [
            new() { PartitionId = 1, StartRange = 0,   EndRange = 199, Generation = 1, State = RaftPartitionState.Active, RoutingMode = RaftRoutingMode.HashRange },
            new() { PartitionId = 2, StartRange = 200, EndRange = 499, Generation = 1, State = RaftPartitionState.Active, RoutingMode = RaftRoutingMode.HashRange },
            new() { PartitionId = 3, StartRange = 500, EndRange = 999, Generation = 1, State = RaftPartitionState.Active, RoutingMode = RaftRoutingMode.HashRange },
        ]));
        await WaitForIdleAsync(manager);

        manager.SystemCoordinator.ReplicateOverride = AlwaysSucceed();
        manager.SystemCoordinator.StartPartitionsOverride = ranges => manager.StartUserPartitions(ranges);

        TaskCompletionSource<(RaftOperationStatus Status, long Generation)> tcs =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        manager.SystemCoordinator.Send(new RaftSystemRequest(
            new RaftMergePlan { SurvivorPartitionId = 3, SourcePartitionId = 1 }, tcs));
        await WaitForIdleAsync(manager);

        (RaftOperationStatus status, long gen) =
            await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        Assert.Equal(RaftOperationStatus.Errored, status);

        // Neither partition must have been touched.
        Assert.Equal(1, manager.GetPartitionGeneration(1));
        Assert.Equal(1, manager.GetPartitionGeneration(3));
        Assert.Equal(3, manager.Partitions.Count);
    }

    // ── Test 7: Cross-RoutingMode merge is rejected ───────────────────────────

    [Fact]
    public async Task Merge_CrossRoutingMode_IsRejectedWithError()
    {
        using RaftManager manager = Build();

        // P1 is HashRange, P2 is Unrouted — mixing modes must be rejected even if
        // their integer ranges happen to be adjacent.
        manager.SystemCoordinator.Send(MakeConfigReplicated(
        [
            new() { PartitionId = 1, StartRange = 0, EndRange = 499, Generation = 1, State = RaftPartitionState.Active, RoutingMode = RaftRoutingMode.HashRange },
            new() { PartitionId = 2, StartRange = 500, EndRange = 999, Generation = 1, State = RaftPartitionState.Active, RoutingMode = RaftRoutingMode.Unrouted },
        ]));
        await WaitForIdleAsync(manager);

        manager.SystemCoordinator.ReplicateOverride = AlwaysSucceed();
        manager.SystemCoordinator.StartPartitionsOverride = ranges => manager.StartUserPartitions(ranges);

        TaskCompletionSource<(RaftOperationStatus Status, long Generation)> tcs =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        manager.SystemCoordinator.Send(new RaftSystemRequest(
            new RaftMergePlan { SurvivorPartitionId = 2, SourcePartitionId = 1 }, tcs));
        await WaitForIdleAsync(manager);

        (RaftOperationStatus status, _) =
            await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        Assert.Equal(RaftOperationStatus.Errored, status);

        // Both partitions must be untouched.
        Assert.Equal(1, manager.GetPartitionGeneration(1));
        Assert.Equal(1, manager.GetPartitionGeneration(2));
    }

    // ── Test 8: Idempotency — duplicate merge while source is Draining is rejected ─

    [Fact]
    public async Task Merge_DuplicateRequestWhileDraining_IsRejectedWithError()
    {
        using RaftManager manager = Build();

        manager.SystemCoordinator.Send(MakeConfigReplicated(
        [
            new() { PartitionId = 1, StartRange = 0,   EndRange = 499, Generation = 1, State = RaftPartitionState.Active, RoutingMode = RaftRoutingMode.HashRange },
            new() { PartitionId = 2, StartRange = 500, EndRange = 999, Generation = 1, State = RaftPartitionState.Active, RoutingMode = RaftRoutingMode.HashRange },
        ]));
        await WaitForIdleAsync(manager);

        manager.SystemCoordinator.ReplicateOverride = AlwaysSucceed();

        // The duplicate is queued from inside Phase 1's StartPartitions call.
        // At that point the coordinator has updated the map (src is Draining, gen=2) but
        // has not yet sent MergePartitionCommit, so the channel ordering is:
        //   [duplicate, MergePartitionCommit]
        // The coordinator processes the duplicate first → guard fires → Errored.
        // MergePartitionCommit is processed next → Phase 2 completes normally.
        TaskCompletionSource<(RaftOperationStatus Status, long Generation)> tcs =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        int startCount = 0;
        manager.SystemCoordinator.StartPartitionsOverride = ranges =>
        {
            manager.StartUserPartitions(ranges);
            if (++startCount == 1)
            {
                // Phase 1 just committed; source is Draining at gen=2.
                manager.SystemCoordinator.Send(new RaftSystemRequest(
                    new RaftMergePlan { SurvivorPartitionId = 1, SourcePartitionId = 2 }, tcs));
            }
        };

        manager.SystemCoordinator.Send(new RaftSystemRequest(
            new RaftMergePlan { SurvivorPartitionId = 1, SourcePartitionId = 2 }));

        (RaftOperationStatus status, long gen) =
            await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        // Guard must have rejected the duplicate: Errored with the current (Draining) generation.
        Assert.Equal(RaftOperationStatus.Errored, status);
        Assert.Equal(2, gen);

        // Wait for Phase 2 to complete (MergePartitionCommit was queued after the duplicate).
        await WaitForIdleAsync(manager);
        Assert.False(manager.Partitions.ContainsKey(2));
    }
}
