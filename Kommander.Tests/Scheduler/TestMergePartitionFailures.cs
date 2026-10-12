
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
/// Failure-leg tests for the two-phase merge protocol: what survives when a phase is
/// refused, lost, or made stale by a map that already moved on.
///
/// Tests cover:
///   1. Phase 2 refused by lost leadership — both partitions stay readable and
///      uncorrupted; a later commit retry finishes the merge
///   2. Phase 1 refused — neither partition is touched (no Draining state, no
///      generation bump) and the merge can simply be proposed again
///   3. A stale Phase 2 that runs against a map which already absorbed the source
///      is a duplicate no-op: no second commit, no double absorption, and the
///      pending entry is cleared so further re-deliveries are inert
/// </summary>
public sealed class TestMergePartitionFailures
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
    /// Builds a manager with the I/O schedulers started, required for partition
    /// executor operations (ReplicateLogs, fence check) that tests drive directly.
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

    private static List<RaftPartitionRange> TwoAdjacentActiveRanges() =>
    [
        new() { PartitionId = 1, StartRange = 0,   EndRange = 499, Generation = 1, State = RaftPartitionState.Active, RoutingMode = RaftRoutingMode.HashRange },
        new() { PartitionId = 2, StartRange = 500, EndRange = 999, Generation = 1, State = RaftPartitionState.Active, RoutingMode = RaftRoutingMode.HashRange },
    ];

    /// <summary>The map as it looks once the merge has fully landed elsewhere.</summary>
    private static List<RaftPartitionRange> CompletedMergeRanges() =>
    [
        new() { PartitionId = 1, StartRange = 0,   EndRange = 999, Generation = 2, State = RaftPartitionState.Active,  RoutingMode = RaftRoutingMode.HashRange },
        new() { PartitionId = 2, StartRange = 500, EndRange = 999, Generation = 3, State = RaftPartitionState.Removed, RoutingMode = RaftRoutingMode.HashRange },
    ];

    // ── Test 1: Phase 2 refused by lost leadership ─────────────────────────────

    [Fact]
    public async Task Merge_CommitRefusedByLostLeadership_BothPartitionsStayReadableThenRetryCompletes()
    {
        using RaftManager manager = BuildWithSchedulers();

        manager.SystemCoordinator.Send(MakeConfigReplicated(TwoAdjacentActiveRanges()));
        await WaitForIdleAsync(manager);

        await SimulateLeaderAsync(manager, manager.Partitions[1]);
        await SimulateLeaderAsync(manager, manager.Partitions[2]);

        int replicateCallCount = 0;
        void InstallReplication(bool allowPhaseTwo)
        {
            manager.SystemCoordinator.ReplicateOverride = (_, _, _, _) =>
            {
                int n = Interlocked.Increment(ref replicateCallCount);
                bool success = allowPhaseTwo || n == 1;
                return Task.FromResult(new RaftReplicationResult(
                    success,
                    success ? RaftOperationStatus.Success : RaftOperationStatus.NodeIsNotLeader,
                    HLCTimestamp.Zero,
                    n));
            };
        }

        InstallReplication(allowPhaseTwo: false);
        // The override sees the full map including Removed tombstones, unlike GetPartitionMap.
        List<RaftPartitionRange>? lastStartedRanges = null;
        manager.SystemCoordinator.StartPartitionsOverride = ranges =>
        {
            lastStartedRanges = [..ranges];
            manager.StartUserPartitions(ranges);
        };

        TaskCompletionSource<(RaftOperationStatus Status, long Generation)> tcs =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        manager.SystemCoordinator.Send(new RaftSystemRequest(
            new RaftMergePlan { SurvivorPartitionId = 1, SourcePartitionId = 2 }, tcs));

        (RaftOperationStatus refusedStatus, long refusedGeneration) =
            await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        Assert.Equal(RaftOperationStatus.NodeIsNotLeader, refusedStatus);
        Assert.Equal(0, refusedGeneration);

        // Phase 1 committed (the source is Draining) but Phase 2 never landed: the
        // source is still hosted and neither range was absorbed or dropped.
        List<RaftPartitionRange> stalled = [..manager.GetPartitionMap()];
        RaftPartitionRange survivor = stalled.Single(r => r.PartitionId == 1);
        RaftPartitionRange source = stalled.Single(r => r.PartitionId == 2);
        Assert.Equal(RaftPartitionState.Active, survivor.State);
        Assert.Equal(RaftPartitionState.Draining, source.State);
        Assert.Equal(0, survivor.StartRange);
        Assert.Equal(499, survivor.EndRange);
        Assert.True(manager.Partitions.ContainsKey(1));
        Assert.True(manager.Partitions.ContainsKey(2));

        // Both partitions must stay readable while the commit is parked.
        RaftPartition survivorPartition = manager.Partitions[1];
        survivorPartition.Leader = manager.LocalEndpoint;
        (bool _, RaftOperationStatus survivorRead, _) =
            await survivorPartition.ReplicateLogs("test", "survivor-read"u8.ToArray(), autoCommit: true, expectedGeneration: 0);
        Assert.NotEqual(RaftOperationStatus.PartitionMoved, survivorRead);

        RaftPartition sourcePartition = manager.Partitions[2];
        sourcePartition.Leader = manager.LocalEndpoint;
        (bool _, RaftOperationStatus sourceRead, _) =
            await sourcePartition.ReplicateLogs("test", "draining-read"u8.ToArray(), autoCommit: true, expectedGeneration: 0);
        Assert.NotEqual(RaftOperationStatus.PartitionMoved, sourceRead);

        // Once leadership is recoverable the commit retry finishes the merge.
        InstallReplication(allowPhaseTwo: true);
        manager.SystemCoordinator.Send(new RaftSystemRequest(RaftSystemRequestType.MergePartitionCommit, 2));
        await WaitForIdleAsync(manager);

        Assert.NotNull(lastStartedRanges);
        List<RaftPartitionRange> finished = lastStartedRanges!;
        RaftPartitionRange mergedSurvivor = finished.Single(r => r.PartitionId == 1);
        RaftPartitionRange removedSource = finished.Single(r => r.PartitionId == 2);
        Assert.Equal(RaftPartitionState.Active, mergedSurvivor.State);
        Assert.Equal(RaftPartitionState.Removed, removedSource.State);
        Assert.Equal(0, mergedSurvivor.StartRange);
        Assert.Equal(999, mergedSurvivor.EndRange);
        Assert.False(manager.Partitions.ContainsKey(2));
        Assert.True(manager.Partitions.ContainsKey(1));
    }

    // ── Test 2: Phase 1 refused — nothing is touched ───────────────────────────

    [Fact]
    public async Task Merge_Phase1Refused_NeitherPartitionTouchedAndMergeRetriable()
    {
        using RaftManager manager = Build();

        manager.SystemCoordinator.Send(MakeConfigReplicated(TwoAdjacentActiveRanges()));
        await WaitForIdleAsync(manager);

        int replicateCallCount = 0;
        manager.SystemCoordinator.ReplicateOverride = (_, _, _, _) =>
        {
            Interlocked.Increment(ref replicateCallCount);
            return Task.FromResult(new RaftReplicationResult(false, RaftOperationStatus.NodeIsNotLeader, HLCTimestamp.Zero, 0));
        };
        manager.SystemCoordinator.StartPartitionsOverride = ranges => manager.StartUserPartitions(ranges);

        TaskCompletionSource<(RaftOperationStatus Status, long Generation)> tcs =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        manager.SystemCoordinator.Send(new RaftSystemRequest(
            new RaftMergePlan { SurvivorPartitionId = 1, SourcePartitionId = 2 }, tcs));

        (RaftOperationStatus status, long generation) =
            await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        Assert.Equal(RaftOperationStatus.NodeIsNotLeader, status);
        Assert.Equal(0, generation);

        // A refused Phase 1 must fast-abort after exactly one attempt: the map it
        // mutated in memory (Draining, generation bump) was never committed, so both
        // partitions are exactly as they were.
        Assert.Equal(1, replicateCallCount);
        List<RaftPartitionRange> ranges = [..manager.GetPartitionMap()];
        Assert.All(ranges, r => Assert.Equal(RaftPartitionState.Active, r.State));
        Assert.All(ranges, r => Assert.Equal(1, r.Generation));
        Assert.Equal(2, manager.Partitions.Count);

        // The refusal leaves no stale pending entry: proposing the merge again on a
        // healthy leadership runs both phases to completion. The completion is the SECOND
        // start application (Phase 1 applies Draining first), so wait for that call rather
        // than racing a single idle drain.
        TaskCompletionSource done = new(TaskCreationOptions.RunContinuationsAsynchronously);
        int startCount = 0;
        List<RaftPartitionRange>? finalRanges = null;
        manager.SystemCoordinator.StartPartitionsOverride = ranges =>
        {
            manager.StartUserPartitions(ranges);
            if (Interlocked.Increment(ref startCount) == 2)
            {
                finalRanges = [..ranges];
                done.TrySetResult();
            }
        };
        manager.SystemCoordinator.ReplicateOverride = (_, _, _, _) =>
            Task.FromResult(new RaftReplicationResult(true, RaftOperationStatus.Success, HLCTimestamp.Zero, 1));

        manager.SystemCoordinator.Send(new RaftSystemRequest(
            new RaftMergePlan { SurvivorPartitionId = 1, SourcePartitionId = 2 }));
        await done.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        Assert.NotNull(finalRanges);
        Assert.Equal(RaftPartitionState.Active, finalRanges!.Single(r => r.PartitionId == 1).State);
        Assert.Equal(RaftPartitionState.Removed, finalRanges!.Single(r => r.PartitionId == 2).State);
        Assert.False(manager.Partitions.ContainsKey(2));
    }

    // ── Test 3: stale Phase 2 against an already-absorbed map ──────────────────

    [Fact]
    public async Task Merge_CommitAgainstMapThatAlreadyAbsorbedSource_IsDuplicateNoOp()
    {
        using RaftManager manager = Build();

        manager.SystemCoordinator.Send(MakeConfigReplicated(TwoAdjacentActiveRanges()));
        await WaitForIdleAsync(manager);

        int replicateCallCount = 0;
        manager.SystemCoordinator.ReplicateOverride = (_, _, _, _) =>
        {
            int n = Interlocked.Increment(ref replicateCallCount);
            bool success = n == 1;
            return Task.FromResult(new RaftReplicationResult(
                success,
                success ? RaftOperationStatus.Success : RaftOperationStatus.NodeIsNotLeader,
                HLCTimestamp.Zero,
                n));
        };
        manager.SystemCoordinator.StartPartitionsOverride = ranges => manager.StartUserPartitions(ranges);

        TaskCompletionSource<(RaftOperationStatus Status, long Generation)> tcs =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        manager.SystemCoordinator.Send(new RaftSystemRequest(
            new RaftMergePlan { SurvivorPartitionId = 1, SourcePartitionId = 2 }, tcs));

        // Phase 1 commits; the local Phase 2 attempt loses leadership and fails, so
        // this node's pending entry survives while the merge lands elsewhere.
        (RaftOperationStatus status, _) =
            await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        Assert.Equal(RaftOperationStatus.NodeIsNotLeader, status);
        await WaitForIdleAsync(manager);
        Assert.Equal(2, replicateCallCount);

        // The completed merge arrives through replication: the survivor already
        // absorbed the range and the source is already Removed. The override sees
        // the full map including the tombstone, unlike GetPartitionMap.
        TaskCompletionSource mapApplied = new(TaskCreationOptions.RunContinuationsAsynchronously);
        List<RaftPartitionRange>? lastStartedRanges = null;
        manager.SystemCoordinator.StartPartitionsOverride = ranges =>
        {
            lastStartedRanges = [..ranges];
            manager.StartUserPartitions(ranges);
            mapApplied.TrySetResult();
        };
        manager.SystemCoordinator.Send(MakeConfigReplicated(CompletedMergeRanges(), mapVersion: 3));
        await mapApplied.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        await WaitForIdleAsync(manager);
        Assert.NotNull(lastStartedRanges);

        manager.SystemCoordinator.ReplicateOverride = (_, _, _, _) =>
        {
            Interlocked.Increment(ref replicateCallCount);
            return Task.FromResult(new RaftReplicationResult(true, RaftOperationStatus.Success, HLCTimestamp.Zero, 1));
        };

        // The stale Phase 2 retry must recognize the finished merge: no second
        // commit, no double absorption, no re-removal of the source.
        manager.SystemCoordinator.Send(new RaftSystemRequest(RaftSystemRequestType.MergePartitionCommit, 2));
        await WaitForIdleAsync(manager);
        await WaitForIdleAsync(manager);

        Assert.Equal(2, replicateCallCount);

        List<RaftPartitionRange> ranges = lastStartedRanges!;
        RaftPartitionRange survivor = ranges.Single(r => r.PartitionId == 1);
        RaftPartitionRange source = ranges.Single(r => r.PartitionId == 2);
        Assert.Equal(RaftPartitionState.Active, survivor.State);
        Assert.Equal(2, survivor.Generation); // not bumped again by the stale commit
        Assert.Equal(0, survivor.StartRange);
        Assert.Equal(999, survivor.EndRange);
        Assert.Equal(RaftPartitionState.Removed, source.State);
        Assert.Equal(3, source.Generation);
        Assert.True(manager.Partitions.ContainsKey(1));
        // The map application treats the Removed range as a tombstone and never re-creates or
        // touches a hosted partition, so a source this node hosted stays hosted until the drain
        // lifecycle reaps it — the eviction itself belongs to the merge-commit path this scenario
        // deliberately bypassed (the merge landed elsewhere).

        // The pending entry was cleared: a further re-delivery is inert too.
        manager.SystemCoordinator.Send(new RaftSystemRequest(RaftSystemRequestType.MergePartitionCommit, 2));
        await WaitForIdleAsync(manager);
        Assert.Equal(2, replicateCallCount);
    }
}
