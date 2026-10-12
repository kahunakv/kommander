
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
/// Failure-leg tests for the two-phase split protocol: the phases that can be refused
/// or lost mid-flight and what the coordinator must leave behind when they are.
///
/// Tests cover:
///   1. Phase 2 refused by lost leadership — the split pair stays consistent and the
///      source keeps serving; a later commit retry completes the split
///   2. A commit re-delivered after the split already completed is a no-op, never a
///      second Phase-2 commit (lost-ack retry)
///   3. Checkpoint replication failing after a snapshot was imported does not abort
///      the split — it is retried exactly once and the split completes
/// </summary>
public sealed class TestSplitPartitionFailures
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

    /// <summary>A snapshot transfer whose export and import succeed, so only the checkpoint step can fail.</summary>
    private sealed class WorkingSnapshotTransfer : IRaftStateMachineTransfer
    {
        public int ExportCallCount;

        public Task<Stream> ExportRange(RaftSplitPlan plan, long upToIndex, CancellationToken ct)
        {
            ExportCallCount++;
            return Task.FromResult<Stream>(new MemoryStream("snapshot"u8.ToArray()));
        }

        public Task ImportRange(int targetPartitionId, Stream snapshot, CancellationToken ct) =>
            snapshot.CopyToAsync(new MemoryStream(), ct);
    }

    // ── Test 1: Phase 2 refused by lost leadership ─────────────────────────────

    [Fact]
    public async Task Split_CommitRefusedByLostLeadership_KeepsPairConsistentThenRetryCompletes()
    {
        using RaftManager manager = BuildWithSchedulers();

        manager.SystemCoordinator.Send(MakeConfigReplicated(
        [
            new() { PartitionId = 1, StartRange = 0, EndRange = 999, Generation = 1, State = RaftPartitionState.Active, RoutingMode = RaftRoutingMode.HashRange }
        ]));
        await WaitForIdleAsync(manager);

        await SimulateLeaderAsync(manager, manager.Partitions[1]);

        // Phase 1 commits; the Phase 2 attempt is refused because leadership moved.
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
        manager.SystemCoordinator.StartPartitionsOverride = ranges => manager.StartUserPartitions(ranges);

        TaskCompletionSource<(RaftOperationStatus Status, long Generation)> tcs =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        manager.SystemCoordinator.Send(new RaftSystemRequest(
            1, new RaftSplitPlan { TargetRoutingMode = RaftRoutingMode.HashRange, AutoCommit = false }, tcs));
        await WaitForIdleAsync(manager);

        // Phase 1 committed; the explicit Phase 2 commit is refused by the lost leadership.
        manager.SystemCoordinator.Send(new RaftSystemRequest(RaftSystemRequestType.SplitPartitionCommit, 1));
        (RaftOperationStatus refusedStatus, long refusedGeneration) =
            await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        Assert.Equal(RaftOperationStatus.NodeIsNotLeader, refusedStatus);
        Assert.Equal(0, refusedGeneration);

        // The map still holds a consistent Splitting pair — the source was not cut over
        // alone and the target was not abandoned mid-phase.
        List<RaftPartitionRange> stalled = [..manager.GetPartitionMap()];
        Assert.Equal(2, stalled.Count);
        RaftPartitionRange source = stalled.Single(r => r.PartitionId == 1);
        RaftPartitionRange target = stalled.Single(r => r.PartitionId != 1);
        Assert.Equal(RaftPartitionState.Splitting, source.State);
        Assert.Equal(RaftPartitionState.Splitting, target.State);
        Assert.Equal(2, source.Generation);
        Assert.Equal(1, target.Generation);
        Assert.Equal(source.EndRange + 1, target.StartRange); // ranges still tile

        // The Splitting source must keep serving traffic while the commit is parked.
        RaftPartition stalledPartition = manager.Partitions[1];
        stalledPartition.Leader = manager.LocalEndpoint;
        (bool _, RaftOperationStatus readStatus, _) =
            await stalledPartition.ReplicateLogs("test", "read-while-commit-parked"u8.ToArray(), autoCommit: true, expectedGeneration: 0);
        Assert.NotEqual(RaftOperationStatus.PartitionMoved, readStatus);

        // A retry once leadership is recoverable completes the split exactly once.
        InstallReplication(allowPhaseTwo: true);
        manager.SystemCoordinator.Send(new RaftSystemRequest(RaftSystemRequestType.SplitPartitionCommit, 1));
        await WaitForIdleAsync(manager);

        List<RaftPartitionRange> finished = [..manager.GetPartitionMap()];
        Assert.Equal(2, finished.Count);
        Assert.All(finished, r => Assert.Equal(RaftPartitionState.Active, r.State));
        Assert.Equal(3, finished.Single(r => r.PartitionId == 1).Generation);
        Assert.Equal(2, finished.Single(r => r.PartitionId != 1).Generation);
        Assert.Equal(2, manager.Partitions.Count);
    }

    // ── Test 2: commit re-delivered after completion is a no-op ────────────────

    [Fact]
    public async Task Split_CommitRedeliveredAfterCompletion_DoesNotCommitPhase2Twice()
    {
        using RaftManager manager = Build();

        manager.SystemCoordinator.Send(MakeConfigReplicated(
        [
            new() { PartitionId = 1, StartRange = 0, EndRange = 999, Generation = 1, State = RaftPartitionState.Active, RoutingMode = RaftRoutingMode.HashRange }
        ]));
        await WaitForIdleAsync(manager);

        int replicateCallCount = 0;
        manager.SystemCoordinator.ReplicateOverride = (_, _, _, _) =>
        {
            Interlocked.Increment(ref replicateCallCount);
            return Task.FromResult(new RaftReplicationResult(true, RaftOperationStatus.Success, HLCTimestamp.Zero, 1));
        };

        TaskCompletionSource done = new(TaskCreationOptions.RunContinuationsAsynchronously);
        int startCount = 0;
        manager.SystemCoordinator.StartPartitionsOverride = ranges =>
        {
            manager.StartUserPartitions(ranges);
            if (++startCount == 2)
                done.TrySetResult();
        };

        manager.SystemCoordinator.Send(new RaftSystemRequest(
            1, new RaftSplitPlan { TargetRoutingMode = RaftRoutingMode.HashRange, AutoCommit = true }));
        await done.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        await WaitForIdleAsync(manager);

        int callsAfterCompletion = replicateCallCount;
        Assert.Equal(2, callsAfterCompletion); // Phase 1 + Phase 2, nothing more

        // The caller never saw the Phase 2 ack and re-sends the commit. The pending
        // entry is gone, so this must be a plain no-op — no third replication, no
        // generation bumps, no state churn.
        manager.SystemCoordinator.Send(new RaftSystemRequest(RaftSystemRequestType.SplitPartitionCommit, 1));
        await WaitForIdleAsync(manager);

        Assert.Equal(callsAfterCompletion, replicateCallCount);

        List<RaftPartitionRange> ranges = [..manager.GetPartitionMap()];
        Assert.Equal(2, ranges.Count);
        Assert.All(ranges, r => Assert.Equal(RaftPartitionState.Active, r.State));
        Assert.Equal(3, ranges.Single(r => r.PartitionId == 1).Generation);
        Assert.Equal(2, ranges.Single(r => r.PartitionId != 1).Generation);
    }

    // ── Test 3: checkpoint replication failure does not abort the split ────────

    [Fact]
    public async Task Split_CheckpointReplicationFails_RetriedOnceAndSplitStillCompletes()
    {
        using RaftManager manager = Build();

        manager.SystemCoordinator.Send(MakeConfigReplicated(
        [
            new() { PartitionId = 1, StartRange = 0, EndRange = 999, Generation = 1, State = RaftPartitionState.Active, RoutingMode = RaftRoutingMode.HashRange }
        ]));
        await WaitForIdleAsync(manager);

        manager.SystemCoordinator.ReplicateOverride = (_, _, _, _) =>
            Task.FromResult(new RaftReplicationResult(true, RaftOperationStatus.Success, HLCTimestamp.Zero, 1));

        // Export and import succeed; only the checkpoint replication into the target
        // partition's log keeps failing.
        WorkingSnapshotTransfer transfer = new();
        manager.RegisterStateMachineTransfer(transfer);

        int checkpointCalls = 0;
        manager.SystemCoordinator.ReplicateCheckpointOverride = (_, _) =>
        {
            Interlocked.Increment(ref checkpointCalls);
            return Task.FromResult(new RaftReplicationResult(false, RaftOperationStatus.Errored, HLCTimestamp.Zero, 0));
        };

        TaskCompletionSource done = new(TaskCreationOptions.RunContinuationsAsynchronously);
        int startCount = 0;
        manager.SystemCoordinator.StartPartitionsOverride = ranges =>
        {
            manager.StartUserPartitions(ranges);
            if (++startCount == 2)
                done.TrySetResult();
        };

        manager.SystemCoordinator.Send(new RaftSystemRequest(
            1, new RaftSplitPlan { TargetRoutingMode = RaftRoutingMode.HashRange, AutoCommit = true }));
        await done.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        await WaitForIdleAsync(manager);

        // The snapshot step is best-effort: one attempt plus exactly one retry, and
        // the split itself still runs Phase 2 to completion.
        Assert.Equal(2, checkpointCalls);
        Assert.Equal(1, transfer.ExportCallCount);

        List<RaftPartitionRange> ranges = [..manager.GetPartitionMap()];
        Assert.Equal(2, ranges.Count);
        Assert.All(ranges, r => Assert.Equal(RaftPartitionState.Active, r.State));
    }
}
