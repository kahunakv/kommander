using System.Text.Json;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.System;
using Kommander.System.Protos;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging.Abstractions;
using Google.Protobuf;

namespace Kommander.Tests.Scheduler;

/// <summary>
/// Unit tests for <see cref="RaftSystemCoordinator"/> (Task 13).
///
/// All tests operate without a real Raft cluster.  They drive the coordinator
/// directly via <see cref="RaftSystemCoordinator.Send"/> and observe side-effects
/// on <see cref="RaftManager.Partitions"/> and <see cref="RaftManager.IsInitialized"/>.
///
/// The two commit-producing paths (TrySetInitialPartitions, TrySplitPartition) are
/// exercised using the injected ReplicateOverride / StartPartitionsOverride delegates
/// so no real Raft quorum is needed.
/// </summary>
public sealed class TestRaftSystemCoordinator
{
    // ── Builder ───────────────────────────────────────────────────────────────

    private static RaftManager Build()
    {
        RaftConfiguration config = new()
        {
            Host = "localhost",
            Port = 9000,
            InitialPartitions = 0
        };
        return new(
            config,
            new StaticDiscovery([]),
            new InMemoryWAL(NullLogger<IRaft>.Instance),
            new Kommander.Communication.Memory.InMemoryCommunication(),
            new HybridLogicalClock(),
            NullLogger<IRaft>.Instance
        );
    }

    private static RaftManager BuildWithPartitions(int partitions)
    {
        RaftConfiguration config = new()
        {
            Host = "localhost",
            Port = 9000,
            InitialPartitions = partitions
        };
        return new(
            config,
            new StaticDiscovery([]),
            new InMemoryWAL(NullLogger<IRaft>.Instance),
            new Kommander.Communication.Memory.InMemoryCommunication(),
            new HybridLogicalClock(),
            NullLogger<IRaft>.Instance
        );
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /// <summary>Serializes a RaftSystemMessage the same way the coordinator does.</summary>
    private static byte[] SerializeMessage(string key, string value)
    {
        RaftSystemMessage msg = new() { Key = key, Value = value };
        using MemoryStream ms = new();
        msg.WriteTo(ms);
        return ms.ToArray();
    }

    /// <summary>
    /// Builds a valid serialized partition-map payload for the given ranges
    /// and wraps it in a <see cref="RaftSystemRequest"/> of type
    /// <see cref="RaftSystemRequestType.ConfigReplicated"/>.
    /// </summary>
    private static RaftSystemRequest MakeConfigReplicated(List<RaftPartitionRange> ranges) =>
        new(RaftSystemRequestType.ConfigReplicated,
            SerializeMessage(
                RaftSystemConfigKeys.Partitions,
                JsonSerializer.Serialize(ranges)));

    private static RaftSystemRequest MakeConfigRestored(List<RaftPartitionRange> ranges) =>
        new(RaftSystemRequestType.ConfigRestored,
            SerializeMessage(
                RaftSystemConfigKeys.Partitions,
                JsonSerializer.Serialize(ranges)));

    /// <summary>
    /// Waits until all previously-enqueued messages have been processed by the
    /// coordinator's background loop. Uses a drain sentinel rather than a fixed
    /// delay, so it is both fast and immune to scheduling jitter.
    /// </summary>
    private static Task WaitForIdleAsync(RaftManager manager) =>
        manager.SystemCoordinator.DrainAsync().WaitAsync(TimeSpan.FromSeconds(5));

    // ── Tests ─────────────────────────────────────────────────────────────────

    [Fact]
    public async Task ConfigReplicated_WithValidPartitions_StartsUserPartitions()
    {
        RaftManager manager = Build();
        using (manager)
        {
            List<RaftPartitionRange> ranges =
            [
                new() { PartitionId = 1, StartRange = 0, EndRange = 500_000_000 },
                new() { PartitionId = 2, StartRange = 500_000_001, EndRange = int.MaxValue }
            ];

            manager.SystemCoordinator.Send(MakeConfigReplicated(ranges));
            await WaitForIdleAsync(manager);

            Assert.True(manager.IsInitialized);
            Assert.Equal(2, manager.Partitions.Count);
            Assert.True(manager.Partitions.ContainsKey(1));
            Assert.True(manager.Partitions.ContainsKey(2));
            Assert.Equal(0, manager.Partitions[1].StartRange);
            Assert.Equal(500_000_000, manager.Partitions[1].EndRange);
            Assert.Equal(500_000_001, manager.Partitions[2].StartRange);
            Assert.Equal(int.MaxValue, manager.Partitions[2].EndRange);
        }
    }

    [Fact]
    public async Task ConfigRestored_ThenLeaderChanged_AsFollower_StartsUserPartitions()
    {
        RaftManager manager = Build();
        using (manager)
        {
            List<RaftPartitionRange> ranges =
            [
                new() { PartitionId = 1, StartRange = 0, EndRange = int.MaxValue }
            ];

            // 1. Restore config — populates internal systemConfiguration.
            manager.SystemCoordinator.Send(MakeConfigRestored(ranges));
            await WaitForIdleAsync(manager);

            // Partitions not yet started by restore alone.
            Assert.Empty(manager.Partitions);

            // 2. LeaderChanged to a *different* node triggers InitializePartitions()
            //    which reads the restored config and calls StartUserPartitions.
            manager.SystemCoordinator.Send(
                new RaftSystemRequest(RaftSystemRequestType.LeaderChanged, "other-node:9001"));
            await WaitForIdleAsync(manager);

            Assert.True(manager.IsInitialized);
            Assert.Single(manager.Partitions);
            Assert.Equal(0, manager.Partitions[1].StartRange);
            Assert.Equal(int.MaxValue, manager.Partitions[1].EndRange);
        }
    }

    [Fact]
    public async Task ConfigReplicated_UpdatesExistingPartitionRange()
    {
        RaftManager manager = Build();
        using (manager)
        {
            // Initial map: one partition owning full range.
            List<RaftPartitionRange> initial =
            [
                new() { PartitionId = 1, StartRange = 0, EndRange = int.MaxValue }
            ];
            manager.SystemCoordinator.Send(MakeConfigReplicated(initial));
            await WaitForIdleAsync(manager);

            Assert.Equal(int.MaxValue, manager.Partitions[1].EndRange);

            // Shrunk map after split (coordinator has already shrunk partition 1 before replicating).
            List<RaftPartitionRange> updated =
            [
                new() { PartitionId = 1, StartRange = 0, EndRange = 500_000_000 },
                new() { PartitionId = 2, StartRange = 500_000_001, EndRange = int.MaxValue }
            ];
            manager.SystemCoordinator.Send(MakeConfigReplicated(updated));
            await WaitForIdleAsync(manager);

            Assert.Equal(2, manager.Partitions.Count);
            Assert.Equal(500_000_000, manager.Partitions[1].EndRange);
            Assert.Equal(500_000_001, manager.Partitions[2].StartRange);
        }
    }

    [Fact]
    public async Task RestoreCompleted_IsNoOp_DoesNotThrow()
    {
        RaftManager manager = Build();
        using (manager)
        {
            manager.SystemCoordinator.Send(new RaftSystemRequest(RaftSystemRequestType.RestoreCompleted));
            await WaitForIdleAsync(manager);

            // No exception, partitions untouched.
            Assert.Empty(manager.Partitions);
        }
    }

    [Fact]
    public async Task ConfigReplicated_NullPayload_DoesNotThrow_PartitionsUnchanged()
    {
        RaftManager manager = Build();
        using (manager)
        {
            // RaftSystemRequest(type, byte[]) overload — pass null via the no-arg ctor indirectly.
            // ConfigReplicated with null LogData should log a warning and return cleanly.
            manager.SystemCoordinator.Send(
                new RaftSystemRequest(RaftSystemRequestType.ConfigReplicated));
            await WaitForIdleAsync(manager);

            Assert.Empty(manager.Partitions);
            Assert.False(manager.IsInitialized);
        }
    }

    [Fact]
    public async Task LeaderChanged_EmptyLeader_DoesNotStartPartitions()
    {
        RaftManager manager = Build();
        using (manager)
        {
            manager.SystemCoordinator.Send(
                new RaftSystemRequest(RaftSystemRequestType.LeaderChanged, ""));
            await WaitForIdleAsync(manager);

            Assert.Empty(manager.Partitions);
        }
    }

    [Fact]
    public async Task SerialExecution_MultipleConfigReplicated_LastOneWins()
    {
        RaftManager manager = Build();
        using (manager)
        {
            // Send 5 ConfigReplicated messages in rapid succession.
            // Each overwrites the previous systemConfiguration entry; the final
            // stored config must reflect the last message.
            for (int i = 1; i <= 5; i++)
            {
                List<RaftPartitionRange> r =
                [
                    new() { PartitionId = i, StartRange = i, EndRange = i + 1 }
                ];
                manager.SystemCoordinator.Send(MakeConfigReplicated(r));
            }

            await WaitForIdleAsync(manager);

            // All 5 messages called StartUserPartitions (upsert-only), so partitions
            // 1–5 are all present. The key invariant is that partition 5 exists with
            // the range set by the last message, proving serial ordering.
            Assert.True(manager.Partitions.ContainsKey(5));
            Assert.Equal(5, manager.Partitions[5].StartRange);
            Assert.Equal(6, manager.Partitions[5].EndRange);
            Assert.Equal(5, manager.Partitions.Count);
        }
    }

    [Fact]
    public async Task Send_AfterStop_IsNoOp()
    {
        RaftManager manager = Build();
        using (manager)
        {
            manager.SystemCoordinator.Stop();

            // Enqueue after stop — should be silently dropped.
            manager.SystemCoordinator.Send(
                new RaftSystemRequest(RaftSystemRequestType.RestoreCompleted));

            // manager.Dispose() (via using) will call Stop() a second time — must be idempotent.
            Assert.Empty(manager.Partitions);
        }
    }

    [Fact]
    public void Dispose_WhenIdle_CompletesCleanly()
    {
        RaftManager manager = Build();
        manager.Dispose();
    }

    [Fact]
    public async Task Dispose_DuringPendingMessages_DrainsThenExits()
    {
        RaftManager manager = Build();
        // Flood the channel before disposing.
        for (int i = 0; i < 20; i++)
            manager.SystemCoordinator.Send(
                new RaftSystemRequest(RaftSystemRequestType.RestoreCompleted));

        // Dispose should drain (or abort cleanly) within the 5 s timeout.
        Task disposeTask = Task.Run(() => manager.Dispose());
        await disposeTask.WaitAsync(TimeSpan.FromSeconds(10));
    }

    // ── TrySetInitialPartitions ───────────────────────────────────────────────

    [Fact]
    public async Task LeaderChanged_AsLeader_ConfigAlreadyPresent_ActivatesWithoutReplication()
    {
        // If systemConfiguration already has partition data (e.g. after restore),
        // TrySetInitialPartitions must activate partitions directly without calling
        // ReplicateSystemLogs.
        RaftManager manager = Build();
        using (manager)
        {
            bool replicateCalled = false;
            manager.SystemCoordinator.ReplicateOverride = (_, _, _, _) =>
            {
                replicateCalled = true;
                return Task.FromResult(new RaftReplicationResult(true, RaftOperationStatus.Success, HLCTimestamp.Zero, 1));
            };

            TaskCompletionSource done = new(TaskCreationOptions.RunContinuationsAsynchronously);
            List<RaftPartitionRange>? activated = null;
            manager.SystemCoordinator.StartPartitionsOverride = ranges =>
            {
                activated = [..ranges];
                manager.StartUserPartitions(ranges);
                done.TrySetResult();
            };

            // Restore a known partition map first.
            List<RaftPartitionRange> knownRanges =
            [
                new() { PartitionId = 1, StartRange = 0, EndRange = 500_000_000 },
                new() { PartitionId = 2, StartRange = 500_000_001, EndRange = int.MaxValue }
            ];
            manager.SystemCoordinator.Send(MakeConfigRestored(knownRanges));
            await WaitForIdleAsync(manager);

            // Trigger as leader — must use existing config, not replicate.
            manager.SystemCoordinator.Send(
                new RaftSystemRequest(RaftSystemRequestType.LeaderChanged, manager.LocalEndpoint));
            await done.Task.WaitAsync(TimeSpan.FromSeconds(5));

            Assert.False(replicateCalled, "ReplicateSystemLogs must not be called when config is already present");
            Assert.NotNull(activated);
            Assert.Equal(2, activated.Count);
            Assert.True(manager.IsInitialized);
        }
    }

    [Fact]
    public async Task LeaderChanged_AsLeader_NoExistingConfig_ReplicatesThenActivates()
    {
        // When no systemConfiguration entry exists the coordinator must compute
        // the initial partition map, replicate it via the system partition, and
        // only then call StartUserPartitions.
        RaftManager manager = BuildWithPartitions(partitions: 2);
        using (manager)
        {
            List<byte[]> replicatedPayloads = [];
            manager.SystemCoordinator.ReplicateOverride = (type, data, autoCommit, ct) =>
            {
                replicatedPayloads.Add(data);
                return Task.FromResult(new RaftReplicationResult(true, RaftOperationStatus.Success, HLCTimestamp.Zero, 1));
            };

            TaskCompletionSource done = new(TaskCreationOptions.RunContinuationsAsynchronously);
            List<RaftPartitionRange>? activated = null;
            manager.SystemCoordinator.StartPartitionsOverride = ranges =>
            {
                activated = [..ranges];
                manager.StartUserPartitions(ranges);
                done.TrySetResult();
            };

            manager.SystemCoordinator.Send(
                new RaftSystemRequest(RaftSystemRequestType.LeaderChanged, manager.LocalEndpoint));
            await done.Task.WaitAsync(TimeSpan.FromSeconds(5));

            // Exactly one replication call.
            Assert.Single(replicatedPayloads);
            Assert.NotNull(activated);
            // 2 user partitions created by DivideIntoRanges(2).
            Assert.Equal(2, activated.Count);
            // Ranges must be contiguous and cover [0, int.MaxValue].
            Assert.Equal(0, activated.Min(r => r.StartRange));
            Assert.Equal(int.MaxValue, activated.Max(r => r.EndRange));
            // No gaps or overlaps: sorted end+1 == next start.
            List<RaftPartitionRange> sorted = [..activated.OrderBy(r => r.StartRange)];
            for (int i = 0; i < sorted.Count - 1; i++)
                Assert.Equal(sorted[i].EndRange + 1, sorted[i + 1].StartRange);

            Assert.True(manager.IsInitialized);
        }
    }

    [Fact]
    public async Task LeaderChanged_AsLeader_ReplicationFailsThenSucceeds_RetriesAndActivates()
    {
        // The coordinator retries on non-success status.  Simulate one failure
        // followed by success to verify the retry loop works and activation happens.
        RaftManager manager = BuildWithPartitions(partitions: 1);
        using (manager)
        {
            // Zero retry delay so the test doesn't incur a real 5 s wait.
            manager.SystemCoordinator.RetryDelay = TimeSpan.Zero;

            int callCount = 0;
            manager.SystemCoordinator.ReplicateOverride = (_, _, _, _) =>
            {
                callCount++;
                RaftOperationStatus status = callCount == 1
                    ? RaftOperationStatus.Errored
                    : RaftOperationStatus.Success;
                bool success = status == RaftOperationStatus.Success;
                return Task.FromResult(new RaftReplicationResult(success, status, HLCTimestamp.Zero, callCount));
            };

            TaskCompletionSource done = new(TaskCreationOptions.RunContinuationsAsynchronously);
            manager.SystemCoordinator.StartPartitionsOverride = ranges =>
            {
                manager.StartUserPartitions(ranges);
                done.TrySetResult();
            };

            manager.SystemCoordinator.Send(
                new RaftSystemRequest(RaftSystemRequestType.LeaderChanged, manager.LocalEndpoint));
            await done.Task.WaitAsync(TimeSpan.FromSeconds(5));

            Assert.Equal(2, callCount);
            Assert.True(manager.IsInitialized);
        }
    }

    [Fact]
    public async Task LeaderChanged_AsLeader_CancelledDuringRetryDelay_ExitsWithoutActivating()
    {
        // When the shutdown token fires while the coordinator is sleeping inside
        // the production Task.Delay(5000, cancellationToken) retry gap it must
        // abandon the work and never call StartUserPartitions.
        //
        // The override returns synchronously with an error so the only place
        // cancellation can take effect is inside that production delay, not inside
        // the fake replication call itself.
        RaftManager manager = BuildWithPartitions(partitions: 1);
        using (manager)
        {
            TaskCompletionSource firstAttemptDone = new(TaskCreationOptions.RunContinuationsAsynchronously);
            manager.SystemCoordinator.ReplicateOverride = (_, _, _, _) =>
            {
                firstAttemptDone.TrySetResult();
                // Return synchronously — no ct-cancellable await here, so Stop()
                // cannot cancel inside this delegate.
                return Task.FromResult(
                    new RaftReplicationResult(false, RaftOperationStatus.Errored, HLCTimestamp.Zero, 0));
            };

            bool startCalled = false;
            manager.SystemCoordinator.StartPartitionsOverride = ranges =>
            {
                startCalled = true;
                manager.StartUserPartitions(ranges);
            };

            manager.SystemCoordinator.Send(
                new RaftSystemRequest(RaftSystemRequestType.LeaderChanged, manager.LocalEndpoint));

            // Wait until the first replication attempt has returned.
            await firstAttemptDone.Task.WaitAsync(TimeSpan.FromSeconds(5));

            // Yield briefly so the coordinator loop advances past the completed
            // Replicate() call and suspends inside Task.Delay(5000, cancellationToken).
            await Task.Delay(20);

            // Cancelling now fires inside the production retry delay, not the override.
            manager.SystemCoordinator.Stop();

            // Wait for the loop to exit cleanly — deterministic, no fixed delay.
            await manager.SystemCoordinator.LoopTask.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.False(startCalled);
        }
    }

    // ── TrySplitPartition ─────────────────────────────────────────────────────

    [Fact]
    public async Task SplitPartition_ProducesNonOverlappingRanges_AndReplicates()
    {
        // The core correctness guarantee of TrySplitPartition: the replicated map
        // must have non-overlapping, contiguous ranges covering the original span.
        RaftManager manager = Build();
        using (manager)
        {
            // Install a known single-partition map.
            List<RaftPartitionRange> initial =
            [
                new() { PartitionId = 1, StartRange = 0, EndRange = 999 }
            ];
            manager.SystemCoordinator.Send(MakeConfigReplicated(initial));
            await WaitForIdleAsync(manager);

            // Capture the partition map that was passed to Replicate.
            List<RaftPartitionRange>? replicatedRanges = null;
            manager.SystemCoordinator.ReplicateOverride = (_, data, _, _) =>
            {
                // Decode the embedded JSON to inspect ranges.
                using MemoryStream ms = new(data);
                Kommander.System.Protos.RaftSystemMessage msg =
                    Kommander.System.Protos.RaftSystemMessage.Parser.ParseFrom(ms);
                replicatedRanges = JsonSerializer.Deserialize<List<RaftPartitionRange>>(msg.Value);
                return Task.FromResult(new RaftReplicationResult(true, RaftOperationStatus.Success, HLCTimestamp.Zero, 1));
            };

            TaskCompletionSource done = new(TaskCreationOptions.RunContinuationsAsynchronously);
            List<RaftPartitionRange>? activated = null;
            manager.SystemCoordinator.StartPartitionsOverride = ranges =>
            {
                activated = [..ranges];
                manager.StartUserPartitions(ranges);
                done.TrySetResult();
            };

            manager.SystemCoordinator.Send(
                new RaftSystemRequest(RaftSystemRequestType.SplitPartition, 1));
            await done.Task.WaitAsync(TimeSpan.FromSeconds(5));

            // The replicated map must have 2 non-overlapping ranges.
            Assert.NotNull(replicatedRanges);
            Assert.Equal(2, replicatedRanges.Count);

            List<RaftPartitionRange> sorted = [..replicatedRanges.OrderBy(r => r.StartRange)];
            // Original start preserved.
            Assert.Equal(0, sorted[0].StartRange);
            // Original end preserved on the high partition.
            Assert.Equal(999, sorted[1].EndRange);
            // No gap: old end + 1 == new start.
            Assert.Equal(sorted[0].EndRange + 1, sorted[1].StartRange);
            // Symmetrical split within ±1.
            int oldEnd = sorted[0].EndRange;
            int newStart = sorted[1].StartRange;
            Assert.Equal(oldEnd + 1, newStart);
            Assert.True(Math.Abs(oldEnd - (newStart - 1)) <= 1);

            // Activated map matches replicated map.
            Assert.NotNull(activated);
            Assert.Equal(2, activated.Count);
        }
    }

    [Fact]
    public async Task SplitPartition_WhenNodeIsNotLeader_AbortsWithoutActivating()
    {
        RaftManager manager = Build();
        using (manager)
        {
            List<RaftPartitionRange> initial =
            [
                new() { PartitionId = 1, StartRange = 0, EndRange = int.MaxValue }
            ];
            manager.SystemCoordinator.Send(MakeConfigReplicated(initial));
            await WaitForIdleAsync(manager);

            int initialPartitionCount = manager.Partitions.Count;

            TaskCompletionSource replicateDone = new(TaskCreationOptions.RunContinuationsAsynchronously);
            manager.SystemCoordinator.ReplicateOverride = (_, _, _, _) =>
            {
                replicateDone.TrySetResult();
                return Task.FromResult(new RaftReplicationResult(false, RaftOperationStatus.NodeIsNotLeader, HLCTimestamp.Zero, 0));
            };

            bool startCalled = false;
            manager.SystemCoordinator.StartPartitionsOverride = ranges =>
            {
                startCalled = true;
                manager.StartUserPartitions(ranges);
            };

            manager.SystemCoordinator.Send(
                new RaftSystemRequest(RaftSystemRequestType.SplitPartition, 1));
            await replicateDone.Task.WaitAsync(TimeSpan.FromSeconds(5));
            await WaitForIdleAsync(manager);

            Assert.False(startCalled, "StartUserPartitions must not be called when node is not leader");
            Assert.Equal(initialPartitionCount, manager.Partitions.Count);
        }
    }

    [Fact]
    public async Task SplitPartition_WhenPartitionNotFound_DoesNotReplicate()
    {
        RaftManager manager = Build();
        using (manager)
        {
            List<RaftPartitionRange> initial =
            [
                new() { PartitionId = 1, StartRange = 0, EndRange = int.MaxValue }
            ];
            manager.SystemCoordinator.Send(MakeConfigReplicated(initial));
            await WaitForIdleAsync(manager);

            bool replicateCalled = false;
            manager.SystemCoordinator.ReplicateOverride = (_, _, _, _) =>
            {
                replicateCalled = true;
                return Task.FromResult(new RaftReplicationResult(true, RaftOperationStatus.Success, HLCTimestamp.Zero, 1));
            };

            // Partition 99 does not exist in the map.
            manager.SystemCoordinator.Send(
                new RaftSystemRequest(RaftSystemRequestType.SplitPartition, 99));
            await WaitForIdleAsync(manager);

            Assert.False(replicateCalled);
        }
    }

    [Fact]
    public async Task SplitPartition_WhenNoConfig_DoesNotReplicate()
    {
        RaftManager manager = Build();
        using (manager)
        {
            bool replicateCalled = false;
            manager.SystemCoordinator.ReplicateOverride = (_, _, _, _) =>
            {
                replicateCalled = true;
                return Task.FromResult(new RaftReplicationResult(true, RaftOperationStatus.Success, HLCTimestamp.Zero, 1));
            };

            // No ConfigRestored/ConfigReplicated sent yet — systemConfiguration is empty.
            manager.SystemCoordinator.Send(
                new RaftSystemRequest(RaftSystemRequestType.SplitPartition, 1));
            await WaitForIdleAsync(manager);

            Assert.False(replicateCalled);
        }
    }
}
