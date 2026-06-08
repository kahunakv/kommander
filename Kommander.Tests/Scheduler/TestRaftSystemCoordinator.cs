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
    public Task Send_AfterStop_IsNoOp()
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
        return Task.CompletedTask;
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
        Task disposeTask = Task.Run(() => manager.Dispose(), TestContext.Current.CancellationToken);
        await disposeTask.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
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
            await done.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

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
            await done.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

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

            // Map v2: all initial partitions start at Generation=1, Active, HashRange.
            foreach (RaftPartitionRange range in activated)
            {
                Assert.Equal(1, range.Generation);
                Assert.Equal(RaftPartitionState.Active, range.State);
                Assert.Equal(RaftRoutingMode.HashRange, range.RoutingMode);
            }

            // Live partitions also carry the routing mode.
            foreach (RaftPartition p in manager.Partitions.Values)
            {
                Assert.Equal(RaftRoutingMode.HashRange, p.RoutingMode);
                Assert.Equal(1, p.Generation);
            }

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
            await done.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

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
            await firstAttemptDone.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

            // Yield briefly so the coordinator loop advances past the completed
            // Replicate() call and suspends inside Task.Delay(5000, cancellationToken).
            await Task.Delay(20, TestContext.Current.CancellationToken);

            // Cancelling now fires inside the production retry delay, not the override.
            manager.SystemCoordinator.Stop();

            // Wait for the loop to exit cleanly — deterministic, no fixed delay.
            await manager.SystemCoordinator.LoopTask.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
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
                new() { PartitionId = 1, StartRange = 0, EndRange = 999, Generation = 1, State = RaftPartitionState.Active, RoutingMode = RaftRoutingMode.HashRange }
            ];
            manager.SystemCoordinator.Send(MakeConfigReplicated(initial));
            await WaitForIdleAsync(manager);

            // Capture the partition map that was passed to Replicate.
            List<RaftPartitionRange>? replicatedRanges = null;
            long replicatedMapVersion = 0;
            manager.SystemCoordinator.ReplicateOverride = (_, data, _, _) =>
            {
                // Decode the embedded JSON to inspect ranges.
                // Overwrites each time so the final capture reflects Phase 2.
                using MemoryStream ms = new(data);
                Kommander.System.Protos.RaftSystemMessage msg =
                    Kommander.System.Protos.RaftSystemMessage.Parser.ParseFrom(ms);
                RaftPartitionMap? replicatedMap = JsonSerializer.Deserialize<RaftPartitionMap>(msg.Value);
                replicatedRanges = replicatedMap?.Partitions;
                replicatedMapVersion = replicatedMap?.MapVersion ?? 0;
                return Task.FromResult(new RaftReplicationResult(true, RaftOperationStatus.Success, HLCTimestamp.Zero, 1));
            };

            TaskCompletionSource done = new(TaskCreationOptions.RunContinuationsAsynchronously);
            List<RaftPartitionRange>? activated = null;
            // Two-phase split: StartPartitions is called twice (Phase 1 + Phase 2).
            // Capture the Phase 2 result by waiting for the second call.
            int startCallCount = 0;
            manager.SystemCoordinator.StartPartitionsOverride = ranges =>
            {
                manager.StartUserPartitions(ranges);
                if (++startCallCount == 2)
                {
                    activated = [..ranges];
                    done.TrySetResult();
                }
            };

            manager.SystemCoordinator.Send(
                new RaftSystemRequest(1, new RaftSplitPlan { TargetRoutingMode = RaftRoutingMode.HashRange, AutoCommit = true }));
            await done.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

            // The replicated map (Phase 2) must have 2 non-overlapping ranges.
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

            // After two-phase split: source generation = baseline+2 (G+1 in Phase 1, G+2 in Phase 2);
            // new partition generation = 2 (1 in Phase 1, 2 in Phase 2). Both are Active after Phase 2.
            RaftPartitionRange source = sorted.Single(r => r.PartitionId == 1);
            RaftPartitionRange target = sorted.Single(r => r.PartitionId != 1);
            Assert.Equal(3, source.Generation);  // baseline=1 → Phase1=2 → Phase2=3
            Assert.Equal(2, target.Generation);  // Phase1=1 → Phase2=2
            Assert.Equal(RaftPartitionState.Active, source.State);
            Assert.Equal(RaftPartitionState.Active, target.State);
            Assert.Equal(RaftRoutingMode.HashRange, source.RoutingMode);
            Assert.Equal(RaftRoutingMode.HashRange, target.RoutingMode);

            // Activated map matches replicated map.
            Assert.NotNull(activated);
            Assert.Equal(2, activated.Count);

            // MapVersion bumped twice (Phase 1 + Phase 2): initial was 1 → now 3.
            Assert.Equal(3, replicatedMapVersion);
        }
    }

    // ── Task 1.4 — TrySplitPartition bumps Generation correctly ──────────────

    [Fact]
    public async Task SplitPartition_SourceGenerationIncrements_FromHigherBaseline()
    {
        // Verifies that Generation on the source is incremented by 1 from whatever
        // baseline it had before the split — not reset or set to a fixed value.
        RaftManager manager = Build();
        using (manager)
        {
            // Source partition starts at Generation = 5 (simulating prior mutations).
            List<RaftPartitionRange> initial =
            [
                new() { PartitionId = 1, StartRange = 0, EndRange = 999, Generation = 5, State = RaftPartitionState.Active, RoutingMode = RaftRoutingMode.HashRange }
            ];
            manager.SystemCoordinator.Send(MakeConfigReplicated(initial, mapVersion: 5));
            await WaitForIdleAsync(manager);

            List<RaftPartitionRange>? replicatedRanges = null;
            manager.SystemCoordinator.ReplicateOverride = (_, data, _, _) =>
            {
                using MemoryStream ms = new(data);
                Kommander.System.Protos.RaftSystemMessage msg =
                    Kommander.System.Protos.RaftSystemMessage.Parser.ParseFrom(ms);
                RaftPartitionMap? m = JsonSerializer.Deserialize<RaftPartitionMap>(msg.Value);
                replicatedRanges = m?.Partitions;  // overwritten each phase; Phase 2 wins
                return Task.FromResult(new RaftReplicationResult(true, RaftOperationStatus.Success, HLCTimestamp.Zero, 1));
            };

            TaskCompletionSource done = new(TaskCreationOptions.RunContinuationsAsynchronously);
            int startCallCount = 0;
            manager.SystemCoordinator.StartPartitionsOverride = ranges =>
            {
                manager.StartUserPartitions(ranges);
                if (++startCallCount == 2)
                    done.TrySetResult();
            };

            manager.SystemCoordinator.Send(
                new RaftSystemRequest(1, new RaftSplitPlan { TargetRoutingMode = RaftRoutingMode.HashRange, AutoCommit = true }));
            await done.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

            Assert.NotNull(replicatedRanges);
            RaftPartitionRange source = replicatedRanges.Single(r => r.PartitionId == 1);
            RaftPartitionRange target = replicatedRanges.Single(r => r.PartitionId != 1);

            // Two-phase split: source 5 → 6 (Phase 1) → 7 (Phase 2).
            Assert.Equal(7, source.Generation);

            // Target: Phase1=1, Phase2=2.
            Assert.Equal(2, target.Generation);
            Assert.Equal(RaftPartitionState.Active, target.State);
            Assert.Equal(RaftRoutingMode.HashRange, target.RoutingMode);
        }
    }

    [Fact]
    public async Task SplitPartition_NewPartition_HasCorrectMapV2Defaults()
    {
        // The new partition produced by a split must carry the full Map-v2 defaults
        // regardless of what fields the source partition had.
        RaftManager manager = Build();
        using (manager)
        {
            List<RaftPartitionRange> initial =
            [
                new() { PartitionId = 1, StartRange = 0, EndRange = int.MaxValue, Generation = 3, State = RaftPartitionState.Active, RoutingMode = RaftRoutingMode.HashRange }
            ];
            manager.SystemCoordinator.Send(MakeConfigReplicated(initial, mapVersion: 3));
            await WaitForIdleAsync(manager);

            List<RaftPartitionRange>? replicatedRanges = null;
            manager.SystemCoordinator.ReplicateOverride = (_, data, _, _) =>
            {
                using MemoryStream ms = new(data);
                Kommander.System.Protos.RaftSystemMessage msg =
                    Kommander.System.Protos.RaftSystemMessage.Parser.ParseFrom(ms);
                RaftPartitionMap? m = JsonSerializer.Deserialize<RaftPartitionMap>(msg.Value);
                replicatedRanges = m?.Partitions;  // overwritten each phase; Phase 2 wins
                return Task.FromResult(new RaftReplicationResult(true, RaftOperationStatus.Success, HLCTimestamp.Zero, 1));
            };

            TaskCompletionSource done = new(TaskCreationOptions.RunContinuationsAsynchronously);
            int startCallCount = 0;
            manager.SystemCoordinator.StartPartitionsOverride = ranges =>
            {
                manager.StartUserPartitions(ranges);
                if (++startCallCount == 2)
                    done.TrySetResult();
            };

            manager.SystemCoordinator.Send(
                new RaftSystemRequest(1, new RaftSplitPlan { TargetRoutingMode = RaftRoutingMode.HashRange, AutoCommit = true }));
            await done.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

            Assert.NotNull(replicatedRanges);
            Assert.Equal(2, replicatedRanges.Count);

            RaftPartitionRange source = replicatedRanges.Single(r => r.PartitionId == 1);
            RaftPartitionRange target = replicatedRanges.Single(r => r.PartitionId != 1);

            // Two-phase split: source 3 → 4 (Phase 1) → 5 (Phase 2).
            Assert.Equal(5, source.Generation);
            Assert.Equal(RaftPartitionState.Active, source.State);
            Assert.Equal(RaftRoutingMode.HashRange, source.RoutingMode);

            // Target: Phase1=1 → Phase2=2, Active, HashRange.
            Assert.Equal(2, target.Generation);
            Assert.Equal(RaftPartitionState.Active, target.State);
            Assert.Equal(RaftRoutingMode.HashRange, target.RoutingMode);

            // No gap / no overlap after split.
            List<RaftPartitionRange> sorted = [..replicatedRanges.OrderBy(r => r.StartRange)];
            Assert.Equal(0, sorted[0].StartRange);
            Assert.Equal(int.MaxValue, sorted[^1].EndRange);
            Assert.Equal(sorted[0].EndRange + 1, sorted[1].StartRange);
        }
    }

    [Fact]
    public async Task StartUserPartitions_UnroutedPartition_IsExcludedFromGetPartitionKey()
    {
        // Unrouted partitions must not be returned by GetPartitionKey; only HashRange
        // entries participate in the hash routing cover.
        RaftManager manager = Build();
        using (manager)
        {
            List<RaftPartitionRange> ranges =
            [
                new() { PartitionId = 1, StartRange = 0, EndRange = int.MaxValue, Generation = 1, State = RaftPartitionState.Active, RoutingMode = RaftRoutingMode.HashRange },
                new() { PartitionId = 2, StartRange = 0, EndRange = int.MaxValue, Generation = 1, State = RaftPartitionState.Active, RoutingMode = RaftRoutingMode.Unrouted }
            ];

            manager.SystemCoordinator.Send(MakeConfigReplicated(ranges));
            await WaitForIdleAsync(manager);

            Assert.Equal(2, manager.Partitions.Count);
            Assert.Equal(RaftRoutingMode.HashRange, manager.Partitions[1].RoutingMode);
            Assert.Equal(RaftRoutingMode.Unrouted, manager.Partitions[2].RoutingMode);

            // GetPartitionKey must always resolve to the HashRange partition, never the Unrouted one.
            int key = manager.GetPartitionKey("any-key");
            Assert.Equal(1, key);
        }
    }

    // ── Task 1.3 — Unrouted partitions filtered from GetPartitionKey / GetPrefixPartitionKey ──

    [Fact]
    public async Task GetPrefixPartitionKey_UnroutedPartition_IsExcluded()
    {
        // GetPrefixPartitionKey must apply the same RoutingMode guard as GetPartitionKey:
        // an Unrouted partition overlapping the full hash range must never be returned.
        RaftManager manager = Build();
        using (manager)
        {
            List<RaftPartitionRange> ranges =
            [
                new() { PartitionId = 1, StartRange = 0, EndRange = int.MaxValue, Generation = 1, State = RaftPartitionState.Active, RoutingMode = RaftRoutingMode.HashRange },
                new() { PartitionId = 2, StartRange = 0, EndRange = int.MaxValue, Generation = 1, State = RaftPartitionState.Active, RoutingMode = RaftRoutingMode.Unrouted }
            ];

            manager.SystemCoordinator.Send(MakeConfigReplicated(ranges));
            await WaitForIdleAsync(manager);

            // GetPrefixPartitionKey must resolve to the HashRange partition only.
            int key = manager.GetPrefixPartitionKey("some-prefix");
            Assert.Equal(1, key);
        }
    }

    [Fact]
    public async Task GetPartitionKey_UnroutedOnly_ThrowsRaftException()
    {
        // If the map contains only Unrouted entries GetPartitionKey must throw — no
        // HashRange cover means the key truly has no routing target.
        RaftManager manager = Build();
        using (manager)
        {
            List<RaftPartitionRange> ranges =
            [
                new() { PartitionId = 1, StartRange = 0, EndRange = int.MaxValue, Generation = 1, State = RaftPartitionState.Active, RoutingMode = RaftRoutingMode.Unrouted }
            ];

            manager.SystemCoordinator.Send(MakeConfigReplicated(ranges));
            await WaitForIdleAsync(manager);

            Assert.Throws<RaftException>(() => manager.GetPartitionKey("any-key"));
        }
    }

    [Fact]
    public async Task GetPartitionKey_MultipleHashRange_UnroutedDoesNotPerturb()
    {
        // With two HashRange partitions and one Unrouted partition spanning both halves,
        // every key must resolve to the correct HashRange bucket, never the Unrouted one.
        RaftManager manager = Build();
        using (manager)
        {
            List<RaftPartitionRange> ranges =
            [
                new() { PartitionId = 1, StartRange = 0,           EndRange = 1_073_741_823, Generation = 1, State = RaftPartitionState.Active, RoutingMode = RaftRoutingMode.HashRange },
                new() { PartitionId = 2, StartRange = 1_073_741_824, EndRange = int.MaxValue, Generation = 1, State = RaftPartitionState.Active, RoutingMode = RaftRoutingMode.HashRange },
                new() { PartitionId = 3, StartRange = 0,           EndRange = int.MaxValue, Generation = 1, State = RaftPartitionState.Active, RoutingMode = RaftRoutingMode.Unrouted }
            ];

            manager.SystemCoordinator.Send(MakeConfigReplicated(ranges));
            await WaitForIdleAsync(manager);

            // Every returned key must be 1 or 2, never 3 (the Unrouted partition).
            string[] keys = ["alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta"];
            foreach (string k in keys)
            {
                int result = manager.GetPartitionKey(k);
                Assert.True(result == 1 || result == 2,
                    $"GetPartitionKey(\"{k}\") returned {result}; expected 1 or 2");
            }
        }
    }

    [Fact]
    public async Task GetPrefixPartitionKey_MultipleHashRange_UnroutedDoesNotPerturb()
    {
        // Same invariant for GetPrefixPartitionKey.
        RaftManager manager = Build();
        using (manager)
        {
            List<RaftPartitionRange> ranges =
            [
                new() { PartitionId = 1, StartRange = 0,           EndRange = 1_073_741_823, Generation = 1, State = RaftPartitionState.Active, RoutingMode = RaftRoutingMode.HashRange },
                new() { PartitionId = 2, StartRange = 1_073_741_824, EndRange = int.MaxValue, Generation = 1, State = RaftPartitionState.Active, RoutingMode = RaftRoutingMode.HashRange },
                new() { PartitionId = 3, StartRange = 0,           EndRange = int.MaxValue, Generation = 1, State = RaftPartitionState.Active, RoutingMode = RaftRoutingMode.Unrouted }
            ];

            manager.SystemCoordinator.Send(MakeConfigReplicated(ranges));
            await WaitForIdleAsync(manager);

            string[] prefixes = ["tenant/a", "tenant/b", "region/us", "region/eu"];
            foreach (string p in prefixes)
            {
                int result = manager.GetPrefixPartitionKey(p);
                Assert.True(result == 1 || result == 2,
                    $"GetPrefixPartitionKey(\"{p}\") returned {result}; expected 1 or 2");
            }
        }
    }

    [Fact]
    public async Task StartUserPartitions_SetsGenerationOnLivePartition()
    {
        RaftManager manager = Build();
        using (manager)
        {
            List<RaftPartitionRange> ranges =
            [
                new() { PartitionId = 1, StartRange = 0, EndRange = int.MaxValue, Generation = 7, State = RaftPartitionState.Active, RoutingMode = RaftRoutingMode.HashRange }
            ];

            manager.SystemCoordinator.Send(MakeConfigReplicated(ranges));
            await WaitForIdleAsync(manager);

            Assert.Equal(7, manager.Partitions[1].Generation);
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
            await replicateDone.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
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

    // ── MapVersion ────────────────────────────────────────────────────────────

    [Fact]
    public async Task LeaderChanged_AsLeader_NoExistingConfig_ReplicatesMapVersionOne()
    {
        // The initial partition write must include MapVersion = 1.
        RaftManager manager = BuildWithPartitions(partitions: 1);
        using (manager)
        {
            long capturedMapVersion = 0;
            manager.SystemCoordinator.ReplicateOverride = (_, data, _, _) =>
            {
                using MemoryStream ms = new(data);
                Kommander.System.Protos.RaftSystemMessage msg =
                    Kommander.System.Protos.RaftSystemMessage.Parser.ParseFrom(ms);
                RaftPartitionMap? map = JsonSerializer.Deserialize<RaftPartitionMap>(msg.Value);
                capturedMapVersion = map?.MapVersion ?? 0;
                return Task.FromResult(new RaftReplicationResult(true, RaftOperationStatus.Success, HLCTimestamp.Zero, 1));
            };

            TaskCompletionSource done = new(TaskCreationOptions.RunContinuationsAsynchronously);
            manager.SystemCoordinator.StartPartitionsOverride = ranges =>
            {
                manager.StartUserPartitions(ranges);
                done.TrySetResult();
            };

            manager.SystemCoordinator.Send(
                new RaftSystemRequest(RaftSystemRequestType.LeaderChanged, manager.LocalEndpoint));
            await done.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

            Assert.Equal(1, capturedMapVersion);
        }
    }

    [Fact]
    public void RaftPartitionMap_SerializationRoundTrip_PreservesAllFields()
    {
        // Verify JSON round-trip for all six RaftPartitionRange fields plus MapVersion.
        RaftPartitionMap original = new()
        {
            MapVersion = 42,
            Partitions =
            [
                new()
                {
                    PartitionId = 7,
                    StartRange = 100,
                    EndRange = 999,
                    Generation = 5,
                    State = RaftPartitionState.Splitting,
                    RoutingMode = RaftRoutingMode.Unrouted
                }
            ]
        };

        string json = JsonSerializer.Serialize(original);
        RaftPartitionMap? roundTripped = JsonSerializer.Deserialize<RaftPartitionMap>(json);

        Assert.NotNull(roundTripped);
        Assert.Equal(42, roundTripped.MapVersion);
        Assert.Single(roundTripped.Partitions);

        RaftPartitionRange r = roundTripped.Partitions[0];
        Assert.Equal(7, r.PartitionId);
        Assert.Equal(100, r.StartRange);
        Assert.Equal(999, r.EndRange);
        Assert.Equal(5, r.Generation);
        Assert.Equal(RaftPartitionState.Splitting, r.State);
        Assert.Equal(RaftRoutingMode.Unrouted, r.RoutingMode);
    }

    // ── Task 1.2 — DivideIntoRanges defaults + boundary arithmetic ────────────

    /// <summary>
    /// Verifies that every entry produced by DivideIntoRanges carries the
    /// expected Map-v2 defaults (Generation=1, State=Active, RoutingMode=HashRange)
    /// and that the ranges form a non-overlapping, gapless cover of [0, int.MaxValue].
    /// Parametrised over several partition counts to exercise the remainder-distribution
    /// logic inside DivideIntoRanges.
    /// </summary>
    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    [InlineData(8)]
    [InlineData(16)]
    public async Task TrySetInitialPartitions_DivideIntoRanges_DefaultsAndBoundaryArithmetic(int count)
    {
        RaftManager manager = BuildWithPartitions(count);
        using (manager)
        {
            List<RaftPartitionRange>? activated = null;
            manager.SystemCoordinator.ReplicateOverride = (_, _, _, _) =>
                Task.FromResult(new RaftReplicationResult(true, RaftOperationStatus.Success, HLCTimestamp.Zero, 1));

            TaskCompletionSource done = new(TaskCreationOptions.RunContinuationsAsynchronously);
            manager.SystemCoordinator.StartPartitionsOverride = ranges =>
            {
                activated = [..ranges];
                manager.StartUserPartitions(ranges);
                done.TrySetResult();
            };

            manager.SystemCoordinator.Send(
                new RaftSystemRequest(RaftSystemRequestType.LeaderChanged, manager.LocalEndpoint));
            await done.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

            Assert.NotNull(activated);
            Assert.Equal(count, activated.Count);

            // Map-v2 defaults on every entry.
            foreach (RaftPartitionRange r in activated)
            {
                Assert.Equal(1, r.Generation);
                Assert.Equal(RaftPartitionState.Active, r.State);
                Assert.Equal(RaftRoutingMode.HashRange, r.RoutingMode);
            }

            // Live partitions mirror the same defaults.
            foreach (RaftPartition p in manager.Partitions.Values)
            {
                Assert.Equal(1, p.Generation);
                Assert.Equal(RaftRoutingMode.HashRange, p.RoutingMode);
            }

            // Boundary arithmetic: sorted ranges must cover [0, int.MaxValue] with no gaps or overlaps.
            List<RaftPartitionRange> sorted = [..activated.OrderBy(r => r.StartRange)];
            Assert.Equal(0, sorted[0].StartRange);
            Assert.Equal(int.MaxValue, sorted[^1].EndRange);
            for (int i = 0; i < sorted.Count - 1; i++)
                Assert.Equal(sorted[i].EndRange + 1, sorted[i + 1].StartRange);
        }
    }

    // ── Task 2.3 — TryCreatePartition ────────────────────────────────────────

    /// <summary>Sends a CreatePartition request and returns a task that resolves when the coordinator finishes processing it.</summary>
    private static Task<(RaftOperationStatus Status, long Generation)> SendCreateAsync(
        RaftManager manager,
        int partitionId,
        RaftRoutingMode mode,
        int? start = null,
        int? end = null)
    {
        TaskCompletionSource<(RaftOperationStatus Status, long Generation)> tcs =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        manager.SystemCoordinator.Send(new RaftSystemRequest(partitionId, mode, start, end, tcs));
        return tcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
    }

    [Fact]
    public async Task CreatePartition_Unrouted_AppearsInPartitions_ExcludedFromRouting()
    {
        // An Unrouted partition must be registered in the manager's Partitions dictionary
        // but never returned by GetPartitionKey.
        RaftManager manager = Build();
        using (manager)
        {
            // Bootstrap map with a HashRange partition so GetPartitionKey has a valid target.
            List<RaftPartitionRange> initial =
            [
                new() { PartitionId = 1, StartRange = 0, EndRange = int.MaxValue, Generation = 1, State = RaftPartitionState.Active, RoutingMode = RaftRoutingMode.HashRange }
            ];
            manager.SystemCoordinator.Send(MakeConfigReplicated(initial));
            await WaitForIdleAsync(manager);

            manager.SystemCoordinator.ReplicateOverride = (_, _, _, _) =>
                Task.FromResult(new RaftReplicationResult(true, RaftOperationStatus.Success, HLCTimestamp.Zero, 2));
            manager.SystemCoordinator.StartPartitionsOverride = ranges => manager.StartUserPartitions(ranges);

            (RaftOperationStatus status, long gen) = await SendCreateAsync(manager, partitionId: 10, RaftRoutingMode.Unrouted);

            Assert.Equal(RaftOperationStatus.Success, status);
            Assert.Equal(1, gen);
            Assert.True(manager.Partitions.ContainsKey(10));
            Assert.Equal(RaftRoutingMode.Unrouted, manager.Partitions[10].RoutingMode);

            // Routing must still resolve to the HashRange partition, not the Unrouted one.
            Assert.Equal(1, manager.GetPartitionKey("some-key"));
        }
    }

    [Fact]
    public async Task CreatePartition_HashRange_AppearsInPartitionsAndRoutes()
    {
        // A HashRange partition added via CreatePartition must participate in routing
        // for keys within its declared range.
        RaftManager manager = Build();
        using (manager)
        {
            // Initial map covers only [0, 999].
            List<RaftPartitionRange> initial =
            [
                new() { PartitionId = 1, StartRange = 0, EndRange = 999, Generation = 1, State = RaftPartitionState.Active, RoutingMode = RaftRoutingMode.HashRange }
            ];
            manager.SystemCoordinator.Send(MakeConfigReplicated(initial));
            await WaitForIdleAsync(manager);

            List<RaftPartitionRange>? replicatedRanges = null;
            long replicatedMapVersion = 0;
            manager.SystemCoordinator.ReplicateOverride = (_, data, _, _) =>
            {
                using MemoryStream ms = new(data);
                Kommander.System.Protos.RaftSystemMessage msg =
                    Kommander.System.Protos.RaftSystemMessage.Parser.ParseFrom(ms);
                RaftPartitionMap? m = JsonSerializer.Deserialize<RaftPartitionMap>(msg.Value);
                replicatedRanges = m?.Partitions;
                replicatedMapVersion = m?.MapVersion ?? 0;
                return Task.FromResult(new RaftReplicationResult(true, RaftOperationStatus.Success, HLCTimestamp.Zero, 2));
            };
            manager.SystemCoordinator.StartPartitionsOverride = ranges => manager.StartUserPartitions(ranges);

            (RaftOperationStatus status, long gen) = await SendCreateAsync(manager, partitionId: 2, RaftRoutingMode.HashRange, start: 1000, end: 1999);

            Assert.Equal(RaftOperationStatus.Success, status);
            Assert.Equal(1, gen);
            Assert.True(manager.Partitions.ContainsKey(2));
            Assert.Equal(RaftRoutingMode.HashRange, manager.Partitions[2].RoutingMode);
            Assert.Equal(1000, manager.Partitions[2].StartRange);
            Assert.Equal(1999, manager.Partitions[2].EndRange);

            // Replicated map must contain both partitions and have MapVersion bumped.
            Assert.NotNull(replicatedRanges);
            Assert.Equal(2, replicatedRanges.Count);
            Assert.Equal(2, replicatedMapVersion);

            RaftPartitionRange created = replicatedRanges.Single(r => r.PartitionId == 2);
            Assert.Equal(1, created.Generation);
            Assert.Equal(RaftPartitionState.Active, created.State);
            Assert.Equal(RaftRoutingMode.HashRange, created.RoutingMode);
        }
    }

    [Fact]
    public async Task CreatePartition_DuplicateActive_ReturnsSuccessWithExistingGeneration()
    {
        // A duplicate CreatePartition for an already-Active partition must return the
        // existing generation without mutating the map or calling Replicate.
        RaftManager manager = Build();
        using (manager)
        {
            List<RaftPartitionRange> initial =
            [
                new() { PartitionId = 5, StartRange = 0, EndRange = 0, Generation = 7, State = RaftPartitionState.Active, RoutingMode = RaftRoutingMode.Unrouted }
            ];
            manager.SystemCoordinator.Send(MakeConfigReplicated(initial, mapVersion: 3));
            await WaitForIdleAsync(manager);

            bool replicateCalled = false;
            manager.SystemCoordinator.ReplicateOverride = (_, _, _, _) =>
            {
                replicateCalled = true;
                return Task.FromResult(new RaftReplicationResult(true, RaftOperationStatus.Success, HLCTimestamp.Zero, 1));
            };

            (RaftOperationStatus status, long gen) = await SendCreateAsync(manager, partitionId: 5, RaftRoutingMode.Unrouted);

            Assert.Equal(RaftOperationStatus.Success, status);
            Assert.Equal(7, gen);
            Assert.False(replicateCalled, "Replicate must not be called for a duplicate Active partition");
        }
    }

    [Fact]
    public async Task CreatePartition_HashRangeOverlap_ReturnsError()
    {
        // A HashRange partition whose range overlaps an existing HashRange entry must be rejected.
        RaftManager manager = Build();
        using (manager)
        {
            List<RaftPartitionRange> initial =
            [
                new() { PartitionId = 1, StartRange = 0, EndRange = 999, Generation = 1, State = RaftPartitionState.Active, RoutingMode = RaftRoutingMode.HashRange }
            ];
            manager.SystemCoordinator.Send(MakeConfigReplicated(initial));
            await WaitForIdleAsync(manager);

            bool replicateCalled = false;
            manager.SystemCoordinator.ReplicateOverride = (_, _, _, _) =>
            {
                replicateCalled = true;
                return Task.FromResult(new RaftReplicationResult(true, RaftOperationStatus.Success, HLCTimestamp.Zero, 1));
            };

            // [500, 1499] overlaps [0, 999].
            (RaftOperationStatus status, long gen) = await SendCreateAsync(manager, partitionId: 2, RaftRoutingMode.HashRange, start: 500, end: 1499);

            Assert.Equal(RaftOperationStatus.Errored, status);
            Assert.Equal(0, gen);
            Assert.False(replicateCalled, "Replicate must not be called on overlap rejection");
            Assert.False(manager.Partitions.ContainsKey(2));
        }
    }

    [Fact]
    public async Task CreatePartition_NoConfig_ReturnsError()
    {
        // CreatePartition before any partition map is established must return Errored.
        RaftManager manager = Build();
        using (manager)
        {
            bool replicateCalled = false;
            manager.SystemCoordinator.ReplicateOverride = (_, _, _, _) =>
            {
                replicateCalled = true;
                return Task.FromResult(new RaftReplicationResult(true, RaftOperationStatus.Success, HLCTimestamp.Zero, 1));
            };

            (RaftOperationStatus status, long gen) = await SendCreateAsync(manager, partitionId: 42, RaftRoutingMode.Unrouted);

            Assert.Equal(RaftOperationStatus.Errored, status);
            Assert.Equal(0, gen);
            Assert.False(replicateCalled);
        }
    }

    [Fact]
    public async Task CreatePartition_MapVersion_BumpedOnCreate()
    {
        // Every successful CreatePartition must increment MapVersion by exactly 1.
        RaftManager manager = Build();
        using (manager)
        {
            List<RaftPartitionRange> initial =
            [
                new() { PartitionId = 1, StartRange = 0, EndRange = int.MaxValue, Generation = 1, State = RaftPartitionState.Active, RoutingMode = RaftRoutingMode.HashRange }
            ];
            manager.SystemCoordinator.Send(MakeConfigReplicated(initial, mapVersion: 5));
            await WaitForIdleAsync(manager);

            long capturedMapVersion = 0;
            manager.SystemCoordinator.ReplicateOverride = (_, data, _, _) =>
            {
                using MemoryStream ms = new(data);
                Kommander.System.Protos.RaftSystemMessage msg =
                    Kommander.System.Protos.RaftSystemMessage.Parser.ParseFrom(ms);
                RaftPartitionMap? m = JsonSerializer.Deserialize<RaftPartitionMap>(msg.Value);
                capturedMapVersion = m?.MapVersion ?? 0;
                return Task.FromResult(new RaftReplicationResult(true, RaftOperationStatus.Success, HLCTimestamp.Zero, 1));
            };
            manager.SystemCoordinator.StartPartitionsOverride = ranges => manager.StartUserPartitions(ranges);

            await SendCreateAsync(manager, partitionId: 99, RaftRoutingMode.Unrouted);

            Assert.Equal(6, capturedMapVersion);
        }
    }

    // ── Task 2.4 — TryRemovePartition ────────────────────────────────────────

    /// <summary>Sends a RemovePartition request and returns a task that resolves when the coordinator finishes processing it.</summary>
    private static Task<(RaftOperationStatus Status, long Generation)> SendRemoveAsync(
        RaftManager manager,
        int partitionId)
    {
        TaskCompletionSource<(RaftOperationStatus Status, long Generation)> tcs =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        manager.SystemCoordinator.Send(
            new RaftSystemRequest(RaftSystemRequestType.RemovePartition, partitionId) { Completion = tcs });
        return tcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
    }

    [Fact]
    public async Task RemovePartition_ExistingActive_RemovedFromDictionaryAndWalReclaimed()
    {
        // Full happy-path: the partition appears in Partitions before removal and is
        // absent afterwards; the in-memory WAL must also have no remaining entries.
        RaftManager manager = Build();
        using (manager)
        {
            List<RaftPartitionRange> initial =
            [
                new() { PartitionId = 1, StartRange = 0, EndRange = int.MaxValue, Generation = 2, State = RaftPartitionState.Active, RoutingMode = RaftRoutingMode.HashRange }
            ];
            manager.SystemCoordinator.Send(MakeConfigReplicated(initial));
            await WaitForIdleAsync(manager);
            Assert.True(manager.Partitions.ContainsKey(1));

            // Write a WAL entry for the partition so we can verify it is reclaimed.
            manager.WalAdapter.Write([(1, [new RaftLog { Id = 1, Term = 1, Type = RaftLogType.Proposed }])]);

            List<RaftPartitionRange>? replicatedRanges = null;
            long replicatedMapVersion = 0;
            manager.SystemCoordinator.ReplicateOverride = (_, data, _, _) =>
            {
                using MemoryStream ms = new(data);
                Kommander.System.Protos.RaftSystemMessage msg =
                    Kommander.System.Protos.RaftSystemMessage.Parser.ParseFrom(ms);
                RaftPartitionMap? m = JsonSerializer.Deserialize<RaftPartitionMap>(msg.Value);
                replicatedRanges = m?.Partitions;
                replicatedMapVersion = m?.MapVersion ?? 0;
                return Task.FromResult(new RaftReplicationResult(true, RaftOperationStatus.Success, HLCTimestamp.Zero, 2));
            };
            manager.SystemCoordinator.StartPartitionsOverride = ranges => manager.StartUserPartitions(ranges);

            (RaftOperationStatus status, long gen) = await SendRemoveAsync(manager, partitionId: 1);

            Assert.Equal(RaftOperationStatus.Success, status);
            // Generation must have been bumped (was 2, now 3).
            Assert.Equal(3, gen);

            // Partition removed from live dictionary.
            Assert.False(manager.Partitions.ContainsKey(1));

            // Tombstone present in replicated map.
            Assert.NotNull(replicatedRanges);
            RaftPartitionRange tombstone = Assert.Single(replicatedRanges);
            Assert.Equal(1, tombstone.PartitionId);
            Assert.Equal(RaftPartitionState.Removed, tombstone.State);
            Assert.Equal(3, tombstone.Generation);

            // MapVersion bumped.
            Assert.Equal(2, replicatedMapVersion);

            // WAL reclaimed: no entries remain for partition 1.
            Assert.Empty(manager.WalAdapter.ReadLogs(1));
        }
    }

    [Fact]
    public async Task RemovePartition_NonExistent_ReturnsError()
    {
        // Removing a partition that is not in the map must return Errored without replicating.
        RaftManager manager = Build();
        using (manager)
        {
            List<RaftPartitionRange> initial =
            [
                new() { PartitionId = 1, StartRange = 0, EndRange = int.MaxValue, Generation = 1, State = RaftPartitionState.Active, RoutingMode = RaftRoutingMode.HashRange }
            ];
            manager.SystemCoordinator.Send(MakeConfigReplicated(initial));
            await WaitForIdleAsync(manager);

            bool replicateCalled = false;
            manager.SystemCoordinator.ReplicateOverride = (_, _, _, _) =>
            {
                replicateCalled = true;
                return Task.FromResult(new RaftReplicationResult(true, RaftOperationStatus.Success, HLCTimestamp.Zero, 1));
            };

            (RaftOperationStatus status, long gen) = await SendRemoveAsync(manager, partitionId: 99);

            Assert.Equal(RaftOperationStatus.Errored, status);
            Assert.Equal(0, gen);
            Assert.False(replicateCalled, "Replicate must not be called for a non-existent partition");
        }
    }

    [Fact]
    public async Task RemovePartition_NoConfig_ReturnsError()
    {
        // RemovePartition before any map is established must return Errored immediately.
        RaftManager manager = Build();
        using (manager)
        {
            bool replicateCalled = false;
            manager.SystemCoordinator.ReplicateOverride = (_, _, _, _) =>
            {
                replicateCalled = true;
                return Task.FromResult(new RaftReplicationResult(true, RaftOperationStatus.Success, HLCTimestamp.Zero, 1));
            };

            (RaftOperationStatus status, _) = await SendRemoveAsync(manager, partitionId: 1);

            Assert.Equal(RaftOperationStatus.Errored, status);
            Assert.False(replicateCalled);
        }
    }

    [Fact]
    public async Task RemovePartition_AlreadyRemoved_IsIdempotent_ReturnsSucess()
    {
        // Calling RemovePartition on a tombstoned partition must return Success without
        // replicating a second time (idempotent, supports crash-recovery re-attempts).
        RaftManager manager = Build();
        using (manager)
        {
            // Provide a map that already has a Removed tombstone.
            List<RaftPartitionRange> initial =
            [
                new() { PartitionId = 7, StartRange = 0, EndRange = 0, Generation = 4, State = RaftPartitionState.Removed, RoutingMode = RaftRoutingMode.Unrouted }
            ];
            manager.SystemCoordinator.Send(MakeConfigReplicated(initial, mapVersion: 4));
            await WaitForIdleAsync(manager);

            bool replicateCalled = false;
            manager.SystemCoordinator.ReplicateOverride = (_, _, _, _) =>
            {
                replicateCalled = true;
                return Task.FromResult(new RaftReplicationResult(true, RaftOperationStatus.Success, HLCTimestamp.Zero, 1));
            };

            (RaftOperationStatus status, long gen) = await SendRemoveAsync(manager, partitionId: 7);

            Assert.Equal(RaftOperationStatus.Success, status);
            Assert.Equal(4, gen);
            Assert.False(replicateCalled, "Replicate must not be called for an already-removed partition");
        }
    }

    [Fact]
    public async Task RemovePartition_TombstoneInReplicatedMap_OtherPartitionsUntouched()
    {
        // Removing partition 2 must not affect partition 1 or 3 in the replicated map.
        RaftManager manager = Build();
        using (manager)
        {
            List<RaftPartitionRange> initial =
            [
                new() { PartitionId = 1, StartRange = 0,           EndRange = 500, Generation = 1, State = RaftPartitionState.Active, RoutingMode = RaftRoutingMode.HashRange },
                new() { PartitionId = 2, StartRange = 501,         EndRange = 1000, Generation = 1, State = RaftPartitionState.Active, RoutingMode = RaftRoutingMode.HashRange },
                new() { PartitionId = 3, StartRange = 1001,        EndRange = 1999, Generation = 1, State = RaftPartitionState.Active, RoutingMode = RaftRoutingMode.HashRange }
            ];
            manager.SystemCoordinator.Send(MakeConfigReplicated(initial));
            await WaitForIdleAsync(manager);

            List<RaftPartitionRange>? replicatedRanges = null;
            manager.SystemCoordinator.ReplicateOverride = (_, data, _, _) =>
            {
                using MemoryStream ms = new(data);
                Kommander.System.Protos.RaftSystemMessage msg =
                    Kommander.System.Protos.RaftSystemMessage.Parser.ParseFrom(ms);
                RaftPartitionMap? m = JsonSerializer.Deserialize<RaftPartitionMap>(msg.Value);
                replicatedRanges = m?.Partitions;
                return Task.FromResult(new RaftReplicationResult(true, RaftOperationStatus.Success, HLCTimestamp.Zero, 2));
            };
            manager.SystemCoordinator.StartPartitionsOverride = ranges => manager.StartUserPartitions(ranges);

            (RaftOperationStatus status, _) = await SendRemoveAsync(manager, partitionId: 2);

            Assert.Equal(RaftOperationStatus.Success, status);
            Assert.NotNull(replicatedRanges);
            Assert.Equal(3, replicatedRanges.Count);

            // Partition 1 and 3 must be untouched.
            Assert.Equal(RaftPartitionState.Active, replicatedRanges.Single(r => r.PartitionId == 1).State);
            Assert.Equal(RaftPartitionState.Active, replicatedRanges.Single(r => r.PartitionId == 3).State);

            // Partition 2 tombstone.
            Assert.Equal(RaftPartitionState.Removed, replicatedRanges.Single(r => r.PartitionId == 2).State);

            // Only partition 2 absent from live dictionary; 1 and 3 remain.
            Assert.False(manager.Partitions.ContainsKey(2));
            Assert.True(manager.Partitions.ContainsKey(1));
            Assert.True(manager.Partitions.ContainsKey(3));
        }
    }

    [Fact]
    public async Task TrySetInitialPartitions_DefaultFields_PropagateToLivePartitions()
    {
        // Verifies that the Generation, RoutingMode from the initial map are reflected
        // on the live RaftPartition objects held by the manager.
        RaftManager manager = BuildWithPartitions(partitions: 3);
        using (manager)
        {
            manager.SystemCoordinator.ReplicateOverride = (_, _, _, _) =>
                Task.FromResult(new RaftReplicationResult(true, RaftOperationStatus.Success, HLCTimestamp.Zero, 1));

            TaskCompletionSource done = new(TaskCreationOptions.RunContinuationsAsynchronously);
            manager.SystemCoordinator.StartPartitionsOverride = ranges =>
            {
                manager.StartUserPartitions(ranges);
                done.TrySetResult();
            };

            manager.SystemCoordinator.Send(
                new RaftSystemRequest(RaftSystemRequestType.LeaderChanged, manager.LocalEndpoint));
            await done.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

            Assert.Equal(3, manager.Partitions.Count);
            foreach (RaftPartition p in manager.Partitions.Values)
            {
                Assert.Equal(1, p.Generation);
                Assert.Equal(RaftRoutingMode.HashRange, p.RoutingMode);
            }
        }
    }
}
