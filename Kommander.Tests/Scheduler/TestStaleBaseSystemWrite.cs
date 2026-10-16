using System.Text.Json;
using Kommander.Communication;
using Kommander.Communication.Memory;
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
/// A system write is never built from a system configuration that committed entries have moved
/// past.
///
/// <para>
/// Every map write carries the whole map, built from the coordinator's installed copy when the
/// handler started. In the Kahuna nightly of 2026-10-16 a <c>TryAddReplica</c> held the
/// single-reader coordinator loop for 75 s on a remote read-index call. Meanwhile the node lost P0
/// leadership, another leader committed eleven map entries, and the node won P0 back. The
/// placement requests queued during the wait then ran as leader on the old map and committed it,
/// and the eleven entries, still queued behind them, were refused as older than that commit: the
/// committed map went from generation 7 back to generation 5 on every node, and generations 6 and
/// 7 were reused for other replicas.
/// </para>
///
/// All tests use the coordinator-override harness (no real Raft quorum). The ranges are hosted on
/// remote endpoints only, so installing a map starts no local partition.
/// </summary>
public sealed class TestStaleBaseSystemWrite
{
    private static RaftManager Build(ICommunication? communication = null, TimeSpan? leadershipConfirmationTimeout = null)
    {
        RaftConfiguration configuration = new() { Host = "localhost", Port = 9000, InitialPartitions = 0 };
        if (leadershipConfirmationTimeout is { } timeout)
            configuration.LeadershipConfirmationTimeout = timeout;

        RaftManager manager = new(
            configuration,
            new StaticDiscovery([]),
            new InMemoryWAL(NullLogger<IRaft>.Instance),
            communication ?? new InMemoryCommunication(),
            new HybridLogicalClock(),
            NullLogger<IRaft>.Instance);

        ((FairReadScheduler)manager.ReadScheduler).Start();
        ((FairWalScheduler)manager.WalScheduler).Start();

        return manager;
    }

    private static byte[] SerializeMessage(string key, string value)
    {
        RaftSystemMessage msg = new() { Key = key, Value = value };
        using MemoryStream ms = new();
        msg.WriteTo(ms);
        return ms.ToArray();
    }

    private static RaftSystemRequest MakeConfigReplicated(List<RaftPartitionRange> ranges, long mapVersion, long logIndex) =>
        new(RaftSystemRequestType.ConfigReplicated,
            SerializeMessage(
                RaftSystemConfigKeys.Partitions,
                JsonSerializer.Serialize(new RaftPartitionMap { MapVersion = mapVersion, Partitions = ranges })))
        {
            LogIndex = logIndex
        };

    private static Task WaitForIdleAsync(RaftManager manager) =>
        manager.SystemCoordinator.DrainAsync().WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

    private static RaftReplica Voter(string endpoint) =>
        new() { Endpoint = endpoint, Role = RaftReplicaRole.Voter, SinceGeneration = 1 };

    /// <summary>Two hash ranges, each on its own remote replica set.</summary>
    private static List<RaftPartitionRange> Ranges(
        long generation1, string[] replicas1, long generation2, string[] replicas2) =>
    [
        new()
        {
            PartitionId = 1, StartRange = 0, EndRange = int.MaxValue / 2, Generation = generation1,
            State = RaftPartitionState.Active, RoutingMode = RaftRoutingMode.HashRange,
            Replicas = [.. replicas1.Select(Voter)]
        },
        new()
        {
            PartitionId = 2, StartRange = int.MaxValue / 2 + 1, EndRange = int.MaxValue, Generation = generation2,
            State = RaftPartitionState.Active, RoutingMode = RaftRoutingMode.HashRange,
            Replicas = [.. replicas2.Select(Voter)]
        }
    ];

    /// <summary>
    /// Routes replication back into the manager with no quorum. Each commit gets the next log
    /// index from <paramref name="firstLogIndex"/>, and the committed payloads are recorded.
    /// </summary>
    private static List<byte[]> OverrideReplication(RaftManager manager, long firstLogIndex)
    {
        List<byte[]> committed = [];
        long nextLogIndex = firstLogIndex;

        manager.SystemCoordinator.ReplicateOverride = (_, data, _, _) =>
        {
            lock (committed)
                committed.Add(data);

            return Task.FromResult(
                new RaftReplicationResult(true, RaftOperationStatus.Success, HLCTimestamp.Zero, nextLogIndex++));
        };

        return committed;
    }

    private static Task<(RaftOperationStatus Status, long Generation)> SendAddReplica(
        RaftManager manager, int partitionId, string endpoint)
    {
        TaskCompletionSource<(RaftOperationStatus Status, long Generation)> tcs =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        manager.SystemCoordinator.Send(new RaftSystemRequest(RaftSystemRequestType.AddReplica, partitionId, endpoint, 0, tcs));
        return tcs.Task;
    }

    private static RaftPartitionRange MapEntry(RaftManager manager, int partitionId) =>
        Assert.Single(manager.GetPartitionMap(), r => r.PartitionId == partitionId);

    private static RaftPartitionMap DecodeMap(byte[] payload)
    {
        RaftSystemMessage message = RaftSystemMessage.Parser.ParseFrom(payload);
        Assert.Equal(RaftSystemConfigKeys.Partitions, message.Key);
        return JsonSerializer.Deserialize(message.Value, SystemJsonContext.Default.RaftPartitionMap)!;
    }

    /// <summary>
    /// Holds the first membership-fence confirmation until <paramref name="release"/> completes,
    /// and reports when it has started waiting. Later confirmations pass at once.
    /// </summary>
    private static TaskCompletionSource HoldFirstConfirmation(RaftManager manager, Task<bool> release)
    {
        TaskCompletionSource waiting = new(TaskCreationOptions.RunContinuationsAsynchronously);
        int calls = 0;

        manager.SystemCoordinator.ConfirmPartitionLeadershipOverride = async (_, _) =>
        {
            if (Interlocked.Increment(ref calls) > 1)
                return true;

            waiting.TrySetResult();
            return await release.ConfigureAwait(false);
        };

        return waiting;
    }

    /// <summary>
    /// The production failure. A replica change holds the loop on its membership fence; a second
    /// one queues behind it, the way the placement passes did; another leader's committed entry is
    /// delivered while both wait. The held change must be refused, not committed over the entry,
    /// and the queued one must build on the entry. Before the fix the held change committed the
    /// pre-entry map at a higher index, the entry was then ignored as older, and partition 1 lost
    /// the other leader's replica.
    /// </summary>
    [Fact]
    public async Task HandlerHeldAcrossAnotherLeadersCommit_IsRefused_AndTheQueuedRequestBuildsOnTheCommit()
    {
        using RaftManager manager = Build();
        List<byte[]> committed = OverrideReplication(manager, firstLogIndex: 101);

        manager.SystemCoordinator.Send(MakeConfigReplicated(
            Ranges(5, ["b:1", "c:1", "d:1"], 5, ["b:1", "c:1", "d:1"]), mapVersion: 10, logIndex: 33));
        await WaitForIdleAsync(manager);

        TaskCompletionSource<bool> release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource waiting = HoldFirstConfirmation(manager, release.Task);

        Task<(RaftOperationStatus Status, long Generation)> held = SendAddReplica(manager, 1, "e:1");
        await waiting.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        Task<(RaftOperationStatus Status, long Generation)> queued = SendAddReplica(manager, 2, "e:1");

        // Another P0 leader committed this while the handler above was waiting.
        manager.SystemCoordinator.Send(MakeConfigReplicated(
            Ranges(7, ["b:1", "c:1", "d:1", "f:1"], 7, ["b:1", "c:1", "d:1", "g:1"]), mapVersion: 14, logIndex: 36));

        release.SetResult(true);

        (RaftOperationStatus heldStatus, _) = await held.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        Assert.Equal(RaftOperationStatus.StaleSystemConfiguration, heldStatus);

        (RaftOperationStatus queuedStatus, long queuedGeneration) = await queued.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        Assert.Equal(RaftOperationStatus.Success, queuedStatus);
        Assert.Equal(8, queuedGeneration);

        await WaitForIdleAsync(manager);

        // One proposal only, the queued one, built on entry 36.
        byte[] proposal = Assert.Single(committed);
        RaftPartitionMap proposed = DecodeMap(proposal);
        Assert.Contains(proposed.Partitions.Single(r => r.PartitionId == 1).Replicas, r => r.Endpoint == "f:1");
        Assert.Contains(proposed.Partitions.Single(r => r.PartitionId == 2).Replicas, r => r.Endpoint == "g:1");

        RaftPartitionRange range1 = MapEntry(manager, 1);
        Assert.Equal(7, range1.Generation);
        Assert.Contains(range1.Replicas, r => r.Endpoint == "f:1");
        Assert.DoesNotContain(range1.Replicas, r => r.Endpoint == "e:1");

        RaftPartitionRange range2 = MapEntry(manager, 2);
        Assert.Equal(8, range2.Generation);
        Assert.Contains(range2.Replicas, r => r.Endpoint == "g:1");
        Assert.Contains(range2.Replicas, r => r.Endpoint == "e:1" && r.Role == RaftReplicaRole.Learner);

        Assert.Equal(101, manager.SystemCoordinator.AppliedSystemLogIndexForTest);
    }

    /// <summary>
    /// A committed entry that arrives behind queued handler requests is installed before any of
    /// them runs. The first request holds the loop and then refuses on its own (its partition
    /// leadership is not confirmed), so it never reaches <c>Replicate</c>: the only place the
    /// entry can be installed is the queued request's dispatch. Before the fix the queued request
    /// ran on the old map, ahead of the entry.
    /// </summary>
    [Fact]
    public async Task DeliveryBehindQueuedRequests_IsInstalledAtTheirDispatch()
    {
        using RaftManager manager = Build();
        List<byte[]> committed = OverrideReplication(manager, firstLogIndex: 101);

        manager.SystemCoordinator.Send(MakeConfigReplicated(
            Ranges(5, ["b:1", "c:1", "d:1"], 5, ["b:1", "c:1", "d:1"]), mapVersion: 10, logIndex: 33));
        await WaitForIdleAsync(manager);

        TaskCompletionSource<bool> release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource waiting = HoldFirstConfirmation(manager, release.Task);

        Task<(RaftOperationStatus Status, long Generation)> held = SendAddReplica(manager, 2, "e:1");
        await waiting.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        // Queued before the delivery. Partition 1 already holds a transitional replica in the
        // delivered map, so a request that sees the map refuses the change; one that runs on the
        // old map would add a second mover.
        Task<(RaftOperationStatus Status, long Generation)> queued = SendAddReplica(manager, 1, "e:1");

        List<RaftPartitionRange> delivered = Ranges(6, ["b:1", "c:1", "d:1"], 5, ["b:1", "c:1", "d:1"]);
        delivered[0].Replicas.Add(new RaftReplica { Endpoint = "f:1", Role = RaftReplicaRole.Learner, SinceGeneration = 6 });
        manager.SystemCoordinator.Send(MakeConfigReplicated(delivered, mapVersion: 11, logIndex: 34));

        release.SetResult(false);

        Assert.Equal(RaftOperationStatus.LeadershipNotConfirmed, (await held.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken)).Status);
        Assert.Equal(RaftOperationStatus.ConcurrentMembershipChange, (await queued.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken)).Status);

        await WaitForIdleAsync(manager);
        Assert.Empty(committed);
        Assert.Equal(34, manager.SystemCoordinator.AppliedSystemLogIndexForTest);
        Assert.Contains(MapEntry(manager, 1).Replicas, r => r.Endpoint == "f:1" && r.Role == RaftReplicaRole.Learner);
    }

    /// <summary>
    /// The fence trips only on an install. A delivery at or below the installed index (the
    /// applicator's echo of this node's own commit, or a late older entry) is not installed, so a
    /// handler that waited across it still commits.
    /// </summary>
    [Fact]
    public async Task OlderDeliveryWhileHandlerWaits_DoesNotTripTheFence()
    {
        using RaftManager manager = Build();
        List<byte[]> committed = OverrideReplication(manager, firstLogIndex: 101);

        manager.SystemCoordinator.Send(MakeConfigReplicated(
            Ranges(5, ["b:1", "c:1", "d:1"], 5, ["b:1", "c:1", "d:1"]), mapVersion: 10, logIndex: 33));
        await WaitForIdleAsync(manager);

        TaskCompletionSource<bool> release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource waiting = HoldFirstConfirmation(manager, release.Task);

        Task<(RaftOperationStatus Status, long Generation)> held = SendAddReplica(manager, 1, "e:1");
        await waiting.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        manager.SystemCoordinator.Send(MakeConfigReplicated(
            Ranges(4, ["b:1", "c:1", "d:1"], 4, ["b:1", "c:1", "d:1"]), mapVersion: 9, logIndex: 30));

        release.SetResult(true);

        (RaftOperationStatus status, long generation) = await held.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        Assert.Equal(RaftOperationStatus.Success, status);
        Assert.Equal(6, generation);

        await WaitForIdleAsync(manager);
        Assert.Single(committed);
        Assert.Equal(101, manager.SystemCoordinator.AppliedSystemLogIndexForTest);
        Assert.Contains(MapEntry(manager, 1).Replicas, r => r.Endpoint == "e:1" && r.Role == RaftReplicaRole.Learner);
    }

    /// <summary>
    /// A refusal re-arms the fence: a request dispatched after the refused one, with no new
    /// delivery in between, commits normally.
    /// </summary>
    [Fact]
    public async Task AfterARefusal_TheNextRequestCommits()
    {
        using RaftManager manager = Build();
        List<byte[]> committed = OverrideReplication(manager, firstLogIndex: 101);

        manager.SystemCoordinator.Send(MakeConfigReplicated(
            Ranges(5, ["b:1", "c:1", "d:1"], 5, ["b:1", "c:1", "d:1"]), mapVersion: 10, logIndex: 33));
        await WaitForIdleAsync(manager);

        TaskCompletionSource<bool> release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource waiting = HoldFirstConfirmation(manager, release.Task);

        Task<(RaftOperationStatus Status, long Generation)> held = SendAddReplica(manager, 1, "e:1");
        await waiting.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        manager.SystemCoordinator.Send(MakeConfigReplicated(
            Ranges(6, ["b:1", "c:1", "d:1"], 5, ["b:1", "c:1", "d:1"]), mapVersion: 11, logIndex: 34));
        release.SetResult(true);

        Assert.Equal(RaftOperationStatus.StaleSystemConfiguration, (await held.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken)).Status);

        (RaftOperationStatus status, long generation) =
            await SendAddReplica(manager, 1, "e:1").WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        Assert.Equal(RaftOperationStatus.Success, status);
        Assert.Equal(7, generation);
        Assert.Single(committed);
    }

    /// <summary>
    /// <see cref="ICommunication.GetReadIndex"/> that never answers until it is cancelled, like a
    /// call on an HTTP/2 connection that died during a network partition.
    /// </summary>
    private sealed class UnansweredReadIndexCommunication : InMemoryCommunication, ICommunication
    {
        public int Calls;

        async Task<GetReadIndexResponse> ICommunication.GetReadIndex(
            RaftManager manager, RaftNode node, GetReadIndexRequest request, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref Calls);
            await Task.Delay(Timeout.Infinite, cancellationToken).ConfigureAwait(false);
            return new GetReadIndexResponse(false);
        }
    }

    /// <summary>
    /// The read-index leader resolution runs on the coordinator loop, so each remote call is
    /// bounded by <see cref="RaftConfiguration.RemoteProbeTimeout"/>: an unanswered candidate
    /// fails closed in seconds instead of holding the loop until the transport gives up.
    /// </summary>
    [Fact]
    public async Task UnansweredReadIndex_FailsClosedWithinTheProbeTimeout()
    {
        UnansweredReadIndexCommunication communication = new();
        using RaftManager manager = Build(communication, leadershipConfirmationTimeout: TimeSpan.FromMilliseconds(200));

        manager.SystemCoordinator.Send(MakeConfigReplicated(
            Ranges(5, ["b:1"], 5, ["c:1"]), mapVersion: 10, logIndex: 33));
        await WaitForIdleAsync(manager);

        TimeSpan bound = manager.Configuration.RemoteProbeTimeout;
        global::System.Diagnostics.Stopwatch watch = global::System.Diagnostics.Stopwatch.StartNew();

        string? leader = await manager.SystemCoordinator.ResolvePartitionLeaderAsync(1, TestContext.Current.CancellationToken)
            .WaitAsync(bound + TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);

        Assert.Null(leader);
        Assert.Equal(1, communication.Calls);
        Assert.True(watch.Elapsed >= bound - TimeSpan.FromMilliseconds(100), $"returned after {watch.Elapsed}, before the bound {bound}");
    }

    private static async Task WaitUntilAsync(Func<bool> condition, string what)
    {
        DateTime deadline = DateTime.UtcNow + TimeSpan.FromSeconds(5);
        while (!condition())
        {
            Assert.True(DateTime.UtcNow < deadline, $"timed out waiting until {what}");
            await Task.Delay(20, TestContext.Current.CancellationToken);
        }
    }

    /// <summary>
    /// A split's Phase 2 commit that meets a newer entry is refused, sends itself back through
    /// the channel, and completes from the installed map as a new dispatch, instead of leaving the
    /// split half done until the next P0 leadership change.
    /// </summary>
    [Fact]
    public async Task StaleSplitCommit_IsReEnqueued_AndCompletes()
    {
        using RaftManager manager = Build();
        List<byte[]> committed = OverrideReplication(manager, firstLogIndex: 101);
        manager.SystemCoordinator.StartPartitionsOverride = ranges => manager.StartUserPartitions(ranges);

        manager.SystemCoordinator.Send(MakeConfigReplicated(
        [
            new() { PartitionId = 1, StartRange = 0, EndRange = 999, Generation = 1, State = RaftPartitionState.Active, RoutingMode = RaftRoutingMode.HashRange }
        ], mapVersion: 1, logIndex: 5));
        await WaitForIdleAsync(manager);

        // Phase 2's first write meets a newer committed entry: the Phase 1 map, as another leader
        // would have re-asserted it at a higher index.
        int replicateCalls = 0;
        manager.SystemCoordinator.BeforeReplicateForTest = () =>
        {
            if (Interlocked.Increment(ref replicateCalls) == 2)
                manager.SystemCoordinator.Send(new RaftSystemRequest(RaftSystemRequestType.ConfigReplicated, committed[0]) { LogIndex = 150 });
        };

        manager.SystemCoordinator.Send(new RaftSystemRequest(
            1, new RaftSplitPlan { TargetRoutingMode = RaftRoutingMode.HashRange, AutoCommit = true }));

        await WaitUntilAsync(
            () => manager.GetPartitionMap().Count == 2 && manager.GetPartitionMap().All(r => r.State == RaftPartitionState.Active),
            "both halves of the split are Active");
        await WaitForIdleAsync(manager);

        Assert.Equal(3, Volatile.Read(ref replicateCalls));
        Assert.Equal(2, committed.Count);
        Assert.All(DecodeMap(committed[1]).Partitions, r => Assert.Equal(RaftPartitionState.Active, r.State));
    }

    /// <summary>
    /// A new P0 leader re-asserts its installed map. When a newer committed map is installed at
    /// that moment, the re-assert is refused and <c>TrySetInitialPartitions</c> starts over as a
    /// new dispatch: it re-asserts the newer map and never starts the older one.
    /// </summary>
    [Fact]
    public async Task StaleMapReassert_StartsOverFromTheInstalledMap()
    {
        using RaftManager manager = Build();
        List<byte[]> committed = OverrideReplication(manager, firstLogIndex: 101);

        manager.SystemCoordinator.Send(MakeConfigReplicated(
            Ranges(5, ["b:1", "c:1", "d:1"], 5, ["b:1", "c:1", "d:1"]), mapVersion: 10, logIndex: 33));
        await WaitForIdleAsync(manager);

        int replicateCalls = 0;
        manager.SystemCoordinator.BeforeReplicateForTest = () =>
        {
            if (Interlocked.Increment(ref replicateCalls) == 1)
                manager.SystemCoordinator.Send(MakeConfigReplicated(
                    Ranges(7, ["b:1", "c:1", "f:1"], 5, ["b:1", "c:1", "d:1"]), mapVersion: 12, logIndex: 36));
        };

        manager.SystemCoordinator.Send(new RaftSystemRequest(RaftSystemRequestType.LeaderChanged, manager.LocalEndpoint));

        await WaitUntilAsync(() => { lock (committed) return committed.Count >= 1; }, "the map is re-asserted");
        await WaitForIdleAsync(manager);

        RaftPartitionMap reasserted = DecodeMap(committed[0]);
        RaftPartitionRange reasserted1 = reasserted.Partitions.Single(r => r.PartitionId == 1);
        Assert.Equal(7, reasserted1.Generation);
        Assert.Contains(reasserted1.Replicas, r => r.Endpoint == "f:1");

        RaftPartitionRange range1 = MapEntry(manager, 1);
        Assert.Equal(7, range1.Generation);
        Assert.Contains(range1.Replicas, r => r.Endpoint == "f:1");
    }

    /// <summary>
    /// <see cref="ICommunication.GetReadIndex"/> where one endpoint never answers and another
    /// answers as the partition's leader.
    /// </summary>
    private sealed class OneLeaderOneSilentCommunication(string silent, string leader) : InMemoryCommunication, ICommunication
    {
        async Task<GetReadIndexResponse> ICommunication.GetReadIndex(
            RaftManager manager, RaftNode node, GetReadIndexRequest request, CancellationToken cancellationToken)
        {
            if (node.Endpoint == leader)
                return new GetReadIndexResponse(true, 42);

            if (node.Endpoint == silent)
                await Task.Delay(Timeout.Infinite, cancellationToken).ConfigureAwait(false);

            return new GetReadIndexResponse(false);
        }
    }

    /// <summary>
    /// Candidates are probed at once: a silent candidate does not delay the answer of the leader
    /// behind it, so one dead connection does not cost a full probe timeout per replica change.
    /// </summary>
    [Fact]
    public async Task SilentCandidate_DoesNotDelayTheLeadersAnswer()
    {
        OneLeaderOneSilentCommunication communication = new(silent: "b:1", leader: "c:1");
        using RaftManager manager = Build(communication);

        manager.SystemCoordinator.Send(MakeConfigReplicated(
            Ranges(5, ["b:1", "c:1"], 5, ["d:1"]), mapVersion: 10, logIndex: 33));
        await WaitForIdleAsync(manager);

        global::System.Diagnostics.Stopwatch watch = global::System.Diagnostics.Stopwatch.StartNew();
        string? leader = await manager.SystemCoordinator.ResolvePartitionLeaderAsync(1, TestContext.Current.CancellationToken)
            .WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);

        Assert.Equal("c:1", leader);
        Assert.True(watch.Elapsed < manager.Configuration.RemoteProbeTimeout, $"took {watch.Elapsed}");
    }
}
