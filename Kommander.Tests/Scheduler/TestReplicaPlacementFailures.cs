
using System.Text.Json;
using Kommander.Communication;
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
/// Failure-leg tests for the replica placement service: what the committed
/// replica-lifecycle mutations do when replication is refused, when the
/// leadership fence cannot confirm, and when a learner is not yet caught up.
///
/// All tests run without a real Raft cluster: replication is intercepted with
/// <see cref="RaftSystemCoordinator.ReplicateOverride"/> and maps/rosters are
/// injected through the coordinator channel, mirroring
/// <see cref="TestReplicaPlacement"/>.
/// </summary>
public sealed class TestReplicaPlacementFailures
{
    private const string Local = "localhost:9000";

    private static RaftManager Build(
        int replicationFactor = 0,
        Kommander.Communication.Memory.InMemoryCommunication? communication = null,
        TimeSpan? learnerPromotionStableWindow = null)
    {
        RaftConfiguration config = new()
        {
            Host = "localhost",
            Port = 9000,
            InitialPartitions = 0,
            ReplicationFactor = replicationFactor
        };
        if (learnerPromotionStableWindow is { } window)
            config.LearnerPromotionStableWindow = window;

        return new(
            config,
            new StaticDiscovery([]),
            new InMemoryWAL(NullLogger<IRaft>.Instance),
            communication ?? new Kommander.Communication.Memory.InMemoryCommunication(),
            new HybridLogicalClock(),
            NullLogger<IRaft>.Instance
        );
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

    private static RaftSystemRequest MakeMembersReplicated(params string[] voterEndpoints)
    {
        ClusterMembership membership = new()
        {
            MembershipVersion = 1,
            Members =
            [
                .. voterEndpoints.Select((endpoint, i) => new ClusterMember
                {
                    Endpoint = endpoint, NodeId = i + 1, Role = ClusterMemberRole.Voter, JoinedVersion = 1
                })
            ]
        };

        return new RaftSystemRequest(
            RaftSystemRequestType.ConfigReplicated,
            SerializeMessage(RaftSystemConfigKeys.Members, JsonSerializer.Serialize(membership)));
    }

    private static Task WaitForIdleAsync(RaftManager manager) =>
        manager.SystemCoordinator.DrainAsync().WaitAsync(TimeSpan.FromSeconds(5));

    /// <summary>
    /// Grants the membership-fence confirmation and accepts every map replication: these
    /// tests run without a Raft quorum on the data ranges.
    /// </summary>
    private static void AcceptReplication(RaftManager manager)
    {
        manager.SystemCoordinator.ConfirmPartitionLeadershipOverride = static (_, _) => Task.FromResult(true);
        manager.SystemCoordinator.ReplicateOverride = static (_, _, _, _) =>
            Task.FromResult(new RaftReplicationResult(true, RaftOperationStatus.Success, HLCTimestamp.Zero, 1));
    }

    /// <summary>
    /// Every replication attempt is refused with lost leadership — a fast abort with no retry.
    /// Returns a reader for the attempt count.
    /// </summary>
    private static Func<int> RefuseReplicationWithLostLeadership(RaftManager manager)
    {
        int calls = 0;
        manager.SystemCoordinator.ReplicateOverride = (_, _, _, _) =>
        {
            Interlocked.Increment(ref calls);
            return Task.FromResult(new RaftReplicationResult(false, RaftOperationStatus.NodeIsNotLeader, HLCTimestamp.Zero, 0));
        };
        return () => Volatile.Read(ref calls);
    }

    private static RaftReplica Replica(string endpoint, RaftReplicaRole role = RaftReplicaRole.Voter, long since = 1) =>
        new() { Endpoint = endpoint, Role = role, SinceGeneration = since };

    private static List<RaftPartitionRange> PlacedRange(
        int partitionId, long generation, params RaftReplica[] replicas) =>
    [
        new()
        {
            PartitionId = partitionId,
            StartRange = 0,
            EndRange = int.MaxValue,
            Generation = generation,
            State = RaftPartitionState.Active,
            RoutingMode = RaftRoutingMode.HashRange,
            Replicas = [.. replicas]
        }
    ];

    private static async Task<(RaftOperationStatus Status, long Generation)> SendReplicaChange(
        RaftManager manager, RaftSystemRequestType type, int partitionId, string endpoint)
    {
        TaskCompletionSource<(RaftOperationStatus Status, long Generation)> tcs =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        manager.SystemCoordinator.Send(new RaftSystemRequest(type, partitionId, endpoint, 0, tcs));
        return await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
    }

    private static RaftPartitionRange MapEntry(RaftManager manager, int partitionId) =>
        Assert.Single(manager.GetPartitionMap(), r => r.PartitionId == partitionId);

    /// <summary>
    /// Makes the harness manager pass <c>AmILeaderQuick(P0)</c> — the gate the placement pass
    /// self-checks. For data ranges the hook mirrors the real contract: it throws the typed
    /// <see cref="PartitionNotHostedException"/> for a range this node does not host.
    /// </summary>
    private static void ForceP0Leadership(RaftManager manager) =>
        manager._amILeaderQuickHookForTesting = partitionId =>
            partitionId == RaftSystemConfig.SystemPartition
                ? ValueTask.FromResult(true)
                : manager.HostsPartition(partitionId)
                    ? ValueTask.FromResult(false)
                    : throw new PartitionNotHostedException(partitionId);

    /// <summary>
    /// Drains the coordinator twice: the placement pass self-enqueues its planned replica
    /// mutations behind the first drain sentinel, so a second drain is needed before the
    /// committed map reflects them.
    /// </summary>
    private static async Task RunEnqueuedPassToCompletionAsync(RaftManager manager)
    {
        await WaitForIdleAsync(manager);
        await WaitForIdleAsync(manager);
    }

    /// <summary>
    /// Seeds the gossip-reduced leader hint for a range this node does not host, by ingesting a
    /// load report in which <paramref name="leaderEndpoint"/> claims <paramref name="partitionId"/>.
    /// </summary>
    private static async Task SeedLeaderHintAsync(RaftManager manager, string leaderEndpoint, int partitionId)
    {
        manager.SystemCoordinator.Send(new RaftSystemRequest(new NodeLoadReport
        {
            Endpoint = leaderEndpoint,
            ReportVersion = 1,
            Time = manager.HybridLogicalClock.SendOrLocalEvent(0),
            Leaderships = [new PartitionLoad { PartitionId = partitionId }]
        }));
        await WaitForIdleAsync(manager);
    }

    // ── AddReplica: refused map replication ────────────────────────────────────

    [Fact]
    public async Task AddReplica_MapReplicationRefused_LearnerNotCommittedAndRetrySucceeds()
    {
        RaftManager manager = Build();
        using (manager)
        {
            AcceptReplication(manager);
            manager.SystemCoordinator.Send(MakeConfigReplicated(
                PlacedRange(1, 5, Replica(Local), Replica("b:1"), Replica("c:1"))));
            await WaitForIdleAsync(manager);

            Func<int> replicationAttempts = RefuseReplicationWithLostLeadership(manager);

            (RaftOperationStatus status, long generation) =
                await SendReplicaChange(manager, RaftSystemRequestType.AddReplica, 1, "d:1");

            // The refusal surfaces to the caller and nothing was committed: the map
            // is exactly as it was, with no learner half-added.
            Assert.Equal(RaftOperationStatus.NodeIsNotLeader, status);
            Assert.Equal(0, generation); // a failed commit resolves with no generation
            Assert.Equal(1, replicationAttempts()); // fast abort, no retry storm
            RaftPartitionRange unchanged = MapEntry(manager, 1);
            Assert.Equal(5, unchanged.Generation);
            Assert.DoesNotContain(unchanged.Replicas, r => r.Endpoint == "d:1");

            // A later attempt on a healthy leadership commits the learner.
            manager.SystemCoordinator.ReplicateOverride = static (_, _, _, _) =>
                Task.FromResult(new RaftReplicationResult(true, RaftOperationStatus.Success, HLCTimestamp.Zero, 1));

            (RaftOperationStatus retryStatus, long retryGeneration) =
                await SendReplicaChange(manager, RaftSystemRequestType.AddReplica, 1, "d:1");

            Assert.Equal(RaftOperationStatus.Success, retryStatus);
            Assert.Equal(6, retryGeneration);
            RaftReplica learner = Assert.Single(MapEntry(manager, 1).Replicas, r => r.Endpoint == "d:1");
            Assert.Equal(RaftReplicaRole.Learner, learner.Role);
            Assert.Equal(6, learner.SinceGeneration);
        }
    }

    // ── RemoveReplica: refused final drop is re-driven by a later pass ─────────

    [Fact]
    public async Task RemoveReplica_FinalDropRefused_InterruptedRemovalSurvivesAndPassRetries()
    {
        RaftManager manager = Build();
        using (manager)
        {
            AcceptReplication(manager);
            // A crash shape between the two removal commits: the replica already left
            // the quorum denominator at the first commit.
            manager.SystemCoordinator.Send(MakeConfigReplicated(
                PlacedRange(1, 2, Replica(Local), Replica("b:1"), Replica("c:1", RaftReplicaRole.Removing))));
            await WaitForIdleAsync(manager);

            Func<int> replicationAttempts = RefuseReplicationWithLostLeadership(manager);

            (RaftOperationStatus status, long generation) =
                await SendReplicaChange(manager, RaftSystemRequestType.RemoveReplica, 1, "c:1");

            // The final drop was refused; the Removing replica must survive so the
            // removal can be retried rather than silently abandoned.
            Assert.Equal(RaftOperationStatus.NodeIsNotLeader, status);
            Assert.Equal(0, generation); // a failed commit resolves with no generation
            Assert.Equal(1, replicationAttempts());
            RaftReplica stillRemoving = Assert.Single(MapEntry(manager, 1).Replicas, r => r.Endpoint == "c:1");
            Assert.Equal(RaftReplicaRole.Removing, stillRemoving.Role);

            // The next placement pass re-drives the interrupted removal and, with
            // replication healthy again, finishes the drop.
            manager.SystemCoordinator.ReplicateOverride = static (_, _, _, _) =>
                Task.FromResult(new RaftReplicationResult(true, RaftOperationStatus.Success, HLCTimestamp.Zero, 1));
            ForceP0Leadership(manager);

            manager.SystemCoordinator.Send(new RaftSystemRequest(RaftSystemRequestType.RunPlacementPass));
            await RunEnqueuedPassToCompletionAsync(manager);

            Assert.DoesNotContain(MapEntry(manager, 1).Replicas, r => r.Endpoint == "c:1");
            Assert.Equal(3, MapEntry(manager, 1).Generation);
        }
    }

    // ── PromoteReplica: only a Learner can be promoted ────────────────────────

    [Fact]
    public async Task PromoteReplica_ReplicaAlreadyRemoving_IsRejected()
    {
        RaftManager manager = Build();
        using (manager)
        {
            AcceptReplication(manager);
            manager.SystemCoordinator.Send(MakeConfigReplicated(
                PlacedRange(1, 2, Replica(Local), Replica("b:1"), Replica("c:1", RaftReplicaRole.Removing))));
            await WaitForIdleAsync(manager);

            bool replicateCalled = false;
            manager.SystemCoordinator.ReplicateOverride = (_, _, _, _) =>
            {
                replicateCalled = true;
                return Task.FromResult(new RaftReplicationResult(true, RaftOperationStatus.Success, HLCTimestamp.Zero, 1));
            };

            (RaftOperationStatus status, _) =
                await SendReplicaChange(manager, RaftSystemRequestType.PromoteReplica, 1, "c:1");

            // Promoting a replica that is on its way OUT would re-admit a node the
            // removal already discounted from quorum.
            Assert.Equal(RaftOperationStatus.Errored, status);
            Assert.False(replicateCalled);
            Assert.Equal(
                RaftReplicaRole.Removing,
                Assert.Single(MapEntry(manager, 1).Replicas, r => r.Endpoint == "c:1").Role);
        }
    }

    // ── Membership fence: the probe itself throws ──────────────────────────────

    [Fact]
    public async Task AddReplica_LeadershipConfirmationThrows_RefusedAsNotConfirmed()
    {
        RaftManager manager = Build();
        using (manager)
        {
            AcceptReplication(manager);
            manager.SystemCoordinator.Send(MakeConfigReplicated(
                PlacedRange(1, 5, Replica(Local), Replica("b:1"), Replica("c:1"))));
            await WaitForIdleAsync(manager);

            bool replicateCalled = false;
            manager.SystemCoordinator.ReplicateOverride = (_, _, _, _) =>
            {
                replicateCalled = true;
                return Task.FromResult(new RaftReplicationResult(true, RaftOperationStatus.Success, HLCTimestamp.Zero, 1));
            };

            // A transport-level failure inside the leadership probe must fail closed,
            // not be mistaken for a confirmation.
            manager.SystemCoordinator.ConfirmPartitionLeadershipOverride = static (_, _) =>
                throw new InvalidOperationException("simulated probe transport failure");

            (RaftOperationStatus status, long generation) =
                await SendReplicaChange(manager, RaftSystemRequestType.AddReplica, 1, "d:1");

            Assert.Equal(RaftOperationStatus.LeadershipNotConfirmed, status);
            Assert.Equal(5, generation);
            Assert.False(replicateCalled, "An unconfirmable leadership must block the commit, not just log");
            Assert.DoesNotContain(MapEntry(manager, 1).Replicas, r => r.Endpoint == "d:1");
        }
    }

    // ── SetReplicationFactor: invalid and no-op values ─────────────────────────

    [Fact]
    public async Task SetReplicationFactor_NegativeValue_IsRejectedWithoutReplication()
    {
        RaftManager manager = Build(replicationFactor: 3);
        using (manager)
        {
            AcceptReplication(manager);
            manager.SystemCoordinator.Send(MakeConfigReplicated(
                PlacedRange(1, 4, Replica(Local), Replica("b:1"), Replica("c:1"))));
            await WaitForIdleAsync(manager);

            bool replicateCalled = false;
            manager.SystemCoordinator.ReplicateOverride = (_, _, _, _) =>
            {
                replicateCalled = true;
                return Task.FromResult(new RaftReplicationResult(true, RaftOperationStatus.Success, HLCTimestamp.Zero, 1));
            };

            TaskCompletionSource<(RaftOperationStatus Status, long Generation)> tcs =
                new(TaskCreationOptions.RunContinuationsAsynchronously);
            manager.SystemCoordinator.Send(new RaftSystemRequest(RaftSystemRequestType.SetReplicationFactor, 1)
            {
                ReplicationFactorValue = -2,
                Completion = tcs
            });
            (RaftOperationStatus status, long generation) =
                await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

            Assert.Equal(RaftOperationStatus.Errored, status);
            Assert.Equal(0, generation);
            Assert.False(replicateCalled);
            Assert.Equal(3, manager.GetEffectiveReplicationFactor(1)); // global default unchanged
        }
    }

    [Fact]
    public async Task SetReplicationFactor_ValueAlreadySet_IsIdempotentNoOp()
    {
        RaftManager manager = Build(replicationFactor: 3);
        using (manager)
        {
            AcceptReplication(manager);
            manager.SystemCoordinator.Send(MakeConfigReplicated(
                PlacedRange(1, 4, Replica(Local), Replica("b:1"), Replica("c:1"))));
            await WaitForIdleAsync(manager);

            bool replicateCalled = false;
            manager.SystemCoordinator.ReplicateOverride = (_, _, _, _) =>
            {
                replicateCalled = true;
                return Task.FromResult(new RaftReplicationResult(true, RaftOperationStatus.Success, HLCTimestamp.Zero, 1));
            };

            // The range has no override (0 = inherit); setting 0 again is a retry of a
            // change that already took effect and must not commit a second time.
            TaskCompletionSource<(RaftOperationStatus Status, long Generation)> tcs =
                new(TaskCreationOptions.RunContinuationsAsynchronously);
            manager.SystemCoordinator.Send(new RaftSystemRequest(RaftSystemRequestType.SetReplicationFactor, 1)
            {
                ReplicationFactorValue = 0,
                Completion = tcs
            });
            (RaftOperationStatus status, long generation) =
                await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

            Assert.Equal(RaftOperationStatus.Success, status);
            Assert.Equal(4, generation);
            Assert.False(replicateCalled, "A no-op RF change must not replicate a map or kick a pass");
        }
    }

    // ── Placement pass: learner promotion refused while not caught up ─────────

    /// <summary>
    /// Minimal remote node for the in-memory transport: answers only the follower-lag probe
    /// (<see cref="ICommunication"/> routes <c>GetRemoteFollowerLag</c> through
    /// <see cref="IRaft.GetFollowerLagAsync"/> on the target) and throws for everything else.
    /// </summary>
    private sealed class RemoteLagRaft : IRaft
    {
        public required Func<int, string, long?> Lag { get; init; }

        public ValueTask<long?> GetFollowerLagAsync(int partitionId, string followerEndpoint) =>
            ValueTask.FromResult(Lag(partitionId, followerEndpoint));

        public ValueTask<bool> ConfirmLeadershipAsync(int partitionId, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public ValueTask<bool> ConfirmLocalApplicationAsync(int partitionId, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public bool Joined => throw new NotImplementedException();
        public IWAL WalAdapter => throw new NotImplementedException();
        public ICommunication Communication => throw new NotImplementedException();
        public IDiscovery Discovery => throw new NotImplementedException();
        public RaftConfiguration Configuration => throw new NotImplementedException();
        public HybridLogicalClock HybridLogicalClock => throw new NotImplementedException();
        public IRaftReadScheduler ReadScheduler => throw new NotImplementedException();
        public IRaftWalScheduler WalScheduler => throw new NotImplementedException();
        public bool IsInitialized => throw new NotImplementedException();
        public ClusterMemberRole LocalRole => throw new NotImplementedException();

        public event Action<int>? OnRestoreStarted { add { } remove { } }
        public event Action<int>? OnRestoreFinished { add { } remove { } }
        public event Action<int, RaftLog>? OnReplicationError { add { } remove { } }
        public event Func<int, RaftLog, Task<bool>>? OnLogRestored { add { } remove { } }
        public event Func<int, RaftLog, Task<bool>>? OnReplicationReceived { add { } remove { } }
        public event Func<int, string, Task<bool>>? OnLeaderChanged { add { } remove { } }
        public event Func<int, long, Task>? OnLeadershipLost { add { } remove { } }
        public event Action<IReadOnlyList<RaftPartitionRange>>? OnPartitionMapChanged { add { } remove { } }
        public event Action<ClusterMembership>? OnMembershipChanged { add { } remove { } }

        public Task JoinCluster(CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task JoinCluster(IEnumerable<string> seeds, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task LeaveCluster(bool dispose = false, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<LeaveClusterResult> RequestLeaveAsync(CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task UpdateNodes() => throw new NotImplementedException();
        public ClusterMembership GetMembership() => throw new NotImplementedException();
        public IList<RaftNode> GetNodes() => throw new NotImplementedException();
        public HLCTimestamp GetLastNodeActivity(string endpoint) => throw new NotImplementedException();
        public IReadOnlyList<string> GetActiveNodes(TimeSpan within) => throw new NotImplementedException();
        public Task Handshake(HandshakeRequest request) => throw new NotImplementedException();
        public void RequestVote(RequestVotesRequest request) => throw new NotImplementedException();
        public void Vote(VoteRequest request) => throw new NotImplementedException();
        public void AppendLogs(AppendLogsRequest request) => throw new NotImplementedException();
        public void CompleteAppendLogs(CompleteAppendLogsRequest request) => throw new NotImplementedException();
        public Task<RaftReplicationResult> ReplicateLogs(int partitionId, string type, byte[] data, bool autoCommit = true, long expectedGeneration = 0, long expectedTerm = 0, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<RaftReplicationResult> ReplicateLogs(int partitionId, string type, IEnumerable<byte[]> logs, bool autoCommit = true, long expectedGeneration = 0, long expectedTerm = 0, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<RaftBatchReplicationResult> ReplicateEntries(int partitionId, IReadOnlyList<RaftProposalEntry> entries, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<RaftReplicationResult> ReplicateCheckpoint(int partitionId, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<(bool success, RaftOperationStatus status, long commitLogId)> CommitLogs(int partitionId, HLCTimestamp ticketId, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<(bool success, RaftOperationStatus status, long commitLogId)> RollbackLogs(int partitionId, HLCTimestamp ticketId, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public void SetMinRetainIndex(int partitionId, long index) => throw new NotImplementedException();
        public long GetCommitIndex(int partitionId) => throw new NotImplementedException();
        public long GetStaleProposedSkippedCount(int partitionId) => throw new NotImplementedException();
        public IDisposable AcquireRetentionHold(int partitionId, long index) => throw new NotImplementedException();
        public string GetLocalEndpoint() => throw new NotImplementedException();
        public int GetLocalNodeId() => throw new NotImplementedException();
        public string GetLocalNodeName() => throw new NotImplementedException();
        public ValueTask<bool> AmILeaderQuick(int partitionId) => throw new NotImplementedException();
        public ValueTask<bool> AmILeader(int partitionId, CancellationToken cancellationToken) => throw new NotImplementedException();
        public ValueTask<string> WaitForLeader(int partitionId, CancellationToken cancellationToken) => throw new NotImplementedException();
        public ValueTask<string> WaitForLeaderStableAsync(int partitionId, TimeSpan minStableFor, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public ValueTask<string> WaitForLeaderStableAsync(int partitionId, TimeSpan minStableFor, TimeSpan timeout, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<RaftOperationStatus> ForceLeaderForTestingAsync(int partitionId, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<RaftOperationStatus> StepDownAsync(int partitionId, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<RaftOperationStatus> TransferLeadershipAsync(int partitionId, string targetEndpoint, CancellationToken cancellationToken = default) => throw new NotImplementedException();

        public Task<RaftOperationStatus> RequestReseedAsync(int partitionId, CancellationToken cancellationToken = default) => throw new NotImplementedException();

        public RaftOperationStatus SetCandidacyWithheld(int partitionId, bool withheld) => throw new NotImplementedException();

        public bool IsCandidacyWithheld(int partitionId) => throw new NotImplementedException();
        public Task<RaftOperationStatus> SuspendHeartbeatsAsync(int partitionId, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<RaftOperationStatus> ResumeHeartbeatsAsync(int partitionId, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public IDisposable HoldCommittedProposalRepliesForTesting(int partitionId, Action<HeldProposalReply> onHeld) => throw new NotImplementedException();
        public IDisposable SetSnapshotInstallGateForTesting(int partitionId, SnapshotInstallPhase phase, Func<SnapshotInstallSignal, ValueTask> gate) => throw new NotImplementedException();
        public Task<RaftOperationStatus> HoldConsumerAppliesForTesting(int partitionId, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<RaftOperationStatus> ResumeConsumerAppliesForTesting(int partitionId, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<RaftPartitionLifecycleResult> CreatePartitionAsync(int partitionId, RaftRoutingMode mode = RaftRoutingMode.Unrouted, (int start, int end)? hashRange = null, CancellationToken ct = default) => throw new NotImplementedException();
        public Task<RaftPartitionLifecycleResult> RemovePartitionAsync(int partitionId, CancellationToken ct = default) => throw new NotImplementedException();
        public Task<RaftPartitionLifecycleResult> SplitPartitionAsync(int sourcePartitionId, int targetPartitionId = 0, RaftSplitPlan? plan = null, CancellationToken ct = default) => throw new NotImplementedException();
        public Task<RaftPartitionLifecycleResult> MergePartitionsAsync(int survivorPartitionId, int sourcePartitionId, RaftMergePlan? plan = null, CancellationToken ct = default) => throw new NotImplementedException();
        public long GetPartitionGeneration(int partitionId) => throw new NotImplementedException();
        public long GetPartitionTerm(int partitionId) => throw new NotImplementedException();
        public bool HostsPartition(int partitionId) => throw new NotImplementedException();
        public IReadOnlyList<RaftReplica> GetPartitionReplicas(int partitionId) => throw new NotImplementedException();
        public string? GetPartitionLeaderHint(int partitionId) => throw new NotImplementedException();
        public int GetEffectiveReplicationFactor(int partitionId) => throw new NotImplementedException();
        public Task<RaftPartitionLifecycleResult> SetReplicationFactorAsync(int partitionId, int replicationFactor, CancellationToken ct = default) => throw new NotImplementedException();
        public double GetPartitionLogOpsPerSecond(int partitionId) => throw new NotImplementedException();
        public int GetPartitionWalQueueDepth(int partitionId) => throw new NotImplementedException();
        public double GetPartitionCommitWaitMs(int partitionId) => throw new NotImplementedException();
        public IReadOnlyList<RaftPartitionRange> GetPartitionMap() => throw new NotImplementedException();
        public int GetPartitionKey(string partitionKey) => throw new NotImplementedException();
        public int GetPrefixPartitionKey(string prefixPartitionKey) => throw new NotImplementedException();
        public void RegisterStateMachineTransfer(IRaftStateMachineTransfer? transfer) => throw new NotImplementedException();
        public void RegisterSystemStateTransfer(IRaftSystemStateTransfer? transfer) => throw new NotImplementedException();
        public void RegisterPartitionStateTransfer(IRaftPartitionStateTransfer? transfer) => throw new NotImplementedException();
        public IReadOnlyList<RaftSnapshotStatus> GetSnapshotStatuses(int partitionId) => throw new NotImplementedException();

        public IReadOnlyList<RaftBackfillStatus> GetBackfillStatuses(int partitionId) => throw new NotImplementedException();
    }

    [Fact]
    public async Task RunPlacementPass_LearnerLaggingBehindLeader_IsNotPromoted()
    {
        Kommander.Communication.Memory.InMemoryCommunication communication = new();
        // The range leader reports itself at index 1000 and the learner at 10: a lag
        // of 990 entries, far beyond the promotion threshold.
        RaftManager manager = Build(
            replicationFactor: 3, communication: communication,
            learnerPromotionStableWindow: TimeSpan.Zero);
        using (manager)
        {
            communication.SetNodes(new Dictionary<string, IRaft>
            {
                ["b:1"] = new RemoteLagRaft { Lag = (_, follower) => follower == "b:1" ? 1000 : 10 }
            });

            AcceptReplication(manager);
            manager.SystemCoordinator.Send(MakeMembersReplicated(Local, "b:1", "c:1"));
            manager.SystemCoordinator.Send(MakeConfigReplicated(
                PlacedRange(1, 2, Replica("b:1"), Replica("c:1", RaftReplicaRole.Learner))));
            await WaitForIdleAsync(manager);
            Assert.False(manager.HostsPartition(1));

            await SeedLeaderHintAsync(manager, "b:1", 1);
            ForceP0Leadership(manager);

            // Several passes (a zero stable window would promote instantly if the
            // learner were caught up) must all leave the learner outside the quorum.
            for (int pass = 0; pass < 3; pass++)
            {
                manager.SystemCoordinator.Send(new RaftSystemRequest(RaftSystemRequestType.RunPlacementPass));
                await RunEnqueuedPassToCompletionAsync(manager);
            }

            RaftReplica learner = Assert.Single(MapEntry(manager, 1).Replicas, r => r.Endpoint == "c:1");
            Assert.Equal(RaftReplicaRole.Learner, learner.Role);
            Assert.False(manager.IsPartitionVoter(1, "c:1"));
        }
    }

    [Fact]
    public async Task RunPlacementPass_ConfirmedLeaderUnreachable_LearnerStaysUnpromoted()
    {
        // The gossiped hint is empty and the read-index round names a leader that
        // then fails the lag probe: no usable measurement exists, so promotion must
        // be refused rather than guessed.
        Kommander.Communication.Memory.InMemoryCommunication communication = new();
        RaftManager manager = Build(
            replicationFactor: 3, communication: communication,
            learnerPromotionStableWindow: TimeSpan.Zero);
        using (manager)
        {
            // Nothing is registered on the transport: every probe answer is null.
            manager.SystemCoordinator.ResolvePartitionLeaderOverride = static (_, _) => Task.FromResult<string?>("b:1");

            AcceptReplication(manager);
            manager.SystemCoordinator.Send(MakeMembersReplicated(Local, "b:1", "c:1"));
            manager.SystemCoordinator.Send(MakeConfigReplicated(
                PlacedRange(1, 2, Replica("b:1"), Replica("c:1", RaftReplicaRole.Learner))));
            await WaitForIdleAsync(manager);
            Assert.Null(manager.GetPartitionLeaderHint(1));

            ForceP0Leadership(manager);

            manager.SystemCoordinator.Send(new RaftSystemRequest(RaftSystemRequestType.RunPlacementPass));
            await RunEnqueuedPassToCompletionAsync(manager);
            manager.SystemCoordinator.Send(new RaftSystemRequest(RaftSystemRequestType.RunPlacementPass));
            await RunEnqueuedPassToCompletionAsync(manager);

            RaftReplica learner = Assert.Single(MapEntry(manager, 1).Replicas, r => r.Endpoint == "c:1");
            Assert.Equal(RaftReplicaRole.Learner, learner.Role);
        }
    }
}
