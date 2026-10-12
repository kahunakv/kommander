using Grpc.Core;
using Kommander;
using Kommander.Communication;
using Kommander.Communication.Grpc;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL;
using Kommander.WAL.IO;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.Communication;

/// <summary>
/// One batch item whose dispatch throws must be contained: the items around it in the same batch
/// are still delivered with their payloads mapped intact, and the stream answers one response per
/// inbound message instead of faulting.
/// </summary>
/// <remarks>
/// The recording raft stub is deliberately not a <see cref="RaftManager"/>, so the manager-only
/// item types throw exactly where a misrouted or half-mapped item would in production — inside the
/// per-item loop, between two items that must still be processed. Before the per-item guard, the
/// first throw aborted the whole stream: the surviving items were lost and no response was
/// written, which is what these assertions pin against.
/// </remarks>
public sealed class TestRaftServiceBatchContainment
{
    [Fact]
    public async Task ThrowingItem_DoesNotAbortSiblingItemsOrTheResponse()
    {
        RecordingRaft raft = new();
        RaftService service = new(raft, NullLogger<IRaft>.Instance);

        GrpcBatchRequestsRequest first = new();
        first.Requests.Add(new GrpcBatchRequestsRequestItem
        {
            Type = GrpcBatchRequestsRequestType.StepDownNotice
            // No payload: dispatch throws before it reaches the state machine.
        });
        first.Requests.Add(new GrpcBatchRequestsRequestItem
        {
            Type = GrpcBatchRequestsRequestType.Vote,
            Vote = new GrpcVoteRequest
            {
                Partition = 3,
                Term = 11,
                MaxLogId = 900,
                LastLogTerm = 10,
                TimeNode = 1,
                TimePhysical = 1234,
                TimeCounter = 5,
                Endpoint = "node2:8005",
                PreVote = true
            }
        });

        GrpcBatchRequestsRequest second = new();
        second.Requests.Add(new GrpcBatchRequestsRequestItem
        {
            Type = GrpcBatchRequestsRequestType.TransferLeadership
            // No payload: throws again, proving the containment survived the first failure.
        });
        second.Requests.Add(new GrpcBatchRequestsRequestItem
        {
            Type = GrpcBatchRequestsRequestType.Handshake,
            Handshake = new GrpcHandshakeRequest
            {
                NodeId = 7,
                Partition = 4,
                MaxLogId = 42,
                Endpoint = "node3:8006"
            }
        });

        ListStreamReader reader = new([first, second]);
        RecordingResponseWriter writer = new();

        // Must complete without throwing despite the two failing items.
        await service.BatchRequests(reader, writer, CreateContext());

        VoteRequest vote = Assert.Single(raft.Votes);
        Assert.Equal(3, vote.Partition);
        Assert.Equal(11, vote.Term);
        Assert.Equal(900, vote.MaxLogId);
        Assert.Equal(10, vote.LastLogTerm);
        Assert.Equal(new HLCTimestamp(1, 1234, 5), vote.Time);
        Assert.Equal("node2:8005", vote.Endpoint);
        Assert.True(vote.PreVote);

        HandshakeRequest handshake = Assert.Single(raft.Handshakes);
        Assert.Equal(7, handshake.NodeId);
        Assert.Equal(4, handshake.Partition);
        Assert.Equal(42, handshake.MaxLogId);
        Assert.Equal("node3:8006", handshake.Endpoint);

        Assert.Equal(2, writer.WriteCount);
    }

    private static ServerCallContext CreateContext() => new FakeServerCallContext();

    /// <summary>
    /// Minimal <see cref="ServerCallContext"/>: <c>ValidateAuth</c> returns before touching the
    /// context when the raft instance is not a <see cref="RaftManager"/>, and the per-item error
    /// path reads <c>Peer</c> for its log line, so Peer must return a real value.
    /// </summary>
    private sealed class FakeServerCallContext : ServerCallContext
    {
        protected override string MethodCore => "/Rafter/BatchRequests";
        protected override string HostCore => "node2:8005";
        protected override string PeerCore => "ipv4:127.0.0.1:44100";
        protected override DateTime DeadlineCore => DateTime.MaxValue;
        protected override Metadata RequestHeadersCore { get; } = [];
        protected override CancellationToken CancellationTokenCore => CancellationToken.None;
        protected override Metadata ResponseTrailersCore { get; } = [];
        protected override Status StatusCore { get; set; }
        protected override WriteOptions? WriteOptionsCore { get; set; }
        protected override AuthContext AuthContextCore => new(null, new Dictionary<string, List<AuthProperty>>());
        protected override IDictionary<object, object> UserStateCore { get; } = new Dictionary<object, object>();

        protected override ContextPropagationToken CreatePropagationTokenCore(ContextPropagationOptions? options) =>
            throw new NotImplementedException();

        protected override Task WriteResponseHeadersAsyncCore(Metadata responseHeaders) =>
            throw new NotImplementedException();
    }

    /// <summary>Replays a fixed list of inbound batch messages through the duplex read API.</summary>
    private sealed class ListStreamReader(IReadOnlyList<GrpcBatchRequestsRequest> messages)
        : IAsyncStreamReader<GrpcBatchRequestsRequest>
    {
        private int index = -1;
        public GrpcBatchRequestsRequest Current => messages[index];

        public Task<bool> MoveNext(CancellationToken cancellationToken)
        {
            index++;
            return Task.FromResult(index < messages.Count);
        }
    }

    /// <summary>Counts the responses the server writes back on the duplex stream.</summary>
    private sealed class RecordingResponseWriter : IServerStreamWriter<GrpcBatchRequestsResponse>
    {
        public int WriteCount { get; private set; }
        public WriteOptions? WriteOptions { get; set; }

        public Task WriteAsync(GrpcBatchRequestsResponse message)
        {
            WriteCount++;
            return Task.CompletedTask;
        }
    }

    /// <summary>
    /// Minimal <see cref="IRaft"/> stub that records the two call types the test asserts on and
    /// throws for everything else. Deliberately not a <see cref="RaftManager"/>, which is what
    /// makes the manager-only batch items throw on dispatch.
    /// </summary>
    private sealed class RecordingRaft : IRaft
    {
        public List<VoteRequest> Votes { get; } = [];
        public List<HandshakeRequest> Handshakes { get; } = [];

        public void Vote(VoteRequest request) => Votes.Add(request);

        public Task Handshake(HandshakeRequest request)
        {
            Handshakes.Add(request);
            return Task.CompletedTask;
        }

        public void AppendLogs(AppendLogsRequest request) => throw new NotImplementedException();
        public void CompleteAppendLogs(CompleteAppendLogsRequest request) => throw new NotImplementedException();
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
        public event Func<int, string, Task<bool>>? OnLeaderChanged { add { } remove { } } public event Func<int, long, Task>? OnLeadershipLost { add { } remove { } }
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
        public void RequestVote(RequestVotesRequest request) => throw new NotImplementedException();
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
        public ValueTask<long?> GetFollowerLagAsync(int partitionId, string followerEndpoint) => throw new NotImplementedException();
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
        public long GetPartitionGeneration(int partitionId) => throw new NotImplementedException(); public long GetPartitionTerm(int partitionId) => throw new NotImplementedException();
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
}
