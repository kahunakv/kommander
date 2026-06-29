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
/// Covers the response cadence of <see cref="RaftService.BatchRequests"/>: it writes exactly one response per
/// inbound batch message, not one per item. The client drains and discards these responses (real
/// acknowledgement is the protocol-level <c>CompleteAppendLogs</c>), so collapsing per-item frames
/// to one-per-batch removes reverse-direction HTTP/2 traffic on the hot path while still feeding the
/// client's fault-detection drain loop. The test also asserts every item is still processed.
/// </summary>
public sealed class TestBatchRequestsResponseCadence
{
    [Theory]
    [InlineData(1, 1)]
    [InlineData(1, 64)]
    [InlineData(4, 8)]
    [InlineData(3, 0)]   // empty batches still emit exactly one response each
    public async Task BatchRequests_WritesOneResponsePerInboundBatch(int batches, int itemsPerBatch)
    {
        CountingRaft raft = new();
        RaftService service = new(raft, NullLogger<IRaft>.Instance);

        List<GrpcBatchRequestsRequest> inbound = [];
        for (int b = 0; b < batches; b++)
        {
            GrpcBatchRequestsRequest message = new();
            for (int i = 0; i < itemsPerBatch; i++)
            {
                // Alternate the two hot-path fire-and-forget message types.
                message.Requests.Add((b + i) % 2 == 0
                    ? new GrpcBatchRequestsRequestItem
                    {
                        Type = GrpcBatchRequestsRequestType.AppendLogs,
                        AppendLogs = new GrpcAppendLogsRequest { Partition = 0, Endpoint = "node:1" }
                    }
                    : new GrpcBatchRequestsRequestItem
                    {
                        Type = GrpcBatchRequestsRequestType.CompleteAppendLogs,
                        CompleteAppendLogs = new GrpcCompleteAppendLogsRequest { Partition = 0, Endpoint = "node:1" }
                    });
            }
            inbound.Add(message);
        }

        ListStreamReader reader = new(inbound);
        CountingResponseWriter writer = new();

        await service.BatchRequests(reader, writer, CreateContext());

        // One response per inbound batch — independent of how many items each batch carried.
        Assert.Equal(batches, writer.WriteCount);
        // Every item was still dispatched to the state machine.
        Assert.Equal(batches * itemsPerBatch, raft.AppendLogsCount + raft.CompleteAppendLogsCount);
    }

    private static ServerCallContext CreateContext() => new FakeServerCallContext();

    /// <summary>
    /// Minimal <see cref="ServerCallContext"/>: <c>RaftService.ValidateAuth</c> returns before
    /// touching the context when the raft instance is not a <see cref="RaftManager"/>, and
    /// <c>BatchRequests</c> never reads the context otherwise, so every member can throw.
    /// </summary>
    private sealed class FakeServerCallContext : ServerCallContext
    {
        protected override string MethodCore => throw new NotImplementedException();
        protected override string HostCore => throw new NotImplementedException();
        protected override string PeerCore => throw new NotImplementedException();
        protected override DateTime DeadlineCore => throw new NotImplementedException();
        protected override Metadata RequestHeadersCore => throw new NotImplementedException();
        protected override CancellationToken CancellationTokenCore => throw new NotImplementedException();
        protected override Metadata ResponseTrailersCore => throw new NotImplementedException();
        protected override Status StatusCore { get; set; }
        protected override WriteOptions? WriteOptionsCore { get; set; }
        protected override AuthContext AuthContextCore => throw new NotImplementedException();
        protected override ContextPropagationToken CreatePropagationTokenCore(ContextPropagationOptions? options) => throw new NotImplementedException();
        protected override Task WriteResponseHeadersAsyncCore(Metadata responseHeaders) => throw new NotImplementedException();
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

    /// <summary>Counts how many responses the server writes back on the duplex stream.</summary>
    private sealed class CountingResponseWriter : IServerStreamWriter<GrpcBatchRequestsResponse>
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
    /// Minimal <see cref="IRaft"/> stub: counts the two fire-and-forget hot-path calls the test
    /// drives and throws for everything else. It is deliberately not a <see cref="RaftManager"/>,
    /// so <c>RaftService.ValidateAuth</c> short-circuits and no transport security setup is needed.
    /// </summary>
    private sealed class CountingRaft : IRaft
    {
        public int AppendLogsCount { get; private set; }
        public int CompleteAppendLogsCount { get; private set; }

        public void AppendLogs(AppendLogsRequest request) => AppendLogsCount++;
        public void CompleteAppendLogs(CompleteAppendLogsRequest request) => CompleteAppendLogsCount++;

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
        public event Action<IReadOnlyList<RaftPartitionRange>>? OnPartitionMapChanged { add { } remove { } }
        public event Action<ClusterMembership>? OnMembershipChanged { add { } remove { } }

        public Task JoinCluster(CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task JoinCluster(IEnumerable<string> seeds, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task LeaveCluster(bool dispose = false, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task UpdateNodes() => throw new NotImplementedException();
        public ClusterMembership GetMembership() => throw new NotImplementedException();
        public IList<RaftNode> GetNodes() => throw new NotImplementedException();
        public HLCTimestamp GetLastNodeActivity(string endpoint) => throw new NotImplementedException();
        public IReadOnlyList<string> GetActiveNodes(TimeSpan within) => throw new NotImplementedException();
        public Task Handshake(HandshakeRequest request) => throw new NotImplementedException();
        public void RequestVote(RequestVotesRequest request) => throw new NotImplementedException();
        public void Vote(VoteRequest request) => throw new NotImplementedException();
        public Task<RaftReplicationResult> ReplicateLogs(int partitionId, string type, byte[] data, bool autoCommit = true, long expectedGeneration = 0, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<RaftReplicationResult> ReplicateLogs(int partitionId, string type, IEnumerable<byte[]> logs, bool autoCommit = true, long expectedGeneration = 0, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<RaftReplicationResult> ReplicateCheckpoint(int partitionId, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<(bool success, RaftOperationStatus status, long commitLogId)> CommitLogs(int partitionId, HLCTimestamp ticketId) => throw new NotImplementedException();
        public Task<(bool success, RaftOperationStatus status, long commitLogId)> RollbackLogs(int partitionId, HLCTimestamp ticketId) => throw new NotImplementedException();
        public void SetMinRetainIndex(int partitionId, long index) => throw new NotImplementedException();
        public string GetLocalEndpoint() => throw new NotImplementedException();
        public int GetLocalNodeId() => throw new NotImplementedException();
        public string GetLocalNodeName() => throw new NotImplementedException();
        public ValueTask<long?> GetFollowerLagAsync(int partitionId, string followerEndpoint) => throw new NotImplementedException();
        public ValueTask<bool> AmILeaderQuick(int partitionId) => throw new NotImplementedException();
        public ValueTask<bool> AmILeader(int partitionId, CancellationToken cancellationToken) => throw new NotImplementedException();
        public ValueTask<string> WaitForLeader(int partitionId, CancellationToken cancellationToken) => throw new NotImplementedException();
        public ValueTask<string> WaitForLeaderStableAsync(int partitionId, TimeSpan minStableFor, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<RaftOperationStatus> ForceLeaderForTestingAsync(int partitionId, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<RaftOperationStatus> StepDownAsync(int partitionId, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<RaftOperationStatus> TransferLeadershipAsync(int partitionId, string targetEndpoint, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<RaftOperationStatus> SuspendHeartbeatsAsync(int partitionId, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<RaftOperationStatus> ResumeHeartbeatsAsync(int partitionId, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<RaftPartitionLifecycleResult> CreatePartitionAsync(int partitionId, RaftRoutingMode mode = RaftRoutingMode.Unrouted, (int start, int end)? hashRange = null, CancellationToken ct = default) => throw new NotImplementedException();
        public Task<RaftPartitionLifecycleResult> RemovePartitionAsync(int partitionId, CancellationToken ct = default) => throw new NotImplementedException();
        public Task<RaftPartitionLifecycleResult> SplitPartitionAsync(int sourcePartitionId, int targetPartitionId = 0, RaftSplitPlan? plan = null, CancellationToken ct = default) => throw new NotImplementedException();
        public Task<RaftPartitionLifecycleResult> MergePartitionsAsync(int survivorPartitionId, int sourcePartitionId, RaftMergePlan? plan = null, CancellationToken ct = default) => throw new NotImplementedException();
        public long GetPartitionGeneration(int partitionId) => throw new NotImplementedException();
        public double GetPartitionLogOpsPerSecond(int partitionId) => throw new NotImplementedException();
        public int GetPartitionWalQueueDepth(int partitionId) => throw new NotImplementedException();
        public double GetPartitionCommitWaitMs(int partitionId) => throw new NotImplementedException();
        public IReadOnlyList<RaftPartitionRange> GetPartitionMap() => throw new NotImplementedException();
        public int GetPartitionKey(string partitionKey) => throw new NotImplementedException();
        public int GetPrefixPartitionKey(string prefixPartitionKey) => throw new NotImplementedException();
        public void RegisterStateMachineTransfer(IRaftStateMachineTransfer? transfer) => throw new NotImplementedException();
    }
}
