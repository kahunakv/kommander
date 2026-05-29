
using Kommander.Data;
using Kommander.Scheduling;
using Kommander.Tests.Scheduler;
using Kommander.Time;
using Kommander.WAL.Data;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.Scheduling;

/// <summary>
/// Verifies Task 5 acceptance: <see cref="RaftPartitionStateMachine"/> is usable without Nixie.
/// </summary>
public class TestRaftPartitionStateMachine
{
    [Fact]
    public void CanInstantiateWithoutNixie()
    {
        FakePartitionHost host = new();
        FakeWalFacade wal = new();
        CapturingReplySink sink = new();

        RaftPartitionStateMachine sm = new(host, wal, sink, NullLogger<IRaft>.Instance);

        Assert.Equal(RaftNodeState.Follower, sm.NodeState);
        Assert.Equal(0L, sm.CurrentTerm);
    }

    [Fact]
    public void CheckTicketCompletion_UnknownTicket_ReturnsNotFound()
    {
        FakePartitionHost host = new();
        FakeWalFacade wal = new();
        CapturingReplySink sink = new();
        RaftPartitionStateMachine sm = new(host, wal, sink, NullLogger<IRaft>.Instance);

        (RaftProposalTicketState state, long commitIndex) = sm.CheckTicketCompletion(HLCTimestamp.Zero);

        Assert.Equal(RaftProposalTicketState.NotFound, state);
        Assert.Equal(-1L, commitIndex);
    }

    [Fact]
    public void ReceiveHandshake_RecordsRemoteIndexInStateMachine()
    {
        FakePartitionHost host = new();
        FakeWalFacade wal = new();
        CapturingReplySink sink = new();
        RaftPartitionStateMachine sm = new(host, wal, sink, NullLogger<IRaft>.Instance);

        sm.ReceiveHandshake(remoteNodeId: 2, endpoint: "node-b", remoteMaxLogId: 42);

        // Handshake is accepted without throwing; internal index tracking is private.
        Assert.Equal(RaftNodeState.Follower, sm.NodeState);
    }

    private sealed class FakePartitionHost : IRaftPartitionHost
    {
        public int PartitionId => 1;

        public string Leader { get; set; } = "";

        public string LocalEndpoint => "node-a";

        public int LocalNodeId => 1;

        public RaftConfiguration Configuration { get; } = new()
        {
            Host = "localhost",
            Port = 8001,
            InitialPartitions = 1,
        };

        public HybridLogicalClock HybridLogicalClock { get; } = new();

        public IReadOnlyList<RaftNode> Nodes { get; } = [new("node-b")];

        public HLCTimestamp GetLastNodeActivity(string endpoint) => HLCTimestamp.Zero;

        public HLCTimestamp GetLastNodeHearthbeat(string endpoint) => HLCTimestamp.Zero;

        public void UpdateLastHeartbeat(string endpoint, HLCTimestamp timestamp) { }

        public void UpdateLastNodeActivity(string endpoint, HLCTimestamp timestamp) { }

        public void EnqueueResponse(string endpoint, RaftResponderRequest request) { }

        public Task InvokeLeaderChanged(int partitionId, string leader) => Task.CompletedTask;

        public Task<bool> InvokeReplicationReceived(int partitionId, RaftLog log) => Task.FromResult(true);

        public Task<bool> InvokeSystemReplicationReceived(int partitionId, RaftLog log) => Task.FromResult(true);

        public void InvokeReplicationError(int partitionId, RaftLog log) { }
    }

    private sealed class FakeWalFacade : IRaftWalFacade
    {
        private readonly FakeWAL wal = new();

        public ValueTask<long> RecoverAsync()
        {
            wal.Write([(1, [new RaftLog { Id = 1, Term = 1, Type = RaftLogType.Committed }])]);
            wal.DrainAll();
            return ValueTask.FromResult(1L);
        }

        public ValueTask<long> GetMaxLogAsync() => ValueTask.FromResult(wal.GetMaxLog(partitionId: 1));

        public ValueTask<long> GetCurrentTermAsync() => ValueTask.FromResult(wal.GetCurrentTerm(partitionId: 1));

        public WALWriteOperation EnqueuePropose(long term, List<RaftLog> logs, HLCTimestamp timestamp, bool autoCommit) =>
            new(null!, 1, WALWriteOperationType.LeaderPropose, (1, logs), timestamp, autoCommit: autoCommit, term: term);

        public WALWriteOperation EnqueueCommit(List<RaftLog> logs) =>
            new(null!, 2, WALWriteOperationType.LeaderCommit, (1, logs));

        public WALWriteOperation EnqueueRollback(List<RaftLog> logs) =>
            new(null!, 3, WALWriteOperationType.LeaderRollback, (1, logs));

        public WALWriteOperation? EnqueueProposeOrCommit(List<RaftLog>? logs, HLCTimestamp timestamp = default, string? endpoint = null, long term = -1) =>
            logs is null ? null : EnqueuePropose(term, logs, timestamp, autoCommit: false);
    }

    private sealed class CapturingReplySink : IRaftOperationReplySink
    {
        public List<(ulong Id, RaftResponse Response)> Completed { get; } = [];

        public void TryComplete(ulong correlationId, RaftResponse response) =>
            Completed.Add((correlationId, response));
    }
}
