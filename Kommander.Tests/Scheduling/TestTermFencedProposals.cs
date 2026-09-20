using System.Diagnostics;
using Kommander;
using Kommander.Data;
using Kommander.Gossip;
using Kommander.Scheduling;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL.Data;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.Scheduling;

/// <summary>
/// Pins the term fence on proposals (<c>expectedTerm</c>) and the published-term surface behind
/// <c>IRaft.GetPartitionTerm</c> / <c>IRaft.OnLeadershipLost</c>, at the state-machine level:
///
/// <list type="bullet">
///   <item>A proposal stamped with a term other than the node's current term is refused with
///         <see cref="RaftOperationStatus.TermMismatch"/> BEFORE anything is appended — the answer
///         is a definite "did not take effect".</item>
///   <item>A matching stamp, or no stamp (0), is admitted.</item>
///   <item>A non-leader answers <see cref="RaftOperationStatus.NodeIsNotLeader"/> first, so a
///         router keeps its "try another replica" semantics for that status.</item>
///   <item>The coalesced batch dispatch carries the fence per message.</item>
///   <item>The term is published for off-thread readers, and a leadership stint that ends reports
///         the term it was held in through the host, at most once.</item>
/// </list>
/// </summary>
public class TestTermFencedProposals
{
    private const int Partition = 1;

    private static List<RaftLog> Logs() =>
        [new() { Type = RaftLogType.Proposed, LogType = "t", LogData = [1] }];

    private static (RaftPartitionStateMachine sm, FakePartitionHost host, FakeWalFacade wal, CapturingReplySink sink) Build(
        long? leaderTerm, params string[] peers)
    {
        FakePartitionHost host = new() { Nodes = peers.Select(p => new RaftNode(p)).ToList() };
        FakeWalFacade wal = new();
        CapturingReplySink sink = new();
        RaftPartitionStateMachine sm = new(host, wal, sink, NullLogger<IRaft>.Instance);

        if (leaderTerm is { } term)
        {
            sm.SetLeaderForTesting(term);
            sm.SetLocalCommittedIndexForTesting(-1);
        }

        return (sm, host, wal, sink);
    }

    [Fact]
    public async Task StaleExpectedTerm_IsRefusedBeforeAccept_WithTermMismatch()
    {
        (RaftPartitionStateMachine sm, _, FakeWalFacade wal, CapturingReplySink sink) = Build(leaderTerm: 3, "node-b", "node-c");

        await sm.ReplicateLogsAsync(Logs(), autoCommit: true, expectedTerm: 2, replyCorrelationId: 1);

        Assert.Contains(sink.Completed, r => r.Id == 1 && r.Response.Status == RaftOperationStatus.TermMismatch);
        Assert.Equal(0, wal.ProposeCalls); // nothing reached the log
    }

    [Fact]
    public async Task FutureExpectedTerm_IsRefusedBeforeAccept()
    {
        (RaftPartitionStateMachine sm, _, FakeWalFacade wal, CapturingReplySink sink) = Build(leaderTerm: 3, "node-b", "node-c");

        await sm.ReplicateLogsAsync(Logs(), autoCommit: true, expectedTerm: 4, replyCorrelationId: 2);

        Assert.Contains(sink.Completed, r => r.Id == 2 && r.Response.Status == RaftOperationStatus.TermMismatch);
        Assert.Equal(0, wal.ProposeCalls);
    }

    [Fact]
    public async Task MatchingExpectedTerm_IsAdmitted()
    {
        (RaftPartitionStateMachine sm, _, FakeWalFacade wal, CapturingReplySink sink) = Build(leaderTerm: 3, "node-b", "node-c");

        await sm.ReplicateLogsAsync(Logs(), autoCommit: true, expectedTerm: 3, replyCorrelationId: 3);

        Assert.DoesNotContain(sink.Completed, r => r.Id == 3 && r.Response.Status is RaftOperationStatus.TermMismatch or RaftOperationStatus.NodeIsNotLeader);
        Assert.Equal(1, wal.ProposeCalls);
    }

    [Fact]
    public async Task ZeroExpectedTerm_IsNotFenced()
    {
        (RaftPartitionStateMachine sm, _, FakeWalFacade wal, CapturingReplySink sink) = Build(leaderTerm: 3, "node-b", "node-c");

        await sm.ReplicateLogsAsync(Logs(), autoCommit: true, expectedTerm: 0, replyCorrelationId: 4);

        Assert.DoesNotContain(sink.Completed, r => r.Id == 4 && r.Response.Status == RaftOperationStatus.TermMismatch);
        Assert.Equal(1, wal.ProposeCalls);
    }

    [Fact]
    public async Task NonLeader_AnswersNodeIsNotLeader_BeforeTheTermCheck()
    {
        (RaftPartitionStateMachine sm, _, FakeWalFacade wal, CapturingReplySink sink) = Build(leaderTerm: null, "node-b", "node-c");

        await sm.ReplicateLogsAsync(Logs(), autoCommit: true, expectedTerm: 7, replyCorrelationId: 5);

        Assert.Contains(sink.Completed, r => r.Id == 5 && r.Response.Status == RaftOperationStatus.NodeIsNotLeader);
        Assert.Equal(0, wal.ProposeCalls);
    }

    [Fact]
    public async Task CoalescedBatchDispatch_CarriesTheFencePerMessage()
    {
        (RaftPartitionStateMachine sm, _, FakeWalFacade wal, CapturingReplySink sink) = Build(leaderTerm: 3, "node-b", "node-c");

        await sm.ReplicateLogsBatchAsync(
        [
            (Logs(), true, 2L, (ulong?)6),   // stale: refused
            (Logs(), true, 3L, (ulong?)7),   // current: admitted
        ]);

        Assert.Contains(sink.Completed, r => r.Id == 6 && r.Response.Status == RaftOperationStatus.TermMismatch);
        Assert.DoesNotContain(sink.Completed, r => r.Id == 7 && r.Response.Status == RaftOperationStatus.TermMismatch);
        Assert.Equal(1, wal.ProposeCalls);
    }

    [Fact]
    public void PublishedTerm_FollowsTheCurrentTerm()
    {
        (RaftPartitionStateMachine sm, _, _, _) = Build(leaderTerm: 7, "node-b");

        Assert.Equal(7, sm.CurrentTerm);
        Assert.Equal(7, sm.PublishedTerm);
    }

    [Fact]
    public async Task StepDown_ReportsTheLedTerm_ExactlyOnce()
    {
        (RaftPartitionStateMachine sm, FakePartitionHost host, _, _) = Build(leaderTerm: 3, "node-b", "node-c");

        await sm.StepDownAsync(replyCorrelationId: null);
        Assert.Equal(RaftNodeState.Follower, sm.NodeState);

        // The fake host's leader-changed notification does not take the pending loss (the real
        // host adapter does); the tick backstop reports it, and only once.
        await sm.CheckPartitionLeadershipAsync();
        Assert.Equal((Partition, 3L), Assert.Single(host.LeadershipLost));

        await sm.CheckPartitionLeadershipAsync();
        Assert.Single(host.LeadershipLost);
    }

    [Fact]
    public async Task HigherTermAdoption_ReportsTheTermThatWasLed_NotTheAdoptedOne()
    {
        (RaftPartitionStateMachine sm, FakePartitionHost host, _, _) = Build(leaderTerm: 3, "node-b", "node-c");

        // A leader RPC from a higher term demotes this node and moves its term to 5; the loss
        // must name the stint that ended (term 3) — that is what the consumer's staged state is
        // keyed on.
        await sm.InstallSnapshotAsync(new SnapshotInstallRequest
        {
            PartitionId = Partition,
            SnapshotIndex = 10,
            LastIncludedTerm = 5,
            LeaderTerm = 5,
            LeaderEndpoint = "node-b",
            Kind = SnapshotKind.PartitionState,
            Snapshot = new MemoryStream([1]),
        });

        await sm.CheckPartitionLeadershipAsync();

        Assert.Equal(RaftNodeState.Follower, sm.NodeState);
        Assert.Equal(5, sm.CurrentTerm);
        Assert.Equal((Partition, 3L), Assert.Single(host.LeadershipLost));
    }

    // ── stubs ─────────────────────────────────────────────────────────────────

    private sealed class FakePartitionHost : IRaftPartitionHost
    {
        public int PartitionId => Partition;
        public string Leader { get; set; } = "";
        public string LocalEndpoint => "node-a";
        public int LocalNodeId => 1;
        public ClusterMemberRole LocalRole => ClusterMemberRole.Voter;
        public bool IsVoter(string endpoint) => true;

        public RaftConfiguration Configuration { get; } = new()
        {
            Host = "localhost",
            Port = 8001,
            InitialPartitions = 1,
        };

        public HybridLogicalClock HybridLogicalClock { get; } = new();
        public long GetMonotonicTimestamp() => Stopwatch.GetTimestamp();
        public IReadOnlyList<RaftNode> Nodes { get; set; } = [];
        public List<(int Partition, long Term)> LeadershipLost { get; } = [];

        public MemberLivenessState GetNodeLiveness(string endpoint) => MemberLivenessState.Alive;
        public HLCTimestamp GetLastNodeActivity(string endpoint, int partitionId) => HLCTimestamp.Zero;
        public void UpdateLastNodeActivity(string endpoint, int partitionId, HLCTimestamp timestamp) { }
        public void EnqueueResponse(string endpoint, RaftResponderRequest request) { }

        public Task InvokeLeaderChanged(int partitionId, string leader) => Task.CompletedTask;

        public Task InvokeLeadershipLost(int partitionId, long term)
        {
            LeadershipLost.Add((partitionId, term));
            return Task.CompletedTask;
        }

        public Task<bool> InvokeReplicationReceived(int partitionId, RaftLog log) => Task.FromResult(true);
        public Task<bool> InvokeSystemReplicationReceived(int partitionId, RaftLog log) => Task.FromResult(true);
        public void InvokeReplicationError(int partitionId, RaftLog log) { }

        public IRaftStateMachineTransfer? StateMachineTransfer => null;
        public IRaftSystemStateTransfer? SystemStateTransfer => null;
        public IRaftPartitionStateTransfer? PartitionStateTransfer { get; } = new NoopPartitionTransfer();

        public Task<SnapshotResponse> SendInstallSnapshotAsync(RaftNode node, SnapshotRequest request, CancellationToken cancellationToken)
            => Task.FromResult(new SnapshotResponse(false));
    }

    private sealed class NoopPartitionTransfer : IRaftPartitionStateTransfer
    {
        public Task<Stream> ExportPartitionState(int partitionId, long upToIndex, CancellationToken ct) =>
            Task.FromResult<Stream>(new MemoryStream());

        public Task ImportPartitionState(int partitionId, Stream snapshot, CancellationToken ct) => Task.CompletedTask;
    }

    private sealed class FakeWalFacade : IRaftWalFacade
    {
        public int ProposeCalls { get; private set; }

        public ValueTask<IReadOnlyList<RaftLog>> LoadRestoreLogsAsync()
        {
            IReadOnlyList<RaftLog> none = [];
            return ValueTask.FromResult(none);
        }

        public ValueTask CompleteRestoreAsync(IReadOnlyList<RaftLog> logs) => ValueTask.CompletedTask;
        public ValueTask<long> GetMaxLogAsync() => ValueTask.FromResult(0L);
        public ValueTask<long> TruncateLogsAfterAsync(long afterLogId) => ValueTask.FromResult(afterLogId);
        public ValueTask<long> GetCurrentTermAsync() => ValueTask.FromResult(0L);
        public ValueTask<List<RaftLog>> GetRangeAsync(long startLogIndex, int maxEntries) => ValueTask.FromResult(new List<RaftLog>());
        public ValueTask<long> GetAnyTermAtAsync(long logIndex) => ValueTask.FromResult(-1L);
        public ValueTask<long> GetLastCheckpointAsync() => ValueTask.FromResult(-1L);
        public long GetCommitIndex() => 0;

        public WALWriteOperation EnqueuePropose(long term, List<RaftLog> logs, HLCTimestamp timestamp, bool autoCommit)
        {
            ProposeCalls++;
            return new(null!, 1, WALWriteOperationType.LeaderPropose, (1, logs), timestamp, autoCommit: autoCommit, term: term);
        }

        public WALWriteOperation EnqueueCommit(List<RaftLog> logs) =>
            new(null!, 2, WALWriteOperationType.LeaderCommit, (1, logs));

        public WALWriteOperation EnqueueRollback(List<RaftLog> logs) =>
            new(null!, 3, WALWriteOperationType.LeaderRollback, (1, logs));

        public WALWriteOperation? EnqueueProposeOrCommit(List<RaftLog>? logs, HLCTimestamp timestamp = default, string? endpoint = null, long term = -1) =>
            logs is null ? null : EnqueuePropose(term, logs, timestamp, autoCommit: false);

        public void NotifyCommitted() { }

        public ValueTask<(RaftOperationStatus Status, bool SuffixTruncated)> InstallSnapshotBoundaryAsync(long snapshotIndex, long lastIncludedTerm) =>
            ValueTask.FromResult((RaftOperationStatus.Success, false));

        public ValueTask<bool> PersistHardStateAsync(long currentTerm, string? votedFor) => ValueTask.FromResult(true);
    }

    private sealed class CapturingReplySink : IRaftOperationReplySink
    {
        public List<(ulong Id, RaftResponse Response)> Completed { get; } = [];

        public void TryComplete(ulong correlationId, RaftResponse response) =>
            Completed.Add((correlationId, response));
    }
}
