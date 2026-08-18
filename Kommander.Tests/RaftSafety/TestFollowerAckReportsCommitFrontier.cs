using Kommander.Data;
using Kommander.Gossip;
using Kommander.Scheduling;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL.Data;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.RaftSafety;

/// <summary>
/// Pins the acknowledgement contract of the follower-append WAL completion: the committedIndex a
/// follower reports back to its leader must be the WAL's <b>gap-aware commit frontier</b>
/// (<see cref="IRaftWalFacade.GetCommitIndex"/>), never the raw maximum id of the batch that just
/// landed (<c>completion.MaxLogIndex</c>).
///
/// <para><b>Why this matters.</b> The unanchored live-propose path ships <c>prevLogIndex == 0</c>
/// and skips the Log Matching check, so a behind follower can persist a lone high entry over a gap
/// in its log. If the ack advertised that high id, the leader's backfill gate would compute
/// <c>localCommittedIndex - reported == 0</c>, conclude the follower is caught up, and never repair
/// the missing prefix — a stable non-contiguous log and a permanently stranded replica. The
/// gap-aware frontier stops at the hole, so the leader keeps its progress cursors behind it and
/// backfills the prefix forward until the log is contiguous.</para>
///
/// <para>This is the regression anchor for the <c>CompleteFollowerAppend</c> commit-frontier fix.
/// It exists so the contract is enforced by a test rather than by a comment, and so the behaviour
/// is pinned independently of which type happens to host the completion handler.</para>
/// </summary>
public sealed class TestFollowerAckReportsCommitFrontier
{
    /// <summary>The gap-aware committed frontier the stub WAL reports: entries 1..3 are contiguous
    /// and committed, and there is a hole above 3.</summary>
    private const long GapAwareCommitFrontier = 3;

    /// <summary>The id of the lone high entry delivered over the gap — the raw batch max that must
    /// NOT be advertised to the leader.</summary>
    private const long RawBatchMaxOverTheGap = 7;

    private const long FollowerAppendOperationId = 4242;

    internal sealed class StubHost : IRaftPartitionHost
    {
        private readonly RaftConfiguration _config = new()
        {
            StartElectionTimeout = 50,
            EndElectionTimeout = 100,
        };

        public int PartitionId => 0;
        public string Leader { get; set; } = "";
        public string LocalEndpoint => "follower-node";
        public int LocalNodeId => 1;
        public ClusterMemberRole LocalRole => ClusterMemberRole.Voter;
        public bool IsVoter(string endpoint) => true;
        public bool IsMember(string endpoint) => true;
        public RaftConfiguration Configuration => _config;
        public HybridLogicalClock HybridLogicalClock { get; } = new();
        public IReadOnlyList<RaftNode> Nodes { get; set; } = Array.Empty<RaftNode>();
        public List<(string Endpoint, RaftResponderRequest Request)> EnqueuedResponses { get; } = [];

        public HLCTimestamp GetLastNodeActivity(string endpoint, int partitionId) => HLCTimestamp.Zero;
        public HLCTimestamp GetLastNodeHearthbeat(string endpoint, int partitionId) => HLCTimestamp.Zero;
        public void UpdateLastHeartbeat(string endpoint, int partitionId, HLCTimestamp timestamp) { }
        public void UpdateLastNodeActivity(string endpoint, int partitionId, HLCTimestamp timestamp) { }
        public void EnqueueResponse(string endpoint, RaftResponderRequest request) => EnqueuedResponses.Add((endpoint, request));
        public Task InvokeLeaderChanged(int partitionId, string leader) => Task.CompletedTask;
        public Task<bool> InvokeReplicationReceived(int partitionId, RaftLog log) => Task.FromResult(true);
        public Task<bool> InvokeSystemReplicationReceived(int partitionId, RaftLog log) => Task.FromResult(true);
        public void InvokeReplicationError(int partitionId, RaftLog log) { }
        public IRaftStateMachineTransfer? StateMachineTransfer => null;
        public IRaftSystemStateTransfer? SystemStateTransfer => null;
        public Task<SnapshotResponse> SendInstallSnapshotAsync(RaftNode node, SnapshotRequest request, CancellationToken ct) => Task.FromResult(new SnapshotResponse(false));
        public MemberLivenessState GetNodeLiveness(string endpoint) => MemberLivenessState.Alive;
    }

    /// <summary>
    /// A WAL whose committed frontier deliberately sits BELOW the highest id it has been handed —
    /// the shape a follower is left in when an unanchored entry lands over a gap.
    /// </summary>
    internal sealed class GappyWal : IRaftWalFacade
    {
        public ValueTask<IReadOnlyList<RaftLog>> LoadRestoreLogsAsync() => ValueTask.FromResult<IReadOnlyList<RaftLog>>([]);
        public ValueTask CompleteRestoreAsync(IReadOnlyList<RaftLog> logs) => ValueTask.CompletedTask;
        public ValueTask<long> GetMaxLogAsync() => ValueTask.FromResult(RawBatchMaxOverTheGap);
        public ValueTask<long> TruncateLogsAfterAsync(long afterLogId) => ValueTask.FromResult(afterLogId);
        public ValueTask<long> GetCurrentTermAsync() => ValueTask.FromResult(0L);
        public ValueTask<List<RaftLog>> GetRangeAsync(long startLogIndex, int maxEntries) => ValueTask.FromResult(new List<RaftLog>());
        public ValueTask<long> GetAnyTermAtAsync(long logIndex) => ValueTask.FromResult(-1L);
        public ValueTask<long> GetLastCheckpointAsync() => ValueTask.FromResult(-1L);

        /// <summary>The gap-aware frontier: stops at the hole, well below the raw batch max.</summary>
        public long GetCommitIndex() => GapAwareCommitFrontier;

        public WALWriteOperation EnqueuePropose(long term, List<RaftLog> logs, HLCTimestamp ts, bool autoCommit) => MakeNoOp();
        public WALWriteOperation EnqueueCommit(List<RaftLog> logs) => MakeNoOp();
        public WALWriteOperation EnqueueRollback(List<RaftLog> logs) => MakeNoOp();

        public WALWriteOperation? EnqueueProposeOrCommit(List<RaftLog>? logs, HLCTimestamp timestamp = default, string? endpoint = null, long term = -1) =>
            new(_ => { }, FollowerAppendOperationId, WALWriteOperationType.FollowerAppend, (0, logs ?? []), timestamp, endpoint, term);

        public void NotifyCommitted() { }

        private static WALWriteOperation MakeNoOp() => new(_ => { }, 0, WALWriteOperationType.LeaderPropose, (0, []));
    }

    private sealed class RelaySink : IRaftOperationReplySink
    {
        internal RaftPartitionExecutor? Executor;
        public void TryComplete(ulong correlationId, RaftResponse response) => Executor?.DeliverReply(correlationId, response);
    }

    /// <summary>
    /// A follower whose committed frontier is below the batch it just persisted must advertise the
    /// frontier, so the leader still sees it as behind and keeps backfilling the missing prefix.
    /// </summary>
    [Fact]
    public async Task FollowerAck_ReportsGapAwareCommitFrontier_NotTheRawBatchMax()
    {
        StubHost host = new();
        GappyWal wal = new();
        RelaySink sink = new();
        RaftPartitionStateMachine sm = new(host, wal, sink, NullLogger<IRaft>.Instance);
        using RaftPartitionExecutor executor = new(sm, 0, slowThresholdMs: 0, NullLogger<IRaft>.Instance);
        sink.Executor = executor;
        executor.Start();
        await executor.RestoreTask;

        HLCTimestamp timestamp = host.HybridLogicalClock.TrySendOrLocalEvent(host.LocalNodeId);

        // The leader ships a lone high entry with no anchor (the unanchored live-propose shape).
        // This registers the follower-append pending entry the completion below resolves.
        // Posted rather than asked: the append's reply is deliberately withheld until its WAL
        // operation completes, so awaiting it here would wait on the completion sent below.
        executor.Post(new RaftRequest(
            RaftRequestType.AppendLogs,
            term: 1,
            timestamp: timestamp,
            endpoint: "leader-node",
            logs: [new RaftLog { Id = RawBatchMaxOverTheGap, Type = RaftLogType.Committed }]));

        await executor.DrainAsync(TestContext.Current.CancellationToken)
            .WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);

        host.EnqueuedResponses.Clear();

        // The WAL write completes carrying the raw batch max — the value that must NOT be reported.
        await executor.Ask(
            new RaftRequest(
                RaftRequestType.WriteOperationCompleted,
                new RaftWalCompletion(
                    PartitionId: 0,
                    OperationId: FollowerAppendOperationId,
                    Term: -1,
                    MinLogIndex: RawBatchMaxOverTheGap,
                    MaxLogIndex: RawBatchMaxOverTheGap,
                    OperationType: WALWriteOperationType.FollowerAppend,
                    Status: RaftOperationStatus.Success)),
            TestContext.Current.CancellationToken);

        CompleteAppendLogsRequest ack = host.EnqueuedResponses
            .Select(x => x.Request.CompleteAppendLogsRequest)
            .OfType<CompleteAppendLogsRequest>()
            .Single();

        Assert.Equal(GapAwareCommitFrontier, ack.CommitIndex);
        Assert.NotEqual(RawBatchMaxOverTheGap, ack.CommitIndex);
    }
}
