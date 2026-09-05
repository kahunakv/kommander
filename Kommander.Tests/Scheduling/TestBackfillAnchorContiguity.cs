using System.Collections.Concurrent;
using Kommander.Data;
using Kommander.Gossip;
using Kommander.Scheduling;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL.Data;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.Scheduling;

/// <summary>
/// An anchored AppendLogs batch asserts, via (prevLogIndex, prevLogTerm), that its entries
/// immediately follow the anchor — nothing more. When the leader's backfill read skips a run of
/// uncommitted entries (an inherited range whose commit markers were lost and not yet durably
/// re-committed), the resulting batch starts ABOVE the anchor: shipping it lands the entries over
/// the follower's gap, advances no frontier, and repeats forever with no error anywhere — the
/// Jepsen one-stuck-entry wedge, seen from both sides. Two independent guards close it: the leader
/// refuses to ship a non-contiguous anchored batch, and the follower refuses to accept one.
/// </summary>
public class TestBackfillAnchorContiguity
{
    private const string VoterA    = "follower-a:9001";
    private const string VoterB    = "follower-b:9002";
    private const string LeaderEp  = "old-leader:9005";

    /// <summary>
    /// Leader-side guard: a backfill read whose first committed entry sits above the anchor (the
    /// WAL holds 1..47 committed, 48..86 Proposed, 87..117 committed) must ship nothing rather
    /// than an anchored batch that skips the Proposed run.
    /// </summary>
    [Fact]
    public async Task Leader_RefusesToShipNonContiguousBackfillBatch()
    {
        CapturingHost host = new();
        GapWal wal = new(committedThrough: 47, gapFrom: 48, gapTo: 86, tailThrough: 117);

        RaftPartitionStateMachine sm = new(host, wal, new NoopSink(), NullLogger<IRaft>.Instance);
        IReadOnlyList<RaftLog> logs = await sm.StartRestoreAsync();
        await sm.CompleteRestoreAsync(logs);
        sm.SetPostToExecutor(_ => { });

        // Bypass the promotion machinery: what matters here is a serving leader whose in-memory
        // committed view (117) is ahead of a follower and whose ON-DISK committed range has a
        // Proposed run in the middle — the post-restart shape the wedge run exhibited.
        sm.SetLeaderForTesting(term: 2);
        sm.SetLocalCommittedIndexForTesting(117);
        Assert.Equal(RaftNodeState.Leader, sm.NodeState);

        // The follower truthfully reports its frontier at 47 — 70 entries behind.
        await sm.CompleteAppendLogsAsync(VoterA, host.HybridLogicalClock.TrySendOrLocalEvent(1),
                                         RaftOperationStatus.Success, committedIndex: 47);

        host.Requests.Clear();
        await sm.CheckPartitionLeadershipAsync();

        Assert.Equal(0, EntryBatchesTo(host, VoterA));
    }

    /// <summary>
    /// Follower-side guard: an anchored batch whose first entry does not immediately follow
    /// prevLogIndex must be rejected with LogMismatch and written nowhere — before this guard it
    /// was silently appended over the gap the anchor never vouched for.
    /// </summary>
    [Fact]
    public async Task Follower_RejectsNonContiguousAnchoredBatch()
    {
        (RaftPartitionStateMachine sm, CapturingHost host, GapWal wal) = await BuildFollower(maxLog: 47);

        List<RaftLog> skipping =
        [
            new() { Id = 87, Term = 1, Type = RaftLogType.Committed, LogType = "t" },
            new() { Id = 88, Term = 1, Type = RaftLogType.Committed, LogType = "t" },
        ];

        host.Requests.Clear();
        await sm.AppendLogsAsync(LeaderEp, 1, host.HybridLogicalClock.TrySendOrLocalEvent(2),
                                 skipping, prevLogIndex: 47, prevLogTerm: 1);

        Assert.Equal(0, wal.AppendCalls);
        CompleteAppendLogsRequest? reply = host.Requests
            .Select(r => r.CompleteAppendLogsRequest)
            .FirstOrDefault(r => r is not null);
        Assert.NotNull(reply);
        Assert.Equal(RaftOperationStatus.LogMismatch, reply!.Status);
    }

    /// <summary>
    /// Control: a genuinely contiguous anchored batch is still accepted — without this, the two
    /// guards above would pass on any change that simply rejects every anchored batch.
    /// </summary>
    [Fact]
    public async Task Follower_AcceptsContiguousAnchoredBatch()
    {
        (RaftPartitionStateMachine sm, CapturingHost host, GapWal wal) = await BuildFollower(maxLog: 47);

        List<RaftLog> contiguous =
        [
            new() { Id = 48, Term = 1, Type = RaftLogType.Committed, LogType = "t" },
            new() { Id = 49, Term = 1, Type = RaftLogType.Committed, LogType = "t" },
        ];

        host.Requests.Clear();
        await sm.AppendLogsAsync(LeaderEp, 1, host.HybridLogicalClock.TrySendOrLocalEvent(2),
                                 contiguous, prevLogIndex: 47, prevLogTerm: 1);

        Assert.Equal(1, wal.AppendCalls);
        Assert.DoesNotContain(host.Requests,
            r => r.CompleteAppendLogsRequest?.Status == RaftOperationStatus.LogMismatch);
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    private static async Task<(RaftPartitionStateMachine, CapturingHost, GapWal)> BuildFollower(long maxLog)
    {
        CapturingHost host = new();
        GapWal wal = new(committedThrough: maxLog, gapFrom: long.MaxValue, gapTo: long.MaxValue, tailThrough: maxLog);

        RaftPartitionStateMachine sm = new(host, wal, new NoopSink(), NullLogger<IRaft>.Instance);
        IReadOnlyList<RaftLog> logs = await sm.StartRestoreAsync();
        await sm.CompleteRestoreAsync(logs);
        sm.SetPostToExecutor(_ => { });

        return (sm, host, wal);
    }

    private static int EntryBatchesTo(CapturingHost host, string endpoint) =>
        host.Requests.Count(r => r.Node?.Endpoint == endpoint
                                 && r.AppendLogsRequest?.Logs is { Count: > 0 });

    // ── stubs ────────────────────────────────────────────────────────────────

    /// <summary>
    /// WAL facade with a committed prefix, an uncommitted (Proposed) middle run, and a committed
    /// tail: the exact on-disk shape of a promoted leader whose inherited range lost its commit
    /// markers. <see cref="GetRangeAsync"/> mirrors the real facade by filtering the Proposed run,
    /// so a read anchored inside or below it comes back non-contiguous.
    /// </summary>
    private sealed class GapWal : IRaftWalFacade
    {
        private readonly long committedThrough;
        private readonly long gapFrom;
        private readonly long gapTo;
        private readonly long tailThrough;

        public int AppendCalls { get; private set; }

        public GapWal(long committedThrough, long gapFrom, long gapTo, long tailThrough)
        {
            this.committedThrough = committedThrough;
            this.gapFrom = gapFrom;
            this.gapTo = gapTo;
            this.tailThrough = tailThrough;
        }

        public ValueTask<IReadOnlyList<RaftLog>> LoadRestoreLogsAsync() =>
            ValueTask.FromResult<IReadOnlyList<RaftLog>>([]);
        public ValueTask CompleteRestoreAsync(IReadOnlyList<RaftLog> logs) => ValueTask.CompletedTask;
        public ValueTask<long> GetMaxLogAsync() => ValueTask.FromResult(tailThrough);
        public ValueTask<long> TruncateLogsAfterAsync(long afterLogId) => ValueTask.FromResult(afterLogId);
        public ValueTask<long> GetCurrentTermAsync() => ValueTask.FromResult(1L);

        public ValueTask<List<RaftLog>> GetRangeAsync(long startLogIndex, int maxEntries)
        {
            // Committed entries only: the prefix and the tail; the gapFrom..gapTo run is Proposed
            // and filtered out, exactly like RaftWriteAhead.GetRangeAsync.
            List<RaftLog> batch = [];
            for (long id = startLogIndex; id <= tailThrough && batch.Count < maxEntries; id++)
            {
                if (id >= gapFrom && id <= gapTo)
                    continue;
                batch.Add(new() { Id = id, Term = 1, Type = RaftLogType.Committed, LogType = "t" });
            }

            return ValueTask.FromResult(batch);
        }

        public ValueTask<long> GetAnyTermAtAsync(long logIndex) => ValueTask.FromResult(1L);
        public ValueTask<long> GetLastCheckpointAsync() => ValueTask.FromResult(0L);
        public long GetCommitIndex() => committedThrough;
        public WALWriteOperation EnqueuePropose(long term, List<RaftLog> logs, HLCTimestamp ts, bool autoCommit) => MakeNoOp();
        public WALWriteOperation EnqueueCommit(List<RaftLog> logs) => MakeNoOp();
        public WALWriteOperation EnqueueRollback(List<RaftLog> logs) => MakeNoOp();

        public WALWriteOperation? EnqueueProposeOrCommit(List<RaftLog>? logs, HLCTimestamp timestamp = default, string? endpoint = null, long term = -1)
        {
            if (logs is null)
                return null;
            AppendCalls++;
            return MakeNoOp();
        }

        public void NotifyCommitted() { }

        private static WALWriteOperation MakeNoOp() =>
            new(_ => { }, 0, WALWriteOperationType.LeaderPropose, (0, []));
    }

    private sealed class CapturingHost : IRaftPartitionHost
    {
        public ConcurrentBag<RaftResponderRequest> Requests { get; } = [];

        public int PartitionId => 1;
        public string Leader { get; set; } = "";
        public string LocalEndpoint => "leader:9000";
        public int LocalNodeId => 1;
        public ClusterMemberRole LocalRole => ClusterMemberRole.Voter;
        public bool IsVoter(string endpoint) => true;

        public RaftConfiguration Configuration { get; } = new()
        {
            Host = "leader", Port = 9000, InitialPartitions = 1, BackfillThreshold = 10,
            HeartbeatInterval = TimeSpan.Zero, RecentHeartbeat = TimeSpan.Zero,
        };

        public HybridLogicalClock HybridLogicalClock { get; } = new();
        public IReadOnlyList<RaftNode> Nodes { get; set; } = [new(VoterA), new(VoterB)];
        public MemberLivenessState GetNodeLiveness(string endpoint) => MemberLivenessState.Alive;

        public HLCTimestamp GetLastNodeActivity(string e, int p) => HLCTimestamp.Zero;
        public void UpdateLastNodeActivity(string e, int p, HLCTimestamp t) { }
        public void EnqueueResponse(string e, RaftResponderRequest r) => Requests.Add(r);
        public Task InvokeLeaderChanged(int p, string l) => Task.CompletedTask;
        public Task<bool> InvokeReplicationReceived(int p, RaftLog l) => Task.FromResult(true);
        public Task<bool> InvokeSystemReplicationReceived(int p, RaftLog l) => Task.FromResult(true);
        public void InvokeReplicationError(int p, RaftLog l) { }

        public IRaftStateMachineTransfer? StateMachineTransfer => null;
        public IRaftSystemStateTransfer? SystemStateTransfer => null;

        public Task<SnapshotResponse> SendInstallSnapshotAsync(RaftNode node, SnapshotRequest request, CancellationToken ct) =>
            Task.FromResult(new SnapshotResponse(true));
    }

    private sealed class NoopSink : IRaftOperationReplySink
    {
        public void TryComplete(ulong correlationId, RaftResponse response) { }
    }
}
