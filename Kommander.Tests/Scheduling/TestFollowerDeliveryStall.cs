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
/// A follower that withholds a drain must eventually deliver the withheld entries, without
/// depending on more writes arriving.
///
/// <para>
/// <c>DrainCommittedAppliesAsync</c> returns false — withholding everything from the blocking id
/// onwards — when the next expected id is absent above the snapshot floor, or when an entry inside
/// the range is still <c>Proposed</c>. That is correct: delivering past it would skip it. Its
/// docstring then says the situation is routine on a follower because "the leader's
/// re-ship/backfill retries the drain".
/// </para>
///
/// <para>
/// That recovery assumption is what these tests probe. On a follower the drain has exactly one
/// trigger — a WAL write completing for the partition — and the leader stops sending entries as
/// soon as the follower's <em>commit</em> index catches up, which it does whether or not anything
/// was applied. An empty heartbeat carries no logs, enqueues no write, and completes nothing, so
/// once the withheld condition clears there is no longer anything to notice. The applied frontier
/// stays where it stopped, permanently.
/// </para>
///
/// <para>
/// This is the shape Jepsen run 31750742525 reported as <c>{:node "n3" :partition 1 :applied 0
/// :log 74 :behind 74}</c>: a node holding 74 entries that had delivered none of them, while the
/// leader correctly computed <c>gap=0</c> from the commit indexes the follower itself reported.
/// All 889 losses in that run were of this kind.
/// </para>
/// </summary>
public class TestFollowerDeliveryStall
{
    private const string LeaderEndpoint = "leader:8000";

    /// <summary>
    /// The reproduction. A batch lands with its first entry still unresolved, so the drain
    /// correctly withholds. The entry then resolves — but no further write arrives, exactly as on
    /// a follower whose log has caught up. The entries must still reach the consumer.
    /// </summary>
    [Fact]
    public async Task Follower_DeliversWithheldEntries_WhenNoFurtherWritesArrive()
    {
        (RaftPartitionStateMachine sm, RecordingHost host, StallWal wal) = Build();

        // Entries 1..5 arrive; 1 is still Proposed (its commit marker has not landed yet).
        await DeliverBatchAsync(sm, wal, unresolvedFirst: true);

        // Correct, and the reason this is not itself the bug: delivering 2..5 would skip 1.
        Assert.Empty(host.Delivered);

        // The marker lands. Nothing else happens — no new entries, because by commit index this
        // follower is current, so the leader sends only empty heartbeats from here on.
        wal.Resolve(1);

        // The periodic tick is all that remains on an idle follower.
        for (int i = 0; i < 5; i++)
            await sm.CheckPartitionLeadershipAsync();

        Assert.Equal([1L, 2L, 3L, 4L, 5L], host.Delivered);
    }

    /// <summary>
    /// Control: when the batch has nothing blocking it, delivery happens on the write completion
    /// itself and needs no tick. Without this, a fix that simply drained on every tick would look
    /// indistinguishable from one that fixed the trigger.
    /// </summary>
    [Fact]
    public async Task Follower_DeliversImmediately_WhenNothingIsWithheld()
    {
        (RaftPartitionStateMachine sm, RecordingHost host, StallWal wal) = Build();

        await DeliverBatchAsync(sm, wal, unresolvedFirst: false);

        Assert.Equal([1L, 2L, 3L, 4L, 5L], host.Delivered);
    }

    /// <summary>
    /// Control: a drain that is still legitimately blocked must stay blocked. A tick may retry,
    /// but retrying must not become a licence to deliver past an unresolved entry — that is the
    /// hole finding 1 was about.
    /// </summary>
    [Fact]
    public async Task Follower_StillWithholds_WhileTheBlockingEntryIsUnresolved()
    {
        (RaftPartitionStateMachine sm, RecordingHost host, StallWal wal) = Build();

        await DeliverBatchAsync(sm, wal, unresolvedFirst: true);

        for (int i = 0; i < 5; i++)
            await sm.CheckPartitionLeadershipAsync();

        Assert.Empty(host.Delivered);
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    /// <summary>
    /// Drives one AppendLogs batch of ids 1..5 down the real follower path: the state machine
    /// enqueues it, registers the pending WAL operation, and applies on completion.
    /// </summary>
    private static async Task DeliverBatchAsync(RaftPartitionStateMachine sm, StallWal wal, bool unresolvedFirst)
    {
        List<RaftLog> logs = [];
        for (long id = 1; id <= 5; id++)
            logs.Add(new RaftLog
            {
                Id      = id,
                Term    = 1,
                LogType = "test",
                Type    = unresolvedFirst && id == 1 ? RaftLogType.Proposed : RaftLogType.Committed,
            });

        await sm.AppendLogsAsync(LeaderEndpoint, term: 1, timestamp: new HLCTimestamp(1, 1, 0),
                                 logs: logs, prevLogIndex: 0, prevLogTerm: 0);

        await sm.CompleteWalOperationAsync(new RaftWalCompletion(
            PartitionId: 1, OperationId: wal.LastOperationId, Term: 1,
            MinLogIndex: 1, MaxLogIndex: 5,
            OperationType: WALWriteOperationType.FollowerAppend,
            Status: RaftOperationStatus.Success));
    }

    private static (RaftPartitionStateMachine, RecordingHost, StallWal) Build()
    {
        RecordingHost host = new() { Leader = LeaderEndpoint };
        StallWal wal = new();

        RaftPartitionStateMachine sm = new(host, wal, new NoopSink(), NullLogger<IRaft>.Instance);
        sm.SetPostToExecutor(_ => { });

        return (sm, host, wal);
    }

    // ── stubs ────────────────────────────────────────────────────────────────

    /// <summary>Records every entry the state machine delivers to the consumer, in order.</summary>
    private sealed class RecordingHost : IRaftPartitionHost
    {
        private readonly RaftConfiguration _config = new()
        {
            Host = "follower", Port = 8001, InitialPartitions = 1,
            StartElectionTimeout = 100000, EndElectionTimeout = 200000,  // never self-elect
        };

        public List<long> Delivered { get; } = [];

        public int PartitionId => 1;
        public string Leader { get; set; } = "";
        public string LocalEndpoint => "follower:8001";
        public int LocalNodeId => 2;
        public ClusterMemberRole LocalRole => ClusterMemberRole.Voter;
        public bool IsVoter(string endpoint) => true;
        public RaftConfiguration Configuration => _config;
        public HybridLogicalClock HybridLogicalClock { get; } = new();
        public IReadOnlyList<RaftNode> Nodes => [new(LeaderEndpoint)];
        public MemberLivenessState GetNodeLiveness(string endpoint) => MemberLivenessState.Alive;

        public HLCTimestamp GetLastNodeActivity(string e, int p) => HLCTimestamp.Zero;
        public void UpdateLastNodeActivity(string e, int p, HLCTimestamp t) { }
        public void EnqueueResponse(string e, RaftResponderRequest r) { }
        public Task InvokeLeaderChanged(int p, string l) => Task.CompletedTask;

        public Task<bool> InvokeReplicationReceived(int p, RaftLog l)
        {
            Delivered.Add(l.Id);
            return Task.FromResult(true);
        }

        public Task<bool> InvokeSystemReplicationReceived(int p, RaftLog l) => Task.FromResult(true);
        public void InvokeReplicationError(int p, RaftLog l) { }

        public IRaftStateMachineTransfer? StateMachineTransfer => null;
        public IRaftSystemStateTransfer? SystemStateTransfer => null;

        public Task<SnapshotResponse> SendInstallSnapshotAsync(RaftNode node, SnapshotRequest request, CancellationToken ct) =>
            Task.FromResult(new SnapshotResponse(false));
    }

    /// <summary>
    /// A WAL that keeps whatever is appended and lets a test resolve a still-Proposed entry
    /// afterwards, which is what a commit marker landing looks like from the drain's side.
    /// </summary>
    private sealed class StallWal : IRaftWalFacade
    {
        private readonly SortedDictionary<long, RaftLog> _entries = [];
        private long _commitIndex;
        private long _nextOperationId;

        public long LastOperationId => _nextOperationId;

        /// <summary>Flips a Proposed entry to Committed — the marker arriving late.</summary>
        public void Resolve(long id)
        {
            if (_entries.TryGetValue(id, out RaftLog? log))
                log.Type = RaftLogType.Committed;
        }

        public long GetCommitIndex() => _commitIndex;

        public ValueTask<List<RaftLog>> GetRangeAllTypesAsync(long start, int max) =>
            ValueTask.FromResult(_entries.Values.Where(l => l.Id >= start).Take(max).ToList());

        public ValueTask<List<RaftLog>> GetRangeAsync(long start, int max) =>
            ValueTask.FromResult(_entries.Values
                .Where(l => l.Id >= start && l.Type == RaftLogType.Committed).Take(max).ToList());

        public WALWriteOperation? EnqueueProposeOrCommit(List<RaftLog>? logs, HLCTimestamp t = default,
                                                         string? ep = null, long term = -1)
        {
            if (logs is null || logs.Count == 0)
                return null;

            foreach (RaftLog log in logs)
            {
                _entries[log.Id] = log;
                if (log.Type == RaftLogType.Committed && log.Id > _commitIndex)
                    _commitIndex = log.Id;
            }

            // The commit frontier covers the whole batch: the leader told us these are committed,
            // which is precisely why the drain is expected to reach the end of it.
            _commitIndex = Math.Max(_commitIndex, logs.Max(l => l.Id));

            return new(null!, Interlocked.Increment(ref _nextOperationId),
                       WALWriteOperationType.FollowerAppend, (1, logs), logIndex: logs.Max(l => l.Id));
        }

        public ValueTask<IReadOnlyList<RaftLog>> LoadRestoreLogsAsync() =>
            ValueTask.FromResult<IReadOnlyList<RaftLog>>([]);
        public ValueTask CompleteRestoreAsync(IReadOnlyList<RaftLog> logs) => ValueTask.CompletedTask;
        public ValueTask<long> GetMaxLogAsync() => ValueTask.FromResult(_entries.Count > 0 ? _entries.Keys.Max() : 0L);
        public ValueTask<long> TruncateLogsAfterAsync(long afterLogId) => ValueTask.FromResult(afterLogId);
        public ValueTask<long> GetCurrentTermAsync() => ValueTask.FromResult(1L);
        public ValueTask<long> GetAnyTermAtAsync(long logIndex) => ValueTask.FromResult(1L);
        public ValueTask<long> GetLastCheckpointAsync() => ValueTask.FromResult(-1L);

        public WALWriteOperation EnqueuePropose(long term, List<RaftLog> logs, HLCTimestamp ts, bool autoCommit) =>
            EnqueueProposeOrCommit(logs, ts, null, term)!;
        public WALWriteOperation EnqueueCommit(List<RaftLog> logs) => EnqueueProposeOrCommit(logs)!;
        public WALWriteOperation EnqueueRollback(List<RaftLog> logs) => EnqueueProposeOrCommit(logs)!;
        public void NotifyCommitted() { }
    }

    private sealed class NoopSink : IRaftOperationReplySink
    {
        public void TryComplete(ulong correlationId, RaftResponse response) { }
    }
}
