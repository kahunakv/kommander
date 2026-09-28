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
/// A follower acknowledges an append before it delivers the entries the append committed, and
/// delivers them in executor turns of their own (<see cref="RaftConfiguration.FollowerApplyInOwnTurn"/>).
///
/// <para>The reason is the round: the consumer's callbacks for the previous commit used to hold the
/// follower's executor while the next proposal's append waited behind them. These tests pin what may
/// not change with it — every committed entry delivered exactly once, in log order, with the applied
/// cursor and its waiters moving only over delivered entries — and what must: the ack goes out first,
/// a turn delivers at most its budget, and a backlog beyond the high water is delivered anyway so a slow
/// consumer still slows the follower.</para>
///
/// <para>The executor is simulated: posted <see cref="RaftRequestType.ApplyCommittedEntries"/> requests
/// are collected and run by the test, which is how the partition executor dispatches them.</para>
/// </summary>
public class TestFollowerApplyInOwnTurn
{
    private const string LeaderEndpoint = "leader:8000";

    [Fact]
    public async Task Follower_AcksBeforeDelivering_ThenDeliversInItsOwnTurn()
    {
        Harness h = await BuildAsync();

        await h.AppendAsync(1, 5);

        Assert.Equal(["ack:Success"], h.Host.Events);
        Assert.Empty(h.Host.Delivered);
        Assert.Equal(1, h.PostedTurns);

        await h.RunPostedTurnsAsync();

        Assert.Equal([1L, 2L, 3L, 4L, 5L], h.Host.Delivered);
        Assert.Equal(["ack:Success", "apply:1", "apply:2", "apply:3", "apply:4", "apply:5"], h.Host.Events);
    }

    /// <summary>Control: with the option off the follower delivers inside the completion, before its ack.</summary>
    [Fact]
    public async Task OptionOff_DeliversInsideTheCompletion_BeforeTheAck()
    {
        Harness h = await BuildAsync(configure: c => c.FollowerApplyInOwnTurn = false);

        await h.AppendAsync(1, 5);

        Assert.Equal(0, h.PostedTurns);
        Assert.Equal(["apply:1", "apply:2", "apply:3", "apply:4", "apply:5", "ack:Success"], h.Host.Events);
    }

    /// <summary>The system partition carries the cluster's own configuration and always delivers inline.</summary>
    [Fact]
    public async Task SystemPartition_DeliversInsideTheCompletion()
    {
        Harness h = await BuildAsync(partitionId: RaftSystemConfig.SystemPartition);

        await h.AppendAsync(1, 3);

        Assert.Equal(0, h.PostedTurns);
        Assert.Equal(["apply:1", "apply:2", "apply:3", "ack:Success"], h.Host.Events);
    }

    /// <summary>
    /// A turn yields after its budget and queues the next one, so an append that arrives meanwhile waits
    /// for one turn at most. Nothing is queued once the backlog is delivered.
    /// </summary>
    [Fact]
    public async Task ApplyTurn_DeliversAtMostTheBudget_AndQueuesTheNextTurn()
    {
        Harness h = await BuildAsync(configure: c => c.FollowerApplyTurnBudget = 2);

        await h.AppendAsync(1, 5);

        Assert.True(await h.RunOnePostedTurnAsync());
        Assert.Equal([1L, 2L], h.Host.Delivered);

        Assert.True(await h.RunOnePostedTurnAsync());
        Assert.Equal([1L, 2L, 3L, 4L], h.Host.Delivered);

        Assert.True(await h.RunOnePostedTurnAsync());
        Assert.Equal([1L, 2L, 3L, 4L, 5L], h.Host.Delivered);

        Assert.False(await h.RunOnePostedTurnAsync());
    }

    /// <summary>
    /// The turn bound is in time: a turn yields once its time is spent, after at least one entry, so an
    /// append waits for one slow callback at most rather than for a whole entry cap of them.
    /// </summary>
    [Fact]
    public async Task ApplyTurn_YieldsWhenItsTimeIsSpent()
    {
        Harness h = await BuildAsync(configure: c =>
        {
            c.FollowerApplyTurnTime = TimeSpan.FromMilliseconds(1);
            c.FollowerApplyTurnBudget = 1024;
        });
        h.Host.ApplySpin = TimeSpan.FromMilliseconds(3);

        await h.AppendAsync(1, 3);

        Assert.True(await h.RunOnePostedTurnAsync());
        Assert.Equal([1L], h.Host.Delivered);

        await h.RunPostedTurnsAsync();
        Assert.Equal([1L, 2L, 3L], h.Host.Delivered);
    }

    /// <summary>
    /// Backpressure: past <see cref="Kommander.Consensus.FollowerApplyLane.BacklogHighWaterTurns"/> turns'
    /// worth of backlog a turn delivers the excess too, so a consumer slower than the commit rate slows the
    /// follower instead of growing the backlog without bound.
    /// </summary>
    [Fact]
    public async Task ApplyTurn_DeliversTheBacklogAboveTheHighWater()
    {
        const int Budget = 2;
        int highWater = Budget * Kommander.Consensus.FollowerApplyLane.BacklogHighWaterTurns;
        Harness h = await BuildAsync(configure: c => c.FollowerApplyTurnBudget = Budget);

        await h.AppendAsync(1, 30);

        Assert.True(await h.RunOnePostedTurnAsync());
        Assert.Equal(30 - highWater, h.Host.Delivered.Count);

        Assert.True(await h.RunOnePostedTurnAsync());
        Assert.Equal(30 - highWater + Budget, h.Host.Delivered.Count);

        await h.RunPostedTurnsAsync();
        Assert.Equal(Enumerable.Range(1, 30).Select(i => (long)i), h.Host.Delivered);
    }

    /// <summary>
    /// Every other delivery path reads the WAL from the applied cursor. One that runs while the lane still
    /// holds the entries (here the tick's retry) must not make the lane deliver them a second time.
    /// </summary>
    [Fact]
    public async Task EntriesDeliveredBeforeTheQueuedTurn_AreNotDeliveredTwice()
    {
        Harness h = await BuildAsync();

        await h.AppendAsync(1, 5);
        await h.Sm.CheckPartitionLeadershipAsync();
        Assert.Equal([1L, 2L, 3L, 4L, 5L], h.Host.Delivered);

        await h.AppendAsync(6, 8);
        await h.Sm.ResumeConsumerAppliesForTesting(null);   // a WAL drain from the cursor
        await h.RunPostedTurnsAsync();

        Assert.Equal([1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L], h.Host.Delivered);
    }

    /// <summary>
    /// A queued turn that never runs (a host that drops posted requests) cannot strand the entries: the
    /// tick's retry is itself a lane turn.
    /// </summary>
    [Fact]
    public async Task DroppedTurn_TheTickStillDelivers()
    {
        Harness h = await BuildAsync();

        await h.AppendAsync(1, 5);
        h.DropPostedTurns();

        await h.Sm.CheckPartitionLeadershipAsync();

        Assert.Equal([1L, 2L, 3L, 4L, 5L], h.Host.Delivered);
    }

    /// <summary>
    /// <c>ConfirmLocalApplicationAsync</c>'s follower half parks until the applied cursor covers the
    /// index. The cursor now moves in the lane's turn, and that is where the waiter must be released.
    /// </summary>
    [Fact]
    public async Task LocalApplicationWaiter_IsReleasedByTheTurnThatCoversIt()
    {
        Harness h = await BuildAsync(configure: c => c.FollowerApplyTurnBudget = 4);

        // The first append adopts the leader, and adopting a leader fails every parked waiter: park after it.
        await h.AppendAsync(1, 5);
        h.Sm.WaitLocalApplication(8, replyCorrelationId: 77);
        await h.AppendAsync(6, 10);
        await h.RunOnePostedTurnAsync();
        Assert.DoesNotContain(77UL, h.Sink.Completed.Keys);

        await h.RunPostedTurnsAsync();

        Assert.Equal(RaftOperationStatus.Success, h.Sink.Completed[77].Status);
    }

    /// <summary>
    /// The inline path (option off) advances the cursor in its own loop; it must release the waiters it
    /// covers too, or they stay parked until a later drain or their timeout.
    /// </summary>
    [Fact]
    public async Task OptionOff_LocalApplicationWaiter_IsReleasedByTheInlineDelivery()
    {
        Harness h = await BuildAsync(configure: c => c.FollowerApplyInOwnTurn = false);

        await h.AppendAsync(1, 5);
        h.Sm.WaitLocalApplication(8, replyCorrelationId: 77);
        Assert.DoesNotContain(77UL, h.Sink.Completed.Keys);

        await h.AppendAsync(6, 10);

        Assert.Equal(RaftOperationStatus.Success, h.Sink.Completed[77].Status);
    }

    /// <summary>
    /// While applies are held (a pending re-seed), a turn delivers nothing and the cursor stays put;
    /// resuming delivers everything once, in order.
    /// </summary>
    [Fact]
    public async Task HeldApplies_TurnDeliversNothing_ResumeDeliversOnce()
    {
        Harness h = await BuildAsync();

        h.Sm.HoldConsumerAppliesForTesting(null);
        await h.AppendAsync(1, 5);
        await h.RunPostedTurnsAsync();
        await h.Sm.CheckPartitionLeadershipAsync();
        Assert.Empty(h.Host.Delivered);

        await h.Sm.ResumeConsumerAppliesForTesting(null);
        await h.RunPostedTurnsAsync();

        Assert.Equal([1L, 2L, 3L, 4L, 5L], h.Host.Delivered);
    }

    /// <summary>
    /// A batch whose first entry is not committed yet is withheld exactly as the inline path withheld it,
    /// and delivered once its commit arrives in a later append.
    /// </summary>
    [Fact]
    public async Task ProposedEntries_AreWithheldUntilTheirCommitArrives()
    {
        Harness h = await BuildAsync();

        await h.AppendAsync(1, 3, type: RaftLogType.Proposed);
        await h.RunPostedTurnsAsync();
        Assert.Empty(h.Host.Delivered);

        await h.AppendAsync(1, 3);
        await h.RunPostedTurnsAsync();

        Assert.Equal([1L, 2L, 3L], h.Host.Delivered);
    }

    /// <summary>
    /// With pipelined proposals, commit broadcasts reach a follower in quorum order, not log order. A batch
    /// that lands above a gap is held, not delivered, and once the batch below it arrives both are
    /// delivered in id order from memory — the inline path re-read the upper batch from the WAL.
    /// </summary>
    [Fact]
    public async Task OutOfOrderCommits_AreHeldAndDeliveredInIdOrder_WithoutAWalRead()
    {
        Harness h = await BuildAsync();

        await h.AppendAsync(4, 6);
        await h.RunPostedTurnsAsync();
        Assert.Empty(h.Host.Delivered);

        await h.AppendAsync(1, 3);
        int readsBefore = h.Wal.RangeReads;
        await h.RunPostedTurnsAsync();

        Assert.Equal([1L, 2L, 3L, 4L, 5L, 6L], h.Host.Delivered);
        Assert.Equal(readsBefore, h.Wal.RangeReads);
    }

    /// <summary>
    /// A held run whose entries a WAL drain delivered meanwhile is pruned, not delivered again, and a
    /// re-sent batch that overlaps a held one delivers only what is new.
    /// </summary>
    [Fact]
    public async Task OverlappingRuns_DeliverEachEntryOnce()
    {
        Harness h = await BuildAsync();

        await h.AppendAsync(1, 5);
        await h.AppendAsync(3, 8);
        await h.RunPostedTurnsAsync();

        Assert.Equal([1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L], h.Host.Delivered);
    }

    // ── harness ──────────────────────────────────────────────────────────────

    /// <summary>
    /// A follower state machine past restore, with an empty log: restore seeds the applied cursor at
    /// the commit frontier (0), as on every real node.
    /// </summary>
    private static async Task<Harness> BuildAsync(int partitionId = 1, Action<RaftConfiguration>? configure = null)
    {
        RecordingHost host = new(partitionId) { Leader = LeaderEndpoint };

        // The entry cap bounds the turns here, so the counts do not depend on how fast the test runs;
        // the time bound has its own test.
        host.Configuration.FollowerApplyTurnTime = TimeSpan.Zero;
        configure?.Invoke(host.Configuration);

        RecordingWal wal = new();
        RecordingSink sink = new();

        RaftPartitionStateMachine sm = new(host, wal, sink, NullLogger<IRaft>.Instance);
        await sm.CompleteRestoreAsync([]);
        sm.MarkRestoredForTesting();

        Harness h = new(sm, host, wal, sink);
        sm.SetPostToExecutor(h.Posted.Add);
        return h;
    }

    private sealed class Harness(RaftPartitionStateMachine sm, RecordingHost host, RecordingWal wal, RecordingSink sink)
    {
        public RaftPartitionStateMachine Sm { get; } = sm;
        public RecordingHost Host { get; } = host;
        public RecordingSink Sink { get; } = sink;
        public RecordingWal Wal { get; } = wal;
        public List<RaftRequest> Posted { get; } = [];

        public int PostedTurns => Posted.Count(r => r.Type == RaftRequestType.ApplyCommittedEntries);

        /// <summary>Drives one AppendLogs batch down the real follower path and completes its WAL write.</summary>
        public async Task AppendAsync(long first, long last, RaftLogType type = RaftLogType.Committed)
        {
            List<RaftLog> logs = [];
            for (long id = first; id <= last; id++)
                logs.Add(new RaftLog { Id = id, Term = 1, LogType = "test", Type = type });

            await Sm.AppendLogsAsync(LeaderEndpoint, term: 1, timestamp: new HLCTimestamp(1, 1, 0),
                                     logs: logs, prevLogIndex: 0, prevLogTerm: 0);

            await Sm.CompleteWalOperationAsync(new RaftWalCompletion(
                PartitionId: Host.PartitionId, OperationId: Wal.LastOperationId, Term: 1,
                MinLogIndex: first, MaxLogIndex: last,
                OperationType: WALWriteOperationType.FollowerAppend,
                Status: RaftOperationStatus.Success));
        }

        /// <summary>Runs the oldest queued apply turn, as the executor would. False when none is queued.</summary>
        public async Task<bool> RunOnePostedTurnAsync()
        {
            int index = Posted.FindIndex(r => r.Type == RaftRequestType.ApplyCommittedEntries);
            if (index < 0)
                return false;

            Posted.RemoveAt(index);
            await Sm.RunFollowerApplyTurnAsync();
            return true;
        }

        public async Task RunPostedTurnsAsync()
        {
            for (int i = 0; i < 1000 && await RunOnePostedTurnAsync(); i++)
            {
            }
        }

        public void DropPostedTurns() => Posted.RemoveAll(r => r.Type == RaftRequestType.ApplyCommittedEntries);
    }

    /// <summary>Records deliveries and acks in one ordered event list.</summary>
    private sealed class RecordingHost(int partitionId) : IRaftPartitionHost
    {
        public List<long> Delivered { get; } = [];
        public List<string> Events { get; } = [];

        public int PartitionId => partitionId;
        public string Leader { get; set; } = "";
        public string LocalEndpoint => "follower:8001";
        public int LocalNodeId => 2;
        public ClusterMemberRole LocalRole => ClusterMemberRole.Voter;
        public bool IsVoter(string endpoint) => true;
        public RaftConfiguration Configuration { get; } = new()
        {
            Host = "follower", Port = 8001, InitialPartitions = 1,
            StartElectionTimeout = 100000, EndElectionTimeout = 200000,  // never self-elect
        };
        public HybridLogicalClock HybridLogicalClock { get; } = new();
        public IReadOnlyList<RaftNode> Nodes => [new(LeaderEndpoint)];
        public MemberLivenessState GetNodeLiveness(string endpoint) => MemberLivenessState.Alive;

        public HLCTimestamp GetLastNodeActivity(string e, int p) => HLCTimestamp.Zero;
        public void UpdateLastNodeActivity(string e, int p, HLCTimestamp t) { }

        public void EnqueueResponse(string e, RaftResponderRequest r)
        {
            if (r.Type == RaftResponderRequestType.CompleteAppendLogs)
                Events.Add($"ack:{r.CompleteAppendLogsRequest!.Status}");
        }

        public Task InvokeLeaderChanged(int p, string l) => Task.CompletedTask;

        /// <summary>CPU the consumer spends per delivered entry.</summary>
        public TimeSpan ApplySpin { get; set; }

        public Task<bool> InvokeReplicationReceived(int p, RaftLog l)
        {
            if (ApplySpin > TimeSpan.Zero)
            {
                global::System.Diagnostics.Stopwatch spin = global::System.Diagnostics.Stopwatch.StartNew();
                while (spin.Elapsed < ApplySpin)
                    Thread.SpinWait(16);
            }

            Delivered.Add(l.Id);
            Events.Add($"apply:{l.Id}");
            return Task.FromResult(true);
        }

        public Task<bool> InvokeSystemReplicationReceived(int p, RaftLog l) => InvokeReplicationReceived(p, l);
        public void InvokeReplicationError(int p, RaftLog l) { }

        public IRaftStateMachineTransfer? StateMachineTransfer => null;
        public IRaftSystemStateTransfer? SystemStateTransfer => null;

        public Task<SnapshotResponse> SendInstallSnapshotAsync(RaftNode node, SnapshotRequest request, CancellationToken ct) =>
            Task.FromResult(new SnapshotResponse(false));
    }

    /// <summary>A WAL that keeps what is appended; the commit frontier covers every committed entry written.</summary>
    private sealed class RecordingWal : IRaftWalFacade
    {
        private readonly SortedDictionary<long, RaftLog> _entries = [];
        private long _commitIndex;
        private long _nextOperationId;

        public long LastOperationId => _nextOperationId;

        /// <summary>WAL range reads the drains made.</summary>
        public int RangeReads { get; private set; }

        public long GetCommitIndex() => _commitIndex;

        public ValueTask<List<RaftLog>> GetRangeAllTypesAsync(long start, int max)
        {
            RangeReads++;
            return ValueTask.FromResult(_entries.Values.Where(l => l.Id >= start).Take(max).ToList());
        }

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
                if (log.Type == RaftLogType.Committed)
                {
                    // Contiguous: the frontier stops at the first id that is not committed.
                    while (_entries.TryGetValue(_commitIndex + 1, out RaftLog? next) && next.Type == RaftLogType.Committed)
                        _commitIndex++;
                }
            }

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

    private sealed class RecordingSink : IRaftOperationReplySink
    {
        public Dictionary<ulong, RaftResponse> Completed { get; } = [];

        public void TryComplete(ulong correlationId, RaftResponse response) => Completed[correlationId] = response;
    }
}
