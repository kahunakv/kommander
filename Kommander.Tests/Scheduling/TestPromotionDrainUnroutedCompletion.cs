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
/// A node that wins an election while the write of its last committed entry has not yet been reported
/// back to the partition executor must still be able to promote.
///
/// <para>
/// The promotion drain retries inside one executor operation until the committed entries below the
/// frontier are readable, because its reads race the WAL write queue. The bound a drain normally
/// consults before it reads (<see cref="IRaftWalFacade.GetReadableResolvedHighWater"/>) is advanced
/// only when a WAL completion is routed, and routing a completion is itself an executor operation: it
/// waits behind the promotion. A drain that trusted the bound there could never see it move, so the
/// promotion spent its whole <see cref="RaftConfiguration.LeadershipBarrierTimeout"/> and was refused
/// while the entry it waited for sat readable in the log.
/// </para>
///
/// <para>
/// This is the shape of a leadership transfer issued right after a write: the target holds the entry,
/// the commit marker's completion is still queued when the vote quorum arrives, and the old leader
/// wins the next term back while the target is stuck. Observed as a transfer that answered
/// <c>Pending</c> after its full settle window with the old leader leading again.
/// </para>
/// </summary>
public class TestPromotionDrainUnroutedCompletion
{
    private const string LeaderEndpoint = "leader:8000";

    private const string LocalEndpoint = "target:8001";

    /// <summary>
    /// The write landed before the election was won; only its completion is outstanding. The
    /// promotion must deliver the entry and publish leadership without waiting for the completion.
    /// </summary>
    [Fact]
    public async Task Promotion_DeliversACommittedEntry_WhoseWriteCompletionIsStillQueued()
    {
        (RaftPartitionStateMachine sm, RecordingHost host, UnroutedCompletionWal wal) = await BuildAsync(TimeSpan.FromSeconds(30));

        await ReceiveCommittedEntryWithoutItsCompletionAsync(sm);
        wal.LandQueuedWrites();

        Stopwatch promotion = Stopwatch.StartNew();
        await WinTransferElectionAsync(sm);
        promotion.Stop();

        Assert.Equal(LocalEndpoint, host.Leader);
        Assert.Equal([1L], host.Delivered);
        Assert.True(promotion.Elapsed < TimeSpan.FromSeconds(5), $"the promotion took {promotion.Elapsed}");

        // The completion arrives afterwards, from the term the node has left. It must not deliver the
        // entry a second time.
        await sm.CompleteWalOperationAsync(FollowerAppendCompletion(wal));
        Assert.Equal([1L], host.Delivered);
    }

    /// <summary>
    /// The write is still in the WAL queue when the election is won and lands while the promotion
    /// waits. The retry must read the log again and find it.
    /// </summary>
    [Fact]
    public async Task Promotion_DeliversACommittedEntry_WhoseWriteLandsWhileItWaits()
    {
        (RaftPartitionStateMachine sm, RecordingHost host, UnroutedCompletionWal wal) = await BuildAsync(TimeSpan.FromSeconds(30));

        await ReceiveCommittedEntryWithoutItsCompletionAsync(sm);

        Task landing = Task.Run(async () =>
        {
            await Task.Delay(150);
            wal.LandQueuedWrites();
        }, TestContext.Current.CancellationToken);

        Stopwatch promotion = Stopwatch.StartNew();
        await WinTransferElectionAsync(sm);
        promotion.Stop();
        await landing;

        Assert.Equal(LocalEndpoint, host.Leader);
        Assert.Equal([1L], host.Delivered);
        Assert.True(promotion.Elapsed < TimeSpan.FromSeconds(5), $"the promotion took {promotion.Elapsed}");
    }

    /// <summary>
    /// Control: reading past the bound must not turn a withheld drain into a delivery. An entry that
    /// never becomes readable still refuses the promotion once the barrier timeout has run.
    /// </summary>
    [Fact]
    public async Task Promotion_IsStillRefused_WhenTheCommittedEntryNeverBecomesReadable()
    {
        (RaftPartitionStateMachine sm, RecordingHost host, _) = await BuildAsync(TimeSpan.FromMilliseconds(300));

        await ReceiveCommittedEntryWithoutItsCompletionAsync(sm);

        RaftException refused = await Assert.ThrowsAsync<RaftException>(() => WinTransferElectionAsync(sm));

        Assert.Contains("committed drain stopped at 0 below the frontier 1", refused.Message);
        Assert.NotEqual(LocalEndpoint, host.Leader);
        Assert.Empty(host.Delivered);
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    /// <summary>
    /// The leader ships entry 1 already committed. The follower queues the write, which advances its
    /// protocol commit frontier at once, and the test withholds the completion: from here on the
    /// state machine is in the window between "write queued" and "completion routed".
    /// </summary>
    private static Task ReceiveCommittedEntryWithoutItsCompletionAsync(RaftPartitionStateMachine sm) =>
        sm.AppendLogsAsync(LeaderEndpoint, term: 1, timestamp: new HLCTimestamp(1, 1, 0),
            logs: [new RaftLog { Id = 1, Term = 1, LogType = "test", Type = RaftLogType.Committed }],
            prevLogIndex: 0, prevLogTerm: 0);

    /// <summary>
    /// The leader hands the partition over and grants its vote: with one voter peer that is the
    /// quorum, so the grant runs the promotion inline, as the executor would.
    /// </summary>
    private static async Task WinTransferElectionAsync(RaftPartitionStateMachine sm)
    {
        await sm.ReceiveTransferLeadershipAsync(
            new TransferLeadershipRequest(partition: 1, term: 1, new HLCTimestamp(1, 2, 0), LeaderEndpoint, LocalEndpoint));

        await sm.ReceivedVoteAsync(LeaderEndpoint, voteTerm: 2, remoteMaxLogId: 1, preVote: false, remoteLastLogTerm: 1);
    }

    private static RaftWalCompletion FollowerAppendCompletion(UnroutedCompletionWal wal) => new(
        PartitionId: 1, OperationId: wal.LastOperationId, Term: 1,
        MinLogIndex: 1, MaxLogIndex: 1,
        OperationType: WALWriteOperationType.FollowerAppend,
        Status: RaftOperationStatus.Success);

    private static async Task<(RaftPartitionStateMachine, RecordingHost, UnroutedCompletionWal)> BuildAsync(TimeSpan barrierTimeout)
    {
        RecordingHost host = new(barrierTimeout) { Leader = LeaderEndpoint };
        UnroutedCompletionWal wal = new();

        RaftPartitionStateMachine sm = new(host, wal, new NoopSink(), NullLogger<IRaft>.Instance);

        // The real restore, not the test shortcut: it seeds the applied cursor at the restored commit
        // frontier (0 for an empty log). A cursor left at its pre-restore sentinel makes the drain treat
        // the first id as below the log's start and accept any gap in front of it.
        await sm.CompleteRestoreAsync(await sm.StartRestoreAsync());
        sm.SetPostToExecutor(_ => { });

        return (sm, host, wal);
    }

    // ── stubs ────────────────────────────────────────────────────────────────

    /// <summary>Records every entry the state machine delivers to the consumer, in order.</summary>
    private sealed class RecordingHost(TimeSpan barrierTimeout) : IRaftPartitionHost
    {
        private readonly RaftConfiguration _config = new()
        {
            Host = "target", Port = 8001, InitialPartitions = 1,
            StartElectionTimeout = 100000, EndElectionTimeout = 200000,  // elections only when the test asks
            LeadershipBarrierTimeout = barrierTimeout,
        };

        public List<long> Delivered { get; } = [];

        public int PartitionId => 1;
        public string Leader { get; set; } = "";
        public string LocalEndpoint => TestPromotionDrainUnroutedCompletion.LocalEndpoint;
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
    /// A WAL that separates the three moments the real one separates: a write is queued (the presence
    /// and protocol commit frontiers advance), the write lands (the row becomes readable), and its
    /// completion is routed (<see cref="MarkResolutionWritten"/> raises the readable bound). The test
    /// decides when the second happens and withholds the third.
    /// </summary>
    private sealed class UnroutedCompletionWal : IRaftWalFacade
    {
        private readonly object _gate = new();
        private readonly SortedDictionary<long, RaftLog> _queued = [];
        private readonly SortedDictionary<long, RaftLog> _landed = [];
        private long _commitIndex;
        private long _presentIndex;
        private long _readableResolvedHighWater;
        private long _nextOperationId;

        public long LastOperationId => _nextOperationId;

        /// <summary>The write scheduler executes what is queued: the rows become readable.</summary>
        public void LandQueuedWrites()
        {
            lock (_gate)
            {
                foreach (KeyValuePair<long, RaftLog> entry in _queued)
                    _landed[entry.Key] = entry.Value;

                _queued.Clear();
            }
        }

        public long GetCommitIndex() => _commitIndex;

        public long GetPresentIndex() => _presentIndex;

        public long GetPresentTerm() => 1;

        public long GetReadableResolvedHighWater() => _readableResolvedHighWater;

        public void MarkResolutionWritten(long resolvedMinLogIndex, long resolvedMaxLogIndex, bool synced)
        {
            if (resolvedMaxLogIndex > _readableResolvedHighWater)
                _readableResolvedHighWater = resolvedMaxLogIndex;
        }

        public ValueTask<List<RaftLog>> GetRangeAllTypesAsync(long start, int max)
        {
            lock (_gate)
                return ValueTask.FromResult(_landed.Values.Where(l => l.Id >= start).Take(max).ToList());
        }

        public ValueTask<List<RaftLog>> GetRangeAsync(long start, int max)
        {
            lock (_gate)
                return ValueTask.FromResult(_landed.Values
                    .Where(l => l.Id >= start && l.Type == RaftLogType.Committed).Take(max).ToList());
        }

        public WALWriteOperation? EnqueueProposeOrCommit(List<RaftLog>? logs, HLCTimestamp t = default,
                                                         string? ep = null, long term = -1)
        {
            if (logs is null || logs.Count == 0)
                return null;

            lock (_gate)
            {
                foreach (RaftLog log in logs)
                    _queued[log.Id] = log;
            }

            long max = logs.Max(l => l.Id);
            _presentIndex = Math.Max(_presentIndex, max);
            _commitIndex = Math.Max(_commitIndex, max);

            return new(null!, Interlocked.Increment(ref _nextOperationId),
                       WALWriteOperationType.FollowerAppend, (1, logs), logIndex: max);
        }

        public ValueTask<IReadOnlyList<RaftLog>> LoadRestoreLogsAsync() =>
            ValueTask.FromResult<IReadOnlyList<RaftLog>>([]);
        public ValueTask CompleteRestoreAsync(IReadOnlyList<RaftLog> logs) => ValueTask.CompletedTask;

        public ValueTask<long> GetMaxLogAsync()
        {
            lock (_gate)
                return ValueTask.FromResult(_landed.Count > 0 ? _landed.Keys.Max() : 0L);
        }

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
