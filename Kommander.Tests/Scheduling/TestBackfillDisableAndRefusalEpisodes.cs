using System.Collections.Concurrent;
using Kommander.Data;
using Kommander.Gossip;
using Kommander.Scheduling;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL.Data;
using Microsoft.Extensions.Logging;

namespace Kommander.Tests.Scheduling;

/// <summary>
/// Two operational contracts around leader-driven backfill.
///
/// <para><b>Disabling actually disables.</b> <c>BackfillThreshold</c> gates only the actively-behind
/// trigger, so a consumer raising it to <c>int.MaxValue</c> to mean "off" still got backfill from the
/// idle-tail and crash-restart-regression triggers — and idle is exactly when it expected the
/// suppression to hold. <c>BackfillEnabled</c> must hold on all three.</para>
///
/// <para><b>The non-contiguous refusal is episode-scoped.</b> Where the underlying range is never
/// repaired (a peer permanently below the compaction floor) the refusal used to warn on every
/// heartbeat forever. It must warn once per episode, re-warn when the condition genuinely changes,
/// say so when it clears, and stay queryable throughout.</para>
/// </summary>
public class TestBackfillDisableAndRefusalEpisodes
{
    private const string VoterA = "follower-a:9001";
    private const string VoterB = "follower-b:9002";

    // ── Issue 1: BackfillEnabled holds on every trigger ──────────────────────

    /// <summary>
    /// Control for <see cref="IdleTailGap_ShipsNothing_WhenBackfillDisabled"/>: the idle-tail trigger
    /// fires on a sub-threshold gap once live replication goes quiet. Without this, the disabled test
    /// would pass on a setup that never triggered backfill in the first place.
    /// </summary>
    [Fact]
    public async Task IdleTailGap_ShipsEntries_WhenBackfillEnabled()
    {
        (RaftPartitionStateMachine sm, CapturingHost host, _) = await BuildIdleTailLeader(backfillEnabled: true);

        await sm.CheckPartitionLeadershipAsync();

        Assert.True(EntryBatchesTo(host, VoterA) > 0);
    }

    /// <summary>
    /// The idle-tail trigger ignores <c>BackfillThreshold</c> by design — a residual tail gap cannot
    /// be healed by empty heartbeats — so it is the trigger that leaked past the old "disable" lever.
    /// </summary>
    [Fact]
    public async Task IdleTailGap_ShipsNothing_WhenBackfillDisabled()
    {
        (RaftPartitionStateMachine sm, CapturingHost host, _) = await BuildIdleTailLeader(backfillEnabled: false);

        await sm.CheckPartitionLeadershipAsync();

        Assert.Equal(0, EntryBatchesTo(host, VoterA));
        Assert.Empty(sm.GetSnapshotStatuses());
    }

    /// <summary>
    /// Raising the threshold to <see cref="int.MaxValue"/> — the mitigation consumers actually wrote —
    /// does <b>not</b> hold: this is the leak the flag exists to close, pinned so the two levers are
    /// never conflated again.
    /// </summary>
    [Fact]
    public async Task IdleTailGap_ShipsEntries_DespiteMaxThreshold()
    {
        (RaftPartitionStateMachine sm, CapturingHost host, _) =
            await BuildIdleTailLeader(backfillEnabled: true, backfillThreshold: int.MaxValue);

        await sm.CheckPartitionLeadershipAsync();

        Assert.True(EntryBatchesTo(host, VoterA) > 0);
    }

    /// <summary>
    /// Control for <see cref="RegressedFrontier_ShipsNothing_WhenBackfillDisabled"/>. The setup keeps
    /// the gap at or below the threshold and the committed frontier at the live floor, so neither the
    /// actively-behind nor the idle-tail trigger can fire — only the crash-restart regression.
    /// </summary>
    [Fact]
    public async Task RegressedFrontier_ShipsEntries_WhenBackfillEnabled()
    {
        (RaftPartitionStateMachine sm, CapturingHost host) = await BuildRegressedLeader(backfillEnabled: true);

        await sm.CheckPartitionLeadershipAsync();

        Assert.True(EntryBatchesTo(host, VoterA) > 0);
    }

    [Fact]
    public async Task RegressedFrontier_ShipsNothing_WhenBackfillDisabled()
    {
        (RaftPartitionStateMachine sm, CapturingHost host) = await BuildRegressedLeader(backfillEnabled: false);

        await sm.CheckPartitionLeadershipAsync();

        Assert.Equal(0, EntryBatchesTo(host, VoterA));
    }

    // ── Issue 2: the refusal is episode-scoped and queryable ─────────────────

    /// <summary>
    /// A peer whose anchor can never be served keeps triggering the refusal every heartbeat. The
    /// condition stays reported — one Warning, then Debug repeats and a live status entry — instead
    /// of one Warning per heartbeat for as long as the deployment runs.
    /// </summary>
    [Fact]
    public async Task NonContiguousRefusal_WarnsOncePerEpisode()
    {
        (RaftPartitionStateMachine sm, CapturingHost host, LevelCountingLogger logger, _) =
            await BuildRefusingLeader();

        const int rounds = 12;
        for (int i = 0; i < rounds; i++)
            await sm.CheckPartitionLeadershipAsync();

        Assert.Equal(1, logger.Count(LogLevel.Warning, "Refusing non-contiguous backfill batch"));
        Assert.Equal(rounds - 1, logger.Count(LogLevel.Debug, "Still refusing non-contiguous backfill batch"));
        Assert.Equal(0, EntryBatchesTo(host, VoterA));

        // Queryable while it persists: the anchor, what the leader could actually read, and how many
        // times it has fired — the information the suppressed log lines carried.
        RaftBackfillStatus status = Assert.Single(sm.GetBackfillStatuses(), s => s.FollowerEndpoint == VoterA);
        Assert.Equal(48, status.AnchorIndex);
        Assert.Equal(87, status.FirstAvailableIndex);
        Assert.Equal(rounds, status.Occurrences);
    }

    /// <summary>
    /// Episode identity is the (anchor, first-available) pair, so a genuinely different condition is
    /// not swallowed by an open episode — suppression must be per condition, not per endpoint.
    /// </summary>
    [Fact]
    public async Task NonContiguousRefusal_ChangedConditionOpensNewEpisode()
    {
        (RaftPartitionStateMachine sm, _, LevelCountingLogger logger, GapWal wal) = await BuildRefusingLeader();

        await sm.CheckPartitionLeadershipAsync();
        await sm.CheckPartitionLeadershipAsync();
        Assert.Equal(1, logger.Count(LogLevel.Warning, "Refusing non-contiguous backfill batch"));

        // The uncommitted run grows: same anchor, different first readable entry.
        wal.GapTo = 95;
        await sm.CheckPartitionLeadershipAsync();

        Assert.Equal(2, logger.Count(LogLevel.Warning, "Refusing non-contiguous backfill batch"));
        RaftBackfillStatus status = Assert.Single(sm.GetBackfillStatuses(), s => s.FollowerEndpoint == VoterA);
        Assert.Equal(96, status.FirstAvailableIndex);
        Assert.Equal(1, status.Occurrences);
    }

    /// <summary>
    /// Recovery is stated, not inferred from silence: when the range is repaired and a contiguous
    /// batch ships, the episode closes with its own line and stops being reported.
    /// </summary>
    [Fact]
    public async Task NonContiguousRefusal_ClearsWhenContiguousBatchShips()
    {
        (RaftPartitionStateMachine sm, CapturingHost host, LevelCountingLogger logger, GapWal wal) =
            await BuildRefusingLeader();

        await sm.CheckPartitionLeadershipAsync();
        Assert.NotEmpty(sm.GetBackfillStatuses());

        // The inherited range re-commits: the entries at the anchor are readable again.
        wal.GapFrom = long.MaxValue;
        wal.GapTo = long.MaxValue;
        host.Requests.Clear();

        await sm.CheckPartitionLeadershipAsync();

        Assert.True(EntryBatchesTo(host, VoterA) > 0);
        Assert.Equal(1, logger.Count(LogLevel.Information, "Non-contiguous backfill for"));
        Assert.Empty(sm.GetBackfillStatuses());
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    /// <summary>
    /// Leader whose committed frontier sits one live commit above the promotion floor with a peer
    /// trailing by less than the threshold: the idle-tail trigger, and only that trigger.
    /// </summary>
    private static async Task<(RaftPartitionStateMachine, CapturingHost, GapWal)> BuildIdleTailLeader(
        bool backfillEnabled, int backfillThreshold = 10)
    {
        CapturingHost host = new();
        host.Configuration.BackfillEnabled = backfillEnabled;
        host.Configuration.BackfillThreshold = backfillThreshold;

        GapWal wal = new(committedThrough: 47, gapFrom: long.MaxValue, gapTo: long.MaxValue, tailThrough: 50);
        RaftPartitionStateMachine sm = await BuildLeader(host, wal);

        // liveCommitFloor is seeded from the WAL commit index (47) at promotion; 50 is a live commit
        // above it, which is what the idle-tail trigger requires.
        sm.SetLocalCommittedIndexForTesting(50);

        await sm.CompleteAppendLogsAsync(VoterA, host.HybridLogicalClock.TrySendOrLocalEvent(1),
                                         RaftOperationStatus.Success, committedIndex: 45);

        host.Requests.Clear();
        return (sm, host, wal);
    }

    /// <summary>
    /// Leader with a peer that reported a frontier below its recorded matchIndex (the crash-restart
    /// signature). The committed frontier equals the promotion floor so the idle-tail trigger cannot
    /// fire, and the gap stays at the threshold so the actively-behind trigger cannot either.
    /// </summary>
    private static async Task<(RaftPartitionStateMachine, CapturingHost)> BuildRegressedLeader(bool backfillEnabled)
    {
        CapturingHost host = new();
        host.Configuration.BackfillEnabled = backfillEnabled;

        GapWal wal = new(committedThrough: 55, gapFrom: long.MaxValue, gapTo: long.MaxValue, tailThrough: 60);
        RaftPartitionStateMachine sm = await BuildLeader(host, wal);

        await sm.CompleteAppendLogsAsync(VoterA, host.HybridLogicalClock.TrySendOrLocalEvent(1),
                                         RaftOperationStatus.Success, committedIndex: 50);
        // Restart: the peer lost its lazy commit markers and reports below its recorded match.
        await sm.CompleteAppendLogsAsync(VoterA, host.HybridLogicalClock.TrySendOrLocalEvent(1),
                                         RaftOperationStatus.Success, committedIndex: 45);

        host.Requests.Clear();
        return (sm, host);
    }

    /// <summary>
    /// Leader whose WAL has an uncommitted run starting exactly at the follower's anchor, so every
    /// heartbeat produces a non-contiguous read and the refusal fires indefinitely.
    /// </summary>
    private static async Task<(RaftPartitionStateMachine, CapturingHost, LevelCountingLogger, GapWal)> BuildRefusingLeader()
    {
        LevelCountingLogger logger = new();
        CapturingHost host = new();
        GapWal wal = new(committedThrough: 47, gapFrom: 48, gapTo: 86, tailThrough: 117);

        RaftPartitionStateMachine sm = await BuildLeader(host, wal, logger);

        // The ack lands while the leader's committed frontier still equals the follower's, so the
        // per-ack eager catch-up does not fire and the episode is opened by the heartbeat path under
        // test rather than by the setup.
        await sm.CompleteAppendLogsAsync(VoterA, host.HybridLogicalClock.TrySendOrLocalEvent(1),
                                         RaftOperationStatus.Success, committedIndex: 47);
        sm.SetLocalCommittedIndexForTesting(117);

        host.Requests.Clear();
        logger.Reset();
        return (sm, host, logger, wal);
    }

    private static async Task<RaftPartitionStateMachine> BuildLeader(
        CapturingHost host, GapWal wal, ILogger<IRaft>? logger = null)
    {
        RaftPartitionStateMachine sm = new(host, wal, new NoopSink(), logger ?? new LevelCountingLogger());
        IReadOnlyList<RaftLog> logs = await sm.StartRestoreAsync();
        await sm.CompleteRestoreAsync(logs);
        sm.SetPostToExecutor(_ => { });
        sm.SetLeaderForTesting(term: 2);
        return sm;
    }

    private static int EntryBatchesTo(CapturingHost host, string endpoint) =>
        host.Requests.Count(r => r.Node?.Endpoint == endpoint
                                 && r.AppendLogsRequest?.Logs is { Count: > 0 });

    // ── stubs ────────────────────────────────────────────────────────────────

    /// <summary>
    /// Counts log lines by level and substring. Enabled at every level: the paths under test log at
    /// Warning, Debug and Information, and asserting the level is half the point — the fix moved
    /// repeats from Warning to Debug rather than deleting them.
    /// </summary>
    private sealed class LevelCountingLogger : ILogger<IRaft>
    {
        private readonly List<(LogLevel Level, string Message)> messages = [];
        private readonly object sync = new();

        public int Count(LogLevel level, string substring)
        {
            lock (sync)
                return messages.Count(m => m.Level == level && m.Message.Contains(substring));
        }

        public void Reset()
        {
            lock (sync)
                messages.Clear();
        }

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => logLevel != LogLevel.None;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
                                Func<TState, Exception?, string> formatter)
        {
            lock (sync)
                messages.Add((logLevel, formatter(state, exception)));
        }
    }

    /// <summary>
    /// WAL facade with a committed prefix, an uncommitted (Proposed) middle run, and a committed
    /// tail. <see cref="GetRangeAsync"/> filters the run exactly as the real facade does, so a read
    /// anchored at or below it comes back non-contiguous. The gap bounds are mutable so a test can
    /// change or repair the condition between heartbeat rounds.
    /// </summary>
    private sealed class GapWal : IRaftWalFacade
    {
        private readonly long committedThrough;
        private readonly long tailThrough;

        public long GapFrom { get; set; }
        public long GapTo { get; set; }

        public GapWal(long committedThrough, long gapFrom, long gapTo, long tailThrough)
        {
            this.committedThrough = committedThrough;
            this.tailThrough = tailThrough;
            GapFrom = gapFrom;
            GapTo = gapTo;
        }

        public ValueTask<IReadOnlyList<RaftLog>> LoadRestoreLogsAsync() =>
            ValueTask.FromResult<IReadOnlyList<RaftLog>>([]);
        public ValueTask CompleteRestoreAsync(IReadOnlyList<RaftLog> logs) => ValueTask.CompletedTask;
        public ValueTask<long> GetMaxLogAsync() => ValueTask.FromResult(tailThrough);
        public ValueTask<long> TruncateLogsAfterAsync(long afterLogId) => ValueTask.FromResult(afterLogId);
        public ValueTask<long> GetCurrentTermAsync() => ValueTask.FromResult(1L);

        public ValueTask<List<RaftLog>> GetRangeAsync(long startLogIndex, int maxEntries)
        {
            List<RaftLog> batch = [];
            for (long id = startLogIndex; id <= tailThrough && batch.Count < maxEntries; id++)
            {
                if (id >= GapFrom && id <= GapTo)
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

        public WALWriteOperation? EnqueueProposeOrCommit(List<RaftLog>? logs, HLCTimestamp timestamp = default, string? endpoint = null, long term = -1) =>
            logs is null ? null : MakeNoOp();

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
