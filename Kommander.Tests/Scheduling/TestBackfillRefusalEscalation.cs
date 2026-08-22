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
/// A voter pinned below the leader's compaction floor must be rescued from EVERY refusal path.
///
/// <para>The Caraxes two-hour soak wedged a healthy 3-voter cluster
/// permanently: a follower's progress reset to zero, the leader's backfill anchored at 1 below a
/// compaction floor near 2,000,000, and every batch was refused. The snapshot escalation lived only
/// on the heartbeat call site, so 24,253 refusals that arrived through the ack fast-path produced
/// zero snapshot attempts, and the partition never committed again. The escalation now lives inside
/// <c>BackfillSender.TrySendBackfillBatchAsync</c> itself — the single choke point every
/// entry-carrying batch passes through — so a refusal without an escalation is impossible.</para>
///
/// <para>The soak also produced 24,253 Warnings from 4 distinct refusal conditions, because episode
/// churn re-opened the same condition endlessly. Two guards pin that here: a contiguous batch
/// shipped ABOVE an episode's anchor no longer closes the episode, and a re-open of an
/// already-warned condition inside the cooldown window logs at Debug.</para>
/// </summary>
public class TestBackfillRefusalEscalation
{
    private const string VoterA = "follower-a:9001";
    private const string VoterB = "follower-b:9002";

    // ── The rescue fires from every refusal path ─────────────────────────────

    /// <summary>
    /// The Caraxes wedge, minimized: a Success ack reports a commit frontier of 0, the ack
    /// fast-path anchors backfill at 1, the WAL's first committed entry sits far above it, and the
    /// batch is refused. The refusal must escalate: with no snapshot transfer registered, the
    /// unproducible condition must be recorded and queryable. Before the fix the ack path discarded
    /// the refusal result and this stayed empty forever.
    /// </summary>
    [Fact]
    public async Task AckFastPathRefusal_EscalatesToSnapshot()
    {
        (RaftPartitionStateMachine sm, CapturingHost host, LevelCountingLogger logger) = await BuildCompactedLeader();

        await sm.CompleteAppendLogsAsync(VoterA, host.HybridLogicalClock.TrySendOrLocalEvent(1),
                                         RaftOperationStatus.Success, committedIndex: 0);

        Assert.Equal(0, EntryBatchesTo(host, VoterA));
        Assert.Equal(1, logger.Count(LogLevel.Warning, "Refusing non-contiguous backfill batch"));

        RaftSnapshotStatus status = Assert.Single(sm.GetSnapshotStatuses(), s => s.FollowerEndpoint == VoterA);
        Assert.True(status.Unproducible);
    }

    /// <summary>
    /// The same wedge with a snapshot transfer registered: the refusal must start an actual
    /// snapshot transfer to the pinned voter, not just record a condition. The transfer runs on a
    /// background task, so the assertion polls briefly.
    /// </summary>
    [Fact]
    public async Task AckFastPathRefusal_StartsSnapshotTransfer_WhenTransferRegistered()
    {
        (RaftPartitionStateMachine sm, CapturingHost host, _) = await BuildCompactedLeader();
        host.StateMachineTransferOverride = new MemoryTransfer();

        await sm.CompleteAppendLogsAsync(VoterA, host.HybridLogicalClock.TrySendOrLocalEvent(1),
                                         RaftOperationStatus.Success, committedIndex: 0);

        // The chunk send happens on a fire-and-forget task; wait bounded for it to land.
        for (int i = 0; i < 100 && host.SnapshotChunks.IsEmpty; i++)
            await Task.Delay(20, TestContext.Current.CancellationToken);

        SnapshotRequest chunk = host.SnapshotChunks.First(c => c.FollowerEndpoint == VoterA);
        Assert.Equal(86, chunk.SnapshotIndex);
    }

    /// <summary>
    /// Control: the heartbeat path keeps its escalation after the move into the sender. The
    /// threshold is raised above the leader's frontier so the ack fast-path stays quiet and only
    /// the heartbeat's idle-tail trigger can produce the refusal.
    /// </summary>
    [Fact]
    public async Task HeartbeatRefusal_EscalatesToSnapshot()
    {
        (RaftPartitionStateMachine sm, CapturingHost host, _) =
            await BuildCompactedLeader(backfillThreshold: 500, bootCommitIndex: 110);

        await sm.CompleteAppendLogsAsync(VoterA, host.HybridLogicalClock.TrySendOrLocalEvent(1),
                                         RaftOperationStatus.Success, committedIndex: 0);
        Assert.Empty(sm.GetSnapshotStatuses());

        await sm.CheckPartitionLeadershipAsync();

        RaftSnapshotStatus status = Assert.Single(sm.GetSnapshotStatuses(), s => s.FollowerEndpoint == VoterA);
        Assert.True(status.Unproducible);
    }

    // ── The refusal episode does not churn ───────────────────────────────────

    /// <summary>
    /// The two backfill anchors diverge on a pinned peer: <c>nextIndex</c> tracks the monotonic
    /// matchIndex while the refusal anchors at the reported frontier. A contiguous batch shipped
    /// from the high anchor proves nothing about the pinned low anchor, so it must NOT close the
    /// episode — and the next low-anchored refusal must count as a repeat, not open a fresh episode
    /// with a fresh Warning. Before the fix this exact alternation cleared and re-opened the
    /// episode on every cycle, which is the Caraxes 24,253-warning storm.
    /// </summary>
    [Fact]
    public async Task PinnedEpisode_SurvivesHigherAnchoredShip_AndWarnsOnce()
    {
        (RaftPartitionStateMachine sm, CapturingHost host, LevelCountingLogger logger) = await BuildCompactedLeader();
        HLCTimestamp ts = host.HybridLogicalClock.TrySendOrLocalEvent(1);

        // 1. Frontier-0 ack: the fast-path refuses at anchor 1 and opens the episode.
        await sm.CompleteAppendLogsAsync(VoterA, ts, RaftOperationStatus.Success, committedIndex: 0);
        Assert.Equal(1, logger.Count(LogLevel.Warning, "Refusing non-contiguous backfill batch"));

        // 2. A near-caught-up ack raises matchIndex to 110, so nextIndex points high (111) while
        //    the refusal condition below stays anchored at 1 — the diverged-anchor state.
        await sm.CompleteAppendLogsAsync(VoterA, ts, RaftOperationStatus.Success, committedIndex: 110);

        // 3. The frontier regresses to 0. The fast-path re-supply ships a contiguous batch from
        //    the HIGH nextIndex anchor (111) — the ship that used to close the pinned episode —
        //    and records the regression note for the heartbeat.
        await sm.CompleteAppendLogsAsync(VoterA, ts, RaftOperationStatus.Success, committedIndex: 0);
        Assert.True(EntryBatchesTo(host, VoterA) > 0);
        RaftBackfillStatus open = Assert.Single(sm.GetBackfillStatuses(), s => s.FollowerEndpoint == VoterA);
        Assert.Equal(1, open.AnchorIndex);

        // 4. The heartbeat acts on the regression note, anchors at the reported frontier (1), and
        //    refuses again. Same condition: a repeat of the surviving episode, no new Warning.
        await sm.CheckPartitionLeadershipAsync();

        Assert.Equal(1, logger.Count(LogLevel.Warning, "Refusing non-contiguous backfill batch"));
        RaftBackfillStatus status = Assert.Single(sm.GetBackfillStatuses(), s => s.FollowerEndpoint == VoterA);
        Assert.Equal(1, status.AnchorIndex);
        Assert.True(status.Occurrences >= 2);
    }

    /// <summary>
    /// The memoized refusal escalates for every follower anchored at the refused index, not only
    /// for the one whose read populated the round cache — each pinned peer needs its own rescue.
    /// </summary>
    [Fact]
    public async Task RoundCachedRefusal_EscalatesForEveryFollower()
    {
        (RaftPartitionStateMachine sm, CapturingHost host, _) =
            await BuildCompactedLeader(backfillThreshold: 500, bootCommitIndex: 110);
        HLCTimestamp ts = host.HybridLogicalClock.TrySendOrLocalEvent(1);

        await sm.CompleteAppendLogsAsync(VoterA, ts, RaftOperationStatus.Success, committedIndex: 0);
        await sm.CompleteAppendLogsAsync(VoterB, ts, RaftOperationStatus.Success, committedIndex: 0);

        await sm.CheckPartitionLeadershipAsync();

        Assert.Contains(sm.GetSnapshotStatuses(), s => s.FollowerEndpoint == VoterA);
        Assert.Contains(sm.GetSnapshotStatuses(), s => s.FollowerEndpoint == VoterB);
    }

    // ── A no-report ack must not fabricate progress ──────────────────────────

    /// <summary>
    /// The "anchored at 1" producer, minimized (Caraxes run E, task 1). The legacy two-fsync
    /// heartbeat ack reports committedIndex = -1: "no frontier report", not a position. Right
    /// after an election win the optimistic seed holds matchIndex = 0; feeding the -1 through the
    /// progress math left newMatchIndex at 0, set nextIndex to 1, and the eager fast path shipped
    /// a backfill anchored at 1 — refused below the compaction floor and escalated to a full
    /// snapshot transfer for a peer that was in sync. A no-report ack must leave progress and the
    /// backfill triggers untouched.
    /// </summary>
    [Fact]
    public async Task NoReportAck_AfterElectionSeed_DoesNotAnchorBackfillAtOne()
    {
        (RaftPartitionStateMachine sm, CapturingHost host, LevelCountingLogger logger) = await BuildCompactedLeader();
        sm.SeedOptimisticProgressForTesting(VoterA, 117);

        await sm.CompleteAppendLogsAsync(VoterA, host.HybridLogicalClock.TrySendOrLocalEvent(1),
                                         RaftOperationStatus.Success, committedIndex: -1);

        Assert.Equal(0, EntryBatchesTo(host, VoterA));
        Assert.Equal(0, logger.Count(LogLevel.Warning, "Refusing non-contiguous backfill batch"));
        Assert.Empty(sm.GetSnapshotStatuses());
    }

    /// <summary>
    /// The heartbeat half of the same defect: the -1 seed recorded for a contacted, never-reported
    /// peer is "position unknown", not "position 0". The round must anchor at the best positional
    /// evidence the leader holds — where the peer's log started per its handshake/vote — and ship
    /// a contiguous batch from there, not refuse an anchor-0 read on every beat.
    /// </summary>
    [Fact]
    public async Task NoReportAck_ThenHeartbeat_BackfillsFromKnownLogStart()
    {
        (RaftPartitionStateMachine sm, CapturingHost host, LevelCountingLogger logger) = await BuildCompactedLeader();
        sm.SetStartCommitIndexForTesting(VoterA, 100);

        await sm.CompleteAppendLogsAsync(VoterA, host.HybridLogicalClock.TrySendOrLocalEvent(1),
                                         RaftOperationStatus.Success, committedIndex: -1);
        Assert.Equal(0, EntryBatchesTo(host, VoterA));

        await sm.CheckPartitionLeadershipAsync();

        Assert.True(EntryBatchesTo(host, VoterA) > 0);
        Assert.Equal(0, logger.Count(LogLevel.Warning, "Refusing non-contiguous backfill batch"));

        RaftResponderRequest batch = host.Requests.First(r =>
            r.Node?.Endpoint == VoterA && r.AppendLogsRequest?.Logs is { Count: > 0 });
        Assert.Equal(100, batch.AppendLogsRequest!.PrevLogIndex);
    }

    /// <summary>
    /// A contacted peer with no frontier report AND no positional evidence at all is a blank
    /// follower: the round anchors at 1, the compacted WAL refuses, and the refusal escalates into
    /// the snapshot rescue — the correct outcome for a peer that provably holds nothing.
    /// </summary>
    [Fact]
    public async Task NoReportAck_UnknownPeer_IsRescuedBySnapshotFromAnchorOne()
    {
        (RaftPartitionStateMachine sm, CapturingHost host, _) = await BuildCompactedLeader();

        await sm.CompleteAppendLogsAsync(VoterA, host.HybridLogicalClock.TrySendOrLocalEvent(1),
                                         RaftOperationStatus.Success, committedIndex: -1);
        Assert.Equal(0, EntryBatchesTo(host, VoterA));

        await sm.CheckPartitionLeadershipAsync();

        RaftBackfillStatus status = Assert.Single(sm.GetBackfillStatuses(), s => s.FollowerEndpoint == VoterA);
        Assert.Equal(1, status.AnchorIndex);
        Assert.Contains(sm.GetSnapshotStatuses(), s => s.FollowerEndpoint == VoterA);
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    /// <summary>
    /// Leader over a compacted WAL: committed entries exist only from 87 through 117, the last
    /// checkpoint sits at 86, and the leader's committed frontier is 117. Any backfill anchored at
    /// or below 86 is non-contiguous and can never be served from the log.
    /// </summary>
    private static async Task<(RaftPartitionStateMachine, CapturingHost, LevelCountingLogger)> BuildCompactedLeader(
        int backfillThreshold = 10, long bootCommitIndex = 117)
    {
        LevelCountingLogger logger = new();
        CapturingHost host = new();
        host.Configuration.BackfillThreshold = backfillThreshold;

        CompactedWal wal = new(firstAvailable: 87, tailThrough: 117, lastCheckpoint: 86,
                               bootCommitIndex: bootCommitIndex);

        RaftPartitionStateMachine sm = new(host, wal, new NoopSink(), logger);
        IReadOnlyList<RaftLog> logs = await sm.StartRestoreAsync();
        await sm.CompleteRestoreAsync(logs);
        sm.SetPostToExecutor(_ => { });
        sm.SetLeaderForTesting(term: 2);
        sm.SetLocalCommittedIndexForTesting(117);

        host.Requests.Clear();
        logger.Reset();
        return (sm, host, logger);
    }

    private static int EntryBatchesTo(CapturingHost host, string endpoint) =>
        host.Requests.Count(r => r.Node?.Endpoint == endpoint
                                 && r.AppendLogsRequest?.Logs is { Count: > 0 });

    // ── stubs ────────────────────────────────────────────────────────────────

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
    /// WAL facade for a log whose prefix was compacted away: committed entries exist only in
    /// [firstAvailable, tailThrough], and <see cref="GetLastCheckpointAsync"/> reports the floor.
    /// A range read anchored below <c>firstAvailable</c> comes back starting above the anchor,
    /// which is exactly the refused shape.
    /// </summary>
    private sealed class CompactedWal : IRaftWalFacade
    {
        private readonly long firstAvailable;
        private readonly long tailThrough;
        private readonly long lastCheckpoint;
        private readonly long bootCommitIndex;

        public CompactedWal(long firstAvailable, long tailThrough, long lastCheckpoint, long bootCommitIndex)
        {
            this.firstAvailable = firstAvailable;
            this.tailThrough = tailThrough;
            this.lastCheckpoint = lastCheckpoint;
            this.bootCommitIndex = bootCommitIndex;
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
            for (long id = Math.Max(startLogIndex, firstAvailable); id <= tailThrough && batch.Count < maxEntries; id++)
                batch.Add(new() { Id = id, Term = 1, Type = RaftLogType.Committed, LogType = "t" });

            return ValueTask.FromResult(batch);
        }

        public ValueTask<long> GetAnyTermAtAsync(long logIndex) => ValueTask.FromResult(1L);
        public ValueTask<long> GetLastCheckpointAsync() => ValueTask.FromResult(lastCheckpoint);
        public long GetCommitIndex() => bootCommitIndex;
        public WALWriteOperation EnqueuePropose(long term, List<RaftLog> logs, HLCTimestamp ts, bool autoCommit) => MakeNoOp();
        public WALWriteOperation EnqueueCommit(List<RaftLog> logs) => MakeNoOp();
        public WALWriteOperation EnqueueRollback(List<RaftLog> logs) => MakeNoOp();

        public WALWriteOperation? EnqueueProposeOrCommit(List<RaftLog>? logs, HLCTimestamp timestamp = default, string? endpoint = null, long term = -1) =>
            logs is null ? null : MakeNoOp();

        public void NotifyCommitted() { }

        private static WALWriteOperation MakeNoOp() =>
            new(_ => { }, 0, WALWriteOperationType.LeaderPropose, (0, []));
    }

    /// <summary>Serves a tiny in-memory snapshot so the transfer path can complete end to end.</summary>
    private sealed class MemoryTransfer : IRaftStateMachineTransfer
    {
        public Task<Stream> ExportRange(RaftSplitPlan plan, long upToIndex, CancellationToken ct) =>
            Task.FromResult<Stream>(new MemoryStream([1, 2, 3]));

        public Task ImportRange(int targetPartitionId, Stream snapshot, CancellationToken ct) =>
            Task.CompletedTask;
    }

    private sealed class CapturingHost : IRaftPartitionHost
    {
        public ConcurrentBag<RaftResponderRequest> Requests { get; } = [];
        public ConcurrentBag<SnapshotRequest> SnapshotChunks { get; } = [];
        public IRaftStateMachineTransfer? StateMachineTransferOverride { get; set; }

        public int PartitionId => 1;
        public string Leader { get; set; } = "";
        public string LocalEndpoint => "leader:9000";
        public int LocalNodeId => 1;
        public ClusterMemberRole LocalRole => ClusterMemberRole.Voter;
        public bool IsVoter(string endpoint) => true;

        public RaftConfiguration Configuration { get; } = new()
        {
            Host = "leader", Port = 9000, InitialPartitions = 1, BackfillThreshold = 10,
            HeartbeatInterval = TimeSpan.Zero,
        };

        public HybridLogicalClock HybridLogicalClock { get; } = new();
        public IReadOnlyList<RaftNode> Nodes { get; set; } = [new(VoterA), new(VoterB)];
        public MemberLivenessState GetNodeLiveness(string endpoint) => MemberLivenessState.Alive;

        public HLCTimestamp GetLastNodeActivity(string e, int p) => HLCTimestamp.Zero;
        public HLCTimestamp GetLastNodeHearthbeat(string e, int p) => HLCTimestamp.Zero;
        public void UpdateLastHeartbeat(string e, int p, HLCTimestamp t) { }
        public void UpdateLastNodeActivity(string e, int p, HLCTimestamp t) { }
        public void EnqueueResponse(string e, RaftResponderRequest r) => Requests.Add(r);
        public Task InvokeLeaderChanged(int p, string l) => Task.CompletedTask;
        public Task<bool> InvokeReplicationReceived(int p, RaftLog l) => Task.FromResult(true);
        public Task<bool> InvokeSystemReplicationReceived(int p, RaftLog l) => Task.FromResult(true);
        public void InvokeReplicationError(int p, RaftLog l) { }

        public IRaftStateMachineTransfer? StateMachineTransfer => StateMachineTransferOverride;
        public IRaftSystemStateTransfer? SystemStateTransfer => null;

        public Task<SnapshotResponse> SendInstallSnapshotAsync(RaftNode node, SnapshotRequest request, CancellationToken ct)
        {
            SnapshotChunks.Add(request);
            return Task.FromResult(new SnapshotResponse(true));
        }
    }

    private sealed class NoopSink : IRaftOperationReplySink
    {
        public void TryComplete(ulong correlationId, RaftResponse response) { }
    }
}
