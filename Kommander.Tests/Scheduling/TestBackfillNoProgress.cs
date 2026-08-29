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
/// A follower that acknowledges backfill batches without its commit frontier advancing must not
/// keep the leader in a read-and-ship loop.
///
/// <para>The producing state: a follower loses one lazy commit marker, so its gap-aware commit
/// frontier pins below the leader's monotonic matchIndex while its log holds the entries. Every
/// batch the leader anchors at nextIndex is a duplicate; the follower acks it with Success and
/// the same frontier; and on the single-fsync path each such ack funnels straight back into the
/// backfill sender. Unpaced, that ping-pong runs at network speed forever — one WAL range read
/// per iteration on the shared read scheduler, with zero writes, zero progress, and zero log
/// lines. A soaked cluster held ~800 MiB/s of pure WAL reads for over half an hour after its
/// workload stopped, and application reads starved behind them.</para>
///
/// <para>The defenses under test: fruitless ships pace themselves with an exponential pause, the
/// anchor falls back to the follower's reported frontier (which re-ships the entry whose marker
/// is missing), any frontier advance resets both, and a persistent episode logs exactly one
/// Warning.</para>
///
/// <para>The counting is evidence-gated, and that boundary is under test too: a ship counts as
/// fruitless only when a later Success ack reports a frontier at or below the one at ship time.
/// A ship the peer never answered proves nothing — a dead or restarting peer must not accrue a
/// pause it then serves on return, and the take-once anchored repairs must never be paced at all.
/// Counting silent ships starved meta-partition repair after restarts (the Jepsen
/// <c>snapshot / partition,kill</c> regression at Kommander 1.3.4: 12&#215; fewer backfill batches,
/// 8&#215; more log mismatches).</para>
/// </summary>
public class TestBackfillNoProgress
{
    private const string VoterA = "follower-a:9001";
    private const string VoterB = "follower-b:9002";

    // ── Fruitless acks stop producing WAL reads ──────────────────────────────

    /// <summary>
    /// The loop, minimized: acks reporting the same stuck frontier arrive back to back. The first
    /// ack may ship (both fast-path triggers fire), but once a ship is known fruitless, further
    /// acks inside the pause window must not read or ship anything.
    /// </summary>
    [Fact]
    public async Task RepeatedNoProgressAcks_ShipBoundedBatches()
    {
        (RaftPartitionStateMachine sm, CapturingHost host, _) =
            await BuildFullLogLeader(heartbeatInterval: TimeSpan.FromMinutes(5));
        HLCTimestamp ts = host.HybridLogicalClock.TrySendOrLocalEvent(1);

        await sm.CompleteAppendLogsAsync(VoterA, ts, RaftOperationStatus.Success, committedIndex: 50);
        int shippedAfterFirstAck = EntryBatchesTo(host, VoterA);
        Assert.True(shippedAfterFirstAck > 0);

        for (int i = 0; i < 5; i++)
            await sm.CompleteAppendLogsAsync(VoterA, ts, RaftOperationStatus.Success, committedIndex: 50);

        // Every further ack found the streak fruitless and the pause unexpired: no new batches.
        Assert.Equal(shippedAfterFirstAck, EntryBatchesTo(host, VoterA));
    }

    /// <summary>
    /// A frontier advance proves the peer is consuming: the probe resets and the next ack ships
    /// immediately instead of waiting out the previous streak's pause.
    /// </summary>
    [Fact]
    public async Task FrontierAdvance_ResetsThePause()
    {
        (RaftPartitionStateMachine sm, CapturingHost host, _) =
            await BuildFullLogLeader(heartbeatInterval: TimeSpan.FromMinutes(5));
        HLCTimestamp ts = host.HybridLogicalClock.TrySendOrLocalEvent(1);

        await sm.CompleteAppendLogsAsync(VoterA, ts, RaftOperationStatus.Success, committedIndex: 50);
        await sm.CompleteAppendLogsAsync(VoterA, ts, RaftOperationStatus.Success, committedIndex: 50);
        int shippedWhileStuck = EntryBatchesTo(host, VoterA);

        await sm.CompleteAppendLogsAsync(VoterA, ts, RaftOperationStatus.Success, committedIndex: 60);

        Assert.True(EntryBatchesTo(host, VoterA) > shippedWhileStuck,
            "an advancing frontier must lift the no-progress pause");
    }

    /// <summary>
    /// Peers are paced independently: one stuck follower must not delay batches to a healthy one.
    /// </summary>
    [Fact]
    public async Task Pacing_IsPerPeer()
    {
        (RaftPartitionStateMachine sm, CapturingHost host, _) =
            await BuildFullLogLeader(heartbeatInterval: TimeSpan.FromMinutes(5));
        HLCTimestamp ts = host.HybridLogicalClock.TrySendOrLocalEvent(1);

        for (int i = 0; i < 4; i++)
            await sm.CompleteAppendLogsAsync(VoterA, ts, RaftOperationStatus.Success, committedIndex: 50);
        Assert.True(EntryBatchesTo(host, VoterA) > 0);

        await sm.CompleteAppendLogsAsync(VoterB, ts, RaftOperationStatus.Success, committedIndex: 50);

        Assert.True(EntryBatchesTo(host, VoterB) > 0,
            "a healthy peer's first batch must not inherit another peer's pause");
    }

    /// <summary>
    /// Ships the peer never answered must not build a streak. The peer here acks once and then
    /// goes silent — the kill window of a restart-heavy fault profile. Every heartbeat still
    /// ships a batch: silence is not evidence that shipping failed to help, and a streak accrued
    /// against a dead peer was served as a capped pause the moment it restarted, which starved
    /// its repair across whole fault cycles.
    /// </summary>
    [Fact]
    public async Task ShipsWithoutAcks_DoNotBuildAStreak()
    {
        (RaftPartitionStateMachine sm, CapturingHost host, _) =
            await BuildFullLogLeader(heartbeatInterval: TimeSpan.FromMinutes(5));
        HLCTimestamp ts = host.HybridLogicalClock.TrySendOrLocalEvent(1);

        await sm.CompleteAppendLogsAsync(VoterA, ts, RaftOperationStatus.Success, committedIndex: 50);
        int shippedAfterFirstAck = EntryBatchesTo(host, VoterA);
        Assert.True(shippedAfterFirstAck > 0);

        // Three heartbeat rounds with no ack in between: each must ship one unpaced batch.
        for (int i = 0; i < 3; i++)
            await sm.ResumeHeartbeatsAsync(null);

        Assert.Equal(shippedAfterFirstAck + 3, EntryBatchesTo(host, VoterA));
    }

    /// <summary>
    /// The take-once anchored repairs must never be paced. A streak stands (one ship, one
    /// equal-frontier ack proving it fruitless), and then the peer rejects an append with
    /// LogMismatch — a restarted follower with a log hole answers every batch this way, because
    /// the over-gap ack gate withholds its Success acks. The next heartbeat's mismatch-anchored
    /// batch must ship through the standing pause: a paced-out attempt consumed the take-once
    /// note and shipped nothing, so the repair waited for the peer's next rejection AND the pause
    /// expiry, and the pause doubles.
    /// </summary>
    [Fact]
    public async Task MismatchAnchoredRepair_IsNotPacedByTheStreak()
    {
        (RaftPartitionStateMachine sm, CapturingHost host, _) =
            await BuildFullLogLeader(heartbeatInterval: TimeSpan.FromMinutes(5));
        HLCTimestamp ts = host.HybridLogicalClock.TrySendOrLocalEvent(1);

        await sm.CompleteAppendLogsAsync(VoterA, ts, RaftOperationStatus.Success, committedIndex: 50);
        await sm.CompleteAppendLogsAsync(VoterA, ts, RaftOperationStatus.Success, committedIndex: 50);
        int shippedWhileStuck = EntryBatchesTo(host, VoterA);

        await sm.CompleteAppendLogsAsync(VoterA, ts, RaftOperationStatus.LogMismatch, committedIndex: 200);
        await sm.ResumeHeartbeatsAsync(null);

        Assert.True(EntryBatchesTo(host, VoterA) > shippedWhileStuck,
            "the mismatch-anchored repair must ship through a standing no-progress pause");

        // The anchor is the mismatch note clamped to the reported frontier (50), not nextIndex.
        Assert.Contains(host.Requests, r => r.Node?.Endpoint == VoterA
            && r.AppendLogsRequest?.Logs is { Count: > 0 }
            && r.AppendLogsRequest.PrevLogIndex == 50);
    }

    // ── The anchor falls back to the reported frontier ───────────────────────

    /// <summary>
    /// The wedge's second half: matchIndex was pinned high by an overshooting report, so
    /// nextIndex anchors every batch above the entry the follower actually needs. After the
    /// configured number of fruitless ships the anchor must drop to the reported frontier,
    /// re-shipping the first uncommitted entry (and its commit marker) instead of duplicates.
    /// </summary>
    [Fact]
    public async Task FruitlessShipsAtNextIndex_ReanchorAtReportedFrontier()
    {
        // Zero heartbeat interval disables the pause so every ack ships and the fallback is
        // reached in a handful of acks.
        (RaftPartitionStateMachine sm, CapturingHost host, _) =
            await BuildFullLogLeader(heartbeatInterval: TimeSpan.Zero);
        HLCTimestamp ts = host.HybridLogicalClock.TrySendOrLocalEvent(1);

        // Pins matchIndex at 110 → nextIndex 111. The leader's frontier (500) is far above, so
        // the regression note's "was caught up" clause cannot arm and nothing else re-anchors.
        await sm.CompleteAppendLogsAsync(VoterA, ts, RaftOperationStatus.Success, committedIndex: 110);

        for (int i = 0; i < 3; i++)
            await sm.CompleteAppendLogsAsync(VoterA, ts, RaftOperationStatus.Success, committedIndex: 50);

        Assert.Contains(host.Requests, r => r.Node?.Endpoint == VoterA
            && r.AppendLogsRequest?.Logs is { Count: > 0 }
            && r.AppendLogsRequest.PrevLogIndex == 110);

        Assert.Contains(host.Requests, r => r.Node?.Endpoint == VoterA
            && r.AppendLogsRequest?.Logs is { Count: > 0 }
            && r.AppendLogsRequest.PrevLogIndex == 50);
    }

    // ── One Warning per episode ──────────────────────────────────────────────

    /// <summary>
    /// A persistent no-progress episode logs exactly one Warning however long it runs — the loop
    /// it replaces produced no evidence at all, and per-ship warnings would be the opposite
    /// failure.
    /// </summary>
    [Fact]
    public async Task NoProgressEpisode_WarnsExactlyOnce()
    {
        (RaftPartitionStateMachine sm, CapturingHost host, LevelCountingLogger logger) =
            await BuildFullLogLeader(heartbeatInterval: TimeSpan.Zero);
        HLCTimestamp ts = host.HybridLogicalClock.TrySendOrLocalEvent(1);

        for (int i = 0; i < 10; i++)
            await sm.CompleteAppendLogsAsync(VoterA, ts, RaftOperationStatus.Success, committedIndex: 50);

        Assert.Equal(1, logger.Count(LogLevel.Warning, "without its reported commit frontier advancing"));
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    /// <summary>
    /// Leader over a full, uncompacted log: committed entries 1..500, committed frontier 500,
    /// backfill threshold 10 — every anchored batch is contiguous and ships, which isolates the
    /// no-progress pacing from the refusal/escalation paths.
    /// </summary>
    private static async Task<(RaftPartitionStateMachine, CapturingHost, LevelCountingLogger)> BuildFullLogLeader(
        TimeSpan heartbeatInterval)
    {
        LevelCountingLogger logger = new();
        CapturingHost host = new();
        host.Configuration.HeartbeatInterval = heartbeatInterval;

        FullWal wal = new(tailThrough: 500);

        RaftPartitionStateMachine sm = new(host, wal, new NoopSink(), logger);
        IReadOnlyList<RaftLog> logs = await sm.StartRestoreAsync();
        await sm.CompleteRestoreAsync(logs);
        sm.SetPostToExecutor(_ => { });
        sm.SetLeaderForTesting(term: 2);
        sm.SetLocalCommittedIndexForTesting(500);

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
    /// WAL facade over a fully-present committed log 1..tailThrough: any anchored range read is
    /// contiguous, so batches always ship and only the sender's pacing decides whether a read
    /// happens.
    /// </summary>
    private sealed class FullWal : IRaftWalFacade
    {
        private readonly long tailThrough;

        public FullWal(long tailThrough) => this.tailThrough = tailThrough;

        public ValueTask<IReadOnlyList<RaftLog>> LoadRestoreLogsAsync() =>
            ValueTask.FromResult<IReadOnlyList<RaftLog>>([]);
        public ValueTask CompleteRestoreAsync(IReadOnlyList<RaftLog> logs) => ValueTask.CompletedTask;
        public ValueTask<long> GetMaxLogAsync() => ValueTask.FromResult(tailThrough);
        public ValueTask<long> TruncateLogsAfterAsync(long afterLogId) => ValueTask.FromResult(afterLogId);
        public ValueTask<long> GetCurrentTermAsync() => ValueTask.FromResult(1L);

        public ValueTask<List<RaftLog>> GetRangeAsync(long startLogIndex, int maxEntries)
        {
            List<RaftLog> batch = [];
            for (long id = Math.Max(startLogIndex, 1); id <= tailThrough && batch.Count < maxEntries; id++)
                batch.Add(new() { Id = id, Term = 1, Type = RaftLogType.Committed, LogType = "t" });

            return ValueTask.FromResult(batch);
        }

        public ValueTask<long> GetAnyTermAtAsync(long logIndex) => ValueTask.FromResult(1L);
        public ValueTask<long> GetLastCheckpointAsync() => ValueTask.FromResult(0L);
        public long GetCommitIndex() => tailThrough;
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
