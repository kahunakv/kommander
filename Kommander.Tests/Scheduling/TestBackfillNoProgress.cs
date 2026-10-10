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

        Assert.Equal(1, logger.Count(LogLevel.Warning, "without any of its reported frontiers advancing"));
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    /// <summary>
    /// Leader over a full, uncompacted log: committed entries 1..500, committed frontier 500,
    /// backfill threshold 10 — every anchored batch is contiguous and ships, which isolates the
    /// no-progress pacing from the refusal/escalation paths.
    /// </summary>
    [Fact]
    public async Task NoProgressEpisode_EscalatesToASnapshot()
    {
        // The anchor fallback re-anchored at the frontier the peer itself reported and the batch
        // still produced no advance: log shipping cannot converge this peer, so once the streak
        // reaches the warning threshold the peer is offered a snapshot from the last checkpoint.
        // (2026-09-19: a new leader whose WAL still served the stuck learner's anchor never
        // refused a batch, so the refusal-driven escalation never ran and the learner stayed at
        // frontier 0 for the run.)
        (RaftPartitionStateMachine sm, CapturingHost host, LevelCountingLogger logger) =
            await BuildFullLogLeader(heartbeatInterval: TimeSpan.Zero, checkpoint: 100, transfer: new InstantTransfer());
        HLCTimestamp ts = host.HybridLogicalClock.TrySendOrLocalEvent(1);

        for (int i = 0; i < 10; i++)
            await sm.CompleteAppendLogsAsync(VoterA, ts, RaftOperationStatus.Success, committedIndex: 50);

        TimeSpan budget = TestTimeouts.Scale(TimeSpan.FromSeconds(5));
        long started = global::System.Diagnostics.Stopwatch.GetTimestamp();
        while (host.SnapshotChunksTo(VoterA) == 0)
        {
            if (global::System.Diagnostics.Stopwatch.GetElapsedTime(started) > budget)
                Assert.Fail("the no-progress episode never escalated to a snapshot transfer");
            await Task.Delay(10, TestContext.Current.CancellationToken);
        }

        Assert.Equal(1, logger.Count(LogLevel.Warning, "offered a snapshot"));
        Assert.Equal(0, host.SnapshotChunksTo(VoterB)); // the healthy peer is not touched

        // The transfer-start line names the trigger and the numbers behind it. It used to assert
        // "sits below the WAL compaction floor" for every path, which on the rl6 soak was false
        // three times per arm and sent the first reading toward retention.
        Assert.Equal(1, logger.Count(LogLevel.Warning, "triggered by NoProgressProbe: 4 consecutive batches (last anchor"));
        Assert.Equal(1, logger.Count(LogLevel.Warning, "leader checkpoint 100"));
        Assert.True(logger.Count(LogLevel.Warning, "peer commit 50") >= 1);
        Assert.Equal(0, logger.Count(LogLevel.Warning, "below the WAL compaction floor"));
    }

    // ── Slow is not stuck ────────────────────────────────────────────────────

    /// <summary>
    /// The rl6 shape (CamusDB fault soak, 2026-10-10): a voter resuming from a 30-s pause closes a
    /// log hole while the live stream keeps landing above it. Its commit frontier is pinned under
    /// the hole for as long as the hole is open, but every batch anchored at the hole advances its
    /// contiguous presence frontier. The probe escalated after four such ships and re-seeded the
    /// follower three times. A presence advance through the shipped range is progress: no streak,
    /// no Warning, no snapshot.
    /// </summary>
    [Fact]
    public async Task PresenceAdvanceThroughTheShippedRange_IsProgress_AndNeverEscalates()
    {
        (RaftPartitionStateMachine sm, CapturingHost host, LevelCountingLogger logger) =
            await BuildFullLogLeader(heartbeatInterval: TimeSpan.Zero, checkpoint: 100, transfer: new InstantTransfer());
        HLCTimestamp ts = host.HybridLogicalClock.TrySendOrLocalEvent(1);

        // Commit frontier pinned at 50 under the hole. Each round is what the rl6 follower sent:
        // Success acks (heartbeats) reporting the pinned frontier and its presence frontier, and the
        // hole report — a LogMismatch anchored at the presence frontier — which the next heartbeat
        // answers with a batch anchored there (VerifiedPresenceAnchorAsync). The ack fast path
        // meanwhile keeps shipping duplicates at nextIndex = 51, which can extend nothing. Each
        // round the presence frontier has moved past the anchored batch's anchor: that ship is
        // credited, the streak resets, and the duplicates never reach the threshold.
        long present = 50;
        for (int round = 0; round < 5; round++)
        {
            for (int ack = 0; ack < 3; ack++)
                await sm.CompleteAppendLogsAsync(VoterA, ts, RaftOperationStatus.Success, committedIndex: 50, presentIndex: present, presentTerm: 1);

            await sm.CompleteAppendLogsAsync(VoterA, ts, RaftOperationStatus.LogMismatch, committedIndex: present, presentIndex: present, presentTerm: 1);
            await sm.ResumeHeartbeatsAsync(null);

            Assert.Contains(host.Requests, r => r.Node?.Endpoint == VoterA
                && r.AppendLogsRequest?.Logs is { Count: > 0 }
                && r.AppendLogsRequest.PrevLogIndex == present);

            present += 100;
        }

        await Task.Delay(100, TestContext.Current.CancellationToken);
        Assert.Equal(0, host.SnapshotChunksTo(VoterA));
        Assert.Empty(sm.GetSnapshotStatuses());
        Assert.Equal(0, logger.Count(LogLevel.Warning, "without any of its reported frontiers advancing"));
    }

    /// <summary>
    /// The wedge the probe exists for, under load: a follower that lost a commit marker holds the
    /// live stream contiguously above its pinned commit frontier, so its presence frontier climbs
    /// with the leader's head while every batch anchored at the frontier is a duplicate the batch
    /// can never extend. That presence advance is NOT progress: the streak builds, the episode warns
    /// once, and the peer is offered a snapshot — exactly as before.
    /// </summary>
    [Fact]
    public async Task PresenceAdvanceAboveTheShippedRange_IsNotProgress_AndStillEscalates()
    {
        (RaftPartitionStateMachine sm, CapturingHost host, LevelCountingLogger logger) =
            await BuildFullLogLeader(heartbeatInterval: TimeSpan.Zero, checkpoint: 100, transfer: new InstantTransfer());
        HLCTimestamp ts = host.HybridLogicalClock.TrySendOrLocalEvent(1);

        // Batches anchored at 51 reach 178; the presence frontier reported is always above that.
        long present = 400;
        for (int i = 0; i < 10; i++)
        {
            await sm.CompleteAppendLogsAsync(VoterA, ts, RaftOperationStatus.Success, committedIndex: 50, presentIndex: present, presentTerm: 1);
            present += 10;
        }

        TimeSpan budget = TestTimeouts.Scale(TimeSpan.FromSeconds(5));
        long started = global::System.Diagnostics.Stopwatch.GetTimestamp();
        while (host.SnapshotChunksTo(VoterA) == 0)
        {
            if (global::System.Diagnostics.Stopwatch.GetElapsedTime(started) > budget)
                Assert.Fail("a presence frontier climbing above the shipped range must not be read as progress");
            await Task.Delay(10, TestContext.Current.CancellationToken);
        }

        Assert.Equal(1, logger.Count(LogLevel.Warning, "offered a snapshot"));
    }

    /// <summary>
    /// The durable frontier is the durable contiguous commit frontier: a peer whose disk keeps
    /// answering for more of its log is converging even while its in-memory commit report is
    /// stale. An advance in it is progress on its own.
    /// </summary>
    [Fact]
    public async Task DurableFrontierAdvance_IsProgress_AndNeverEscalates()
    {
        (RaftPartitionStateMachine sm, CapturingHost host, LevelCountingLogger logger) =
            await BuildFullLogLeader(heartbeatInterval: TimeSpan.Zero, checkpoint: 100, transfer: new InstantTransfer());
        HLCTimestamp ts = host.HybridLogicalClock.TrySendOrLocalEvent(1);

        long durable = 40;
        for (int i = 0; i < 12; i++)
        {
            await sm.CompleteAppendLogsAsync(VoterA, ts, RaftOperationStatus.Success, committedIndex: 50, durableIndex: durable);
            durable += 5;
        }

        await Task.Delay(100, TestContext.Current.CancellationToken);
        Assert.Equal(0, host.SnapshotChunksTo(VoterA));
        Assert.Equal(0, logger.Count(LogLevel.Warning, "without any of its reported frontiers advancing"));
    }

    /// <summary>
    /// A durable frontier that stands still proves nothing either way: the streak is still judged
    /// on the commit frontier, and the stuck peer still escalates.
    /// </summary>
    [Fact]
    public async Task FlatDurableFrontier_DoesNotMaskAStuckPeer()
    {
        (RaftPartitionStateMachine sm, CapturingHost host, _) =
            await BuildFullLogLeader(heartbeatInterval: TimeSpan.Zero, checkpoint: 100, transfer: new InstantTransfer());
        HLCTimestamp ts = host.HybridLogicalClock.TrySendOrLocalEvent(1);

        for (int i = 0; i < 10; i++)
            await sm.CompleteAppendLogsAsync(VoterA, ts, RaftOperationStatus.Success, committedIndex: 50, durableIndex: 50);

        TimeSpan budget = TestTimeouts.Scale(TimeSpan.FromSeconds(5));
        long started = global::System.Diagnostics.Stopwatch.GetTimestamp();
        while (host.SnapshotChunksTo(VoterA) == 0)
        {
            if (global::System.Diagnostics.Stopwatch.GetElapsedTime(started) > budget)
                Assert.Fail("a peer whose every frontier is flat must still be offered a snapshot");
            await Task.Delay(10, TestContext.Current.CancellationToken);
        }
    }

    // ── A seeded follower gets a grace ───────────────────────────────────────

    /// <summary>
    /// After an install the follower is behind by everything committed during the transfer, and
    /// closing that from the log can leave its frontiers flat for several ships. A second
    /// escalation inside that window can only repeat the install (rl6: the second escalation came
    /// five seconds after the first install landed). The probe still paces and still warns, but the
    /// snapshot is deferred.
    /// </summary>
    [Fact]
    public async Task SeededFollower_IsNotReEscalatedInsideItsGrace()
    {
        (RaftPartitionStateMachine sm, CapturingHost host, LevelCountingLogger logger) =
            await BuildFullLogLeader(heartbeatInterval: TimeSpan.Zero, checkpoint: 100, transfer: new InstantTransfer());
        HLCTimestamp ts = host.HybridLogicalClock.TrySendOrLocalEvent(1);

        // The install landed at 60 with the leader at 500: a 440-entry lag handed to the follower,
        // which is still below the checkpoint (100) the next export would be taken at.
        sm.CompleteSnapshotInstalled(VoterA, 60);

        for (int i = 0; i < 10; i++)
            await sm.CompleteAppendLogsAsync(VoterA, ts, RaftOperationStatus.Success, committedIndex: 60);

        await Task.Delay(100, TestContext.Current.CancellationToken);
        Assert.Equal(0, host.SnapshotChunksTo(VoterA));
        Assert.Equal(1, logger.Count(LogLevel.Warning, "a further snapshot is deferred"));
        Assert.Equal(0, logger.Count(LogLevel.Warning, "offered a snapshot"));
        Assert.True(EntryBatchesTo(host, VoterA) > 0, "the seeded peer is still backfilled");
    }

    /// <summary>
    /// The grace is bounded: once it lapses, a peer whose frontiers are still flat is offered the
    /// snapshot. Here the leader committed nothing since the install, so the catch-up estimate is
    /// the cap, and the clock is moved past the cap plus the pause cap.
    /// </summary>
    [Fact]
    public async Task SeededFollower_IsEscalatedOnceItsGraceLapses()
    {
        (RaftPartitionStateMachine sm, CapturingHost host, _) =
            await BuildFullLogLeader(heartbeatInterval: TimeSpan.Zero, checkpoint: 100, transfer: new InstantTransfer());
        HLCTimestamp ts = host.HybridLogicalClock.TrySendOrLocalEvent(1);

        host.MonotonicTicks = global::System.Diagnostics.Stopwatch.GetTimestamp();
        sm.CompleteSnapshotInstalled(VoterA, 60);

        TimeSpan grace = host.Configuration.BackfillSeededCatchUpGraceCap + host.Configuration.BackfillNoProgressPauseCap;
        host.MonotonicTicks += (long)(grace.TotalSeconds * global::System.Diagnostics.Stopwatch.Frequency) + global::System.Diagnostics.Stopwatch.Frequency;

        for (int i = 0; i < 10; i++)
            await sm.CompleteAppendLogsAsync(VoterA, ts, RaftOperationStatus.Success, committedIndex: 60);

        TimeSpan budget = TestTimeouts.Scale(TimeSpan.FromSeconds(5));
        long started = global::System.Diagnostics.Stopwatch.GetTimestamp();
        while (host.SnapshotChunksTo(VoterA) == 0)
        {
            if (global::System.Diagnostics.Stopwatch.GetElapsedTime(started) > budget)
                Assert.Fail("a seeded peer still stuck after its grace must be offered a snapshot");
            await Task.Delay(10, TestContext.Current.CancellationToken);
        }
    }

    /// <summary>
    /// The grace scales with the lag the install handed the follower and the commit rate the
    /// leader has sustained since: a lag the follower can close in under a second at that rate
    /// adds under a second to the pause cap, and once that has passed the stuck peer escalates —
    /// well inside the 3-minute cap a rate that could not be measured would have earned it.
    /// </summary>
    [Fact]
    public async Task SeededFollower_GraceIsSizedByTheLagAndTheCommitRate()
    {
        (RaftPartitionStateMachine sm, CapturingHost host, _) =
            await BuildFullLogLeader(heartbeatInterval: TimeSpan.Zero, checkpoint: 100, transfer: new InstantTransfer());
        HLCTimestamp ts = host.HybridLogicalClock.TrySendOrLocalEvent(1);

        long frequency = global::System.Diagnostics.Stopwatch.Frequency;
        host.MonotonicTicks = global::System.Diagnostics.Stopwatch.GetTimestamp();
        host.Configuration.BackfillNoProgressPauseCap = TimeSpan.FromSeconds(2);

        // Installed at 60 with the leader at 500: a 440-entry lag.
        sm.CompleteSnapshotInstalled(VoterA, 60);

        // Three seconds later the leader is 2,000 entries further on (667 entries/s): the follower
        // needs 0.66 s for its lag, so the grace is the 2-s pause cap plus 0.66 s — already lapsed.
        host.MonotonicTicks += 3 * frequency;
        sm.SetLocalCommittedIndexForTesting(2500);

        for (int i = 0; i < 10; i++)
            await sm.CompleteAppendLogsAsync(VoterA, ts, RaftOperationStatus.Success, committedIndex: 60);

        TimeSpan budget = TestTimeouts.Scale(TimeSpan.FromSeconds(5));
        long started = global::System.Diagnostics.Stopwatch.GetTimestamp();
        while (host.SnapshotChunksTo(VoterA) == 0)
        {
            if (global::System.Diagnostics.Stopwatch.GetElapsedTime(started) > budget)
                Assert.Fail("a grace sized by a small lag must have lapsed");
            await Task.Delay(10, TestContext.Current.CancellationToken);
        }
    }

    /// <summary>A zero cap disables the grace: a seeded peer escalates like any other.</summary>
    [Fact]
    public async Task SeededFollower_WithTheGraceDisabled_EscalatesAtOnce()
    {
        (RaftPartitionStateMachine sm, CapturingHost host, _) =
            await BuildFullLogLeader(heartbeatInterval: TimeSpan.Zero, checkpoint: 100, transfer: new InstantTransfer());
        HLCTimestamp ts = host.HybridLogicalClock.TrySendOrLocalEvent(1);

        host.Configuration.BackfillSeededCatchUpGraceCap = TimeSpan.Zero;
        sm.CompleteSnapshotInstalled(VoterA, 60);

        for (int i = 0; i < 10; i++)
            await sm.CompleteAppendLogsAsync(VoterA, ts, RaftOperationStatus.Success, committedIndex: 60);

        TimeSpan budget = TestTimeouts.Scale(TimeSpan.FromSeconds(5));
        long started = global::System.Diagnostics.Stopwatch.GetTimestamp();
        while (host.SnapshotChunksTo(VoterA) == 0)
        {
            if (global::System.Diagnostics.Stopwatch.GetElapsedTime(started) > budget)
                Assert.Fail("with the grace disabled the seeded peer must be offered a snapshot");
            await Task.Delay(10, TestContext.Current.CancellationToken);
        }
    }

    [Fact]
    public async Task NoProgressEpisode_PeerAlreadyPastTheCheckpoint_IsNotSentASnapshot()
    {
        // The streak builds exactly as above — a follower busy delivering a large backfill stops
        // acknowledging progress — but the peer's own reports already reach past the checkpoint. A
        // snapshot there carries nothing it lacks: the transfer would only cost a whole-partition
        // export and an install the receiver must recognise as redundant. Log shipping stays the
        // repair, and the episode is still reported.
        (RaftPartitionStateMachine sm, CapturingHost host, LevelCountingLogger logger) =
            await BuildFullLogLeader(heartbeatInterval: TimeSpan.Zero, checkpoint: 100, transfer: new InstantTransfer());
        HLCTimestamp ts = host.HybridLogicalClock.TrySendOrLocalEvent(1);

        for (int i = 0; i < 10; i++)
            await sm.CompleteAppendLogsAsync(VoterA, ts, RaftOperationStatus.Success, committedIndex: 150, durableIndex: 150);

        await Task.Delay(100, TestContext.Current.CancellationToken);
        Assert.Equal(0, host.SnapshotChunksTo(VoterA));
        Assert.Empty(sm.GetSnapshotStatuses());
        Assert.True(EntryBatchesTo(host, VoterA) > 0);
        Assert.Equal(1, logger.Count(LogLevel.Warning, "without any of its reported frontiers advancing"));
    }

    [Fact]
    public async Task NoProgressEpisode_CommittedPastTheCheckpointButNotDurablyHeld_StillEscalates()
    {
        // The peer's commit frontier passes the checkpoint but its durable frontier does not: it does
        // not attest to holding the range, so the snapshot remains the rescue.
        (RaftPartitionStateMachine sm, CapturingHost host, _) =
            await BuildFullLogLeader(heartbeatInterval: TimeSpan.Zero, checkpoint: 100, transfer: new InstantTransfer());
        HLCTimestamp ts = host.HybridLogicalClock.TrySendOrLocalEvent(1);

        for (int i = 0; i < 10; i++)
            await sm.CompleteAppendLogsAsync(VoterA, ts, RaftOperationStatus.Success, committedIndex: 150, durableIndex: 80);

        TimeSpan budget = TestTimeouts.Scale(TimeSpan.FromSeconds(5));
        long started = global::System.Diagnostics.Stopwatch.GetTimestamp();
        while (host.SnapshotChunksTo(VoterA) == 0)
        {
            if (global::System.Diagnostics.Stopwatch.GetElapsedTime(started) > budget)
                Assert.Fail("a peer that is not durably past the checkpoint was never offered a snapshot");
            await Task.Delay(10, TestContext.Current.CancellationToken);
        }
    }

    [Fact]
    public async Task NoProgressEpisode_WithoutACheckpoint_DoesNotEscalate()
    {
        // No checkpoint means no consistent boundary to export from: the pacing and the
        // re-anchoring still apply, but nothing is shipped as a snapshot.
        (RaftPartitionStateMachine sm, CapturingHost host, _) =
            await BuildFullLogLeader(heartbeatInterval: TimeSpan.Zero, checkpoint: 0, transfer: new InstantTransfer());
        HLCTimestamp ts = host.HybridLogicalClock.TrySendOrLocalEvent(1);

        for (int i = 0; i < 10; i++)
            await sm.CompleteAppendLogsAsync(VoterA, ts, RaftOperationStatus.Success, committedIndex: 50);

        await Task.Delay(100, TestContext.Current.CancellationToken);
        Assert.Equal(0, host.SnapshotChunksTo(VoterA));
    }

    private static async Task<(RaftPartitionStateMachine, CapturingHost, LevelCountingLogger)> BuildFullLogLeader(
        TimeSpan heartbeatInterval, long checkpoint = 0, IRaftPartitionStateTransfer? transfer = null)
    {
        LevelCountingLogger logger = new();
        CapturingHost host = new() { PartitionStateTransfer = transfer };
        host.Configuration.HeartbeatInterval = heartbeatInterval;

        FullWal wal = new(tailThrough: 500, checkpoint: checkpoint);

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
    private sealed class InstantTransfer : IRaftPartitionStateTransfer
    {
        public Task<Stream> ExportPartitionState(int partitionId, long upToIndex, CancellationToken ct) =>
            Task.FromResult<Stream>(new MemoryStream([0xAB, 0xCD]));

        public Task ImportPartitionState(int partitionId, Stream snapshot, CancellationToken ct) =>
            Task.CompletedTask;
    }

    private sealed class FullWal : IRaftWalFacade
    {
        private readonly long tailThrough;
        private readonly long checkpoint;

        public FullWal(long tailThrough, long checkpoint = 0)
        {
            this.tailThrough = tailThrough;
            this.checkpoint = checkpoint;
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
            for (long id = Math.Max(startLogIndex, 1); id <= tailThrough && batch.Count < maxEntries; id++)
                batch.Add(new() { Id = id, Term = 1, Type = RaftLogType.Committed, LogType = "t" });

            return ValueTask.FromResult(batch);
        }

        public ValueTask<long> GetAnyTermAtAsync(long logIndex) => ValueTask.FromResult(1L);
        public ValueTask<long> GetLastCheckpointAsync() => ValueTask.FromResult(checkpoint);
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
            HeartbeatInterval = TimeSpan.Zero, RecentHeartbeat = TimeSpan.Zero,
        };

        public HybridLogicalClock HybridLogicalClock { get; } = new();
        public IReadOnlyList<RaftNode> Nodes { get; set; } = [new(VoterA), new(VoterB)];
        public MemberLivenessState GetNodeLiveness(string endpoint) => MemberLivenessState.Alive;

        /// <summary>When set, the monotonic clock every elapsed-time gate reads; null means the real one.</summary>
        public long? MonotonicTicks { get; set; }

        public long GetMonotonicTimestamp() => MonotonicTicks ?? global::System.Diagnostics.Stopwatch.GetTimestamp();

        public HLCTimestamp GetLastNodeActivity(string e, int p) => HLCTimestamp.Zero;
        public void UpdateLastNodeActivity(string e, int p, HLCTimestamp t) { }
        public void EnqueueResponse(string e, RaftResponderRequest r) => Requests.Add(r);
        public Task InvokeLeaderChanged(int p, string l) => Task.CompletedTask;
        public Task<bool> InvokeReplicationReceived(int p, RaftLog l) => Task.FromResult(true);
        public Task<bool> InvokeSystemReplicationReceived(int p, RaftLog l) => Task.FromResult(true);
        public void InvokeReplicationError(int p, RaftLog l) { }

        public IRaftStateMachineTransfer? StateMachineTransfer => null;
        public IRaftSystemStateTransfer? SystemStateTransfer => null;
        public IRaftPartitionStateTransfer? PartitionStateTransfer { get; init; }

        private readonly ConcurrentDictionary<string, int> snapshotChunks = new();

        public int SnapshotChunksTo(string endpoint) => snapshotChunks.GetValueOrDefault(endpoint, 0);

        public Task<SnapshotResponse> SendInstallSnapshotAsync(RaftNode node, SnapshotRequest request, CancellationToken ct)
        {
            snapshotChunks.AddOrUpdate(node.Endpoint, 1, static (_, n) => n + 1);
            return Task.FromResult(new SnapshotResponse(request.IsLast ? SnapshotInstallOutcome.Installed : SnapshotInstallOutcome.ChunkAccepted));
        }
    }

    private sealed class NoopSink : IRaftOperationReplySink
    {
        public void TryComplete(ulong correlationId, RaftResponse response) { }
    }
}
