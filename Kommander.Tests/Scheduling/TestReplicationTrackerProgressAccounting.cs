using System.Diagnostics;
using Kommander;
using Kommander.Consensus;
using Kommander.Data;
using Kommander.Gossip;
using Kommander.Scheduling;
using Kommander.System;
using Kommander.Time;

namespace Kommander.Tests.Scheduling;

/// <summary>
/// The leader's per-follower progress accounting at its edges: a rejection's position report
/// lowering every cursor a restarted peer can no longer justify, the once-per-episode snapshot
/// deferral warning, a snapshot install superseding a regressed-range note, the backfill probe
/// resetting on genuine progress, a compacted-anchor rejection only counting when it proves the
/// anchor uncommitted, and the per-peer cap on live resolution shipments.
/// </summary>
public class TestReplicationTrackerProgressAccounting
{
    private const string Peer = "node-b:9001";

    // ── LowerProgressTo ───────────────────────────────────────────────────────

    /// <summary>
    /// A rejection ack with no usable position (a negative reported max) must change nothing: it
    /// proves nothing about the peer's log, and lowering on it would strand progress records the
    /// next real ack still needs.
    /// </summary>
    [Fact]
    public void RejectionWithoutAPositionReport_ChangesNoProgressRecord()
    {
        MiniHost host = new();
        ReplicationTracker tracker = new(host);

        tracker.SetCommitFrontier(Peer, 18);
        tracker.SetDurableFrontier(Peer, 15);
        tracker.SetPresenceFrontier(Peer, 20, 3);
        tracker.SetMatchIndex(Peer, 12);

        bool lowered = tracker.LowerProgressTo(Peer, -1);

        Assert.False(lowered);
        Assert.Equal(18, tracker.GetCommitFrontierOrDefault(Peer, -1));
        Assert.True(tracker.TryGetDurableFrontier(Peer, out long durable));
        Assert.Equal(15, durable);
        Assert.True(tracker.TryGetPresenceFrontier(Peer, out long presence, out _));
        Assert.Equal(20, presence);
        Assert.True(tracker.TryGetMatchIndex(Peer, out long matched));
        Assert.Equal(12, matched);
    }

    /// <summary>
    /// A peer that proved it holds nothing above the reported max (a crash-restart under the same
    /// endpoint) must have every cursor it can no longer justify pulled back: the presence
    /// frontier is removed outright (its term at the position is unknown), the durable frontier and
    /// matchIndex are lowered, and live resolution shipments recorded for the previous incarnation
    /// stop counting as coverage — otherwise every repair steered above what the blank peer holds.
    /// </summary>
    [Fact]
    public void RejectionAboveThePeersHeldPosition_LowersDurableAndMatch_AndDropsInFlightResolutions()
    {
        MiniHost host = new();
        ReplicationTracker tracker = new(host);
        long now = host.MonotonicTicks;

        tracker.SetCommitFrontier(Peer, 18);
        tracker.SetDurableFrontier(Peer, 15);
        tracker.SetPresenceFrontier(Peer, 20, 3);
        tracker.SetMatchIndex(Peer, 12);
        tracker.RecordResolutionShipped(Peer, 13, 18, now);

        bool lowered = tracker.LowerProgressTo(Peer, 10);

        Assert.True(lowered);
        Assert.Equal(10, tracker.GetCommitFrontierOrDefault(Peer, -1));
        // The durable frontier and matchIndex cannot claim entries the peer proved absent.
        Assert.True(tracker.TryGetDurableFrontier(Peer, out long durable));
        Assert.Equal(10, durable);
        Assert.True(tracker.TryGetMatchIndex(Peer, out long matched));
        Assert.Equal(10, matched);
        Assert.False(tracker.TryGetPresenceFrontier(Peer, out _, out _), "presence is removed, not lowered: its term at the reported position is unknown");
        Assert.False(
            tracker.AreResolutionsInFlight(Peer, 12, 18, now, TimeSpan.FromMinutes(1)),
            "broadcasts shipped to the previous incarnation prove nothing about this one");
    }

    // ── WAL-stall episode bookkeeping ─────────────────────────────────────────

    /// <summary>
    /// The snapshot-deferral warning flag fires exactly once per stall episode: the first ask
    /// returns true, repeats return false while the episode lasts, and a new episode (a stall
    /// reported again after it ended) earns exactly one more warning.
    /// </summary>
    [Fact]
    public void SnapshotDeferralWarning_FiresOncePerStallEpisode()
    {
        MiniHost host = new();
        ReplicationTracker tracker = new(host);

        Assert.Equal(ReplicationTracker.WalStallTransition.Began, tracker.RecordWalStallReport(Peer, 5_000));
        Assert.True(tracker.TryMarkWalStallSnapshotDeferralWarned(Peer));
        Assert.False(tracker.TryMarkWalStallSnapshotDeferralWarned(Peer), "one deferral Warning per episode, not one per refused heartbeat");
        Assert.True(tracker.IsReportingWalStall(Peer));

        Assert.Equal(ReplicationTracker.WalStallTransition.Ended, tracker.RecordWalStallReport(Peer, 0));
        Assert.False(tracker.IsReportingWalStall(Peer));
        Assert.False(tracker.TryMarkWalStallSnapshotDeferralWarned(Peer), "a peer that is not reporting a stall has nothing to warn about");

        Assert.Equal(ReplicationTracker.WalStallTransition.Began, tracker.RecordWalStallReport(Peer, 6_000));
        Assert.True(tracker.TryMarkWalStallSnapshotDeferralWarned(Peer), "a new episode earns a fresh single warning");
    }

    // ── snapshot install ──────────────────────────────────────────────────────

    /// <summary>
    /// A confirmed snapshot install supersedes a pending regression note that pointed below the
    /// installed boundary: acting on it would re-anchor repair below the floor the install just
    /// raised. The install must also advance every cursor monotonically so the rescue cannot loop.
    /// </summary>
    [Fact]
    public void ConfirmedSnapshotInstall_DropsRegressionNoteBelowTheBoundary_AndAdvancesEveryCursor()
    {
        MiniHost host = new();
        ReplicationTracker tracker = new(host);

        tracker.SetMatchIndex(Peer, 4);
        tracker.SetNextIndex(Peer, 5);
        tracker.RecordRegressedFrontier(Peer, 6);

        tracker.AdvanceProgressFromSnapshotInstall(Peer, snapshotIndex: 10, leaderCommitIndex: 9);

        Assert.False(tracker.TryTakeRegressedFrontier(Peer, out _), "the installed state supersedes the regressed range it pointed at");
        Assert.True(tracker.TryGetMatchIndex(Peer, out long matched));
        Assert.Equal(10, matched);
        // nextIndex must advance past the boundary or every batch re-anchors below the compaction floor.
        Assert.True(tracker.TryGetNextIndex(Peer, out long next));
        Assert.Equal(11, next);
        Assert.Equal(10, tracker.GetCommitFrontierOrDefault(Peer, -1));
        Assert.True(tracker.TryGetSnapshotSeed(Peer, out ReplicationTracker.SnapshotSeed seed));
        Assert.Equal(10, seed.Index);

        // Monotonicity: an install confirmation that raced a newer ack must not drag progress back.
        tracker.AdvanceProgressFromSnapshotInstall(Peer, snapshotIndex: 7, leaderCommitIndex: 7);
        Assert.True(tracker.TryGetMatchIndex(Peer, out matched));
        Assert.Equal(10, matched);
        Assert.Equal(10, tracker.GetCommitFrontierOrDefault(Peer, -1));
    }

    // ── backfill convergence probe ────────────────────────────────────────────

    /// <summary>
    /// A ship that observes the peer's frontier has moved since the previous ship is progress: the
    /// fruitless streak resets and the warned flag clears, so a slow-but-converging peer is never
    /// escalated to a snapshot by its own steady progress.
    /// </summary>
    [Fact]
    public void BackfillShip_ObservingFrontierProgress_ResetsTheFruitlessStreak()
    {
        MiniHost host = new();
        ReplicationTracker tracker = new(host);

        // First ship paces on frontier 5; an ack still at 5 proves that ship fruitless.
        Assert.Equal(0, tracker.RecordBackfillShip(Peer, reportedFrontier: 5, lastShippedId: 8));
        tracker.RecordBackfillAckFrontier(Peer, reportedFrontier: 5);
        Assert.True(tracker.TryMarkBackfillNoProgressWarned(Peer));

        // The next ship sees the frontier moved to 7: the probe resets rather than counting on.
        Assert.Equal(0, tracker.RecordBackfillShip(Peer, reportedFrontier: 7, lastShippedId: 9));

        // A ship that carries the peer's presence frontier extends the credit threshold, and the
        // streak stays clean until an ack proves otherwise.
        tracker.SetPresenceFrontier(Peer, 9, 2);
        tracker.RecordBackfillShip(Peer, reportedFrontier: 7, lastShippedId: 9);
        tracker.RecordBackfillAckFrontier(Peer, reportedFrontier: 7);

        // Only the ack after the reset counts — the pre-progress ship must not.
        ReplicationTracker.BackfillProgress progress = tracker.ObserveBackfillProgress(Peer, reportedFrontier: 7);
        Assert.Equal(1, progress.FruitlessShips);

        // The warned flag reset with the progress: a fresh no-progress episode can warn again.
        Assert.True(tracker.TryMarkBackfillNoProgressWarned(Peer));
    }

    // ── compacted-anchor refusals ─────────────────────────────────────────────

    /// <summary>
    /// Only a rejection that proves the anchor is uncommitted on the peer counts toward the
    /// compacted-anchor refusal streak: a peer whose durable frontier reaches the anchor holds it
    /// inside its committed prefix, and a peer that reports no durable frontier proves nothing —
    /// counting either sent snapshots to healthy followers.
    /// </summary>
    [Fact]
    public void CompactedAnchorRefusal_CountsOnlyWhenThePeerProvesTheAnchorUncommitted()
    {
        ReplicationTracker tracker = new(null!);

        // No compacted-anchor ship on record yet.
        Assert.Equal((0, -1L), tracker.RecordCompactedAnchorRefusal(Peer, reportedDurable: 10));

        tracker.RecordAnchorShip(Peer, anchor: 21, anchorCompacted: true);

        // A durable frontier at or above the anchor holds it committed; no durable report proves
        // nothing either way. None of these may count.
        Assert.Equal((0, 21L), tracker.RecordCompactedAnchorRefusal(Peer, reportedDurable: 21));
        Assert.Equal((0, 21L), tracker.RecordCompactedAnchorRefusal(Peer, reportedDurable: 30));
        Assert.Equal((0, 21L), tracker.RecordCompactedAnchorRefusal(Peer, reportedDurable: -1));

        // A durable frontier below the anchor proves the batch can never be accepted.
        Assert.Equal((1, 21L), tracker.RecordCompactedAnchorRefusal(Peer, reportedDurable: 15));
        Assert.Equal((2, 21L), tracker.RecordCompactedAnchorRefusal(Peer, reportedDurable: 15));

        // A new anchor does not blind the proof: a durable frontier still below it immediately
        // counts — the anchor is proven unaccepted the moment the peer reports beneath it.
        tracker.RecordAnchorShip(Peer, anchor: 40, anchorCompacted: true);
        Assert.Equal((1, 40L), tracker.RecordCompactedAnchorRefusal(Peer, reportedDurable: 15));

        tracker.ClearCompactedAnchorIfCovered(Peer, committedFrontier: 40);
        Assert.Equal((0, -1L), tracker.RecordCompactedAnchorRefusal(Peer, reportedDurable: 15));
    }

    // ── live resolution shipments ─────────────────────────────────────────────

    /// <summary>
    /// A shipment record with an unusable range (negative or inverted ids) is refused rather than
    /// stored: a corrupt range would poison the coverage walk for every later query.
    /// </summary>
    [Fact]
    public void ResolutionShipped_WithAnInvertedRange_IsIgnored()
    {
        ReplicationTracker tracker = new(null!);
        long now = Stopwatch.GetTimestamp();

        tracker.RecordResolutionShipped(Peer, minId: -5, maxId: 10, nowTicks: now);
        tracker.RecordResolutionShipped(Peer, minId: 30, maxId: 20, nowTicks: now);

        Assert.False(tracker.AreResolutionsInFlight(Peer, 0, 10, now, TimeSpan.FromMinutes(1)));
    }

    /// <summary>
    /// The per-peer shipment list is capped: past the cap the lowest range — the one most likely
    /// already resolved — is evicted, which makes the coverage walk report a gap and re-supply the
    /// range rather than grow without bound under a commit storm.
    /// </summary>
    [Fact]
    public void ResolutionShipments_CappedPerPeer_EvictTheLowestRange()
    {
        ReplicationTracker tracker = new(null!);
        long now = Stopwatch.GetTimestamp();

        const int ranges = 300; // above the per-peer cap
        for (int i = 0; i < ranges; i++)
            tracker.RecordResolutionShipped(Peer, minId: i * 10 + 1, maxId: i * 10 + 10, nowTicks: now);

        // The lowest ranges were evicted: coverage below the surviving window is a gap.
        Assert.False(tracker.AreResolutionsInFlight(Peer, 0, 10, now, TimeSpan.FromMinutes(1)));

        // The newest ranges are all still covered, contiguously.
        long firstSurviving = (ranges - 256) * 10 + 1;
        Assert.True(
            tracker.AreResolutionsInFlight(Peer, firstSurviving - 1, ranges * 10, now, TimeSpan.FromMinutes(1)),
            "the surviving shipments must still cover their window contiguously");
    }

    /// <summary>
    /// Minimal host: a deterministic monotonic tick the test can read, plus the configuration the
    /// stall thresholds consult. Everything the tracker does not read is defaulted.
    /// </summary>
    private sealed class MiniHost : IRaftPartitionHost
    {
        public RaftConfiguration Config { get; } = new()
        {
            HeartbeatInterval = TimeSpan.FromMilliseconds(150),
            WalStallWarnThreshold = TimeSpan.FromMilliseconds(500),
        };

        public long MonotonicTicks { get; set; } = Stopwatch.GetTimestamp();

        public int PartitionId => 1;
        public string Leader { get; set; } = "";
        public string LocalEndpoint => "node-a:9000";
        public int LocalNodeId => 1;
        public ClusterMemberRole LocalRole => ClusterMemberRole.Voter;
        public bool IsVoter(string endpoint) => true;
        public RaftConfiguration Configuration => Config;
        public HybridLogicalClock HybridLogicalClock { get; } = new();
        public IReadOnlyList<RaftNode> Nodes { get; set; } = [];
        public MemberLivenessState GetNodeLiveness(string endpoint) => MemberLivenessState.Alive;
        public HLCTimestamp GetLastNodeActivity(string endpoint, int partitionId) => HLCTimestamp.Zero;
        public long GetLastNodeActivityTicks(string endpoint, int partitionId) => 0;
        public void UpdateLastNodeActivity(string endpoint, int partitionId, HLCTimestamp timestamp) { }
        public void EnqueueResponse(string endpoint, RaftResponderRequest request) { }
        public Task InvokeLeaderChanged(int partitionId, string leader) => Task.CompletedTask;
        public Task<bool> InvokeReplicationReceived(int partitionId, RaftLog log) => Task.FromResult(true);
        public Task<bool> InvokeSystemReplicationReceived(int partitionId, RaftLog log) => Task.FromResult(true);
        public void InvokeReplicationError(int partitionId, RaftLog log) { }
        public IRaftStateMachineTransfer? StateMachineTransfer => null;
        public IRaftSystemStateTransfer? SystemStateTransfer => null;
        public Task<SnapshotResponse> SendInstallSnapshotAsync(RaftNode node, SnapshotRequest request, CancellationToken ct)
            => Task.FromResult(new SnapshotResponse(false));

        public long GetMonotonicTimestamp() => MonotonicTicks;
    }
}
