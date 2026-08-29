using System.Diagnostics;
using Kommander.Data;
using Kommander.Scheduling;

namespace Kommander.Consensus;

/// <summary>
/// Per-follower replication bookkeeping owned by the leader: how far each peer's log has been
/// replicated, what commit frontier it last reported about itself, where its log started, and the
/// two transient notes (crash-restart regression, saturation back-off) that pace repair.
///
/// <para><b>Why one type.</b> These six maps were separate fields on the partition state machine
/// and were reset together at fourteen different sites — twelve clearing all four Raft-state maps
/// and two deliberately keeping the commit frontiers. Every one of those sites was a chance to
/// forget a map, and a forgotten map is a stale-progress correctness bug, not a cosmetic one.
/// Owning them together turns each reset into a single named call whose exact semantics are
/// stated once, here.</para>
///
/// <para><b>Concurrency.</b> Leader-side state, touched only on the partition executor thread;
/// holds no locks by design.</para>
/// </summary>
internal sealed class ReplicationTracker
{
    private readonly IRaftPartitionHost host;

    /// <summary>
    /// Per-peer commit frontier as the peer last reported it about itself: the highest log id the
    /// follower's gap-aware WAL frontier had committed when it acknowledged an AppendLogs.
    /// This is what the heartbeat computes <c>followerGap</c> from, so every backfill
    /// trigger derives from it. Invariants: written only from a <see cref="RaftOperationStatus.Success"/>
    /// ack's self-report (or a confirmed snapshot install boundary) — never from a rejection ack,
    /// whose committedIndex field carries the follower's raw max log id, an over-estimate of the
    /// frontier whenever the log has an uncommitted or non-contiguous tail — and last-writer-wins,
    /// so a genuinely regressed follower (crash-restart) can lower it and become visible as behind
    /// again. Violating either invariant pins an over-estimate no later ack can correct, and the
    /// peer is then never backfilled however far behind it really is (Jepsen stranded-replica
    /// findings: commit frontier stalls while the log keeps growing and the leader sees no gap).
    /// </summary>
    private readonly Dictionary<string, long> lastCommitIndexes = [];

    /// <summary>
    /// Where each peer's log started, as reported by handshakes and vote replies. A log id, not a
    /// committed frontier — the two are deliberately not interchangeable, which is why a vote
    /// never seeds <see cref="lastCommitIndexes"/>.
    /// </summary>
    private readonly Dictionary<string, long> startCommitIndexes = [];

    /// <summary>
    /// Per-follower replication cursor: the index of the next log entry to send to each peer.
    /// Seeded to <c>leaderMaxLog + 1</c> on election win (optimistic: assume peer is in sync).
    /// Backtracked on <see cref="RaftOperationStatus.LogMismatch"/> and advanced on
    /// <see cref="RaftOperationStatus.Success"/> replies.  Only meaningful while this node is
    /// Leader; cleared on every leader→follower transition so stale progress never leaks across terms.
    /// </summary>
    private readonly Dictionary<string, long> nextIndex = [];

    /// <summary>
    /// Per-follower highest log index known to be replicated on that peer.
    /// Zero until the peer confirms receipt of at least one entry.  Advanced in lock-step with
    /// <see cref="nextIndex"/> on a success reply; used to detect full catch-up
    /// (<c>matchIndex[peer] == leaderMaxLog</c>).
    /// </summary>
    private readonly Dictionary<string, long> matchIndex = [];

    /// <summary>
    /// Per-follower backfill cooldown: the monotonic tick before which this leader will not
    /// send another entry-carrying batch to that peer, set when the peer reports
    /// <see cref="RaftOperationStatus.FollowerWalSaturated"/>.
    /// </summary>
    /// <remarks>
    /// Deliberately *not* cleared on a leader transition, unlike <see cref="nextIndex"/> and
    /// <see cref="matchIndex"/>. Those are Raft state and a stale value is a correctness
    /// hazard; this is a transient throttle whose entries are absolute deadlines, so a stale
    /// one expires on its own within the backoff window and can at worst delay one batch. The
    /// alternative — clearing it at every site that resets peer progress — buys nothing
    /// and is one forgotten site away from a bug.
    /// </remarks>
    private readonly Dictionary<string, long> backfillPausedUntilTicks = [];

    /// <summary>
    /// Per-peer note of a detected commit-frontier regression (crash-restart signature): the endpoint
    /// maps to the committed frontier the peer reported below its recorded <see cref="matchIndex"/>.
    ///
    /// <para>Written by the ack path on the hot path (detection only, cheap) and consumed by the
    /// heartbeat once per interval, which performs the actual (WAL-read + AppendLogs) re-supply
    /// anchored at the recorded frontier and then clears the entry. The split is deliberate: doing
    /// the re-supply inline on every ack livelocked the cluster under load (an anchored re-ship
    /// fighting an in-flight catch-up starved the executor and stalled elections). The ack path
    /// also CLEARS the note when a peer reports normal progress, so a transient reordered ack that
    /// momentarily looked like a regression self-heals before the next heartbeat acts.</para>
    /// </summary>
    private readonly Dictionary<string, long> regressedFrontiers = [];

    /// <summary>
    /// Per-peer anchored-repair notes recorded when a peer answers an append with
    /// <see cref="Kommander.Data.RaftOperationStatus.LogMismatch"/>. The note carries the anchor the
    /// peer reported (its contiguous position — the presence frontier under the over-gap ack gate,
    /// or its raw max on a genuine divergence), and the next heartbeat acts on it with an anchored
    /// backfill batch from exactly that position. This is the only repair driver for a peer whose
    /// COMMITTED frontier matches the leader's while its log is missing part of the leader's
    /// uncommitted (inherited prior-term) tail — the committed-gap triggers all measure zero there,
    /// so without this note a new leader whose promotion barrier lands above such a gap can never
    /// commit the barrier (the post-heal wedge behind the over-gap ack gate). Same paced,
    /// take-once discipline as <see cref="regressedFrontiers"/>: acting inline on every ack is the
    /// shape that livelocked before.
    /// </summary>
    private readonly Dictionary<string, long> mismatchAnchors = [];

    /// <summary>
    /// Per-peer convergence probe for entry-carrying backfill: the follower's reported commit
    /// frontier at the moment the last batch shipped, when it shipped, and how many ships a later
    /// acknowledgement has proven fruitless.
    ///
    /// <para>This exists because the ack fast-path re-supply is self-exciting: a follower whose
    /// frontier is stuck (a lost lazy commit marker below the leader's monotonic
    /// <see cref="matchIndex"/>) acknowledges every duplicate batch with Success and the same
    /// frontier, and each such ack triggers another WAL range read and another ship — a
    /// network-speed ping-pong that reads the WAL tail forever, saturates the shared read
    /// scheduler, and starves application reads, with zero writes and zero log lines. The probe
    /// makes "shipping is not helping" observable so the sender can pace itself and re-anchor.</para>
    ///
    /// <para><b>A ship counts as fruitless only when an acknowledgement proves it.</b> The proof is
    /// a later Success ack whose frontier report sits at or below the frontier recorded at ship
    /// time (<see cref="RecordBackfillAckFrontier"/>). A ship the peer never answered proves
    /// nothing: the peer may be dead, partitioned away, or mid-restart. An earlier version counted
    /// every ship whose recorded frontier had not moved, and the frontier only moves on a Success
    /// ack — so a killed follower accrued a capped pause from silent ships and then served that
    /// pause the moment it restarted, exactly when catch-up mattered most. That starved
    /// meta-partition repair under a restart-heavy fault profile (12&#215; fewer backfill batches, the
    /// Jepsen <c>snapshot / partition,kill</c> regression at Kommander 1.3.4). The wedge this probe
    /// exists for is characterized by acks that DO arrive, so gating the count on an ack keeps the
    /// wedge fully detected. A dead peer that never acks is bounded by the outbound-queue
    /// saturation gate instead.</para>
    ///
    /// <para>Only peers that report a commit frontier (&gt;= 0) are tracked: a legacy-path peer
    /// reports -1 forever, its catch-up is driven by snapshot installs rather than frontier
    /// advances, and pacing it on a frontier that can never move would throttle a healthy
    /// catch-up.</para>
    /// </summary>
    private sealed class BackfillProgressProbe
    {
        public long FrontierAtLastShip;
        public long LastShipTicks;
        public int FruitlessShips;
        public bool Warned;

        /// <summary>
        /// True while at least one ship since the last fruitless-count update awaits its verdict.
        /// Set on every ship, cleared when an ack proves the outstanding ship(s) fruitless — so a
        /// burst of ships answered by one stuck ack counts once, and a flood of heartbeat acks
        /// between two ships cannot inflate the count either.
        /// </summary>
        public bool ShipOutstanding;
    }

    private readonly Dictionary<string, BackfillProgressProbe> backfillProgress = [];

    /// <summary>Snapshot of one peer's backfill-convergence probe, consumed by the sender.</summary>
    public readonly record struct BackfillProgress(int FruitlessShips, long LastShipTicks);

    public ReplicationTracker(IRaftPartitionHost host) => this.host = host;

    // ── bulk resets ───────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Discards all per-follower Raft progress. Called on every leader→follower transition: stale
    /// progress from a previous term describes a log the peers may no longer hold, and acting on it
    /// is a correctness hazard rather than a mere inefficiency.
    /// <para>The saturation back-off map is deliberately NOT cleared — see
    /// <see cref="backfillPausedUntilTicks"/>.</para>
    /// </summary>
    public void ClearAll()
    {
        lastCommitIndexes.Clear();
        nextIndex.Clear();
        matchIndex.Clear();
        regressedFrontiers.Clear();
        mismatchAnchors.Clear();
        backfillProgress.Clear();
    }

    /// <summary>
    /// Discards the replication cursors but KEEPS the reported commit frontiers. Used on the
    /// election-win seeding path: a vote reports a log id, not a committed frontier, so the
    /// frontiers a peer reported about itself remain the best information available and a zero
    /// seed would re-ship every follower's log from index 1 on every election.
    /// </summary>
    public void ClearProgressKeepingCommitFrontiers()
    {
        nextIndex.Clear();
        matchIndex.Clear();
        regressedFrontiers.Clear();
        mismatchAnchors.Clear();
        backfillProgress.Clear();
    }

    /// <summary>
    /// Discards every piece of progress recorded for one peer. Returns whether a commit frontier
    /// had been recorded for it, which the caller uses to decide whether the reset is worth
    /// reporting.
    /// </summary>
    public bool RemovePeer(string endpoint)
    {
        bool hadProgress = lastCommitIndexes.Remove(endpoint);
        nextIndex.Remove(endpoint);
        matchIndex.Remove(endpoint);
        regressedFrontiers.Remove(endpoint);
        startCommitIndexes.Remove(endpoint);
        backfillProgress.Remove(endpoint);
        return hadProgress;
    }

    // ── reported commit frontier ──────────────────────────────────────────────────────────────

    public bool TryGetCommitFrontier(string endpoint, out long value) => lastCommitIndexes.TryGetValue(endpoint, out value);

    public long GetCommitFrontierOrDefault(string endpoint, long fallback) => lastCommitIndexes.GetValueOrDefault(endpoint, fallback);

    public bool HasCommitFrontier(string endpoint) => lastCommitIndexes.ContainsKey(endpoint);

    /// <summary>
    /// Records a peer's self-reported commit frontier, last-writer-wins. Only a
    /// <see cref="RaftOperationStatus.Success"/> ack may reach here — see
    /// <see cref="lastCommitIndexes"/> for why a rejection ack must never feed this map.
    /// </summary>
    public void SetCommitFrontier(string endpoint, long value) => lastCommitIndexes[endpoint] = value;

    /// <summary>
    /// Raises a peer's recorded frontier, never lowers it. Used by the snapshot-install completion,
    /// which confirms a boundary rather than reporting a live frontier.
    /// </summary>
    public void AdvanceCommitFrontier(string endpoint, long value)
    {
        if (!lastCommitIndexes.TryGetValue(endpoint, out long current) || value > current)
            lastCommitIndexes[endpoint] = value;
    }

    /// <summary>
    /// Records a CONFIRMED snapshot install at <paramref name="snapshotIndex"/> as replication
    /// progress for one peer: the commit frontier, <c>matchIndex</c>, <c>nextIndex</c>, and the
    /// log-start position all advance to the installed boundary.
    ///
    /// <para><b>Why every cursor must advance, not only the frontier.</b> On the legacy two-fsync
    /// path a follower's acks carry <c>committedIndex = -1</c> ("no report"), so no ack ever
    /// advances <c>matchIndex</c> or <c>nextIndex</c>. The install confirmation is therefore the
    /// leader's ONLY progress channel for such a peer. An earlier version advanced only the
    /// frontier; the stale <c>nextIndex</c> stayed pinned below the WAL compaction floor, and the
    /// backfill path prefers <c>nextIndex</c> as its anchor — so every batch re-anchored at an
    /// index the WAL could no longer serve, was refused, re-escalated to another snapshot, and the
    /// rescue looped every cooldown while the follower fell monotonically further behind (the
    /// Caraxes <c>bank-optimistic-2h-f</c> frozen-anchor finding).</para>
    ///
    /// <para>Every advance is monotonic — a completion that raced a newer ack must not drag
    /// progress backwards. The boundary covers only committed state, so claiming it in
    /// <c>matchIndex</c> can never over-count a commit quorum. A pending regression note below the
    /// boundary is dropped: the installed state supersedes the regressed range it pointed at, and
    /// acting on it would re-anchor below the floor again.</para>
    /// </summary>
    public void AdvanceProgressFromSnapshotInstall(string endpoint, long snapshotIndex)
    {
        AdvanceCommitFrontier(endpoint, snapshotIndex);
        AdvanceStartCommitIndex(endpoint, snapshotIndex);

        // The installed boundary supersedes whatever range log shipping was failing to converge;
        // the peer earns a fresh, undamped backfill start from the new position.
        backfillProgress.Remove(endpoint);

        if (!matchIndex.TryGetValue(endpoint, out long match) || snapshotIndex > match)
            matchIndex[endpoint] = snapshotIndex;

        if (!nextIndex.TryGetValue(endpoint, out long next) || snapshotIndex + 1 > next)
            nextIndex[endpoint] = snapshotIndex + 1;

        if (regressedFrontiers.TryGetValue(endpoint, out long regressed) && regressed < snapshotIndex)
            regressedFrontiers.Remove(endpoint);
    }

    // ── log-start positions ───────────────────────────────────────────────────────────────────

    public long GetStartCommitIndexOrDefault(string endpoint, long fallback) => startCommitIndexes.GetValueOrDefault(endpoint, fallback);

    public void SetStartCommitIndex(string endpoint, long value) => startCommitIndexes[endpoint] = value;

    /// <summary>Raises the recorded log-start position for a peer, never lowers it.</summary>
    public void AdvanceStartCommitIndex(string endpoint, long value)
    {
        if (startCommitIndexes.TryGetValue(endpoint, out long current))
        {
            if (value > current)
                startCommitIndexes[endpoint] = value;
        }
        else
            startCommitIndexes[endpoint] = value;
    }

    /// <summary>
    /// The highest log position this leader has any evidence the peer holds, from either the
    /// frontier it reported or where its log started.
    /// </summary>
    public long GetKnownRemoteMaxLogId(string endpoint) =>
        Math.Max(
            lastCommitIndexes.GetValueOrDefault(endpoint, -1),
            startCommitIndexes.GetValueOrDefault(endpoint, -1));

    // ── replication cursors ───────────────────────────────────────────────────────────────────

    public bool TryGetNextIndex(string endpoint, out long value) => nextIndex.TryGetValue(endpoint, out value);

    public long GetNextIndexOrDefault(string endpoint, long fallback) => nextIndex.GetValueOrDefault(endpoint, fallback);

    public void SetNextIndex(string endpoint, long value) => nextIndex[endpoint] = value;

    public bool TryGetMatchIndex(string endpoint, out long value) => matchIndex.TryGetValue(endpoint, out value);

    public void SetMatchIndex(string endpoint, long value) => matchIndex[endpoint] = value;

    /// <summary>
    /// Seeds one peer's progress optimistically on an election win: assume it is in sync at
    /// <paramref name="leaderMaxLog"/>. A peer that is actually behind corrects this through
    /// <see cref="RaftOperationStatus.LogMismatch"/> backtracking.
    /// </summary>
    public void SeedOptimisticProgress(string endpoint, long leaderMaxLog)
    {
        nextIndex[endpoint] = leaderMaxLog + 1;
        matchIndex[endpoint] = 0;
    }

    // ── crash-restart regression notes ────────────────────────────────────────────────────────

    /// <summary>
    /// Takes and clears any recorded regression note for <paramref name="endpoint"/>. Taking rather
    /// than peeking is the contract the heartbeat relies on: the note is cleared whether or not a
    /// repair batch goes out, because a peer that is still behind re-records it on its next ack.
    /// </summary>
    public bool TryTakeRegressedFrontier(string endpoint, out long value)
    {
        if (!regressedFrontiers.TryGetValue(endpoint, out value))
            return false;

        regressedFrontiers.Remove(endpoint);
        return true;
    }

    public void RecordRegressedFrontier(string endpoint, long value) => regressedFrontiers[endpoint] = value;

    public void ClearRegressedFrontier(string endpoint) => regressedFrontiers.Remove(endpoint);

    // ── anchored-repair notes (LogMismatch) ───────────────────────────────────────────────────

    /// <summary>
    /// Takes and clears any anchored-repair note for <paramref name="endpoint"/> (see
    /// <see cref="mismatchAnchors"/>). Take-once: a peer that is still mismatched re-records the
    /// note on its next rejection, so the heartbeat never spins on a stale note.
    /// </summary>
    public bool TryTakeMismatchAnchor(string endpoint, out long value)
    {
        if (!mismatchAnchors.TryGetValue(endpoint, out value))
            return false;

        mismatchAnchors.Remove(endpoint);
        return true;
    }

    /// <summary>Records the anchor a peer reported with a LogMismatch rejection (see <see cref="mismatchAnchors"/>).</summary>
    public void RecordMismatchAnchor(string endpoint, long value) => mismatchAnchors[endpoint] = value;

    // ── backfill convergence probe ────────────────────────────────────────────────────────────

    /// <summary>
    /// Consults (and self-heals) the peer's backfill-convergence probe before a ship attempt.
    /// A reported frontier above the one recorded at the last ship proves progress: the probe is
    /// dropped and the default (undamped) state returned. A frontier of -1 means the peer does
    /// not report one; such peers are never tracked (see <see cref="BackfillProgressProbe"/>).
    /// </summary>
    public BackfillProgress ObserveBackfillProgress(string endpoint, long reportedFrontier)
    {
        if (!backfillProgress.TryGetValue(endpoint, out BackfillProgressProbe? probe))
            return default;

        if (reportedFrontier > probe.FrontierAtLastShip)
        {
            backfillProgress.Remove(endpoint);
            return default;
        }

        return new(probe.FruitlessShips, probe.LastShipTicks);
    }

    /// <summary>
    /// Records that an entry-carrying batch shipped to <paramref name="endpoint"/> while its
    /// reported commit frontier stood at <paramref name="reportedFrontier"/>, and returns the
    /// current fruitless streak. The ship itself never grows the streak — only a later ack proves
    /// a ship fruitless (<see cref="RecordBackfillAckFrontier"/>); see
    /// <see cref="BackfillProgressProbe"/> for why silent ships must not count. A negative
    /// frontier (no report) clears any probe and returns 0 — pacing on a frontier that can never
    /// move would throttle a healthy legacy-path catch-up.
    /// </summary>
    public int RecordBackfillShip(string endpoint, long reportedFrontier)
    {
        if (reportedFrontier < 0)
        {
            backfillProgress.Remove(endpoint);
            return 0;
        }

        long nowTicks = host.GetMonotonicTimestamp();

        if (backfillProgress.TryGetValue(endpoint, out BackfillProgressProbe? probe))
        {
            if (reportedFrontier > probe.FrontierAtLastShip)
            {
                probe.FruitlessShips = 0;
                probe.Warned = false;
            }

            probe.FrontierAtLastShip = reportedFrontier;
            probe.LastShipTicks = nowTicks;
            probe.ShipOutstanding = true;
            return probe.FruitlessShips;
        }

        backfillProgress[endpoint] = new()
        {
            FrontierAtLastShip = reportedFrontier,
            LastShipTicks = nowTicks,
            FruitlessShips = 0,
            ShipOutstanding = true,
        };
        return 0;
    }

    /// <summary>
    /// Feeds one Success ack's frontier self-report into the peer's backfill-convergence probe.
    /// This is the ONLY place the fruitless streak grows: a report above the frontier at the last
    /// ship is progress and drops the probe; a report at or below it, while a ship awaits its
    /// verdict, proves that ship fruitless and counts it (once — the outstanding flag clears, so
    /// neither a ship burst nor a heartbeat-ack flood inflates the streak). An ack with no
    /// outstanding ship changes nothing: the peer is merely repeating what the sender already
    /// paced on. Callers pass Success-ack frontier reports only, mirroring
    /// <see cref="SetCommitFrontier"/> — a rejection's committedIndex is a raw max log id and
    /// proves nothing about commit progress.
    /// </summary>
    public void RecordBackfillAckFrontier(string endpoint, long reportedFrontier)
    {
        if (!backfillProgress.TryGetValue(endpoint, out BackfillProgressProbe? probe))
            return;

        if (reportedFrontier > probe.FrontierAtLastShip)
        {
            backfillProgress.Remove(endpoint);
            return;
        }

        if (probe.ShipOutstanding)
        {
            probe.ShipOutstanding = false;
            probe.FruitlessShips++;
        }
    }

    /// <summary>
    /// Marks the peer's current no-progress episode as warned, returning true exactly once per
    /// episode so the log carries one Warning instead of one per ship. The flag resets when a
    /// ship observes frontier progress.
    /// </summary>
    public bool TryMarkBackfillNoProgressWarned(string endpoint)
    {
        if (!backfillProgress.TryGetValue(endpoint, out BackfillProgressProbe? probe) || probe.Warned)
            return false;

        probe.Warned = true;
        return true;
    }

    // ── saturation back-off ───────────────────────────────────────────────────────────────────

    /// <summary>
    /// Whether entry-carrying backfill to <paramref name="endpoint"/> is still paused after the peer
    /// reported a saturated WAL. Expired pauses are dropped here, so the map self-prunes on the
    /// same path that reads it.
    /// </summary>
    public bool IsBackfillPaused(string endpoint)
    {
        if (!backfillPausedUntilTicks.TryGetValue(endpoint, out long pausedUntil))
            return false;

        if (host.GetMonotonicTimestamp() < pausedUntil)
            return true;

        backfillPausedUntilTicks.Remove(endpoint);
        return false;
    }

    /// <summary>
    /// Pauses entry-carrying backfill to a saturated peer for <paramref name="backoff"/>. A
    /// saturated follower is the one rejection that must change the leader's behaviour rather than
    /// just its logs: re-sending immediately is what keeps the peer from ever having room.
    /// </summary>
    public void PauseBackfill(string endpoint, TimeSpan backoff) =>
        backfillPausedUntilTicks[endpoint] =
            host.GetMonotonicTimestamp() + (long)(backoff.TotalSeconds * Stopwatch.Frequency);
}
