
using Kommander.Data;
using Kommander.Time;
using Kommander.WAL.Data;

namespace Kommander.Scheduling;

/// <summary>
/// Non-blocking WAL facade used by <see cref="RaftPartitionStateMachine"/>.
///
/// <para>Enqueue methods schedule work without performing synchronous storage I/O
/// inline.  Read methods remain async and are invoked by the state machine through
/// this facade so a future executor can route them to <c>IRaftWalScheduler</c>.</para>
/// </summary>
public interface IRaftWalFacade
{
    /// <summary>
    /// Phase 1 of the nonblocking restore: reads all persisted log entries from WAL
    /// storage using the I/O scheduler.  The returned list is delivered back to the
    /// partition executor as a <see cref="Kommander.Data.RaftRequestType.RestoreLogsLoaded"/>
    /// maintenance event for replay under the single-owner guarantee.
    /// </summary>
    ValueTask<IReadOnlyList<RaftLog>> LoadRestoreLogsAsync();

    /// <summary>
    /// Phase 2 of the nonblocking restore: replays the loaded log entries by invoking
    /// the application replication callbacks and updating the WAL commit index.
    /// Must be called on the partition executor thread.
    /// </summary>
    ValueTask CompleteRestoreAsync(IReadOnlyList<RaftLog> logs);

    ValueTask<long> GetMaxLogAsync();

    ValueTask<long> GetCurrentTermAsync();

    /// <summary>
    /// Reads up to <paramref name="maxEntries"/> committed log entries with id ≥
    /// <paramref name="startLogIndex"/>, sorted ascending. Used by the leader to
    /// backfill stale followers one bounded chunk at a time.
    /// </summary>
    ValueTask<List<RaftLog>> GetRangeAsync(long startLogIndex, int maxEntries);

    /// <summary>
    /// Reads up to <paramref name="maxEntries"/> log entries of ANY type (Proposed,
    /// Committed, RolledBack, etc.) with id ≥ <paramref name="startLogIndex"/>,
    /// sorted ascending.  Unlike <see cref="GetRangeAsync"/>, uncommitted entries are
    /// <b>not</b> filtered out.
    ///
    /// <para>Used on promotion to identify inherited Proposed entries from a prior term
    /// that were committed by quorum but whose lazy-commit markers may have been lost
    /// on the single-fsync fast path.  The state machine filters by term before
    /// delivering them to the consumer so that in-flight current-term proposals are
    /// never applied prematurely.</para>
    ///
    /// <para>The default implementation delegates to <see cref="GetRangeAsync"/>, which
    /// returns only committed entries — sufficient for backends that never retain Proposed
    /// entries after a crash (in-memory fakes, legacy-path WAL).  Durable backends on the
    /// single-fsync fast path must override this to return all entry types.</para>
    /// </summary>
    ValueTask<List<RaftLog>> GetRangeAllTypesAsync(long startLogIndex, int maxEntries)
        => GetRangeAsync(startLogIndex, maxEntries);

    /// <summary>
    /// Byte-budgeted variant of <see cref="GetRangeAllTypesAsync(long, int)"/>: the batch also
    /// stops once adding the next entry would exceed <paramref name="maxBytes"/> of payload, while
    /// always returning at least one entry when one exists. This is the leader-backfill read; the
    /// budget bounds the materialized allocation, which an entry count alone does not.
    /// <para>The default implementation ignores the budget so in-memory fakes need no override;
    /// the production adapter forwards it to the storage engine.</para>
    /// </summary>
    ValueTask<List<RaftLog>> GetRangeAllTypesAsync(long startLogIndex, int maxEntries, long maxBytes)
        => GetRangeAllTypesAsync(startLogIndex, maxEntries);

    /// <summary>
    /// Returns the term of the entry at exactly <paramref name="logIndex"/>, or <c>-1</c> if
    /// no entry with that id exists.  Unlike <see cref="GetRangeAsync"/>, this reads <em>any</em>
    /// entry regardless of commit status (Proposed, Committed, etc.) so it can be used for the
    /// Log Matching Property anchor check, which must succeed even when the entry at prevLogIndex
    /// is still in the Proposed state on the follower.
    /// </summary>
    ValueTask<long> GetAnyTermAtAsync(long logIndex);

    /// <summary>
    /// Returns the id of the last <see cref="Kommander.WAL.Data.RaftLogType.CommittedCheckpoint"/> WAL entry for
    /// this partition, or -1 if no checkpoint exists.  Used by the leader to detect when a
    /// follower's acknowledged log index falls below the compaction floor and a snapshot transfer
    /// is required.
    /// </summary>
    ValueTask<long> GetLastCheckpointAsync();

    /// <summary>
    /// Highest committed log id (excludes proposed-but-uncommitted tail entries). Used to seed the
    /// leader's per-follower backfill cursor on election, so a leader that has committed nothing in
    /// its current term can still backfill a stale follower without waiting for a new write.
    /// </summary>
    long GetCommitIndex();

    /// <summary>
    /// Highest log id durably present with <b>no holes below it</b> (any entry type), or -1 when the
    /// facade does not track presence (test stubs). Unlike <see cref="GetMaxLogAsync"/>, this can
    /// never overshoot a gap left by an out-of-order append (the unanchored live-propose broadcast
    /// can write a lone high entry over a hole on a behind follower), so it is the log position a
    /// node must use for Raft §5.4.1 election freshness — comparing raw max ids would let a node
    /// with holes win an election while missing committed entries. Synchronous in-memory read.
    /// </summary>
    long GetPresentIndex() => -1;

    /// <summary>
    /// True when this node holds durable entries buffered above an unfilled gap — it knows its own
    /// log is missing a range some peer may hold. The election path uses this to defer candidacy
    /// to a known fresher live voter instead of churning elections that the promotion gates would
    /// refuse. Default false for facades that do not track presence (test stubs).
    /// </summary>
    bool HasPresenceGap() => false;

    /// <summary>
    /// Absorbs a prefix proven resolved by other bookkeeping (the applied cursor, capped by
    /// contiguous presence) into the commit frontier. Used at promotion so a follower-era frontier
    /// bookkeeping miss cannot be frozen for a whole leader tenure — see the implementation notes
    /// in <c>RaftWriteAhead</c>. Default no-op for facades that do not track the frontier.
    /// </summary>
    void AbsorbResolvedPrefix(long throughId) { }

    /// <summary>
    /// Seeds the propose-id allocator at promotion to exactly <paramref name="nextId"/> —
    /// one above the promotion-time log tail — so a new leader stamps client writes at
    /// <c>lastLogIndex + 1</c> (Raft §5.3) and can neither reissue a durably occupied index
    /// (two values committing at one index) nor open a hole by allocating above the tail.
    /// Default no-op for facades that do not allocate ids (test stubs).
    /// </summary>
    void SeedProposeAllocator(long nextId) { }

    /// <summary>
    /// Term of the entry at <see cref="GetPresentIndex"/> (the §5.4.1 pair must describe the same
    /// log position), or -1 when the facade does not track presence.
    /// </summary>
    long GetPresentTerm() => -1;

    /// <summary>
    /// Undoes the optimistic frontier advance for a WAL write that completed with a failure
    /// (e.g. a full disk): the enqueue paths advance the presence/commit frontiers when an
    /// operation is accepted, so a failed completion leaves them certifying entries that are not
    /// on disk — an election-freshness and backfill-suppression hazard. Called by the completion
    /// router before any fence can discard the failed completion. Default no-op for facades that
    /// do not track frontiers (test stubs).
    /// </summary>
    ValueTask RegressFrontiersAfterFailedWriteAsync(long minLogIndex, long maxLogIndex, bool regressPresence, bool regressCommit)
        => ValueTask.CompletedTask;

    /// <summary>
    /// Advances the in-memory contiguous commit frontier over an entry the consensus layer has
    /// PROVEN committed but whose durable marker has not landed yet — the Raft §5.4.2 inherited
    /// prior-term entry that the promotion/commit drain delivers before its lazy re-commit marker
    /// is enqueued. Without this, the applied cursor leads the advertised commit frontier for the
    /// whole delivery window (and indefinitely if the batched re-commit enqueue fails), which
    /// reads as a commit-below-applied inversion to external observers. The durable marker still
    /// rides the batched re-commit; a failed enqueue leaves the row Proposed, which the restore
    /// reconstruction and the as-stored backfill both tolerate. Default no-op for facades that do
    /// not track frontiers (test stubs).
    /// </summary>
    void MarkInheritedCommitted(long id) { }

    /// <summary>
    /// Seeds the in-memory commit/propose frontier to a freshly installed snapshot boundary so
    /// <see cref="GetCommitIndex"/> reflects the compacted prefix as committed rather than an unfilled gap.
    /// <paramref name="snapshotTerm"/> carries the boundary's last-included term so the presence
    /// frontier's advertised (term, index) pair stays consistent after the jump.
    /// Must be called on the partition executor after the snapshot's WAL boundary is durable.
    /// <para>Default no-op: the production <c>RaftWriteAhead</c> facade overrides this; test stubs that never
    /// install snapshots inherit the no-op.</para>
    /// </summary>
    void SeedCommitFrontierFromSnapshot(long snapshotIndex, long snapshotTerm = 0) { }

    /// <summary>
    /// Removes every log entry with id &gt; <paramref name="afterLogId"/> and returns the
    /// post-truncation max log id.  The truncate and the subsequent max-log read execute inside
    /// a single scheduled WAL action so the pair is atomic: no concurrent write can be
    /// interleaved between them.
    /// <para>No-op-safe: if <paramref name="afterLogId"/> is at or above the current max, the log
    /// is unchanged and the current max is returned.</para>
    /// </summary>
    ValueTask<long> TruncateLogsAfterAsync(long afterLogId);

    /// <summary>
    /// Atomically installs a durable snapshot boundary at <paramref name="snapshotIndex"/> with term
    /// <paramref name="lastIncludedTerm"/>: stamps a <c>CommittedCheckpoint</c> there, retaining the log
    /// suffix above the index when the stored term matches (Raft log matching) and truncating it when it
    /// conflicts. The retain-vs-truncate decision is made atomically inside the backend op. Returns
    /// whether the suffix was truncated. Used by the follower-side snapshot install on the single-writer
    /// executor path. The default throws so a fake that reaches this path fails loudly rather than
    /// silently no-op'ing a durability-critical step.
    /// </summary>
    ValueTask<(Kommander.Data.RaftOperationStatus Status, bool SuffixTruncated)> InstallSnapshotBoundaryAsync(
        long snapshotIndex, long lastIncludedTerm) =>
        throw new NotSupportedException("InstallSnapshotBoundaryAsync is not implemented by this WAL facade.");

    WALWriteOperation EnqueuePropose(long term, List<RaftLog> logs, HLCTimestamp timestamp, bool autoCommit);

    WALWriteOperation EnqueueCommit(List<RaftLog> logs);

    WALWriteOperation EnqueueRollback(List<RaftLog> logs);

    WALWriteOperation? EnqueueProposeOrCommit(List<RaftLog>? logs, HLCTimestamp timestamp = default, string? endpoint = null, long term = -1);

    /// <summary>
    /// Signals that a commit/append WAL operation persisted successfully, for
    /// automatic compaction triggering.
    /// </summary>
    void NotifyCommitted();

    /// <summary>
    /// Publishes the leader's live-replica retention floor: the lowest log index a live, acking
    /// follower still needs (its replicated position + 1), or <see cref="long.MaxValue"/> when no
    /// follower constrains retention. The heartbeat round republishes it every interval;
    /// compaction holds its floor there — bounded by
    /// <see cref="RaftConfiguration.CompactionLiveReplicaLagBudget"/> and a staleness window, so a
    /// stepped-down or stalled leader's value expires on its own. Without this, a leader
    /// compacting on its ordinary cadence repeatedly pushed a live follower back below the floor
    /// and the snapshot rescue could never converge. Default no-op for facades that do not
    /// compact (test stubs).
    /// </summary>
    void SetLiveReplicaRetentionFloor(long floor) { }

    /// <summary>
    /// Persists this partition's Raft hard state — the current term and the endpoint we last granted our
    /// vote to in that term. Durability rides the backend's existing WAL fsync cadence (no dedicated
    /// fsync), so the last vote/term can be lost on power failure. The default is a no-op so non-durable
    /// test fakes simply ignore hard state.
    /// </summary>
    ValueTask PersistHardStateAsync(long currentTerm, string? votedFor) => ValueTask.CompletedTask;

    /// <summary>
    /// Loads the persisted hard state, or <see langword="null"/> when none exists yet (fresh node or a
    /// legacy WAL predating hard state), in which case the caller infers the term from the log tail. The
    /// default returns <see langword="null"/> so fakes preserve their prior behaviour.
    /// </summary>
    ValueTask<(long CurrentTerm, string? VotedFor)?> LoadHardStateAsync()
        => ValueTask.FromResult<(long CurrentTerm, string? VotedFor)?>(null);
}
