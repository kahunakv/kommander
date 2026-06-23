
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
    /// Removes every log entry with id &gt; <paramref name="afterLogId"/> and returns the
    /// post-truncation max log id.  The truncate and the subsequent max-log read execute inside
    /// a single scheduled WAL action so the pair is atomic: no concurrent write can be
    /// interleaved between them.
    /// <para>No-op-safe: if <paramref name="afterLogId"/> is at or above the current max, the log
    /// is unchanged and the current max is returned.</para>
    /// </summary>
    ValueTask<long> TruncateLogsAfterAsync(long afterLogId);

    WALWriteOperation EnqueuePropose(long term, List<RaftLog> logs, HLCTimestamp timestamp, bool autoCommit);

    WALWriteOperation EnqueueCommit(List<RaftLog> logs);

    WALWriteOperation EnqueueRollback(List<RaftLog> logs);

    WALWriteOperation? EnqueueProposeOrCommit(List<RaftLog>? logs, HLCTimestamp timestamp = default, string? endpoint = null, long term = -1);

    /// <summary>
    /// Signals that a commit/append WAL operation persisted successfully, for
    /// automatic compaction triggering.
    /// </summary>
    void NotifyCommitted();
}
