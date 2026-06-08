
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
