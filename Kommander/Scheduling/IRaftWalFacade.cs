
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
    ValueTask<long> RecoverAsync();

    ValueTask<long> GetMaxLogAsync();

    ValueTask<long> GetCurrentTermAsync();

    WALWriteOperation EnqueuePropose(long term, List<RaftLog> logs, HLCTimestamp timestamp, bool autoCommit);

    WALWriteOperation EnqueueCommit(List<RaftLog> logs);

    WALWriteOperation EnqueueRollback(List<RaftLog> logs);

    WALWriteOperation? EnqueueProposeOrCommit(List<RaftLog>? logs, HLCTimestamp timestamp = default, string? endpoint = null, long term = -1);
}
