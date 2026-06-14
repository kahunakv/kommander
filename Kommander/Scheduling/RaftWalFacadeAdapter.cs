
using Kommander.Data;
using Kommander.Time;
using Kommander.WAL.Data;

namespace Kommander;

/// <summary>
/// Bridges <see cref="RaftWriteAhead"/> to <see cref="Scheduling.IRaftWalFacade"/>.
/// </summary>
internal sealed class RaftWalFacadeAdapter : Scheduling.IRaftWalFacade
{
    private readonly RaftWriteAhead wal;

    public RaftWalFacadeAdapter(RaftWriteAhead wal) => this.wal = wal;

    public ValueTask<IReadOnlyList<RaftLog>> LoadRestoreLogsAsync() => wal.LoadRestoreLogsAsync();

    public ValueTask CompleteRestoreAsync(IReadOnlyList<RaftLog> logs) => wal.CompleteRestoreAsync(logs);

    public async ValueTask<long> GetMaxLogAsync() => await wal.GetMaxLog().ConfigureAwait(false);

    public async ValueTask<long> GetCurrentTermAsync() => await wal.GetCurrentTerm().ConfigureAwait(false);

    public async ValueTask<List<RaftLog>> GetRangeAsync(long startLogIndex, int maxEntries) =>
        await wal.GetRangeAsync(startLogIndex, maxEntries).ConfigureAwait(false);

    public async ValueTask<long> GetLastCheckpointAsync() =>
        await wal.GetLastCheckpointAsync().ConfigureAwait(false);

    public long GetCommitIndex() => wal.GetCommitIndex();

    public WALWriteOperation EnqueuePropose(long term, List<RaftLog> logs, HLCTimestamp timestamp, bool autoCommit) =>
        wal.EnqueuePropose(term, logs, timestamp, autoCommit);

    public WALWriteOperation EnqueueCommit(List<RaftLog> logs) => wal.EnqueueCommit(logs);

    public WALWriteOperation EnqueueRollback(List<RaftLog> logs) => wal.EnqueueRollback(logs);

    public WALWriteOperation? EnqueueProposeOrCommit(List<RaftLog>? logs, HLCTimestamp timestamp = default, string? endpoint = null, long term = -1) =>
        wal.EnqueueProposeOrCommit(logs, timestamp, endpoint, term);

    public void NotifyCommitted() => wal.NotifyCommitted();
}
