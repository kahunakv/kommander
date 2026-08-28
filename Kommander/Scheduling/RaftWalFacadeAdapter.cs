
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

    // Pass-throughs, not `async ... => await ...` re-wraps: the facade adds no logic, and an async
    // wrapper would box a second state machine per read on the hottest read paths.
    public ValueTask<long> GetMaxLogAsync() => new(wal.GetMaxLog());

    public ValueTask<long> GetCurrentTermAsync() => new(wal.GetCurrentTerm());

    public ValueTask<List<RaftLog>> GetRangeAsync(long startLogIndex, int maxEntries) =>
        wal.GetRangeAsync(startLogIndex, maxEntries);

    public ValueTask<List<RaftLog>> GetRangeAllTypesAsync(long startLogIndex, int maxEntries) =>
        wal.GetRangeAllTypesAsync(startLogIndex, maxEntries);

    public ValueTask<List<RaftLog>> GetRangeAllTypesAsync(long startLogIndex, int maxEntries, long maxBytes) =>
        wal.GetRangeAllTypesAsync(startLogIndex, maxEntries, maxBytes);

    public ValueTask<long> GetAnyTermAtAsync(long logIndex) =>
        wal.GetAnyTermAtAsync(logIndex);

    public ValueTask<long> GetLastCheckpointAsync() =>
        wal.GetLastCheckpointAsync();

    public long GetCommitIndex() => wal.GetCommitIndex();

    public bool HasPresenceGap() => wal.HasPresenceGap();

    public long GetPresentIndex() => wal.GetPresentIndex();

    public void SeedProposeAllocator(long nextId) => wal.SeedProposeAllocator(nextId);

    public long GetPresentTerm() => wal.GetPresentTerm();

    public ValueTask RegressFrontiersAfterFailedWriteAsync(long minLogIndex, long maxLogIndex, bool regressPresence, bool regressCommit) =>
        wal.RegressFrontiersAfterFailedWriteAsync(minLogIndex, maxLogIndex, regressPresence, regressCommit);

    public void SeedCommitFrontierFromSnapshot(long snapshotIndex, long snapshotTerm = 0) =>
        wal.SeedCommitFrontierFromSnapshot(snapshotIndex, snapshotTerm);

    public ValueTask<long> TruncateLogsAfterAsync(long afterLogId) =>
        wal.TruncateLogsAfterAsync(afterLogId);

    public ValueTask<(RaftOperationStatus Status, bool SuffixTruncated)> InstallSnapshotBoundaryAsync(
        long snapshotIndex, long lastIncludedTerm) =>
        wal.InstallSnapshotBoundaryAsync(snapshotIndex, lastIncludedTerm);

    public WALWriteOperation EnqueuePropose(long term, List<RaftLog> logs, HLCTimestamp timestamp, bool autoCommit) =>
        wal.EnqueuePropose(term, logs, timestamp, autoCommit);

    public WALWriteOperation EnqueueCommit(List<RaftLog> logs) => wal.EnqueueCommit(logs);

    public WALWriteOperation EnqueueRollback(List<RaftLog> logs) => wal.EnqueueRollback(logs);

    public WALWriteOperation? EnqueueProposeOrCommit(List<RaftLog>? logs, HLCTimestamp timestamp = default, string? endpoint = null, long term = -1) =>
        wal.EnqueueProposeOrCommit(logs, timestamp, endpoint, term);

    public void NotifyCommitted() => wal.NotifyCommitted();

    public void SetLiveReplicaRetentionFloor(long floor) => wal.SetLiveReplicaRetentionFloor(floor);

    public ValueTask PersistHardStateAsync(long currentTerm, string? votedFor)
    {
        wal.PersistHardState(currentTerm, votedFor);
        return ValueTask.CompletedTask;
    }

    public ValueTask<(long CurrentTerm, string? VotedFor)?> LoadHardStateAsync() =>
        ValueTask.FromResult(wal.LoadHardState());
}
