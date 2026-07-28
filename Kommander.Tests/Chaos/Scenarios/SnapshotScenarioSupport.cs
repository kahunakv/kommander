
using System.Collections.Concurrent;
using Kommander.Data;
using Kommander.System;
using Kommander.WAL;

namespace Kommander.Tests.Chaos.Scenarios;

/// <summary>
/// Records <see cref="ImportRange"/> calls so a scenario can assert whether (and how many times) a snapshot
/// was actually applied. <see cref="ExportRange"/> returns a small non-empty stream so the chunking logic has
/// bytes to ship. A public sibling of the private helper in <c>TestSnapshotIntegration</c>, reused by the
/// snapshot chaos scenarios.
/// </summary>
public sealed class RecordingTransfer : IRaftStateMachineTransfer
{
    private int _importCount;

    public bool ImportWasCalled => _importCount > 0;
    public int ImportCallCount => _importCount;

    public Task<Stream> ExportRange(RaftSplitPlan plan, long upToIndex, CancellationToken ct) =>
        Task.FromResult<Stream>(new MemoryStream([0xDE, 0xAD, 0xBE, 0xEF]));

    public Task ImportRange(int targetPartitionId, Stream snapshot, CancellationToken ct)
    {
        Interlocked.Increment(ref _importCount);
        return Task.CompletedTask;
    }
}

/// <summary>
/// Wraps <see cref="InMemoryWAL"/> to track <see cref="RaftLogType.CommittedCheckpoint"/> entries per
/// partition so <see cref="GetLastCheckpoint"/> returns a real floor (rather than the InMemoryWAL constant
/// <c>-1</c>) and <see cref="ReadLogsRange"/> returns empty at/below that floor — the precise condition that
/// triggers the snapshot-install path in <c>SendHeartbeat</c>. A public sibling of the private helper in
/// <c>TestSnapshotIntegration</c>, reused by the snapshot chaos scenarios.
/// </summary>
public sealed class CompactableWAL : IWAL
{
    private readonly InMemoryWAL inner;
    private readonly ConcurrentDictionary<int, long> _floors = new();

    public CompactableWAL(InMemoryWAL inner) => this.inner = inner;

    public RaftOperationStatus Write(List<(int, List<RaftLog>)> logs)
    {
        RaftOperationStatus result = inner.Write(logs);
        foreach ((int partId, List<RaftLog> partitionLogs) in logs)
            foreach (RaftLog log in partitionLogs)
                if (log.Type == RaftLogType.CommittedCheckpoint)
                    _floors.AddOrUpdate(partId, log.Id, (_, cur) => Math.Max(cur, log.Id));
        return result;
    }

    public long GetLastCheckpoint(int partitionId) =>
        _floors.TryGetValue(partitionId, out long cp) ? cp : inner.GetLastCheckpoint(partitionId);

    public List<RaftLog> ReadLogsRange(int partitionId, long startLogIndex, int maxEntries = int.MaxValue)
    {
        if (_floors.TryGetValue(partitionId, out long floor) && startLogIndex <= floor)
            return [];
        return inner.ReadLogsRange(partitionId, startLogIndex, maxEntries);
    }

    public List<RaftLog> ReadLogs(int partitionId) => inner.ReadLogs(partitionId);
    public long GetMaxLog(int partitionId) => inner.GetMaxLog(partitionId);
    public long GetCurrentTerm(int partitionId) => inner.GetCurrentTerm(partitionId);
    public int CountPersistedLogs(int partitionId) => inner.CountPersistedLogs(partitionId);
    public int CountRemovableLogs(int partitionId) => inner.CountRemovableLogs(partitionId);
    public string? GetMetaData(string key) => inner.GetMetaData(key);
    public bool SetMetaData(string key, string value) => inner.SetMetaData(key, value);
    public (RaftOperationStatus Status, int Removed) CompactLogsOlderThan(int partitionId, long lastCheckpoint, int compactNumberEntries, int? maxTotalEntries = null) =>
        inner.CompactLogsOlderThan(partitionId, lastCheckpoint, compactNumberEntries, maxTotalEntries);
    public RaftOperationStatus DeletePartitionWAL(int partitionId) => inner.DeletePartitionWAL(partitionId);
    public RaftOperationStatus TruncateLogsAfter(int partitionId, long afterLogId) => inner.TruncateLogsAfter(partitionId, afterLogId);
    public (RaftOperationStatus Status, long MaxLogId) TruncateLogsAfterAndGetMax(int partitionId, long afterLogId) => inner.TruncateLogsAfterAndGetMax(partitionId, afterLogId);
    public void Dispose() => inner.Dispose();
}
