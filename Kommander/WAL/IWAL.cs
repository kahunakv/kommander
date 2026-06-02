
using Kommander.Data;

namespace Kommander.WAL;

public interface IWAL
{
    public List<RaftLog> ReadLogs(int partitionId);

    public List<RaftLog> ReadLogsRange(int partitionId, long startLogIndex);

    public RaftOperationStatus Write(List<(int, List<RaftLog>)> logs);
    
    public long GetMaxLog(int partitionId);
    
    public long GetCurrentTerm(int partitionId);

    public long GetLastCheckpoint(int partitionId);

    /// <summary>
    /// Returns the total number of persisted log rows for the partition.
    /// Unlike <see cref="ReadLogsRange"/>, this is not capped by a range limit.
    /// </summary>
    public int CountPersistedLogs(int partitionId);

    /// <summary>
    /// Returns the number of persisted log rows with id strictly below the partition's
    /// last committed checkpoint. Returns 0 when no checkpoint exists.
    /// </summary>
    public int CountRemovableLogs(int partitionId);

    public string? GetMetaData(string key);
    
    public bool SetMetaData(string key, string value);
    
    public (RaftOperationStatus Status, int Removed) CompactLogsOlderThan(int partitionId, long lastCheckpoint, int compactNumberEntries);

    public void Dispose();
}