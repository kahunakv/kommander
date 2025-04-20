
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

    public string? GetMetaData(string key);
    
    public bool SetMetaData(string key, string value);
    
    public RaftOperationStatus CompactLogsOlderThan(int partitionId, long lastCheckpoint, int compactNumberEntries);

    public void Dispose();
}