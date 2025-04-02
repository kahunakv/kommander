
using Kommander.Data;

namespace Kommander.WAL;

public interface IWAL
{
    public List<RaftLog> ReadLogs(int partitionId);

    public List<RaftLog> ReadLogsRange(int partitionId, long startLogIndex);

    public RaftOperationStatus Propose(int partitionId, RaftLog log);
    
    public RaftOperationStatus Commit(int partitionId, RaftLog log);
    
    public RaftOperationStatus Rollback(int partitionId, RaftLog log);
    
    public RaftOperationStatus ProposeMany(int partitionId, List<RaftLog> logs);
    
    public RaftOperationStatus CommitMany(int partitionId, List<RaftLog> logs);
    
    public RaftOperationStatus RollbackMany(int partitionId, List<RaftLog> logs);
    
    public long GetMaxLog(int partitionId);
    
    public long GetCurrentTerm(int partitionId);

    public long GetLastCheckpoint(int partitionId);

    public string? GetMetaData(string key);
    
    public bool SetMetaData(string key, string value);
    
    public RaftOperationStatus CompactLogsOlderThan(int partitionId, long lastCheckpoint, int compactNumberEntries);
}