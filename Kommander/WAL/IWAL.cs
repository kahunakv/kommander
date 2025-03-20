
using Kommander.Data;

namespace Kommander.WAL;

public interface IWAL
{
    public IAsyncEnumerable<RaftLog> ReadLogs(int partitionId);

    public IAsyncEnumerable<RaftLog> ReadLogsRange(int partitionId, long startLogIndex);

    public Task Propose(int partitionId, RaftLog log);
    
    public Task Commit(int partitionId, RaftLog log);
    
    public Task Rollback(int partitionId, RaftLog log);
    
    public Task<long> GetMaxLog(int partitionId);
    
    public Task<long> GetCurrentTerm(int partitionId);
}