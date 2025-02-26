
using Kommander.Data;

namespace Kommander.WAL;

public interface IWAL
{
    public IAsyncEnumerable<RaftLog> ReadLogs(int partitionId);

    public IAsyncEnumerable<RaftLog> ReadLogsRange(int partitionId, long startLogIndex);

    public Task Append(int partitionId, RaftLog log);

    public Task AppendUpdate(int partitionId, RaftLog log);
    
    public Task<long> GetMaxLog(int partitionId);
    
    public Task<long> GetCurrentTerm(int partitionId);
}