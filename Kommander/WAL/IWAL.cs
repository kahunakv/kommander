
using Kommander.Data;

namespace Kommander.WAL;

public interface IWAL
{
    public IAsyncEnumerable<RaftLog> ReadLogs(int partitionId);

    public IAsyncEnumerable<RaftLog> ReadLogsRange(int partitionId, ulong startLogIndex, ulong endLogIndex);

    public Task Append(int partitionId, RaftLog log);

    public Task AppendUpdate(int partitionId, RaftLog log);

    public Task<bool> ExistLog(int partitionId, ulong id);
    
    public Task<ulong> GetMaxLog(int partitionId);
    
    public Task<long> GetCurrentTerm(int partitionId);
}