
using Lux.Data;

namespace Lux.WAL;

public interface IWAL
{
    public IAsyncEnumerable<RaftLog> ReadLogs(int partitionId);

    public Task Append(int partitionId, RaftLog log);

    public Task AppendUpdate(int partitionId, RaftLog log);

    public Task<bool> ExistLog(int partitionId, ulong id);
}