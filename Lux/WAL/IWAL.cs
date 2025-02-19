
using Lux.Data;

namespace Lux.WAL;

public interface IWAL
{
    public IAsyncEnumerable<RaftLog> ReadLogs(int partitionId);

    Task Append(int partitionId, RaftLog log);
}