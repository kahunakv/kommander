
namespace Lux.WAL;

public class Redis
{
    private const string ClusterWalKeyPrefix = "raft-wal-v2-";

    private const int MaxWalLength = 4096;
}