using Kommander.Data;
using RocksDbSharp;

namespace Kommander.WAL;

public class RocksDbWAL : IWAL
{
    private readonly RocksDb db;
    
    private readonly string path;
    
    private readonly string revision;
    
    public RocksDbWAL(string path, string revision)
    {
        this.path = path;
        this.revision = revision;

        DbOptions dbOptions = new DbOptions()
            .SetCreateIfMissing(true)
            .SetCreateMissingColumnFamilies(true)
            .SetWalRecoveryMode(Recovery.AbsoluteConsistency);
        
        ColumnFamilies columnFamilies = new ColumnFamilies
        {
            { "default", new ColumnFamilyOptions() },
            { "user_data", new ColumnFamilyOptions() }
        };
        
        this.db = RocksDb.Open(dbOptions, $"{path}/{revision}", columnFamilies);

        var handle = db.GetColumnFamily("default");
    }

    public async IAsyncEnumerable<RaftLog> ReadLogs(int partitionId)
    {
        await Task.Yield();
        
        yield return new RaftLog();
    }

    public async IAsyncEnumerable<RaftLog> ReadLogsRange(int partitionId, long startLogIndex)
    {
        await Task.Yield();
        
        yield return new RaftLog();
    }

    public async Task Append(int partitionId, RaftLog log)
    {
        await Task.Yield();
    }

    public async Task AppendUpdate(int partitionId, RaftLog log)
    {
        await Task.Yield();
    }

    public async Task<long> GetMaxLog(int partitionId)
    {
        await Task.Yield();

        return 0;
    }

    public async Task<long> GetCurrentTerm(int partitionId)
    {
        await Task.Yield();

        return 0;
    }
}