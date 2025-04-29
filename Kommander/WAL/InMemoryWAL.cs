
using Kommander.Data;
using Microsoft.Extensions.Logging;

namespace Kommander.WAL;

/// <summary>
/// Keeps a log of all Raft operations in memory
/// Useful for testing and debugging
/// </summary>
public class InMemoryWAL : IWAL, IDisposable
{
    private readonly ReaderWriterLock readerWriterLock = new();
    
    private readonly Dictionary<string, string> allConfigs = new();
    
    private readonly Dictionary<int, SortedDictionary<long, RaftLog>> allLogs = new();

    private readonly ILogger<IRaft> logger;
    
    public InMemoryWAL(ILogger<IRaft> logger)
    {
        this.logger = logger;
    }
    
    public List<RaftLog> ReadLogs(int partitionId)
    {
        try
        {
            readerWriterLock.AcquireReaderLock(TimeSpan.FromMilliseconds(5000));
            
            List<RaftLog> result = [];

            if (allLogs.TryGetValue(partitionId, out SortedDictionary<long, RaftLog>? partitionLogs))
            {
                foreach (KeyValuePair<long, RaftLog> keyValue in partitionLogs)
                    result.Add(keyValue.Value);
            }

            return result;
        }
        finally
        {
            readerWriterLock.ReleaseReaderLock();    
        }
    }

    public List<RaftLog> ReadLogsRange(int partitionId, long startLogIndex)
    {
        try
        {
            readerWriterLock.AcquireReaderLock(TimeSpan.FromMilliseconds(5000));
        
            List<RaftLog> result = [];
            
            if (allLogs.TryGetValue(partitionId, out SortedDictionary<long, RaftLog>? partitionLogs))
            {
                foreach (KeyValuePair<long, RaftLog> keyValue in partitionLogs)
                {
                    if (keyValue.Key >= startLogIndex)
                        result.Add(keyValue.Value);
                }
            }

            return result;
        }
        finally
        {
            readerWriterLock.ReleaseReaderLock();    
        }
    }

    public RaftOperationStatus Write(List<(int, List<RaftLog>)> logs)
    {
        try
        {
            readerWriterLock.AcquireWriterLock(TimeSpan.FromMilliseconds(5000));

            foreach ((int partitionId, List<RaftLog> raftLogs) item in logs)
            {
                foreach (RaftLog log in item.raftLogs)
                {
                    if (allLogs.TryGetValue(item.partitionId, out SortedDictionary<long, RaftLog>? partitionLogs))
                        partitionLogs[log.Id] = log;
                    else
                        allLogs.Add(item.partitionId, new() { { log.Id, log } });
                }
            }

            return RaftOperationStatus.Success;
        }
        finally
        {
            readerWriterLock.ReleaseWriterLock();
        }
    }

    public long GetMaxLog(int partitionId)
    {
        try
        {
            readerWriterLock.AcquireReaderLock(TimeSpan.FromMilliseconds(5000));
        
            if (allLogs.TryGetValue(partitionId, out SortedDictionary<long, RaftLog>? partitionLogs))
            {
                if (partitionLogs.Count > 0)
                    return partitionLogs.Keys.Max();
            }

            return 0;
        }
        finally
        {
            readerWriterLock.ReleaseReaderLock();
        }
    }

    public long GetCurrentTerm(int partitionId)
    {
        try
        {
            readerWriterLock.AcquireReaderLock(TimeSpan.FromMilliseconds(5000));

            if (allLogs.TryGetValue(partitionId, out SortedDictionary<long, RaftLog>? partitionLogs))
            {
                if (partitionLogs.Count > 0)
                    return partitionLogs[partitionLogs.Keys.Max()].Term;
            }

            return 0;
        }
        finally
        {
            readerWriterLock.ReleaseReaderLock();
        }
    }

    public long GetLastCheckpoint(int partitionId)
    {
        return -1;
    }

    public string? GetMetaData(string key)
    {
        try
        {
            readerWriterLock.AcquireReaderLock(TimeSpan.FromMilliseconds(5000));

            return allConfigs.GetValueOrDefault(key);
        }
        finally
        {
            readerWriterLock.ReleaseReaderLock();
        }
    }

    public bool SetMetaData(string key, string value)
    {
        allConfigs[key] = value;
        return true;
    }

    public RaftOperationStatus CompactLogsOlderThan(int partitionId, long lastCheckpoint, int compactNumberEntries)
    {
        return RaftOperationStatus.Success;
    }

    public void Dispose()
    {
        GC.SuppressFinalize(this);
        
        // TODO release managed resources here
    }
}