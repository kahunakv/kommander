
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

    public int CountPersistedLogs(int partitionId)
    {
        try
        {
            readerWriterLock.AcquireReaderLock(TimeSpan.FromMilliseconds(5000));

            if (!allLogs.TryGetValue(partitionId, out SortedDictionary<long, RaftLog>? partitionLogs))
                return 0;

            return partitionLogs.Count;
        }
        finally
        {
            readerWriterLock.ReleaseReaderLock();
        }
    }

    public int CountRemovableLogs(int partitionId)
    {
        return 0;
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

    public (RaftOperationStatus Status, int Removed) CompactLogsOlderThan(int partitionId, long lastCheckpoint, int compactNumberEntries)
    {
        try
        {
            readerWriterLock.AcquireWriterLock(TimeSpan.FromMilliseconds(5000));

            if (!allLogs.TryGetValue(partitionId, out SortedDictionary<long, RaftLog>? partitionLogs))
                return (RaftOperationStatus.Success, 0);

            List<long> toRemove = [];

            foreach (long id in partitionLogs.Keys)
            {
                if (id >= lastCheckpoint)
                    break;

                toRemove.Add(id);

                if (toRemove.Count >= compactNumberEntries)
                    break;
            }

            foreach (long id in toRemove)
                partitionLogs.Remove(id);

            return (RaftOperationStatus.Success, toRemove.Count);
        }
        finally
        {
            readerWriterLock.ReleaseWriterLock();
        }
    }

    public void Dispose()
    {
        GC.SuppressFinalize(this);
        
        // TODO release managed resources here
    }
}