
using Kommander.Data;
using Microsoft.Extensions.Logging;

namespace Kommander.WAL;

/// <summary>
/// Keeps a log of all Raft operations in memory.
/// Useful for testing and debugging.
/// </summary>
public class InMemoryWAL : IWAL, IDisposable
{
    private readonly ReaderWriterLockSlim rwLock = new(LockRecursionPolicy.NoRecursion);

    private readonly Dictionary<string, string> allConfigs = new();

    private readonly Dictionary<int, SortedDictionary<long, RaftLog>> allLogs = new();

    private readonly ILogger<IRaft> logger;

    public InMemoryWAL(ILogger<IRaft> logger)
    {
        this.logger = logger;
    }

    public List<RaftLog> ReadLogs(int partitionId)
    {
        rwLock.EnterReadLock();
        try
        {
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
            rwLock.ExitReadLock();
        }
    }

    public List<RaftLog> ReadLogsRange(int partitionId, long startLogIndex, int maxEntries = int.MaxValue)
    {
        rwLock.EnterReadLock();
        try
        {
            List<RaftLog> result = [];

            if (allLogs.TryGetValue(partitionId, out SortedDictionary<long, RaftLog>? partitionLogs))
            {
                foreach (KeyValuePair<long, RaftLog> keyValue in partitionLogs)
                {
                    if (keyValue.Key < startLogIndex)
                        continue;
                    result.Add(keyValue.Value);
                    if (result.Count >= maxEntries)
                        break;
                }
            }

            return result;
        }
        finally
        {
            rwLock.ExitReadLock();
        }
    }

    public RaftOperationStatus Write(List<(int, List<RaftLog>)> logs)
    {
        rwLock.EnterWriteLock();
        try
        {
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
            rwLock.ExitWriteLock();
        }
    }

    public long GetMaxLog(int partitionId)
    {
        rwLock.EnterReadLock();
        try
        {
            if (allLogs.TryGetValue(partitionId, out SortedDictionary<long, RaftLog>? partitionLogs))
            {
                if (partitionLogs.Count > 0)
                    return partitionLogs.Keys.Max();
            }

            return 0;
        }
        finally
        {
            rwLock.ExitReadLock();
        }
    }

    public long GetCurrentTerm(int partitionId)
    {
        rwLock.EnterReadLock();
        try
        {
            if (allLogs.TryGetValue(partitionId, out SortedDictionary<long, RaftLog>? partitionLogs))
            {
                if (partitionLogs.Count > 0)
                    return partitionLogs[partitionLogs.Keys.Max()].Term;
            }

            return 0;
        }
        finally
        {
            rwLock.ExitReadLock();
        }
    }

    public long GetLastCheckpoint(int partitionId)
    {
        return -1;
    }

    public int CountPersistedLogs(int partitionId)
    {
        rwLock.EnterReadLock();
        try
        {
            if (!allLogs.TryGetValue(partitionId, out SortedDictionary<long, RaftLog>? partitionLogs))
                return 0;

            return partitionLogs.Count;
        }
        finally
        {
            rwLock.ExitReadLock();
        }
    }

    public int CountRemovableLogs(int partitionId)
    {
        return 0;
    }

    public string? GetMetaData(string key)
    {
        rwLock.EnterReadLock();
        try
        {
            return allConfigs.GetValueOrDefault(key);
        }
        finally
        {
            rwLock.ExitReadLock();
        }
    }

    public bool SetMetaData(string key, string value)
    {
        rwLock.EnterWriteLock();
        try
        {
            allConfigs[key] = value;
            return true;
        }
        finally
        {
            rwLock.ExitWriteLock();
        }
    }

    public RaftOperationStatus DeletePartitionWAL(int partitionId)
    {
        rwLock.EnterWriteLock();
        try
        {
            allLogs.Remove(partitionId);
            return RaftOperationStatus.Success;
        }
        finally
        {
            rwLock.ExitWriteLock();
        }
    }

    public RaftOperationStatus TruncateLogsAfter(int partitionId, long afterLogId)
    {
        rwLock.EnterWriteLock();
        try
        {
            if (!allLogs.TryGetValue(partitionId, out SortedDictionary<long, RaftLog>? partitionLogs))
                return RaftOperationStatus.Success;

            List<long> toRemove = [];
            foreach (long id in partitionLogs.Keys)
            {
                if (id > afterLogId)
                    toRemove.Add(id);
            }

            foreach (long id in toRemove)
                partitionLogs.Remove(id);

            return RaftOperationStatus.Success;
        }
        finally
        {
            rwLock.ExitWriteLock();
        }
    }

    public (RaftOperationStatus Status, int Removed) CompactLogsOlderThan(
        int partitionId,
        long lastCheckpoint,
        int compactNumberEntries,
        int? maxTotalEntries = null)
    {
        int passCap = maxTotalEntries ?? compactNumberEntries;

        rwLock.EnterWriteLock();
        try
        {
            if (!allLogs.TryGetValue(partitionId, out SortedDictionary<long, RaftLog>? partitionLogs))
                return (RaftOperationStatus.Success, 0);

            List<long> toRemove = [];

            foreach (long id in partitionLogs.Keys)
            {
                if (id >= lastCheckpoint)
                    break;

                toRemove.Add(id);

                if (toRemove.Count >= passCap)
                    break;
            }

            foreach (long id in toRemove)
                partitionLogs.Remove(id);

            return (RaftOperationStatus.Success, toRemove.Count);
        }
        finally
        {
            rwLock.ExitWriteLock();
        }
    }

    public void Dispose()
    {
        GC.SuppressFinalize(this);

        rwLock.Dispose();
    }
}
