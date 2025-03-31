
using Kommander.Data;

namespace Kommander.WAL;

/// <summary>
/// Keeps a log of all Raft operations in memory
/// Useful for testing and debugging
/// </summary>
public class InMemoryWAL : IWAL
{
    private readonly ReaderWriterLock readerWriterLock = new();
    
    private readonly Dictionary<string, string> allConfigs = new();
    
    private readonly Dictionary<int, SortedDictionary<long, RaftLog>> allLogs = new();
    
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

    public RaftOperationStatus Propose(int partitionId, RaftLog log)
    {
        if (log.Type != RaftLogType.Proposed && log.Type != RaftLogType.ProposedCheckpoint)
            throw new RaftException("Log must be proposed or proposed checkpoint");

        try
        {
            readerWriterLock.AcquireWriterLock(TimeSpan.FromMilliseconds(5000));

            if (allLogs.TryGetValue(partitionId, out SortedDictionary<long, RaftLog>? partitionLogs))
                partitionLogs.Add(log.Id, log);
            else
                allLogs.Add(partitionId, new() { { log.Id, log } });

            return RaftOperationStatus.Success;
        }
        finally
        {
            readerWriterLock.ReleaseWriterLock();
        }
    }
    
    public RaftOperationStatus Commit(int partitionId, RaftLog log)
    {
        try
        {
            readerWriterLock.AcquireWriterLock(TimeSpan.FromMilliseconds(5000));

            if (log.Type != RaftLogType.Committed && log.Type != RaftLogType.CommittedCheckpoint)
                throw new RaftException("Log must be committed or committed checkpoint");

            if (allLogs.TryGetValue(partitionId, out SortedDictionary<long, RaftLog>? partitionLogs))
                partitionLogs[log.Id] = log; // Always replace the log
            else
                allLogs.Add(partitionId, new() { { log.Id, log } });

            return RaftOperationStatus.Success;
        }
        finally
        {
            readerWriterLock.ReleaseWriterLock();
        }
    }
    
    public RaftOperationStatus Rollback(int partitionId, RaftLog log)
    {
        if (log.Type != RaftLogType.RolledBack && log.Type != RaftLogType.RolledBackCheckpoint)
            throw new RaftException("Log must be rolledback or rolledback checkpoint");

        try
        {
            readerWriterLock.AcquireWriterLock(TimeSpan.FromMilliseconds(5000));

            if (allLogs.TryGetValue(partitionId, out SortedDictionary<long, RaftLog>? partitionLogs))
                partitionLogs[log.Id] = log; // Always replace the log
            else
                allLogs.Add(partitionId, new() { { log.Id, log } });

            return RaftOperationStatus.Success;
        }
        finally
        {
            readerWriterLock.ReleaseWriterLock();
        }
    }

    public RaftOperationStatus ProposeMany(int partitionId, List<RaftLog> logs)
    {
        try
        {
            readerWriterLock.AcquireWriterLock(TimeSpan.FromMilliseconds(5000));

            foreach (RaftLog log in logs)
            {
                if (log.Type != RaftLogType.Proposed && log.Type != RaftLogType.ProposedCheckpoint)
                    throw new RaftException("Log must be proposed or proposed checkpoint: " + log.Type);

                if (allLogs.TryGetValue(partitionId, out SortedDictionary<long, RaftLog>? partitionLogs))
                    partitionLogs.Add(log.Id, log);
                else
                    allLogs.Add(partitionId, new() { { log.Id, log } });
            }

            return RaftOperationStatus.Success;
        }
        finally
        {
            readerWriterLock.ReleaseWriterLock();
        }
    }

    public RaftOperationStatus CommitMany(int partitionId, List<RaftLog> logs)
    {
        try
        {
            readerWriterLock.AcquireWriterLock(TimeSpan.FromMilliseconds(5000));

            foreach (RaftLog log in logs)
            {
                if (log.Type != RaftLogType.Committed && log.Type != RaftLogType.CommittedCheckpoint)
                    throw new RaftException("Log must be committed or committed checkpoint");

                if (allLogs.TryGetValue(partitionId, out SortedDictionary<long, RaftLog>? partitionLogs))
                    partitionLogs[log.Id] = log;
                else
                    allLogs.Add(partitionId, new() { { log.Id, log } });
            }

            return RaftOperationStatus.Success;
        }
        finally
        {
            readerWriterLock.ReleaseWriterLock();
        }
    }

    public RaftOperationStatus RollbackMany(int partitionId, List<RaftLog> logs)
    {
        try
        {
            readerWriterLock.AcquireWriterLock(TimeSpan.FromMilliseconds(5000));

            foreach (RaftLog log in logs)
            {
                if (log.Type != RaftLogType.RolledBack && log.Type != RaftLogType.RolledBackCheckpoint)
                    throw new RaftException("Log must be rolledback or rolledback checkpoint");

                if (allLogs.TryGetValue(partitionId, out SortedDictionary<long, RaftLog>? partitionLogs))
                    partitionLogs[log.Id] = log;
                else
                    allLogs.Add(partitionId, new() { { log.Id, log } });
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
}