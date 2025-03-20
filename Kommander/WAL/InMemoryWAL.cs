using Kommander.Data;

namespace Kommander.WAL;

/// <summary>
/// Keeps a log of all Raft operations in memory
/// Useful for testing and debugging
/// </summary>
public class InMemoryWAL : IWAL
{
    private readonly Dictionary<int, SortedDictionary<long, RaftLog>> logs = new();
    
    public async IAsyncEnumerable<RaftLog> ReadLogs(int partitionId)
    {
        await Task.CompletedTask;
        
        if (logs.TryGetValue(partitionId, out SortedDictionary<long, RaftLog>? partitionLogs))
        {
            foreach (KeyValuePair<long, RaftLog> keyValue in partitionLogs)
                yield return keyValue.Value;
        }
    }

    public async IAsyncEnumerable<RaftLog> ReadLogsRange(int partitionId, long startLogIndex)
    {
        await Task.CompletedTask;
        
        if (logs.TryGetValue(partitionId, out SortedDictionary<long, RaftLog>? partitionLogs))
        {
            foreach (KeyValuePair<long, RaftLog> keyValue in partitionLogs)
            {
                if (keyValue.Key >= startLogIndex)
                    yield return keyValue.Value;
            }
        }
    }

    public Task Propose(int partitionId, RaftLog log)
    {
        if (log.Type != RaftLogType.Proposed && log.Type != RaftLogType.ProposedCheckpoint)
            throw new RaftException("Log must be proposed or proposed checkpoint");
        
        if (logs.TryGetValue(partitionId, out SortedDictionary<long, RaftLog>? partitionLogs))
            partitionLogs.Add(log.Id, log);
        else
            logs.Add(partitionId, new() {{ log.Id, log }});
        
        return Task.CompletedTask;
    }
    
    public Task Commit(int partitionId, RaftLog log)
    {
        if (log.Type != RaftLogType.Committed && log.Type != RaftLogType.CommittedCheckpoint)
            throw new RaftException("Log must be committed or committed checkpoint");
        
        if (logs.TryGetValue(partitionId, out SortedDictionary<long, RaftLog>? partitionLogs))
            partitionLogs[log.Id] = log; // Always replace the log
        else
            logs.Add(partitionId, new() {{ log.Id, log }});

        return Task.CompletedTask;
    }
    
    public Task Rollback(int partitionId, RaftLog log)
    {
        if (log.Type != RaftLogType.RolledBack && log.Type != RaftLogType.RolledBackCheckpoint)
            throw new RaftException("Log must be rolledback or rolledback checkpoint");
        
        if (logs.TryGetValue(partitionId, out SortedDictionary<long, RaftLog>? partitionLogs))
            partitionLogs[log.Id] = log; // Always replace the log
        else
            logs.Add(partitionId, new() {{ log.Id, log }});

        return Task.CompletedTask;
    }

    public Task<long> GetMaxLog(int partitionId)
    {
        if (logs.TryGetValue(partitionId, out SortedDictionary<long, RaftLog>? partitionLogs))
        {
            if (partitionLogs.Count > 0)
                return Task.FromResult(partitionLogs.Keys.Max());
        }

        return Task.FromResult<long>(0);
    }

    public Task<long> GetCurrentTerm(int partitionId)
    {
        if (logs.TryGetValue(partitionId, out SortedDictionary<long, RaftLog>? partitionLogs))
        {
            if (partitionLogs.Count > 0)
                return Task.FromResult(partitionLogs[partitionLogs.Keys.Max()].Term);
        }

        return Task.FromResult<long>(0);
    }
}