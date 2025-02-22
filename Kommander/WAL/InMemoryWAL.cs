using Kommander.Data;

namespace Kommander.WAL;

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

    public async Task Append(int partitionId, RaftLog log)
    {
        await Task.CompletedTask;
        
        if (logs.TryGetValue(partitionId, out SortedDictionary<long, RaftLog>? partitionLogs))
            partitionLogs.Add(log.Id, log);
        else
            logs.Add(partitionId, new() {{ log.Id, log }});
    }

    public async Task AppendUpdate(int partitionId, RaftLog log)
    {
        await Append(partitionId, log);
    }

    public Task<bool> ExistLog(int partitionId, long id)
    {
        if (logs.TryGetValue(partitionId, out SortedDictionary<long, RaftLog>? partitionLogs))
            return Task.FromResult(partitionLogs.ContainsKey(id));
        
        return Task.FromResult(false);
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