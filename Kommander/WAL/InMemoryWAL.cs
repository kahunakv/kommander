using Kommander.Data;

namespace Kommander.WAL;

public class InMemoryWAL : IWAL
{
    private readonly Dictionary<int, SortedDictionary<ulong, RaftLog>> logs = new();
    
    public async IAsyncEnumerable<RaftLog> ReadLogs(int partitionId)
    {
        await Task.CompletedTask;
        
        if (logs.TryGetValue(partitionId, out SortedDictionary<ulong, RaftLog>? partitionLogs))
        {
            foreach (KeyValuePair<ulong, RaftLog> keyValue in partitionLogs)
                yield return keyValue.Value;
        }
    }

    public IAsyncEnumerable<RaftLog> ReadLogsRange(int partitionId, ulong startLogIndex, ulong endLogIndex)
    {
        throw new NotImplementedException();
    }

    public async Task Append(int partitionId, RaftLog log)
    {
        await Task.CompletedTask;
        
        if (logs.TryGetValue(partitionId, out SortedDictionary<ulong, RaftLog>? partitionLogs))
            partitionLogs.Add(log.Id, log);
        else
            logs.Add(partitionId, new() {{ log.Id, log }});
    }

    public async Task AppendUpdate(int partitionId, RaftLog log)
    {
        await Append(partitionId, log);
    }

    public Task<bool> ExistLog(int partitionId, ulong id)
    {
        if (logs.TryGetValue(partitionId, out SortedDictionary<ulong, RaftLog>? partitionLogs))
            return Task.FromResult(partitionLogs.ContainsKey(id));
        
        return Task.FromResult(false);
    }

    public Task<ulong> GetMaxLog(int partitionId)
    {
        if (logs.TryGetValue(partitionId, out SortedDictionary<ulong, RaftLog>? partitionLogs))
        {
            if (partitionLogs.Count > 0)
                return Task.FromResult(partitionLogs.Keys.Max());
        }

        return Task.FromResult<ulong>(0);
    }
}