
using System.Diagnostics;
using Flurl.Http;

using Nixie;
using System.Text.Json;
using Lux.Data;

namespace Lux;

public sealed class RaftWriteAheadActor : IActorStruct<RaftWALRequest, RaftWALResponse>
{
    private const string ClusterWalKeyPrefix = "raft-wal-v2-";

    private const int MaxWalLength = 4096;

    private const int MaxLogEntries = 10000;

    private readonly RaftManager manager;

    private readonly RaftPartition partition;

    //private readonly ILogger<IRaft> logger;

    private bool recovered;

    private ulong nextId;

    private ulong operations;

    private readonly SortedDictionary<ulong, RaftLog> logs = [];

    private readonly List<RaftLog> requestLogs = new(8);

    private readonly List<ulong> modifications = [];

    public RaftWriteAheadActor(
        IActorContextStruct<RaftWriteAheadActor, RaftWALRequest, RaftWALResponse> _, 
        RaftManager manager, 
        RaftPartition partition
    )
    {
        this.manager = manager;
        this.partition = partition;
    }

    public async Task<RaftWALResponse> Receive(RaftWALRequest message)
    {
        try
        {
            operations++;

            switch (message.Type)
            {
                case RaftWALActionType.Ping:
                    await Ping(message.Term);
                    break;

                case RaftWALActionType.Append:
                    await Append(message.Term, message.Logs);
                    break;

                case RaftWALActionType.AppendCheckpoint:
                    await AppendCheckpoint(message.Term);
                    break;

                case RaftWALActionType.Update:
                    await Update(message.Logs);
                    break;

                case RaftWALActionType.Recover:
                    return new(await Recover());
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine("[{0}/{1}] {2}\n{3}", manager.LocalEndpoint, partition.PartitionId, ex.Message, ex.StackTrace);
        }

        return new();
    }

    private async ValueTask Ping(long term)
    {
        AppendLogsRequest request = new(partition.PartitionId, term, manager.LocalEndpoint);

        string payload = JsonSerializer.Serialize(request); // , RaftJsonContext.Default.AppendLogsRequest

        await AppendLogsToNodes(payload);
    }

    private async ValueTask<ulong> Recover()
    {
        if (recovered)
            return 0;

        recovered = true;
        
        long currentTime = GetCurrentTime();
        Stopwatch stopWatch = Stopwatch.StartNew();

        manager.InvokeRestoreStarted();

        /*RedisConnection connection = await GetConnection();

        RedisValue[] values = await connection.BasicRetry(async database => await database.ListRangeAsync(ClusterWalKeyPrefix + partition.PartitionId, -MaxWalLength, -1));

        foreach (RedisValue value in values)
        {
            byte[]? data = (byte[]?)value;
            if (data is null)
                continue;

            RaftLog? log = MessagePackSerializer.Deserialize<RaftLog>(data);
            if (log is null)
                continue;

            if (logs.ContainsKey(log.Id))
            {
                logger.LogInformation("[{LocalEndpoint}/{PartitionId}] Log #{Id} is already in the WAL", RaftManager.LocalEndpoint, partition.PartitionId, log.Id);
                continue;
            }

            if (log.Time == 0 || (log.Time > 0 && (currentTime - log.Time) > 1800))
            {
                nextId = log.Id + 1;
                continue;
            }

            logs.Add(log.Id, log);

            nextId = log.Id + 1;

            await manager.InvokeReplicationReceived(log.Message);
        }

        if (values.Length == 0)
            nextId = 1;

        manager.InvokeRestoreFinished();

        //logger.LogInformation("[{LocalEndpoint}/{PartitionId}] WAL restored at #{NextId} in {Elapsed}ms", RaftManager.LocalEndpoint, partition.PartitionId, nextId - 1, stopWatch.GetElapsedMilliseconds());

        //Sender.Tell(nextId);*/

        return nextId;
    }

    private async Task Append(long term, List<RaftLog>? appendLogs)
    {
        if (appendLogs is null)
            return;
        
        long currentTime = GetCurrentTime();

        /*RedisConnection connection = await GetConnection();        

        foreach (RaftLog log in appendLogs)
        {
            log.Id = nextId++;
            log.Time = currentTime;

            byte[] command = MessagePackSerializer.Serialize(log);
            string walKey = ClusterWalKeyPrefix + partition.PartitionId;

            await connection.BasicRetry(async database => await database.ListRightPushAsync(walKey, command));

            logs.Add(log.Id, log);
        }

        requestLogs.Clear();

        for (ulong i = (nextId - 8); i < nextId; i++)
        {
            if (logs.TryGetValue(i, out RaftLog? value))
                requestLogs.Add(value);
        }

        AppendLogsRequest request = new(partition.PartitionId, term, RaftManager.LocalEndpoint, requestLogs);
        string payload = JsonSerializer.Serialize(request); // RaftJsonContext.Default.AppendLogsRequest
        await AppendLogsToNodes(payload);*/

        Collect(currentTime);

        await Compact(currentTime);
    }

    private async Task AppendCheckpoint(long term)
    {
        /*RedisConnection connection = await GetConnection();

        await connection.BasicRetry(async database => await database.KeyDeleteAsync(ClusterWalKeyPrefix + partition.PartitionId));

        RaftLog log = new()
        {
            Id = nextId++,
            Type = RaftLogType.Checkpoint,
            Time = GetCurrentTime()
        };

        logs.Add(log.Id, log);

        AppendLogsRequest request = new(partition.PartitionId, term, RaftManager.LocalEndpoint, [log]);
        string payload = JsonSerializer.Serialize(request, RaftJsonContext.Default.AppendLogsRequest);
        await AppendLogsToNodes(payload);*/

        await Task.CompletedTask;
    }

    private async Task Update(List<RaftLog>? updateLogs)
    {
        if (updateLogs is null)
            return;

        foreach (RaftLog log in updateLogs)
        {
            if (logs.ContainsKey(log.Id))
                continue;

            if (log.Id != nextId)
            {
                // logger.LogWarning("[{LocalEndpoint}/{PartitionId}] Received logs from the leader with old id={Id} expecting={NextId}. Ignoring...", RaftManager.LocalEndpoint, partition.PartitionId, log.Id, nextId);
                continue;
            }

            nextId = log.Id + 1;

            if (log.Type == RaftLogType.Checkpoint)
            {
                // logger.LogInformation("[{LocalEndpoint}/{PartitionId}] Setting WAL log to checkpoint", RaftManager.LocalEndpoint, partition.PartitionId);

                logs.Clear();
                continue;
            }

            logs.Add(log.Id, log);

            await manager.InvokeReplicationReceived(log.Message);
        }

        Collect(GetCurrentTime());
    }

    private async Task AppendLogToNode(RaftNode node, string payload)
    {
        try
        {
            await ("http://" + node.Ip)
                    .WithOAuthBearerToken("x")
                    .AppendPathSegments("v1/raft/append-logs")
                    .WithHeader("Accept", "application/json")
                    .WithHeader("Content-Type", "application/json")
                    .WithTimeout(10)
                    .WithSettings(o => o.HttpVersion = "2.0")
                    .PostStringAsync(payload)
                    .ReceiveJson<AppendLogsResponse>();
        }
        catch (Exception e)
        {
            // logger.LogWarning("[{LocalEndpoint}/{PartitionId}] {Message}", RaftManager.LocalEndpoint, partition.PartitionId, e.Message);
        }
    }

    private async Task AppendLogsToNodes(string payload)
    {
        if (manager.Nodes.Count == 0)
        {
            // logger.LogWarning("[{LocalEndpoint}/{PartitionId}] No other nodes availables to replicate logs", RaftManager.LocalEndpoint, partition.PartitionId);
            return;
        }

        List<RaftNode> nodes = manager.Nodes;

        List<Task> tasks = new(nodes.Count);

        foreach (RaftNode node in nodes)
            tasks.Add(AppendLogToNode(node, payload));

        await Task.WhenAll(tasks);
    }

    private static long GetCurrentTime()
    {
        return ((DateTimeOffset)DateTime.UtcNow).ToUnixTimeMilliseconds();
    }

    private void Collect(long currentTime)
    {
        if (logs.Count < MaxLogEntries)
            return;

        if (operations % 500 != 0)
            return;

        modifications.Clear();

        foreach (KeyValuePair<ulong, RaftLog> keyValue in logs)
        {
            RaftLog raftLog = keyValue.Value;

            if (raftLog.Time > 0 && (currentTime - raftLog.Time) > 1800)
                modifications.Add(keyValue.Key);
        }

        if (modifications.Count == 0)
            return;

        foreach (ulong index in modifications)
            logs.Remove(index);
    }

    private async Task Compact(long currentTime)
    {
        if (operations % 500 != 0)
            return;

        /*RedisConnection connection = await GetConnection();

        string key = ClusterWalKeyPrefix + partition.PartitionId;

        long length = await connection.BasicRetry(async database => await database.ListLengthAsync(key));

        if (length < MaxLogEntries)
            return;

        RedisValue[] values = await connection.BasicRetry(async database => await database.ListRangeAsync(ClusterWalKeyPrefix + partition.PartitionId, 0, 1024));

        int oldest = 0;

        foreach (RedisValue value in values)
        {
            byte[]? data = (byte[]?)value;
            if (data is null)
                continue;

            RaftLog? raftLog = MessagePackSerializer.Deserialize<RaftLog>(data);
            if (raftLog is null)
                continue;

            if (raftLog.Time > 0 && (currentTime - raftLog.Time) > 1800)
                oldest++;
        }

        if (oldest == 0)
            return;

        // logger.LogWarning("[{LocalEndpoint}/{PartitionId}] Compacting log at #{Oldest}", RaftManager.LocalEndpoint, partition.PartitionId, oldest);

        await connection.BasicRetry(async database =>
        {
            await database.ListTrimAsync(key, 0, oldest - 1);
            return Task.CompletedTask;
        });*/

        await Task.CompletedTask;
    }
}
