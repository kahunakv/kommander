
using System.Diagnostics;
using Flurl.Http;

using Nixie;
using System.Text.Json;
using Lux.Data;
using Lux.WAL;

namespace Lux;

public sealed class RaftWriteAheadActor : IActorStruct<RaftWALRequest, RaftWALResponse>
{
    private const string ClusterWalKeyPrefix = "raft-wal-v2-";

    private const int MaxWalLength = 4096;

    private const int MaxLogEntries = 10000;

    private readonly RaftManager manager;

    private readonly RaftPartition partition;

    private readonly IWAL walAdapter;

    //private readonly ILogger<IRaft> logger;

    private bool recovered;

    private ulong nextId = 1;

    private ulong operations;

    private readonly SortedDictionary<ulong, RaftLog> logs = [];

    private readonly List<ulong> modifications = [];

    public RaftWriteAheadActor(
        IActorContextStruct<RaftWriteAheadActor, RaftWALRequest, RaftWALResponse> _, 
        RaftManager manager, 
        RaftPartition partition,
        IWAL walAdapter
    )
    {
        this.manager = manager;
        this.partition = partition;
        this.walAdapter = walAdapter;
    }

    public async Task<RaftWALResponse> Receive(RaftWALRequest message)
    {
        try
        {
            operations++;

            switch (message.Type)
            {
                case RaftWALActionType.Append:
                    return new(await Append(message.Logs));

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

    private async ValueTask<ulong> Recover()
    {
        if (recovered)
            return 0;

        await Task.CompletedTask;

        recovered = true;
        
        long currentTime = GetCurrentTime();
        Stopwatch stopWatch = Stopwatch.StartNew();

        manager.InvokeRestoreStarted();

        await foreach (RaftLog log in walAdapter.ReadLogs(partition.PartitionId))
        {
            if (logs.ContainsKey(log.Id))
            {
                Console.WriteLine("[{0}/{1}] Log #{2} is already in the WAL", manager.LocalEndpoint, partition.PartitionId, log.Id);
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

        //if (values.Length == 0)
        //    nextId = 1;

        manager.InvokeRestoreFinished();

        Console.WriteLine("[{0}/{1}] WAL restored at #{2} in {3}ms", manager.LocalEndpoint, partition.PartitionId, nextId - 1, stopWatch.ElapsedMilliseconds);

        return nextId;
    }

    private async Task<List<RaftLog>> Append(List<RaftLog>? appendLogs)
    {
        if (appendLogs is null)
            return [];
        
        long currentTime = GetCurrentTime();

        foreach (RaftLog log in appendLogs)
        {
            log.Id = nextId++;
            log.Time = currentTime;
            logs.Add(log.Id, log);
            
            await walAdapter.Append(partition.PartitionId, log);
        }

        List<RaftLog> requestLogs = new(8);

        for (ulong i = (nextId - 8); i < nextId; i++)
        {
            if (logs.TryGetValue(i, out RaftLog? value))
                requestLogs.Add(value);
        }

        Collect(currentTime);

        await Compact(currentTime);

        return requestLogs;
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
                Console.WriteLine("[{0}/{1}] Received logs from the leader with old id={2} expecting={3}. Ignoring...", manager.LocalEndpoint, partition.PartitionId, log.Id, nextId);
                continue;
            }

            nextId = log.Id + 1;

            if (log.Type == RaftLogType.Checkpoint)
            {
                Console.WriteLine("[{0}/{1}] Setting WAL log to checkpoint", manager.LocalEndpoint, partition.PartitionId);

                logs.Clear();
                continue;
            }

            logs.Add(log.Id, log);
            
            await walAdapter.AppendUpdate(partition.PartitionId, log);

            await manager.InvokeReplicationReceived(log.Message);
        }

        Collect(GetCurrentTime());
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
