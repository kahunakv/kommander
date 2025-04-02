
using System.Diagnostics;
using System.Text;
using Kommander.Data;
using Kommander.Diagnostics;

namespace Kommander.Services;

public class ReplicationService : BackgroundService //, IDisposable
{
    private readonly IRaft raft;

    private readonly ILogger<IRaft> logger;

    public ReplicationService(IRaft raft, ILogger<IRaft> logger)
    {
        this.raft = raft;
        this.logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        logger.LogInformation("Waiting for cluster to stabilize...");
        
        await raft.JoinCluster().ConfigureAwait(false);
        
        List<Task> tasks = new(40);
        
        while (true)
        {
            tasks.Clear();
            
            for (int i = 1; i < 20; i++)
            {
                tasks.Add(ReplicateToPartition(i));
                tasks.Add(ReplicateToPartition(i));
            }

            await Task.WhenAll(tasks).ConfigureAwait(false);

            await Task.Delay(20000, stoppingToken).ConfigureAwait(false);
        }
    }

    private async Task ReplicateToPartition(int i)
    {
        try
        {
            string key = Guid.NewGuid().ToString();
            
            int partitionId = raft.GetPartitionKey(key);
            
            if (partitionId == 0)
                throw new Exception("System partition");

            if (!await raft.AmILeader(partitionId, CancellationToken.None).ConfigureAwait(false))
                return;
            
            const string logType = "Greeting";
            byte[] data = Encoding.UTF8.GetBytes("Hello, World! " + key);
            
            RaftReplicationResult result;
            ValueStopwatch stopwatch = ValueStopwatch.StartNew();

            for (int j = 0; j < 20; j++)
            {
                result = await raft.ReplicateLogs(i, logType, data).ConfigureAwait(false);
                if (result.Success)
                    Console.WriteLine("{0} #1 Replicated log with id: {1} {2}ms", partitionId, result.LogIndex, stopwatch.GetElapsedMilliseconds());
                else
                    Console.WriteLine("{0} #1 Replication failed {1} {2}ms", partitionId, result.Status, stopwatch.GetElapsedMilliseconds());

                stopwatch = ValueStopwatch.StartNew();

                result = await raft.ReplicateLogs(i, logType, [data, data, data, data, data, data, data, data]).ConfigureAwait(false);
                if (result.Success)
                    Console.WriteLine("{0} #2 Replicated log with id: {1} {2}ms", partitionId, result.LogIndex, stopwatch.GetElapsedMilliseconds());
                else
                    Console.WriteLine("{0} #2 Replication failed {1} {2}ms", partitionId, result.Status, stopwatch.GetElapsedMilliseconds());

                /*result = await raftManager.ReplicateLogs(i, logType, data).ConfigureAwait(false);
                if (result.Success)
                    Console.WriteLine("{0} #3 Replicated log with id: {1}", i, result.LogIndex);
                else
                    Console.WriteLine("{0} #3 Replication failed {1}", i, result.Status);*/
            }

            result = await raft.ReplicateCheckpoint(partitionId).ConfigureAwait(false);
            if (result.Success)
                Console.WriteLine("#C Replicated checkpoint log with id: {0}", result.LogIndex);
            else
                Console.WriteLine("#C Replication checkpoint failed {0}", result.Status);
        }
        catch (Exception ex)
        {
            Console.WriteLine("ReplicationServiceException: {0}# {1}", i, ex.Message);
        }
    }
}