
using System.Diagnostics;
using System.Text;
using Kommander.Data;

namespace Kommander.Services;

public class ReplicationService : BackgroundService //, IDisposable
{
    private readonly IRaft raftManager;

    private readonly ILogger<IRaft> logger;

    public ReplicationService(IRaft raftManager, ILogger<IRaft> logger)
    {
        this.raftManager = raftManager;
        this.logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await raftManager.JoinCluster().ConfigureAwait(false);
        
        await raftManager.UpdateNodes().ConfigureAwait(false);
        
        logger.LogInformation("Waiting for cluster to stabilize...");
        
        await Task.Delay(30000, stoppingToken).ConfigureAwait(false);
        
        while (true)
        {
            List<Task> tasks = new(raftManager.Configuration.MaxPartitions);

            for (int i = 0; i < raftManager.Configuration.MaxPartitions; i++)
                tasks.Add(ReplicateToPartition(i));
            
            await Task.WhenAll(tasks).ConfigureAwait(false);

            await Task.Delay(20000, stoppingToken).ConfigureAwait(false);
        }
    }

    private async Task ReplicateToPartition(int i)
    {
        try
        {
            if (await raftManager.AmILeader(i, CancellationToken.None).ConfigureAwait(false))
            {
                const string logType = "Greeting";
                byte[] data = Encoding.UTF8.GetBytes("Hello, World! " + DateTime.UtcNow);
                
                RaftReplicationResult result;
                Stopwatch stopwatch = Stopwatch.StartNew();

                for (int j = 0; j < 20; j++)
                {
                    result = await raftManager.ReplicateLogs(i, logType, data).ConfigureAwait(false);
                    if (result.Success)
                        Console.WriteLine("{0} #1 Replicated log with id: {1} {2}ms", i, result.LogIndex, stopwatch.ElapsedMilliseconds);
                    else
                        Console.WriteLine("{0} #1 Replication failed {1} {2}ms", i, result.Status, stopwatch.ElapsedMilliseconds);
                    
                    stopwatch.Restart();

                    result = await raftManager.ReplicateLogs(i, logType, data).ConfigureAwait(false);
                    if (result.Success)
                        Console.WriteLine("{0} #2 Replicated log with id: {1} {2}ms", i, result.LogIndex, stopwatch.ElapsedMilliseconds);
                    else
                        Console.WriteLine("{0} #2 Replication failed {1} {2}ms", i, result.Status, stopwatch.ElapsedMilliseconds);

                    /*result = await raftManager.ReplicateLogs(i, logType, data).ConfigureAwait(false);
                    if (result.Success)
                        Console.WriteLine("{0} #3 Replicated log with id: {1}", i, result.LogIndex);
                    else
                        Console.WriteLine("{0} #3 Replication failed {1}", i, result.Status);*/
                }

                result = await raftManager.ReplicateCheckpoint(i).ConfigureAwait(false);
                if (result.Success)
                    Console.WriteLine("#C Replicated checkpoint log with id: {0}", result.LogIndex);
                else
                    Console.WriteLine("#C Replication checkpoint failed {0}", result.Status);
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine("ReplicationServiceException: {0}# {1}", i, ex.Message);
        }
    }
}