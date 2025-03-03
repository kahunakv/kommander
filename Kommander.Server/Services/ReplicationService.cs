
using System.Text;

namespace Kommander.Services;

public class ReplicationService : BackgroundService //, IDisposable
{
    private readonly IRaft raftManager;

    public ReplicationService(IRaft raftManager)
    {
        //_logger = logger;
        this.raftManager = raftManager;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await raftManager.JoinCluster();
        
        while (true)
        {
            await raftManager.UpdateNodes();

            for (int i = 0; i < raftManager.Configuration.MaxPartitions ; i++)
            {
                try
                {
                    if (await raftManager.AmILeader(i, stoppingToken))
                    {
                        const string logType = "Greeting";
                        byte[] data = Encoding.UTF8.GetBytes("Hello, World! " + DateTime.UtcNow);

                        for (int j = 0; j < 20; j++)
                        {
                            (bool success, long commitLogId) = await raftManager.ReplicateLogs(i, logType, data);
                            if (success)
                                Console.WriteLine("#1 Replicated log with id: {0}", commitLogId);
                            else
                                Console.WriteLine("#1 Replicated failed");

                            (success, commitLogId) = await raftManager.ReplicateLogs(i, logType, data);
                            if (success)
                                Console.WriteLine("#2 Replicated log with id: {0}", commitLogId);
                            else
                                Console.WriteLine("#2 Replicated failed");

                            (success, commitLogId) = await raftManager.ReplicateLogs(i, logType, data);
                            if (success)
                                Console.WriteLine("#3 Replicated log with id: {0}", commitLogId);
                            else
                                Console.WriteLine("#3 Replicated failed");
                        }

                        await raftManager.ReplicateCheckpoint(i);
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine("{0}# {1}", i, ex.Message);
                }
            }

            await Task.Delay(20000, stoppingToken);
        }
    }
}