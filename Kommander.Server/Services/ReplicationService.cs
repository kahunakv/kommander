
using System.Text;
using Kommander.Data;

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
        await raftManager.JoinCluster().ConfigureAwait(false);
        
        while (true)
        {
            await raftManager.UpdateNodes().ConfigureAwait(false);

            for (int i = 0; i < raftManager.Configuration.MaxPartitions ; i++)
            {
                try
                {
                    if (await raftManager.AmILeaderQuick(i).ConfigureAwait(false))
                    {
                        const string logType = "Greeting";
                        byte[] data = Encoding.UTF8.GetBytes("Hello, World! " + DateTime.UtcNow);

                        //for (int j = 0; j < 20; j++)
                        //{
                            (bool success, RaftOperationStatus status, long commitLogId) = await raftManager.ReplicateLogs(i, logType, data).ConfigureAwait(false);
                            if (success)
                                Console.WriteLine("#1 Replicated log with id: {0}", commitLogId);
                            else
                                Console.WriteLine("#1 Replicated failed {0}", status);

                            /*(success, commitLogId) = await raftManager.ReplicateLogs(i, logType, data).ConfigureAwait(false);
                            if (success)
                                Console.WriteLine("#2 Replicated log with id: {0}", commitLogId);
                            else
                                Console.WriteLine("#2 Replicated failed");

                            (success, commitLogId) = await raftManager.ReplicateLogs(i, logType, data).ConfigureAwait(false);
                            if (success)
                                Console.WriteLine("#3 Replicated log with id: {0}", commitLogId);
                            else
                                Console.WriteLine("#3 Replicated failed");*/
                        //}

                        await raftManager.ReplicateCheckpoint(i).ConfigureAwait(false);
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine("{0}# {1}", i, ex.Message);
                }
            }

            await Task.Delay(10000, stoppingToken);
        }
    }
}