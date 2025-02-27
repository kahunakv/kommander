
using System.Text;

namespace Kommander.Services;

public class InstrumentationService : BackgroundService //, IDisposable
{
    private readonly IRaft raftManager;

    public InstrumentationService(IRaft raftManager)
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
                if (await raftManager.AmILeader(i, stoppingToken))
                {
                    byte[] s = Encoding.UTF8.GetBytes("Hello, World! " + DateTime.UtcNow);
                    
                    for (int j = 0; j < 50; j++)
                    {
                        (bool success, long commitLogId) = await raftManager.ReplicateLogs(i, s);
                        if (success)
                            Console.WriteLine("#1 Replicated log with id: {0}", commitLogId);

                        (success, commitLogId) = await raftManager.ReplicateLogs(i, s);
                        if (success)
                            Console.WriteLine("#2 Replicated log with id: {0}", commitLogId);

                        (success, commitLogId) = await raftManager.ReplicateLogs(i, s);
                        if (success)
                            Console.WriteLine("#3 Replicated log with id: {0}", commitLogId);
                    }
                }
            }

            await Task.Delay(3000, stoppingToken);
        }
    }
}