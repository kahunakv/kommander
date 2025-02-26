
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
                    await raftManager.ReplicateLogs(i, Encoding.UTF8.GetBytes("Hello, World! " + DateTime.UtcNow));
            }

            await Task.Delay(3000, stoppingToken);
        }
    }
}