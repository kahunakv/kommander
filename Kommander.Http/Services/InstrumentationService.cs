
namespace Kommander.Services;

public class InstrumentationService : BackgroundService //, IDisposable
{
    private readonly RaftManager raftManager;

    public InstrumentationService(RaftManager raftManager)
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
                if (await raftManager.AmILeader(i))
                    raftManager.ReplicateLogs(i, "Hello, World! " + DateTime.UtcNow);
            }

            await Task.Delay(3000, stoppingToken);
        }
    }
}