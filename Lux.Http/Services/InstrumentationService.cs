namespace Lux.Services;

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

            if (await raftManager.AmILeader(0))
            {
                raftManager.ReplicateLogs(0, "Hello, World! " + DateTime.UtcNow);
            }
    
            await Task.Delay(1000, stoppingToken);
        }
    }
}