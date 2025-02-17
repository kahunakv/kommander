namespace Lux.Services;

public class InstrumentationService : BackgroundService //, IDisposable
{
    //private readonly ILogger<InstrumentationService> _logger;
    
    //private Timer _timer;

    private readonly RaftManager raftManager;

    public bool IsRunning { get; private set; }

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
    
            await Task.Delay(1000, stoppingToken);
        }
    }
}