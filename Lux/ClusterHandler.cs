using Lux.Discovery;

namespace Lux;

internal sealed class ClusterHandler
{
    private readonly RaftManager manager;
    
    private readonly IDiscovery discovery;

    public ClusterHandler(RaftManager manager, IDiscovery discovery)
    {
        this.manager = manager;
        this.discovery = discovery;
    }

    public async Task JoinCluster(RaftConfiguration configuration)
    {
        await discovery.Register(configuration);
    }

    public async Task UpdateNodes()
    {
        manager.Nodes = discovery.GetNodes();

        await Task.CompletedTask;
    }
}