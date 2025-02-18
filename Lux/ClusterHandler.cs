using Lux.Discovery;

namespace Lux;

internal sealed class ClusterHandler
{
    private readonly RaftManager manager;
    
    private readonly Multicast d = new();

    public ClusterHandler(RaftManager manager)
    {
        this.manager = manager;
    }

    public async Task JoinCluster(RaftConfiguration configuration)
    {
        await d.Register(configuration);
    }

    public async Task UpdateNodes()
    {
        manager.Nodes = d.GetNodes();

        await Task.CompletedTask;
    }
}