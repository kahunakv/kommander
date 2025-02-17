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

    public async Task JoinCluster()
    {
        await d.Register();
    }

    public async Task UpdateNodes()
    {
        manager.Nodes = d.GetNodes();

        await Task.CompletedTask;
    }
}