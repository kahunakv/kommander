
using Kommander.Discovery;

namespace Kommander;

/// <summary>
/// Manages the operations of joining and leaving a cluster.
/// Keeps state about the current cluster and node status.
/// </summary>
internal sealed class ClusterHandler
{
    public bool Joined { get; private set; }
    
    private readonly RaftManager manager;
    
    private readonly IDiscovery discovery;

    public ClusterHandler(RaftManager manager, IDiscovery discovery)
    {
        this.manager = manager;
        this.discovery = discovery;
    }

    public async Task JoinCluster(RaftConfiguration configuration)
    {
        await discovery.Register(configuration).ConfigureAwait(false);
        Joined = true;
    }
    
    public async Task LeaveCluster(RaftConfiguration configuration)
    {
        await discovery.Register(configuration).ConfigureAwait(false);
        Joined = false;
    }

    public async Task UpdateNodes()
    {
        manager.Nodes = discovery.GetNodes();

        await Task.CompletedTask;
    }
}