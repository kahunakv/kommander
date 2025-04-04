
using Kommander.Discovery;

namespace Kommander;

/// <summary>
/// Manages the operations of joining and leaving a cluster.
/// Keeps state about the current cluster and node status.
/// </summary>
public sealed class ClusterHandler
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

    public Task UpdateNodes()
    {
        manager.Nodes = discovery.GetNodes();

        /*if (manager.Logger.IsEnabled(LogLevel.Debug))
        {
            StringBuilder builder = new();

            foreach (RaftNode node in manager.Nodes)
            {
                builder.Append(node.Endpoint);
                builder.Append(' ');
            }

            manager.Logger.LogDebug("[{Endpoint}] Nodes: {Nodes}", manager.LocalEndpoint, builder.ToString());
        }*/

        return Task.CompletedTask;
    }

    public bool IsNode(string endpoint)
    {
        foreach (RaftNode node in manager.Nodes)
        {
            if (node.Endpoint == endpoint)
                return true;
        }

        return false;
    }
}