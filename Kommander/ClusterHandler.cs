
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

    /// <summary>
    /// Joins a cluster by registering the current node with the discovery service
    /// and marking the node as part of a cluster.
    /// </summary>
    /// <param name="configuration">The configuration details required to join the cluster.</param>
    /// <returns>A task representing the asynchronous join operation.</returns>
    public async Task JoinCluster(RaftConfiguration configuration)
    {
        await discovery.Register(configuration).ConfigureAwait(false);
        Joined = true;
    }

    /// <summary>
    /// Leaves the cluster by unregistering the current node from the discovery service
    /// and marking the node as no longer part of the cluster.
    /// </summary>
    /// <param name="configuration">The configuration details required to leave the cluster.</param>
    /// <returns>A task representing the asynchronous leave operation.</returns>
    public async Task LeaveCluster(RaftConfiguration configuration)
    {
        await discovery.Register(configuration).ConfigureAwait(false);
        Joined = false;
    }

    /// <summary>
    /// Updates the list of nodes in the cluster by retrieving the latest node information
    /// from the discovery service and assigning it to the raft manager.
    /// </summary>
    /// <returns>A completed task representing the update operation.</returns>
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

    /// <summary>
    /// Determines if a specified endpoint matches any node in the cluster.
    /// </summary>
    /// <param name="endpoint">The endpoint to check for membership in the cluster.</param>
    /// <returns>True if the endpoint corresponds to a node in the cluster; otherwise, false.</returns>
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