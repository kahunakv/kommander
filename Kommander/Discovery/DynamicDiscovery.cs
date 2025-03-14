
namespace Kommander.Discovery;

/// <summary>
/// Allow discovery of other Raft nodes using a static list of nodes
/// Nodes can be added or removed from the internal list
/// </summary>
public sealed class DynamicDiscovery : IDiscovery
{
    private readonly Dictionary<string, RaftNode> nodes = new();
    
    public DynamicDiscovery(List<RaftNode> nodes)
    {
        foreach (RaftNode node in nodes)
            this.nodes.Add(node.Endpoint, node);
    }
    
    public Task Register(RaftConfiguration configuration)
    {
        return Task.CompletedTask;
    }

    public List<RaftNode> GetNodes()
    {
        return nodes.Values.ToList();
    }

    public void AddNode(RaftNode node)
    {
        Console.WriteLine("Added node to cluster {0}", node.Endpoint);
        
        nodes.Add(node.Endpoint, node);
    }
    
    public void RemoveNode(RaftNode node)
    {
        Console.WriteLine("Removed node from cluster {0}", node.Endpoint);
        
        nodes.Remove(node.Endpoint);
    }
}