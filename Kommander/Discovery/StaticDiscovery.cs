
namespace Kommander.Discovery;

/// <summary>
/// Allow discovery of other Raft nodes using a static list of nodes
/// </summary>
public class StaticDiscovery : IDiscovery
{
    private readonly List<RaftNode> nodes;
    
    public StaticDiscovery(List<RaftNode> nodes)
    {
        this.nodes = nodes;
    }
    
    public Task Register(RaftConfiguration configuration)
    {
        return Task.CompletedTask;
    }

    public List<RaftNode> GetNodes()
    {
        return nodes;
    }
}