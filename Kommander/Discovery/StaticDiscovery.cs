
namespace Kommander.Discovery;

/// <summary>
/// StaticDiscovery is an implementation of the IDiscovery interface that provides a static list
/// of Raft nodes to be used in a Raft distributed cluster. This class is used in scenarios where the nodes
/// in the cluster are predefined and do not change dynamically at runtime.
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