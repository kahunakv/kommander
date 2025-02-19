
namespace Kommander.Discovery;

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