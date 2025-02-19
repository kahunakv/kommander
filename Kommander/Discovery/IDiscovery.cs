
namespace Kommander.Discovery;

public interface IDiscovery
{
    public Task Register(RaftConfiguration configuration);

    public List<RaftNode> GetNodes();
}