
namespace Lux.Discovery;

public interface IDiscovery
{
    public Task Register();

    public List<RaftNode> GetNodes();
}