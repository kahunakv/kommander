
namespace Lux;

public sealed class RaftNode
{
    public string Ip { get; }

    public RaftNode(string ip)
    {
        Ip = ip;
    }
}
