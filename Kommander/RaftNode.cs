
namespace Kommander;

public sealed class RaftNode
{
    public string Endpoint { get; }

    public RaftNode(string endpoint)
    {
        Endpoint = endpoint;
    }
}
