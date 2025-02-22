
namespace Kommander;

/// <summary>
/// Represents a node in a Raft cluster.
/// </summary>
public sealed class RaftNode
{
    public string Endpoint { get; }

    public RaftNode(string endpoint)
    {
        Endpoint = endpoint;
    }
}
