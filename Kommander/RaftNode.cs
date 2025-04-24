
namespace Kommander;

/// <summary>
/// Represents a node in a Raft cluster.
/// </summary>
public sealed class RaftNode
{
    /// <summary>
    /// Represents the endpoint address of the node.
    /// The Endpoint property uniquely identifies a node within a Raft cluster
    /// and is used for communication and discovery between nodes.
    /// </summary>
    public string Endpoint { get; }

    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="endpoint"></param>
    public RaftNode(string endpoint)
    {
        Endpoint = endpoint;
    }
}
