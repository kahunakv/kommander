
namespace Kommander.Data;

/// <summary>
/// Represents a request used during the handshake process in a distributed system.
/// </summary>
/// <remarks>
/// This class encapsulates information required to initialize communication
/// between nodes in a cluster. It is typically sent when establishing a connection between nodes.
/// </remarks>
public sealed class HandshakeRequest
{
    public int NodeId { get; set; }
    
    public int Partition { get; set; }
    
    public long MaxLogId { get; set; } 

    public string Endpoint { get; set; }

    public HandshakeRequest(int nodeId, int partition, long maxLogId, string endpoint)
    {
        NodeId = nodeId;
        Partition = partition;
        MaxLogId = maxLogId;
        Endpoint = endpoint;
    }
}