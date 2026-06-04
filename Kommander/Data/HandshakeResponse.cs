
namespace Kommander.Data;

public class HandshakeResponse
{
    public int NodeId { get; set; }

    public long MaxLogId { get; set; }

    public string Endpoint { get; set; } = "";

    public HandshakeResponse()
    {
    }

    public HandshakeResponse(int nodeId, long maxLogId, string endpoint)
    {
        NodeId = nodeId;
        MaxLogId = maxLogId;
        Endpoint = endpoint;
    }
}
