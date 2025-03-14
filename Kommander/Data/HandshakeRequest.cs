namespace Kommander.Data;

public sealed class HandshakeRequest
{
    public int Partition { get; set; }
    
    public long MaxLogId { get; set; } 

    public string Endpoint { get; set; }

    public HandshakeRequest(int partition, long maxLogId, string endpoint)
    {
        Partition = partition;
        MaxLogId = maxLogId;
        Endpoint = endpoint;
    }
}