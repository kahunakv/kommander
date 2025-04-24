
using Kommander.Time;

namespace Kommander.Data;

/// <summary>
/// Represents a request used in a distributed consensus algorithm for requesting votes between nodes.
/// This request is typically sent during the election process in a cluster to gather support
/// from peer nodes for leadership candidacy.
/// </summary>
public sealed class RequestVotesRequest
{
    public int Partition { get; set; }

    public long Term { get; set; }
    
    public long MaxLogId { get; set; }
    
    public HLCTimestamp Time { get; set; }

    public string Endpoint { get; set; }

    public RequestVotesRequest(int partition, long term, long maxLogId, HLCTimestamp time, string endpoint)
    {
        Partition = partition;
        Term = term;
        Time = time;
        MaxLogId = maxLogId;
        Endpoint = endpoint;
    }
}
