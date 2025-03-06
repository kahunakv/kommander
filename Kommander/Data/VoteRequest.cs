
using Kommander.Time;

namespace Kommander.Data;

public sealed class VoteRequest
{
    public int Partition { get; set; }

    public long Term { get; set; }
    
    public long MaxLogId { get; set; }
    
    public HLCTimestamp Time { get; set; }

    public string Endpoint { get; set; }

    public VoteRequest(int partition, long term, long maxLogId, HLCTimestamp time, string endpoint)
    {
        Partition = partition;
        Term = term;
        MaxLogId = maxLogId;
        Time = time;
        Endpoint = endpoint;
    }
}
