
using Kommander.Time;

namespace Kommander.Data;

public sealed class VoteRequest
{
    public int Partition { get; set; }

    public long Term { get; set; }
    
    public HLCTimestamp Time { get; set; }

    public string Endpoint { get; set; }

    public VoteRequest(int partition, long term, HLCTimestamp time, string endpoint)
    {
        Partition = partition;
        Term = term;
        Time = time;
        Endpoint = endpoint;
    }
}
