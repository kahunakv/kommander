using Kommander.Time;

namespace Kommander.Data;

public sealed class TransferLeadershipRequest
{
    public int Partition { get; set; }

    public long Term { get; set; }

    public HLCTimestamp Time { get; set; }

    public string Endpoint { get; set; }

    public string TargetEndpoint { get; set; }

    public TransferLeadershipRequest(int partition, long term, HLCTimestamp time, string endpoint, string targetEndpoint)
    {
        Partition = partition;
        Term = term;
        Time = time;
        Endpoint = endpoint;
        TargetEndpoint = targetEndpoint;
    }
}
