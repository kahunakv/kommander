
namespace Kommander.Data;

public sealed class VoteRequest
{
    public int Partition { get; set; }

    public long Term { get; set; }

    public string Endpoint { get; set; }

    public VoteRequest(int partition, long term, string endpoint)
    {
        Partition = partition;
        Term = term;
        Endpoint = endpoint;
    }
}
