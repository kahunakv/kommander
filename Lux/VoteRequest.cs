
namespace Lux;

public sealed class VoteRequest
{
    public int Partition { get; set; }

    public int Term { get; set; }

    public string Endpoint { get; set; }

    public VoteRequest(int partition, int term, string endpoint)
    {
        Partition = partition;
        Term = term;
        Endpoint = endpoint;
    }
}
