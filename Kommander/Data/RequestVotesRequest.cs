
namespace Kommander.Data;

public sealed class RequestVotesRequest
{
    public int Partition { get; set; }

    public long Term { get; set; }

    public string Endpoint { get; set; }

    public RequestVotesRequest(int partition, long term, string endpoint)
    {
        Partition = partition;
        Term = term;
        Endpoint = endpoint;
    }
}
