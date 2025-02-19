
namespace Kommander.Data;

public sealed class AppendLogsRequest
{
    public int Partition { get; set; }

    public long Term { get; set; }

    public string Endpoint { get; set; }

    public List<RaftLog>? Logs { get; set; }

    public AppendLogsRequest(int partition, long term, string endpoint, List<RaftLog>? logs = null)
    {
        Partition = partition;
        Term = term;
        Endpoint = endpoint;
        Logs = logs;
    }
}
