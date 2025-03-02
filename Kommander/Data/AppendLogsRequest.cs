
using Kommander.Time;

namespace Kommander.Data;

public sealed class AppendLogsRequest
{
    public int Partition { get; set; }

    public long Term { get; set; }
    
    public HLCTimestamp Time { get; set; }

    public string Endpoint { get; set; }

    public List<RaftLog>? Logs { get; set; }

    public AppendLogsRequest(int partition, long term, HLCTimestamp time, string endpoint, List<RaftLog>? logs = null)
    {
        Partition = partition;
        Term = term;
        Time = time;
        Endpoint = endpoint;
        Logs = logs;
    }
}
