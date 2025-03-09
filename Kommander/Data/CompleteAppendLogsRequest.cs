
using Kommander.Time;

namespace Kommander.Data;

public sealed class CompleteAppendLogsRequest
{
    public int Partition { get; set; }

    public long Term { get; set; }
    
    public HLCTimestamp Time { get; set; }

    public string Endpoint { get; set; }
    
    public RaftOperationStatus Status { get; set; }
    
    public long CommitIndex { get; set; }

    public CompleteAppendLogsRequest(int partition, long term, HLCTimestamp time, string endpoint, RaftOperationStatus status, long commitIndex)
    {
        Partition = partition;
        Term = term;
        Time = time;
        Endpoint = endpoint;
        Status = status;
        CommitIndex = commitIndex;
    }
}