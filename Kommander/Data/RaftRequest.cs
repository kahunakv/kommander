
using Kommander.Time;

namespace Kommander.Data;

public sealed class RaftRequest
{
    public RaftRequestType Type { get; }

    public long Term { get; } = -1;
    
    public long CommitIndex { get; }
    
    public HLCTimestamp Timestamp { get; }

    public string? Endpoint { get; } 

    public List<RaftLog>? Logs { get; }
    
    public RaftOperationStatus Status { get; }
    
    public bool AutoCommit { get; } 

    public RaftRequest(
        RaftRequestType type, 
        long term = -1, 
        long commitIndex = 0, 
        HLCTimestamp timestamp = default, 
        string? endpoint = null, 
        RaftOperationStatus status = RaftOperationStatus.Success, 
        List<RaftLog>? logs = null
    )
    {
        Type = type;
        Term = term;
        CommitIndex = commitIndex;
        Timestamp = timestamp;
        Endpoint = endpoint;
        Status = status;
        Logs = logs;
    }

    public RaftRequest(RaftRequestType type, List<RaftLog> logs, bool autoCommit)
    {
        Type = type;
        Logs = logs;
        AutoCommit = autoCommit;
    }
    
    public RaftRequest(RaftRequestType type, HLCTimestamp timestamp, bool autoCommit)
    {
        Type = type;
        Timestamp = timestamp;
        AutoCommit = autoCommit;
    }
}

