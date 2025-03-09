
using Kommander.Time;

namespace Kommander.Data;

public readonly struct RaftRequest
{
    public RaftRequestType Type { get; }

    public long Term { get; } = -1;
    
    public long CommitIndex { get; } = 0;
    
    public HLCTimestamp Timestamp { get; }

    public string? Endpoint { get; } = null; 

    public List<RaftLog>? Logs { get; } = null;
    
    public RaftOperationStatus Status { get; }

    public RaftRequest(RaftRequestType type, long term = -1, long commitIndex = 0, HLCTimestamp timestamp = default, string? endpoint = null, List<RaftLog>? logs = null)
    {
        Type = type;
        Term = term;
        CommitIndex = commitIndex;
        Timestamp = timestamp;
        Endpoint = endpoint;
        Logs = logs;
    }

    public RaftRequest(RaftRequestType type, List<RaftLog> logs)
    {
        Type = type;
        Logs = logs;
    }
    
    public RaftRequest(RaftRequestType type, RaftOperationStatus status)
    {
        Type = type;
        Status = status;
    }
}

