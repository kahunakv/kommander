
using Kommander.Time;

namespace Kommander.Data;

public readonly struct RaftWALRequest
{
    public RaftWALActionType Type { get; }

    public long Term { get; } = 0;
    
    public long CurrentIndex { get; } = 0;
    
    public HLCTimestamp Timestamp { get; } = HLCTimestamp.Zero;

    public List<RaftLog>? Logs { get; } = null;

    public RaftWALRequest(RaftWALActionType type)
    {
        Type = type;
    }

    public RaftWALRequest(RaftWALActionType type, long term, HLCTimestamp timestamp)
    {
        Type = type;
        Term = term;
        Timestamp = timestamp;
    }

    public RaftWALRequest(RaftWALActionType type, long term, HLCTimestamp timestamp, List<RaftLog> log)
    {
        Type = type;
        Term = term;
        Timestamp = timestamp;
        Logs = log;
    }
    
    public RaftWALRequest(RaftWALActionType type, long term, long currentIndex)
    {
        Type = type;
        Term = term;
        CurrentIndex = currentIndex;
    }
}
