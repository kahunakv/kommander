
namespace Kommander.Data;

public readonly struct RaftWALRequest
{
    public RaftWALActionType Type { get; }

    public long Term { get; } = 0;
    
    public long CurrentIndex { get; } = 0;

    public List<RaftLog>? Logs { get; } = null;

    public RaftWALRequest(RaftWALActionType type)
    {
        Type = type;
    }

    public RaftWALRequest(RaftWALActionType type, long term)
    {
        Type = type;
        Term = term;
    }

    public RaftWALRequest(RaftWALActionType type, long term, List<RaftLog> log)
    {
        Type = type;
        Term = term;
        Logs = log;
    }
    
    public RaftWALRequest(RaftWALActionType type, long term, long currentIndex)
    {
        Type = type;
        Term = term;
        CurrentIndex = currentIndex;
    }
}
