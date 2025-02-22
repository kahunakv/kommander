namespace Kommander.Data;

public readonly struct RaftResponse
{
    public RaftResponseType Type { get; }

    public NodeState State { get; } = NodeState.Follower;
    
    public long CurrentIndex { get; } = 0;
    
    public RaftResponse(RaftResponseType type, NodeState state)
    {
        Type = type;
        State = state;
    }
    
    public RaftResponse(RaftResponseType type)
    {
        Type = type;
    }
    
    public RaftResponse(RaftResponseType type, long currentIndex)
    {
        Type = type;
        CurrentIndex = currentIndex;
    }
}