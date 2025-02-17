namespace Lux;

public readonly struct RaftResponse
{
    public RaftResponseType Type { get; }

    public NodeState State { get; } = NodeState.Follower;
    
    public RaftResponse(RaftResponseType type, NodeState state)
    {
        Type = type;
        State = state;
    }
    
    public RaftResponse(RaftResponseType type)
    {
        Type = type;
    }
}