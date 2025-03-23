namespace Kommander.Data;

public sealed class RaftLeaderSupervisorRequest
{
    public RaftLeaderSupervisorRequestType Type { get; }
    
    public RaftLeaderSupervisorRequest(RaftLeaderSupervisorRequestType type) => Type = type;
}