
namespace Kommander.Data;

public readonly struct RaftWALResponse
{
    public ulong NextId { get; }
    
    public List<RaftLog>? Logs { get; }
    
    public RaftWALResponse(ulong nextId)
    {
        NextId = nextId;
    }
    
    public RaftWALResponse(List<RaftLog> logs)
    {
        Logs = logs;
    }
}