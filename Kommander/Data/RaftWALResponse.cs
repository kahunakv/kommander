
namespace Kommander.Data;

public readonly struct RaftWALResponse
{
    public long NextId { get; }
    
    public List<RaftLog>? Logs { get; }
    
    public RaftWALResponse(long nextId)
    {
        NextId = nextId;
    }
    
    public RaftWALResponse(List<RaftLog> logs)
    {
        Logs = logs;
    }
}