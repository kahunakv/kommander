
namespace Kommander.Data;

public readonly struct RaftWALResponse
{
    public long Index { get; }
    
    public List<RaftLog>? Logs { get; }
    
    public RaftWALResponse(long index)
    {
        Index = index;
    }
    
    public RaftWALResponse(List<RaftLog> logs)
    {
        Logs = logs;
    }
}