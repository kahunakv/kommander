
namespace Kommander.Data;

public sealed class RaftWALResponse
{
    public RaftOperationStatus Status { get; }
    
    public long Index { get; }
    
    public List<RaftLog>? Logs { get; }
    
    public RaftWALResponse(RaftOperationStatus status, long index)
    {
        Status = status;
        Index = index;
    }
    
    public RaftWALResponse(RaftOperationStatus status, List<RaftLog> logs)
    {
        Status = status;
        Logs = logs;
    }
}