
namespace Kommander.Data;

public sealed class AppendLogsResponse
{
    public RaftOperationStatus Status { get; set; }
    
    public long CommittedIndex { get; set; }
    
    public AppendLogsResponse(RaftOperationStatus status, long committedIndex)
    {
        Status = status;
        CommittedIndex = committedIndex;
    }
}
