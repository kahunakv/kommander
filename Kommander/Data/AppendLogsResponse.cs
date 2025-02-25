
namespace Kommander.Data;

public sealed class AppendLogsResponse
{
    public RaftOperationStatus Status { get; set; }
    
    public long CommitedIndex { get; set; }
    
    public AppendLogsResponse(RaftOperationStatus status, long commitedIndex)
    {
        Status = status;
        CommitedIndex = commitedIndex;
    }
}
