
namespace Kommander.Data;

public sealed class AppendLogsResponse
{
    public long CommitedIndex { get; set; }
    
    public AppendLogsResponse(long commitedIndex)
    {
        CommitedIndex = commitedIndex;
    }
}
