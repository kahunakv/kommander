namespace Kommander.Data;

public sealed class CompleteAppendLogsBatchRequest
{
    public List<CompleteAppendLogsRequest>? CompleteLogs { get; set; }
}