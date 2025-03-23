namespace Kommander.Data;

public sealed class AppendLogsBatchRequest
{
    public List<AppendLogsRequest>? AppendLogs { get; set; }
}