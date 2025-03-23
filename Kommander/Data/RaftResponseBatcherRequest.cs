namespace Kommander.Data;

public sealed class RaftResponseBatcherRequest
{
    public RaftResponderRequestType Type { get; }
    
    public AppendLogsRequest? AppendLogsRequest { get; }
    
    public RaftNode? Node { get; }
    
    public RaftResponseBatcherRequest(RaftResponderRequestType type, RaftNode node, AppendLogsRequest request)
    {
        Type = type;
        Node = node;
        AppendLogsRequest = request;
    }
    
    public RaftResponseBatcherRequest(RaftResponderRequestType type)
    {
        Type = type;
    }
}