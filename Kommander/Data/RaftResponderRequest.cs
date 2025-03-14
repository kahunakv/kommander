
namespace Kommander.Data;

public sealed class RaftResponderRequest
{
    public RaftResponderRequestType Type { get; }
    
    public HandshakeRequest? HandshakeRequest { get; }
    
    public VoteRequest? VoteRequest { get; }
    
    public RequestVotesRequest? RequestVotesRequest { get; }
    
    public AppendLogsRequest? AppendLogsRequest { get; }
    
    public CompleteAppendLogsRequest? CompleteAppendLogsRequest { get; }
    
    public RaftNode? Node { get; }

    public RaftResponderRequest(RaftResponderRequestType type, RaftNode node, HandshakeRequest request)
    {
        Type = type;
        Node = node;
        HandshakeRequest = request;
    }
    
    public RaftResponderRequest(RaftResponderRequestType type, RaftNode node, VoteRequest request)
    {
        Type = type;
        Node = node;
        VoteRequest = request;
    }

    public RaftResponderRequest(RaftResponderRequestType type, RaftNode node, RequestVotesRequest request)
    {
        Type = type;
        Node = node;
        RequestVotesRequest = request;
    }
    
    public RaftResponderRequest(RaftResponderRequestType type, RaftNode node, AppendLogsRequest request)
    {
        Type = type;
        Node = node;
        AppendLogsRequest = request;
    }
    
    public RaftResponderRequest(RaftResponderRequestType type, RaftNode node, CompleteAppendLogsRequest request)
    {
        Type = type;
        Node = node;
        CompleteAppendLogsRequest = request;
    }
}