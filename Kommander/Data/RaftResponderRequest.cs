namespace Kommander.Data;

public class RaftResponderRequest
{
    public RaftResponderRequestType Type { get; }
    
    public RequestVotesRequest? RequestVotesRequest { get; }

    public RaftResponderRequest(RaftResponderRequestType type, RequestVotesRequest request)
    {
        Type = type;
        RequestVotesRequest = request;
    }
}