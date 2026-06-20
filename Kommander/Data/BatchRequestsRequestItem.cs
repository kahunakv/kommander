
namespace Kommander.Data;

public sealed class BatchRequestsRequestItem
{
    public BatchRequestsRequestType Type { get; set; }
    
    public HandshakeRequest? Handshake { get; set; }
    
    public VoteRequest? Vote { get; set; }
    
    public RequestVotesRequest? RequestVotes { get; set; }

    public StepDownNoticeRequest? StepDownNotice { get; set; }

    public TransferLeadershipRequest? TransferLeadership { get; set; }
    
    public AppendLogsRequest? AppendLogs { get; set; }
    
    public CompleteAppendLogsRequest? CompleteAppendLogs { get; set; }

    public TransferLeadershipSuggestionRequest? TransferLeadershipSuggestion { get; set; }
}
