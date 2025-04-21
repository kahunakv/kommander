
using Kommander.Data;

namespace Kommander.Communication;

public interface ICommunication
{
    public Task<HandshakeResponse> Handshake(RaftManager manager, RaftNode node, HandshakeRequest request);
    
    public Task<RequestVotesResponse> RequestVotes(RaftManager manager, RaftNode node, RequestVotesRequest request);

    public Task<VoteResponse> Vote(RaftManager manager, RaftNode node, VoteRequest request);

    public Task<AppendLogsResponse> AppendLogs(RaftManager manager, RaftNode node, AppendLogsRequest request);

    public Task<CompleteAppendLogsResponse> CompleteAppendLogs(RaftManager manager, RaftNode node, CompleteAppendLogsRequest request);

    public Task<BatchRequestsResponse> BatchRequests(RaftManager manager, RaftNode node, BatchRequestsRequest request);
}