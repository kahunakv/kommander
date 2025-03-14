
using Kommander.Data;

namespace Kommander.Communication;

public interface ICommunication
{
    public Task<HandshakeResponse> Handshake(RaftManager manager, RaftPartition partition, RaftNode node, HandshakeRequest request);
    
    public Task<RequestVotesResponse> RequestVotes(RaftManager manager, RaftPartition partition, RaftNode node, RequestVotesRequest request);

    public Task<VoteResponse> Vote(RaftManager manager, RaftPartition partition, RaftNode node, VoteRequest request);

    public Task<AppendLogsResponse> AppendLogs(RaftManager manager, RaftPartition partition, RaftNode node, AppendLogsRequest request);

    public Task<CompleteAppendLogsResponse> CompleteAppendLogs(RaftManager manager, RaftPartition partition, RaftNode node, CompleteAppendLogsRequest request);
}