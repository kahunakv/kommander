
using Kommander.Data;

namespace Kommander.Communication;

public interface ICommunication
{
    public Task<RequestVotesResponse> RequestVotes(RaftManager manager, RaftPartition partition, RaftNode node, RequestVotesRequest request);

    public Task<VoteResponse> Vote(RaftManager manager, RaftPartition partition, RaftNode node, VoteRequest request);

    public Task<AppendLogsResponse> AppendLogToNode(RaftManager manager, RaftPartition partition, RaftNode node, AppendLogsRequest request);
}