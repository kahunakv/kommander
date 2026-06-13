
using Kommander.Data;

namespace Kommander.Communication;

/// <summary>
/// Represents an abstraction for handling communication between Raft nodes.
/// </summary>
public interface ICommunication
{
    public Task<HandshakeResponse> Handshake(RaftManager manager, RaftNode node, HandshakeRequest request);
    
    public Task<RequestVotesResponse> RequestVotes(RaftManager manager, RaftNode node, RequestVotesRequest request);

    public Task<VoteResponse> Vote(RaftManager manager, RaftNode node, VoteRequest request);

    public Task<AppendLogsResponse> AppendLogs(RaftManager manager, RaftNode node, AppendLogsRequest request);

    public Task<CompleteAppendLogsResponse> CompleteAppendLogs(RaftManager manager, RaftNode node, CompleteAppendLogsRequest request);

    public Task<BatchRequestsResponse> BatchRequests(RaftManager manager, RaftNode node, BatchRequestsRequest request);

    /// <summary>
    /// Sends a <see cref="JoinRequest"/> to <paramref name="node"/> asking to be admitted into
    /// the cluster roster as a Learner.  If the target is not the P0 leader it returns
    /// <see cref="JoinResponse.LeaderHint"/> pointing to the current leader.
    /// </summary>
    public Task<JoinResponse> SendJoin(RaftManager manager, RaftNode node, JoinRequest request);
}