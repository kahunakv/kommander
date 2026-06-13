
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

    /// <summary>
    /// Sends a <see cref="LeaveRequest"/> to <paramref name="node"/> asking the P0 leader to
    /// remove the departing endpoint from the committed roster.  If the target is not the P0
    /// leader it returns <see cref="LeaveResponse.LeaderHint"/> so the caller can retry.
    /// <paramref name="cancellationToken"/> bounds the per-attempt wait so the caller's deadline
    /// is always respected even when the target node is stopped or unreachable.
    /// </summary>
    public Task<LeaveResponse> SendLeave(RaftManager manager, RaftNode node, LeaveRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Queries the remote node at <paramref name="node"/> for the last committed log index it has
    /// recorded for <paramref name="followerEndpoint"/> on <paramref name="partitionId"/>.
    /// <para>
    /// This is used by <c>CheckLearnerPromotionsAsync</c> when the local P0 leader is not the
    /// leader of a given partition — it asks the actual partition leader for the learner's lag so
    /// the promotion gate can cover all partitions, not just those led locally.
    /// </para>
    /// <para>
    /// Implementations that do not support this query (e.g. the gRPC transport before a dedicated
    /// RPC is added) return <see langword="null"/>, causing the caller to skip the partition's lag
    /// check — the same behaviour as before this method existed.
    /// </para>
    /// </summary>
    public Task<long?> GetRemoteFollowerLag(RaftManager manager, RaftNode node, int partitionId, string followerEndpoint)
        => Task.FromResult<long?>(null);
}