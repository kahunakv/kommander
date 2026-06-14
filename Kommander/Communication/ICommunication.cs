
using Kommander.Data;
using Kommander.Gossip;

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
    /// Sends a <see cref="GossipMessage"/> to <paramref name="node"/> for membership anti-entropy.
    /// The receiver applies the roster to its local cache when the sender's version is strictly
    /// higher, and responds with its own roster when it holds newer data, enabling push-pull
    /// convergence in one round trip.  Gossip never writes to the Raft log.
    /// <para>
    /// Implementations that do not yet support gossip (e.g. the gRPC transport before a
    /// dedicated RPC is added) return a <see cref="GossipAck"/> with version 0 and a
    /// null roster, which causes the caller to skip the update silently.
    /// </para>
    /// </summary>
    public Task<GossipAck> SendGossip(
        RaftManager manager, RaftNode node, GossipMessage digest,
        CancellationToken cancellationToken = default)
        => Task.FromResult(new GossipAck(0, null));

    /// <summary>
    /// Sends a SWIM direct probe to <paramref name="node"/>.  The receiver replies immediately
    /// (while alive) with its current incarnation so the sender can record an up-to-date
    /// <see cref="Gossip.MemberLivenessState.Alive"/> entry.
    /// <para>
    /// Implementations that do not yet support this RPC return
    /// <c>PingResponse(false, 0)</c>, causing the sender to treat the probe as timed out.
    /// </para>
    /// </summary>
    public Task<Gossip.PingResponse> SendPing(
        RaftManager manager, RaftNode node, Gossip.PingRequest request,
        CancellationToken cancellationToken = default)
        => Task.FromResult(new Gossip.PingResponse(false, 0));

    /// <summary>
    /// Asks <paramref name="node"/> to relay a direct ping to
    /// <see cref="Gossip.PingReqRequest.TargetEndpoint"/> on the caller's behalf.
    /// Used as an indirect probe step when a direct ping times out.
    /// <para>
    /// Implementations that do not yet support this RPC return
    /// <c>PingReqResponse(false)</c>, causing the indirect probe to be counted as a failure.
    /// </para>
    /// </summary>
    public Task<Gossip.PingReqResponse> SendPingReq(
        RaftManager manager, RaftNode node, Gossip.PingReqRequest request,
        CancellationToken cancellationToken = default)
        => Task.FromResult(new Gossip.PingReqResponse(false));

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

    /// <summary>
    /// Ships a full-partition snapshot to a follower that is below the leader's compaction floor.
    /// The follower installs the snapshot and seeds its WAL with a <c>CommittedCheckpoint</c>
    /// at <see cref="SnapshotRequest.SnapshotIndex"/> so backfill can resume normally.
    /// <para>
    /// Implementations that do not yet support this RPC return
    /// <c>SnapshotResponse(false)</c>, leaving the follower stuck until a future transfer is
    /// attempted.
    /// </para>
    /// </summary>
    public Task<SnapshotResponse> SendInstallSnapshot(
        RaftManager manager, RaftNode node, SnapshotRequest request,
        CancellationToken cancellationToken = default)
        => Task.FromResult(new SnapshotResponse(false));

    /// <summary>
    /// Notifies a joining node that its promotion has been permanently blocked (e.g., the
    /// learner is below the WAL compaction floor and no <see cref="IRaftStateMachineTransfer"/>
    /// is registered).  The target node's <c>JoinCluster(seeds)</c> loop polls for this and
    /// throws <see cref="System.InvalidOperationException"/> immediately rather than spinning
    /// to the 60-second deadline.
    ///
    /// <para>
    /// The default implementation is a no-op: transports that do not yet wire this notification
    /// leave the joiner to time out, which is the pre-existing behaviour.
    /// </para>
    /// </summary>
    public Task NotifyJoinBlocked(
        RaftManager manager, string targetEndpoint, string reason,
        CancellationToken cancellationToken = default)
        => Task.CompletedTask;
}