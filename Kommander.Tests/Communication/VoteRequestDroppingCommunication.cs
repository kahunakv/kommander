using Kommander.Communication;
using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Gossip;

namespace Kommander.Tests.Communication;

/// <summary>
/// Test-only <see cref="ICommunication"/> decorator that drops the vote requests a predicate
/// selects and forwards every other call unchanged to the wrapped transport. A candidate whose
/// vote requests are dropped stays a candidate until its voting round times out, which is what a
/// split vote looks like from that candidate.
///
/// <para>Vote requests travel both as single calls and as items of a batch, so both paths apply
/// the predicate. A filtered batch is a new request: the list of the caller is never changed.</para>
///
/// <para>Every member is forwarded, including those with a default body on the interface: a
/// member left out would run the interface default instead of the real transport.</para>
/// </summary>
internal sealed class VoteRequestDroppingCommunication : ICommunication
{
    private static readonly Task<RequestVotesResponse> droppedResponse = Task.FromResult(new RequestVotesResponse());

    private readonly InMemoryCommunication inner;

    private volatile Func<RequestVotesRequest, bool>? drop;

    private long dropped;

    public VoteRequestDroppingCommunication(InMemoryCommunication inner) => this.inner = inner;

    /// <summary>Forwards node registrations to the wrapped transport.</summary>
    public void SetNodes(Dictionary<string, IRaft> nodes) => inner.SetNodes(nodes);

    /// <summary>Drops every vote request for which <paramref name="predicate"/> returns true.</summary>
    public void DropVoteRequests(Func<RequestVotesRequest, bool> predicate) => drop = predicate;

    /// <summary>Delivers every vote request again.</summary>
    public void DeliverAllVoteRequests() => drop = null;

    /// <summary>The number of vote requests dropped so far.</summary>
    public long Dropped => Interlocked.Read(ref dropped);

    private bool ShouldDrop(RequestVotesRequest request)
    {
        Func<RequestVotesRequest, bool>? predicate = drop;

        if (predicate is null || !predicate(request))
            return false;

        Interlocked.Increment(ref dropped);
        return true;
    }

    public Task<HandshakeResponse> Handshake(RaftManager manager, RaftNode node, HandshakeRequest request) =>
        inner.Handshake(manager, node, request);

    public Task<RequestVotesResponse> RequestVotes(RaftManager manager, RaftNode node, RequestVotesRequest request) =>
        ShouldDrop(request) ? droppedResponse : inner.RequestVotes(manager, node, request);

    public Task<VoteResponse> Vote(RaftManager manager, RaftNode node, VoteRequest request) =>
        inner.Vote(manager, node, request);

    public Task<AppendLogsResponse> AppendLogs(RaftManager manager, RaftNode node, AppendLogsRequest request) =>
        inner.AppendLogs(manager, node, request);

    public Task<CompleteAppendLogsResponse> CompleteAppendLogs(RaftManager manager, RaftNode node, CompleteAppendLogsRequest request) =>
        inner.CompleteAppendLogs(manager, node, request);

    public Task<BatchRequestsResponse> BatchRequests(RaftManager manager, RaftNode node, BatchRequestsRequest request)
    {
        if (drop is null || request.Requests is not { Count: > 0 } items)
            return inner.BatchRequests(manager, node, request);

        List<BatchRequestsRequestItem>? kept = null;

        for (int i = 0; i < items.Count; i++)
        {
            BatchRequestsRequestItem item = items[i];

            if (item.RequestVotes is { } voteRequest && ShouldDrop(voteRequest))
            {
                // First dropped item: copy the items before it, then keep the rest one by one.
                kept ??= items.GetRange(0, i);
                continue;
            }

            kept?.Add(item);
        }

        return kept is null
            ? inner.BatchRequests(manager, node, request)
            : inner.BatchRequests(manager, node, new BatchRequestsRequest { Requests = kept });
    }

    public Task<JoinResponse> SendJoin(RaftManager manager, RaftNode node, JoinRequest request) =>
        inner.SendJoin(manager, node, request);

    public Task<LeaveResponse> SendLeave(RaftManager manager, RaftNode node, LeaveRequest request, CancellationToken cancellationToken = default) =>
        inner.SendLeave(manager, node, request, cancellationToken);

    public Task<SetMemberRoleResponse> SendSetMemberRole(RaftManager manager, RaftNode node, SetMemberRoleRequest request, CancellationToken cancellationToken = default) =>
        inner.SendSetMemberRole(manager, node, request, cancellationToken);

    public Task<GossipAck> SendGossip(RaftManager manager, RaftNode node, GossipMessage digest, CancellationToken cancellationToken = default) =>
        inner.SendGossip(manager, node, digest, cancellationToken);

    public Task<Gossip.PingResponse> SendPing(RaftManager manager, RaftNode node, Gossip.PingRequest request, CancellationToken cancellationToken = default) =>
        inner.SendPing(manager, node, request, cancellationToken);

    public Task<Gossip.PingReqResponse> SendPingReq(RaftManager manager, RaftNode node, Gossip.PingReqRequest request, CancellationToken cancellationToken = default) =>
        inner.SendPingReq(manager, node, request, cancellationToken);

    public Task<long?> GetRemoteFollowerLag(RaftManager manager, RaftNode node, int partitionId, string followerEndpoint) =>
        inner.GetRemoteFollowerLag(manager, node, partitionId, followerEndpoint);

    public Task<SnapshotResponse> SendInstallSnapshot(RaftManager manager, RaftNode node, SnapshotRequest request, CancellationToken cancellationToken = default) =>
        inner.SendInstallSnapshot(manager, node, request, cancellationToken);

    public Task NotifyJoinBlocked(RaftManager manager, string targetEndpoint, string reason, CancellationToken cancellationToken = default) =>
        inner.NotifyJoinBlocked(manager, targetEndpoint, reason, cancellationToken);

    public Task<GetReadIndexResponse> GetReadIndex(RaftManager manager, RaftNode node, GetReadIndexRequest request, CancellationToken cancellationToken = default) =>
        inner.GetReadIndex(manager, node, request, cancellationToken);

    public Task<RaftReplicationResult?> ForwardReplicateLogs(
        RaftManager manager, RaftNode node, int partitionId, string type,
        IReadOnlyList<byte[]> logs, bool autoCommit, long expectedGeneration, long expectedTerm,
        CancellationToken cancellationToken = default) =>
        inner.ForwardReplicateLogs(manager, node, partitionId, type, logs, autoCommit, expectedGeneration, expectedTerm, cancellationToken);
}
