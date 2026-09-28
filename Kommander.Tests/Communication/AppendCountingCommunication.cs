using System.Collections.Concurrent;
using Kommander.Communication;
using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Gossip;

namespace Kommander.Tests.Communication;

/// <summary>
/// Test-only <see cref="ICommunication"/> decorator that counts entry-carrying <c>AppendLogs</c>
/// frames per (partition, target) and forwards every call unchanged to the wrapped transport. The
/// same shape as <c>Kommander.Benchmark.CountingCommunication</c>, scoped by partition so a test
/// can read one partition's replication traffic.
///
/// <para>Every member is forwarded, including those with a default body on the interface: a
/// member left out would run the interface default instead of the real transport.</para>
/// </summary>
internal sealed class AppendCountingCommunication : ICommunication
{
    private readonly InMemoryCommunication inner;

    private readonly ConcurrentDictionary<(int PartitionId, string Target), long[]> frames = new();

    private long acks;

    public AppendCountingCommunication(InMemoryCommunication inner) => this.inner = inner;

    /// <summary>Forwards node registrations to the wrapped transport.</summary>
    public void SetNodes(Dictionary<string, IRaft> nodes) => inner.SetNodes(nodes);

    /// <summary>Entry-carrying <c>AppendLogs</c> frames sent to <paramref name="target"/> on <paramref name="partitionId"/> so far.</summary>
    public long FramesTo(int partitionId, string target) =>
        frames.TryGetValue((partitionId, target), out long[]? counter) ? Interlocked.Read(ref counter[0]) : 0;

    /// <summary><c>CompleteAppendLogs</c> acks sent by any node, all partitions.</summary>
    public long Acks => Interlocked.Read(ref acks);

    private void CountAppend(RaftNode node, AppendLogsRequest request)
    {
        if (request.Logs is not { Count: > 0 })
            return;

        long[] counter = frames.GetOrAdd((request.Partition, node.Endpoint), static _ => new long[1]);
        Interlocked.Increment(ref counter[0]);
    }

    public Task<HandshakeResponse> Handshake(RaftManager manager, RaftNode node, HandshakeRequest request) =>
        inner.Handshake(manager, node, request);

    public Task<RequestVotesResponse> RequestVotes(RaftManager manager, RaftNode node, RequestVotesRequest request) =>
        inner.RequestVotes(manager, node, request);

    public Task<VoteResponse> Vote(RaftManager manager, RaftNode node, VoteRequest request) =>
        inner.Vote(manager, node, request);

    public Task<AppendLogsResponse> AppendLogs(RaftManager manager, RaftNode node, AppendLogsRequest request)
    {
        CountAppend(node, request);
        return inner.AppendLogs(manager, node, request);
    }

    public Task<CompleteAppendLogsResponse> CompleteAppendLogs(RaftManager manager, RaftNode node, CompleteAppendLogsRequest request)
    {
        Interlocked.Increment(ref acks);
        return inner.CompleteAppendLogs(manager, node, request);
    }

    public Task<BatchRequestsResponse> BatchRequests(RaftManager manager, RaftNode node, BatchRequestsRequest request)
    {
        if (request.Requests is { } items)
        {
            foreach (BatchRequestsRequestItem item in items)
            {
                if (item.AppendLogs is { } append)
                    CountAppend(node, append);

                if (item.CompleteAppendLogs is not null)
                    Interlocked.Increment(ref acks);
            }
        }

        return inner.BatchRequests(manager, node, request);
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
