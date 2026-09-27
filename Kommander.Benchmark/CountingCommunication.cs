using System.Collections.Concurrent;
using Kommander.Communication;
using Kommander.Data;
using Kommander.Gossip;

namespace Kommander.Benchmark;

/// <summary>
/// An <see cref="ICommunication"/> decorator that counts the replication traffic each node sends:
/// <c>AppendLogs</c> frames and the entries and log bytes they carry, and <c>CompleteAppendLogs</c>
/// acks. The benchmark divides the counts by the proposals in the window to report frames and
/// bytes per proposal per follower.
///
/// <para><b>Every member is forwarded, including the ones with a default body on the
/// interface.</b> A member left out would silently run the interface default (for example a
/// gossip or read-index call that answers "false") instead of the real transport, and the
/// cluster would behave differently from production.</para>
///
/// <para><b>Log bytes, not wire bytes.</b> The byte count is the sum of each entry's payload and
/// log-type length. It leaves out protobuf framing, HTTP/2 and TLS, which the transport does not
/// expose. It is the right number to compare frames of different batch sizes; it is not the
/// socket byte count.</para>
/// </summary>
public sealed class CountingCommunication : ICommunication
{
    private readonly ICommunication inner;

    private readonly ConcurrentDictionary<string, TrafficCounter> appendByTarget = new();

    private long acks;

    public CountingCommunication(ICommunication inner) => this.inner = inner;

    /// <summary>Per-target counters. Updated with interlocked adds on the send path.</summary>
    public sealed class TrafficCounter
    {
        /// <summary>Entry-carrying frames. Heartbeats (no entries) are counted apart.</summary>
        public long Frames;
        public long Heartbeats;
        public long Entries;
        public long LogBytes;
    }

    /// <summary>Snapshot of the counters: per target endpoint, and the ack total.</summary>
    public (IReadOnlyDictionary<string, TrafficSnapshot> Appends, long Acks) Snapshot()
    {
        Dictionary<string, TrafficSnapshot> appends = new();

        foreach (KeyValuePair<string, TrafficCounter> kv in appendByTarget)
            appends[kv.Key] = new(
                Interlocked.Read(ref kv.Value.Frames),
                Interlocked.Read(ref kv.Value.Heartbeats),
                Interlocked.Read(ref kv.Value.Entries),
                Interlocked.Read(ref kv.Value.LogBytes));

        return (appends, Interlocked.Read(ref acks));
    }

    private void CountAppend(RaftNode node, AppendLogsRequest request)
    {
        TrafficCounter counter = appendByTarget.GetOrAdd(node.Endpoint, static _ => new());

        if (request.Logs is not { Count: > 0 } logs)
        {
            Interlocked.Increment(ref counter.Heartbeats);
            return;
        }

        Interlocked.Increment(ref counter.Frames);

        long bytes = 0;
        foreach (RaftLog log in logs)
            bytes += (log.LogData?.Length ?? 0) + (log.LogType?.Length ?? 0);

        Interlocked.Add(ref counter.Entries, logs.Count);
        Interlocked.Add(ref counter.LogBytes, bytes);
    }

    /// <summary>Counters of the <c>AppendLogs</c> traffic to one target, at one instant.</summary>
    public readonly record struct TrafficSnapshot(long Frames, long Heartbeats, long Entries, long LogBytes)
    {
        public static TrafficSnapshot operator -(TrafficSnapshot a, TrafficSnapshot b) =>
            new(a.Frames - b.Frames, a.Heartbeats - b.Heartbeats, a.Entries - b.Entries, a.LogBytes - b.LogBytes);
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
