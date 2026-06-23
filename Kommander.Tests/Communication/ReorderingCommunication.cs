
using Kommander.Communication;
using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Gossip;

namespace Kommander.Tests.Communication;

/// <summary>
/// Test-only <see cref="ICommunication"/> decorator that wraps <see cref="InMemoryCommunication"/>
/// and can intercept exactly one outbound <see cref="AppendLogsRequest"/> to a configured
/// <c>(partitionId, targetEndpoint)</c> pair.
///
/// <para>The shim is inert by default.  Call <see cref="ConfigureHold"/> before the cluster
/// begins sending to activate it.  Once the first matching message is captured, every
/// subsequent call passes through unchanged and the hold is never re-armed.</para>
///
/// <para>Pattern:
/// <list type="number">
///   <item>Wrap an <see cref="InMemoryCommunication"/> and pass this shim to all nodes.</item>
///   <item>Call <see cref="ConfigureHold"/> to configure which endpoint/partition/predicate to intercept.</item>
///   <item>Run the cluster until <see cref="WaitForHeldAsync"/> completes.</item>
///   <item>Inject a later batch directly to the follower (via <c>IRaft.AppendLogs</c>) to produce a hole.</item>
///   <item>Call <see cref="ReleaseHeld"/> to deliver the delayed message and allow the hole-repair path to run.</item>
/// </list>
/// </para>
/// </summary>
internal sealed class ReorderingCommunication : ICommunication
{
    private readonly InMemoryCommunication _inner;

    // ── Hold configuration ────────────────────────────────────────────────────

    private int _holdPartitionId;
    private string? _holdTargetEndpoint;
    private Func<AppendLogsRequest, bool>? _holdPredicate;

    // ── Hold state ────────────────────────────────────────────────────────────

    private volatile bool _holdFired;
    private (RaftManager Manager, RaftNode Node, AppendLogsRequest Request)? _held;
    private readonly object _holdLock = new();
    private readonly TaskCompletionSource _heldSignal =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    // ── Construction ──────────────────────────────────────────────────────────

    public ReorderingCommunication(InMemoryCommunication inner) => _inner = inner;

    /// <summary>Forwards node registrations to the wrapped transport.</summary>
    public void SetNodes(Dictionary<string, IRaft> nodes) => _inner.SetNodes(nodes);

    // ── Configuration ─────────────────────────────────────────────────────────

    /// <summary>
    /// Configures the shim to capture the first <see cref="AppendLogsRequest"/> sent to
    /// <paramref name="targetEndpoint"/> on <paramref name="partitionId"/> that satisfies
    /// <paramref name="predicate"/> (or any message if <paramref name="predicate"/> is null).
    /// Must be called before the cluster begins sending.
    /// </summary>
    public void ConfigureHold(
        int partitionId,
        string targetEndpoint,
        Func<AppendLogsRequest, bool>? predicate = null)
    {
        _holdPartitionId = partitionId;
        _holdTargetEndpoint = targetEndpoint;
        _holdPredicate = predicate;
    }

    // ── Observable state ──────────────────────────────────────────────────────

    /// <summary>
    /// Returns a task that completes once a message has been captured.
    /// </summary>
    public Task WaitForHeldAsync(CancellationToken ct = default) =>
        _heldSignal.Task.WaitAsync(ct);

    /// <summary>True while at least one message is currently held.</summary>
    public bool HasHeld => _held.HasValue;

    /// <summary>
    /// The currently-held <see cref="AppendLogsRequest"/>, or <see langword="null"/> if nothing is
    /// held. Tests use this to assert against the entries that were actually delayed, rather than
    /// assuming which log ids the leader chose to ship first (the leadership NoOp may already have
    /// replicated before the hold was armed).
    /// </summary>
    public AppendLogsRequest? HeldRequest
    {
        get { lock (_holdLock) return _held?.Request; }
    }

    // ── Release ───────────────────────────────────────────────────────────────

    /// <summary>
    /// Delivers the held message to the inner transport and clears the hold.
    /// No-op if nothing is held.
    /// </summary>
    public void ReleaseHeld()
    {
        (RaftManager Manager, RaftNode Node, AppendLogsRequest Request)? held;
        lock (_holdLock)
        {
            held = _held;
            _held = null;
        }

        if (held is { } h)
            _inner.AppendLogs(h.Manager, h.Node, h.Request);
    }

    // ── ICommunication ────────────────────────────────────────────────────────

    public Task<AppendLogsResponse> AppendLogs(RaftManager manager, RaftNode node, AppendLogsRequest request)
    {
        if (Classify(manager, node, request) != Disposition.PassThrough)
            return Task.FromResult(new AppendLogsResponse());

        return _inner.AppendLogs(manager, node, request);
    }

    public Task<BatchRequestsResponse> BatchRequests(RaftManager manager, RaftNode node, BatchRequestsRequest request)
    {
        if (request.Requests is null
            || _holdTargetEndpoint is null
            || node.Endpoint != _holdTargetEndpoint)
            return _inner.BatchRequests(manager, node, request);

        // Drop every matching AppendLogs item (captured or swallowed) from the forwarded batch,
        // keeping all other items.  remainder stays null until the first drop, so the common
        // no-match case forwards the original request untouched.
        List<BatchRequestsRequestItem>? remainder = null;
        for (int i = 0; i < request.Requests.Count; i++)
        {
            BatchRequestsRequestItem item = request.Requests[i];

            bool drop = item.Type == BatchRequestsRequestType.AppendLogs
                && item.AppendLogs is not null
                && Classify(manager, node, item.AppendLogs) != Disposition.PassThrough;

            if (drop)
                remainder ??= [.. request.Requests.Take(i)];
            else
                remainder?.Add(item);
        }

        if (remainder is null)
            return _inner.BatchRequests(manager, node, request);
        if (remainder.Count == 0)
            return Task.FromResult(new BatchRequestsResponse());

        return _inner.BatchRequests(manager, node, new BatchRequestsRequest { Requests = remainder });
    }

    // ── Hold classifier ─────────────────────────────────────────────────────────

    private enum Disposition
    {
        /// <summary>Forward to the inner transport unchanged.</summary>
        PassThrough,
        /// <summary>First match: captured into the hold and signalled to waiters.</summary>
        Capture,
        /// <summary>A message is already held: drop this one so retries cannot fill the hole.</summary>
        Swallow,
    }

    /// <summary>
    /// Decides the fate of one outbound <see cref="AppendLogsRequest"/> and, on the first match,
    /// atomically captures it.
    /// <para>
    /// While a message is held, every subsequent matching message is <see cref="Disposition.Swallow"/>ed
    /// rather than passed through. This is what makes the induced hole <b>stable</b>: a leader whose
    /// AppendLogs goes unacknowledged re-sends the same entries every heartbeat, and a one-shot
    /// "capture then passthrough" shim would let those retries refill the hole before a test can
    /// observe it. Passthrough resumes only after <see cref="ReleaseHeld"/> clears the hold.
    /// </para>
    /// </summary>
    private Disposition Classify(RaftManager manager, RaftNode node, AppendLogsRequest request)
    {
        if (_holdTargetEndpoint is null
            || node.Endpoint != _holdTargetEndpoint
            || request.Partition != _holdPartitionId)
            return Disposition.PassThrough;

        if (_holdPredicate is not null && !_holdPredicate(request))
            return Disposition.PassThrough;

        lock (_holdLock)
        {
            if (_held is not null)
                return Disposition.Swallow;     // hold active — block retries until released
            if (_holdFired)
                return Disposition.PassThrough; // already released — resume normal delivery

            _holdFired = true;
            _held = (manager, node, request);
            _heldSignal.TrySetResult();         // safe under lock: RunContinuationsAsynchronously
            return Disposition.Capture;
        }
    }

    // ── Passthrough delegates ─────────────────────────────────────────────────

    public Task<HandshakeResponse> Handshake(RaftManager manager, RaftNode node, HandshakeRequest request) =>
        _inner.Handshake(manager, node, request);

    public Task<RequestVotesResponse> RequestVotes(RaftManager manager, RaftNode node, RequestVotesRequest request) =>
        _inner.RequestVotes(manager, node, request);

    public Task<VoteResponse> Vote(RaftManager manager, RaftNode node, VoteRequest request) =>
        _inner.Vote(manager, node, request);

    public Task<CompleteAppendLogsResponse> CompleteAppendLogs(RaftManager manager, RaftNode node, CompleteAppendLogsRequest request) =>
        _inner.CompleteAppendLogs(manager, node, request);

    public Task<JoinResponse> SendJoin(RaftManager manager, RaftNode node, JoinRequest request) =>
        _inner.SendJoin(manager, node, request);

    public Task<LeaveResponse> SendLeave(RaftManager manager, RaftNode node, LeaveRequest request, CancellationToken cancellationToken = default) =>
        _inner.SendLeave(manager, node, request, cancellationToken);

    public Task<GossipAck> SendGossip(RaftManager manager, RaftNode node, GossipMessage digest, CancellationToken cancellationToken = default) =>
        _inner.SendGossip(manager, node, digest, cancellationToken);

    public Task<Gossip.PingResponse> SendPing(RaftManager manager, RaftNode node, Gossip.PingRequest request, CancellationToken cancellationToken = default) =>
        _inner.SendPing(manager, node, request, cancellationToken);

    public Task<Gossip.PingReqResponse> SendPingReq(RaftManager manager, RaftNode node, Gossip.PingReqRequest request, CancellationToken cancellationToken = default) =>
        _inner.SendPingReq(manager, node, request, cancellationToken);

    public Task<long?> GetRemoteFollowerLag(RaftManager manager, RaftNode node, int partitionId, string followerEndpoint) =>
        _inner.GetRemoteFollowerLag(manager, node, partitionId, followerEndpoint);

    public Task<SnapshotResponse> SendInstallSnapshot(RaftManager manager, RaftNode node, SnapshotRequest request, CancellationToken cancellationToken = default) =>
        _inner.SendInstallSnapshot(manager, node, request, cancellationToken);

    public Task NotifyJoinBlocked(RaftManager manager, string targetEndpoint, string reason, CancellationToken cancellationToken = default) =>
        _inner.NotifyJoinBlocked(manager, targetEndpoint, reason, cancellationToken);
}
