
using Kommander.Communication;
using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Gossip;

namespace Kommander.Tests.Chaos;

/// <summary>
/// A test-only <see cref="ICommunication"/> decorator around <see cref="InMemoryCommunication"/> that
/// perturbs every logical transport message. It intercepts all 14 interface methods (and every
/// <c>BatchRequests</c> item independently), normalizes each into an immutable <see cref="NemesisEnvelope"/>,
/// and applies a per-directed-link fault policy — <see cref="FaultAction.Pass"/>, <see cref="FaultAction.Drop"/>,
/// <see cref="FaultAction.Fail"/>, <see cref="FaultAction.Delay"/>, <see cref="FaultAction.Hold"/>, or
/// <see cref="FaultAction.Duplicate"/> — chosen either by scripted rules (stable selectors + occurrence,
/// deterministic replay) or a seeded random profile (deterministic for a given observed envelope order).
///
/// <para><b>Concurrency.</b> A single lock owns policy state, sequence allocation, the PRNG, the held-message
/// queue, and the event log. The lock is <b>never</b> held while awaiting or invoking the inner transport,
/// so real node threads are not serialized by the nemesis.</para>
///
/// <para><b>Buffers.</b> Deferred actions (Hold/Delay, and the second copy of Duplicate) deep-copy any
/// request that owns mutable buffers (notably <see cref="SnapshotRequest.Data"/> and
/// <see cref="AppendLogsRequest.Logs"/>) at intercept time, before the sender can reuse its pooled buffer.</para>
/// </summary>
public sealed class NemesisCommunication : ICommunication
{
    private readonly ICommunication _inner;
    private readonly object _lock = new();
    private readonly List<NemesisRule> _rules = [];
    private readonly List<HeldItem> _held = [];
    private readonly List<NemesisEvent> _allEvents = [];
    private readonly List<(Func<NemesisEnvelope, bool> Predicate, TaskCompletionSource Tcs)> _eventWaiters = [];

    private readonly Random? _rng;
    private NemesisRandomProfile? _randomProfile;
    private long _seq;

    // The event log is bounded: a long soak emits millions of events, so retain only a recent window (enough
    // for RecentEvents + barrier scans) and track the true total separately. Trimmed in chunks so append stays
    // amortized O(1). Without this the list grows unbounded and a soak run OOM/GC-thrashes.
    private const int MaxRetainedEvents = 8192;
    private long _totalEvents;

    // Must be called under _lock.
    private void RecordEventLocked(NemesisEvent evt)
    {
        _allEvents.Add(evt);
        _totalEvents++;
        if (_allEvents.Count >= MaxRetainedEvents * 2)
            _allEvents.RemoveRange(0, _allEvents.Count - MaxRetainedEvents);
    }

    /// <summary>The total number of events ever recorded (the retained window may be smaller).</summary>
    public long TotalEventCount { get { lock (_lock) return _totalEvents; } }

    /// <summary>
    /// Non-blocking hook fired after every event is appended (a decision, delivery, release, …). The chaos
    /// harness wires this to the invariant checker's coalesced <c>Notify</c> so invariants re-evaluate after
    /// transport activity without ever backpressuring the transport.
    /// </summary>
    public Action? OnEvent { get; set; }

    /// <summary>
    /// Wraps an inner transport. The canonical inner is <see cref="InMemoryCommunication"/> (used by the
    /// chaos harness); tests may inject any <see cref="ICommunication"/> to observe delivery directly.
    /// </summary>
    public NemesisCommunication(ICommunication inner, int? seed = null)
    {
        _inner = inner;
        _rng = seed is int s ? new Random(s) : null;
    }

    public void SetNodes(Dictionary<string, IRaft> nodes)
    {
        if (_inner is InMemoryCommunication imc)
            imc.SetNodes(nodes);
    }

    // ── configuration ───────────────────────────────────────────────────────────

    /// <summary>Adds a scripted rule. Rules are evaluated in insertion order; first eligible match wins.</summary>
    public NemesisCommunication AddRule(NemesisRule rule)
    {
        lock (_lock) _rules.Add(rule);
        return this;
    }

    public NemesisCommunication Drop(string source, string destination, NemesisVerb? verb = null, int? partition = null, string? name = null) =>
        AddRule(new NemesisRule { Source = source, Destination = destination, Verb = verb, Partition = partition, Action = FaultAction.Drop, Name = name });

    public NemesisCommunication Fail(string source, string destination, Func<Exception>? exception = null, NemesisVerb? verb = null, int? partition = null, int? occurrence = null, string? name = null) =>
        AddRule(new NemesisRule { Source = source, Destination = destination, Verb = verb, Partition = partition, Occurrence = occurrence, Action = FaultAction.Fail, ExceptionFactory = exception, Name = name });

    public NemesisCommunication Delay(string source, string destination, TimeSpan delay, NemesisVerb? verb = null, int? partition = null, int? occurrence = null, string? name = null) =>
        AddRule(new NemesisRule { Source = source, Destination = destination, Verb = verb, Partition = partition, Occurrence = occurrence, Action = FaultAction.Delay, Delay = delay, Name = name });

    public NemesisCommunication Hold(string source, string destination, NemesisVerb? verb = null, int? partition = null, int? occurrence = null, string? name = null) =>
        AddRule(new NemesisRule { Source = source, Destination = destination, Verb = verb, Partition = partition, Occurrence = occurrence, Action = FaultAction.Hold, Name = name });

    public NemesisCommunication Duplicate(string source, string destination, NemesisVerb? verb = null, int? partition = null, int? occurrence = null, string? name = null) =>
        AddRule(new NemesisRule { Source = source, Destination = destination, Verb = verb, Partition = partition, Occurrence = occurrence, Action = FaultAction.Duplicate, Name = name });

    /// <summary>One-way partition: drop every message on the directed link (optionally scoped to a verb/partition).</summary>
    public NemesisCommunication Partition(string source, string destination, string name, NemesisVerb? verb = null, int? partition = null) =>
        Drop(source, destination, verb, partition, name);

    /// <summary>Symmetric partition: drop both directions between the two endpoints under one name.</summary>
    public NemesisCommunication PartitionSymmetric(string a, string b, string name, NemesisVerb? verb = null, int? partition = null)
    {
        Drop(a, b, verb, partition, name);
        Drop(b, a, verb, partition, name);
        return this;
    }

    /// <summary>Removes exactly the rules installed under <paramref name="name"/> (heals a partition/cut).</summary>
    public int Heal(string name)
    {
        lock (_lock) return _rules.RemoveAll(r => r.Name == name);
    }

    public void SetRandomProfile(NemesisRandomProfile profile)
    {
        lock (_lock) _randomProfile = profile;
    }

    /// <summary>Removes all scripted rules and clears the random profile (full heal).</summary>
    public void ClearRules()
    {
        lock (_lock) { _rules.Clear(); _randomProfile = null; }
    }

    /// <summary>Drops every held message without delivering, completing each waiter as canceled. Used on disposal.</summary>
    public void DropAllHeld()
    {
        List<HeldItem> held;
        lock (_lock) { held = [.. _held]; _held.Clear(); }
        foreach (HeldItem h in held)
        {
            h.Cancel();
            AppendEvent(new NemesisEvent(NemesisEventKind.Cancellation, h.Envelope, "dropped on dispose"));
        }
    }

    // ── release / observability ───────────────────────────────────────────────────

    /// <summary>
    /// Releases held messages matching the selector, in the order they were held (FIFO). Returns the count
    /// released. Each released item is delivered outside the lock.
    /// </summary>
    public async Task<int> ReleaseHeldAsync(string? source = null, string? destination = null, NemesisVerb? verb = null, int? partition = null)
    {
        List<HeldItem> toRelease;
        lock (_lock)
        {
            toRelease = _held
                .Where(h => (source is null || h.Envelope.Source == source)
                            && (destination is null || h.Envelope.Destination == destination)
                            && (verb is null || h.Envelope.Verb == verb)
                            && (partition is null || h.Envelope.Partition == partition))
                .OrderBy(h => h.Envelope.Sequence)
                .ToList();
            foreach (HeldItem h in toRelease)
                _held.Remove(h);
        }

        foreach (HeldItem h in toRelease)
        {
            AppendEvent(new NemesisEvent(NemesisEventKind.Release, h.Envelope));
            await h.Deliver().ConfigureAwait(false);
        }
        return toRelease.Count;
    }

    public int HeldCount { get { lock (_lock) return _held.Count; } }

    /// <summary>The last 20 events, oldest first, for failure output.</summary>
    public IReadOnlyList<NemesisEvent> RecentEvents()
    {
        lock (_lock)
        {
            int from = Math.Max(0, _allEvents.Count - 20);
            return _allEvents.GetRange(from, _allEvents.Count - from);
        }
    }

    /// <summary>The full event log, for a test artifact.</summary>
    public IReadOnlyList<NemesisEvent> AllEvents()
    {
        lock (_lock) return [.. _allEvents];
    }

    /// <summary>
    /// A named event barrier: completes when an envelope matching <paramref name="predicate"/> has been (or
    /// is) observed. Scripted tests coordinate on these instead of wall-clock guesses.
    /// </summary>
    public Task WaitForEventAsync(Func<NemesisEnvelope, bool> predicate, CancellationToken ct = default)
    {
        TaskCompletionSource tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
        lock (_lock)
        {
            if (_allEvents.Any(e => predicate(e.Envelope)))
                tcs.TrySetResult();
            else
                _eventWaiters.Add((predicate, tcs));
        }
        return tcs.Task.WaitAsync(ct);
    }

    /// <summary>Test hook: run the decision logic for an envelope without any I/O (advances sequence + PRNG).</summary>
    internal FaultAction ClassifyForTesting(string source, string destination, NemesisVerb verb, int? partition = null, string? correlation = null)
    {
        NemesisEnvelope env;
        NemesisDecision d;
        lock (_lock)
        {
            env = new NemesisEnvelope(++_seq, source, destination, verb, partition, correlation);
            d = Decide(env);
        }
        AppendEvent(new NemesisEvent(NemesisEventKind.Decision, env, d.Action.ToString()));
        return d.Action;
    }

    // ── decision core ─────────────────────────────────────────────────────────────

    private readonly struct NemesisDecision(FaultAction action, TimeSpan delay, Func<Exception> exception)
    {
        public FaultAction Action { get; } = action;
        public TimeSpan Delay { get; } = delay;
        public Func<Exception> Exception { get; } = exception;
    }

    // Must be called under _lock.
    private NemesisDecision Decide(NemesisEnvelope env)
    {
        foreach (NemesisRule rule in _rules)
        {
            if (!rule.SelectorsMatch(env))
                continue;
            rule.MatchCount++;
            if (rule.Occurrence is null || rule.MatchCount == rule.Occurrence)
                return new NemesisDecision(rule.Action, rule.Delay, rule.ExceptionFactory ?? DefaultException);
        }

        // The random profile targets USER-partition traffic only: faulting system-partition (0) consensus or
        // null-partition control/gossip (ping, join, leave, gossip) cluster-wide destabilizes membership and
        // leadership everywhere, which is neither the intent of a user-workload soak nor reproducible as a
        // scripted scenario. A scenario that wants to perturb the system partition installs an explicit scripted
        // rule (which is matched above, before this branch) instead.
        if (_rng is not null && _randomProfile is not null && env.Partition is int p && p != 0) // 0 = system partition
        {
            FaultAction a = _randomProfile.Roll(_rng);
            return new NemesisDecision(a, _randomProfile.DelayDuration, _randomProfile.ExceptionFactory);
        }

        return new NemesisDecision(FaultAction.Pass, default, DefaultException);
    }

    private static readonly Func<Exception> DefaultException = static () => new NemesisTransportException();

    private void AppendEvent(NemesisEvent evt)
    {
        List<TaskCompletionSource>? fire = null;
        lock (_lock)
        {
            RecordEventLocked(evt);
            for (int i = _eventWaiters.Count - 1; i >= 0; i--)
            {
                if (_eventWaiters[i].Predicate(evt.Envelope))
                {
                    (fire ??= []).Add(_eventWaiters[i].Tcs);
                    _eventWaiters.RemoveAt(i);
                }
            }
        }
        if (fire is not null)
            foreach (TaskCompletionSource tcs in fire)
                tcs.TrySetResult();

        OnEvent?.Invoke();
    }

    /// <summary>Held/delayed unit of work: an envelope plus how to deliver it and how to cancel it.</summary>
    private sealed record HeldItem(long Sequence, NemesisEnvelope Envelope, Func<Task> Deliver, Action Cancel);

    // ── generic intercept ──────────────────────────────────────────────────────────

    private async Task<TResp> InterceptAsync<TResp>(
        string source, string destination, NemesisVerb verb, int? partition, string? correlation,
        Func<Task<TResp>> deliverNow,
        Func<Func<Task<TResp>>> makeDeferredDeliver,
        TResp neutral,
        CancellationToken ct)
    {
        NemesisEnvelope env;
        NemesisDecision decision;
        lock (_lock)
        {
            env = new NemesisEnvelope(++_seq, source, destination, verb, partition, correlation);
            decision = Decide(env);
        }
        AppendEvent(new NemesisEvent(NemesisEventKind.Decision, env, decision.Action.ToString()));

        switch (decision.Action)
        {
            case FaultAction.Pass:
            {
                TResp r = await deliverNow().ConfigureAwait(false);
                AppendEvent(new NemesisEvent(NemesisEventKind.Delivery, env));
                return r;
            }

            case FaultAction.Drop:
                AppendEvent(new NemesisEvent(NemesisEventKind.Drop, env));
                return neutral;

            case FaultAction.Fail:
                AppendEvent(new NemesisEvent(NemesisEventKind.Fail, env));
                throw decision.Exception();

            case FaultAction.Delay:
            {
                Func<Task<TResp>> deferred = makeDeferredDeliver(); // deep-copy now, before buffer reuse
                AppendEvent(new NemesisEvent(NemesisEventKind.Delay, env, decision.Delay.ToString()));
                try
                {
                    await Task.Delay(decision.Delay, ct).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    AppendEvent(new NemesisEvent(NemesisEventKind.Cancellation, env));
                    throw;
                }
                TResp r = await deferred().ConfigureAwait(false);
                AppendEvent(new NemesisEvent(NemesisEventKind.Delivery, env));
                return r;
            }

            case FaultAction.Duplicate:
            {
                Func<Task<TResp>> deferred = makeDeferredDeliver(); // deep-copied second copy
                AppendEvent(new NemesisEvent(NemesisEventKind.Duplicate, env));
                TResp first = await deliverNow().ConfigureAwait(false);
                AppendEvent(new NemesisEvent(NemesisEventKind.Delivery, env, "copy 1"));
                await deferred().ConfigureAwait(false);
                AppendEvent(new NemesisEvent(NemesisEventKind.Delivery, env, "copy 2"));
                return first;
            }

            case FaultAction.Hold:
            default:
            {
                Func<Task<TResp>> deferred = makeDeferredDeliver(); // deep-copy now
                TaskCompletionSource<TResp> tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
                HeldItem item = new(
                    env.Sequence, env,
                    Deliver: async () =>
                    {
                        try { tcs.TrySetResult(await deferred().ConfigureAwait(false)); }
                        catch (Exception e) { tcs.TrySetException(e); }
                    },
                    Cancel: () => tcs.TrySetCanceled());
                lock (_lock) _held.Add(item);
                AppendEvent(new NemesisEvent(NemesisEventKind.Hold, env));

                if (ct.CanBeCanceled)
                {
                    ct.Register(() =>
                    {
                        bool removed;
                        lock (_lock) removed = _held.Remove(item);
                        if (removed)
                        {
                            item.Cancel();
                            AppendEvent(new NemesisEvent(NemesisEventKind.Cancellation, env));
                        }
                    });
                }

                return await tcs.Task.ConfigureAwait(false);
            }
        }
    }

    // ── ICommunication (non-batch) ─────────────────────────────────────────────────

    public Task<HandshakeResponse> Handshake(RaftManager manager, RaftNode node, HandshakeRequest request) =>
        InterceptAsync(manager.LocalEndpoint, node.Endpoint, NemesisVerb.Handshake, request.Partition, $"maxLog={request.MaxLogId}",
            () => _inner.Handshake(manager, node, request),
            () => () => _inner.Handshake(manager, node, request),
            new HandshakeResponse(), CancellationToken.None);

    public Task<RequestVotesResponse> RequestVotes(RaftManager manager, RaftNode node, RequestVotesRequest request) =>
        InterceptAsync(manager.LocalEndpoint, node.Endpoint, NemesisVerb.RequestVotes, request.Partition, $"term={request.Term}",
            () => _inner.RequestVotes(manager, node, request),
            () => () => _inner.RequestVotes(manager, node, request),
            new RequestVotesResponse(), CancellationToken.None);

    public Task<VoteResponse> Vote(RaftManager manager, RaftNode node, VoteRequest request) =>
        InterceptAsync(manager.LocalEndpoint, node.Endpoint, NemesisVerb.Vote, request.Partition, $"term={request.Term}",
            () => _inner.Vote(manager, node, request),
            () => () => _inner.Vote(manager, node, request),
            new VoteResponse(), CancellationToken.None);

    public Task<AppendLogsResponse> AppendLogs(RaftManager manager, RaftNode node, AppendLogsRequest request) =>
        InterceptAsync(manager.LocalEndpoint, node.Endpoint, NemesisVerb.AppendLogs, request.Partition, $"prev={request.PrevLogIndex}",
            () => _inner.AppendLogs(manager, node, request),
            () => { AppendLogsRequest copy = CloneAppendLogs(request); return () => _inner.AppendLogs(manager, node, copy); },
            new AppendLogsResponse(), CancellationToken.None);

    public Task<CompleteAppendLogsResponse> CompleteAppendLogs(RaftManager manager, RaftNode node, CompleteAppendLogsRequest request) =>
        InterceptAsync(manager.LocalEndpoint, node.Endpoint, NemesisVerb.CompleteAppendLogs, request.Partition, $"commit={request.CommitIndex}",
            () => _inner.CompleteAppendLogs(manager, node, request),
            () => () => _inner.CompleteAppendLogs(manager, node, request),
            new CompleteAppendLogsResponse(), CancellationToken.None);

    public Task<JoinResponse> SendJoin(RaftManager manager, RaftNode node, JoinRequest request) =>
        InterceptAsync(manager.LocalEndpoint, node.Endpoint, NemesisVerb.Join, null, request.Endpoint,
            () => _inner.SendJoin(manager, node, request),
            () => () => _inner.SendJoin(manager, node, request),
            new JoinResponse(false), CancellationToken.None);

    public Task<LeaveResponse> SendLeave(RaftManager manager, RaftNode node, LeaveRequest request, CancellationToken cancellationToken = default) =>
        InterceptAsync(manager.LocalEndpoint, node.Endpoint, NemesisVerb.Leave, null, request.Endpoint,
            () => _inner.SendLeave(manager, node, request, cancellationToken),
            () => () => _inner.SendLeave(manager, node, request, cancellationToken),
            new LeaveResponse(false), cancellationToken);

    public Task<GossipAck> SendGossip(RaftManager manager, RaftNode node, GossipMessage digest, CancellationToken cancellationToken = default) =>
        InterceptAsync(manager.LocalEndpoint, node.Endpoint, NemesisVerb.Gossip, null, null,
            () => _inner.SendGossip(manager, node, digest, cancellationToken),
            () => () => _inner.SendGossip(manager, node, digest, cancellationToken),
            new GossipAck(0, null), cancellationToken);

    public Task<Gossip.PingResponse> SendPing(RaftManager manager, RaftNode node, Gossip.PingRequest request, CancellationToken cancellationToken = default) =>
        InterceptAsync(manager.LocalEndpoint, node.Endpoint, NemesisVerb.Ping, null, null,
            () => _inner.SendPing(manager, node, request, cancellationToken),
            () => () => _inner.SendPing(manager, node, request, cancellationToken),
            new Gossip.PingResponse(false, 0), cancellationToken);

    public Task<Gossip.PingReqResponse> SendPingReq(RaftManager manager, RaftNode node, Gossip.PingReqRequest request, CancellationToken cancellationToken = default) =>
        InterceptAsync(manager.LocalEndpoint, node.Endpoint, NemesisVerb.PingReq, null, request.TargetEndpoint,
            () => _inner.SendPingReq(manager, node, request, cancellationToken),
            () => () => _inner.SendPingReq(manager, node, request, cancellationToken),
            new Gossip.PingReqResponse(false), cancellationToken);

    public Task<long?> GetRemoteFollowerLag(RaftManager manager, RaftNode node, int partitionId, string followerEndpoint) =>
        InterceptAsync(manager.LocalEndpoint, node.Endpoint, NemesisVerb.GetFollowerLag, partitionId, followerEndpoint,
            () => _inner.GetRemoteFollowerLag(manager, node, partitionId, followerEndpoint),
            () => () => _inner.GetRemoteFollowerLag(manager, node, partitionId, followerEndpoint),
            (long?)null, CancellationToken.None);

    public Task<SnapshotResponse> SendInstallSnapshot(RaftManager manager, RaftNode node, SnapshotRequest request, CancellationToken cancellationToken = default) =>
        InterceptAsync(manager.LocalEndpoint, node.Endpoint, NemesisVerb.InstallSnapshot, request.PartitionId, $"{request.SessionId}#{request.ChunkIndex}{(request.IsLast ? "L" : "")}",
            () => _inner.SendInstallSnapshot(manager, node, request, cancellationToken),
            () => { SnapshotRequest copy = CloneSnapshot(request); return () => _inner.SendInstallSnapshot(manager, node, copy, cancellationToken); },
            new SnapshotResponse(false), cancellationToken);

    public Task NotifyJoinBlocked(RaftManager manager, string targetEndpoint, string reason, CancellationToken cancellationToken = default) =>
        InterceptAsync(manager.LocalEndpoint, targetEndpoint, NemesisVerb.NotifyJoinBlocked, null, reason,
            async () => { await _inner.NotifyJoinBlocked(manager, targetEndpoint, reason, cancellationToken).ConfigureAwait(false); return (object?)null; },
            () => async () => { await _inner.NotifyJoinBlocked(manager, targetEndpoint, reason, cancellationToken).ConfigureAwait(false); return (object?)null; },
            null, cancellationToken);

    // ── BatchRequests (per-item classification) ────────────────────────────────────

    public async Task<BatchRequestsResponse> BatchRequests(RaftManager manager, RaftNode node, BatchRequestsRequest request)
    {
        if (request.Requests is null || request.Requests.Count == 0)
            return await _inner.BatchRequests(manager, node, request).ConfigureAwait(false);

        string source = manager.LocalEndpoint;
        string dest = node.Endpoint;

        List<BatchRequestsRequestItem> passItems = [];
        List<(NemesisEnvelope Env, Func<Task> Deliver)> delayed = [];
        List<(NemesisEnvelope Env, Func<Task> Deliver)> duplicated = [];

        lock (_lock)
        {
            foreach (BatchRequestsRequestItem item in request.Requests)
            {
                NemesisVerb verb = VerbOfBatchItem(item.Type);
                int? partition = PartitionOfBatchItem(item);
                NemesisEnvelope env = new(++_seq, source, dest, verb, partition, item.Type.ToString());
                NemesisDecision decision = Decide(env);
                RecordEventLocked(new NemesisEvent(NemesisEventKind.Decision, env, decision.Action.ToString()));

                switch (decision.Action)
                {
                    case FaultAction.Pass:
                        passItems.Add(item);
                        break;

                    case FaultAction.Drop:
                        RecordEventLocked(new NemesisEvent(NemesisEventKind.Drop, env));
                        break;

                    case FaultAction.Fail:
                        // No per-item response exists in a batch; a failed item is simply not delivered.
                        RecordEventLocked(new NemesisEvent(NemesisEventKind.Fail, env));
                        break;

                    case FaultAction.Hold:
                    {
                        BatchRequestsRequestItem copy = CloneBatchItem(item);
                        NemesisEnvelope e = env;
                        HeldItem held = new(e.Sequence, e,
                            Deliver: () => _inner.BatchRequests(manager, node, new BatchRequestsRequest { Requests = [copy] }),
                            Cancel: static () => { });
                        _held.Add(held);
                        RecordEventLocked(new NemesisEvent(NemesisEventKind.Hold, e));
                        break;
                    }

                    case FaultAction.Delay:
                    {
                        BatchRequestsRequestItem copy = CloneBatchItem(item);
                        TimeSpan d = decision.Delay;
                        NemesisEnvelope e = env;
                        delayed.Add((e, async () =>
                        {
                            await Task.Delay(d).ConfigureAwait(false);
                            await _inner.BatchRequests(manager, node, new BatchRequestsRequest { Requests = [copy] }).ConfigureAwait(false);
                        }));
                        RecordEventLocked(new NemesisEvent(NemesisEventKind.Delay, e, d.ToString()));
                        break;
                    }

                    case FaultAction.Duplicate:
                    {
                        BatchRequestsRequestItem copy = CloneBatchItem(item);
                        BatchRequestsRequestItem original = item;
                        NemesisEnvelope e = env;
                        duplicated.Add((e, async () =>
                        {
                            await _inner.BatchRequests(manager, node, new BatchRequestsRequest { Requests = [original] }).ConfigureAwait(false);
                            await _inner.BatchRequests(manager, node, new BatchRequestsRequest { Requests = [copy] }).ConfigureAwait(false);
                        }));
                        RecordEventLocked(new NemesisEvent(NemesisEventKind.Duplicate, e));
                        break;
                    }
                }
            }
        }

        // Deliver duplicated items (their own single-item batches), outside the lock.
        foreach ((NemesisEnvelope env, Func<Task> deliver) in duplicated)
        {
            await deliver().ConfigureAwait(false);
            AppendEvent(new NemesisEvent(NemesisEventKind.Delivery, env, "duplicated"));
        }

        // Fire-and-forget the delayed single-item batches (they must not block the passing items).
        foreach ((NemesisEnvelope env, Func<Task> deliver) in delayed)
            _ = DeliverDelayedBatchItem(env, deliver);

        if (passItems.Count > 0)
        {
            BatchRequestsResponse resp = await _inner.BatchRequests(manager, node, new BatchRequestsRequest { Requests = passItems }).ConfigureAwait(false);
            foreach (BatchRequestsRequestItem item in passItems)
                AppendEvent(new NemesisEvent(NemesisEventKind.Delivery, new NemesisEnvelope(0, source, dest, VerbOfBatchItem(item.Type), PartitionOfBatchItem(item), item.Type.ToString())));
            return resp;
        }

        return new BatchRequestsResponse();
    }

    private async Task DeliverDelayedBatchItem(NemesisEnvelope env, Func<Task> deliver)
    {
        await deliver().ConfigureAwait(false);
        AppendEvent(new NemesisEvent(NemesisEventKind.Delivery, env, "delayed"));
    }

    // ── mapping + cloning ─────────────────────────────────────────────────────────

    private static NemesisVerb VerbOfBatchItem(BatchRequestsRequestType type) => type switch
    {
        BatchRequestsRequestType.Ping => NemesisVerb.Ping,
        BatchRequestsRequestType.Handshake => NemesisVerb.Handshake,
        BatchRequestsRequestType.Vote => NemesisVerb.Vote,
        BatchRequestsRequestType.RequestVote => NemesisVerb.RequestVotes,
        BatchRequestsRequestType.AppendLogs => NemesisVerb.AppendLogs,
        BatchRequestsRequestType.CompleteAppendLogs => NemesisVerb.CompleteAppendLogs,
        BatchRequestsRequestType.StepDownNotice => NemesisVerb.StepDownNotice,
        BatchRequestsRequestType.TransferLeadership => NemesisVerb.TransferLeadership,
        BatchRequestsRequestType.TransferLeadershipSuggestion => NemesisVerb.TransferLeadershipSuggestion,
        _ => NemesisVerb.Ping,
    };

    private static int? PartitionOfBatchItem(BatchRequestsRequestItem item) => item.Type switch
    {
        BatchRequestsRequestType.Handshake => item.Handshake?.Partition,
        BatchRequestsRequestType.Vote => item.Vote?.Partition,
        BatchRequestsRequestType.RequestVote => item.RequestVotes?.Partition,
        BatchRequestsRequestType.AppendLogs => item.AppendLogs?.Partition,
        BatchRequestsRequestType.CompleteAppendLogs => item.CompleteAppendLogs?.Partition,
        BatchRequestsRequestType.StepDownNotice => item.StepDownNotice?.Partition,
        BatchRequestsRequestType.TransferLeadership => item.TransferLeadership?.Partition,
        BatchRequestsRequestType.TransferLeadershipSuggestion => item.TransferLeadershipSuggestion?.Partition,
        _ => null,
    };

    private static BatchRequestsRequestItem CloneBatchItem(BatchRequestsRequestItem item)
    {
        // Only AppendLogs carries reused buffers; deep-copy it, otherwise reuse the immutable-ish payload.
        if (item.Type == BatchRequestsRequestType.AppendLogs && item.AppendLogs is not null)
            return new BatchRequestsRequestItem { Type = item.Type, AppendLogs = CloneAppendLogs(item.AppendLogs) };
        return item;
    }

    private static SnapshotRequest CloneSnapshot(SnapshotRequest r) => new()
    {
        SessionId = r.SessionId,
        PartitionId = r.PartitionId,
        SnapshotIndex = r.SnapshotIndex,
        FollowerEndpoint = r.FollowerEndpoint,
        LeaderEndpoint = r.LeaderEndpoint,
        LeaderTerm = r.LeaderTerm,
        LastIncludedTerm = r.LastIncludedTerm,
        ChunkIndex = r.ChunkIndex,
        IsLast = r.IsLast,
        Kind = r.Kind,
        Data = r.Data.ToArray(), // deep copy the pooled bytes
    };

    private static AppendLogsRequest CloneAppendLogs(AppendLogsRequest r) => new(
        r.Partition, r.Term, r.Time, r.Endpoint,
        r.Logs is null ? null : [.. r.Logs.Select(CloneLog)],
        r.PrevLogIndex, r.PrevLogTerm)
    { Quiesce = r.Quiesce };

    private static RaftLog CloneLog(RaftLog l) => new()
    {
        Id = l.Id,
        Type = l.Type,
        Term = l.Term,
        Time = l.Time,
        LogType = l.LogType,
        LogData = l.LogData is null ? null : l.LogData[..],
    };
}
