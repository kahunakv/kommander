using Kommander.Data;
using Kommander.Scheduling;
using Microsoft.Extensions.Logging;

namespace Kommander.Consensus;

/// <summary>
/// Read-index leadership confirmation (Raft dissertation §6.4) and check-quorum bookkeeping for
/// one partition.
///
/// <para><b>What it guarantees.</b> A local read served after a successful confirmation is
/// linearizable. Without it, a minority-partitioned leader that has not yet heard of its own
/// deposition serves stale state as an authoritative success — writes already fail on such a node
/// because replication fails, and this closes the read half. Confirmation is two halves and both
/// are required: a same-term quorum ack round proves leadership, and the local applied frontier
/// must then cover the commit index captured at confirmation time.</para>
///
/// <para><b>Cost.</b> Concurrent callers coalesce into the in-flight round, and a confirmation
/// completed within the last heartbeat interval is reused outright, so steady-state cost is about
/// one quorum round-trip per heartbeat interval regardless of read volume.</para>
///
/// <para><b>Concurrency.</b> Invoked only on the partition executor thread and holds no locks by
/// design. Every waiter list is a plain collection for that reason.</para>
/// </summary>
internal sealed class ReadIndexCoordinator
{
    private readonly IRaftPartitionHost host;
    private readonly RaftPartitionCoreState coreState;
    private readonly IRaftOperationReplySink replySink;
    private readonly ILogger<IRaft> logger;

    /// <summary>
    /// Forces a heartbeat so every peer produces a same-term ack. Injected rather than called on a
    /// parent reference: the ack round is the leadership proof, so opening a round has to be able
    /// to drive the heartbeat path without this type knowing anything else about it.
    /// </summary>
    private readonly Func<bool, Task> sendHeartbeat;

    /// <summary>
    /// The in-flight read-index confirmation round, or <see langword="null"/> when none is open.
    /// Failed wholesale on every leadership-loss transition via <see cref="FailAllWaiters"/>, so a
    /// surviving round always belongs to the current term.
    /// </summary>
    private ReadIndexRound? readIndexRound;

    /// <summary>
    /// Confirmation callers that arrived while a round was in flight but could NOT join it because
    /// the commit frontier had advanced past the round's captured read index — joining would let
    /// their read miss a write that completed before the read started. They form the next round,
    /// started as soon as the current one confirms or expires. StartedTicks is each caller's own
    /// arrival time, so its timeout budget spans the full wait, not just its own round.
    /// </summary>
    private readonly List<(ulong CorrelationId, long StartedTicks)> readIndexPendingWaiters = [];

    /// <summary>
    /// Readers waiting for <see cref="RaftPartitionCoreState.LastAppliedIndex"/> to cover a
    /// quorum-confirmed commit index — the second half of the read-index contract. Shared by two
    /// producers: leader-side <see cref="ConfirmLeadershipAsync"/> waiters (their round confirmed
    /// here) and non-leader <see cref="WaitLocalApplication"/> waiters (their index was confirmed
    /// on the remote leader), so expiry must run from the tick in every node state, not just
    /// Leader.
    /// </summary>
    private readonly List<(ulong CorrelationId, long RequiredIndex, long StartedTicks)> readIndexApplyWaiters = [];

    /// <summary>Monotonic timestamp of the last completed quorum confirmation (0 = none). A
    /// confirmation no older than one heartbeat interval is reused as a fast path — it is exactly
    /// as fresh as the acks it counted, and pre-vote leader stickiness prevents any rival from
    /// assembling an election quorum inside that window.</summary>
    private long lastLeadershipConfirmedTicks;

    /// <summary>Term the last quorum confirmation was completed in; the fast path is fenced on it
    /// so a confirmation from a previous leadership stint can never be reused.</summary>
    private long lastLeadershipConfirmedTerm = -1;

    /// <summary>
    /// Per-peer monotonic timestamp of the last same-term successful append/heartbeat ack.
    /// Feeds the check-quorum window (<see cref="RaftConfiguration.EnableCheckQuorum"/>): unlike
    /// the replication progress maps these carry recency, which is what an isolated leader lacks.
    /// Only term-stamped acks are recorded — an unstamped ack passed the term fence by default and
    /// proves nothing about this term. Cleared on every leadership transition.
    /// </summary>
    private readonly Dictionary<string, long> lastVoterAckTicks = [];

    /// <summary>Monotonic timestamp when this leader last heard same-term acks from a majority of
    /// voters (refreshed while quiesced, since a quiesced leader legitimately receives none).
    /// When it falls behind the check-quorum window the leader steps down.</summary>
    private long lastQuorumContactTicks;

    /// <summary>
    /// Bookkeeping for one read-index quorum round: the commit frontier captured at round start,
    /// the same-term acks collected so far, and the reply correlations awaiting the result.
    /// Only ever touched on the partition executor thread.
    /// </summary>
    private sealed class ReadIndexRound
    {
        public long Term { get; init; }
        public long ReadIndex { get; init; }
        public long StartedTicks { get; init; }
        public HashSet<string> Acks { get; } = [];
        public List<ulong> Waiters { get; } = [];
    }

    public ReadIndexCoordinator(
        IRaftPartitionHost host,
        RaftPartitionCoreState coreState,
        IRaftOperationReplySink replySink,
        ILogger<IRaft> logger,
        Func<bool, Task> sendHeartbeat)
    {
        this.host = host;
        this.coreState = coreState;
        this.replySink = replySink;
        this.logger = logger;
        this.sendHeartbeat = sendHeartbeat;
    }

    /// <summary>
    /// Whether any read-index work is outstanding. The tick uses this to skip the expiry sweep
    /// entirely in the steady state, which is the common case.
    /// </summary>
    public bool HasOutstandingWork =>
        readIndexRound is not null || readIndexPendingWaiters.Count > 0 || readIndexApplyWaiters.Count > 0;

    private void CompleteReply(ulong? correlationId, RaftResponse response)
    {
        if (correlationId is not null)
            replySink.TryComplete(correlationId.Value, response);
    }

    /// <summary>
    /// Read-index leadership confirmation. Proves this node is still the leader with a same-term
    /// quorum ack round, then completes the reply once the local applied frontier covers the
    /// commit index captured at confirmation time.
    /// <para>Fails immediately when this node is not the <b>published</b> leader: while the
    /// promotion barrier is armed, <c>NodeState</c> is already Leader but <c>host.Leader</c> is
    /// not, and a confirmation must not leak through before the barrier publishes.</para>
    /// <para>Expiry (<see cref="RaftConfiguration.LeadershipConfirmationTimeout"/>) is enforced
    /// from the leader tick; leadership loss fails all waiters via
    /// <see cref="FailAllWaiters"/>.</para>
    /// </summary>
    public async Task ConfirmLeadershipAsync(ulong? replyCorrelationId)
    {
        if (coreState.NodeState != RaftNodeState.Leader || host.Leader != host.LocalEndpoint)
        {
            CompleteReply(replyCorrelationId, new(RaftResponseType.None, RaftOperationStatus.NodeIsNotLeader, 0L));
            return;
        }

        // No observer: nothing to confirm for. (The public path always registers a reply.)
        if (replyCorrelationId is null)
            return;

        long nowTicks = host.GetMonotonicTimestamp();

        // Fast path: a confirmation completed within the last heartbeat interval is exactly as
        // fresh as the acks it counted — pre-vote leader stickiness keeps any rival from winning
        // an election quorum inside that window. The applied-frontier wait still runs against the
        // CURRENT commit frontier (strictly stronger than the round's capture), so a write this
        // leader committed after the confirmation is never missed by a later read.
        if (lastLeadershipConfirmedTerm == coreState.CurrentTerm
            && lastLeadershipConfirmedTicks != 0
            && RaftMonotonic.Elapsed(lastLeadershipConfirmedTicks, nowTicks) < host.Configuration.HeartbeatInterval)
        {
            CompleteOrParkReadIndexWaiter(replyCorrelationId.Value, coreState.LocalCommittedIndex, nowTicks);
            return;
        }

        if (readIndexRound is not null)
        {
            // Coalesce: joining an in-flight round is only linearizable while the round's captured
            // read index still covers the commit frontier — otherwise a write that completed after
            // round start (but before this read began) could be missed. Late arrivals chain into
            // the next round instead.
            if (readIndexRound.ReadIndex >= coreState.LocalCommittedIndex)
                readIndexRound.Waiters.Add(replyCorrelationId.Value);
            else
                readIndexPendingWaiters.Add((replyCorrelationId.Value, nowTicks));
            return;
        }

        await StartReadIndexRoundAsync(replyCorrelationId.Value, nowTicks).ConfigureAwait(false);
    }

    /// <summary>
    /// Opens a read-index round: captures <c>(CurrentTerm, LocalCommittedIndex)</c> and fires a
    /// forced heartbeat so every peer produces a same-term ack. A quiesced leader is woken first —
    /// the ack round itself is the leadership proof, so quiescence needs no safety carve-out, only
    /// this wake-up; the tick then keeps heartbeating (and thus retrying the round) until the
    /// waiters confirm or expire.
    /// </summary>
    private async Task StartReadIndexRoundAsync(ulong waiterCorrelationId, long startedTicks)
    {
        ReadIndexRound round = new()
        {
            Term = coreState.CurrentTerm,
            ReadIndex = coreState.LocalCommittedIndex,
            StartedTicks = startedTicks,
        };
        round.Waiters.Add(waiterCorrelationId);
        readIndexRound = round;

        // Single-voter cluster: the leader alone is the quorum — confirm without any ack round.
        if (TryConfirmReadIndexQuorum())
            return;

        if (coreState.Quiesced)
            coreState.SetQuiesced(false);

        await sendHeartbeat(true).ConfigureAwait(false);
    }

    /// <summary>
    /// Confirms the in-flight round if its same-term acks (plus the leader itself) reach a
    /// majority of voters. Voter status is re-evaluated at count time so an endpoint demoted
    /// mid-round cannot carry the quorum. On confirmation every waiter moves to the
    /// applied-frontier wait keyed on the round's captured read index.
    /// </summary>
    private bool TryConfirmReadIndexQuorum()
    {
        ReadIndexRound? round = readIndexRound;
        if (round is null)
            return false;

        int votersTotal = 1;  // the local leader
        int acked = 0;
        foreach (RaftNode node in host.Nodes)
        {
            if (!host.IsVoter(node.Endpoint))
                continue;
            votersTotal++;
            if (round.Acks.Contains(node.Endpoint))
                acked++;
        }

        if (acked + 1 < votersTotal / 2 + 1)
            return false;

        readIndexRound = null;
        lastLeadershipConfirmedTicks = host.GetMonotonicTimestamp();
        lastLeadershipConfirmedTerm = round.Term;

        foreach (ulong waiter in round.Waiters)
            CompleteOrParkReadIndexWaiter(waiter, round.ReadIndex, round.StartedTicks);

        return true;
    }

    /// <summary>
    /// Second half of the read-index contract: the reply completes only once
    /// <see cref="RaftPartitionCoreState.LastAppliedIndex"/> covers the captured commit index, so a
    /// local read issued after the confirmation observes every entry committed at capture time.
    /// </summary>
    private void CompleteOrParkReadIndexWaiter(ulong correlationId, long requiredIndex, long startedTicks)
    {
        if (coreState.LastAppliedIndex >= requiredIndex)
        {
            CompleteReply(correlationId, new(RaftResponseType.None, RaftOperationStatus.Success, requiredIndex));
            return;
        }

        readIndexApplyWaiters.Add((correlationId, requiredIndex, startedTicks));
    }

    /// <summary>
    /// Non-leader half of <c>IRaft.ConfirmLocalApplicationAsync</c>: parks the caller until
    /// <see cref="RaftPartitionCoreState.LastAppliedIndex"/> covers <paramref name="requiredIndex"/> — a commit index the
    /// partition leader confirmed with a same-term quorum ack round <b>after the caller's request
    /// began</b>. The leadership proof already happened remotely, so no node-state guard applies
    /// here; this method only supplies the local applied-frontier wait, reusing the same
    /// applied-frontier waiter list (completed wherever the applied frontier advances, expired
    /// from the tick in every node state, failed on leadership transitions). A non-success reply
    /// means "not confirmed" — the caller must skip or defer its destructive action, never treat
    /// it as "confirmed enough".
    /// </summary>
    public void WaitLocalApplication(long requiredIndex, ulong? replyCorrelationId)
    {
        if (replyCorrelationId is null)
            return;

        CompleteOrParkReadIndexWaiter(replyCorrelationId.Value, requiredIndex, host.GetMonotonicTimestamp());
    }

    /// <summary>
    /// Feeds a same-term successful append/heartbeat ack into the in-flight read-index round.
    /// Any such ack proves the peer still recognises this leader at the current term
    /// no earlier than round start (rounds only accumulate acks while open), so heartbeat and
    /// live-replication acks both count. Confirming may chain-start the next round for waiters
    /// that arrived after the commit frontier moved.
    /// </summary>
    public async ValueTask RegisterAckAsync(string endpoint)
    {
        ReadIndexRound? round = readIndexRound;
        if (round is null || !host.IsVoter(endpoint))
            return;

        round.Acks.Add(endpoint);

        if (TryConfirmReadIndexQuorum())
            await StartPendingReadIndexRoundAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Records a same-term successful ack for the check-quorum window. Separate from
    /// <see cref="RegisterAckAsync"/> because the two consume the same event for different
    /// purposes: the round proves leadership once, this tracks per-peer recency continuously.
    /// </summary>
    public void RecordVoterAck(string endpoint, long nowTicks) => lastVoterAckTicks[endpoint] = nowTicks;

    /// <summary>
    /// Starts a new round for the callers that could not join the previous one (the commit
    /// frontier had moved past its capture). Their timeout budget is anchored at the earliest
    /// caller's arrival, so chaining cannot extend a caller's wait past the configured timeout.
    /// </summary>
    private async ValueTask StartPendingReadIndexRoundAsync()
    {
        if (readIndexPendingWaiters.Count == 0 || coreState.NodeState != RaftNodeState.Leader)
            return;

        ReadIndexRound round = new()
        {
            Term = coreState.CurrentTerm,
            ReadIndex = coreState.LocalCommittedIndex,
            StartedTicks = readIndexPendingWaiters[0].StartedTicks,
        };
        foreach ((ulong correlationId, _) in readIndexPendingWaiters)
            round.Waiters.Add(correlationId);
        readIndexPendingWaiters.Clear();
        readIndexRound = round;

        if (TryConfirmReadIndexQuorum())
            return;

        if (coreState.Quiesced)
            coreState.SetQuiesced(false);

        await sendHeartbeat(true).ConfigureAwait(false);
    }

    /// <summary>
    /// Completes any quorum-confirmed readers whose required commit index the applied frontier
    /// now covers. Called wherever <see cref="RaftPartitionCoreState.LastAppliedIndex"/> advances;
    /// O(1) when no reader is parked, which is the steady state.
    /// </summary>
    public void CompleteApplyWaiters()
    {
        if (readIndexApplyWaiters.Count == 0)
            return;

        for (int i = readIndexApplyWaiters.Count - 1; i >= 0; i--)
        {
            (ulong correlationId, long requiredIndex, _) = readIndexApplyWaiters[i];
            if (coreState.LastAppliedIndex < requiredIndex)
                continue;

            CompleteReply(correlationId, new(RaftResponseType.None, RaftOperationStatus.Success, requiredIndex));
            readIndexApplyWaiters.RemoveAt(i);
        }
    }

    /// <summary>
    /// Enforces <see cref="RaftConfiguration.LeadershipConfirmationTimeout"/> from the leader
    /// tick: an expired round fails all its waiters (a minority-partitioned leader reaches this —
    /// it can never collect the acks), pending waiters that outwaited the timeout fail
    /// individually, and applied-frontier waiters are bounded too so a wedged consumer cannot
    /// park readers forever. An expired round chain-starts the next one so queued callers still
    /// get their own attempt.
    /// </summary>
    public async ValueTask ExpireWaitersAsync(long nowTicks)
    {
        TimeSpan timeout = host.Configuration.LeadershipConfirmationTimeout;

        if (readIndexRound is not null && RaftMonotonic.Elapsed(readIndexRound.StartedTicks, nowTicks) >= timeout)
        {
            ReadIndexRound round = readIndexRound;
            readIndexRound = null;

            logger.LogWarning(
                "[{LocalEndpoint}/{PartitionId}/{State}] Read-index round expired without quorum ({Waiters} waiter(s), {Acks} ack(s)). Term={CurrentTerm}",
                host.LocalEndpoint, host.PartitionId, coreState.NodeState, round.Waiters.Count, round.Acks.Count, coreState.CurrentTerm);

            foreach (ulong waiter in round.Waiters)
                CompleteReply(waiter, new(RaftResponseType.None, RaftOperationStatus.ProposalTimeout, 0L));

            await StartPendingReadIndexRoundAsync().ConfigureAwait(false);
        }

        for (int i = readIndexPendingWaiters.Count - 1; i >= 0; i--)
        {
            if (RaftMonotonic.Elapsed(readIndexPendingWaiters[i].StartedTicks, nowTicks) < timeout)
                continue;

            CompleteReply(readIndexPendingWaiters[i].CorrelationId, new(RaftResponseType.None, RaftOperationStatus.ProposalTimeout, 0L));
            readIndexPendingWaiters.RemoveAt(i);
        }

        for (int i = readIndexApplyWaiters.Count - 1; i >= 0; i--)
        {
            if (RaftMonotonic.Elapsed(readIndexApplyWaiters[i].StartedTicks, nowTicks) < timeout)
                continue;

            CompleteReply(readIndexApplyWaiters[i].CorrelationId, new(RaftResponseType.None, RaftOperationStatus.ProposalTimeout, 0L));
            readIndexApplyWaiters.RemoveAt(i);
        }
    }

    /// <summary>
    /// Check-quorum evaluation for one leader tick. Returns <see langword="true"/> when this
    /// leader has not heard same-term acks from a majority of voters for the configured window and
    /// must therefore step down.
    ///
    /// <para>A quiesced leader stops heartbeating, so an absence of acks proves nothing; the grace
    /// window is kept fresh instead so it restarts on un-quiesce. This does not close the
    /// stale-read hole (<see cref="ConfirmLeadershipAsync"/> does); it bounds how long an isolated
    /// leader lingers so minority-side callers fail fast.</para>
    /// </summary>
    public bool ShouldStepDownOnQuorumLoss(long nowTicks)
    {
        if (coreState.Quiesced)
        {
            lastQuorumContactTicks = nowTicks;
            return false;
        }

        TimeSpan window = host.Configuration.HeartbeatInterval * host.Configuration.CheckQuorumIntervalMultiplier;
        int votersTotal = 1;    // the local leader
        int reachable = 1;
        foreach (RaftNode node in host.Nodes)
        {
            if (!host.IsVoter(node.Endpoint))
                continue;
            votersTotal++;
            if (lastVoterAckTicks.TryGetValue(node.Endpoint, out long ackTicks)
                && RaftMonotonic.Elapsed(ackTicks, nowTicks) < window)
                reachable++;
        }

        if (reachable >= votersTotal / 2 + 1)
        {
            lastQuorumContactTicks = nowTicks;
            return false;
        }

        return lastQuorumContactTicks != 0
            && RaftMonotonic.Elapsed(lastQuorumContactTicks, nowTicks) >= window;
    }

    /// <summary>
    /// Fails every read-index waiter (in-flight round, chained, and applied-frontier) and resets
    /// the confirmation fast path and check-quorum bookkeeping. Must run on every leadership-loss
    /// transition — a confirmation must never survive the term it was requested in. Invoked from
    /// <c>FailAllActiveProposalWaiters</c>, which every demotion path already calls.
    /// </summary>
    public void FailAllWaiters()
    {
        lastLeadershipConfirmedTicks = 0;
        lastLeadershipConfirmedTerm = -1;
        lastVoterAckTicks.Clear();
        lastQuorumContactTicks = 0;

        if (readIndexRound is not null)
        {
            foreach (ulong waiter in readIndexRound.Waiters)
                CompleteReply(waiter, new(RaftResponseType.None, RaftOperationStatus.NodeIsNotLeader, 0L));
            readIndexRound = null;
        }

        if (readIndexPendingWaiters.Count > 0)
        {
            foreach ((ulong correlationId, _) in readIndexPendingWaiters)
                CompleteReply(correlationId, new(RaftResponseType.None, RaftOperationStatus.NodeIsNotLeader, 0L));
            readIndexPendingWaiters.Clear();
        }

        if (readIndexApplyWaiters.Count > 0)
        {
            foreach ((ulong correlationId, _, _) in readIndexApplyWaiters)
                CompleteReply(correlationId, new(RaftResponseType.None, RaftOperationStatus.NodeIsNotLeader, 0L));
            readIndexApplyWaiters.Clear();
        }
    }

    /// <summary>
    /// Resets read-index and check-quorum bookkeeping at the start of a leadership stint: any
    /// remembered confirmation or ack recency belongs to a previous stint and must not be reused,
    /// and the check-quorum grace window starts now (a fresh leader has heard from a quorum by
    /// definition — its election — but those grants are not append acks).
    /// </summary>
    public void ResetForNewLeadership(long nowTicks)
    {
        lastLeadershipConfirmedTicks = 0;
        lastLeadershipConfirmedTerm = -1;
        lastVoterAckTicks.Clear();
        lastQuorumContactTicks = nowTicks;
    }
}
