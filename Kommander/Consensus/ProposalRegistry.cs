using System.Diagnostics.CodeAnalysis;
using Kommander.Data;
using Kommander.Scheduling;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kommander.Consensus;

/// <summary>
/// The leader's in-flight proposal ledger: every proposal awaiting quorum, the client tickets that
/// resolve against them, and the pending-WAL-operation metadata that links a WAL completion back to
/// the operation that enqueued it.
///
/// <para><b>Why the two live together.</b> A proposal's lifetime is bounded by its WAL operations:
/// the ticket is released when the propose quorum is durable, and the pending-operation record is
/// what carries the reply correlation from enqueue to completion. Splitting them would put the two
/// halves of one lifetime in different types with no owner for the invariant that both are cleared
/// on the same transition.</para>
///
/// <para><b>Allocation.</b> Both scratch lists and the pending-operation pool are deliberate
/// hot-path allocation guards — the retry sweep and the settled-proposal drain run per heartbeat,
/// and pending records are rented on the highest-frequency path a leader has. They are fields
/// rather than locals for that reason; keep them here rather than reintroducing per-call
/// allocations at the type boundary. A benchmark chose pooling over a struct value type, which
/// regressed by enlarging every dictionary entry (see <see cref="RaftPendingWalOperation"/>).</para>
///
/// <para><b>Concurrency.</b> Touched only on the partition executor thread; holds no locks by
/// design.</para>
/// </summary>
internal sealed class ProposalRegistry
{
    private const int MaxPendingWalOpPool = 256;

    private readonly IRaftPartitionHost host;
    private readonly RaftPartitionCoreState coreState;
    private readonly ILogger<IRaft> logger;

    /// <summary>
    /// Re-sends one proposal's entries to a peer that has not acked. Injected because the send path
    /// is the replication concern, not the ledger's: the registry decides <em>which</em> proposals
    /// are stale and <em>who</em> still owes an ack, and hands the sending to the caller.
    /// </summary>
    private readonly Action<RaftNode, HLCTimestamp, List<RaftLog>> appendLogToNode;

    private readonly Dictionary<HLCTimestamp, RaftProposalQuorum> activeProposals = [];

    private readonly Dictionary<long, RaftPendingWalOperation> pendingWalOperations = [];

    // Per-instance pool for the pending-WAL-op metadata objects. Rented on insert, returned once the
    // completion has drained the entry. Safe without synchronization because the state machine runs
    // single-threaded on its partition executor; bounded so a burst of in-flight ops cannot retain an
    // unbounded number of pooled objects.
    private readonly Stack<RaftPendingWalOperation> pendingWalOpPool = new();

    // Reusable scratch buffer for the settled-proposal drain so the periodic sweep does not allocate
    // a collection every time. Executor thread only; always cleared before use.
    private readonly List<HLCTimestamp> settledProposalScratch = [];

    /// <summary>
    /// Scratch buffer for <see cref="RetryUnresolved"/>: the not-yet-acked voter endpoints of one
    /// proposal. Executor thread only, cleared before each use.
    /// </summary>
    private readonly List<string> proposalResendScratch = [];

    public ProposalRegistry(
        IRaftPartitionHost host,
        RaftPartitionCoreState coreState,
        ILogger<IRaft> logger,
        Action<RaftNode, HLCTimestamp, List<RaftLog>> appendLogToNode)
    {
        this.host = host;
        this.coreState = coreState;
        this.logger = logger;
        this.appendLogToNode = appendLogToNode;
    }

    // ── active proposals ──────────────────────────────────────────────────────────────────────

    public int ActiveCount => activeProposals.Count;

    public bool TryAdd(HLCTimestamp ticket, RaftProposalQuorum proposal) => activeProposals.TryAdd(ticket, proposal);

    public bool TryGet(HLCTimestamp ticket, [MaybeNullWhen(false)] out RaftProposalQuorum proposal) => activeProposals.TryGetValue(ticket, out proposal);

    /// <summary>
    /// Whether any proposal is still short of quorum. Gates the checkpoint path, which must not
    /// interleave with an unresolved proposal.
    /// </summary>
    public bool HasUnresolvedProposal()
    {
        foreach (KeyValuePair<HLCTimestamp, RaftProposalQuorum> proposal in activeProposals)
        {
            if (!proposal.Value.HasQuorum())
                return true;
        }

        return false;
    }

    /// <summary>
    /// Completes the event-driven waiters for all active proposals with a failure result so that
    /// any caller awaiting them via <c>WaitForQuorum</c> is unblocked immediately when leadership is
    /// lost, then drops them.
    /// <para>The two steps are one method because doing them in the wrong order silently strands
    /// every awaiting caller: the proposal objects must still be reachable when their waiters are
    /// completed. This was previously two adjacent statements repeated at ten call sites.</para>
    /// </summary>
    public void FailAllWaitersAndClear()
    {
        foreach (RaftProposalQuorum proposal in activeProposals.Values)
            proposal.CompleteWaiter(RaftProposalTicketState.NotFound, -1);

        activeProposals.Clear();
    }

    /// <summary>
    /// Drops all active proposals <b>without</b> completing their waiters. Only for the paths that
    /// historically did exactly this; prefer <see cref="FailAllWaitersAndClear"/>.
    /// </summary>
    public void ClearWithoutFailingWaiters() => activeProposals.Clear();

    /// <summary>
    /// Returns the event-driven completion task for an active proposal so that callers can
    /// await it directly instead of polling <see cref="CheckTicketCompletion"/>.
    /// Returns <c>null</c> when the proposal is not found (already cleaned up or never registered),
    /// in which case the caller should fall back to a single <see cref="CheckTicketCompletion"/> poll.
    /// </summary>
    public Task<(RaftProposalTicketState, long)>? GetTicketWaiterTask(HLCTimestamp timestamp)
    {
        if (!activeProposals.TryGetValue(timestamp, out RaftProposalQuorum? proposal))
            return null;
        return proposal.GetWaiterTask();
    }

    /// <summary>
    /// Checks whether a proposal has been completed/committed or not.
    /// </summary>
    public (RaftProposalTicketState state, long commitIndex) CheckTicketCompletion(HLCTimestamp timestamp)
    {
        if (!activeProposals.TryGetValue(timestamp, out RaftProposalQuorum? proposal))
            return (RaftProposalTicketState.NotFound, -1);

        if (proposal is { AutoCommit: false, State: RaftProposalState.Completed } or { AutoCommit: true, State: RaftProposalState.Committed } or { AutoCommit: false, State: RaftProposalState.Committed })
            return (RaftProposalTicketState.Committed, proposal.LastLogIndex);

        return (RaftProposalTicketState.Proposed, -1);
    }

    /// <summary>
    /// Single-fsync commit fast path (<see cref="RaftConfiguration.WalSingleFsyncCommit"/>).
    /// For an <c>autoCommit</c> proposal whose propose quorum is already durable, advances the
    /// leader's commit frontier and moves the proposal to <see cref="RaftProposalState.Committed"/>
    /// <b>before</b> the commit fsync is enqueued, so the client ticket is released on
    /// quorum-durable rather than on the leader's own second fsync. The per-entry <c>Committed</c>
    /// record is still written by the subsequent <c>EnqueueCommit</c>; only the acknowledgement
    /// point moves earlier.
    /// <para>
    /// Safe because propose-quorum-durable is the true Raft commit point — a quorum holds the entry
    /// on disk — so "acked ⇒ durable on a quorum" is preserved. The frontier value reused here
    /// (<see cref="RaftProposalQuorum.LastLogIndex"/>) is exactly what the commit completion would
    /// set from <c>completion.MaxLogIndex</c>, and a quorum acking this proposal implies it holds
    /// every lower-id entry too (followers append contiguously), so advancing the frontier here
    /// cannot skip an unreplicated predecessor. The commit completion still runs afterward and
    /// re-applies the same (idempotent) advance.
    /// </para>
    /// <para>No-op unless the flag is on or the proposal is not <c>autoCommit</c>; the explicit
    /// two-phase path is untouched.</para>
    /// </summary>
    public void TryReleaseTicketOnQuorumDurable(RaftProposalQuorum proposal)
    {
        if (!host.Configuration.WalSingleFsyncCommit || !proposal.AutoCommit)
            return;

        if (proposal.LastLogIndex > coreState.LocalCommittedIndex)
            coreState.LocalCommittedIndex = proposal.LastLogIndex;

        proposal.SetState(RaftProposalState.Committed);
        // Unblock any caller awaiting event-driven completion; the commit completion will
        // also fire TrySetResult, but TrySetResult is idempotent so the duplicate is safe.
        proposal.CompleteWaiter(RaftProposalTicketState.Committed, proposal.LastLogIndex);
    }

    /// <summary>
    /// Re-sends the entries of proposals that have gone a heartbeat without reaching quorum, to the
    /// voters that still owe an ack. Bounded per round so a backlog retries across successive beats
    /// instead of flooding the transport. Proposals stay active until they resolve or leadership
    /// changes (which clears the map), so the retry naturally stops at both terminal outcomes.
    /// </summary>
    public void RetryUnresolved(HLCTimestamp currentTime)
    {
        const int MaxProposalsPerRound = 8;

        if (activeProposals.Count == 0)
            return;

        TimeSpan minAge = host.Configuration.HeartbeatInterval;
        int retried = 0;

        foreach (KeyValuePair<HLCTimestamp, RaftProposalQuorum> entry in activeProposals)
        {
            RaftProposalQuorum proposal = entry.Value;

            if (proposal.State != RaftProposalState.Incomplete || proposal.HasQuorum())
                continue;

            if (currentTime - proposal.StartTimestamp < minAge)
                continue;

            proposalResendScratch.Clear();
            proposal.CollectPendingEndpoints(proposalResendScratch);
            if (proposalResendScratch.Count == 0)
                continue;

            foreach (string endpoint in proposalResendScratch)
            {
                RaftNode? node = RaftPeers.FindByEndpoint(host.Nodes, endpoint);
                if (node is not null)
                    appendLogToNode(node, entry.Key, proposal.Logs);
            }

            if (logger.IsEnabled(LogLevel.Information))
                logger.LogInformation("[{LocalEndpoint}/{PartitionId}/{State}] Retrying unresolved proposal {Ticket} ({Count} entries, first {FirstId}) to {Pending} pending voter(s)",
                    host.LocalEndpoint, host.PartitionId, coreState.NodeState, entry.Key, proposal.Logs.Count,
                    proposal.Logs.Count > 0 ? proposal.Logs[0].Id : -1, proposalResendScratch.Count);

            if (++retried >= MaxProposalsPerRound)
                break;
        }

        proposalResendScratch.Clear();
    }

    /// <summary>
    /// Releases settled proposals, returning each to the pool so its log payload is not retained
    /// until the next leadership change.
    /// </summary>
    public void PruneSettled(HLCTimestamp currentTime)
    {
        TimeSpan range = TimeSpan.FromSeconds(30);

        settledProposalScratch.Clear();

        foreach (KeyValuePair<HLCTimestamp, RaftProposalQuorum> proposal in activeProposals)
        {
            if (proposal.Value.HasQuorum() && currentTime - proposal.Value.StartTimestamp > range)
                settledProposalScratch.Add(proposal.Key);
        }

        foreach (HLCTimestamp key in settledProposalScratch)
        {
            if (activeProposals.Remove(key, out RaftProposalQuorum? settled))
                RaftProposalQuorumPool.Return(settled);
        }

        settledProposalScratch.Clear();
    }

    // ── pending WAL operations ────────────────────────────────────────────────────────────────

    /// <summary>Rents a pending-operation record from the pool, or allocates when it is empty.</summary>
    public RaftPendingWalOperation RentPending() =>
        pendingWalOpPool.Count > 0 ? pendingWalOpPool.Pop() : new();

    /// <summary>Registers a rented record against the WAL operation id that will complete it.</summary>
    public void TrackPending(long operationId, RaftPendingWalOperation pending) => pendingWalOperations[operationId] = pending;

    /// <summary>Removes and yields the record for a completed WAL operation, if it is still tracked.</summary>
    public bool TryTakePending(long operationId, [MaybeNullWhen(false)] out RaftPendingWalOperation pending) =>
        pendingWalOperations.Remove(operationId, out pending);

    /// <summary>Returns a drained record to the pool, resetting it first.</summary>
    public void ReturnPending(RaftPendingWalOperation op)
    {
        op.Reset();
        if (pendingWalOpPool.Count < MaxPendingWalOpPool)
            pendingWalOpPool.Push(op);
    }
}
