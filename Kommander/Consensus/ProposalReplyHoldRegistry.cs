using Kommander.Data;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kommander.Consensus;

/// <summary>
/// The registration behind <c>IRaft.HoldCommittedProposalRepliesForTesting</c>: it intercepts the
/// release of a committed proposal's caller so a test can model a leader that is durable at quorum
/// but has not yet answered.
///
/// <para><b>What is held, and what is not.</b> Only the reply. By the time a success completion
/// reaches this type the entry is durable on a quorum, the commit markers have been fanned out,
/// the leader has applied it locally and the commit frontier has advanced. Holding the executor
/// turn instead would model a stalled leader, which is a different fault and already testable.
/// Failure completions (rollback, leader loss, pool drain) are never held: holding a failure would
/// mask the very outcome a test asserts.</para>
///
/// <para><b>No hold outlives its bound.</b> Every hold carries a timer set to
/// <see cref="RaftConfiguration.ProposalTimeout"/> — the caller's own bound. Past it the hold
/// changes nothing and only hides a leak, so the reply is released and the event is logged at
/// warning with the ticket. Disposing the registration, or stopping the partition, releases
/// everything still held, so a test that throws mid-way cannot leave a node unable to answer.</para>
///
/// <para><b>Concurrency.</b> Unlike the rest of <see cref="ProposalRegistry"/>, this type is
/// touched from four directions: the partition executor thread offers completions, the test thread
/// calls <see cref="HeldProposalReply.Release"/> / <see cref="HeldProposalReply.Drop"/>, a timer
/// thread auto-releases, and any thread may dispose. The single arbiter is
/// <see cref="HeldProposalReply.TryClaim"/> — whoever wins it owns the outcome, and every path
/// carries the entry it claimed rather than looking it up again, so clearing the map can never
/// strand a caller mid-resolution. The lock guards only the map.</para>
/// </summary>
internal sealed class ProposalReplyHoldRegistry : IDisposable
{
    /// <summary>One held reply plus the machinery that resolves it exactly once.</summary>
    private sealed class Entry
    {
        /// <summary>
        /// Assigned immediately after construction: the reply's resolve callback closes over this
        /// entry, so the two cannot be built in one step.
        /// </summary>
        public HeldProposalReply Reply = null!;

        /// <summary>
        /// The waiter source captured at hold time rather than the proposal, because proposals are
        /// pooled: by the time the hold resolves the pooled instance may have been reset for a
        /// different ticket, and completing <em>its</em> waiter would answer the wrong caller.
        /// Completing a source that was already drained by the pool is a harmless no-op.
        /// </summary>
        public required TaskCompletionSource<(RaftProposalTicketState, long)> Waiter { get; init; }

        public required long CommitIndex { get; init; }

        public Timer? Timer;
    }

    private readonly object gate = new();

    private readonly Dictionary<HLCTimestamp, Entry> holds = [];

    private readonly int partitionId;

    private readonly string localEndpoint;

    private readonly Action<HeldProposalReply> onHeld;

    private readonly TimeSpan holdBound;

    private readonly ILogger<IRaft> logger;

    /// <summary>
    /// Detaches this registration from the partition. Injected so disposal restores ordinary
    /// behaviour without this type knowing where it was installed, and so a registration that has
    /// already been replaced by a newer one cannot detach the newer one.
    /// </summary>
    private readonly Action<ProposalReplyHoldRegistry> detach;

    private int disposed;

    public ProposalReplyHoldRegistry(
        int partitionId,
        string localEndpoint,
        Action<HeldProposalReply> onHeld,
        TimeSpan holdBound,
        ILogger<IRaft> logger,
        Action<ProposalReplyHoldRegistry> detach)
    {
        this.partitionId = partitionId;
        this.localEndpoint = localEndpoint;
        this.onHeld = onHeld;
        this.holdBound = holdBound > TimeSpan.Zero ? holdBound : TimeSpan.FromSeconds(10);
        this.logger = logger;
        this.detach = detach;
    }

    /// <summary>
    /// Offers one <b>successful</b> completion to the hook. Returns <see langword="true"/> when the
    /// reply is now held and the caller must not complete the waiter itself.
    ///
    /// <para>Returns <see langword="false"/> — meaning "complete normally" — when the registration
    /// is disposed, or when the proposal has no live waiter left to hold (already answered, or the
    /// pooled instance was drained). A repeat success completion for a ticket that is already held
    /// returns <see langword="true"/> and is swallowed: the single-fsync fast path and the commit
    /// completion both fire for one auto-commit proposal, and that must produce one hold, not
    /// two.</para>
    ///
    /// <para>Called on the partition executor thread. <c>onHeld</c> is queued to the thread pool
    /// rather than invoked here, so a callback that wants to act while the reply is held does not
    /// run on the turn it is holding up.</para>
    /// </summary>
    public bool TryHoldSuccess(RaftProposalQuorum proposal, long commitIndex, ProposalReplySite site)
    {
        if (Volatile.Read(ref disposed) != 0)
            return false;

        TaskCompletionSource<(RaftProposalTicketState, long)>? waiter = proposal.WaiterSource;
        if (waiter is null || waiter.Task.IsCompleted)
            return false;

        HLCTimestamp ticket = proposal.StartTimestamp;
        Entry entry = new() { Waiter = waiter, CommitIndex = commitIndex };

        long term = proposal.Logs.Count > 0 ? proposal.Logs[0].Term : -1;
        long[] logIds = new long[proposal.Logs.Count];
        for (int i = 0; i < logIds.Length; i++)
            logIds[i] = proposal.Logs[i].Id;

        entry.Reply = new HeldProposalReply(
            partitionId, ticket, commitIndex, term, logIds, site,
            release => Resolve(entry, release));

        lock (gate)
        {
            if (disposed != 0)
                return false;

            if (holds.ContainsKey(ticket))
                return true;

            holds[ticket] = entry;
        }

        // Armed outside the lock: the callback path takes the same lock, and a very short bound
        // would otherwise fire into it while it is still held here.
        Timer timer = new(
            static state =>
            {
                (ProposalReplyHoldRegistry registry, Entry held) = ((ProposalReplyHoldRegistry, Entry))state!;
                registry.AutoRelease(held);
            },
            (this, entry),
            holdBound,
            Timeout.InfiniteTimeSpan);

        entry.Timer = timer;

        // Resolved between the insert and the arm: nothing will dispose this timer, so do it here.
        if (entry.Reply.IsResolved)
            timer.Dispose();

        if (logger.IsEnabled(LogLevel.Information))
            logger.LogInformation("[{LocalEndpoint}/{PartitionId}] Test hook holding the reply of committed proposal {Ticket} at {Site} (commit index {CommitIndex})",
                localEndpoint, partitionId, ticket, site, commitIndex);

        ThreadPool.UnsafeQueueUserWorkItem(
            static state => state.Registry.InvokeOnHeld(state.Reply),
            (Registry: this, Reply: entry.Reply),
            preferLocal: false);

        return true;
    }

    /// <summary>
    /// Discards a hold because the same ticket has just <b>failed</b> (rollback, leader loss, pool
    /// drain). The waiter is deliberately left for the failure completion to answer: a proposal
    /// cannot be simultaneously held-as-committed and failed, and the failure is the outcome the
    /// caller must see. Claiming the reply here also makes a later
    /// <see cref="HeldProposalReply.Release"/> from the test thread a no-op. A no-op when the ticket
    /// is not held, or when another path claimed the hold first — that path answers the caller.
    /// </summary>
    public void DiscardOnFailure(HLCTimestamp ticket)
    {
        Entry? entry;

        lock (gate)
        {
            if (!holds.Remove(ticket, out entry))
                return;
        }

        entry.Timer?.Dispose();

        if (entry.Reply.TryClaim())
            logger.LogWarning("[{LocalEndpoint}/{PartitionId}] Held reply for proposal {Ticket} discarded: the proposal failed while held, so the failure answers the caller",
                localEndpoint, partitionId, ticket);
    }

    /// <summary>
    /// Releases every hold and detaches the registration, restoring ordinary behaviour. Idempotent,
    /// and safe from any thread. Also invoked when the partition stops, so a test that exits without
    /// disposing cannot leave a node unable to answer.
    /// </summary>
    public void Dispose()
    {
        if (Interlocked.Exchange(ref disposed, 1) != 0)
            return;

        detach(this);
        ReleaseAll("the registration was disposed");
    }

    /// <summary>
    /// Releases every hold without detaching. Used when a second registration replaces this one:
    /// the replacement owns the partition from that point, and everything this registration was
    /// holding must be answered rather than stranded.
    /// </summary>
    public void ReplaceWith()
    {
        if (Interlocked.Exchange(ref disposed, 1) != 0)
            return;

        ReleaseAll("the registration was replaced");
    }

    private void ReleaseAll(string reason)
    {
        List<Entry> pending;

        lock (gate)
        {
            if (holds.Count == 0)
                return;

            pending = [.. holds.Values];
            holds.Clear();
        }

        foreach (Entry entry in pending)
        {
            entry.Timer?.Dispose();

            // Lost the claim: whoever won it is completing this waiter through Resolve, which
            // carries the entry and no longer needs the map. Nothing to strand.
            if (!entry.Reply.TryClaim())
                continue;

            logger.LogWarning("[{LocalEndpoint}/{PartitionId}] Releasing held reply for proposal {Ticket} because {Reason}",
                localEndpoint, partitionId, entry.Reply.TicketId, reason);

            entry.Waiter.TrySetResult((RaftProposalTicketState.Committed, entry.CommitIndex));
        }
    }

    /// <summary>
    /// Resolves one claimed hold. <paramref name="release"/> completes the waiter with the committed
    /// outcome; otherwise the waiter is abandoned and the caller waits out its own timeout — the
    /// killed-leader case, which must look to the client exactly like a real proposal timeout.
    /// <para>Takes the entry rather than a ticket so it never depends on the map still holding it:
    /// a concurrent dispose may already have cleared it, and looking the ticket up would then leave
    /// the caller waiting forever.</para>
    /// </summary>
    private void Resolve(Entry entry, bool release)
    {
        lock (gate)
            holds.Remove(entry.Reply.TicketId);

        entry.Timer?.Dispose();

        if (release)
            entry.Waiter.TrySetResult((RaftProposalTicketState.Committed, entry.CommitIndex));
        else if (logger.IsEnabled(LogLevel.Information))
            logger.LogInformation("[{LocalEndpoint}/{PartitionId}] Held reply for proposal {Ticket} dropped; the caller will time out",
                localEndpoint, partitionId, entry.Reply.TicketId);
    }

    /// <summary>
    /// Releases a hold that outlived <see cref="RaftConfiguration.ProposalTimeout"/>. Past the
    /// caller's own bound the hold changes nothing and only hides a leak, so it is answered and the
    /// event is logged at warning with the ticket.
    /// </summary>
    private void AutoRelease(Entry entry)
    {
        if (entry.Reply.IsResolved)
            return;

        logger.LogWarning("[{LocalEndpoint}/{PartitionId}] Held reply for proposal {Ticket} exceeded the {Bound}ms hold bound; releasing it. The test that installed the hook did not resolve it.",
            localEndpoint, partitionId, entry.Reply.TicketId, holdBound.TotalMilliseconds);

        entry.Reply.Release();
    }

    /// <summary>
    /// Runs the registration's callback off the completing turn. An exception from it is logged and
    /// treated as <em>release</em>: a throwing callback must never become a silent hold.
    /// </summary>
    private void InvokeOnHeld(HeldProposalReply reply)
    {
        try
        {
            onHeld(reply);
        }
        catch (Exception ex)
        {
            logger.LogError("[{LocalEndpoint}/{PartitionId}] Hold callback threw for proposal {Ticket}: {Message}. Releasing the reply.",
                localEndpoint, partitionId, reply.TicketId, ex.Message);

            reply.Release();
        }
    }
}
