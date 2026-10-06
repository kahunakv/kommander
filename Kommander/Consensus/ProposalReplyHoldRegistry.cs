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
/// strand a caller mid-resolution. The lock guards the map and each entry's
/// <see cref="Entry.Outcome"/> / <see cref="Entry.AwaitsCommitCompletion"/>.</para>
///
/// <para><b>One ticket, one hold.</b> A ticket can complete successfully more than once: the
/// single-fsync fast path releases it on quorum-durable and the commit completion fires the same
/// waiter again. The second completion can land at any point relative to the test resolving the
/// first — before it, after it, or between the resolution leaving the map and the waiter being
/// completed — so "is the ticket in the map" and "is the waiter completed" cannot between them
/// recognise it. A hold whose commit completion is still owed therefore stays in the map after it
/// is resolved, recording how, until that completion arrives and consumes it: a released ticket is
/// then completed normally (a no-op), a dropped one stays unanswered, and neither is offered to the
/// callback a second time.</para>
/// </summary>
internal sealed class ProposalReplyHoldRegistry : IDisposable
{
    /// <summary>How a hold ended, for the success completion of the same ticket that follows it.</summary>
    private enum HoldOutcome
    {
        /// <summary>Not resolved yet: the caller is still waiting.</summary>
        Held,

        /// <summary>The caller was answered with the committed outcome.</summary>
        Released,

        /// <summary>The caller was abandoned and must stay unanswered.</summary>
        Dropped
    }

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

        /// <summary>How this hold ended. Read and written under the registry lock.</summary>
        public HoldOutcome Outcome;

        /// <summary>
        /// Whether the ticket's commit completion — the last success completion a ticket receives —
        /// has yet to arrive. True for a hold taken at an earlier site; while it is true the entry
        /// stays in the map even after it is resolved. Read and written under the registry lock.
        /// </summary>
        public bool AwaitsCommitCompletion;
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
    /// pooled instance was drained). A repeat success completion for a ticket this registration
    /// already took never produces a second hold: the single-fsync fast path and the commit
    /// completion both fire for one auto-commit proposal, and that must reach the callback once.
    /// The repeat is swallowed (<see langword="true"/>) while the reply is still held or after it
    /// was dropped, and completes normally (<see langword="false"/>) after it was released.</para>
    ///
    /// <para>Called on the partition executor thread. <c>onHeld</c> is queued to the thread pool
    /// rather than invoked here, so a callback that wants to act while the reply is held does not
    /// run on the turn it is holding up.</para>
    /// </summary>
    public bool TryHoldSuccess(RaftProposalQuorum proposal, long commitIndex, ProposalReplySite site)
    {
        if (Volatile.Read(ref disposed) != 0)
            return false;

        HLCTimestamp ticket = proposal.StartTimestamp;

        // The commit completion is the last success completion a ticket receives; a hold taken at
        // an earlier site is owed one more.
        bool isCommitCompletion = site == ProposalReplySite.CommitCompletion;

        Entry entry;

        lock (gate)
        {
            if (disposed != 0)
                return false;

            // Looked up before the waiter is inspected: a ticket this registration already took is
            // answered from what the test decided for it, whatever state its waiter is in by now.
            if (holds.TryGetValue(ticket, out Entry? known))
            {
                if (known.Outcome == HoldOutcome.Held)
                {
                    if (isCommitCompletion)
                        known.AwaitsCommitCompletion = false;

                    return true;
                }

                if (isCommitCompletion)
                    holds.Remove(ticket);

                return known.Outcome == HoldOutcome.Dropped;
            }

            TaskCompletionSource<(RaftProposalTicketState, long)>? waiter = proposal.WaiterSource;
            if (waiter is null || waiter.Task.IsCompleted)
                return false;

            entry = new() { Waiter = waiter, CommitIndex = commitIndex, AwaitsCommitCompletion = !isCommitCompletion };

            long term = proposal.Logs.Count > 0 ? proposal.Logs[0].Term : -1;
            long[] logIds = new long[proposal.Logs.Count];
            for (int i = 0; i < logIds.Length; i++)
                logIds[i] = proposal.Logs[i].Id;

            Entry held = entry;
            entry.Reply = new HeldProposalReply(
                partitionId, ticket, commitIndex, term, logIds, site,
                release => Resolve(held, release));

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

            // Lost the claim: the hold was already resolved and only awaited its commit completion,
            // or whoever won it is completing this waiter through Resolve, which carries the entry
            // and no longer needs the map. Nothing to strand.
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
    /// <para>A hold whose commit completion is still owed is not removed here: it stays in the map
    /// carrying its outcome, so that completion is recognised as a repeat of a resolved ticket
    /// rather than held afresh. It leaves the map when the completion arrives, when the proposal
    /// fails, or with the registration.</para>
    /// </summary>
    private void Resolve(Entry entry, bool release)
    {
        lock (gate)
        {
            if (entry.AwaitsCommitCompletion)
                entry.Outcome = release ? HoldOutcome.Released : HoldOutcome.Dropped;
            else
                holds.Remove(entry.Reply.TicketId);
        }

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
