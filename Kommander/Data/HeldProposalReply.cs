using System.ComponentModel;
using Kommander.Time;

namespace Kommander.Data;

/// <summary>
/// One committed proposal whose caller is being held by
/// <c>IRaft.HoldCommittedProposalRepliesForTesting</c>: the entry is durable on a quorum and
/// visible to every other replica, and the coordinator that proposed it has learned nothing.
///
/// <para><b>Only the reply is held.</b> The commit-marker fan-out to peers, the leader's local
/// applies, the commit-frontier advance and follower delivery all ran already. Holding the
/// executor turn instead would model a stalled leader, which is a different fault.</para>
///
/// <para><b>Thread safety.</b> <see cref="Release"/> and <see cref="Drop"/> are safe from any
/// thread and are both idempotent — the first of the two to run decides the outcome and every
/// later call is a no-op. The partition also resolves the hold on its own when the registration is
/// disposed, when the partition stops, or when the hold outlives
/// <see cref="RaftConfiguration.ProposalTimeout"/>.</para>
/// </summary>
[EditorBrowsable(EditorBrowsableState.Never)]
public sealed class HeldProposalReply
{
    /// <summary>
    /// Resolves the underlying waiter. <c>true</c> completes it with the committed outcome;
    /// <c>false</c> abandons it so the caller waits out its own timeout. Invoked at most once —
    /// <see cref="resolved"/> is the gate.
    /// </summary>
    private readonly Action<bool> resolve;

    private int resolved;

    internal HeldProposalReply(
        int partitionId,
        HLCTimestamp ticketId,
        long commitIndex,
        long term,
        IReadOnlyList<long> logIds,
        ProposalReplySite site,
        Action<bool> resolve)
    {
        PartitionId = partitionId;
        TicketId = ticketId;
        CommitIndex = commitIndex;
        Term = term;
        LogIds = logIds;
        Site = site;
        this.resolve = resolve;
    }

    /// <summary>Partition the held proposal belongs to.</summary>
    public int PartitionId { get; }

    /// <summary>Ticket the proposer holds for this proposal.</summary>
    public HLCTimestamp TicketId { get; }

    /// <summary>Commit index the caller would have been answered with.</summary>
    public long CommitIndex { get; }

    /// <summary>Term the proposal's first entry carries, or <c>-1</c> when the batch is empty.</summary>
    public long Term { get; }

    /// <summary>Log ids carried by the proposal — enough to recognise the entry, without copying payloads.</summary>
    public IReadOnlyList<long> LogIds { get; }

    /// <summary>
    /// Which of the three success sites produced this completion. The site that fired first: an
    /// auto-commit proposal on the single-fsync fast path completes twice, and the second
    /// completion is swallowed rather than held again.
    /// </summary>
    public ProposalReplySite Site { get; }

    /// <summary>
    /// Whether this hold has already been resolved, by either <see cref="Release"/>,
    /// <see cref="Drop"/>, or an automatic release.
    /// </summary>
    public bool IsResolved => Volatile.Read(ref resolved) != 0;

    /// <summary>Completes the waiter now. Idempotent; wins over a later <see cref="Drop"/>.</summary>
    public void Release()
    {
        if (TryClaim())
            resolve(true);
    }

    /// <summary>
    /// Abandons the reply: the caller waits out its own timeout and sees exactly what a real
    /// proposal timeout produces. Idempotent. This is the case that models a killed leader without
    /// killing the process.
    /// </summary>
    public void Drop()
    {
        if (TryClaim())
            resolve(false);
    }

    /// <summary>
    /// Wins the right to resolve this hold, exactly once. Returns <see langword="false"/> when
    /// some other path — the other of <see cref="Release"/>/<see cref="Drop"/>, the hold bound, a
    /// dispose, or a failure of the same proposal — already claimed it. The partition uses this to
    /// retire a hold without completing the waiter itself.
    /// </summary>
    internal bool TryClaim() => Interlocked.Exchange(ref resolved, 1) == 0;
}
