using System.ComponentModel;

namespace Kommander.Data;

/// <summary>
/// Which of the three success paths released a committed proposal's caller.
///
/// <para>A proposal has one caller but more than one completion site: the single-fsync fast path,
/// the manual two-phase propose quorum, and the ordinary commit completion. The fast path and the
/// commit completion both fire for the same auto-commit proposal, so a hold reports the site that
/// fired <b>first</b>. Failure completions (rollback, leader loss, pool drain) have no site — they
/// are never held.</para>
///
/// <para>Diagnostic only. Never use this value to decide protocol behaviour.</para>
/// </summary>
[EditorBrowsable(EditorBrowsableState.Never)]
public enum ProposalReplySite
{
    /// <summary>
    /// <c>WalSingleFsyncCommit</c> released the ticket on propose-quorum-durable, before the
    /// commit fsync was enqueued.
    /// </summary>
    QuorumDurableFastPath,

    /// <summary>
    /// A manual two-phase proposal (<c>autoCommit: false</c>) completed its propose phase on
    /// quorum-durable. The explicit commit answers through the reply-correlation path, not the
    /// waiter, so it is not a hold site.
    /// </summary>
    ManualProposeQuorum,

    /// <summary>The ordinary leader commit completion released the caller.</summary>
    CommitCompletion,
}
