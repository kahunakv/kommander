
namespace Kommander.Data;

/// <summary>
/// What the leader of a partition last heard from one follower about the follower's own disk: its
/// durable contiguous commit frontier and the age of its oldest unanswered WAL write, as carried in
/// every term-valid <c>CompleteAppendLogs</c> acknowledgement (<see cref="CompleteAppendLogsRequest.DurableIndex"/>
/// and <see cref="CompleteAppendLogsRequest.WalStallMs"/>).
///
/// <para><b>Why this is exposed.</b> An application-level protocol that waits on a follower — for a
/// verdict, an attestation, an apply — needs to know whether the follower can possibly answer before
/// it pays the wait. Probe latency is the wrong evidence: a follower that is tens of thousands of
/// entries behind the leader's commit index answers a probe instantly and truthfully from state that
/// is tens of thousands of entries old, and a follower whose disk is paused answers from memory
/// until the entry it is asked about is the one it cannot write. The leader already holds the facts
/// that settle both questions and refreshes them on every acknowledgement; this record hands them
/// out. Snapshots are published from the partition executor thread into a lock-free map and read
/// without any scheduler round-trip, so consulting one per operation costs a dictionary lookup.</para>
///
/// <para><b>Semantics.</b> A snapshot is a point-in-time fact about the follower as of its last
/// acknowledgement, not a promise about its present: a follower that stopped acknowledging keeps its
/// last snapshot until the leader steps down or the peer is removed. Only a leader records
/// followers, so <see cref="IRaft.GetFollowerProgress"/> answers <see langword="null"/> on any node that
/// is not the partition's leader and for any peer the current leadership has not heard from yet.
/// Callers must treat <see langword="null"/> as "unknown", never as "caught up".</para>
/// </summary>
/// <param name="Endpoint">The follower.</param>
/// <param name="Term">The leader's term when the acknowledgement was folded. A snapshot never
/// outlives its leadership — step-down clears the map — so this is informational.</param>
/// <param name="DurableFrontier">The follower's self-reported durable contiguous commit frontier: the
/// highest log id both resolved and durable on its disk, or -1 when it has not reported one (an
/// older peer, or the legacy heartbeat ack path). This is the value WAL retention is held on and
/// the only frontier that describes what the follower can have applied.</param>
/// <param name="ProtocolFrontier">The follower's gap-aware commit frontier as last recorded from a
/// <see cref="RaftOperationStatus.Success"/> acknowledgement, or -1 when none has been recorded. It
/// advances when an append is merely queued for the follower's disk, so during a durable-write
/// stall it keeps rising while <see cref="DurableFrontier"/> stands still; the difference between the
/// two is how much the follower has accepted but not yet written.</param>
/// <param name="LeaderCommitIndex">The leader's own committed index when the acknowledgement was
/// folded, so a reader can size the follower's lag as of the same instant without a second read.</param>
/// <param name="WalStallMs">How long the follower's oldest pending WAL write had gone unanswered by its
/// storage engine when it acknowledged; 0 when its disk is keeping up.</param>
/// <param name="WalStalled">Whether the leader currently holds the follower in a durable-write stall
/// episode: <see cref="WalStallMs"/> reached <see cref="RaftConfiguration.WalStallWarnThreshold"/> and no
/// later acknowledgement has reported it back below. While this is set the leader is already
/// deferring entry-carrying backfill and snapshot transfers to the peer.</param>
/// <param name="ReportedAtTicks">Monotonic tick stamp (<see cref="RaftConfiguration.TickSource"/>) of the
/// acknowledgement this snapshot was taken from, so a reader can tell a live report from the last
/// word of a peer that has gone silent.</param>
public sealed record RaftFollowerProgress(
    string Endpoint,
    long Term,
    long DurableFrontier,
    long ProtocolFrontier,
    long LeaderCommitIndex,
    long WalStallMs,
    bool WalStalled,
    long ReportedAtTicks)
{
    /// <summary>
    /// How many committed entries the follower's durable frontier trails <paramref name="leaderCommitIndex"/>
    /// by (typically the leader's current <see cref="IRaft.GetCommitIndex"/>), floored at zero. An
    /// unreported frontier (-1) counts as the whole log.
    /// </summary>
    public long EntriesBehind(long leaderCommitIndex) => Math.Max(0, leaderCommitIndex - Math.Max(DurableFrontier, 0));

    public override string ToString() =>
        $"{Endpoint} term={Term} durable={DurableFrontier} protocol={ProtocolFrontier} leaderCommit={LeaderCommitIndex}" +
        (WalStalled ? $" stalled={WalStallMs}ms" : WalStallMs > 0 ? $" pendingWrite={WalStallMs}ms" : string.Empty);
}
