namespace Kommander.Data;

/// <summary>
/// The last repair decision a leader made about one peer, and the inputs it made it from.
///
/// <para><b>Why the inputs and not just the outcome.</b> "The leader sent nothing" is true of a
/// healthy cluster and of a wedged one. What separates them is which input was false: a gap of zero
/// says the leader believes the peer is caught up, a live-replication flag still false says the
/// decision has not been reached yet, and backfill switched off says a configuration decided it.
/// Recording the outcome alone leaves a reader inferring the cause from silence, which is what this
/// exists to stop.</para>
///
/// <para><b>Why it is recorded rather than only logged.</b> The same values are written to the log at
/// <c>Debug</c>, under one category shared with every other trace in the build. Raising the level to
/// reach them makes the library format every trace on every operation, and that is enough work to
/// change the timing of the states worth investigating — a simulation run measured a defect at
/// roughly one in ten and then failed to reproduce it in forty runs with that logging on. This
/// record costs one assignment per peer per heartbeat and nothing at all to leave unread.</para>
///
/// <para>Diagnostic only. It is a point-in-time copy of a decision already made, and nothing in the
/// protocol reads it back.</para>
/// </summary>
/// <param name="Sequence">
/// How many decisions this leader has made about this peer, counted from one.
/// <para>The value that separates a wrong decision from an absent one. A leader that decided badly
/// has a high count and stale-looking inputs; a leader that stopped deciding has a count that
/// stopped rising. Without it the two are identical from a single snapshot, and they need opposite
/// investigations.</para>
/// </param>
/// <param name="WillBackfill">Whether the leader decided to ship a repair this round.</param>
/// <param name="FrontierKnown">
/// Whether a committed frontier was known for this peer <em>when the decision was made</em>.
/// <para>Recorded alongside the current value in the view, because the two answer different
/// questions. A record saying the frontier was unknown, beside a view saying it is known now, means
/// no decision has been made since the peer reported — the leader stopped deciding, rather than
/// deciding wrongly.</para>
/// </param>
/// <param name="LocalCommittedIndex">
/// The leader's own committed index at the moment of the decision. The gap is derived from it, so a
/// gap that looks wrong is explained by this being behind rather than by the peer's frontier.
/// </param>
/// <param name="Gap">Committed entries the leader believes this peer is missing.</param>
/// <param name="IdleTailGap">The quiet-cluster trigger: a small gap once writes have paused.</param>
/// <param name="VoterShortPrefix">
/// Whether this peer reported a committed prefix of its own, which exempts a voter from the
/// restored-state confinement.
/// </param>
/// <param name="Regressed">Whether a crash-restart frontier regression was being repaired.</param>
/// <param name="LiveReplicationQuiet">
/// Whether live replication had paused. The idle trigger is gated on this, so a cluster that never
/// looks quiet never repairs a small gap — which is indistinguishable from a leader that decided
/// nothing was wrong, unless this value is visible.
/// </param>
/// <param name="BackfillEnabled">Whether configuration allows any repair at all.</param>
public sealed record RaftPeerBackfillDecision(
    long Sequence,
    bool WillBackfill,
    bool FrontierKnown,
    long LocalCommittedIndex,
    long Gap,
    bool IdleTailGap,
    bool VoterShortPrefix,
    bool Regressed,
    bool LiveReplicationQuiet,
    bool BackfillEnabled)
{
    public override string ToString() =>
        $"seq={Sequence} willBackfill={WillBackfill} frontierKnownThen={FrontierKnown} " +
        $"localCommittedThen={LocalCommittedIndex} gap={Gap} idleTailGap={IdleTailGap} " +
        $"voterShortPrefix={VoterShortPrefix} regressed={Regressed} " +
        $"liveQuiet={LiveReplicationQuiet} enabled={BackfillEnabled}";
}
