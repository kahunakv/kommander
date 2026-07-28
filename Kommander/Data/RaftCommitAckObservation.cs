
namespace Kommander.Data;

/// <summary>
/// An immutable observation, emitted when a proposal reaches commit quorum, recording that a specific node
/// acknowledged a committed entry. Carries the acknowledger's voter role and the voter count at commit time so
/// an out-of-band checker (e.g. the chaos harness's quorum-discipline invariant) can verify that every commit
/// was acknowledged by a voter majority and that no learner was counted toward quorum.
///
/// <para>Emission is gated by <c>IRaftPartitionHost.CommitAckObservationEnabled</c>, which is false in
/// production, so this adds no overhead unless a test has attached a subscriber.</para>
/// </summary>
public readonly record struct RaftCommitAckObservation(
    int Partition,
    long Index,
    long Term,
    string Acker,
    bool AckerIsVoter,
    int VotersTotal);
