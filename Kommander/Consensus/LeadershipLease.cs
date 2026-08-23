namespace Kommander.Consensus;

/// <summary>
/// Immutable snapshot of one completed leadership confirmation, published for off-thread readers.
///
/// <para><b>Why it exists.</b> <c>ReadIndexCoordinator</c> already reuses a confirmation for one
/// heartbeat interval, but only the partition executor thread could reach that fast path, so every
/// caller still paid a full executor round trip (reply source, correlation entry, response object,
/// async boxes) per confirmation. This snapshot moves the *reach* of that window off-thread: a
/// caller that finds a fresh lease confirms with a synchronous field read and allocates nothing.</para>
///
/// <para><b>Safety.</b> The window argument is unchanged from the executor-side fast path (see
/// <c>ReadIndexCoordinator</c>): the confirmation is exactly as fresh as the quorum acks it
/// counted, and pre-vote leader stickiness keeps any rival from assembling an election quorum
/// within one heartbeat interval, which sits well inside the election timeout. The object is
/// immutable and published through a volatile reference, so a reader never observes a torn pair;
/// staleness is bounded by the freshness check, and every demotion/term-change path nulls the
/// reference (see <see cref="RaftPartitionCoreState"/>).</para>
/// </summary>
/// <param name="Term">Term the confirmation completed in (diagnostic; invalidation is structural).</param>
/// <param name="ConfirmedTicks">Monotonic timestamp of the quorum confirmation; freshness anchor.</param>
internal sealed record LeadershipLease(long Term, long ConfirmedTicks);
