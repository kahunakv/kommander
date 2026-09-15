namespace Kommander.Tests.Simulation;

/// <summary>
/// One snapshot install accepted by a node whose own log could still have been repaired by
/// backfill — the sender still retained every entry the node lacked.
/// </summary>
/// <param name="StepNumber">Simulation step at which the import began.</param>
/// <param name="Endpoint">The node that imported.</param>
/// <param name="PartitionId">The partition.</param>
/// <param name="HeldThrough">
/// Highest id such that the node's log is present and resolved at every id from its own covered
/// prefix up to it: the position ordinary backfill would have started above.
/// </param>
/// <param name="FirstRetainedElsewhere">
/// The highest first-retained id among the other running nodes' stores — the most compacted log
/// any possible sender had. A node holding through one below it needs nothing a log cannot serve.
/// </param>
/// <param name="RetainedBy">The node whose store set <see cref="FirstRetainedElsewhere"/>.</param>
public sealed record UnnecessarySnapshotImport(
    int StepNumber,
    string Endpoint,
    int PartitionId,
    long HeldThrough,
    long FirstRetainedElsewhere,
    string RetainedBy);
