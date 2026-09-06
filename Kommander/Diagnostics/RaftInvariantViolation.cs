namespace Kommander.Diagnostics;

/// <summary>
/// One report of a broken consensus invariant, as handed to <see cref="RaftInvariants.Violated"/>.
///
/// <para><b>Why a value type and not the exception.</b> A violation is reported under every
/// <see cref="RaftInvariantPolicy"/> except <see cref="RaftInvariantPolicy.Off"/>, and under
/// <see cref="RaftInvariantPolicy.Log"/> no exception exists. A harness that wants to fail a run on
/// the first violation therefore needs the report itself, not the object one policy happens to
/// throw. The fields are the same ones the log line and the metric tag carry.</para>
/// </summary>
/// <param name="Invariant">One of the name constants on <see cref="RaftInvariants"/>.</param>
/// <param name="PartitionId">Partition whose state broke the rule.</param>
/// <param name="LocalEndpoint">Node whose state broke the rule, when known.</param>
/// <param name="Detail">The values involved, already formatted.</param>
public readonly record struct RaftInvariantViolation(
    string Invariant,
    int PartitionId,
    string? LocalEndpoint,
    string Detail);
