namespace Kommander.Tests.WAL;

/// <summary>
/// Serializes tests that toggle the process-global <see cref="Kommander.Diagnostics.WalPhaseInstrumentation"/>
/// switch or read its accumulators. The instrumentation state is shared across the
/// whole process, so concurrent measurement windows would pollute one another.
/// </summary>
[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class WalInstrumentationCollection
{
    public const string Name = "WAL instrumentation";
}
