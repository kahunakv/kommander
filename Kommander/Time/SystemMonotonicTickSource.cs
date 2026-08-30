namespace Kommander.Time;

/// <summary>
/// The production <see cref="IMonotonicTickSource"/>. Reads the process monotonic clock.
///
/// <para>This type is the only approved place in the library where the consensus path reads
/// <c>Stopwatch.GetTimestamp()</c>. Every other consensus-path read goes through
/// <see cref="RaftConfiguration.TickSource"/>, which holds <see cref="Instance"/> by default.
/// Behavior is therefore identical to the direct reads that this seam replaced.</para>
///
/// <para>The type is stateless and immutable, so one shared instance serves the whole process.</para>
/// </summary>
public sealed class SystemMonotonicTickSource : IMonotonicTickSource
{
    /// <summary>Shared process-wide instance. Allocating another one gains nothing.</summary>
    public static readonly SystemMonotonicTickSource Instance = new();

    /// <inheritdoc />
    public long GetTimestamp() => global::System.Diagnostics.Stopwatch.GetTimestamp();

    /// <inheritdoc />
    public long Frequency => global::System.Diagnostics.Stopwatch.Frequency;
}
