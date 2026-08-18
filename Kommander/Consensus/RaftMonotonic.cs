using System.Diagnostics;

namespace Kommander.Consensus;

/// <summary>
/// Local elapsed-time helpers shared by the partition state machine and its collaborators.
/// </summary>
internal static class RaftMonotonic
{
    /// <summary>
    /// Local elapsed time since a monotonic anchor. Returns <see cref="TimeSpan.MaxValue"/> when the
    /// anchor is unset (0) so an "unset" anchor never satisfies a "&lt; timeout" freshness guard — matching
    /// the historical <c>lastHeartbeat != HLCTimestamp.Zero &amp;&amp; …</c> shape. Callers still gate on the
    /// explicit <c>anchorTicks != 0</c> where the old code gated on <c>!= Zero</c>, for symmetry.
    /// <para>Every elapsed-time GATE in the consensus path measures against these monotonic ticks rather
    /// than subtracting HLC timestamps: HLC subtraction is frozen by a remote peer's clock skew and
    /// stalls elections.</para>
    /// </summary>
    public static TimeSpan Elapsed(long anchorTicks, long nowTicks) =>
        anchorTicks == 0 ? TimeSpan.MaxValue : Stopwatch.GetElapsedTime(anchorTicks, nowTicks);
}
