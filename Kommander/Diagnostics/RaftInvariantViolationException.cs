namespace Kommander.Diagnostics;

/// <summary>
/// Thrown when a consensus invariant is violated and
/// <see cref="RaftInvariants.Policy"/> is <see cref="RaftInvariantPolicy.Throw"/>.
///
/// <para>This is never an expected control-flow exception. It reports a state transition that the
/// protocol says cannot happen, so a catch block that swallows it hides the defect it exists to
/// expose. Let it reach the test runner or the executor's fault handler.</para>
/// </summary>
public sealed class RaftInvariantViolationException : RaftException
{
    /// <summary>The stable name of the violated invariant, for example <c>term_monotonic</c>.
    /// Matches the <c>invariant</c> tag on the <c>raft.invariant.violations_total</c> counter.</summary>
    public string Invariant { get; }

    /// <summary>Creates a violation report for <paramref name="invariant"/>.</summary>
    public RaftInvariantViolationException(string invariant, string message) : base(message) =>
        Invariant = invariant;
}
