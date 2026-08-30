namespace Kommander.Diagnostics;

/// <summary>
/// What <see cref="RaftInvariants"/> does when a consensus invariant is violated.
///
/// <para><b>Why this is a policy and not a compile-time switch.</b> A violation found by an
/// external harness (Jepsen, a Caraxes soak) is found in a release build. A check that compiles
/// away in release therefore never sees the schedules that actually break the system, which is the
/// exact gap the fragility analysis names: the suite proves the sampled past still works, and the
/// unsampled future is only explored outside the build. So the checks stay compiled in every
/// build, and only the reaction to a violation changes.</para>
/// </summary>
public enum RaftInvariantPolicy
{
    /// <summary>
    /// Do nothing. Reserved for a benchmark run that must not pay even the branch. Not recommended
    /// in production: the checks cost one comparison on a path that already does disk work.
    /// </summary>
    Off = 0,

    /// <summary>
    /// Record the violation on the <c>raft.invariant.violations_total</c> counter and log it at
    /// Error level, then let the caller continue. The default for a release build: a long soak
    /// keeps running and the operator gets a timestamped, named first-divergence point instead of
    /// a checker verdict hours later.
    /// </summary>
    Log = 1,

    /// <summary>
    /// Record and log as <see cref="Log"/> does, then throw
    /// <see cref="RaftInvariantViolationException"/>. The default for a debug build, so the test
    /// suite fails at the transition that broke the rule rather than at the symptom.
    /// </summary>
    Throw = 2
}
