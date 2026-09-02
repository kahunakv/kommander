namespace Kommander.Tests.Simulation.Shrinking;

/// <summary>
/// Names the reason a run failed, in a form two runs can be compared on.
///
/// <para><b>Why a shrinker needs one at all.</b> A shrinker removes an action and asks whether the
/// plan still fails. "Still fails" is the whole contract, and without a signature it means "threw
/// something". A plan can easily fail for a second reason once the first is out of reach, and a
/// shrinker that accepted that would hand back a minimal reproduction of a defect nobody was
/// looking at. The signature is what stops the search drifting.</para>
///
/// <para><b>Why the name and not the message.</b> Every message here carries indices, terms and
/// endpoints, and all three move when an action is removed. Comparing messages would reject every
/// true reduction. The invariant name is stable across the reductions and still separates one rule
/// from another, which is exactly the resolution the decision needs.</para>
/// </summary>
public static class FailureSignature
{
    /// <summary>The signature of a run that passed. No plan reduces to this on purpose.</summary>
    public const string None = "none";

    /// <summary>
    /// Reads the signature out of a failure.
    ///
    /// <para>The chain is unwrapped first. A generated run reports its failure wrapped in a message
    /// naming the seed and the plan, so the exception a caller holds is the wrapper, and the rule
    /// that actually fired sits underneath it.</para>
    /// </summary>
    public static string Of(Exception? error)
    {
        for (Exception? current = error; current is not null; current = current.InnerException)
        {
            if (current is InvariantViolationException violation)
                return $"invariant:{violation.InvariantName}";
        }

        return error is null ? None : $"exception:{Innermost(error).GetType().Name}";
    }

    private static Exception Innermost(Exception error)
    {
        Exception current = error;

        while (current.InnerException is not null)
            current = current.InnerException;

        return current;
    }
}
