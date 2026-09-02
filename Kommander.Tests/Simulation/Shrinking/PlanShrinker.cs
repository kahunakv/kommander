using Kommander.Tests.Simulation.Scenarios.Random;

namespace Kommander.Tests.Simulation.Shrinking;

/// <summary>
/// Cuts a failing plan down to the actions the failure needs.
///
/// <para><b>What this is for.</b> A generated plan that fails is forty actions long and most of
/// them are irrelevant. The reader's real question is which three mattered, and answering it by
/// hand costs an afternoon of re-runs. This does the same re-runs and reports the answer.</para>
///
/// <para><b>The algorithm.</b> Contiguous chunk removal at a halving width, then numeric parameter
/// reduction. Wide chunks first, because a plan is mostly padding and one accepted wide cut is
/// worth many narrow ones. Every candidate is normalised before it runs — see
/// <see cref="PlanNormalizer"/> — so a cut never hands the cluster a repair with no damage.</para>
///
/// <para><b>The rule that makes the result mean something.</b> A candidate is accepted only when it
/// fails for the <em>same</em> reason. Any other failure is rejected exactly like a pass. Without
/// that rule a shrink slides from the defect it was called on to whichever one is easiest to reach,
/// and hands back a minimal reproduction of the wrong thing.</para>
///
/// <para><b>The rule the cluster forces on top.</b> These plans are not deterministic — the nodes
/// own their own threads, so one plan can fail on one run and pass on the next. A single passing
/// attempt is therefore not evidence that the removed action was needed, which is why
/// <see cref="ShrinkOptions.AttemptsPerCandidate"/> exists and why a candidate is accepted when
/// <em>any</em> attempt reproduces. Set it to one and the shrinker will keep actions that do
/// nothing; that is a weaker result, not a wrong one.</para>
/// </summary>
public sealed class PlanShrinker
{
    private readonly PlanOracle oracle;
    private readonly ShrinkOptions options;

    private int candidatesRun;
    private int removalsAccepted;
    private int parametersReduced;

    public PlanShrinker(PlanOracle oracle, ShrinkOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(oracle);

        this.oracle = oracle;
        this.options = options ?? new ShrinkOptions();
    }

    /// <summary>
    /// Shrinks a plan that is already known to fail.
    ///
    /// <para>The signature is taken from the caller rather than measured here. The caller has just
    /// watched the run fail and holds the exception; re-running the full plan only to learn what it
    /// already knows would spend a cluster run on nothing.</para>
    /// </summary>
    /// <param name="plan">The failing plan.</param>
    /// <param name="signature">The failure to hold on to, from <see cref="FailureSignature.Of"/>.</param>
    public async Task<ShrinkResult> ShrinkAsync(
        IReadOnlyList<RandomScenarioAction> plan,
        string signature,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(plan);
        ArgumentException.ThrowIfNullOrEmpty(signature);

        if (signature == FailureSignature.None)
            throw new ArgumentException("A passing run has nothing to shrink.", nameof(signature));

        candidatesRun = 0;
        removalsAccepted = 0;
        parametersReduced = 0;

        IReadOnlyList<RandomScenarioAction> best = PlanNormalizer.Normalize(plan);

        best = await RemoveChunksAsync(best, signature, cancellationToken).ConfigureAwait(false);

        if (options.ReduceParameters)
            best = await ReduceParametersAsync(best, signature, cancellationToken).ConfigureAwait(false);

        return new ShrinkResult
        {
            Original = plan,
            Shrunk = best,
            Signature = signature,
            CandidatesRun = candidatesRun,
            RemovalsAccepted = removalsAccepted,
            ParametersReduced = parametersReduced,
            BudgetExhausted = candidatesRun >= options.MaxCandidates,
        };
    }

    // ── Passes ────────────────────────────────────────────────────────────

    /// <summary>
    /// Removes contiguous chunks, widest first, and halves the width when a whole pass finds
    /// nothing.
    ///
    /// <para>Contiguous rather than arbitrary subsets on purpose. The actions in these plans are
    /// ordered in time and their effects overlap, so the interesting groups are neighbours: a fault,
    /// the writes under it, and its repair. Searching arbitrary subsets would cost combinatorially
    /// more for groups the plan does not contain.</para>
    ///
    /// <para>On an accepted cut the window is not advanced. The plan under it is now different, so
    /// the same position is a different candidate and deserves another try.</para>
    /// </summary>
    private async Task<IReadOnlyList<RandomScenarioAction>> RemoveChunksAsync(
        IReadOnlyList<RandomScenarioAction> plan,
        string signature,
        CancellationToken cancellationToken)
    {
        IReadOnlyList<RandomScenarioAction> best = plan;

        for (int width = Math.Max(1, best.Count / 2); width >= 1;)
        {
            bool progressed = false;
            int start = 0;

            while (start < best.Count)
            {
                if (candidatesRun >= options.MaxCandidates)
                    return best;

                if (best.Count - width < options.MinimumPlanLength)
                    break;

                IReadOnlyList<RandomScenarioAction> candidate =
                    PlanNormalizer.Normalize(Without(best, start, width));

                if (candidate.Count >= best.Count)
                {
                    start += width;
                    continue;
                }

                if (await ReproducesAsync(candidate, signature, cancellationToken).ConfigureAwait(false))
                {
                    best = candidate;
                    removalsAccepted++;
                    progressed = true;
                    continue;
                }

                start += width;
            }

            if (!progressed)
                width /= 2;
        }

        return best;
    }

    /// <summary>
    /// Tries a smaller number on every action that carries one.
    ///
    /// <para>The smallest meaningful value first, then the midpoint if that fails. Two probes per
    /// action, and no more: this pass runs after the removals and shares what is left of the
    /// budget, so a bisection to the exact threshold would spend on precision nobody reads. The
    /// point is to turn "duplicated forty times" into "duplicated twice", not to find the smallest
    /// count that still breaks it.</para>
    /// </summary>
    private async Task<IReadOnlyList<RandomScenarioAction>> ReduceParametersAsync(
        IReadOnlyList<RandomScenarioAction> plan,
        string signature,
        CancellationToken cancellationToken)
    {
        IReadOnlyList<RandomScenarioAction> best = plan;

        for (int index = 0; index < best.Count; index++)
        {
            if (MinimumValueOf(best[index]) is not { } minimum)
                continue;

            long current = best[index].Value;

            if (current <= minimum)
                continue;

            foreach (long attempt in new[] { minimum, minimum + (current - minimum) / 2 })
            {
                if (candidatesRun >= options.MaxCandidates)
                    return best;

                if (attempt >= current)
                    continue;

                IReadOnlyList<RandomScenarioAction> candidate = Replace(
                    best, index, best[index] with { Value = attempt });

                if (!await ReproducesAsync(candidate, signature, cancellationToken).ConfigureAwait(false))
                    continue;

                best = candidate;
                parametersReduced++;
                break;
            }
        }

        return best;
    }

    // ── Helpers ───────────────────────────────────────────────────────────

    /// <summary>
    /// Runs a candidate up to <see cref="ShrinkOptions.AttemptsPerCandidate"/> times, and reports
    /// whether any attempt reproduced the target failure.
    ///
    /// <para>Any attempt, not every attempt. The failures worth shrinking are the rare ones, and a
    /// rule that needed every attempt to reproduce would reject the true minimal plan for exactly
    /// the reason it is interesting.</para>
    /// </summary>
    private async Task<bool> ReproducesAsync(
        IReadOnlyList<RandomScenarioAction> candidate,
        string signature,
        CancellationToken cancellationToken)
    {
        for (int attempt = 0; attempt < Math.Max(1, options.AttemptsPerCandidate); attempt++)
        {
            if (candidatesRun >= options.MaxCandidates)
                return false;

            candidatesRun++;

            ShrinkAttempt result = await oracle(candidate, cancellationToken).ConfigureAwait(false);

            if (result.Reproduces(signature))
                return true;
        }

        return false;
    }

    /// <summary>
    /// The smallest value at which an action still does something, or null when its number is not
    /// a dose. A duplicate count of one is ordinary delivery, so two is the floor.
    /// </summary>
    private static long? MinimumValueOf(RandomScenarioAction action) => action.Kind switch
    {
        RandomScenarioActionKind.DuplicateLink => 2,
        RandomScenarioActionKind.FailWrites => 1,
        RandomScenarioActionKind.SlowDisk => 1,
        _ => null,
    };

    private static List<RandomScenarioAction> Without(
        IReadOnlyList<RandomScenarioAction> plan,
        int start,
        int width)
    {
        List<RandomScenarioAction> kept = new(plan.Count);

        for (int index = 0; index < plan.Count; index++)
        {
            if (index < start || index >= start + width)
                kept.Add(plan[index]);
        }

        return kept;
    }

    private static List<RandomScenarioAction> Replace(
        IReadOnlyList<RandomScenarioAction> plan,
        int index,
        RandomScenarioAction action)
    {
        List<RandomScenarioAction> copy = [.. plan];
        copy[index] = action;

        return copy;
    }
}
