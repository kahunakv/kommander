using Kommander.Tests.Simulation.Scenarios.Random;

namespace Kommander.Tests.Simulation.Shrinking;

/// <summary>
/// Makes a reduced plan coherent again.
///
/// <para><b>Why a removal cannot just be a removal.</b> The vocabulary is paired: a fault action
/// and the action that ends it. Cut a chunk out of the middle of a plan and some repairs lose the
/// damage they repair. Most orphans are merely noise — freeing a disk that was never starved
/// changes nothing — but one is worse than noise: <c>RestartNode</c> on a node that never crashed
/// starts a node the plan expected to be running, so the reduced plan no longer describes the run
/// the reader is being handed.</para>
///
/// <para>So every orphaned repair is dropped, not only the harmful one. Uniformity is worth more
/// than precision here: the reader of a shrunk plan should not have to work out which lines are
/// leftovers, and a shorter plan is the entire product.</para>
///
/// <para><b>What is deliberately not repaired.</b> A fault left open at the end of the plan stays
/// open. The runner heals everything before it checks convergence, so an unclosed fault is already
/// handled, and inventing a repair the original run never performed would put an action in the plan
/// that no cluster ever executed.</para>
/// </summary>
public static class PlanNormalizer
{
    /// <summary>
    /// Drops repairs whose fault is gone, then renumbers the survivors.
    ///
    /// <para>Renumbering is cosmetic and matters anyway. The index is what a reader cites when they
    /// talk about a plan, and a shrunk plan numbered 0, 4, 9, 11 invites the reader to look for the
    /// missing ones.</para>
    /// </summary>
    public static IReadOnlyList<RandomScenarioAction> Normalize(
        IReadOnlyList<RandomScenarioAction> plan)
    {
        ArgumentNullException.ThrowIfNull(plan);

        HashSet<string> open = [];
        List<RandomScenarioAction> kept = [];

        foreach (RandomScenarioAction action in plan)
        {
            string? repairs = RepairKeyOf(action);

            if (repairs is not null)
            {
                // The repair is kept only when its damage is still in the plan, and keeping it
                // closes that damage. A second repair of the same fault is an orphan too.
                if (!open.Remove(repairs))
                    continue;

                kept.Add(action);
                continue;
            }

            if (FaultKeyOf(action) is { } fault)
                open.Add(fault);

            kept.Add(action);
        }

        List<RandomScenarioAction> renumbered = new(kept.Count);

        for (int index = 0; index < kept.Count; index++)
            renumbered.Add(kept[index] with { Index = index });

        return renumbered;
    }

    /// <summary>
    /// The fault this action opens, or null when it opens none.
    ///
    /// <para>Keyed by kind family and by target, because two faults of one family on two nodes are
    /// two faults. A link fault is keyed by the ordered pair: the vocabulary blocks one direction
    /// at a time, and the reverse direction is a different fault.</para>
    /// </summary>
    private static string? FaultKeyOf(RandomScenarioAction action) => action.Kind switch
    {
        RandomScenarioActionKind.CrashNode => $"lifecycle/{action.Target}",
        RandomScenarioActionKind.PauseNode => $"pause/{action.Target}",
        RandomScenarioActionKind.BlockLink => $"link/{action.Target}->{action.Secondary}",
        RandomScenarioActionKind.StarveDisk => $"disk/{action.Target}",
        RandomScenarioActionKind.SlowDisk => $"fsync/{action.Target}",
        RandomScenarioActionKind.HoldRetention => $"retention/{action.Target}",

        // A copy count above one is the damage; a count of one is the repair. One kind, two roles,
        // told apart by the parameter rather than by the name.
        RandomScenarioActionKind.DuplicateLink when action.Value > 1 =>
            $"duplicate/{action.Target}->{action.Secondary}",

        _ => null,
    };

    /// <summary>The fault this action closes, or null when it closes none.</summary>
    private static string? RepairKeyOf(RandomScenarioAction action) => action.Kind switch
    {
        RandomScenarioActionKind.RestartNode => $"lifecycle/{action.Target}",
        RandomScenarioActionKind.ResumeNode => $"pause/{action.Target}",
        RandomScenarioActionKind.UnblockLink => $"link/{action.Target}->{action.Secondary}",
        RandomScenarioActionKind.FreeDisk => $"disk/{action.Target}",
        RandomScenarioActionKind.FastDisk => $"fsync/{action.Target}",
        RandomScenarioActionKind.ReleaseRetention => $"retention/{action.Target}",

        RandomScenarioActionKind.DuplicateLink when action.Value <= 1 =>
            $"duplicate/{action.Target}->{action.Secondary}",

        _ => null,
    };
}
