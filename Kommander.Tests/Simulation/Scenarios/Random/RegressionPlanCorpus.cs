using System.Globalization;

namespace Kommander.Tests.Simulation.Scenarios.Random;

/// <summary>
/// The promoted failures: plans that a generated run once failed on, kept as tests.
///
/// <para><b>How a failure is promoted.</b> A failing run writes its plan under
/// <c>dst-artifacts/</c>. Copy that file — or the shrunk one beside it, which is shorter and says
/// the same thing — into <c>Kommander.Tests/Simulation/Scenarios/Random/regressions/</c>. Nothing
/// else. The file is data, it is copied to the output directory by the project file, and it is
/// replayed from then on.</para>
///
/// <para><b>Why this is worth a folder of its own.</b> A defect the random search found once will be
/// found again only if the same seed comes up under the same bounds, and the corpus of seeds moves.
/// A promoted plan is the part that does not move: it fails until the defect is fixed, and it keeps
/// running afterwards, which is what makes it a regression rather than a note.</para>
///
/// <para>An empty folder is a valid state and is reported as such. It means nothing has been
/// promoted yet, not that the loader is broken — a distinction a caller has to be able to make,
/// because the two look identical from a count of zero.</para>
/// </summary>
public static class RegressionPlanCorpus
{
    /// <summary>The folder beside the test binary that holds promoted plans.</summary>
    public const string FolderName = "regressions";

    /// <summary>Loads every promoted plan beside the test binary.</summary>
    public static IReadOnlyList<RegressionPlan> Load() =>
        Load(Path.Combine(AppContext.BaseDirectory, FolderName));

    /// <summary>
    /// Loads every promoted plan in one folder.
    ///
    /// <para>A file that cannot be read throws rather than being skipped. A regression corpus that
    /// quietly dropped an unreadable entry would report a green run over a plan nobody checked, and
    /// the promoted plan is exactly the one somebody meant to keep.</para>
    /// </summary>
    public static IReadOnlyList<RegressionPlan> Load(string directory)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(directory);

        if (!Directory.Exists(directory))
            return [];

        List<RegressionPlan> plans = [];

        foreach (string path in Directory.EnumerateFiles(directory, "*.plan.txt").Order(StringComparer.Ordinal))
            plans.Add(Read(path));

        return plans;
    }

    /// <summary>Reads one promoted plan.</summary>
    public static RegressionPlan Read(string path)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);

        string[] lines = File.ReadAllLines(path);

        IReadOnlyDictionary<string, string> header = RandomScenarioPlan.ParseHeader(lines);
        IReadOnlyList<RandomScenarioAction> actions = RandomScenarioPlan.Parse(lines);

        if (actions.Count == 0)
            throw new FormatException($"Promoted plan '{path}' holds no actions.");

        if (!header.TryGetValue("seed", out string? seedText)
            || !ulong.TryParse(seedText, NumberStyles.None, CultureInfo.InvariantCulture, out ulong seed))
        {
            throw new FormatException($"Promoted plan '{path}' does not name a seed.");
        }

        return new RegressionPlan(
            Path.GetFileName(path),
            seed,
            RandomScenarioOptions.FromParameters(header),
            actions,
            header);
    }
}
