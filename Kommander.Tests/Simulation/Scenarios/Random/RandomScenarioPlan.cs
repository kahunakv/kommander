using System.Globalization;

namespace Kommander.Tests.Simulation.Scenarios.Random;

/// <summary>
/// Reads a plan back from the artifact a failing run wrote.
///
/// <para><b>Why a plan is the replay unit and not the seed.</b> A seed reproduces the draws, and
/// the draws depend on what the generator observed. These runs drive a real cluster whose nodes own
/// their own threads, so two runs of one seed see the same cluster take slightly different paths,
/// and a plan drawn against one reading is not the plan drawn against the other. The plan removes
/// the generator from the loop: the same actions are applied in the same order, whatever the
/// cluster does between them.</para>
///
/// <para><b>What replay still does not pin.</b> The interleaving inside the cluster. Exact
/// step-for-step reproduction needs the driven scheduling mode, where the harness owns every
/// thread; these runs trade that for speed. So a replay reproduces the run's shape, not its every
/// instruction, and a failure that survives replay is worth far more than one that does not.</para>
/// </summary>
public static class RandomScenarioPlan
{
    /// <summary>
    /// Parses the action lines of a plan artifact. Header lines and blank lines are ignored, so the
    /// whole file can be handed over as it was written.
    /// </summary>
    public static IReadOnlyList<RandomScenarioAction> Parse(IEnumerable<string> lines)
    {
        ArgumentNullException.ThrowIfNull(lines);

        List<RandomScenarioAction> plan = [];

        foreach (string line in lines)
        {
            string text = line.Trim();

            if (text.Length == 0 || !char.IsDigit(text[0]))
                continue;

            plan.Add(ParseAction(text));
        }

        return plan;
    }

    /// <summary>Parses a plan artifact from disk.</summary>
    public static IReadOnlyList<RandomScenarioAction> ParseFile(string path) =>
        Parse(File.ReadAllLines(path));

    /// <summary>
    /// Reads the <c>key=value</c> lines above the actions.
    ///
    /// <para>The header holds the seed and the bounds the run was drawn under, which is what makes a
    /// plan replayable rather than merely readable. Reading stops at the first action line, so a
    /// value that happens to contain an equals sign later in the file cannot be mistaken for a
    /// header entry.</para>
    ///
    /// <para>Every key is kept, including ones no option knows about. A plan file also records what
    /// the run <em>did</em> — the steps it took, the entries it compacted — and a reader looking at
    /// a promoted regression wants those beside the bounds.</para>
    /// </summary>
    public static IReadOnlyDictionary<string, string> ParseHeader(IEnumerable<string> lines)
    {
        ArgumentNullException.ThrowIfNull(lines);

        Dictionary<string, string> header = [];

        foreach (string line in lines)
        {
            string text = line.Trim();

            if (text.Length == 0)
                continue;

            if (char.IsDigit(text[0]))
                break;

            int split = text.IndexOf('=', StringComparison.Ordinal);

            if (split > 0)
                header[text[..split]] = text[(split + 1)..];
        }

        return header;
    }

    /// <summary>Reads the header of a plan artifact on disk.</summary>
    public static IReadOnlyDictionary<string, string> ParseHeaderFile(string path) =>
        ParseHeader(File.ReadAllLines(path));

    /// <summary>
    /// Parses one action line.
    ///
    /// <para>Strict on purpose. A line it cannot read is a plan it would replay incorrectly, and a
    /// replay that silently skipped an action would report that a failure did not reproduce.</para>
    /// </summary>
    private static RandomScenarioAction ParseAction(string line)
    {
        string[] parts = line.Split(' ', StringSplitOptions.RemoveEmptyEntries);

        if (parts.Length < 2)
            throw new FormatException($"Not an action line: '{line}'.");

        if (!int.TryParse(parts[0], NumberStyles.None, CultureInfo.InvariantCulture, out int index))
            throw new FormatException($"Action line does not start with an index: '{line}'.");

        if (!Enum.TryParse(parts[1], out RandomScenarioActionKind kind))
            throw new FormatException($"Unknown action '{parts[1]}' in '{line}'.");

        string[] rest = parts[2..];
        long value = 0;

        if (rest.Length > 0 && rest[^1].Contains('=', StringComparison.Ordinal))
        {
            string number = rest[^1].Split('=', 2)[1];

            if (!long.TryParse(number, NumberStyles.Integer, CultureInfo.InvariantCulture, out value))
                throw new FormatException($"Unreadable numeric parameter in '{line}'.");

            rest = rest[..^1];
        }

        return rest.Length switch
        {
            0 => new RandomScenarioAction(index, kind, null, null, value),
            1 => new RandomScenarioAction(index, kind, rest[0], null, value),
            3 when rest[1] == "->" => new RandomScenarioAction(index, kind, rest[0], rest[2], value),
            _ => throw new FormatException($"Unreadable action parameters in '{line}'."),
        };
    }
}
