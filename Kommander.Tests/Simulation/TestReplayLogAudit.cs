using Kommander.Tests.Simulation.Random;
using Kommander.Tests.Simulation.Replay;
using Kommander.Tests.Simulation.Scenarios.Random;

namespace Kommander.Tests.Simulation;

/// <summary>
/// The entrypoint that reads a replay log and says whether it describes the run in hand.
///
/// <para><b>Why this needs its own tests.</b> The step-by-step checks in
/// <see cref="ReplayLogReader"/> compare one entry against one request. They cannot see that the
/// whole file belongs to another run: a log recorded at twenty-four actions replayed against twelve
/// matches entry for entry until it runs out, and the failure a reader then sees is "the log is
/// exhausted", which points at the wrong thing entirely.</para>
/// </summary>
public sealed class TestReplayLogAudit
{
    private const string ScenarioName = "random-cluster";

    /// <summary>A log written by a run matches that run.</summary>
    [Fact]
    [Trait("Category", "DSTSmoke")]
    public void ALogMatchesTheRunThatWroteIt()
    {
        RandomScenarioOptions options = new() { ActionCount = 12, StepsPerAction = 5 };

        ReplayLogReader reader = WriteAndRead(9001, options, choices: 4);

        ReplayLogAudit.RequireMatches(reader, Scenario(9001, options));
        ReplayLogAudit.RequireOneRun(reader);
    }

    /// <summary>
    /// A bound that differs is named, and the message says which one.
    ///
    /// <para>The message is the product here. "The log does not match" sends a reader back to the
    /// files; "actionCount was recorded as 12 and the run sets 24" ends the investigation.</para>
    /// </summary>
    [Fact]
    [Trait("Category", "DSTSmoke")]
    public void ALogRecordedUnderOtherBounds_IsRejectedByName()
    {
        RandomScenarioOptions recorded = new() { ActionCount = 12, StepsPerAction = 5 };
        RandomScenarioOptions replayed = new() { ActionCount = 24, StepsPerAction = 5 };

        ReplayLogReader reader = WriteAndRead(9002, recorded, choices: 3);

        ReplayDivergenceException error = Assert.Throws<ReplayDivergenceException>(
            () => ReplayLogAudit.RequireMatches(reader, Scenario(9002, replayed)));

        Assert.Contains("actionCount", error.Message, StringComparison.Ordinal);
        Assert.Contains("12", error.Message, StringComparison.Ordinal);
        Assert.Contains("24", error.Message, StringComparison.Ordinal);
    }

    /// <summary>A log recorded under another seed is rejected.</summary>
    [Fact]
    [Trait("Category", "DSTSmoke")]
    public void ALogRecordedUnderAnotherSeed_IsRejected()
    {
        RandomScenarioOptions options = new() { ActionCount = 12 };

        ReplayLogReader reader = WriteAndRead(9003, options, choices: 2);

        ReplayDivergenceException error = Assert.Throws<ReplayDivergenceException>(
            () => ReplayLogAudit.RequireMatches(reader, Scenario(9004, options)));

        Assert.Contains("seed", error.Message, StringComparison.Ordinal);
    }

    /// <summary>A log recorded for another scenario is rejected before any step runs.</summary>
    [Fact]
    [Trait("Category", "DSTSmoke")]
    public void ALogRecordedForAnotherScenario_IsRejected()
    {
        RandomScenarioOptions options = new();

        ReplayLogReader reader = WriteAndRead(9005, options, choices: 2);

        RandomClusterScenario other = new("some-other-run", 9005, options.ToParameters());

        ReplayDivergenceException error = Assert.Throws<ReplayDivergenceException>(
            () => ReplayLogAudit.RequireMatches(reader, other));

        Assert.Contains("some-other-run", error.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// A file assembled from two runs is rejected.
    ///
    /// <para>It reads cleanly — every line is valid — and replays nonsense. Two runs writing to one
    /// path is how it happens in practice.</para>
    /// </summary>
    [Fact]
    [Trait("Category", "DSTSmoke")]
    public void ALogHoldingTwoRuns_IsRejected()
    {
        RandomScenarioOptions options = new();

        List<ReplayLogEntry> entries =
        [
            .. Entries(9006, options, choices: 2),
            .. Entries(9007, options, choices: 2),
        ];

        ReplayDivergenceException error = Assert.Throws<ReplayDivergenceException>(
            () => ReplayLogAudit.RequireOneRun(new ReplayLogReader(entries)));

        Assert.Contains("9007", error.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// A run that sets a bound the log never recorded is rejected too.
    ///
    /// <para>The direction that is easy to forget. A caller cannot reproduce what the recording
    /// never knew about, so an extra parameter is a mismatch in exactly the way a changed one
    /// is.</para>
    /// </summary>
    [Fact]
    [Trait("Category", "DSTSmoke")]
    public void ARunSettingABoundTheLogNeverRecorded_IsRejected()
    {
        RandomScenarioOptions options = new();

        Dictionary<string, string> trimmed = new(options.ToParameters());
        trimmed.Remove("maintenanceWeight");

        ReplayLogReader reader = new(Entries(9008, trimmed, choices: 2));

        ReplayDivergenceException error = Assert.Throws<ReplayDivergenceException>(
            () => ReplayLogAudit.RequireMatches(reader, Scenario(9008, options)));

        Assert.Contains("maintenanceWeight", error.Message, StringComparison.Ordinal);
    }

    // ── Helpers ───────────────────────────────────────────────────────────

    private static RandomClusterScenario Scenario(ulong seed, RandomScenarioOptions options) =>
        new(ScenarioName, seed, options.ToParameters());

    /// <summary>One recorded draw. The values are filler: the audit reads the header, not these.</summary>
    private static SimulationRandomChoice Choice(int index) =>
        new()
        {
            Step = index,
            LogicalTime = index * 50L,
            ChoiceName = $"choice-{index}",
            MinInclusive = 0,
            MaxExclusive = 4,
            Value = index % 4,
        };

    /// <summary>Writes a log to disk and reads it back, so the file itself is under test.</summary>
    private static ReplayLogReader WriteAndRead(ulong seed, RandomScenarioOptions options, int choices)
    {
        string directory = Path.Combine(AppContext.BaseDirectory, "dst-artifacts", "audit-check");
        Directory.CreateDirectory(directory);

        string path = Path.Combine(directory, $"audit-seed-{seed}.replay.jsonl");

        using (ReplayLogWriter writer = new(path, Scenario(seed, options)))
        {
            for (int index = 0; index < choices; index++)
                writer.WriteRandomChoice(Choice(index));
        }

        return new ReplayLogReader(path);
    }

    private static List<ReplayLogEntry> Entries(ulong seed, RandomScenarioOptions options, int choices) =>
        Entries(seed, options.ToParameters(), choices);

    private static List<ReplayLogEntry> Entries(
        ulong seed,
        IReadOnlyDictionary<string, string> parameters,
        int choices)
    {
        RandomClusterScenario scenario = new(ScenarioName, seed, parameters);

        return [.. Enumerable.Range(0, choices).Select(index =>
            ReplayLogEntry.ForRandomChoice(
                scenario,
                Choice(index)))];
    }
}
