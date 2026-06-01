using System.Text.Json.Serialization;
using Kommander.Tests.Simulation.Random;

namespace Kommander.Tests.Simulation.Replay;

/// <summary>
/// One JSONL record in a simulation replay log.
/// </summary>
public sealed record ReplayLogEntry
{
    public const int CurrentVersion = 1;

    public required int Version { get; init; }
    public required string EntryType { get; init; }
    public required ulong Seed { get; init; }
    public required string Scenario { get; init; }
    public required IReadOnlyDictionary<string, string> Parameters { get; init; }
    public required int Step { get; init; }
    public required long LogicalTime { get; init; }

    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? ChoiceName { get; init; }

    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public int? ChoiceMinInclusive { get; init; }

    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public int? ChoiceMaxExclusive { get; init; }

    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public int? ChoiceValue { get; init; }

    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public int? EnabledEventCount { get; init; }

    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public long? SelectedEventId { get; init; }

    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? SelectedEventType { get; init; }

    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? SelectedEventSummary { get; init; }

    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? SnapshotHashBefore { get; init; }

    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? SnapshotHashAfter { get; init; }

    public static ReplayLogEntry ForRandomChoice(
        SimulationScenario scenario,
        SimulationRandomChoice choice) =>
        new()
        {
            Version = CurrentVersion,
            EntryType = "random",
            Seed = scenario.Seed,
            Scenario = scenario.Name,
            Parameters = scenario.Parameters,
            Step = choice.Step,
            LogicalTime = choice.LogicalTime,
            ChoiceName = choice.ChoiceName,
            ChoiceMinInclusive = choice.MinInclusive,
            ChoiceMaxExclusive = choice.MaxExclusive,
            ChoiceValue = choice.Value,
        };

    public static ReplayLogEntry ForEventSelection(
        SimulationScenario scenario,
        int step,
        long logicalTime,
        int enabledEventCount,
        SimulationEvent selectedEvent,
        string snapshotHashBefore,
        string snapshotHashAfter) =>
        new()
        {
            Version = CurrentVersion,
            EntryType = "event",
            Seed = scenario.Seed,
            Scenario = scenario.Name,
            Parameters = scenario.Parameters,
            Step = step,
            LogicalTime = logicalTime,
            EnabledEventCount = enabledEventCount,
            SelectedEventId = selectedEvent.Id,
            SelectedEventType = selectedEvent.Type.ToString(),
            SelectedEventSummary = selectedEvent.Summary,
            SnapshotHashBefore = snapshotHashBefore,
            SnapshotHashAfter = snapshotHashAfter,
        };

    public SimulationRandomChoice ToRandomChoice()
    {
        if (!string.Equals(EntryType, "random", StringComparison.Ordinal))
            throw new InvalidOperationException($"Entry type '{EntryType}' is not a random choice.");

        return new SimulationRandomChoice
        {
            Step = Step,
            LogicalTime = LogicalTime,
            ChoiceName = ChoiceName ?? throw new InvalidOperationException("ChoiceName is required."),
            MinInclusive = ChoiceMinInclusive ?? throw new InvalidOperationException("ChoiceMinInclusive is required."),
            MaxExclusive = ChoiceMaxExclusive ?? throw new InvalidOperationException("ChoiceMaxExclusive is required."),
            Value = ChoiceValue ?? throw new InvalidOperationException("ChoiceValue is required."),
        };
    }
}
