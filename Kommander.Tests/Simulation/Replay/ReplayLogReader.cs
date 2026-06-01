using System.IO;
using System.Text.Json;

namespace Kommander.Tests.Simulation.Replay;

/// <summary>
/// Reads simulation replay records from a JSONL file.
/// </summary>
public sealed class ReplayLogReader
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    private readonly IReadOnlyList<ReplayLogEntry> _entries;
    private int _nextEntryIndex;

    public ReplayLogReader(string path)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);

        if (!File.Exists(path))
            throw new FileNotFoundException($"Replay log not found: {path}", path);

        Path = path;
        _entries = File.ReadLines(path)
            .Where(line => !string.IsNullOrWhiteSpace(line))
            .Select(ParseLine)
            .ToList();

        if (_entries.Count == 0)
            throw new ReplayDivergenceException($"Replay log '{path}' is empty.");
    }

    public ReplayLogReader(IReadOnlyList<ReplayLogEntry> entries)
    {
        ArgumentNullException.ThrowIfNull(entries);

        if (entries.Count == 0)
            throw new ReplayDivergenceException("Replay log has no entries.");

        Path = string.Empty;
        _entries = entries;
    }

    public string Path { get; }

    public IReadOnlyList<ReplayLogEntry> Entries => _entries;

    public IReadOnlyList<ReplayLogEntry> EventEntries =>
        _entries.Where(entry => string.Equals(entry.EntryType, "event", StringComparison.Ordinal)).ToList();

    public IReadOnlyList<ReplayLogEntry> RandomEntries =>
        _entries.Where(entry => string.Equals(entry.EntryType, "random", StringComparison.Ordinal)).ToList();

    public ReplayLogEntry Header =>
        _entries[0];

    /// <summary>
    /// Returns the next event entry in the replay log without consuming it.
    /// </summary>
    public ReplayLogEntry PeekNextEventEntry()
    {
        foreach (ReplayLogEntry entry in _entries.Skip(_nextEntryIndex))
        {
            if (string.Equals(entry.EntryType, "event", StringComparison.Ordinal))
                return entry;
        }

        throw new ReplayDivergenceException("Replay log has no remaining event entries.");
    }

    public bool TryTakeNextEventSelection(out ReplayLogEntry entry)
    {
        if (_nextEntryIndex >= _entries.Count)
        {
            entry = null!;
            return false;
        }

        ReplayLogEntry candidate = _entries[_nextEntryIndex++];
        if (!string.Equals(candidate.EntryType, "event", StringComparison.Ordinal))
        {
            throw new ReplayDivergenceException(
                $"Expected replay event entry at log position {_nextEntryIndex - 1}, found '{candidate.EntryType}'.");
        }

        entry = candidate;
        return true;
    }

    /// <summary>
    /// Consumes the next random entry in the replay log and validates it against the request.
    /// </summary>
    public int ConsumeRandomChoice(string choiceName, int minInclusive, int maxExclusive, int step)
    {
        if (_nextEntryIndex >= _entries.Count)
        {
            throw new ReplayDivergenceException(
                $"Replay expected random choice '{choiceName}' at step {step}, but the replay log is exhausted.");
        }

        ReplayLogEntry candidate = _entries[_nextEntryIndex++];
        if (!string.Equals(candidate.EntryType, "random", StringComparison.Ordinal))
        {
            throw new ReplayDivergenceException(
                $"Expected replay random entry at step {step}, found '{candidate.EntryType}'.");
        }

        if (!string.Equals(candidate.ChoiceName, choiceName, StringComparison.Ordinal)
            || candidate.ChoiceMinInclusive != minInclusive
            || candidate.ChoiceMaxExclusive != maxExclusive)
        {
            throw new ReplayDivergenceException(
                $"Random choice mismatch at step {step}: log has '{candidate.ChoiceName}' " +
                $"[{candidate.ChoiceMinInclusive}, {candidate.ChoiceMaxExclusive}), got request for '{choiceName}' " +
                $"[{minInclusive}, {maxExclusive}).");
        }

        int value = candidate.ChoiceValue
            ?? throw new ReplayDivergenceException($"Replay random entry at step {step} is missing choiceValue.");

        if (value < minInclusive || value >= maxExclusive)
        {
            throw new ReplayDivergenceException(
                $"Replayed random value {value} is outside requested range " +
                $"[{minInclusive}, {maxExclusive}) for choice '{choiceName}'.");
        }

        return value;
    }

    /// <summary>Number of replay log entries that were never consumed.</summary>
    public int UnconsumedEntryCount => _entries.Count - _nextEntryIndex;

    /// <summary>
    /// Ensures every entry in the replay log was consumed in order during the run.
    /// </summary>
    public void ValidateFullyConsumed()
    {
        int remaining = UnconsumedEntryCount;
        if (remaining > 0)
        {
            throw new ReplayDivergenceException(
                $"Replay ended with {remaining} unconsumed log entr{(remaining == 1 ? "y" : "ies")}.");
        }
    }

    /// <summary>
    /// Ensures every event entry in the replay log was consumed during the run.
    /// </summary>
    public void ValidateAllEventEntriesConsumed() => ValidateFullyConsumed();

    private static ReplayLogEntry ParseLine(string line)
    {
        try
        {
            return JsonSerializer.Deserialize<ReplayLogEntry>(line, JsonOptions)
                ?? throw new ReplayDivergenceException("Replay log line deserialized to null.");
        }
        catch (JsonException ex)
        {
            throw new ReplayDivergenceException($"Invalid replay log line: {line}", ex);
        }
    }
}
