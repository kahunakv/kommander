using Kommander.Tests.Simulation.Random;

namespace Kommander.Tests.Simulation.Replay;

/// <summary>
/// Checks a replay log against the run somebody is about to perform.
///
/// <para><b>What this catches that the reader does not.</b> <see cref="ReplayLogReader"/> validates
/// a log as it is consumed: the next entry is a random choice, its name matches, its range matches.
/// That is a check on each step. It says nothing about whether the log belongs to this run at all.
/// A log recorded at twenty-four actions, replayed against a run configured for twelve, matches
/// entry for entry until it runs out — and the reader then reports an exhausted log rather than the
/// truth, which is that the two runs were never the same experiment.</para>
///
/// <para><b>Why the header is the right place to catch it.</b> Every entry carries the seed, the
/// scenario name and the parameters the run was recorded under. Comparing those once, before the
/// first step, turns a confusing failure deep in a replay into one sentence naming the parameter
/// that differs.</para>
///
/// <para>Divergence is raised as <see cref="ReplayDivergenceException"/>, the same type the
/// step-by-step checks raise, because to a caller it is the same event: this log does not describe
/// this run.</para>
/// </summary>
public static class ReplayLogAudit
{
    /// <summary>
    /// Requires the log to have been recorded for this scenario, seed and parameters.
    ///
    /// <para>A parameter the log does not carry is a mismatch, not a pass. The log is the record of
    /// what ran; a parameter missing from it was not in force, and replaying with it set is a
    /// different run. The reverse — a parameter in the log that the caller no longer sets — is
    /// reported the same way, because the caller cannot reproduce what it does not know about.</para>
    /// </summary>
    public static void RequireMatches(ReplayLogReader reader, SimulationScenario scenario)
    {
        ArgumentNullException.ThrowIfNull(reader);
        ArgumentNullException.ThrowIfNull(scenario);

        ReplayLogEntry header = reader.Header;

        if (header.Version != ReplayLogEntry.CurrentVersion)
        {
            throw new ReplayDivergenceException(
                $"Replay log was written at version {header.Version}, this build reads version " +
                $"{ReplayLogEntry.CurrentVersion}.");
        }

        if (!string.Equals(header.Scenario, scenario.Name, StringComparison.Ordinal))
        {
            throw new ReplayDivergenceException(
                $"Replay log records scenario '{header.Scenario}', the run is '{scenario.Name}'.");
        }

        if (header.Seed != scenario.Seed)
        {
            throw new ReplayDivergenceException(
                $"Replay log records seed {header.Seed}, the run is seed {scenario.Seed}.");
        }

        List<string> differences = [];

        foreach ((string key, string recorded) in header.Parameters)
        {
            if (!scenario.Parameters.TryGetValue(key, out string? current))
            {
                differences.Add($"'{key}' was recorded as '{recorded}' and the run does not set it");
                continue;
            }

            if (!string.Equals(recorded, current, StringComparison.Ordinal))
                differences.Add($"'{key}' was recorded as '{recorded}' and the run sets '{current}'");
        }

        foreach (string key in scenario.Parameters.Keys)
        {
            if (!header.Parameters.ContainsKey(key))
                differences.Add($"'{key}' is set by the run and was not recorded");
        }

        if (differences.Count > 0)
        {
            throw new ReplayDivergenceException(
                $"Replay log does not describe this run: {string.Join("; ", differences)}.");
        }
    }

    /// <summary>
    /// Requires every entry in the log to name the same run as its header.
    ///
    /// <para>A log is appended one entry at a time and each entry carries the scenario again. Two
    /// runs writing to one path, or a file assembled from two logs by hand, produce a file that
    /// reads cleanly and replays nonsense. This is the cheap check that says so.</para>
    /// </summary>
    public static void RequireOneRun(ReplayLogReader reader)
    {
        ArgumentNullException.ThrowIfNull(reader);

        ReplayLogEntry header = reader.Header;

        for (int index = 1; index < reader.Entries.Count; index++)
        {
            ReplayLogEntry entry = reader.Entries[index];

            if (entry.Seed != header.Seed
                || !string.Equals(entry.Scenario, header.Scenario, StringComparison.Ordinal))
            {
                throw new ReplayDivergenceException(
                    $"Replay log entry {index} names run '{entry.Scenario}' seed {entry.Seed}, " +
                    $"the log opens with '{header.Scenario}' seed {header.Seed}.");
            }
        }
    }
}
