using Kommander.Tests.Simulation.Random;
using Kommander.Tests.Simulation.Replay;

namespace Kommander.Tests.Simulation.Scheduling;

/// <summary>
/// Selects the next enabled simulation event using scripted, random, or replay semantics.
/// </summary>
public sealed class DeterministicScheduler
{
    private readonly SimulationRandom _random;
    private readonly ReplayLogReader? _replayReader;
    private ReplayLogEntry? _pendingEventEntry;

    public DeterministicScheduler(SimulationRandom random, ReplayLogReader? replayReader = null)
    {
        _random = random ?? throw new ArgumentNullException(nameof(random));
        _replayReader = replayReader;
    }

    /// <summary>
    /// Selects the next event from the enabled set.
    /// In replay mode, validates that the enabled set and pre-apply snapshot match the recorded step.
    /// </summary>
    public SimulationEvent SelectNext(
        SimulationSchedulingMode mode,
        int stepNumber,
        long logicalTime,
        IReadOnlyList<SimulationEvent> enabledEvents,
        string? snapshotHashBefore = null)
    {
        ArgumentNullException.ThrowIfNull(enabledEvents);

        if (enabledEvents.Count == 0)
            throw new InvalidOperationException("Cannot select an event from an empty enabled set.");

        _random.SetContext(stepNumber, logicalTime);

        return mode switch
        {
            SimulationSchedulingMode.Random => SelectRandom(stepNumber, logicalTime, enabledEvents),
            SimulationSchedulingMode.Replay => SelectReplay(stepNumber, enabledEvents, snapshotHashBefore),
            _ => throw new NotSupportedException($"Scheduling mode '{mode}' is not supported by DeterministicScheduler."),
        };
    }

    /// <summary>
    /// Validates that the post-apply snapshot hash matches the replay entry selected for this step.
    /// </summary>
    public void ValidateAppliedEvent(int stepNumber, string snapshotHashAfter)
    {
        if (_pendingEventEntry is null)
            throw new ReplayDivergenceException(
                $"Replay expected a post-apply validation at step {stepNumber}, but no replay entry was pending.");

        ReplayLogEntry entry = _pendingEventEntry;
        _pendingEventEntry = null;

        if (entry.Step != stepNumber)
        {
            throw new ReplayDivergenceException(
                $"Replay post-apply step mismatch: log has step {entry.Step}, simulation is at step {stepNumber}.");
        }

        string expectedHash = entry.SnapshotHashAfter
            ?? throw new ReplayDivergenceException($"Replay entry at step {stepNumber} is missing snapshotHashAfter.");

        if (!string.Equals(expectedHash, snapshotHashAfter, StringComparison.Ordinal))
        {
            throw new ReplayDivergenceException(
                $"Snapshot hash diverged after step {stepNumber}: replay recorded '{expectedHash}', " +
                $"current run produced '{snapshotHashAfter}'.");
        }
    }

    private SimulationEvent SelectRandom(int stepNumber, long logicalTime, IReadOnlyList<SimulationEvent> enabledEvents)
    {
        int index = _random.NextInt("select-event-index", minInclusive: 0, maxExclusive: enabledEvents.Count);
        return enabledEvents[index];
    }

    private SimulationEvent SelectReplay(
        int stepNumber,
        IReadOnlyList<SimulationEvent> enabledEvents,
        string? snapshotHashBefore)
    {
        if (_replayReader is null)
            throw new ReplayDivergenceException("Replay mode requires a replay log reader.");

        ReplayLogEntry upcomingEvent = _replayReader.PeekNextEventEntry();

        if (upcomingEvent.Step != stepNumber)
        {
            throw new ReplayDivergenceException(
                $"Replay step mismatch: log has step {upcomingEvent.Step}, simulation is at step {stepNumber}.");
        }

        if (upcomingEvent.EnabledEventCount != enabledEvents.Count)
        {
            throw new ReplayDivergenceException(
                $"Enabled event set diverged at step {stepNumber}: replay recorded {upcomingEvent.EnabledEventCount} " +
                $"enabled events, current run has {enabledEvents.Count}.");
        }

        _random.NextInt("select-event-index", minInclusive: 0, maxExclusive: enabledEvents.Count);

        if (!_replayReader.TryTakeNextEventSelection(out ReplayLogEntry entry))
        {
            throw new ReplayDivergenceException(
                $"Replay log exhausted while simulation still had {enabledEvents.Count} enabled events at step {stepNumber}.");
        }

        if (entry.Step != stepNumber)
        {
            throw new ReplayDivergenceException(
                $"Replay step mismatch: log has step {entry.Step}, simulation is at step {stepNumber}.");
        }

        if (entry.EnabledEventCount != enabledEvents.Count)
        {
            throw new ReplayDivergenceException(
                $"Enabled event set diverged at step {stepNumber}: replay recorded {entry.EnabledEventCount} " +
                $"enabled events, current run has {enabledEvents.Count}.");
        }

        long selectedEventId = entry.SelectedEventId
            ?? throw new ReplayDivergenceException($"Replay entry at step {stepNumber} is missing selectedEventId.");

        SimulationEvent? selected = enabledEvents.FirstOrDefault(enabled => enabled.Id == selectedEventId);
        if (selected is null)
        {
            throw new ReplayDivergenceException(
                $"Replay selected event id {selectedEventId} at step {stepNumber}, " +
                $"but that id is not in the current enabled set: [{string.Join(", ", enabledEvents.Select(e => e.Id))}].");
        }

        string expectedType = entry.SelectedEventType
            ?? throw new ReplayDivergenceException($"Replay entry at step {stepNumber} is missing selectedEventType.");

        if (!string.Equals(expectedType, selected.Type.ToString(), StringComparison.Ordinal))
        {
            throw new ReplayDivergenceException(
                $"Selected event type diverged at step {stepNumber}: replay recorded '{expectedType}', " +
                $"current enabled event has '{selected.Type}'.");
        }

        string expectedSummary = entry.SelectedEventSummary
            ?? throw new ReplayDivergenceException($"Replay entry at step {stepNumber} is missing selectedEventSummary.");

        if (!string.Equals(expectedSummary, selected.Summary, StringComparison.Ordinal))
        {
            throw new ReplayDivergenceException(
                $"Selected event summary diverged at step {stepNumber}: replay recorded '{expectedSummary}', " +
                $"current enabled event has '{selected.Summary}'.");
        }

        string expectedHashBefore = entry.SnapshotHashBefore
            ?? throw new ReplayDivergenceException($"Replay entry at step {stepNumber} is missing snapshotHashBefore.");

        if (string.IsNullOrEmpty(snapshotHashBefore))
        {
            throw new ReplayDivergenceException(
                $"Replay at step {stepNumber} requires a pre-apply snapshot hash, but none was provided.");
        }

        if (!string.Equals(expectedHashBefore, snapshotHashBefore, StringComparison.Ordinal))
        {
            throw new ReplayDivergenceException(
                $"Snapshot hash diverged before step {stepNumber}: replay recorded '{expectedHashBefore}', " +
                $"current run produced '{snapshotHashBefore}'.");
        }

        if (string.IsNullOrEmpty(entry.SnapshotHashAfter))
        {
            throw new ReplayDivergenceException($"Replay entry at step {stepNumber} is missing snapshotHashAfter.");
        }

        _pendingEventEntry = entry;
        return selected;
    }

    /// <summary>
    /// Ensures the replay log was fully consumed and no post-apply validation is still pending.
    /// </summary>
    public void ValidateReplayFullyConsumed()
    {
        if (_replayReader is null)
            return;

        if (_pendingEventEntry is not null)
        {
            throw new ReplayDivergenceException(
                $"Replay ended with a pending event entry at step {_pendingEventEntry.Step} that was never validated after apply.");
        }

        _replayReader.ValidateFullyConsumed();
    }
}
