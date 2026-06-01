using Kommander.Tests.Simulation.Random;
using Kommander.Tests.Simulation.Replay;
using Kommander.Tests.Simulation.Scheduling;
using Kommander.Tests.Simulation.Time;

namespace Kommander.Tests.Simulation;

/// <summary>
/// Owns one complete deterministic simulation run.
/// Future tasks attach transport, WAL, and cluster node implementations here.
/// </summary>
public sealed class SimulationRuntime : IDisposable
{
    private readonly List<SimulationEvent> _eventHistory = [];
    private readonly List<SimulationEvent> _scriptedEvents = [];
    private readonly Dictionary<int, SimulationNodeSnapshot> _nodes = [];
    private readonly Dictionary<int, SimulationPartitionLeaderSnapshot> _partitionLeaders = [];
    private readonly List<SimulationPendingMessageSnapshot> _pendingNetworkMessages = [];
    private readonly List<SimulationPendingWalOperationSnapshot> _pendingWalOperations = [];
    private readonly Dictionary<string, int> _schedulerQueueDepths = [];
    private readonly List<SimulationClientProposalSnapshot> _clientProposals = [];
    private readonly List<Action<SimulationSnapshot>> _invariantChecks = [];

    private readonly SimulationRandom _random;
    private readonly DeterministicScheduler _scheduler;
    private readonly bool _shouldWriteReplayLog;
    private readonly string? _replayLogPath;
    private ReplayLogWriter? _replayWriter;

    private long _nextEventId = 1;
    private SimulationSnapshot? _lastValidSnapshot;

    public SimulationRuntime(SimulationScenario scenario, SimulationRuntimeOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(scenario);
        options ??= new SimulationRuntimeOptions();

        Scenario = scenario;
        Seed = scenario.Seed;
        SchedulingMode = scenario.SchedulingMode;
        _replayLogPath = options.ReplayLogPath;

        ReplayLogReader? replayReader = null;
        if (SchedulingMode == SimulationSchedulingMode.Replay)
        {
            if (string.IsNullOrWhiteSpace(options.ReplayLogPath))
                throw new ArgumentException("Replay mode requires SimulationRuntimeOptions.ReplayLogPath.");

            replayReader = new ReplayLogReader(options.ReplayLogPath);
            ValidateReplayHeader(replayReader.Header, scenario);
            _random = new SimulationRandom(scenario.Seed, replayReader);
        }
        else
        {
            _random = new SimulationRandom(scenario.Seed);
        }

        _scheduler = new DeterministicScheduler(_random, replayReader);
        VirtualTime = new VirtualTime(options.VirtualTimeConfiguration);

        if (SchedulingMode == SimulationSchedulingMode.Random
            && options.WriteReplayLog
            && !string.IsNullOrWhiteSpace(options.ReplayLogPath))
        {
            _shouldWriteReplayLog = true;
        }

        for (int nodeId = 0; nodeId < scenario.NodeCount; nodeId++)
            _nodes[nodeId] = SimulationNodeSnapshot.Initial(nodeId);

        scenario.Configure(this);
        _scriptedEvents.AddRange(scenario.ScriptedEvents());
        _lastValidSnapshot = CaptureSnapshot();
    }

    /// <summary>Scenario metadata for this run.</summary>
    public SimulationScenario Scenario { get; }

    /// <summary>Seed copied from the scenario at construction time.</summary>
    public ulong Seed { get; }

    /// <summary>Active scheduling mode for this run.</summary>
    public SimulationSchedulingMode SchedulingMode { get; }

    /// <summary>Deterministic PRNG for this run.</summary>
    public SimulationRandom Random => _random;

    /// <summary>Virtual logical clock and timer queue for this run.</summary>
    public VirtualTime VirtualTime { get; }

    /// <summary>Path to the replay log when one was read or written.</summary>
    public string? ReplayLogPath => _replayWriter?.FilePath ?? _replayLogPath;

    /// <summary>Current logical tick. Advances only through explicit simulation events.</summary>
    public long LogicalTick { get; private set; }

    /// <summary>Number of events applied so far.</summary>
    public int StepNumber { get; private set; }

    /// <summary>Events applied during this run, in order.</summary>
    public IReadOnlyList<SimulationEvent> EventHistory => _eventHistory;

    /// <summary>Events still waiting to be applied in scripted mode.</summary>
    public IReadOnlyList<SimulationEvent> RemainingScriptedEvents =>
        _scriptedEvents.Skip(StepNumber).ToList();

    /// <summary>Registers an invariant check invoked after every applied event.</summary>
    public void RegisterInvariant(string name, Action<SimulationSnapshot> check) =>
        _invariantChecks.Add(snapshot =>
        {
            try
            {
                check(snapshot);
            }
            catch (Exception ex) when (ex is not InvariantViolationException)
            {
                throw new InvariantViolationException(
                    name,
                    ex.Message,
                    StepNumber,
                    _eventHistory.Count > 0 ? _eventHistory[^1] : null,
                    _lastValidSnapshot,
                    snapshot);
            }
        });

    /// <summary>Sets lifecycle status for a node without touching real runtime objects.</summary>
    public void SetNodeLifecycleStatus(int nodeId, SimulationNodeLifecycleStatus status)
    {
        SimulationNodeSnapshot current = GetNode(nodeId);
        _nodes[nodeId] = current with { LifecycleStatus = status };
    }

    /// <summary>Updates Raft-visible node state for snapshot and invariant checks.</summary>
    public void SetNodeRaftState(
        int nodeId,
        long currentTerm,
        string? votedFor = null,
        string? knownLeader = null)
    {
        SimulationNodeSnapshot current = GetNode(nodeId);
        _nodes[nodeId] = current with
        {
            CurrentTerm = currentTerm,
            VotedFor = votedFor ?? current.VotedFor,
            KnownLeader = knownLeader ?? current.KnownLeader,
        };
    }

    /// <summary>Records WAL log summaries for a node partition.</summary>
    public void SetNodeWalLogs(
        int nodeId,
        IReadOnlyList<SimulationWalLogSummary>? committed = null,
        IReadOnlyList<SimulationWalLogSummary>? proposed = null,
        IReadOnlyList<SimulationWalLogSummary>? rolledBack = null)
    {
        SimulationNodeSnapshot current = GetNode(nodeId);
        _nodes[nodeId] = current with
        {
            CommittedLogs = committed ?? current.CommittedLogs,
            ProposedLogs = proposed ?? current.ProposedLogs,
            RolledBackLogs = rolledBack ?? current.RolledBackLogs,
        };
    }

    /// <summary>Records the current leader for a partition.</summary>
    public void SetPartitionLeader(int partitionId, int leaderNodeId, long term) =>
        _partitionLeaders[partitionId] = new SimulationPartitionLeaderSnapshot
        {
            PartitionId = partitionId,
            LeaderNodeId = leaderNodeId,
            Term = term,
        };

    /// <summary>Queues a pending network message visible in snapshots.</summary>
    public void EnqueuePendingNetworkMessage(SimulationPendingMessageSnapshot message) =>
        _pendingNetworkMessages.Add(message);

    /// <summary>Queues a pending WAL operation visible in snapshots.</summary>
    public void EnqueuePendingWalOperation(SimulationPendingWalOperationSnapshot operation) =>
        _pendingWalOperations.Add(operation);

    /// <summary>Schedules a virtual timer via <see cref="VirtualTime"/>.</summary>
    public VirtualTimer ScheduleTimer(
        int nodeId,
        SimulationTimerKind kind,
        long firesAtLogicalTime,
        long intervalMs = 0,
        int? partitionId = null,
        Action<SimulationRuntime, VirtualTimer>? onFire = null) =>
        VirtualTime.Queue.Schedule(nodeId, kind, firesAtLogicalTime, intervalMs, partitionId, onFire);

    /// <summary>Collects enabled events from the scenario and virtual timer queue.</summary>
    public IReadOnlyList<SimulationEvent> CollectEnabledEvents()
    {
        List<SimulationEvent> enabled = [];
        enabled.AddRange(Scenario.GetEnabledEvents(this));
        enabled.AddRange(VirtualTime.GetEnabledTimerEvents(this));
        return enabled;
    }

    /// <summary>Records scheduler queue depth for diagnostics.</summary>
    public void SetSchedulerQueueDepth(string queueName, int depth) =>
        _schedulerQueueDepths[queueName] = depth;

    /// <summary>Tracks a client proposal visible in snapshots.</summary>
    public void TrackClientProposal(SimulationClientProposalSnapshot proposal) =>
        _clientProposals.Add(proposal);

    /// <summary>Captures the current simulation state.</summary>
    public SimulationSnapshot CaptureSnapshot() =>
        new()
        {
            LogicalTick = LogicalTick,
            StepNumber = StepNumber,
            Nodes = _nodes.Values.OrderBy(node => node.NodeId).ToList(),
            PartitionLeaders = new Dictionary<int, SimulationPartitionLeaderSnapshot>(_partitionLeaders),
            PendingNetworkMessages = _pendingNetworkMessages.ToList(),
            PendingWalOperations = _pendingWalOperations.ToList(),
            PendingTimers = VirtualTime.Queue.GetPendingSnapshots(),
            SchedulerQueueDepths = new Dictionary<string, int>(_schedulerQueueDepths),
            ClientProposals = _clientProposals.ToList(),
        };

    /// <summary>
    /// Runs the scenario using scripted, random, or replay scheduling.
    /// Does not start threads, wall-clock timers, or real network/storage resources.
    /// </summary>
    public SimulationResult Run()
    {
        if (_shouldWriteReplayLog)
            _replayWriter = new ReplayLogWriter(_replayLogPath!, Scenario);

        try
        {
            switch (SchedulingMode)
            {
                case SimulationSchedulingMode.Scripted:
                    RunScripted();
                    break;
                case SimulationSchedulingMode.Random:
                case SimulationSchedulingMode.Replay:
                    RunScheduled();
                    if (SchedulingMode == SimulationSchedulingMode.Replay)
                        _scheduler.ValidateReplayFullyConsumed();
                    break;
                default:
                    throw new NotSupportedException($"Scheduling mode '{SchedulingMode}' is not supported yet.");
            }

            SimulationSnapshot finalSnapshot = CaptureSnapshot();
            RunInvariantChecks(finalSnapshot);
            _lastValidSnapshot = finalSnapshot;

            return SimulationResult.Success(Scenario, this, finalSnapshot, ReplayLogPath);
        }
        catch (InvariantViolationException ex)
        {
            return SimulationResult.Failure(Scenario, this, ex, ReplayLogPath);
        }
        catch (ReplayDivergenceException ex)
        {
            return SimulationResult.Failure(
                Scenario,
                this,
                new InvariantViolationException(
                    "replay-divergence",
                    ex.Message,
                    StepNumber,
                    _eventHistory.Count > 0 ? _eventHistory[^1] : null,
                    _lastValidSnapshot,
                    CaptureSnapshot()),
                ReplayLogPath);
        }
        finally
        {
            Dispose();
        }
    }

    /// <summary>Applies one event, advances step state, and runs invariant checks.</summary>
    public void ApplyEvent(SimulationEvent simulationEvent)
    {
        ArgumentNullException.ThrowIfNull(simulationEvent);

        SimulationSnapshot snapshotBefore = CaptureSnapshot();
        string snapshotHashBefore = snapshotBefore.ComputeContentHash();

        if (simulationEvent.Type == SimulationEventType.TimerTick
            && simulationEvent.TimerId is long timerId
            && VirtualTime.Queue.TryGetTimer(timerId, out VirtualTimer? timer)
            && timer is not null)
        {
            if (simulationEvent.LogicalTime != timer.FiresAtLogicalTime)
            {
                throw new InvalidOperationException(
                    $"TimerTick event for timer {timerId} is stale: event logical time " +
                    $"{simulationEvent.LogicalTime} does not match scheduled fire time {timer.FiresAtLogicalTime}.");
            }

            if (timer.FiresAtLogicalTime > LogicalTick)
                LogicalTick = timer.FiresAtLogicalTime;

            VirtualTime.FireTimer(this, timerId);
        }
        else if (simulationEvent.LogicalTime > LogicalTick)
        {
            LogicalTick = simulationEvent.LogicalTime;
        }

        _eventHistory.Add(simulationEvent);
        StepNumber++;

        SimulationSnapshot snapshot = CaptureSnapshot();
        string snapshotHashAfter = snapshot.ComputeContentHash();

        if (SchedulingMode == SimulationSchedulingMode.Replay)
            _scheduler.ValidateAppliedEvent(StepNumber, snapshotHashAfter);

        if (_replayWriter is not null)
        {
            foreach (SimulationRandomChoice choice in _random.RecordedChoices.Skip(_replayWriterRandomChoiceCount))
                _replayWriter.WriteRandomChoice(choice);

            _replayWriterRandomChoiceCount = _random.RecordedChoices.Count;
            _replayWriter.WriteEventSelection(
                StepNumber,
                LogicalTick,
                _lastEnabledEventCount,
                simulationEvent,
                snapshotHashBefore,
                snapshotHashAfter);
        }

        RunInvariantChecks(snapshot);
        _lastValidSnapshot = snapshot;
    }

    /// <summary>Allocates the next stable event id for scripted or generated events.</summary>
    public long AllocateEventId() => _nextEventId++;

    /// <inheritdoc />
    public void Dispose()
    {
        if (_replayWriter is not null)
        {
            _replayWriter.Dispose();
            _replayWriter = null;
        }
    }

    private int _replayWriterRandomChoiceCount;
    private int _lastEnabledEventCount;

    private void RunScripted()
    {
        while (StepNumber < Scenario.MaxSteps && StepNumber < _scriptedEvents.Count)
            ApplyEvent(_scriptedEvents[StepNumber]);
    }

    private void RunScheduled()
    {
        while (StepNumber < Scenario.MaxSteps && LogicalTick <= Scenario.MaxLogicalTime)
        {
            IReadOnlyList<SimulationEvent> enabledEvents = CollectEnabledEvents();
            if (enabledEvents.Count == 0)
                break;

            _lastEnabledEventCount = enabledEvents.Count;
            int upcomingStep = StepNumber + 1;
            string snapshotHashBefore = CaptureSnapshot().ComputeContentHash();
            SimulationEvent selected = _scheduler.SelectNext(
                SchedulingMode,
                upcomingStep,
                LogicalTick,
                enabledEvents,
                snapshotHashBefore);

            ApplyEvent(selected);
        }
    }

    private static void ValidateReplayHeader(ReplayLogEntry header, SimulationScenario scenario)
    {
        if (header.Seed != scenario.Seed)
        {
            throw new ReplayDivergenceException(
                $"Replay seed mismatch: log has {header.Seed}, scenario has {scenario.Seed}.");
        }

        if (!string.Equals(header.Scenario, scenario.Name, StringComparison.Ordinal))
        {
            throw new ReplayDivergenceException(
                $"Replay scenario mismatch: log has '{header.Scenario}', scenario is '{scenario.Name}'.");
        }
    }

    private SimulationNodeSnapshot GetNode(int nodeId)
    {
        if (!_nodes.TryGetValue(nodeId, out SimulationNodeSnapshot? node))
            throw new ArgumentOutOfRangeException(nameof(nodeId), $"Node {nodeId} is not part of this simulation.");

        return node;
    }

    private void RunInvariantChecks(SimulationSnapshot snapshot)
    {
        foreach (Action<SimulationSnapshot> check in _invariantChecks)
        {
            try
            {
                check(snapshot);
            }
            catch (InvariantViolationException ex)
            {
                throw new InvariantViolationException(
                    ex.InvariantName,
                    ex.Message,
                    StepNumber,
                    _eventHistory.Count > 0 ? _eventHistory[^1] : null,
                    _lastValidSnapshot,
                    snapshot);
            }
        }
    }
}
