using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Kommander.Tests.Simulation;

/// <summary>
/// Immutable simulation state captured after a step for invariant checks and debugging.
/// </summary>
public sealed record SimulationSnapshot
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    /// <summary>Current logical tick when the snapshot was taken.</summary>
    public required long LogicalTick { get; init; }

    /// <summary>Number of events that have been applied so far.</summary>
    public required int StepNumber { get; init; }

    /// <summary>Per-node lifecycle and Raft-visible state.</summary>
    public required IReadOnlyList<SimulationNodeSnapshot> Nodes { get; init; }

    /// <summary>Per-partition leader identity keyed by partition id.</summary>
    public required IReadOnlyDictionary<int, SimulationPartitionLeaderSnapshot> PartitionLeaders { get; init; }

    /// <summary>Network messages waiting for delivery.</summary>
    public required IReadOnlyList<SimulationPendingMessageSnapshot> PendingNetworkMessages { get; init; }

    /// <summary>WAL operations waiting for completion.</summary>
    public required IReadOnlyList<SimulationPendingWalOperationSnapshot> PendingWalOperations { get; init; }

    /// <summary>Timers waiting to fire.</summary>
    public required IReadOnlyList<SimulationPendingTimerSnapshot> PendingTimers { get; init; }

    /// <summary>Scheduler queue depths keyed by queue name.</summary>
    public required IReadOnlyDictionary<string, int> SchedulerQueueDepths { get; init; }

    /// <summary>Outstanding client proposals and their observed results.</summary>
    public required IReadOnlyList<SimulationClientProposalSnapshot> ClientProposals { get; init; }

    /// <summary>Deterministic content hash of this snapshot for replay logs.</summary>
    public string ComputeContentHash()
    {
        byte[] payload = Encoding.UTF8.GetBytes(ToJson(includeHash: false));
        byte[] hash = SHA256.HashData(payload);
        return Convert.ToHexString(hash).ToLowerInvariant();
    }

    /// <summary>Serializes the snapshot to JSON for debugging and replay metadata.</summary>
    public string ToJson(bool includeHash = true)
    {
        SnapshotDocument document = new()
        {
            LogicalTick = LogicalTick,
            StepNumber = StepNumber,
            Nodes = Nodes,
            PartitionLeaders = PartitionLeaders,
            PendingNetworkMessages = PendingNetworkMessages,
            PendingWalOperations = PendingWalOperations,
            PendingTimers = PendingTimers,
            SchedulerQueueDepths = SchedulerQueueDepths,
            ClientProposals = ClientProposals,
            ContentHash = includeHash ? ComputeContentHash() : null,
        };

        return JsonSerializer.Serialize(document, JsonOptions);
    }

    /// <summary>Deserializes a snapshot previously written with <see cref="ToJson"/>.</summary>
    public static SimulationSnapshot FromJson(string json)
    {
        SnapshotDocument? document = JsonSerializer.Deserialize<SnapshotDocument>(json, JsonOptions)
            ?? throw new JsonException("Snapshot JSON did not deserialize to a document.");

        return new SimulationSnapshot
        {
            LogicalTick = document.LogicalTick,
            StepNumber = document.StepNumber,
            Nodes = document.Nodes,
            PartitionLeaders = document.PartitionLeaders,
            PendingNetworkMessages = document.PendingNetworkMessages,
            PendingWalOperations = document.PendingWalOperations,
            PendingTimers = document.PendingTimers,
            SchedulerQueueDepths = document.SchedulerQueueDepths,
            ClientProposals = document.ClientProposals,
        };
    }

    /// <summary>Creates an empty snapshot for a freshly configured runtime.</summary>
    public static SimulationSnapshot Empty(long logicalTick = 0, int stepNumber = 0) =>
        new()
        {
            LogicalTick = logicalTick,
            StepNumber = stepNumber,
            Nodes = [],
            PartitionLeaders = new Dictionary<int, SimulationPartitionLeaderSnapshot>(),
            PendingNetworkMessages = [],
            PendingWalOperations = [],
            PendingTimers = [],
            SchedulerQueueDepths = new Dictionary<string, int>(),
            ClientProposals = [],
        };

    private sealed record SnapshotDocument
    {
        public long LogicalTick { get; init; }
        public int StepNumber { get; init; }
        public IReadOnlyList<SimulationNodeSnapshot> Nodes { get; init; } = [];
        public IReadOnlyDictionary<int, SimulationPartitionLeaderSnapshot> PartitionLeaders { get; init; }
            = new Dictionary<int, SimulationPartitionLeaderSnapshot>();
        public IReadOnlyList<SimulationPendingMessageSnapshot> PendingNetworkMessages { get; init; } = [];
        public IReadOnlyList<SimulationPendingWalOperationSnapshot> PendingWalOperations { get; init; } = [];
        public IReadOnlyList<SimulationPendingTimerSnapshot> PendingTimers { get; init; } = [];
        public IReadOnlyDictionary<string, int> SchedulerQueueDepths { get; init; }
            = new Dictionary<string, int>();
        public IReadOnlyList<SimulationClientProposalSnapshot> ClientProposals { get; init; } = [];
        public string? ContentHash { get; init; }
    }
}

/// <summary>Per-node state visible to invariant checks.</summary>
public sealed record SimulationNodeSnapshot
{
    public required int NodeId { get; init; }
    public required SimulationNodeLifecycleStatus LifecycleStatus { get; init; }
    public required long CurrentTerm { get; init; }
    public string? VotedFor { get; init; }
    public string? KnownLeader { get; init; }
    public required IReadOnlyList<SimulationWalLogSummary> CommittedLogs { get; init; }
    public required IReadOnlyList<SimulationWalLogSummary> ProposedLogs { get; init; }
    public required IReadOnlyList<SimulationWalLogSummary> RolledBackLogs { get; init; }

    public static SimulationNodeSnapshot Initial(int nodeId) =>
        new()
        {
            NodeId = nodeId,
            LifecycleStatus = SimulationNodeLifecycleStatus.Stopped,
            CurrentTerm = 0,
            CommittedLogs = [],
            ProposedLogs = [],
            RolledBackLogs = [],
        };
}

/// <summary>Leader identity for a partition at a point in time.</summary>
public sealed record SimulationPartitionLeaderSnapshot
{
    public required int PartitionId { get; init; }
    public required int LeaderNodeId { get; init; }
    public required long Term { get; init; }
}

/// <summary>Compact WAL log entry summary for invariant checks.</summary>
public sealed record SimulationWalLogSummary
{
    public required int PartitionId { get; init; }
    public required long LogId { get; init; }
    public required long Term { get; init; }
    public required string LogType { get; init; }
    public string? PayloadHash { get; init; }
}

/// <summary>Network message waiting in the simulated transport.</summary>
public sealed record SimulationPendingMessageSnapshot
{
    public required long MessageId { get; init; }
    public required string FromNode { get; init; }
    public required string ToNode { get; init; }
    public required string MessageType { get; init; }
    public long ScheduledDeliveryTime { get; init; }
}

/// <summary>WAL operation waiting for scheduler completion.</summary>
public sealed record SimulationPendingWalOperationSnapshot
{
    public required long OperationId { get; init; }
    public required int NodeId { get; init; }
    public required int PartitionId { get; init; }
    public required string OperationKind { get; init; }
    public long ScheduledCompletionTime { get; init; }
}

/// <summary>Timer waiting to fire at a future logical time.</summary>
public sealed record SimulationPendingTimerSnapshot
{
    public required long TimerId { get; init; }
    public required int NodeId { get; init; }
    public required string TimerKind { get; init; }
    public required long FiresAtLogicalTime { get; init; }
}

/// <summary>Client proposal state observed by the simulation runtime.</summary>
public sealed record SimulationClientProposalSnapshot
{
    public required long ProposalId { get; init; }
    public required int PartitionId { get; init; }
    public required string PayloadHash { get; init; }
    public string? ObservedResult { get; init; }
}
