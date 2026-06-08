
using Kommander.Data;
using Kommander.Time;

namespace Kommander.Scheduling;

/// <summary>
/// Cluster and runtime services required by <see cref="RaftPartitionStateMachine"/>.
/// Abstracts <see cref="RaftManager"/> and partition metadata so the state machine
/// has no actor dependency.
/// </summary>
public interface IRaftPartitionHost
{
    int PartitionId { get; }

    string Leader { get; set; }

    string LocalEndpoint { get; }

    int LocalNodeId { get; }

    RaftConfiguration Configuration { get; }

    HybridLogicalClock HybridLogicalClock { get; }

    IReadOnlyList<RaftNode> Nodes { get; }

    HLCTimestamp GetLastNodeActivity(string endpoint, int partitionId);

    HLCTimestamp GetLastNodeHearthbeat(string endpoint);

    void UpdateLastHeartbeat(string endpoint, HLCTimestamp timestamp);

    void UpdateLastNodeActivity(string endpoint, int partitionId, HLCTimestamp timestamp);

    void EnqueueResponse(string endpoint, RaftResponderRequest request);

    Task InvokeLeaderChanged(int partitionId, string leader);

    Task<bool> InvokeReplicationReceived(int partitionId, RaftLog log);

    Task<bool> InvokeSystemReplicationReceived(int partitionId, RaftLog log);

    void InvokeReplicationError(int partitionId, RaftLog log);
}
