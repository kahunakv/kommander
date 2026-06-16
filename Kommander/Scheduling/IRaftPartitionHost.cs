
using Kommander.Data;
using Kommander.Gossip;
using Kommander.System;
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

    /// <summary>
    /// The local node's role in the committed membership roster.
    /// Returns <see cref="ClusterMemberRole.Voter"/> when no roster exists yet (pre-seed fallback).
    /// </summary>
    ClusterMemberRole LocalRole { get; }

    /// <summary>
    /// Returns true if <paramref name="endpoint"/> is a committed <see cref="ClusterMemberRole.Voter"/>
    /// in the current roster. Always returns true when no roster has been seeded yet (pre-seed fallback).
    /// </summary>
    bool IsVoter(string endpoint);

    RaftConfiguration Configuration { get; }

    HybridLogicalClock HybridLogicalClock { get; }

    IReadOnlyList<RaftNode> Nodes { get; }

    HLCTimestamp GetLastNodeActivity(string endpoint, int partitionId);

    HLCTimestamp GetLastNodeHearthbeat(string endpoint, int partitionId);

    void UpdateLastHeartbeat(string endpoint, int partitionId, HLCTimestamp timestamp);

    void UpdateLastNodeActivity(string endpoint, int partitionId, HLCTimestamp timestamp);

    void EnqueueResponse(string endpoint, RaftResponderRequest request);

    Task InvokeLeaderChanged(int partitionId, string leader);

    Task<bool> InvokeReplicationReceived(int partitionId, RaftLog log);

    Task<bool> InvokeSystemReplicationReceived(int partitionId, RaftLog log);

    void InvokeReplicationError(int partitionId, RaftLog log);

    /// <summary>
    /// Returns the registered <see cref="IRaftStateMachineTransfer"/> for snapshot-based catch-up,
    /// or <see langword="null"/> when none has been registered.  The state machine uses this to
    /// determine whether a compacted follower can be recovered via snapshot transfer.
    /// </summary>
    IRaftStateMachineTransfer? StateMachineTransfer { get; }

    /// <summary>
    /// Sends a snapshot to <paramref name="node"/> on behalf of the partition leader.
    /// Wraps the active <see cref="Kommander.Communication.ICommunication"/> so the state machine
    /// does not need a direct reference to the transport layer.
    /// </summary>
    Task<SnapshotResponse> SendInstallSnapshotAsync(RaftNode node, SnapshotRequest request, CancellationToken ct);

    /// <summary>
    /// SWIM failure-detector view of <paramref name="endpoint"/>. Returns
    /// <see cref="MemberLivenessState.Alive"/> for unknown endpoints (same default as
    /// <see cref="Gossip.LivenessTable.GetState"/>). This reflects the SWIM probing result,
    /// not per-partition <c>lastActivity</c> — the two signals are distinct.
    /// </summary>
    MemberLivenessState GetNodeLiveness(string endpoint);
}
