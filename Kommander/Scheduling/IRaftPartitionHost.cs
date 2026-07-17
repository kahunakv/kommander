
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

    /// <summary>
    /// A monotonically non-decreasing local timestamp in <see cref="global::System.Diagnostics.Stopwatch"/> ticks,
    /// used <b>only</b> to measure elapsed local durations for the election / heartbeat / voting / quiesce
    /// timers (B3). Unlike <see cref="HybridLogicalClock"/> it is never advanced by a remote peer's
    /// timestamp, so a follower's election timeout cannot be frozen for the duration of a leader's clock
    /// skew — the liveness bug that HLC subtraction caused. It is <b>not</b> comparable across nodes and
    /// must never be used for event ordering or log identity; HLC remains the authority there.
    /// <para>The default returns the process-wide monotonic clock; test hosts override it to drive time
    /// deterministically (e.g. to simulate real local elapsed time after a far-future heartbeat).</para>
    /// </summary>
    long GetMonotonicTimestamp() => global::System.Diagnostics.Stopwatch.GetTimestamp();

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
    /// Returns the registered <see cref="IRaftSystemStateTransfer"/> for whole-partition
    /// state snapshots on the system partition (id 0), or <see langword="null"/> when none has
    /// been registered.  The state machine checks this to decide whether a below-floor P0
    /// follower can be repaired via a full-state export rather than individual log entries.
    /// </summary>
    IRaftSystemStateTransfer? SystemStateTransfer { get; }

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
