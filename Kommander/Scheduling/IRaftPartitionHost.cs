
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

    /// <summary>
    /// Returns true if <paramref name="endpoint"/> is a committed member of the roster in <b>any</b> role
    /// (voter or learner). Always returns true when no roster has been seeded yet (pre-seed fallback).
    /// <para>
    /// Used to fence inbound leadership-perturbing RPCs (AppendLogs leader adoption, StepDownNotice,
    /// TransferLeadership, Handshake) against endpoints that are reachable through the transport but were
    /// never admitted to membership: such an endpoint must not be able to churn an established partition's
    /// leadership. Deliberately role-agnostic (unlike <see cref="IsVoter"/>) so a follower whose roster
    /// briefly lags a member's promotion/demotion does not reject a legitimate peer. Defaults to
    /// <see langword="true"/> so unit-test hosts that predate membership are unaffected; only
    /// <c>RaftManager</c>'s adapter consults the real roster.
    /// </para>
    /// </summary>
    bool IsMember(string endpoint) => true;

    /// <summary>
    /// Returns the serialized system-configuration snapshot (<c>RaftSystemCheckpointSnapshot</c>)
    /// to embed in a P0 checkpoint proposal, or <c>null</c> when no configuration exists yet.
    /// Only consulted for the system partition. Carrying the roster and partition map inside the
    /// checkpoint is what keeps them durable across WAL compaction: replay starts at the last
    /// checkpoint, so config records below it would otherwise be unrecoverable after restart.
    /// Defaults to <see langword="null"/> (payload-free checkpoint) so unit-test hosts that
    /// predate membership are unaffected; only <c>RaftManager</c>'s adapter consults the real
    /// coordinator state.
    /// </summary>
    byte[]? GetSystemCheckpointPayload() => null;

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

    /// <summary>
    /// True once this node has completed at least one node-discovery pass (the first
    /// <c>UpdateNodes</c>), so an empty <see cref="Nodes"/> can be trusted to mean "genuinely a
    /// single-node cluster" rather than "peers not loaded yet". A partition must not take the
    /// single-node self-election fast-path (<c>Nodes.Count == 0</c> ⇒ become leader) while this is
    /// <see langword="false"/>: a seed-joining node starts with an empty <see cref="Nodes"/> until
    /// discovery runs, and self-electing in that window makes it usurp the existing cluster's P0
    /// leadership with an empty log — the join then deadlocks because the usurper has no partition
    /// map to serve. Defaults to <see langword="true"/> so unit-test hosts (which wire a fixed node
    /// set up front) are unaffected; only <c>RaftManager</c> gates on real discovery.
    /// </summary>
    bool InitialNodesDiscovered => true;

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
    /// Whether commit-acknowledgement observations should be emitted. False in production (the default), so
    /// the commit path builds and forwards no observation list; a test attaches a subscriber to enable it.
    /// </summary>
    bool CommitAckObservationEnabled => false;

    /// <summary>
    /// Reports the acknowledgements that carried a proposal to commit quorum (one per acknowledging node,
    /// including the leader). Default no-op; only invoked when <see cref="CommitAckObservationEnabled"/> is
    /// true. Used by the chaos harness to feed its live quorum-discipline invariant.
    /// </summary>
    void ObserveCommitAcks(IReadOnlyList<Data.RaftCommitAckObservation> acks) { }

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
