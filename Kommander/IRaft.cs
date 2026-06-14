
using System.ComponentModel;
using Kommander.Data;
using Kommander.System;
using Kommander.WAL;
using Kommander.WAL.IO;
using Kommander.Communication;
using Kommander.Discovery;
using Kommander.Time;

namespace Kommander;

/// <summary>
/// Represents a Raft interface for distributed consensus and coordination.
/// </summary>
public interface IRaft
{
    /// <summary>
    /// Whether the node has joined the Raft cluster
    /// </summary>
    public bool Joined { get; }
    
    /// <summary>
    /// Current WAL adapter
    /// </summary>
    public IWAL WalAdapter { get; }
    
    /// <summary>
    /// Current Communication adapter
    /// </summary>
    public ICommunication Communication { get; }
    
    /// <summary>
    /// Current Discovery adapter
    /// </summary>
    public IDiscovery Discovery { get; }
    
    /// <summary>
    /// Current Raft configuration
    /// </summary>
    public RaftConfiguration Configuration { get; }

    /// <summary>
    /// Hybrid Logical Clock
    /// </summary>
    public HybridLogicalClock HybridLogicalClock { get; }
    
    /// <summary>
    /// Fair read scheduler for partition-tagged synchronous WAL reads.
    /// </summary>
    public IRaftReadScheduler ReadScheduler { get; }
    
    /// <summary>
    /// WAL write scheduler.
    /// </summary>
    public IRaftWalScheduler WalScheduler { get; }
    
    /// <summary>
    /// Whether the Raft partitions are initialized or not
    /// </summary>
    public bool IsInitialized { get; }

    /// <summary>
    /// Event when the restore process starts
    /// </summary>
    public event Action<int>? OnRestoreStarted;        

    /// <summary>
    /// Event when the restore process finishes
    /// </summary>
    public event Action<int>? OnRestoreFinished;
    
    /// <summary>
    /// Event when a replication error occurs
    /// </summary>
    public event Action<int, RaftLog>? OnReplicationError;
    
    /// <summary>
    /// Event when a replication log is restored
    /// </summary>
    public event Func<int, RaftLog, Task<bool>>? OnLogRestored;

    /// <summary>
    /// Event when a replication log is received
    /// </summary>
    public event Func<int, RaftLog, Task<bool>>? OnReplicationReceived;
    
    /// <summary>
    /// Event called when a leader is elected on certain partition
    /// </summary>
    public event Func<int, string, Task<bool>>? OnLeaderChanged;

    /// <summary>
    /// Fired every time <c>StartUserPartitions</c> applies a new partition map —
    /// after <c>ConfigReplicated</c>, <c>ConfigRestored</c>, <c>LeaderChanged</c>,
    /// or any split / merge phase transition.
    /// <para>
    /// <b>Threading:</b> the event fires on whichever thread calls
    /// <c>StartUserPartitions</c>.  In production that is always the
    /// <see cref="RaftSystemCoordinator"/>'s single-consumer loop, so handlers
    /// receive notifications serially and must not block or re-enter the
    /// coordinator.  In tests <c>StartUserPartitions</c> may be called from any
    /// thread via the <c>StartPartitionsOverride</c> hook.
    /// </para>
    /// The argument is a point-in-time snapshot; mutating it has no effect on the live map.
    /// </summary>
    public event Action<IReadOnlyList<RaftPartitionRange>>? OnPartitionMapChanged;

    /// <summary>
    /// Returns the local node's role in the committed cluster roster:
    /// <see cref="System.ClusterMemberRole.Voter"/>, <see cref="System.ClusterMemberRole.Learner"/>,
    /// <see cref="System.ClusterMemberRole.Leaving"/>, or <see cref="System.ClusterMemberRole.NotMember"/>.
    /// <para>
    /// Returns <see cref="System.ClusterMemberRole.Leaving"/> immediately when
    /// <see cref="LeaveCluster"/> has been called, even before the removal commits, so
    /// election gates suppress campaigning during the drain window.
    /// </para>
    /// <para>
    /// Returns <see cref="System.ClusterMemberRole.Voter"/> during the pre-seed transient
    /// (roster version 0) so existing behavior is preserved on greenfield clusters.
    /// </para>
    /// </summary>
    public System.ClusterMemberRole LocalRole { get; }

    /// <summary>
    /// Fired whenever the committed cluster membership roster advances to a new version —
    /// on join, leave, promotion from Learner to Voter, and SWIM-driven eviction.
    /// <para>
    /// The argument is the new <see cref="ClusterMembership"/> snapshot. <c>MembershipVersion</c>
    /// is monotonically increasing within a cluster lifetime.
    /// </para>
    /// <para>
    /// <b>Threading:</b> fires on the <see cref="RaftSystemCoordinator"/> consumer loop.
    /// Handlers must not block or re-enter the coordinator.
    /// </para>
    /// </summary>
    public event Action<System.ClusterMembership>? OnMembershipChanged;

    /// <summary>
    /// Returns a point-in-time snapshot of the current committed cluster membership.
    /// The returned object is immutable; it will not reflect future changes.
    /// </summary>
    public System.ClusterMembership GetMembership();

    /// <summary>
    /// Joins the Raft cluster
    /// </summary>
    /// <returns></returns>
    public Task JoinCluster(CancellationToken cancellationToken = default);

    /// <summary>
    /// Joins the Raft cluster by contacting the given seed endpoints directly,
    /// bypassing the discovery mechanism.  Useful when discovery is unavailable
    /// or when adding a node programmatically.
    /// </summary>
    /// <param name="seeds">One or more endpoints of existing cluster members.</param>
    /// <param name="cancellationToken"></param>
    public Task JoinCluster(IEnumerable<string> seeds, CancellationToken cancellationToken = default);

    /// <summary>
    /// Leaves the Raft cluster
    /// </summary>
    /// <param name="dispose">If true, also disposes the manager</param>
    /// <returns></returns>
    public Task LeaveCluster(bool dispose = false);

    /// <summary>
    /// Updates the active Raft cluster nodes
    /// </summary>
    /// <returns></returns>
    public Task UpdateNodes();

    /// <summary>
    /// Returns all the visible nodes by the local node
    /// </summary>
    /// <returns></returns>
    public IList<RaftNode> GetNodes();

    /// <summary>
    /// Last time the local node observed activity from the specified endpoint.
    /// For a leader, this is the last append response received from that follower.
    /// For a follower, this is the last append received from the leader.
    /// Returns <see cref="HLCTimestamp.Zero"/> if the endpoint has never been seen.
    /// </summary>
    public HLCTimestamp GetLastNodeActivity(string endpoint);

    /// <summary>
    /// Returns the non-local endpoints the local node has observed activity from
    /// within the requested window.
    /// </summary>
    public IReadOnlyList<string> GetActiveNodes(TimeSpan within);

    /// <summary>
    /// Passes the Handshake to the appropriate partition
    /// </summary>
    /// <param name="request"></param>
    public Task Handshake(HandshakeRequest request);

    /// <summary>
    /// Requests a votes from other nodes in the cluster
    /// </summary>
    /// <param name="request"></param>
    public void RequestVote(RequestVotesRequest request);

    /// <summary>
    /// Communicate a vote from a node in the cluster
    /// </summary>
    /// <param name="request"></param>
    public void Vote(VoteRequest request);

    /// <summary>
    /// Append logs from the leader in a partition
    /// </summary>
    /// <param name="request"></param>
    /// <returns></returns>
    public void AppendLogs(AppendLogsRequest request);
    
    /// <summary>
    /// Report a complete logs operation to the leader
    /// </summary>
    /// <param name="request"></param>
    /// <returns></returns>
    public void CompleteAppendLogs(CompleteAppendLogsRequest request);

    /// <summary>
    /// Replicate logs to the followers in the partition
    /// </summary>
    /// <param name="partitionId"></param>
    /// <param name="type"></param>
    /// <param name="data"></param>
    /// <param name="autoCommit"></param>
    /// <param name="cancellationToken"></param>
    /// <param name="expectedGeneration">
    /// When non-zero, the proposal is rejected with <see cref="RaftOperationStatus.PartitionMoved"/>
    /// if the partition's committed generation no longer matches this value.
    /// Zero disables the fence (default behavior).
    /// </param>
    /// <returns></returns>
    public Task<RaftReplicationResult> ReplicateLogs(int partitionId, string type, byte[] data, bool autoCommit = true, long expectedGeneration = 0, CancellationToken cancellationToken = default);

    /// <summary>
    /// Replicate logs to the followers in the partition
    /// </summary>
    /// <param name="partitionId"></param>
    /// <param name="type"></param>
    /// <param name="logs"></param>
    /// <param name="autoCommit"></param>
    /// <param name="cancellationToken"></param>
    /// <param name="expectedGeneration"></param>
    /// <returns></returns>
    public Task<RaftReplicationResult> ReplicateLogs(int partitionId, string type, IEnumerable<byte[]> logs, bool autoCommit = true, long expectedGeneration = 0, CancellationToken cancellationToken = default);

    /// <summary>
    /// Replicate a checkpoint to the followers in the partition
    /// </summary>
    /// <param name="partitionId"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public Task<RaftReplicationResult> ReplicateCheckpoint(int partitionId, CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Commit logs and notify followers in the partition
    /// </summary>
    /// <param name="partitionId"></param>
    /// <param name="ticketId"></param>
    /// <returns></returns>
    public Task<(bool success, RaftOperationStatus status, long commitLogId)> CommitLogs(int partitionId, HLCTimestamp ticketId);

    /// <summary>
    /// Rollback logs and notify followers in the partition
    /// </summary>
    /// <param name="partitionId"></param>
    /// <param name="ticketId"></param>
    /// <returns></returns>
    public Task<(bool success, RaftOperationStatus status, long commitLogId)> RollbackLogs(int partitionId, HLCTimestamp ticketId);

    /// <summary>
    /// Obtains the local endpoint
    /// </summary>
    /// <returns></returns>
    public string GetLocalEndpoint();
    
    /// <summary>
    /// Obtains the local node id
    /// </summary>
    /// <returns></returns>
    public int GetLocalNodeId();
    
    /// <summary>
    /// Obtains the local node name
    /// </summary>
    /// <returns></returns>
    public string GetLocalNodeName();

    /// <summary>
    /// Returns the last committed log index that this node's partition state machine has
    /// recorded for <paramref name="followerEndpoint"/> on <paramref name="partitionId"/>.
    /// <para>
    /// This is only meaningful when the local node is the leader of the requested partition;
    /// only leaders track per-follower committed indexes via <c>CompleteAppendLogs</c> acks.
    /// Returns <see langword="null"/> when the follower has never acked this partition or when
    /// the local node does not lead the partition.
    /// </para>
    /// <para>
    /// Intended for cluster-internal use by the promotion driver and similar lag-checking
    /// code.  Not a public API surface.
    /// </para>
    /// </summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public ValueTask<long?> GetFollowerLagAsync(int partitionId, string followerEndpoint);

    /// <summary>
    /// Checks if the local node is the leader in the given partition
    /// </summary>
    /// <param name="partitionId"></param>
    /// <returns></returns>
    public ValueTask<bool> AmILeaderQuick(int partitionId);

    /// <summary>
    /// Checks if the local node is the leader in the given partition
    /// </summary>
    /// <param name="partitionId"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public ValueTask<bool> AmILeader(int partitionId, CancellationToken cancellationToken);

    /// <summary>
    /// Waits for the local node to check/become the leader in the given partition
    /// </summary>
    /// <param name="partitionId"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public ValueTask<string> WaitForLeader(int partitionId, CancellationToken cancellationToken);

    /// <summary>
    /// Waits until the same non-empty leader endpoint has remained stable for at
    /// least the requested duration.
    /// </summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public ValueTask<string> WaitForLeaderStableAsync(
        int partitionId,
        TimeSpan minStableFor,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Test hook that asks the local node to become leader for a partition if it can
    /// satisfy the normal Raft log freshness and quorum rules.
    /// </summary>
    /// <remarks>
    /// This method is intended for deterministic tests only. It is not a production
    /// leadership-transfer API and must not be exposed through network transports.
    /// </remarks>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public Task<RaftOperationStatus> ForceLeaderForTestingAsync(
        int partitionId,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Test hook that makes the local leader voluntarily step down for a partition
    /// while keeping the node online so it can vote and replicate as a follower.
    /// </summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public Task<RaftOperationStatus> StepDownAsync(
        int partitionId,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Test hook that transfers leadership for a partition from the local leader to
    /// a specific up-to-date target endpoint.
    /// </summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public Task<RaftOperationStatus> TransferLeadershipAsync(
        int partitionId,
        string targetEndpoint,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Test hook that pauses periodic outbound heartbeats for a partition without
    /// blocking other Raft traffic.
    /// </summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public Task<RaftOperationStatus> SuspendHeartbeatsAsync(
        int partitionId,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Test hook that resumes periodic outbound heartbeats for a partition.
    /// </summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public Task<RaftOperationStatus> ResumeHeartbeatsAsync(
        int partitionId,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Creates a new partition. Leader-only. Idempotent: if the partition already exists
    /// with Active state, returns its current generation without mutating the map.
    /// Throws <see cref="RaftException"/> if <paramref name="partitionId"/> is the system
    /// partition (id 0); P0 is permanently protected and can never be created, removed,
    /// split, or merged.
    /// </summary>
    public Task<RaftPartitionLifecycleResult> CreatePartitionAsync(
        int partitionId,
        RaftRoutingMode mode = RaftRoutingMode.Unrouted,
        (int start, int end)? hashRange = null,
        CancellationToken ct = default);

    /// <summary>
    /// Removes a partition. Leader-only. Idempotent: if the partition is already in
    /// the Removed state, re-attempts WAL reclamation and returns Success.
    /// Throws <see cref="RaftException"/> if <paramref name="partitionId"/> is the system
    /// partition (id 0); P0 is permanently protected and can never be created, removed,
    /// split, or merged.
    /// </summary>
    public Task<RaftPartitionLifecycleResult> RemovePartitionAsync(
        int partitionId,
        CancellationToken ct = default);

    /// <summary>
    /// Splits <paramref name="sourcePartitionId"/> into two partitions using the
    /// two-phase (Prepare → Commit) protocol. Leader-only.
    /// <para>
    /// Pass <paramref name="targetPartitionId"/> = 0 to let the coordinator
    /// auto-assign the next available id. The <paramref name="plan"/> controls the
    /// hash boundary and routing mode; <see cref="RaftSplitPlan.HashBoundary"/> = null
    /// computes the midpoint automatically.
    /// </para>
    /// </summary>
    public Task<RaftPartitionLifecycleResult> SplitPartitionAsync(
        int sourcePartitionId,
        int targetPartitionId = 0,
        RaftSplitPlan? plan = null,
        CancellationToken ct = default);

    /// <summary>
    /// Merges two adjacent partitions into one, absorbing <paramref name="sourcePartitionId"/>
    /// into <paramref name="survivorPartitionId"/>.
    /// <para>
    /// The caller must be the leader of both partitions. The source partition enters
    /// <c>Draining</c> state in Phase 1, then is removed and its WAL deleted in Phase 2.
    /// The survivor absorbs the source's hash range.
    /// </para>
    /// </summary>
    public Task<RaftPartitionLifecycleResult> MergePartitionsAsync(
        int survivorPartitionId,
        int sourcePartitionId,
        RaftMergePlan? plan = null,
        CancellationToken ct = default);

    /// <summary>
    /// Returns the committed generation for the given partition, or 0 if it does not exist.
    /// Reads from the in-memory partition dictionary — no WAL I/O.
    /// </summary>
    public long GetPartitionGeneration(int partitionId);

    /// <summary>
    /// Returns a snapshot of the current partition map.
    /// The returned list is a copy; mutating it does not affect the manager.
    /// </summary>
    public IReadOnlyList<RaftPartitionRange> GetPartitionMap();

    /// <summary>
    /// Returns the correct partition id according to the partition key
    /// </summary>
    /// <param name="partitionKey"></param>
    /// <returns></returns>
    public int GetPartitionKey(string partitionKey);

    /// <summary>
    /// Returns the correct partition id according to a prefix key
    /// </summary>
    /// <param name="prefixPartitionKey"></param>
    /// <returns></returns>
    public int GetPrefixPartitionKey(string prefixPartitionKey);

    /// <summary>
    /// Registers an optional <see cref="IRaftStateMachineTransfer"/> implementation that the
    /// split coordinator will use as the primary data-transfer path during
    /// <see cref="SplitPartitionAsync"/>. When an implementation is registered, the coordinator
    /// calls <see cref="IRaftStateMachineTransfer.ExportRange"/> on the source partition and
    /// <see cref="IRaftStateMachineTransfer.ImportRange"/> on the target partition, then
    /// replicates a checkpoint into the target partition's log so all replicas converge.
    /// <para>
    /// If no implementation is registered (or <paramref name="transfer"/> is null), the
    /// coordinator falls back to the log-shipping path: the caller is responsible for moving
    /// data from source to target via <see cref="ReplicateLogs"/> before committing Phase 2.
    /// </para>
    /// Safe to call at any time, including before <see cref="JoinCluster"/>.
    /// The registration is not replicated — each node must register independently.
    /// </summary>
    void RegisterStateMachineTransfer(IRaftStateMachineTransfer? transfer);
}
