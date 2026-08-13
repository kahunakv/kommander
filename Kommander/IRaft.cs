
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
    /// Leaves the Raft cluster gracefully, committing a <c>RemoveMember(self)</c> entry so
    /// surviving nodes drop this node from their committed roster.
    /// <para>
    /// <b>Single-voter short-circuit:</b> when the committed roster contains no other
    /// <c>Voter</c> peer (standalone / embedded case), the membership round-trip is skipped
    /// entirely and the node stops immediately — no 10 s deadline spin.
    /// </para>
    /// <para>
    /// <b>Cancellation:</b> passing a <paramref name="cancellationToken"/> that is cancelled
    /// during shutdown causes the method to return promptly rather than waiting up to
    /// <c>deadlineMs</c>. The local stop sequence always runs regardless.
    /// </para>
    /// </summary>
    /// <param name="dispose">If <c>true</c>, also disposes the manager after stopping.</param>
    /// <param name="cancellationToken">
    /// Token that, when cancelled, aborts any in-progress graceful-leave attempt immediately.
    /// </param>
    public Task LeaveCluster(bool dispose = false, CancellationToken cancellationToken = default);

    /// <summary>
    /// Asks the cluster to remove this node from the committed roster and reports the outcome,
    /// <b>without</b> stopping the node.
    /// <para>
    /// Use this to decommission a running node: the caller learns whether the removal committed,
    /// was permanently refused because it would remove the last voter
    /// (<see cref="LeaveClusterOutcome.RefusedInsufficientVoters"/>), or could not be attempted, and
    /// only then decides whether to stop the process. <see cref="LeaveCluster"/> is the
    /// shutdown-coupled variant that tears the node down regardless of the result.
    /// </para>
    /// <para>
    /// The node stops campaigning once the removal commits, and not before: until then it is still
    /// a full member, and it may have to win the system-partition election to commit its own
    /// removal. A refused or failed attempt therefore leaves it participating normally.
    /// </para>
    /// <para>
    /// Idempotent: calling it again after a successful leave returns
    /// <see cref="LeaveClusterOutcome.NotAMember"/>.
    /// </para>
    /// </summary>
    /// <param name="cancellationToken">
    /// Bounds the attempt. Cancelling yields <see cref="LeaveClusterOutcome.Timeout"/>; the removal
    /// may still commit, so re-read the roster before concluding anything.
    /// </param>
    public Task<LeaveClusterResult> RequestLeaveAsync(CancellationToken cancellationToken = default);

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
    /// Replicate logs to the followers in the partition.
    /// Accepts an already-materialized list or array and avoids the intermediate copy
    /// incurred by the <see cref="IEnumerable{T}"/> overload for list and array callers.
    /// The default implementation materializes to a list and delegates; override in
    /// <see cref="RaftManager"/> for the zero-copy path.
    /// </summary>
    public Task<RaftReplicationResult> ReplicateLogs(int partitionId, string type, IReadOnlyList<byte[]> logs, bool autoCommit = true, long expectedGeneration = 0, CancellationToken cancellationToken = default)
        => ReplicateLogs(partitionId, type, (IEnumerable<byte[]>)logs, autoCommit, expectedGeneration, cancellationToken);

    /// <summary>
    /// Replicate a <b>heterogeneous, per-entry-typed</b> batch to one partition as a single proposal.
    /// <para>
    /// Where <see cref="ReplicateLogs(int,string,IReadOnlyList{byte[]},bool,long,CancellationToken)"/> forces
    /// one <c>type</c> and one <c>autoCommit</c> flag across every payload, this surface accepts entries that
    /// each carry their own <see cref="RaftProposalEntry.Type"/>, so a consumer can coalesce unrelated writes
    /// into <b>one</b> proposal — one AppendEntries round trip and one group-committed WAL flush — while still
    /// receiving an independent <see cref="RaftEntryResult"/> per entry (index-aligned to
    /// <paramref name="entries"/>).
    /// </para>
    /// <para>
    /// Slice-1 scope: the batch is <b>auto-commit only</b>. Any entry with
    /// <see cref="RaftProposalEntry.AutoCommit"/> set to <see langword="false"/>, or a batch that mixes
    /// distinct non-zero <see cref="RaftProposalEntry.ExpectedGeneration"/> values, is a batch-level rejection
    /// (<see cref="RaftBatchReplicationResult.Success"/> is <see langword="false"/> and nothing is appended).
    /// The generation fence is applied at batch granularity against the partition's committed generation.
    /// </para>
    /// </summary>
    public Task<RaftBatchReplicationResult> ReplicateEntries(int partitionId, IReadOnlyList<RaftProposalEntry> entries, CancellationToken cancellationToken = default);

    /// <summary>
    /// Replicate a checkpoint to the followers in the partition
    /// </summary>
    /// <param name="partitionId"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public Task<RaftReplicationResult> ReplicateCheckpoint(int partitionId, CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Commits logs for the given ticket and notifies followers.
    /// <para>
    /// On cancellation, returns <c>(false, <see cref="RaftOperationStatus.OperationCancelled"/>, 0)</c>
    /// rather than throwing; the caller should retry the same ticket after back-off because the
    /// queued executor work may still apply (re-committing an already-committed ticket is a no-op).
    /// </para>
    /// </summary>
    /// <param name="partitionId"></param>
    /// <param name="ticketId"></param>
    /// <param name="cancellationToken">Optional deadline; default leaves the await unbounded.</param>
    /// <returns></returns>
    public Task<(bool success, RaftOperationStatus status, long commitLogId)> CommitLogs(int partitionId, HLCTimestamp ticketId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Rolls back logs for the given ticket and notifies followers.
    /// <para>
    /// On cancellation, returns <c>(false, <see cref="RaftOperationStatus.OperationCancelled"/>, 0)</c>
    /// rather than throwing; the caller should retry the same ticket after back-off because the
    /// queued executor work may still apply (re-rolling-back an already-rolled-back ticket is a no-op).
    /// </para>
    /// </summary>
    /// <param name="partitionId"></param>
    /// <param name="ticketId"></param>
    /// <param name="cancellationToken">Optional deadline; default leaves the await unbounded.</param>
    /// <returns></returns>
    public Task<(bool success, RaftOperationStatus status, long commitLogId)> RollbackLogs(int partitionId, HLCTimestamp ticketId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Sets the minimum WAL log index that compaction must not truncate below on
    /// <paramref name="partitionId"/>, regardless of the checkpoint position.
    /// Consumers use this to protect a retained WAL tail for point-in-time recovery.
    /// <para>
    /// This call is synchronous and non-blocking: the floor is a volatile field read once per
    /// compaction pass and has no ordering relationship to the write path.
    /// </para>
    /// <para>
    /// If <paramref name="partitionId"/> is not hosted on this node the call is a no-op.
    /// </para>
    /// <para>
    /// Values &lt;= 0 are normalised to "no protection" (truncate to checkpoint). The floor is
    /// in-memory and resets to "no protection" on process restart; callers must re-assert it on
    /// every node start before relying on PITR for that node.
    /// </para>
    /// </summary>
    public void SetMinRetainIndex(int partitionId, long index);

    /// <summary>
    /// Acquires a composable retention hold on <paramref name="partitionId"/>'s WAL: committed
    /// entries are retained down to <paramref name="index"/> until the returned handle is disposed.
    /// <para>
    /// Unlike <see cref="SetMinRetainIndex"/>, which is a single last-writer-wins floor, holds
    /// compose: any number may be active concurrently and the effective retention floor is the
    /// <b>minimum</b> across all of them (∞ when none), so independent consumers — PITR horizon,
    /// backup capture, and the like — never clobber one another. <see cref="SetMinRetainIndex"/>
    /// remains an orthogonal floor composed in as the degenerate single-value case.
    /// </para>
    /// <para>
    /// The call is synchronous and thread-safe; the new floor is visible to the next compaction pass
    /// with no scheduling round-trip. If <paramref name="partitionId"/> is not hosted on this node
    /// the call is a no-op that returns a disposable which does nothing. The floor is in-memory and
    /// resets on process restart; consumers must re-assert holds on every node start before relying
    /// on PITR for that node. Disposing the handle is idempotent and releases exactly one hold.
    /// </para>
    /// </summary>
    public IDisposable AcquireRetentionHold(int partitionId, long index);

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
    /// Checks if the local node is the leader in the given partition.
    /// <para>Answered from published local state (leader endpoint, then the partition's role
    /// snapshot) without an executor round-trip, so polling this is cheap and — unlike the previous
    /// Ask-per-poll implementation — cannot starve the election and heartbeat work a caller waiting
    /// for a leader depends on. Callers should still back off between polls.</para>
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
    /// Confirms this node's leadership for the partition with a same-term quorum ack round
    /// (Raft read-index, dissertation §6.4) and waits until the local applied frontier covers the
    /// commit index observed at confirmation time. A local read served after a <see langword="true"/>
    /// result is linearizable.
    /// <para><see cref="AmILeader"/>/<see cref="AmILeaderQuick"/> report pure local belief: a
    /// minority-partitioned leader keeps believing it leads until it <i>receives</i> a higher-term
    /// message, so gating reads on them serves stale state as an authoritative success. This method
    /// is the read-side equivalent of what replication already enforces for writes — consumers
    /// should gate authoritative local reads on it and surface a retry/redirect on
    /// <see langword="false"/>.</para>
    /// <para>Returns <see langword="false"/> when the node is not the published leader (including
    /// while a promotion barrier is still armed), when quorum cannot confirm within
    /// <see cref="RaftConfiguration.LeadershipConfirmationTimeout"/>, or when the request is
    /// rejected by admission control. Concurrent callers coalesce into one in-flight ack round, and
    /// a confirmation completed within the last heartbeat interval is reused, so steady-state cost
    /// is ~one quorum round-trip per heartbeat interval regardless of read volume.</para>
    /// </summary>
    /// <param name="partitionId"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public ValueTask<bool> ConfirmLeadershipAsync(int partitionId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Follower catch-up confirmation: proves this node's <b>applied</b> state for the partition
    /// covers everything committed cluster-wide before the call began, whether or not this node
    /// is the leader. Returns <see langword="true"/> only when a leader-confirmed commit index —
    /// captured by a same-term quorum ack round started after this call — has been applied to
    /// this node's state machines (Raft dissertation §6.4, the follower read).
    /// <para><see cref="ConfirmLeadershipAsync"/> answers this question for the leader only.
    /// Consumers that act <i>destructively</i> on locally-applied replicated state from a
    /// non-leader (e.g. pruning local disk based on a replicated registry) must gate on this
    /// instead: a lagging replica sees a stale projection, and forwarding node-local maintenance
    /// to the leader is not an option. On the leader this call degenerates to
    /// <see cref="ConfirmLeadershipAsync"/>.</para>
    /// <para>Every non-success outcome — no leader known, quorum round failed on the leader,
    /// transport error, restore in progress, applied-frontier wait timeout
    /// (<see cref="RaftConfiguration.LeadershipConfirmationTimeout"/>) — returns
    /// <see langword="false"/>: the caller must skip or defer its destructive action (fail
    /// closed). No retries happen inside the primitive; the caller owns retry cadence.</para>
    /// <para>Freshness contract: entries committed <i>after</i> the call began may or may not be
    /// visible — identical to a leader-side confirmed read.</para>
    /// </summary>
    /// <param name="partitionId"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public ValueTask<bool> ConfirmLocalApplicationAsync(int partitionId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Waits for the local node to check/become the leader in the given partition
    /// </summary>
    /// <param name="partitionId"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    /// <exception cref="RaftNodeNotReadyException">
    /// The node has not completed cluster initialization, so the partition set is unknown and no
    /// leader endpoint can be resolved. Transient and retryable — typical while a restarted node
    /// is rejoining the cluster.
    /// </exception>
    /// <exception cref="RaftException">The partition id is unknown, its restore failed, or no leader could be decided in time.</exception>
    public ValueTask<string> WaitForLeader(int partitionId, CancellationToken cancellationToken);

    /// <summary>
    /// Waits until the same non-empty leader endpoint has remained stable for at
    /// least the requested duration.
    /// <para>
    /// WARNING: <paramref name="minStableFor"/> is a required stability window, not a
    /// deadline. This overload blocks until the window is satisfied or the token is
    /// cancelled — a partition whose leadership keeps churning makes it wait forever.
    /// Callers must supply a bounded <paramref name="cancellationToken"/>, or prefer the
    /// overload that takes an overall <c>timeout</c> and fails fast with
    /// <see cref="TimeoutException"/>.
    /// </para>
    /// </summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public ValueTask<string> WaitForLeaderStableAsync(
        int partitionId,
        TimeSpan minStableFor,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Waits until the same non-empty leader endpoint has remained stable for at
    /// least <paramref name="minStableFor"/>, giving up after an overall
    /// <paramref name="timeout"/>. This is the recommended overload: a churning or
    /// mis-elected partition produces a prompt, diagnosable <see cref="TimeoutException"/>
    /// instead of an unbounded hang.
    /// </summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public ValueTask<string> WaitForLeaderStableAsync(
        int partitionId,
        TimeSpan minStableFor,
        TimeSpan timeout,
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
    /// Returns the committed replica set of the given partition for consumer-side routing:
    /// which nodes host the range and each replica's role. An <b>empty list means legacy full
    /// replication</b> — every roster voter hosts the range (also returned for unknown
    /// partitions). Consumers should cache the set keyed by the range's generation and refresh
    /// on <see cref="OnPartitionMapChanged"/> or a <see cref="Data.RaftOperationStatus.PartitionMoved"/>
    /// rejection — the same fence discipline that guards split/merge cutovers.
    /// </summary>
    public IReadOnlyList<System.RaftReplica> GetPartitionReplicas(int partitionId);

    /// <summary>
    /// Returns the effective replication factor for the partition: its per-range override when
    /// set, otherwise <see cref="RaftConfiguration.ReplicationFactor"/>. 0 means full
    /// replication (every roster voter hosts the range).
    /// </summary>
    public int GetEffectiveReplicationFactor(int partitionId);

    /// <summary>
    /// Commits a per-range replication-factor override (0 clears it, inheriting the global
    /// configuration). Leader-only (P0). The change adjusts the target only — the placement
    /// controller moves replicas toward it on subsequent passes when
    /// <see cref="RaftConfiguration.EnablePlacementRebalancer"/> is on. Deliberately does not
    /// bump the range's generation: routing is unchanged until replicas actually move, so
    /// consumer fences stay valid.
    /// </summary>
    public Task<Data.RaftPartitionLifecycleResult> SetReplicationFactorAsync(
        int partitionId,
        int replicationFactor,
        CancellationToken ct = default);

    /// <summary>
    /// Returns the EWMA rate of <c>ReplicateLogs</c> operations per second for
    /// <paramref name="partitionId"/>, as measured by whichever node currently leads it.
    /// <para>
    /// <b>Path-not-label:</b> the signal is keyed on request type, not node role —
    /// only <c>ReplicateLogs</c> ops (leader-side write path) contribute; follower-replay
    /// <c>AppendLogs</c> and maintenance ops do not. A value of 0 on a follower node for a
    /// partition it does not lead is therefore normal, not an error.
    /// </para>
    /// <para>
    /// <b>Local fast-path:</b> when this node leads <paramref name="partitionId"/>, the
    /// value is read directly from the in-process executor accumulator — no gossip lag.
    /// </para>
    /// <para>
    /// <b>Remote path:</b> when a peer leads the partition, the value is read from the
    /// most-recently-received gossip load report emitted by that peer. The result may
    /// trail the true rate by up to one <see cref="RaftConfiguration.LeaderBalancerReportInterval"/>
    /// plus propagation delay.
    /// </para>
    /// <para>
    /// <b>Sentinel:</b> returns <c>0</c> when the local node does not lead the partition and
    /// no gossip report from the current leader has been received yet, or when
    /// <paramref name="partitionId"/> is unknown. A <c>0</c> return is therefore ambiguous
    /// between "genuinely idle" and "not yet seen" — callers that need to distinguish the
    /// two should gate on whether a report from the leader exists at all.
    /// </para>
    /// </summary>
    public double GetPartitionLogOpsPerSecond(int partitionId);

    /// <summary>
    /// Returns the number of WAL write operations queued for <paramref name="partitionId"/> in
    /// the shared <see cref="FairWalScheduler"/> fsync group at the time of the call. This is an
    /// advisory saturation signal: when <see cref="GetPartitionLogOpsPerSecond"/> plateaus while
    /// this value rises and stays positive, the partition is fsync-bound.
    /// <para>
    /// <b>Local fast-path:</b> when this node leads <paramref name="partitionId"/>, the value is
    /// read directly from the in-process WAL scheduler — no gossip lag.
    /// </para>
    /// <para>
    /// <b>Remote path:</b> when a peer leads the partition, the value is read from the
    /// most-recently-received gossip load report emitted by that peer. The result may trail the
    /// true depth by up to one <see cref="RaftConfiguration.LeaderBalancerReportInterval"/> plus
    /// propagation delay.
    /// </para>
    /// <para>
    /// <b>Sentinel:</b> returns <c>0</c> when the local node does not lead the partition and
    /// no gossip report from the current leader has been received yet, or when
    /// <paramref name="partitionId"/> is unknown.
    /// </para>
    /// </summary>
    public int GetPartitionWalQueueDepth(int partitionId);

    /// <summary>
    /// Returns the EWMA enqueue-to-durable commit-wait latency in milliseconds for
    /// <paramref name="partitionId"/>. This is the most direct per-partition fsync-pressure
    /// signal: it rises under WAL saturation even when <see cref="GetPartitionWalQueueDepth"/>
    /// is coarse (e.g. large batches drain the queue quickly between samples).
    /// <para>
    /// <b>Local fast-path:</b> when this node leads <paramref name="partitionId"/>, the value
    /// is read directly from the in-process WAL scheduler — no gossip lag.
    /// </para>
    /// <para>
    /// <b>Remote path:</b> when a peer leads the partition, the value is read from the
    /// most-recently-received gossip load report. The result may trail by up to one
    /// <see cref="RaftConfiguration.LeaderBalancerReportInterval"/> plus propagation delay.
    /// </para>
    /// <para>
    /// <b>Sentinel:</b> returns <c>0</c> when the local node does not lead the partition and
    /// no gossip report from the current leader has been received yet, or when
    /// <paramref name="partitionId"/> is unknown.
    /// </para>
    /// </summary>
    public double GetPartitionCommitWaitMs(int partitionId);

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

    /// <summary>
    /// Registers an optional <see cref="IRaftSystemStateTransfer"/> implementation that Kommander
    /// will use to repair a system-partition (id 0) follower that has fallen below the WAL
    /// compaction floor.  When registered, the leader calls
    /// <see cref="IRaftSystemStateTransfer.ExportPartitionState"/> and ships the blob to the
    /// lagging follower, which installs it via
    /// <see cref="IRaftSystemStateTransfer.ImportPartitionState"/> and resumes log replication
    /// from the snapshot index.
    /// <para>
    /// Distinct from <see cref="RegisterStateMachineTransfer"/>, which covers key-range splits
    /// and merges.  Both may be registered simultaneously without conflict.
    /// </para>
    /// Pass <see langword="null"/> to clear a previously registered transfer.
    /// Safe to call at any time, including before <see cref="JoinCluster"/>.
    /// The registration is not replicated — each node must register independently.
    /// </summary>
    void RegisterSystemStateTransfer(IRaftSystemStateTransfer? transfer);
}
