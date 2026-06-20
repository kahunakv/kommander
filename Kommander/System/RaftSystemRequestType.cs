
namespace Kommander.System;

public enum RaftSystemRequestType
{
    LeaderChanged,
    RestoreCompleted,
    ConfigRestored,
    ConfigReplicated,
    SplitPartition,
    /// <summary>
    /// Phase-2 commit for a split: transitions both source and target from
    /// <see cref="RaftPartitionState.Splitting"/> to <see cref="RaftPartitionState.Active"/>.
    /// Sent by the coordinator to itself after Phase 1 succeeds (AutoCommit path)
    /// or re-enqueued on restart for crash recovery.
    /// </summary>
    SplitPartitionCommit,
    /// <summary>
    /// Phase 1 of a merge: marks the source partition as
    /// <see cref="RaftPartitionState.Draining"/>. Driven by <see cref="RaftMergePlan"/>.
    /// </summary>
    MergePartition,
    /// <summary>
    /// Phase 2 of a merge: absorbs the source range into the survivor, marks the
    /// source as <see cref="RaftPartitionState.Removed"/>, and reclaims its WAL.
    /// Sent by the coordinator to itself after Phase 1 succeeds.
    /// </summary>
    MergePartitionCommit,
    CreatePartition,
    RemovePartition,
    /// <summary>
    /// Test-only sentinel. When the loop processes this it completes the
    /// corresponding <see cref="TaskCompletionSource"/> registered via
    /// <see cref="RaftSystemCoordinator.DrainAsync"/>, letting tests wait for
    /// all previously-enqueued work to finish without a fixed delay.
    /// </summary>
    DrainSentinel,

    /// <summary>
    /// Adds a new node to the committed cluster roster as a <see cref="ClusterMemberRole.Learner"/>.
    /// Rejected if another membership change is in flight or the expected version is stale.
    /// </summary>
    AddMember,

    /// <summary>
    /// Promotes a committed <see cref="ClusterMemberRole.Learner"/> to <see cref="ClusterMemberRole.Voter"/>.
    /// The node enters quorum at the commit point of this entry.
    /// </summary>
    PromoteMember,

    /// <summary>
    /// Removes a node from the committed cluster roster (graceful leave or failure-driven eviction).
    /// Quorum shrinks at the commit point; single-server safety guarantees the remaining
    /// majority can still commit.
    /// </summary>
    RemoveMember,

    /// <summary>
    /// Applies a gossiped roster to the local membership cache when its version is
    /// strictly newer than the locally committed version.
    /// This path never writes to the Raft log — it only converges the local cache so
    /// that <c>UpdateNodes</c> and peer routing reflect committed changes that have not
    /// yet been replicated to this node via the normal append-logs path.
    /// </summary>
    ApplyGossipRoster,

    /// <summary>
    /// Delivers a <see cref="NodeLoadReport"/> received via gossip to the system coordinator.
    /// The coordinator keeps the newest <see cref="NodeLoadReport.ReportVersion"/> per endpoint
    /// in an in-memory map for use by the Phase 4 balancer controller.
    /// Never written to the Raft log.
    /// </summary>
    ApplyGossipLoadReport,

    /// <summary>
    /// Triggers one leader-balancer planning pass on the P0 leader.
    /// Reduces retained load reports into a <see cref="GlobalLeadershipView"/>, runs the
    /// two-tier planner, dispatches suggestions for each planned move, and updates the
    /// outstanding-move table.  Sent by the timer service every
    /// <see cref="RaftConfiguration.LeaderBalancerInterval"/>; silently ignored when this
    /// node is not the P0 leader or <see cref="RaftConfiguration.EnableLeaderBalancer"/> is false.
    /// </summary>
    RunBalancerPass
}