
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
    DrainSentinel
}