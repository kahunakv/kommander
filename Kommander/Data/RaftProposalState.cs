namespace Kommander.Data;

/// <summary>
/// Represents the state of a Raft proposal within the consensus process.
/// </summary>
/// <remarks>
/// The RaftProposalState enum defines the various stages or outcomes for a proposal
/// in a Raft-based consensus system. These stages guide the progression and
/// management of proposals, from their initial creation to their final resolution.
/// </remarks>
public enum RaftProposalState
{
    /// <summary>
    /// Represents a state where the Raft proposal is incomplete and has not yet reached a decision or resolution.
    /// </summary>
    /// <remarks>
    /// The Incomplete state indicates that the proposal is still in progress, and necessary steps such as quorum
    /// or consensus determination have not been finalized. It is the initial state for proposals within the
    /// Raft consensus process.
    /// </remarks>
    Incomplete,

    /// <summary>
    /// Represents a state where the Raft proposal has been successfully completed and has reached a resolution.
    /// </summary>
    /// <remarks>
    /// The Completed state signifies that the consensus process for the Raft proposal has concluded,
    /// and the necessary quorum or agreement has been achieved. This state indicates the closure of
    /// the proposal's lifecycle within the Raft consensus workflow.
    /// </remarks>
    Completed,

    /// <summary>
    /// Represents a state where the Raft proposal has been accepted and committed by the quorum.
    /// </summary>
    /// <remarks>
    /// The Committed state indicates that the proposal has successfully achieved consensus within the Raft system
    /// and the associated changes or actions have been finalized. This marks the conclusion of the proposal's journey
    /// within the Raft consensus process, ensuring durability and agreement across the nodes.
    /// </remarks>
    Committed,

    /// <summary>
    /// Represents a state where the Raft proposal was reverted and is no longer active within the consensus process.
    /// </summary>
    /// <remarks>
    /// The RolledBack state indicates that the proposal was explicitly undone and reverted, typically as a result
    /// of failing to achieve consensus or due to a conflict resolution within the system. This state marks the
    /// proposal as invalidated and prevents further progress or commitment.
    /// </remarks>
    RolledBack
}