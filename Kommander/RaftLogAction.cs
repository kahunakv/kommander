
namespace Kommander;

/// <summary>
/// Represents the possible actions that can be applied to a Raft log during operations
/// such as proposing, committing, or rolling back changes.
/// </summary>
public enum RaftLogAction
{
    /// <summary>
    /// Represents the action of proposing a new entry to the Raft log.
    /// This action is used during the initial phase of appending a log entry,
    /// allowing cluster consensus to be reached before the entry is committed.
    /// </summary>
    Propose,

    /// <summary>
    /// Represents the action of committing a log entry in the Raft consensus mechanism.
    /// This action finalizes the entry, marking it as persisted and applied to the state machine,
    /// ensuring it is replicated and agreed upon by the majority of the cluster.
    /// </summary>
    Commit,

    /// <summary>
    /// Represents the action of rolling back changes in the Raft log.
    /// This action is typically performed to revert uncommitted or invalid log entries,
    /// ensuring the log remains consistent with the cluster's state.
    /// </summary>
    Rollback
}