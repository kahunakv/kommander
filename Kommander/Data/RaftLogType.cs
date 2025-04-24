
namespace Kommander.Data;

public enum RaftLogType
{
    /// <summary>
    /// Represents a log entry of type "Proposed" in the Raft consensus algorithm.
    /// This type indicates that the log entry has been proposed but is not yet committed.
    /// </summary>
    Proposed = 0,

    /// <summary>
    /// Represents a log entry of type "Committed" in the Raft consensus algorithm.
    /// This type indicates that the log entry has been accepted by a majority of nodes
    /// and is guaranteed to be durable and applied.
    /// </summary>
    Committed = 1,

    /// <summary>
    /// Represents a log entry of type "ProposedCheckpoint" in the Raft consensus algorithm.
    /// This type indicates that a checkpoint has been proposed, but it is not yet finalized or committed.
    /// </summary>
    ProposedCheckpoint = 2,

    /// <summary>
    /// Represents a log entry of type "CommittedCheckpoint" in the Raft consensus algorithm.
    /// This type signifies that a checkpoint has been committed, indicating a stable and durable state in the Raft log sequence.
    /// </summary>
    CommittedCheckpoint = 3,

    /// <summary>
    /// Represents a log entry of type "RolledBack" in the Raft consensus algorithm.
    /// This type indicates that the log entry has been reverted or canceled after being proposed.
    /// </summary>
    RolledBack = 4,

    /// <summary>
    /// Represents a log entry of type "RolledBackCheckpoint" in the Raft consensus algorithm.
    /// This type indicates that the log entry corresponds to a checkpoint that has been rolled back.
    /// </summary>
    RolledBackCheckpoint = 5
}
