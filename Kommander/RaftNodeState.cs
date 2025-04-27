
namespace Kommander;

/// <summary>
/// Represents the state of a node in the Raft consensus algorithm.
/// </summary>
public enum RaftNodeState
{
    /// <summary>
    /// Represents a state in which the node acts as a passive participant within the Raft consensus algorithm.
    /// The Follower state is the default state for all nodes and is responsible for receiving log entries
    /// and responding to requests from the Leader node. A node in the Follower state does not initiate
    /// log entry replication or leadership election but instead relies on periodic heartbeats from the Leader
    /// to maintain synchronization.
    /// </summary>
    Follower = 0,

    /// <summary>
    /// Represents a state in which the node actively participates in the leadership election process
    /// within the Raft consensus algorithm. A node transitions to the Candidate state when it does not
    /// receive a heartbeat from a Leader or does not recognize a current Leader. While in this state, the
    /// node increments its term, requests votes from other nodes, and may transition to Leader if it obtains
    /// a majority of votes.
    /// </summary>
    Candidate = 1,

    /// <summary>
    /// Represents a state in which the node assumes the role of the Leader in the Raft consensus algorithm.
    /// The Leader is responsible for managing and coordinating the cluster by handling client requests,
    /// replicating log entries to followers, and ensuring data consistency. A node enters the Leader state
    /// after winning a leadership election and periodically sends heartbeats to maintain authority.
    /// </summary>
    Leader = 2
}
