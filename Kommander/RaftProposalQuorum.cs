
using Kommander.Time;

namespace Kommander.Data;

/// <summary>
/// Represents a quorum for a Raft proposal, managing state and node completions
/// associated with the proposal during a distributed consensus operation.
/// </summary>
public sealed class RaftProposalQuorum
{
    private readonly Dictionary<string, bool> nodes = [];

    private bool completed;
    
    public RaftProposalState State { get; private set; }

    public List<string> Nodes => nodes.Keys.ToList();

    public List<RaftLog> Logs { get; }
    
    public long LastLogIndex => Logs.Last().Id;

    public bool AutoCommit { get; }
    
    public HLCTimestamp StartTimestamp { get; }

    public RaftProposalQuorum(List<RaftLog> logs, bool autoCommit, HLCTimestamp startTimestamp)
    {
        State = RaftProposalState.Incomplete;
        
        Logs = logs;
        AutoCommit = autoCommit;
        StartTimestamp = startTimestamp;
    }

    /// <summary>
    /// Adds a node to the expected completions for the quorum. This method sets up the node's
    /// state to indicate that it has not yet completed the proposal, ensuring it participates
    /// in the quorum.
    /// </summary>
    /// <param name="nodeId">The unique identifier of the node to be added to the expected completions.</param>
    public void AddExpectedNodeCompletion(string nodeId)
    {
        nodes.Add(nodeId, false);
    }

    /// <summary>
    /// Marks the specified node as having completed its participation in the proposal.
    /// This updates the node's state to indicate that the proposal is complete for that node.
    /// </summary>
    /// <param name="nodeId">The unique identifier of the node to be marked as completed.</param>
    public void MarkNodeCompleted(string nodeId)
    {
        nodes[nodeId] = true;
    }

    /// <summary>
    /// Sets the state of the Raft proposal within the quorum. This method updates the state
    /// of the current proposal to reflect its progress or final outcome in the Raft consensus process.
    /// </summary>
    /// <param name="state">The new state to apply to the Raft proposal. This state indicates the
    /// current or final status of the proposal, such as Incomplete, Completed, Committed, or RolledBack.</param>
    public void SetState(RaftProposalState state)
    {
        State = state;
    }

    /// <summary>
    /// Determines if the quorum has been achieved by evaluating the number of completed nodes
    /// against the required majority. Once the quorum is reached, the proposal is marked as completed.
    /// </summary>
    /// <returns>
    /// A boolean value indicating whether the quorum has been successfully achieved.
    /// </returns>
    public bool HasQuorum()
    {
        if (completed)
            return true;
        
        int quorum = Math.Max(2, (int)Math.Floor((nodes.Count + 1) / 2f));
        if (nodes.Values.Count(x => x) >= quorum)
        {
            completed = true;
            return true;
        }

        return false;
    }
}