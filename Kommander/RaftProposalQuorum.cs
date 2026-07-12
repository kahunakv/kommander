
using Kommander.Data;
using Kommander.Time;

namespace Kommander;

/// <summary>
/// Represents a quorum for a Raft proposal, managing state and node completions
/// associated with the proposal during a distributed consensus operation.
/// </summary>
public sealed class RaftProposalQuorum
{
    /// <summary>
    /// Stores the mapping of node IDs to their completion status within the Raft proposal quorum.
    /// Each node ID represents a participant in the distributed consensus process, and its associated
    /// completion status (`true` or `false`) indicates whether the node has completed its part
    /// of the proposal.
    /// </summary>
    private readonly Dictionary<string, bool> nodes = [];

    /// <summary>
    /// Running count of nodes whose completion flag is currently <c>true</c>. Maintained
    /// incrementally by <see cref="MarkNodeCompleted"/> (incremented only on a false→true
    /// transition, so duplicate acks stay idempotent) so that <see cref="HasQuorum"/> is an
    /// O(1) comparison rather than a per-call LINQ scan of the <see cref="nodes"/> values.
    /// Reset in lockstep with <see cref="nodes"/>: cleared in <see cref="Reset"/> and
    /// <see cref="Clear"/>, left untouched by <see cref="AddExpectedNodeCompletion"/> (new
    /// nodes start incomplete).
    /// </summary>
    private int completedNodeCount;

    /// <summary>
    /// Indicates whether the Raft proposal has been successfully completed
    /// by achieving a sufficient quorum of node completions. Once set to `true`,
    /// it signifies that the distributed consensus process for the current proposal
    /// has been finalized.
    /// </summary>
    private bool completed;

    /// <summary>
    /// Event-driven completion source for the write path. Callers that await
    /// <see cref="GetWaiterTask"/> are unblocked as soon as the proposal reaches a
    /// terminal state (committed, rolled-back, or invalidated by leader loss) rather
    /// than discovering it through periodic polling.
    /// <para>
    /// Created eagerly so that the task is available before any terminal transition
    /// can fire on the executor thread. Reset in <see cref="Reset"/> when the pooled
    /// instance is reused; drained in <see cref="Clear"/> before pool return to
    /// prevent leaks.
    /// </para>
    /// </summary>
    private TaskCompletionSource<(RaftProposalTicketState, long)>? _waiter;

    /// <summary>
    /// Represents the current state of the Raft proposal quorum.
    /// The state can denote whether the proposal is incomplete, completed,
    /// committed, or rolled back, based on the progression and outcome
    /// of the distributed consensus process.
    /// </summary>
    public RaftProposalState State { get; private set; }

    /// <summary>
    /// Contains the collection of Raft logs associated with the proposal within the quorum.
    /// Each log entry represents an operation or state in the consensus process, consisting of
    /// an identifier, term, timestamp, data, and other relevant metadata.
    /// </summary>
    public List<RaftLog> Logs { get; private set; }

    /// <summary>
    /// Indicates whether the logs in the current Raft proposal quorum should be automatically committed upon achieving a quorum.
    /// If set to true, the system will automatically commit the logs, bypassing any additional manual steps. This property
    /// helps streamline the log commitment process in scenarios where automatic completion is preferred.
    /// </summary>
    public bool AutoCommit { get; private set; }

    /// <summary>
    /// Indicates the hybrid logical clock (HLC) timestamp that represents the starting point
    /// of the Raft proposal quorum operation. This timestamp is used to measure the elapsed
    /// time of the quorum process and track proposal-related timing for consistency and diagnostics.
    /// </summary>
    public HLCTimestamp StartTimestamp { get; private set; }

    /// <summary>
    /// Retrieves the index of the last log entry within the proposal quorum.
    /// This index serves as a reference to the most recently added log entry
    /// maintained in the quorum, typically reflecting progress in the log replication process
    /// during distributed consensus.
    /// </summary>
    public long LastLogIndex => Logs.Last().Id;

    /// <summary>
    /// Represents a quorum for a Raft proposal. The quorum is responsible for managing
    /// proposal-specific details such as the logs associated with the proposal,
    /// whether the proposal is set to auto-commit, and its initial timestamp.
    /// </summary>
    public RaftProposalQuorum(List<RaftLog> logs, bool autoCommit, HLCTimestamp startTimestamp)
    {
        State = RaftProposalState.Incomplete;
        _waiter = new(TaskCreationOptions.RunContinuationsAsynchronously);

        Logs = logs;
        AutoCommit = autoCommit;
        StartTimestamp = startTimestamp;
    }

    /// <summary>
    /// Returns the task that completes when the proposal reaches a terminal state.
    /// The result is <see cref="RaftProposalTicketState.Committed"/> and the commit index
    /// on success, or <see cref="RaftProposalTicketState.NotFound"/> and -1 on failure
    /// (rollback, leader loss, or timeout cleanup).
    /// </summary>
    public Task<(RaftProposalTicketState, long)> GetWaiterTask() => _waiter!.Task;

    /// <summary>
    /// Completes the event-driven waiter with the given terminal result. Called from
    /// state-machine terminal transitions (<c>Committed</c>, <c>RolledBack</c>, leader loss).
    /// Safe to call multiple times — <see cref="TaskCompletionSource{T}.TrySetResult"/> is idempotent.
    /// </summary>
    public void CompleteWaiter(RaftProposalTicketState state, long commitIndex)
        => _waiter?.TrySetResult((state, commitIndex));

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
    /// Silently ignores nodes that were not registered via <see cref="AddExpectedNodeCompletion"/>
    /// (e.g. learner acks — learners receive log entries but are excluded from quorum).
    /// </summary>
    /// <param name="nodeId">The unique identifier of the node to be marked as completed.</param>
    public void MarkNodeCompleted(string nodeId)
    {
        // Single hash lookup via CollectionsMarshal: the ref lets us read the current flag and
        // flip it in place. Increment the running count only on a false→true transition so that
        // duplicate follower acks (the same node acking twice) remain idempotent.
        ref bool flag = ref global::System.Runtime.InteropServices.CollectionsMarshal.GetValueRefOrNullRef(nodes, nodeId);
        if (!global::System.Runtime.CompilerServices.Unsafe.IsNullRef(ref flag) && !flag)
        {
            flag = true;
            completedNodeCount++;
        }
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
        
        // nodes includes the local leader, so the standard Raft majority is
        // floor(N / 2) + 1 for the full cluster size. completedNodeCount is maintained
        // incrementally by MarkNodeCompleted, so this is an O(1) check rather than a scan.
        int quorum = (nodes.Count / 2) + 1;
        if (completedNodeCount >= quorum)
        {
            completed = true;
            return true;
        }

        return false;
    }

    /// <summary>
    /// Resets the Raft proposal quorum to its initial state using the provided logs, auto-commit setting,
    /// and start timestamp.
    /// </summary>
    /// <param name="logs">The collection of Raft logs to associate with the quorum.</param>
    /// <param name="autoCommit">A boolean flag indicating whether the proposal should be auto-committed.</param>
    /// <param name="startTimestamp">The timestamp marking the start of the proposal.</param>
    public void Reset(List<RaftLog> logs, bool autoCommit, HLCTimestamp startTimestamp)
    {
        // Drain any pending waiter from a previous pool use before replacing it.
        _waiter?.TrySetResult((RaftProposalTicketState.NotFound, -1));
        _waiter = new(TaskCreationOptions.RunContinuationsAsynchronously);

        State = RaftProposalState.Incomplete;
        completed = false;
        completedNodeCount = 0;
        Logs = logs;
        AutoCommit = autoCommit;
        StartTimestamp = startTimestamp;
    }

    public void Clear()
    {
        // Drain any pending waiter so awaiting callers are unblocked before the object is pooled.
        _waiter?.TrySetResult((RaftProposalTicketState.NotFound, -1));
        _waiter = null;
        Logs.Clear();
        nodes.Clear();
        completedNodeCount = 0;
    }
}
