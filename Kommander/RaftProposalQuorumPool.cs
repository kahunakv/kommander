
using Kommander.Data;
using Kommander.Time;

namespace Kommander;

/// <summary>
/// A static pool for managing and recycling instances of the RaftProposalQuorum class.
/// This facilitates reusing objects to reduce memory allocation overhead and improve performance
/// during the replication of logs in the Raft consensus algorithm.
/// </summary>
public static class RaftProposalQuorumPool
{
    [ThreadStatic]
    private static Stack<RaftProposalQuorum>? _pool;

    /// <summary>
    /// Rents a RaftProposalQuorum object from the pool or creates a new instance if the pool is empty.
    /// </summary>
    /// <param name="logs">A list of RaftLog entries to be associated with the RaftProposalQuorum instance.</param>
    /// <param name="autoCommit">A boolean value indicating whether to auto-commit the proposal.</param>
    /// <param name="startTimestamp">The HLCTimestamp indicating the start time for the quorum's operation.</param>
    /// <returns>A RaftProposalQuorum instance that is either rented from the pool or newly created.</returns>
    public static RaftProposalQuorum Rent(List<RaftLog> logs, bool autoCommit, HLCTimestamp startTimestamp)
    {
        _pool ??= new();
        if (_pool.Count > 0)
        {
            RaftProposalQuorum rented = _pool.Pop();
            rented.Reset(logs, autoCommit, startTimestamp);
            return rented;
        }
        
        return new(logs, autoCommit, startTimestamp);
    }

    /// <summary>
    /// Returns a RaftProposalQuorum instance to the pool for future reuse.
    /// This method clears the state of the RaftProposalQuorum object before returning it to the pool.
    /// </summary>
    /// <param name="obj">The RaftProposalQuorum instance to be returned to the pool.</param>
    public static void Return(RaftProposalQuorum obj)
    {
        obj.Clear();
        _pool ??= new();
        _pool.Push(obj);
    }
}