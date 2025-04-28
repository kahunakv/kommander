
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

    public static void Return(RaftProposalQuorum obj)
    {
        obj.Clear();
        _pool ??= new();
        _pool.Push(obj);
    }
}