
using Kommander.Time;
using Kommander.WAL.Data;

namespace Kommander.Data;

public sealed class RaftRequest
{
    public RaftRequestType Type { get; }

    public long Term { get; } = -1;
    
    public long CommitIndex { get; }
    
    public HLCTimestamp Timestamp { get; }

    public string? Endpoint { get; } 

    public List<RaftLog>? Logs { get; }
    
    public RaftOperationStatus Status { get; }
    
    public bool AutoCommit { get; } 

    public WALWriteOperation? WalOperation { get; }

    /// <summary>
    /// Full WAL completion envelope.  Set when <see cref="Type"/> is
    /// <see cref="RaftRequestType.WriteOperationCompleted"/>.
    /// Carries partition, term, and log-range fencing metadata that
    /// <see cref="RaftPartitionStateMachine.CompleteWalOperationAsync"/> validates
    /// before processing the completion.
    /// </summary>
    public RaftWalCompletion? WalCompletion { get; }

    public RaftRequest(
        RaftRequestType type, 
        long term = -1, 
        long commitIndex = 0, 
        HLCTimestamp timestamp = default, 
        string? endpoint = null, 
        RaftOperationStatus status = RaftOperationStatus.Success, 
        List<RaftLog>? logs = null
    )
    {
        Type = type;
        Term = term;
        CommitIndex = commitIndex;
        Timestamp = timestamp;
        Endpoint = endpoint;
        Status = status;
        Logs = logs;
    }

    public RaftRequest(RaftRequestType type, List<RaftLog> logs, bool autoCommit)
    {
        Type = type;
        Logs = logs;
        AutoCommit = autoCommit;
    }
    
    public RaftRequest(RaftRequestType type, HLCTimestamp timestamp, bool autoCommit)
    {
        Type = type;
        Timestamp = timestamp;
        AutoCommit = autoCommit;
    }

    public RaftRequest(RaftRequestType type, WALWriteOperation walOperation, RaftOperationStatus status)
    {
        Type = type;
        WalOperation = walOperation;
        Status = status;
        Timestamp = walOperation.Timestamp;
        Endpoint = walOperation.Endpoint;
        Term = walOperation.Term;
        AutoCommit = walOperation.AutoCommit;
        CommitIndex = walOperation.LogIndex;
        Logs = walOperation.Logs.Item2;
    }

    /// <summary>
    /// Constructs a <see cref="RaftRequestType.WriteOperationCompleted"/> request that
    /// carries the full <see cref="RaftWalCompletion"/> envelope so that the partition
    /// executor can validate partition, term, and log-range fencing metadata before
    /// advancing state.
    /// </summary>
    public RaftRequest(RaftRequestType type, RaftWalCompletion completion)
    {
        Type = type;
        WalCompletion = completion;
        WalOperation = completion.Operation;
        Status = completion.Status;
        if (completion.Operation is { } op)
        {
            Timestamp = op.Timestamp;
            Endpoint = op.Endpoint;
            Term = op.Term;
            AutoCommit = op.AutoCommit;
            CommitIndex = op.LogIndex;
            Logs = op.Logs.Item2;
        }
    }
}
