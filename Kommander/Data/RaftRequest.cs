
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

    public StepDownNoticeRequest? StepDownNotice { get; }

    public TransferLeadershipRequest? TransferLeadership { get; }
    
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

    /// <summary>
    /// Raw log entries loaded from the WAL during the restore phase.
    /// Set when <see cref="Type"/> is <see cref="RaftRequestType.RestoreLogsLoaded"/>.
    /// Replayed on the executor thread by
    /// <see cref="RaftPartitionStateMachine.CompleteRestoreAsync"/>.
    /// </summary>
    public IReadOnlyList<RaftLog>? RestoredLogs { get; }

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

    public RaftRequest(RaftRequestType type, StepDownNoticeRequest stepDownNotice)
    {
        Type = type;
        StepDownNotice = stepDownNotice;
        Term = stepDownNotice.Term;
        Timestamp = stepDownNotice.Time;
        Endpoint = stepDownNotice.Endpoint;
    }

    public RaftRequest(RaftRequestType type, TransferLeadershipRequest transferLeadership)
    {
        Type = type;
        TransferLeadership = transferLeadership;
        Term = transferLeadership.Term;
        Timestamp = transferLeadership.Time;
        Endpoint = transferLeadership.Endpoint;
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
        Status = completion.Status;
    }

    /// <summary>
    /// Constructs a <see cref="RaftRequestType.RestoreLogsLoaded"/> maintenance request
    /// that carries the raw WAL logs loaded during Phase 1 of the nonblocking restore.
    /// The executor replays these on its own thread via
    /// <see cref="RaftPartitionStateMachine.CompleteRestoreAsync"/>.
    /// </summary>
    public RaftRequest(RaftRequestType type, IReadOnlyList<RaftLog> restoredLogs)
    {
        Type = type;
        RestoredLogs = restoredLogs;
    }
}
