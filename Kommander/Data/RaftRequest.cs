
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

    /// <summary>
    /// When true the carried <see cref="RaftRequestType.RequestVote"/> /
    /// <see cref="RaftRequestType.ReceiveVote"/> is a side-effect-free pre-election probe
    /// (Raft §9.6); the state machine answers/tallies it without persisting term/vote state.
    /// </summary>
    public bool PreVote { get; }

    /// <summary>
    /// When non-zero, the executor rejects the proposal with
    /// <see cref="RaftOperationStatus.PartitionMoved"/> if the partition's
    /// committed <c>Generation</c> no longer matches this value.
    /// Zero means "no fence" — identical to the previous behavior.
    /// </summary>
    public long ExpectedGeneration { get; }

    /// <summary>
    /// Log index of the entry immediately preceding the first entry in <see cref="Logs"/>.
    /// Zero when the batch starts from the beginning of the log.
    /// Carried on <see cref="RaftRequestType.AppendLogs"/> messages so the follower can
    /// enforce the Log Matching Property before writing.
    /// </summary>
    public long PrevLogIndex { get; }

    /// <summary>Term of the entry at <see cref="PrevLogIndex"/>. Zero when <see cref="PrevLogIndex"/> is zero.</summary>
    public long PrevLogTerm { get; }

    /// <summary>
    /// Mirrors <see cref="AppendLogsRequest.Quiesce"/>: the leader's signal that this is
    /// the final AppendLogs before heartbeat suppression, telling the follower to switch to
    /// SWIM-based election gating.
    /// </summary>
    public bool Quiesce { get; }

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
        List<RaftLog>? logs = null,
        bool preVote = false,
        long prevLogIndex = 0,
        long prevLogTerm = 0,
        bool quiesce = false
    )
    {
        Type = type;
        Term = term;
        CommitIndex = commitIndex;
        Timestamp = timestamp;
        Endpoint = endpoint;
        Status = status;
        Logs = logs;
        PreVote = preVote;
        PrevLogIndex = prevLogIndex;
        PrevLogTerm = prevLogTerm;
        Quiesce = quiesce;
    }

    public RaftRequest(RaftRequestType type, List<RaftLog> logs, bool autoCommit, long expectedGeneration = 0)
    {
        Type = type;
        Logs = logs;
        AutoCommit = autoCommit;
        ExpectedGeneration = expectedGeneration;
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
