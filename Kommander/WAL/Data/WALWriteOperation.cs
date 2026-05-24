using Kommander.Data;
using Kommander.Time;
using Nixie;

namespace Kommander.WAL.Data;

public sealed class WALWriteOperation
{
    public IActorRefAggregate<RaftStateActor, RaftRequest, RaftResponse> Actor { get; }
    
    public long OperationId { get; }

    public WALWriteOperationType Type { get; }

    public (int, List<RaftLog>) Logs { get; }

    public HLCTimestamp Timestamp { get; }

    public string? Endpoint { get; }

    public long Term { get; }

    public bool AutoCommit { get; }

    public long LogIndex { get; }
    
    public WALWriteOperation(
        IActorRefAggregate<RaftStateActor, RaftRequest, RaftResponse> actor,
        long operationId,
        WALWriteOperationType type,
        (int, List<RaftLog>) logs,
        HLCTimestamp timestamp = default,
        string? endpoint = null,
        long term = -1,
        bool autoCommit = false,
        long logIndex = -1
    )
    {
        Actor = actor;
        OperationId = operationId;
        Type = type;
        Logs = logs;
        Timestamp = timestamp;
        Endpoint = endpoint;
        Term = term;
        AutoCommit = autoCommit;
        LogIndex = logIndex;
    }
}
