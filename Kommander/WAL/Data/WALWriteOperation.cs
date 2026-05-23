using Kommander.Data;
using Nixie;

namespace Kommander.WAL.Data;

public sealed class WALWriteOperation
{
    public IActorRefAggregate<RaftStateActor, RaftRequest, RaftResponse> Actor { get; }
    
    public (int, List<RaftLog>) Logs { get; }
    
    public WALWriteOperation(
        IActorRefAggregate<RaftStateActor, RaftRequest, RaftResponse> actor,
        (int, List<RaftLog>) logs
    )
    {
        Actor = actor;
        Logs = logs;
    }
}