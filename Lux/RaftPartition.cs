
using Lux.Data;
using Lux.WAL;
using Nixie;

namespace Lux;

public sealed class RaftPartition
{
    private static readonly RaftRequest raftStateRequest = new(RaftRequestType.GetState);

    private readonly IActorRefStruct<RaftStateActor, RaftRequest, RaftResponse> raftActor;

    private readonly RaftManager raftManager;

    internal string Leader { get; set; } = "";

    internal int PartitionId { get; }

    public RaftPartition(ActorSystem actorSystem, RaftManager raftManager, IWAL walAdapter, int partitionId)
    {
        this.raftManager = raftManager;
        PartitionId = partitionId;

        raftActor = actorSystem.SpawnStruct<RaftStateActor, RaftRequest, RaftResponse>(
            "bra-" + partitionId, 
            raftManager, 
            this,
            walAdapter
        );
    }

    public void RequestVote(RequestVotesRequest request)
    {
        raftActor.Send(new(RaftRequestType.RequestVote, request.Term, request.Endpoint));
    }

    public void Vote(VoteRequest request)
    {
        raftActor.Send(new(RaftRequestType.ReceiveVote, request.Term, request.Endpoint));
    }

    public void AppendLogs(AppendLogsRequest request)
    {
        raftActor.Send(new(RaftRequestType.AppendLogs, request.Term, request.Endpoint, request.Logs));
    }

    public void ReplicateLogs(string message)
    {
        raftActor.Send(new(RaftRequestType.ReplicateLogs, [new() { Message = message }]));
    }

    public void ReplicateCheckpoint()
    {
        raftActor.Send(new(RaftRequestType.ReplicateCheckpoint));
    }

    public async ValueTask<NodeState> GetState()
    {
        Console.WriteLine("GetState {0} {1}", Leader, raftManager.LocalEndpoint);
        
        if (!string.IsNullOrEmpty(Leader) && Leader == raftManager.LocalEndpoint)
            return NodeState.Leader;

        RaftResponse response = await raftActor.Ask(raftStateRequest, TimeSpan.FromSeconds(5));
        
        Console.WriteLine("GetState {0} {1} {2}", Leader, raftManager.LocalEndpoint, response.Type);

        if (response.Type == RaftResponseType.None)
            throw new RaftException("Unknown response (2)");

        return response.State;
    }
}