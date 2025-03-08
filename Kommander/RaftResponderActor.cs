
using Nixie;
using Kommander.Communication;
using Kommander.Data;

namespace Kommander;

public class RaftResponderActor : IActor<RaftResponderRequest>
{
    private readonly RaftManager manager;
    
    private readonly RaftPartition partition;

    private readonly ICommunication communication;

    private readonly ILogger<IRaft> logger;

    private readonly IActorRefStruct<RaftStateActor, RaftRequest, RaftResponse> raftStateActor;
    
    public RaftResponderActor(
        IActorContext<RaftResponderActor, RaftResponderRequest> context,
        IActorRefStruct<RaftStateActor, RaftRequest, RaftResponse> raftStateActor,
        RaftManager manager, 
        RaftPartition partition,
        ICommunication communication,
        ILogger<IRaft> logger
    )
    {
        this.raftStateActor = raftStateActor;
        this.manager = manager;
        this.partition = partition;
        this.communication = communication;
        this.logger = logger;
    }

    public async Task Receive(RaftResponderRequest message)
    {
        switch (message.X)
        {
            case RaftResponderRequestType.RequestVotes:
                if (message.RequestVotesRequest is null)
                    return;
                
                List<Task> tasks = new(manager.Nodes.Count);

                foreach (RaftNode node in manager.Nodes)
                {
                    if (node.Endpoint == manager.LocalEndpoint)
                        throw new RaftException("Corrupted nodes");
            
                    logger.LogInformation("[{LocalEndpoint}/{PartitionId}] Asked {Endpoint} for votes on Term={CurrentTerm}", manager.LocalEndpoint, partition.PartitionId, node.Endpoint, currentTerm);
            
                    tasks.Add(communication.RequestVotes(manager, partition, node, message.RequestVotesRequest));
                }
        
                await Task.WhenAny(tasks).ConfigureAwait(false);
        }
    }
}