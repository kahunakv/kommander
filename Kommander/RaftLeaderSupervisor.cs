
using Kommander.Data;
using Microsoft.Extensions.Logging;
using Nixie;

namespace Kommander;

/// <summary>
/// The RaftLeaderSupervisor class is responsible for supervising the leader election process
/// in a distributed consensus system based on the Raft protocol. It ensures periodic checks
/// for leader states and updates Raft partitions accordingly.
/// </summary>
/// <remarks>
/// This class implements an actor-based model for handling requests related to leader election
/// and node updates. 
/// </remarks>
public sealed class RaftLeaderSupervisor : IActor<RaftLeaderSupervisorRequest>
{
    private readonly IActorContext<RaftLeaderSupervisor, RaftLeaderSupervisorRequest> context;

    private readonly RaftManager manager;

    private readonly ILogger<IRaft> logger;
    
    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="context"></param>
    /// <param name="manager"></param>
    /// <param name="partition"></param>
    /// <param name="walAdapter"></param>
    /// <param name="communication"></param>
    public RaftLeaderSupervisor(IActorContext<RaftLeaderSupervisor, RaftLeaderSupervisorRequest> context, RaftManager manager, ILogger<IRaft> logger)
    {
        this.context = context;
        this.manager = manager;
        this.logger = logger;
        
        context.ActorSystem.StartPeriodicTimer(
            context.Self,
            "check-election",
            new(RaftLeaderSupervisorRequestType.CheckLeader),
            TimeSpan.FromMilliseconds(2500),
            manager.Configuration.CheckLeaderInterval
        );
        
        context.ActorSystem.StartPeriodicTimer(
            context.Self,
            "update-nodes",
            new(RaftLeaderSupervisorRequestType.UpdateNodes),
            TimeSpan.FromMilliseconds(2500),
            manager.Configuration.UpdateNodesInterval
        );
    }

    /// <summary>
    /// Handles incoming messages of type RaftLeaderSupervisorRequest and processes
    /// the request based on its specified type.
    /// </summary>
    /// <param name="message">The message containing the type of operation to be handled by the supervisor.</param>
    /// <returns>A task representing the asynchronous operation.</exception>
    /// <exception cref="RaftException">Thrown when an unrecognized or unsupported RaftLeaderSupervisorRequestType is provided.</exception>
    public async Task Receive(RaftLeaderSupervisorRequest message)
    {
        try
        {
            switch (message.Type)
            {
                case RaftLeaderSupervisorRequestType.CheckLeader:
                    await CheckLeader();
                    break;

                case RaftLeaderSupervisorRequestType.UpdateNodes:
                    await UpdateNodes();
                    break;

                default:
                    throw new RaftException("Unknown RaftLeaderSupervisorRequestType");
            }
        }
        catch (Exception ex)
        {
            logger.LogError("RaftLeaderSupervisor: {Message}\n{StackTrace}", ex.Message, ex.StackTrace);
        }
    }

    /// <summary>
    /// Sends a leader check to each partition known by the node.
    /// This ensures that each partition within the distributed system maintains an updated leader state.
    /// </summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    private Task CheckLeader()
    {
        manager.SystemPartition?.CheckLeader();
        
        foreach ((int _, RaftPartition partition) in manager.Partitions)
            partition.CheckLeader();
        
        return Task.CompletedTask;
    }
    
    private async Task UpdateNodes()
    {
        if (manager.Joined)
            await manager.UpdateNodes();
    }
}