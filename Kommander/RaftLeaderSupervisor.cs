
using Kommander.Data;
using Nixie;

namespace Kommander;

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