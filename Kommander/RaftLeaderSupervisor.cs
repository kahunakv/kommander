
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
    }

    public Task Receive(RaftLeaderSupervisorRequest message)
    {
        manager.SystemPartition?.CheckLeader();
        
        foreach ((int _, RaftPartition? partition) in manager.Partitions)
            partition.CheckLeader();
        
        return Task.CompletedTask;
    }
}