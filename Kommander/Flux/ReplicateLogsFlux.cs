using Kommander.Data;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;

namespace Kommander.Flux;

public enum ReplicateLogsFluxSteps
{
    Initialized = 0,
    Propose = 1,
    CreateProposal = 2
}

public class ReplicateLogsFlux
{
    private int step;

    private readonly IWAL walHandler;

    private readonly List<RaftLog> logs;

    private bool autoCommit;
    
    public ReplicateLogsFlux(IWAL walHandler, List<RaftLog> logs, bool autoCommit)
    {
        this.walHandler = walHandler;
        this.logs = logs;
        this.autoCommit = autoCommit;
        this.step = (int)ReplicateLogsFluxSteps.Propose;
    }
    
    public void Next()
    {
        step++;
        
        switch (step)
        {
            case (int)ReplicateLogsFluxSteps.Propose:
                Propose();
                break;
            
            case (int)ReplicateLogsFluxSteps.CreateProposal:
                CreateProposal();
                break;
            
            default:
                throw new ArgumentOutOfRangeException(nameof(step), "Invalid step");
        }
    }

    private void Propose()
    {
        foreach (RaftLog log in logs)
        {
            log.Type = RaftLogType.Proposed;
            log.Time = currentTime;
        }
        
        walHandler.Propose(currentTerm, logs).ConfigureAwait(false);
    }

    private void CreateProposal()
    {
        if (proposeResponse.Status != RaftOperationStatus.Success)
        {
            logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Couldn't save proposed logs to local persistence", manager.LocalEndpoint, partition.PartitionId, nodeState);
            
            return (RaftOperationStatus.Errored, HLCTimestamp.Zero);
        }

        RaftProposalQuorum proposalQuorum = RaftProposalQuorumPool.Rent(logs, autoCommit, currentTime); // new(logs, autoCommit, currentTime);
        
        // Mark itself as completed
        proposalQuorum.MarkNodeCompleted(manager.LocalEndpoint);

        foreach (RaftNode node in nodes)
        {
            if (node.Endpoint == manager.LocalEndpoint)
                throw new RaftException("Corrupted nodes");
            
            proposalQuorum.AddExpectedNodeCompletion(node.Endpoint);
            
            AppendLogToNode(node, currentTime, logs);
        }

        if (!activeProposals.TryAdd(currentTime, proposalQuorum))
            return (RaftOperationStatus.Errored, HLCTimestamp.Zero);
        
        if (logger.IsEnabled(LogLevel.Debug))
            logger.LogDebugProposedLogs(manager.LocalEndpoint, partition.PartitionId, nodeState, currentTime, string.Join(',', logs.Select(x => x.Id.ToString())));

        return (RaftOperationStatus.Success, currentTime);
    }
}