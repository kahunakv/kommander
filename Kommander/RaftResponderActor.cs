
using Nixie;
using Kommander.Communication;
using Kommander.Data;
using Kommander.Support.Parallelization;

namespace Kommander;

public sealed class RaftResponderActor : IActorAggregate<RaftResponderRequest>
{
    private readonly RaftManager manager;

    private readonly ICommunication communication;

    private readonly ILogger<IRaft> logger;

    private readonly RaftNode node;
    
    public RaftResponderActor(
        IActorAggregateContext<RaftResponderActor, RaftResponderRequest> _,
        RaftManager manager, 
        RaftNode node,
        ICommunication communication,
        ILogger<IRaft> logger
    )
    {
        this.manager = manager;
        this.node = node;
        this.communication = communication;
        this.logger = logger;
    }

    public async Task Receive(List<RaftResponderRequest> messages)
    {
        try
        {
            List<BatchRequestsRequestItem> request = [];
            
            foreach (RaftResponderRequest message in messages)
            {
                switch (message.Type)
                {
                    case RaftResponderRequestType.Handshake:
                        request.Add(new() { Type = BatchRequestsRequestType.Handshake, Handshake = message.HandshakeRequest });
                        break;
                    
                    case RaftResponderRequestType.Vote:
                        request.Add(new() { Type = BatchRequestsRequestType.Vote, Vote = message.VoteRequest });
                        break;
                    
                    case RaftResponderRequestType.RequestVotes:
                        request.Add(new() { Type = BatchRequestsRequestType.RequestVote, RequestVotes = message.RequestVotesRequest });
                        break;
                    
                    case RaftResponderRequestType.AppendLogs:
                        request.Add(new() { Type = BatchRequestsRequestType.AppendLogs, AppendLogs = message.AppendLogsRequest });
                        break;
                    
                    case RaftResponderRequestType.CompleteAppendLogs:
                        request.Add(new() { Type = BatchRequestsRequestType.CompleteAppendLogs, CompleteAppendLogs = message.CompleteAppendLogsRequest });
                        break;
                    
                    case RaftResponderRequestType.TryBatch:
                    default:
                        throw new NotImplementedException();
                }
            }
            
            if (request.Count > 10)
                logger.LogDebug("Sending block of {Count} messages", request.Count);
            
            _ = communication.BatchRequests(manager, node, new() { Requests = request }).ConfigureAwait(false);

            await Task.Delay(1);
        }
        catch (Exception ex)
        {
            logger.LogError("Exception {Type}", ex.Message);
        }
    }

    private static bool AreAllAppendLogs(List<RaftResponderRequest> messages)
    {
        foreach (RaftResponderRequest message in messages)
        {
            if (message.Type != RaftResponderRequestType.AppendLogs)
                return false;
        }

        return true;
    }
    
    private static bool AreAllCompleteLogs(List<RaftResponderRequest> messages)
    {
        foreach (RaftResponderRequest message in messages)
        {
            if (message.Type != RaftResponderRequestType.CompleteAppendLogs)
                return false;
        }

        return true;
    }

    private async Task Handshake(RaftResponderRequest message)
    {
        if (message.Node is null)
            return;
        
        if (message.HandshakeRequest is null)
            return;
                
        await communication.Handshake(manager, message.Node, message.HandshakeRequest).ConfigureAwait(false);
    }
    
    private async Task Vote(RaftResponderRequest message)
    {
        if (message.Node is null)
            return;
        
        if (message.VoteRequest is null)
            return;
                
        await communication.Vote(manager, message.Node, message.VoteRequest).ConfigureAwait(false);
    }

    private async Task RequestVotes(RaftResponderRequest message)
    {
        if (message.Node is null)
            return;

        if (message.RequestVotesRequest is null)
            return;

        await communication.RequestVotes(manager, message.Node, message.RequestVotesRequest).ConfigureAwait(false);
    }

    private async Task AppendLogs(RaftResponderRequest message)
    {
        if (message.Node is null)
            return;
        
        if (message.AppendLogsRequest is null)
            return;
        
        await communication.AppendLogs(manager, message.Node, message.AppendLogsRequest).ConfigureAwait(false);
    }
    
    private async Task CompleteAppendLogs(RaftResponderRequest message)
    {
        if (message.Node is null)
            return;
        
        if (message.CompleteAppendLogsRequest is null)
            return;
        
        await communication.CompleteAppendLogs(manager, message.Node, message.CompleteAppendLogsRequest).ConfigureAwait(false);
    }
}