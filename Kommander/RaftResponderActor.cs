
using Nixie;
using Kommander.Communication;
using Kommander.Data;

namespace Kommander;

public sealed class RaftResponderActor : IActorAggregate<RaftResponderRequest>
{
    private const int MinExpectedBatchSize = 10;
    
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
            if (messages.Count == 1)
            {
                RaftResponderRequest message = messages.First();
                
                switch (message.Type)
                {
                    case RaftResponderRequestType.AppendLogs:
                        await AppendLogs(message).ConfigureAwait(false);
                        break;

                    case RaftResponderRequestType.CompleteAppendLogs:
                        await CompleteAppendLogs(message).ConfigureAwait(false);
                        break;

                    case RaftResponderRequestType.Vote:
                        await Vote(message).ConfigureAwait(false);
                        break;

                    case RaftResponderRequestType.RequestVotes:
                        await RequestVotes(message).ConfigureAwait(false);
                        break;

                    case RaftResponderRequestType.Handshake:
                        await Handshake(message).ConfigureAwait(false);
                        break;

                    case RaftResponderRequestType.TryBatch:
                    default:
                        logger.LogError("Unsupported message {Type}", message.Type);
                        break;
                }
                
                await Task.Delay(1);
                return;
            }
            
            logger.LogDebug("Sending block of {Count} messages", messages.Count);
            
            List<BatchRequestsRequestItem> request = new(messages.Count);
            
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
                        logger.LogError("Unsupported message {Type}", message.Type);
                        break;
                }
            }
            
            await communication.BatchRequests(manager, node, new() { Requests = request }).ConfigureAwait(false);

            if (request.Count < MinExpectedBatchSize)
                await Task.Delay(1); // force a big batch next time*/
        }
        catch (Exception ex)
        {
            logger.LogError("Exception {Type} {Message}\n{StackTrace}", ex.GetType().Name, ex.Message, ex.StackTrace);
        }
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