
using Nixie;
using Kommander.Communication;
using Kommander.Data;
using Kommander.Support;

namespace Kommander;

public sealed class RaftResponderActor : IActorAggregate<RaftResponderRequest>
{
    private readonly RaftManager manager;

    private readonly ICommunication communication;

    private readonly ILogger<IRaft> logger;
    
    public RaftResponderActor(
        IActorAggregateContext<RaftResponderActor, RaftResponderRequest> _,
        RaftManager manager, 
        ICommunication communication,
        ILogger<IRaft> logger
    )
    {
        this.manager = manager;
        this.communication = communication;
        this.logger = logger;
    }

    public async Task Receive(List<RaftResponderRequest> messages)
    {
        try
        {
            if (messages.Count > 1)
            {
                if (AreAllAppendLogs(messages))
                {
                    //Console.WriteLine("Got block of {0} append messages", messages.Count);

                    List<AppendLogsRequest> newMessages = new(messages.Count);

                    foreach (RaftResponderRequest message in messages)
                        newMessages.Add(message.AppendLogsRequest!);

                    await communication.AppendLogsBatch(manager, messages[0].Node!, new() { AppendLogs = newMessages }).ConfigureAwait(false);
                    return;
                }
                
                if (AreAllCompleteLogs(messages))
                {
                    //Console.WriteLine("Got block of {0} complete messages", messages.Count);
                
                    List<CompleteAppendLogsRequest> newMessages = new(messages.Count);

                    foreach (RaftResponderRequest message in messages)
                        newMessages.Add(message.CompleteAppendLogsRequest!);
                
                    await communication.CompleteAppendLogsBatch(manager, messages[0].Node!, new() { CompleteLogs = newMessages }).ConfigureAwait(false);
                    return;
                }
            }

            await messages.ForEachAsync(5, async message =>
            {
                switch (message.Type)
                {
                    case RaftResponderRequestType.AppendLogs:
                        await AppendLogs(message);
                        break;

                    case RaftResponderRequestType.CompleteAppendLogs:
                        await CompleteAppendLogs(message);
                        break;

                    case RaftResponderRequestType.Vote:
                        await Vote(message);
                        break;

                    case RaftResponderRequestType.RequestVotes:
                        await RequestVotes(message);
                        break;

                    case RaftResponderRequestType.Handshake:
                        await Handshake(message);
                        break;

                    case RaftResponderRequestType.TryBatch:
                    default:
                        logger.LogError("Unsupported message {Type}", message.Type);
                        break;
                }
            }).ConfigureAwait(false);
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
                
        await communication.Handshake(manager, message.Node, message.HandshakeRequest);
    }
    
    private async Task Vote(RaftResponderRequest message)
    {
        if (message.Node is null)
            return;
        
        if (message.VoteRequest is null)
            return;
                
        await communication.Vote(manager, message.Node, message.VoteRequest);
    }

    private async Task RequestVotes(RaftResponderRequest message)
    {
        if (message.Node is null)
            return;

        if (message.RequestVotesRequest is null)
            return;

        await communication.RequestVotes(manager, message.Node, message.RequestVotesRequest);
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