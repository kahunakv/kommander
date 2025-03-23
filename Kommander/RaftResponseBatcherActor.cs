
using Kommander.Communication;
using Nixie;
using Kommander.Data;

namespace Kommander;

public sealed class RaftResponseBatcherActor : IActor<RaftResponseBatcherRequest>
{
    private readonly RaftManager manager;

    private readonly ICommunication communication;

    private readonly ILogger<IRaft> logger;
    
    private readonly Queue<AppendLogsRequest> requests = new();
    
    private readonly Dictionary<string, List<AppendLogsRequest>> batchedRequests = new();
    
    public RaftResponseBatcherActor(
        IActorContext<RaftResponseBatcherActor, RaftResponseBatcherRequest> context,
        RaftManager manager, 
        ICommunication communication,
        ILogger<IRaft> logger
    )
    {
        this.manager = manager;
        this.communication = communication;
        this.logger = logger;
        
        context.ActorSystem.StartPeriodicTimer(
            context.Self,
            "try-batch",
            new(RaftResponderRequestType.TryBatch),
            TimeSpan.FromMilliseconds(10),
            TimeSpan.FromMilliseconds(10)
        );
    }

    public async Task Receive(RaftResponseBatcherRequest message)
    {
        switch (message.Type)
        {
            case RaftResponderRequestType.AppendLogs:
                EnqueueAppendLogs(message);
                break;
            
            case RaftResponderRequestType.TryBatch:
                await TryBatch();
                break;

            case RaftResponderRequestType.Handshake:
            case RaftResponderRequestType.Vote:
            case RaftResponderRequestType.RequestVotes:
            case RaftResponderRequestType.CompleteAppendLogs:
            default:
                logger.LogError("Unknown RaftResponderRequestType: {Type}", message.Type);
                break;
        }
    }

    private async Task TryBatch()
    {
        if (requests.Count == 0)
            return;

        while (requests.TryDequeue(out AppendLogsRequest? request))
        {
            if (batchedRequests.TryGetValue(request.Endpoint, out List<AppendLogsRequest>? batch))
                batch.Add(request);
            else
                batchedRequests.Add(request.Endpoint, [request]);
        }
        
        List<Task> tasks = [];

        foreach (KeyValuePair<string, List<AppendLogsRequest>> batch in batchedRequests)
        {
            logger.LogDebug("Sending AppendLogs request to {Endpoint} {Count}", batch.Key, batch.Value.Count);

            tasks.Add(communication.AppendLogsBatch(manager, new(batch.Key), new() { AppendLogs = batch.Value }));
        }
        
        await Task.WhenAll(tasks).ConfigureAwait(false);

        foreach (KeyValuePair<string, List<AppendLogsRequest>> batch in batchedRequests)
            batch.Value.Clear();
    }

    private void EnqueueAppendLogs(RaftResponseBatcherRequest message)
    {
        if (message.Node is null)
            return;
        
        if (message.AppendLogsRequest is null)
            return;
        
        requests.Enqueue(message.AppendLogsRequest);
    }
}