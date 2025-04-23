
using System.Collections.Concurrent;
using System.Diagnostics;
using Kommander.Data;

namespace Kommander;

internal sealed class RaftBatcher
{
    private readonly RaftManager manager;
    
    private readonly ConcurrentQueue<RaftBatcherItem> inbox = new();
    
    private readonly List<RaftBatcherItem> messages = [];
    
    private int processing = 1;
    
    private readonly Stopwatch stopwatch = new();

    public RaftBatcher(RaftManager manager)
    {
        this.manager = manager;
    }
    
    /// <summary>
    /// Enqueues a message to the actor and tries to deliver it.
    /// The request/response type actors use an object to assign the response once completed. 
    /// </summary>
    /// <param name="message"></param>
    /// <param name="sender"></param>
    /// <param name="parentReply"></param>
    /// <returns></returns>
    public Task<RaftOperationStatus> Enqueue((int, List<RaftLog>) message)
    {
        TaskCompletionSource<RaftOperationStatus> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        RaftBatcherItem raftBatcherItem = new(message, promise);

        inbox.Enqueue(raftBatcherItem);

        if (1 == Interlocked.Exchange(ref processing, 0))
            _ = DeliverMessages();

        return promise.Task;
    }

    /// <summary>
    /// It retrieves a message from the inbox and invokes the actor by passing one message 
    /// at a time until the pending message list is cleared.
    /// </summary>
    /// <returns></returns>
    private async Task DeliverMessages()
    {
        try
        {
            do
            {
                do
                {
                    while (inbox.TryDequeue(out RaftBatcherItem? message))
                        messages.Add(message);

                    if (messages.Count > 0)
                    {
                        await Receive(messages);

                        messages.Clear();
                    }

                } while (!inbox.IsEmpty);
                
            } while (Interlocked.CompareExchange(ref processing, 1, 0) != 0);
        }
        catch (Exception ex)
        {
            manager.Logger.LogError("[{Endpoint}] {Exception}: {Message}\n{StackTrace}", manager.LocalEndpoint, ex.GetType().Name, ex.Message, ex.StackTrace);
        }
    }

    private async Task Receive(List<RaftBatcherItem> requests)
    {
        int count = 0;
        
        stopwatch.Restart();
        
        List<(int, List<RaftLog>)> logs = new(requests.Count);

        foreach (RaftBatcherItem request in requests)
        {
            logs.Add(request.Request);
            count += request.Request.Item2.Count;
        }

        RaftOperationStatus response = await manager.WriteThreadPool.EnqueueTask(() => manager.WalAdapter.Write(logs));
        
        manager.Logger.LogDebug("[{Endpoint}] Write of {Batch} took {Elapsed}ms", manager.LocalEndpoint, count, stopwatch.ElapsedMilliseconds);

        if (response == RaftOperationStatus.Success)
        {
            foreach (RaftBatcherItem request in requests)
                request.Promise.TrySetResult(response);

            return;
        }
        
        Exception ex = new("Write failed");
        
        foreach (RaftBatcherItem request in requests)
            request.Promise.TrySetException(ex);
    }
}