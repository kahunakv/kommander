
using System.Collections.Concurrent;
using System.Diagnostics;
using Kommander.Data;
using Microsoft.Extensions.Logging;

namespace Kommander;

/// <summary>
/// The RaftBatcher class is responsible for batching and managing the delivery of messages
/// to the Raft actors system. It provides an asynchronous mechanism to enqueue and process
/// messages for Raft operations.
/// </summary>
internal sealed class RaftBatcher
{
    private readonly RaftManager manager;

    /// <summary>
    /// Represents a thread-safe queue used to store pending RaftBatcherItem objects for asynchronous processing.
    /// Utilized within the RaftBatcher to manage messages enqueued for delivery to the Raft actor system.
    /// The queue ensures proper batching and sequencing of Raft operations while supporting concurrent access.
    /// This variable is central to the message delivery mechanism of the RaftBatcher.
    /// </summary>
    private readonly ConcurrentQueue<RaftBatcherItem> inbox = new();
    
    private readonly List<RaftBatcherItem> messages = [];

    /// <summary>
    /// Represents the state of processing for the inbox queue of the RaftBatcher.
    /// Used as a flag to indicate whether the message delivery mechanism is active or idle.
    /// A value of 1 typically indicates that processing is idle, while a value of 0 indicates that it is active.
    /// This variable is updated atomically using interlocked operations to ensure thread safety
    /// during concurrent message processing.
    /// </summary>
    private int processing = 1;

    private readonly Stopwatch stopwatch = new();

    /// <summary>
    /// Constructor
    /// </summary>
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

    /// <summary>
    /// Processes a batch of Raft batcher requests by writing their associated logs to the write-ahead log (WAL)
    /// and resolving or rejecting their respective promises based on the operation's success.
    /// </summary>
    /// <param name="requests">A list of <see cref="RaftBatcherItem"/> objects containing the requests to process
    /// and their associated promises.</param>
    /// <returns>A <see cref="Task"/> that represents the asynchronous operation.</returns>
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