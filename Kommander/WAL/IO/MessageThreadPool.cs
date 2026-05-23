
using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;
using Kommander.WAL.Data;

namespace Kommander.WAL.IO;

public class MessageThreadPool : IDisposable
{
    private readonly IWAL walAdapter;
    
    private readonly ILogger<IRaft> logger;

    /// <summary>
    /// Indicates whether the thread pool has started.
    /// </summary>
    private bool started;

    /// <summary>
    /// An array of task queues. One queue per worker thread.
    /// </summary>
    private BlockingCollection<WALWriteOperation>[]? taskQueues;

    /// <summary>
    /// Number of worker threads in the pool.
    /// </summary>
    private readonly int workerCount;

    /// <summary>
    /// Cancellation support.
    /// </summary>
    private readonly CancellationTokenSource cancellationTokenSource;

    /// <summary>
    /// Threads for processing tasks.
    /// </summary>
    private readonly Thread[] workerThreads;    

    public MessageThreadPool(IWAL walAdapter, ILogger<IRaft> logger, int workerCount)
    {
        this.walAdapter = walAdapter;
        this.logger = logger;
        this.workerCount = workerCount;
        this.cancellationTokenSource = new();
        this.workerThreads = new Thread[this.workerCount];

        // Create one BlockingCollection per worker thread.
        this.taskQueues = new BlockingCollection<WALWriteOperation>[workerCount];
        
        for (int i = 0; i < workerCount; i++)        
            taskQueues[i] = new();        
    }

    /// <summary>
    /// Enqueues a task for execution using least busy scheduling on thread-specific queues.
    /// </summary>
    /// <param name="actor"></param>
    /// <param name="syncOperation"></param>
    /// <exception cref="RaftException"></exception>
    public void EnqueueTask(WALWriteOperation operation)
    {
        if (!started)
            throw new RaftException("Thread pool not started");

        if (taskQueues is null)
            throw new RaftException("Thread pool is disposed");

        // Create a TaskCompletionSource to observe the result asynchronously.
        //TaskCompletionSource<T> tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

        // Find the queue with the fewest tasks.
        int minIndex = 0;
        int minCount = taskQueues[0].Count;  // This is thread-safe snapshot.

        if (minCount > 0)
        {
            for (int i = 1; i < workerCount; i++)
            {
                int currentCount = taskQueues[i].Count;
                if (currentCount < minCount)
                {
                    minCount = currentCount;
                    minIndex = i;

                    if (currentCount == 0) // the queue is empty
                        break;
                }
            }
        }

        // Enqueue the task into the least busy queue.
        BlockingCollection<WALWriteOperation> selectedQueue = taskQueues[minIndex];               

        // Enqueue the task on the selected queue.
        /*selectedQueue.Add(() =>
        {
            try
            {
                // Execute the synchronous operation.
                //T result = syncOperation();
                RaftOperationStatus t = syncOperation();
                
                actor.Send(new(
                    RaftRequestType.WriteOperationCompleted,
                    id
                ));
                
                //tcs.TrySetResult(result);
            }
            catch (Exception ex)
            {
                Console.WriteLine("{0}", ex.Message);
                //tcs.TrySetException(ex);
            }
        });*/

        //return tcs.Task;
    }

    /// <summary>
    /// Starts the worker threads. Each thread processes tasks from its own dedicated queue.
    /// </summary>
    public void Start()
    {
        if (started)
            return;

        started = true;

        for (int i = 0; i < workerCount; i++)
        {
            int workerId = i;

            // Each thread processes its own task queue.
            workerThreads[i] = new(() =>
            {
                // Each thread gets its own queue by index.
                BlockingCollection<WALWriteOperation>? workerQueue = taskQueues?[workerId];
                if (workerQueue is null)
                {
                    logger.LogTrace("Worker {WorkerId} has no assigned queue and is stopping.", workerId);
                    return;
                }

                try
                {
                    // Process tasks until cancellation is signaled.
                    foreach (WALWriteOperation task in workerQueue.GetConsumingEnumerable(cancellationTokenSource.Token))
                    {
                        try
                        {
                            //task();
                            walAdapter.Write([task.Logs]);
                        }
                        catch (OperationCanceledException)
                        {
                            break;
                        }
                        catch (Exception ex)
                        {
                            // Log any unexpected exceptions.
                            logger.LogError("Worker {WorkerId} encountered an error: {Exception}", workerId, ex.Message);
                        }
                    }
                }
                catch (OperationCanceledException ex)
                {
                    logger.LogTrace("Worker {WorkerId} stopped: {Exception}", workerId, ex.Message);
                }
            })
            {
                IsBackground = true,
                Name = $"ReadThreadPool-{workerId}"
            };

            workerThreads[i].Start();
        }
    }

    /// <summary>
    /// Stops the thread pool by cancelling tasks and disposing the queues.
    /// </summary>
    public void Stop()
    {
        // Signal cancellation.
        cancellationTokenSource.Cancel();

        // Wait for all worker threads to finish.
        foreach (Thread thread in workerThreads)
            thread.Join();

        // Dispose of each per-thread queue.
        if (taskQueues != null)
        {
            foreach (BlockingCollection<WALWriteOperation> queue in taskQueues)
                queue.Dispose();
            
            taskQueues = null;
        }
    }

    public void Dispose()
    {
        GC.SuppressFinalize(this);
        
        cancellationTokenSource.Dispose();
        
        if (taskQueues != null)
        {
            foreach (BlockingCollection<WALWriteOperation> queue in taskQueues)            
                queue.Dispose();
            
            taskQueues = null;
        }
    }
}