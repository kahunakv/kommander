
using System.Collections.Concurrent;

namespace Kommander.WAL.IO;

/// <summary>
/// A ThreadPool implementation that manages multiple worker threads and distributes tasks among them
/// using thread-specific queues. Used to prevent thread starvation in the TPL managed thread pool.
///
/// Read and Write operations are scheduled on separate thread pools.
/// </summary>
public class ThreadPool : IDisposable
{
    private readonly ILogger<IRaft> logger;

    /// <summary>
    /// Indicates whether the thread pool has started.
    /// </summary>
    private bool started;

    /// <summary>
    /// An array of task queues. One queue per worker thread.
    /// </summary>
    private BlockingCollection<Action>[]? taskQueues;

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

    public ThreadPool(ILogger<IRaft> logger, int workerCount)
    {
        this.logger = logger;
        this.workerCount = workerCount;
        this.cancellationTokenSource = new();
        this.workerThreads = new Thread[this.workerCount];

        // Create one BlockingCollection per worker thread.
        this.taskQueues = new BlockingCollection<Action>[workerCount];
        
        for (int i = 0; i < workerCount; i++)        
            taskQueues[i] = new();        
    }

    /// <summary>
    /// Enqueues a task for execution using round-robin scheduling on thread-specific queues.
    /// </summary>
    /// <typeparam name="T">The result type of the task.</typeparam>
    /// <param name="syncOperation">A synchronous function to execute.</param>
    /// <returns>A Task that will complete when the operation has executed.</returns>
    public Task<T> EnqueueTask<T>(Func<T> syncOperation)
    {
        if (!started)
            throw new RaftException("Thread pool not started");

        if (taskQueues is null)
            throw new RaftException("Thread pool is disposed");

        // Create a TaskCompletionSource to observe the result asynchronously.
        TaskCompletionSource<T> tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

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
        BlockingCollection<Action> selectedQueue = taskQueues[minIndex];               

        // Enqueue the task on the selected queue.
        selectedQueue.Add(() =>
        {
            try
            {
                // Execute the synchronous operation.
                T result = syncOperation();
                tcs.TrySetResult(result);
            }
            catch (Exception ex)
            {
                tcs.TrySetException(ex);
            }
        });

        return tcs.Task;
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
                BlockingCollection<Action>? workerQueue = taskQueues?[workerId];
                if (workerQueue is null)
                {
                    logger.LogTrace("Worker {WorkerId} has no assigned queue and is stopping.", workerId);
                    return;
                }

                try
                {
                    // Process tasks until cancellation is signaled.
                    foreach (Action task in workerQueue.GetConsumingEnumerable(cancellationTokenSource.Token))
                    {
                        try
                        {
                            task();
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
            foreach (BlockingCollection<Action> queue in taskQueues)
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
            foreach (BlockingCollection<Action> queue in taskQueues)            
                queue.Dispose();
            
            taskQueues = null;
        }
    }
}
