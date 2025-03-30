
using System.Collections.Concurrent;

namespace Kommander.WAL.IO;

public class ThreadPool
{
    private readonly ILogger<IRaft> logger;

    /// <summary>
    /// Whether the thread pool has started
    /// </summary>
    private bool started = false;
    
    /// <summary>
    /// Thread-safe queue to store tasks
    /// </summary>
    private readonly BlockingCollection<Action> taskQueue;
    
    // Number of worker threads in the pool
    private readonly int workerCount;
    
    // Cancellation support
    private readonly CancellationTokenSource cancellationTokenSource;
    
    // Threads for processing tasks
    private readonly Thread[] workerThreads;

    public ThreadPool(ILogger<IRaft> logger, int workerCount)
    {
        this.logger = logger;
        this.workerCount = workerCount;
        taskQueue = new();
        cancellationTokenSource = new();
        workerThreads = new Thread[this.workerCount];
    }

    /// <summary>
    /// Allows to enqueue a read operation on the thread poool
    /// </summary>
    /// <param name="syncOperation"></param>
    /// <typeparam name="T"></typeparam>
    /// <returns></returns>
    public Task<T> EnqueueTask<T>(Func<T> syncOperation)
    {
        // Create a TaskCompletionSource to provide async notification
        TaskCompletionSource<T> tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

        // Enqueue the task
        taskQueue.Add(() =>
        {
            try
            {
                // Execute the synchronous operation
                T result = syncOperation();
                
                // Set the result on the task completion source
                tcs.TrySetResult(result);
            }
            catch (Exception ex)
            {
                // Handle any exceptions
                tcs.TrySetException(ex);
            }
        });

        return tcs.Task;
    }

    /// <summary>
    /// Creates the worker threads and starts processing tasks
    /// </summary>
    public void Start()
    {
        if (started)
            return;

        started = true;
        
        for (int i = 0; i < workerCount; i++)
        {
            int workerId = i;
            
            workerThreads[i] = new(() =>
            {
                try
                {
                    foreach (Action task in taskQueue.GetConsumingEnumerable(cancellationTokenSource.Token))
                    {
                        try
                        {
                            // Execute the task
                            task();
                        }
                        catch (OperationCanceledException)
                        {
                            break;
                        }
                        catch (Exception ex)
                        {
                            // Optional: Log or handle unexpected exceptions
                            logger.LogError("Worker {WorkerId} encountered an error: {Exception}", workerId, ex.Message);
                        }
                    }
                }
                catch (OperationCanceledException ex)
                {
                    logger.LogDebug("Worker {WorkerId} stopped: {Exception}", workerId, ex.Message);
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
    /// Stops the thread pool
    /// </summary>
    public void Stop()
    {
        // Signal cancellation
        cancellationTokenSource.Cancel();
        
        // Wait for all threads to complete
        foreach (Thread thread in workerThreads)
            thread.Join();

        // Complete the task queue
        taskQueue.Dispose();
    }
}