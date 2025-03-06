
// ReSharper disable ParameterHidesMember
// ReSharper disable once InconsistentlySynchronizedField

namespace Kommander;

/// <summary>
/// 
/// </summary>
public class RaftProposalQuorum
{
    private int completionCount;
    
    private TaskCompletionSource<bool>? tcs;
    
    private Timer? timer;
    
    private int expectedMinimumCompletions;
    
    private readonly object _lock = new();

    /// <summary>
    /// Gets the task that will complete when at least two SetCompleted calls have been made,
    /// or will fault after 5 seconds if fewer than two completions occur.
    /// </summary>
    public Task Task {
        get
        {
            if (tcs is null)
                throw new InvalidOperationException();
            return tcs.Task;
        }
    }

    /// <summary>
    /// Whether the quorum has been reached.
    /// </summary>
    public bool GotQuorum => completionCount >= expectedMinimumCompletions;

    /// <summary>
    /// Indicates one completion. Once this method has been called at least two times,
    /// the Task property will be marked as completed.
    /// </summary>
    public void SetCompleted()
    {
        if (tcs is null || timer is null) 
            return;
        
        // Safely increment the completion counter.
        int count = Interlocked.Increment(ref completionCount);

        // If we have reached or exceeded 2 completions, complete the task.
        if (count >= expectedMinimumCompletions)
        {
            // Ensure that we complete the task only once.
            lock (_lock)
            {
                if (!tcs.Task.IsCompleted)
                {
                    tcs.TrySetResult(true);
                    timer.Dispose();
                }
            }
        }
    }

    /// <summary>
    /// Restarts the quorum, setting the expected minimum completions and timeout.
    /// </summary>
    /// <param name="expectedMinimumCompletions"></param>
    /// <param name="timeout"></param>
    public void Restart(int expectedMinimumCompletions, int timeout)
    {
        this.expectedMinimumCompletions = expectedMinimumCompletions;
        
        lock (_lock)
        {
            completionCount = 0;
            tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

            timer?.Dispose();

            timer = new(TimeoutCallback, null, TimeSpan.FromSeconds(timeout), Timeout.InfiniteTimeSpan);
        }
    }

    private void TimeoutCallback(object? state)
    {
        if (tcs is null || timer is null) 
            return;
        
        // When the timer fires, check if we have reached the required completions.
        if (Interlocked.CompareExchange(ref completionCount, 0, 0) < 2)
        {
            // If not, fault the task with a TimeoutException.
            lock (_lock)
            {
                if (!tcs.Task.IsCompleted)
                    tcs.TrySetResult(false);
            }
        }
        
        // Dispose timer resources (if not already disposed).
        timer.Dispose();
    }
}