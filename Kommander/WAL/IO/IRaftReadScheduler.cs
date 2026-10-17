namespace Kommander.WAL.IO;

/// <summary>
/// Abstraction for scheduling synchronous read operations (WAL reads, log-range queries,
/// compaction helpers) on dedicated background threads.
///
/// <para>
/// Reads are tagged by <paramref name="partitionId"/> so the scheduler can apply fair,
/// partition-aware dispatch — a read-heavy partition cannot starve reads or writes
/// for other partitions.  Within a single partition, reads are serialised in FIFO
/// submission order.
/// </para>
///
/// <para>
/// Because the scheduler queues work on dedicated threads, callers never block.
/// The returned <see cref="Task{T}"/> completes once the underlying synchronous
/// storage call returns.
/// </para>
/// </summary>
public interface IRaftReadScheduler
{
    /// <summary>
    /// Submits a synchronous read operation for the given partition.
    /// </summary>
    /// <typeparam name="T">The return type of the read operation.</typeparam>
    /// <param name="partitionId">The Raft partition that owns the data being read.</param>
    /// <param name="operation">
    /// A synchronous delegate that performs the storage read and returns the result.
    /// Must not block the caller (the scheduler runs it on a worker thread).
    /// </param>
    /// <returns>
    /// A <see cref="Task{T}"/> that completes when the operation has executed.
    /// If the delegate throws, the task faults with the same exception.
    /// </returns>
    /// <exception cref="InvalidOperationException">
    /// Thrown when the scheduler has not been started or has been stopped.
    /// </exception>
    /// <exception cref="ReadBackpressureExceededException">
    /// Thrown when the per-partition pending queue depth has reached its limit.
    /// </exception>
    Task<T> EnqueueTask<T>(int partitionId, Func<T> operation);

    /// <summary>
    /// State-carried variant of <see cref="EnqueueTask{T}"/> for hot paths: the caller passes its
    /// inputs through <paramref name="state"/> and a <b>static</b> (non-capturing) delegate, so
    /// the call site allocates no closure. Semantics (fairness, FIFO order, backpressure, faults)
    /// are identical to <see cref="EnqueueTask{T}"/>.
    /// <para>The default implementation forwards through a capturing closure — correct for stub
    /// schedulers; <see cref="FairReadScheduler"/> overrides it with an allocation-lean queue item.</para>
    /// </summary>
    /// <typeparam name="TState">Caller-supplied state passed to the delegate unchanged.</typeparam>
    /// <typeparam name="T">The return type of the read operation.</typeparam>
    Task<T> EnqueueTask<TState, T>(int partitionId, TState state, Func<TState, T> operation) =>
        EnqueueTask(partitionId, () => operation(state));

    /// <summary>
    /// Submits a point-read operation that the scheduler MAY coalesce with other pending
    /// operations sharing the same <paramref name="executor"/> instance into a single
    /// <see cref="IReadBatchExecutor{TArg,T}.ExecuteBatch"/> call (e.g. one RocksDB
    /// <c>MultiGet</c> for a burst of point reads).
    ///
    /// <para>
    /// Operations submitted through this method must be mutually independent — see the
    /// contract on <see cref="IReadBatchExecutor{TArg,T}"/>. Admission semantics (partition
    /// fairness, FIFO claiming, backpressure, drain-on-stop) are identical to
    /// <see cref="EnqueueTask{T}"/>.
    /// </para>
    ///
    /// <para>
    /// The default implementation performs no coalescing: it forwards to
    /// <see cref="EnqueueTask{T}"/> with a single-element batch, which is semantically
    /// equivalent. Schedulers that can batch (see <see cref="FairReadScheduler"/>) override it.
    /// </para>
    /// </summary>
    /// <typeparam name="TArg">Per-operation input (typically a storage key).</typeparam>
    /// <typeparam name="T">Per-operation result.</typeparam>
    /// <param name="partitionId">The Raft partition that owns the data being read.</param>
    /// <param name="arg">The input passed to the executor for this operation.</param>
    /// <param name="executor">
    /// Long-lived executor performing the batched read; batching groups by reference identity
    /// of this instance.
    /// </param>
    /// <returns>A <see cref="Task{T}"/> holding this operation's slot of the batch result.</returns>
    Task<T> EnqueueBatchableTask<TArg, T>(int partitionId, TArg arg, IReadBatchExecutor<TArg, T> executor)
    {
        return EnqueueTask(partitionId, () =>
        {
            T[] results = executor.ExecuteBatch([arg]);

            if (results.Length != 1)
                throw new InvalidOperationException($"IReadBatchExecutor returned {results.Length} results for 1 argument.");

            return results[0];
        });
    }

    /// <summary>
    /// <see cref="EnqueueTask{TState,T}(int,TState,Func{TState,T})"/> with an execution hint. See
    /// <see cref="ReadExecutionHint.InlineWhenIdle"/> for the caller's contract.
    /// <para>The default implementation ignores the hint and queues.</para>
    /// </summary>
    Task<T> EnqueueTask<TState, T>(int partitionId, TState state, Func<TState, T> operation, ReadExecutionHint hint) =>
        EnqueueTask(partitionId, state, operation);

    /// <summary>
    /// <see cref="EnqueueBatchableTask{TArg,T}(int,TArg,IReadBatchExecutor{TArg,T})"/> with an execution
    /// hint. An inline run executes the batch with this one argument.
    /// <para>The default implementation ignores the hint and queues.</para>
    /// </summary>
    Task<T> EnqueueBatchableTask<TArg, T>(int partitionId, TArg arg, IReadBatchExecutor<TArg, T> executor, ReadExecutionHint hint) =>
        EnqueueBatchableTask(partitionId, arg, executor);

    /// <summary>
    /// Completion-callback variant: no <see cref="Task{T}"/>, no <see cref="TaskCompletionSource{T}"/>, and
    /// no continuation queued to the thread pool. The thread that ran the operation (a scheduler worker,
    /// or the caller for an inline run) calls <paramref name="onCompleted"/> right after it, with the
    /// result, or with <c>default</c> and the exception the operation threw.
    ///
    /// <para><b>Callback contract:</b> short, non-blocking, never waits on the scheduler. It may post to a
    /// mailbox or complete a promise that the consumer owns. It runs exactly once for every operation the
    /// scheduler accepted, including the operations that <c>Stop</c> drains. A callback that throws is
    /// logged and swallowed; the other operations in the batch still complete.</para>
    ///
    /// <para>Admission failures (stopping, back-pressure) throw from this call and the callback never
    /// runs.</para>
    ///
    /// <para>The default implementation runs the callback from a continuation of the task-returning
    /// overload: correct, but it pays the pool hop that this overload exists to avoid.</para>
    /// </summary>
    void EnqueueTask<TState, T>(
        int partitionId,
        TState state,
        Func<TState, T> operation,
        Action<TState, T?, Exception?> onCompleted,
        ReadExecutionHint hint = ReadExecutionHint.Queue)
    {
        EnqueueTask(partitionId, state, operation, hint).ContinueWith(
            static (task, boxed) =>
            {
                (TState callbackState, Action<TState, T?, Exception?> callback) = ((TState, Action<TState, T?, Exception?>))boxed!;
                ReadSchedulerCallbacks.Invoke(callback, callbackState, task);
            },
            (state, onCompleted),
            CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);
    }

    /// <summary>
    /// Completion-callback variant of
    /// <see cref="EnqueueBatchableTask{TArg,T}(int,TArg,IReadBatchExecutor{TArg,T})"/>. The callback
    /// receives <paramref name="state"/>, this operation's slot of the batch result, or the exception that
    /// faulted the batch. Same callback contract as the
    /// <see cref="EnqueueTask{TState,T}(int,TState,Func{TState,T},Action{TState,T,Exception},ReadExecutionHint)"/>
    /// overload. Operations with different <typeparamref name="TState"/> still coalesce into one batch when
    /// they share the executor instance.
    /// </summary>
    void EnqueueBatchableTask<TArg, TState, T>(
        int partitionId,
        TArg arg,
        IReadBatchExecutor<TArg, T> executor,
        TState state,
        Action<TState, T?, Exception?> onCompleted,
        ReadExecutionHint hint = ReadExecutionHint.Queue)
    {
        EnqueueBatchableTask(partitionId, arg, executor, hint).ContinueWith(
            static (task, boxed) =>
            {
                (TState callbackState, Action<TState, T?, Exception?> callback) = ((TState, Action<TState, T?, Exception?>))boxed!;
                ReadSchedulerCallbacks.Invoke(callback, callbackState, task);
            },
            (state, onCompleted),
            CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);
    }
}

/// <summary>Shared completion plumbing for the default callback implementations on <see cref="IRaftReadScheduler"/>.</summary>
internal static class ReadSchedulerCallbacks
{
    /// <summary>
    /// Unwraps a completed task into the callback's (result, error) pair. A throwing callback is swallowed:
    /// the default implementation has no logger, and the exception must not fault an unobserved
    /// continuation task.
    /// </summary>
    internal static void Invoke<TState, T>(Action<TState, T?, Exception?> callback, TState state, Task<T> task)
    {
        try
        {
            if (task.IsCompletedSuccessfully)
                callback(state, task.Result, null);
            else
                callback(state, default, task.Exception?.InnerException ?? (Exception?)task.Exception ?? new TaskCanceledException(task));
        }
        catch
        {
            // Contract: a callback must not throw. See the overload docs.
        }
    }
}
