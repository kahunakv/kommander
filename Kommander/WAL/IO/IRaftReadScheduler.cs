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
}
