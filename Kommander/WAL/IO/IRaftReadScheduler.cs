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
}
