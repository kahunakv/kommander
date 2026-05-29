using Kommander.WAL.Data;

namespace Kommander.WAL.IO;

/// <summary>
/// Abstraction for async scheduling of synchronous WAL write work.
///
/// <para>Callers submit a <see cref="WALWriteOperation"/> (a partition-tagged command with a
/// monotonic operation id) and the scheduler executes the underlying synchronous
/// RocksDB/SQLite I/O on a dedicated worker thread.  When the write completes the
/// scheduler posts a <see cref="RaftWalCompletion"/> back to the caller via the
/// <see cref="WALWriteOperation.OnComplete"/> callback.</para>
///
/// <para>Implementations must guarantee:</para>
/// <list type="bullet">
///   <item>Writes for the same partition are applied in submission order.</item>
///   <item>Fair scheduling across partitions — no single partition starves others.</item>
///   <item>Bounded internal queues with back-pressure signalling.</item>
///   <item>Graceful drain on <see cref="Stop"/>.</item>
/// </list>
/// </summary>
public interface IRaftWalScheduler
{
    /// <summary>
    /// Submits a WAL write operation for asynchronous execution.
    /// </summary>
    /// <param name="operation">
    /// The partition-tagged write command.  The scheduler calls
    /// <see cref="WALWriteOperation.OnComplete"/> exactly once when the write
    /// (or an error) has been observed.
    /// </param>
    void Enqueue(WALWriteOperation operation);
}
