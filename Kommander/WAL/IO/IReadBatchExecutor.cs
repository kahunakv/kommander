namespace Kommander.WAL.IO;

/// <summary>
/// Executes a group of independent point-read operations as one backend call
/// (e.g. a RocksDB <c>MultiGet</c> instead of N individual <c>Get</c>s).
///
/// <para>
/// Used with <see cref="IRaftReadScheduler.EnqueueBatchableTask{TArg,T}"/>: operations submitted
/// against the same executor <b>instance</b> may be coalesced by the scheduler's drain cycle into a
/// single <see cref="ExecuteBatch"/> call. The scheduler groups by reference identity, so an
/// executor is expected to be a long-lived singleton per backend, not allocated per call.
/// </para>
///
/// <para><b>Contract:</b></para>
/// <list type="bullet">
///   <item>Operations must be mutually independent: the scheduler chooses batch composition
///       freely (any subset of pending same-executor operations, in any order), so nothing
///       in one operation may depend on another having executed first.</item>
///   <item><see cref="ExecuteBatch"/> returns exactly <c>args.Length</c> results, aligned by
///       index with <paramref name="args"/>. A shorter or longer result array faults every
///       operation in the batch.</item>
///   <item>An exception thrown from <see cref="ExecuteBatch"/> faults every operation in the
///       batch with that same exception — equivalent to each of the N individual reads failing.
///       Per-operation failures must instead be encoded in the result type.</item>
///   <item>The call runs synchronously on a scheduler worker thread and must not block on
///       anything other than the storage I/O itself.</item>
/// </list>
/// </summary>
/// <typeparam name="TArg">Per-operation input (typically a storage key).</typeparam>
/// <typeparam name="T">Per-operation result.</typeparam>
public interface IReadBatchExecutor<TArg, T>
{
    /// <summary>
    /// Performs the batched read. <paramref name="args"/> contains between 1 and the scheduler's
    /// drain-batch limit of inputs; the result array must align index-for-index with it.
    /// </summary>
    T[] ExecuteBatch(TArg[] args);
}
