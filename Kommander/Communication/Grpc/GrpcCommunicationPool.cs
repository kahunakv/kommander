
using System.Collections.Concurrent;
using Kommander.Data;

namespace Kommander.Communication.Grpc;

/// <summary>
/// Object pool for the reusable gRPC streaming request messages.
///
/// <para><b>Why shared and bounded (not <c>[ThreadStatic]</c>):</b> these objects are rented and returned
/// across an <c>await</c> — a caller rents on its thread, awaits the stream write/semaphore, and the return
/// runs on the continuation thread. A per-thread pool therefore leaks the returned object into whatever
/// thread happened to run the continuation while the renting thread's pool starves, and (in the coalescing
/// path) the flusher that returns a batch of drained objects is frequently a different thread than the one
/// that rented them. A single process-wide <see cref="ConcurrentQueue{T}"/> per type gives genuine
/// cross-thread reuse after the await. It is also <b>bounded</b>: the previous thread-static stacks pushed
/// unconditionally with no cap, so every thread that ever sent retained its own high-water mark of messages
/// for the life of the thread. Returns beyond <see cref="MaxRetained"/> are dropped to the GC.</para>
///
/// <para>Ownership is unchanged from before: a rented object is owned by its operation until returned, and in
/// the coalescing path a non-flusher transfers ownership to the pending queue and must not return its own
/// object (the flusher returns all drained objects). Only the pool's backing storage changed.</para>
/// </summary>
public static class GrpcCommunicationPool
{
    /// <summary>Maximum objects retained per pool. Bounds steady-state memory; excess returns are dropped.</summary>
    private const int MaxRetained = 512;

    private static readonly ConcurrentQueue<GrpcAppendLogsRequest> _appendLogsPool = new();
    private static int _appendLogsCount;

    private static readonly ConcurrentQueue<GrpcCompleteAppendLogsRequest> _completeAppendLogsPool = new();
    private static int _completeAppendLogsCount;

    private static readonly ConcurrentQueue<List<BatchRequestsRequestItem>> _batchRequestsItemPool = new();
    private static int _batchRequestsItemCount;

    public static GrpcAppendLogsRequest RentAppendLogsRequest()
    {
        if (_appendLogsPool.TryDequeue(out GrpcAppendLogsRequest? rented))
        {
            Interlocked.Decrement(ref _appendLogsCount);
            return rented;
        }

        return new();
    }

    public static void Return(GrpcAppendLogsRequest obj)
    {
        obj.Logs.Clear();
        obj.PrevLogIndex = 0;
        obj.PrevLogTerm = 0;
        obj.Quiesce = false;

        if (Interlocked.Increment(ref _appendLogsCount) <= MaxRetained)
        {
            _appendLogsPool.Enqueue(obj);
            return;
        }

        // Over the cap — undo the reservation and let this instance be collected.
        Interlocked.Decrement(ref _appendLogsCount);
    }

    public static GrpcCompleteAppendLogsRequest RentCompleteAppendLogsRequest()
    {
        if (_completeAppendLogsPool.TryDequeue(out GrpcCompleteAppendLogsRequest? rented))
        {
            Interlocked.Decrement(ref _completeAppendLogsCount);
            return rented;
        }

        return new();
    }

    public static void Return(GrpcCompleteAppendLogsRequest obj)
    {
        if (Interlocked.Increment(ref _completeAppendLogsCount) <= MaxRetained)
        {
            _completeAppendLogsPool.Enqueue(obj);
            return;
        }

        Interlocked.Decrement(ref _completeAppendLogsCount);
    }

    public static List<BatchRequestsRequestItem> RentListBatchRequestsRequestItem(int capacity)
    {
        if (_batchRequestsItemPool.TryDequeue(out List<BatchRequestsRequestItem>? rented))
        {
            Interlocked.Decrement(ref _batchRequestsItemCount);
            return rented;
        }

        return new(capacity);
    }

    public static void Return(List<BatchRequestsRequestItem> obj)
    {
        obj.Clear();

        if (Interlocked.Increment(ref _batchRequestsItemCount) <= MaxRetained)
        {
            _batchRequestsItemPool.Enqueue(obj);
            return;
        }

        Interlocked.Decrement(ref _batchRequestsItemCount);
    }
}
