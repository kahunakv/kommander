
using System.Collections.Concurrent;
using Kommander.Communication.Grpc;

namespace Kommander.Tests.Communication;

/// <summary>
/// Tests for W2 — per-stream outbound <c>AppendLogs</c> coalescing.
///
/// All behavioral tests drive <see cref="GrpcCommunication.FlushCoalesced"/> directly
/// with an injected write delegate, so no live gRPC server is required. The injected
/// delegate lets each test control when a write "completes" (via
/// <see cref="TaskCompletionSource"/>) to deterministically exercise flusher/non-flusher
/// selection, the re-loop, the cap, and pool-return under exceptions.
/// </summary>
public sealed class TestAppendLogsCoalescing
{
    // ── Helpers ───────────────────────────────────────────────────────────────

    private static PendingAppendLogs MakePending(int partition = 1)
    {
        GrpcAppendLogsRequest req = new() { Partition = partition, Term = 1, Endpoint = "test" };
        GrpcBatchRequestsRequestItem item = new()
        {
            Type = GrpcBatchRequestsRequestType.AppendLogs,
            AppendLogs = req,
        };
        return new PendingAppendLogs(item, req);
    }

    /// <summary>
    /// A controllable fake writer that signals each write's start and blocks until the
    /// test completes it, so items can be injected into the queue mid-write.
    /// </summary>
    private sealed class ControlledWriter
    {
        private readonly SemaphoreSlim _writeStarted = new(0);
        private readonly ConcurrentQueue<TaskCompletionSource> _pending = new();
        public readonly List<GrpcBatchRequestsRequest> Batches = [];

        public Func<GrpcBatchRequestsRequest, Task> Write => async batch =>
        {
            lock (Batches) Batches.Add(batch);
            var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            _pending.Enqueue(tcs);
            _writeStarted.Release();
            await tcs.Task;
        };

        /// <summary>Wait until the next <c>write</c> call has started.</summary>
        public Task WaitForWriteStartAsync() => _writeStarted.WaitAsync();

        /// <summary>Unblock the oldest in-flight write.</summary>
        public void CompleteNextWrite()
        {
            if (_pending.TryDequeue(out TaskCompletionSource? tcs))
                tcs.SetResult();
        }
    }

    // ── Config defaults ───────────────────────────────────────────────────────

    [Fact]
    public void DefaultConfig_CoalescingIsDisabled() =>
        Assert.False(new RaftConfiguration().GrpcEnableAppendLogsCoalescing);

    [Fact]
    public void DefaultConfig_MaxCoalesceBatchIs256() =>
        Assert.Equal(256, new RaftConfiguration().GrpcAppendLogsMaxCoalesceBatch);

    // ── Flusher path — isolated send ─────────────────────────────────────────

    /// <summary>
    /// When the semaphore is free, the caller becomes the flusher and the single item is
    /// written immediately as a batch-of-one.  No items are left in the queue after.
    /// </summary>
    [Fact]
    public async Task Flusher_IsolatedItem_WritesSingleBatchAndEmptiesQueue()
    {
        var pending = new ConcurrentQueue<PendingAppendLogs>();
        var sem = new SemaphoreSlim(1, 1);
        var batches = new List<GrpcBatchRequestsRequest>();

        await GrpcCommunication.FlushCoalesced(
            pending, sem,
            b => { batches.Add(b); return Task.CompletedTask; },
            maxBatch: 256,
            MakePending(partition: 7));

        Assert.Single(batches);
        Assert.Single(batches[0].Requests);
        Assert.Equal(7, batches[0].Requests[0].AppendLogs.Partition);
        Assert.True(pending.IsEmpty);
        Assert.Equal(1, sem.CurrentCount); // semaphore was released
    }

    // ── Non-flusher path ──────────────────────────────────────────────────────

    /// <summary>
    /// When the semaphore is already held, the caller returns without calling write and
    /// leaves its item in the queue for the active flusher to drain.
    /// </summary>
    [Fact]
    public async Task NonFlusher_SemaphoreBusy_ReturnsWithoutWriting()
    {
        var pending = new ConcurrentQueue<PendingAppendLogs>();
        var sem = new SemaphoreSlim(1, 1);
        var batches = new List<GrpcBatchRequestsRequest>();

        sem.Wait(); // simulate an in-flight write holding the semaphore

        await GrpcCommunication.FlushCoalesced(
            pending, sem,
            b => { batches.Add(b); return Task.CompletedTask; },
            maxBatch: 256,
            MakePending(partition: 3));

        Assert.Empty(batches);          // write was NOT called
        Assert.Equal(1, pending.Count); // item is in the queue, ready for the flusher
        Assert.Equal(0, sem.CurrentCount); // still held

        sem.Release();
    }

    /// <summary>
    /// Items queued by non-flushers are all picked up when the next caller becomes the
    /// flusher (semaphore free at that point).
    /// </summary>
    [Fact]
    public async Task NonFlusher_ItemsQueuedWhileSemaphoreBusy_PickedUpByNextFlusher()
    {
        var pending = new ConcurrentQueue<PendingAppendLogs>();
        var sem = new SemaphoreSlim(1, 1);
        var batches = new List<GrpcBatchRequestsRequest>();

        sem.Wait(); // hold the semaphore

        // Three non-flusher calls: each enqueues its item and returns immediately.
        for (int i = 1; i <= 3; i++)
        {
            await GrpcCommunication.FlushCoalesced(
                pending, sem,
                b => { batches.Add(b); return Task.CompletedTask; },
                maxBatch: 256,
                MakePending(partition: i));
        }

        Assert.Empty(batches);
        Assert.Equal(3, pending.Count);

        sem.Release(); // release so the next caller becomes the flusher

        // This caller acquires the semaphore and drains all 4 items (3 queued + its own).
        await GrpcCommunication.FlushCoalesced(
            pending, sem,
            b => { batches.Add(b); return Task.CompletedTask; },
            maxBatch: 256,
            MakePending(partition: 4));

        Assert.Single(batches);
        Assert.Equal(4, batches[0].Requests.Count);
        Assert.True(pending.IsEmpty);
        Assert.Equal(1, sem.CurrentCount);
    }

    // ── Re-loop: items that arrive mid-write ──────────────────────────────────

    /// <summary>
    /// Items enqueued while a write is in flight are picked up in the flusher's re-loop
    /// and sent as a second batch — without an additional semaphore acquire cycle.
    /// </summary>
    [Fact]
    public async Task Flusher_ItemsArriveWhileWriteInFlight_PickedUpInReloop()
    {
        var pending = new ConcurrentQueue<PendingAppendLogs>();
        var sem = new SemaphoreSlim(1, 1);
        var writer = new ControlledWriter();

        // Start the flusher with item A; it will block inside fakeWrite.
        Task flushTask = GrpcCommunication.FlushCoalesced(
            pending, sem, writer.Write, maxBatch: 256, MakePending(partition: 1));

        // Wait until the flusher has started the first write.
        await writer.WaitForWriteStartAsync();

        // Enqueue B and C while the write is still in flight (semaphore held by flusher).
        pending.Enqueue(MakePending(partition: 2));
        pending.Enqueue(MakePending(partition: 3));

        // Complete the first write; the flusher will loop and drain B+C.
        writer.CompleteNextWrite();
        await writer.WaitForWriteStartAsync(); // wait for second write to start

        writer.CompleteNextWrite();
        await flushTask;

        Assert.Equal(2, writer.Batches.Count);
        Assert.Single(writer.Batches[0].Requests);        // first write: only A
        Assert.Equal(1, writer.Batches[0].Requests[0].AppendLogs.Partition);
        Assert.Equal(2, writer.Batches[1].Requests.Count); // second write: B and C
        Assert.Equal(2, writer.Batches[1].Requests[0].AppendLogs.Partition);
        Assert.Equal(3, writer.Batches[1].Requests[1].AppendLogs.Partition);
        Assert.True(pending.IsEmpty);
        Assert.Equal(1, sem.CurrentCount);
    }

    // ── Cap: deep backlog sends bounded frames ────────────────────────────────

    /// <summary>
    /// With a cap of 3 and 7 items queued, the flusher sends three frames (3 + 3 + 1).
    /// No item is lost; the do/while handles all remainder.
    /// </summary>
    [Fact]
    public async Task Flusher_DeepBacklog_SentInCappedFrames()
    {
        var pending = new ConcurrentQueue<PendingAppendLogs>();
        var sem = new SemaphoreSlim(1, 1);
        var batches = new List<GrpcBatchRequestsRequest>();

        sem.Wait(); // pre-queue 6 items as non-flushers
        for (int i = 1; i <= 6; i++)
        {
            await GrpcCommunication.FlushCoalesced(
                pending, sem,
                b => { batches.Add(b); return Task.CompletedTask; },
                maxBatch: 3,
                MakePending(partition: i));
        }
        sem.Release();

        // This caller is the flusher for all 7 items (6 queued + its own), cap=3.
        await GrpcCommunication.FlushCoalesced(
            pending, sem,
            b => { batches.Add(b); return Task.CompletedTask; },
            maxBatch: 3,
            MakePending(partition: 7));

        Assert.Equal(3, batches.Count);            // ceil(7/3) = 3 frames
        Assert.Equal(3, batches[0].Requests.Count);
        Assert.Equal(3, batches[1].Requests.Count);
        Assert.Single(batches[2].Requests);
        Assert.Equal(7, batches.Sum(b => b.Requests.Count)); // all items written
        Assert.True(pending.IsEmpty);
        Assert.Equal(1, sem.CurrentCount);
    }

    /// <summary>
    /// A queue depth equal to exactly one cap fills one frame and the do/while exits
    /// immediately — no spurious second write.
    /// </summary>
    [Fact]
    public async Task Flusher_ExactCapDepth_SingleFrame()
    {
        var pending = new ConcurrentQueue<PendingAppendLogs>();
        var sem = new SemaphoreSlim(1, 1);
        var batches = new List<GrpcBatchRequestsRequest>();

        sem.Wait();
        for (int i = 1; i <= 3; i++)
        {
            await GrpcCommunication.FlushCoalesced(
                pending, sem,
                b => { batches.Add(b); return Task.CompletedTask; },
                maxBatch: 4,
                MakePending(partition: i));
        }
        sem.Release();

        // 4th item brings total to exactly cap=4.
        await GrpcCommunication.FlushCoalesced(
            pending, sem,
            b => { batches.Add(b); return Task.CompletedTask; },
            maxBatch: 4,
            MakePending(partition: 4));

        Assert.Single(batches);
        Assert.Equal(4, batches[0].Requests.Count);
        Assert.True(pending.IsEmpty);
    }

    // ── Exception safety ──────────────────────────────────────────────────────

    /// <summary>
    /// If <c>write</c> throws, the semaphore must still be released so subsequent callers
    /// are not permanently blocked.
    /// </summary>
    [Fact]
    public async Task Flusher_WriteThrows_SemaphoreIsStillReleased()
    {
        var pending = new ConcurrentQueue<PendingAppendLogs>();
        var sem = new SemaphoreSlim(1, 1);

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            GrpcCommunication.FlushCoalesced(
                pending, sem,
                _ => Task.FromException(new InvalidOperationException("simulated gRPC error")),
                maxBatch: 256,
                MakePending(partition: 1)));

        // Semaphore must be available so the next caller is not deadlocked.
        Assert.Equal(1, sem.CurrentCount);
    }

    // ── Ordering ─────────────────────────────────────────────────────────────

    /// <summary>
    /// Items within a single flushed batch must appear in enqueue order.
    /// </summary>
    [Fact]
    public async Task Flusher_MultipleQueuedItems_BatchPreservesEnqueueOrder()
    {
        var pending = new ConcurrentQueue<PendingAppendLogs>();
        var sem = new SemaphoreSlim(1, 1);
        var batches = new List<GrpcBatchRequestsRequest>();

        sem.Wait();
        for (int i = 1; i <= 4; i++)
        {
            await GrpcCommunication.FlushCoalesced(
                pending, sem,
                b => { batches.Add(b); return Task.CompletedTask; },
                maxBatch: 256,
                MakePending(partition: i));
        }
        sem.Release();

        await GrpcCommunication.FlushCoalesced(
            pending, sem,
            b => { batches.Add(b); return Task.CompletedTask; },
            maxBatch: 256,
            MakePending(partition: 5));

        Assert.Single(batches);
        int[] partitions = batches[0].Requests.Select(r => r.AppendLogs.Partition).ToArray();
        Assert.Equal([1, 2, 3, 4, 5], partitions);
    }
}
