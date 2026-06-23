
using System.IO;
using Grpc.Core;
using Kommander.Communication.Grpc;

namespace Kommander.Tests.Communication;

/// <summary>
/// Tests that a faulted <see cref="GrpcInterSharedStreaming"/> slot self-heals: the pool
/// recreates the duplex call in place instead of leaving a permanently dead stream that
/// every subsequent round-robin selection would hand out (the cause of sustained
/// "request stream was aborted / HTTP/2 connection faulted" errors under load).
/// </summary>
public sealed class TestSharedStreamingRecovery
{
    /// <summary>Response reader whose read loop blocks until the test faults or completes it.</summary>
    private sealed class FakeResponseReader : IAsyncStreamReader<GrpcBatchRequestsResponse>
    {
        private readonly TaskCompletionSource<bool> tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public GrpcBatchRequestsResponse Current => new();
        public Task<bool> MoveNext(CancellationToken cancellationToken) => tcs.Task;
        public void Fault(Exception ex) => tcs.TrySetException(ex);
    }

    private sealed class FakeRequestWriter : IClientStreamWriter<GrpcBatchRequestsRequest>
    {
        public WriteOptions? WriteOptions { get; set; }
        public Task WriteAsync(GrpcBatchRequestsRequest message) => Task.CompletedTask;
        public Task CompleteAsync() => Task.CompletedTask;
    }

    private sealed record FakeCall(
        AsyncDuplexStreamingCall<GrpcBatchRequestsRequest, GrpcBatchRequestsResponse> Call,
        FakeResponseReader Reader);

    /// <summary>Factory that records each created call and whether it was disposed.</summary>
    private sealed class CallFactory
    {
        public readonly List<FakeResponseReader> Readers = [];
        public readonly List<bool> Disposed = [];
        public int Created => Readers.Count;

        public AsyncDuplexStreamingCall<GrpcBatchRequestsRequest, GrpcBatchRequestsResponse> Create()
        {
            int index = Readers.Count;
            FakeResponseReader reader = new();
            Readers.Add(reader);
            Disposed.Add(false);
            return new AsyncDuplexStreamingCall<GrpcBatchRequestsRequest, GrpcBatchRequestsResponse>(
                new FakeRequestWriter(),
                reader,
                Task.FromResult(new Metadata()),
                () => Status.DefaultSuccess,
                () => new Metadata(),
                () => Disposed[index] = true);
        }
    }

    private static async Task WaitUntilAsync(Func<bool> condition, int timeoutMs = 2000)
    {
        int waited = 0;
        while (!condition() && waited < timeoutMs)
        {
            await Task.Delay(10);
            waited += 10;
        }
    }

    /// <summary>
    /// A healthy slot is never recreated: <c>EnsureHealthy</c> is a no-op when the stream
    /// has not faulted.
    /// </summary>
    [Fact]
    public void EnsureHealthy_WhenHealthy_DoesNotRecreate()
    {
        CallFactory factory = new();
        GrpcInterSharedStreaming slot = new(factory.Create);

        var first = slot.Streaming;
        slot.EnsureHealthy();
        slot.EnsureHealthy();

        Assert.Equal(1, factory.Created);
        Assert.Same(first, slot.Streaming);
    }

    /// <summary>
    /// After the slot is flagged faulted, <c>EnsureHealthy</c> disposes the dead call and
    /// installs a fresh one — exactly once even if called concurrently.
    /// </summary>
    [Fact]
    public void EnsureHealthy_AfterMarkFaulted_RecreatesStreamOnce()
    {
        CallFactory factory = new();
        GrpcInterSharedStreaming slot = new(factory.Create);
        var first = slot.Streaming;

        slot.MarkFaulted();

        Parallel.For(0, 8, _ => slot.EnsureHealthy());

        Assert.Equal(2, factory.Created);             // recreated exactly once
        Assert.True(factory.Disposed[0]);             // old call disposed
        Assert.NotSame(first, slot.Streaming);        // slot now points at the fresh call
    }

    /// <summary>
    /// The full self-heal path: when the response stream faults (transport error), the
    /// background reader flags the slot, and the next <c>EnsureHealthy</c> recreates it —
    /// no external MarkFaulted required.
    /// </summary>
    [Fact]
    public async Task ResponseStreamFault_TriggersRecreationOnNextEnsureHealthy()
    {
        CallFactory factory = new();
        GrpcInterSharedStreaming slot = new(factory.Create);
        var first = slot.Streaming;

        // Simulate the HTTP/2 connection faulting: the response read loop throws.
        factory.Readers[0].Fault(new IOException("The request stream was aborted."));

        // The pool recreates lazily on the next hand-out; poll EnsureHealthy until it heals.
        await WaitUntilAsync(() =>
        {
            slot.EnsureHealthy();
            return factory.Created == 2;
        });

        Assert.Equal(2, factory.Created);
        Assert.NotSame(first, slot.Streaming);
    }
}
