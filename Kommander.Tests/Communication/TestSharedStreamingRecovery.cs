
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

    /// <summary>
    /// Factory that records each created call and whether it was disposed. Disposing a call faults
    /// its response reader with <see cref="StatusCode.Cancelled"/>, as gRPC does: the drain loop of
    /// a disposed call ends with that exception some time after the dispose returns.
    /// </summary>
    private sealed class CallFactory
    {
        public readonly List<FakeResponseReader> Readers = [];
        public readonly List<bool> Disposed = [];
        public int Created => Readers.Count;

        /// <summary>
        /// When true, <c>GetStatus()</c> on every created call throws as gRPC does for a call that
        /// is still in progress; when false it returns a final status, as for a completed call.
        /// </summary>
        public bool CallsInProgress { get; set; }

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
                () => CallsInProgress
                    ? throw new InvalidOperationException("Unable to get the status because the call is not complete.")
                    : Status.DefaultSuccess,
                () => new Metadata(),
                () =>
                {
                    Disposed[index] = true;
                    reader.Fault(new RpcException(new Status(StatusCode.Cancelled, "gRPC call disposed.")));
                });
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

    /// <summary>
    /// The recreate churn behind the CamusDB fault soak of 2026-10-30: the drain loop of the call
    /// <c>EnsureHealthy</c> disposed ends with a cancellation AFTER the fresh call is installed and
    /// the flag cleared. That late ending must not flag the fresh call — otherwise the next
    /// hand-out disposes the healthy replacement, whose own loop then ends the same way, and the
    /// slot re-opens a stream on every send for as long as traffic flows.
    /// </summary>
    [Fact]
    public async Task StaleDrainLoop_OfAReplacedCall_DoesNotFaultTheReplacement()
    {
        CallFactory factory = new();
        GrpcInterSharedStreaming slot = new(factory.Create);

        slot.MarkFaulted();
        slot.EnsureHealthy();
        Assert.Equal(2, factory.Created);
        var fresh = slot.Streaming;

        // The disposed call's drain loop ends now, after the swap.
        await Task.Delay(100, TestContext.Current.CancellationToken);

        for (int i = 0; i < 5; i++)
            slot.EnsureHealthy();

        Assert.Equal(2, factory.Created);
        Assert.Same(fresh, slot.Streaming);
        Assert.False(factory.Disposed[1]);
    }

    /// <summary>
    /// A frame this side's own <c>MaxSendMessageSize</c> refuses fails the write with
    /// <see cref="StatusCode.ResourceExhausted"/> while the call stays open. The stream is intact,
    /// so it must stay in place: flagging it is what disposed a live call under its drain loop and
    /// started the churn above.
    /// </summary>
    [Fact]
    public void WriteRefusedOnThisSide_LeavesTheStreamInPlace()
    {
        CallFactory factory = new() { CallsInProgress = true };
        GrpcInterSharedStreaming slot = new(factory.Create);
        var call = slot.Streaming;

        slot.MarkFaultedAfterWriteFailure(call, new RpcException(new Status(StatusCode.ResourceExhausted, "Sending message exceeds the maximum configured message size.")));
        slot.EnsureHealthy();

        Assert.Equal(1, factory.Created);
        Assert.Same(call, slot.Streaming);
    }

    /// <summary>
    /// The peer refusing a frame ends the call with the status (the call is complete), and any
    /// other write failure means the stream is broken: both recreate the slot on the next hand-out.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void WriteFailure_OnTheCurrentCall_RecreatesTheStream(bool peerRefusedTheFrame)
    {
        CallFactory factory = new() { CallsInProgress = !peerRefusedTheFrame };
        GrpcInterSharedStreaming slot = new(factory.Create);
        var call = slot.Streaming;

        Exception failure = peerRefusedTheFrame
            ? new RpcException(new Status(StatusCode.ResourceExhausted, "Received message exceeds the maximum configured message size."))
            : new IOException("The request stream was aborted.");

        slot.MarkFaultedAfterWriteFailure(call, failure);
        slot.EnsureHealthy();

        Assert.Equal(2, factory.Created);
        Assert.True(factory.Disposed[0]);
        Assert.NotSame(call, slot.Streaming);
    }

    /// <summary>
    /// A failure reported against a call that was already replaced says nothing about its
    /// replacement: a writer that was mid-frame on the old call when the swap happened must not
    /// fault the fresh one.
    /// </summary>
    [Fact]
    public void WriteFailure_OnAReplacedCall_IsIgnored()
    {
        CallFactory factory = new();
        GrpcInterSharedStreaming slot = new(factory.Create);
        var old = slot.Streaming;

        slot.MarkFaulted();
        slot.EnsureHealthy();
        var fresh = slot.Streaming;
        Assert.Equal(2, factory.Created);

        slot.MarkFaultedAfterWriteFailure(old, new IOException("The request stream was aborted."));
        slot.EnsureHealthy();

        Assert.Equal(2, factory.Created);
        Assert.Same(fresh, slot.Streaming);
    }
}
