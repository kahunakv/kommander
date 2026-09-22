
using Kommander.Data;

namespace Kommander.Tests.Scheduling;

/// <summary>
/// Covers <see cref="FollowerReadIndexFetcher"/>, the follower half of read-index coalescing:
/// concurrent follower reads share one <c>GetReadIndex</c> RPC, but a read never joins a fetch
/// that was already sent, because that fetch's index can predate the read. The transport is a
/// fake that parks every call until the test answers it, so each test controls exactly which
/// reads are in which batch.
/// </summary>
public class TestFollowerReadIndexFetcher
{
    private static readonly TimeSpan WaitBudget = TimeSpan.FromSeconds(5);

    /// <summary>A parked transport call: the token it got and the answer the test gives it.</summary>
    private sealed record PendingFetch(RaftNode Node, int PartitionId, CancellationToken Token, TaskCompletionSource<GetReadIndexResponse> Reply);

    /// <summary>Records every transport call and parks it until the test completes its reply.</summary>
    private sealed class FakeTransport
    {
        private readonly List<PendingFetch> calls = [];
        private readonly SemaphoreSlim called = new(0);

        public Func<RaftNode, GetReadIndexRequest, CancellationToken, Task<GetReadIndexResponse>>? Override { get; set; }

        public int CallCount
        {
            get { lock (calls) return calls.Count; }
        }

        public Task<GetReadIndexResponse> Fetch(RaftNode node, GetReadIndexRequest request, CancellationToken token)
        {
            PendingFetch pending = new(node, request.PartitionId, token, new(TaskCreationOptions.RunContinuationsAsynchronously));

            lock (calls)
                calls.Add(pending);

            called.Release();

            return Override is { } o ? o(node, request, token) : pending.Reply.Task;
        }

        public async Task<PendingFetch> NextCall()
        {
            Assert.True(await called.WaitAsync(WaitBudget), "the fetcher did not call the transport");

            lock (calls)
                return calls[^1];
        }
    }

    private static FollowerReadIndexFetcher Make(FakeTransport transport, TimeSpan? timeout = null) =>
        new(transport.Fetch, () => timeout ?? TimeSpan.FromSeconds(30));

    private static Task<GetReadIndexResponse> Read(FollowerReadIndexFetcher fetcher, string leader = "leader:1", int partition = 1, CancellationToken token = default) =>
        fetcher.FetchAsync(new RaftNode(leader), new GetReadIndexRequest(partition), token);

    [Fact]
    public async Task IdleRead_SendsAtOnce_AndReturnsTheAnswer()
    {
        FakeTransport transport = new();
        FollowerReadIndexFetcher fetcher = Make(transport);

        Task<GetReadIndexResponse> read = Read(fetcher);

        PendingFetch call = await transport.NextCall();
        Assert.Equal("leader:1", call.Node.Endpoint);
        Assert.Equal(1, call.PartitionId);

        call.Reply.SetResult(new GetReadIndexResponse(true, 42));

        GetReadIndexResponse response = await read.WaitAsync(WaitBudget);
        Assert.True(response.Success);
        Assert.Equal(42, response.ReadIndex);
        Assert.Equal(1, transport.CallCount);
    }

    /// <summary>
    /// The safety rule. Reads that arrive while a fetch is on the wire must not get that fetch's
    /// index: they wait for the next fetch, which is sent after they arrived. All of them share
    /// that one next fetch.
    /// </summary>
    [Fact]
    public async Task ReadsArrivingDuringAFetch_ShareTheNextFetch_NotTheOneInFlight()
    {
        FakeTransport transport = new();
        FollowerReadIndexFetcher fetcher = Make(transport);

        Task<GetReadIndexResponse> first = Read(fetcher);
        PendingFetch inFlight = await transport.NextCall();

        Task<GetReadIndexResponse>[] late = Enumerable.Range(0, 16).Select(_ => Read(fetcher)).ToArray();

        // Nothing new is sent while the first fetch is in flight.
        Assert.Equal(1, transport.CallCount);

        inFlight.Reply.SetResult(new GetReadIndexResponse(true, 5));
        Assert.Equal(5, (await first.WaitAsync(WaitBudget)).ReadIndex);

        // The late reads are still waiting: the answer to the first fetch is not theirs.
        PendingFetch next = await transport.NextCall();
        Assert.All(late, t => Assert.False(t.IsCompleted));

        next.Reply.SetResult(new GetReadIndexResponse(true, 9));

        foreach (Task<GetReadIndexResponse> t in late)
            Assert.Equal(9, (await t.WaitAsync(WaitBudget)).ReadIndex);

        Assert.Equal(2, transport.CallCount);
    }

    [Fact]
    public async Task CallerCancellation_EndsOnlyThatWait_AndNeverCancelsTheSharedFetch()
    {
        FakeTransport transport = new();
        FollowerReadIndexFetcher fetcher = Make(transport);

        using CancellationTokenSource firstCts = new();
        Task<GetReadIndexResponse> first = Read(fetcher, token: firstCts.Token);
        PendingFetch inFlight = await transport.NextCall();

        using CancellationTokenSource leaverCts = new();
        Task<GetReadIndexResponse> leaver = Read(fetcher, token: leaverCts.Token);
        Task<GetReadIndexResponse> stayer = Read(fetcher);

        firstCts.Cancel();
        leaverCts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => first);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => leaver);

        // The sole caller of the in-flight fetch gave up; the fetch itself is not cancelled.
        Assert.False(inFlight.Token.IsCancellationRequested);

        inFlight.Reply.SetResult(new GetReadIndexResponse(true, 3));

        PendingFetch next = await transport.NextCall();
        Assert.False(next.Token.IsCancellationRequested);
        next.Reply.SetResult(new GetReadIndexResponse(true, 4));

        Assert.Equal(4, (await stayer.WaitAsync(WaitBudget)).ReadIndex);
    }

    [Fact]
    public async Task TransportException_FailsClosed_AndTheNextReadStillFetches()
    {
        FakeTransport transport = new();
        FollowerReadIndexFetcher fetcher = Make(transport);

        Task<GetReadIndexResponse> failed = Read(fetcher);
        PendingFetch call = await transport.NextCall();
        call.Reply.SetException(new InvalidOperationException("boom"));

        Assert.False((await failed.WaitAsync(WaitBudget)).Success);

        Task<GetReadIndexResponse> retry = Read(fetcher);
        PendingFetch again = await transport.NextCall();
        again.Reply.SetResult(new GetReadIndexResponse(true, 11));

        Assert.Equal(11, (await retry.WaitAsync(WaitBudget)).ReadIndex);
    }

    /// <summary>
    /// A fetch that never answers must not park the key: the shared timeout fails its batch
    /// closed and frees the chain for the reads behind it.
    /// </summary>
    [Fact]
    public async Task HungFetch_IsBoundedByTheSharedTimeout_AndDoesNotParkLaterReads()
    {
        FakeTransport transport = new()
        {
            Override = static async (_, _, token) =>
            {
                await Task.Delay(Timeout.Infinite, token);
                return new GetReadIndexResponse(true, 1);
            }
        };

        FollowerReadIndexFetcher fetcher = Make(transport, TimeSpan.FromMilliseconds(100));

        Task<GetReadIndexResponse> hung = Read(fetcher);
        await transport.NextCall();
        Task<GetReadIndexResponse> behind = Read(fetcher);

        Assert.False((await hung.WaitAsync(WaitBudget)).Success);

        await transport.NextCall();
        Assert.False((await behind.WaitAsync(WaitBudget)).Success);
        Assert.Equal(2, transport.CallCount);
    }

    [Fact]
    public async Task DifferentPartitionsAndLeaders_DoNotWaitForEachOther()
    {
        FakeTransport transport = new();
        FollowerReadIndexFetcher fetcher = Make(transport);

        Task<GetReadIndexResponse> p1 = Read(fetcher, partition: 1);
        PendingFetch p1Call = await transport.NextCall();

        Task<GetReadIndexResponse> p2 = Read(fetcher, partition: 2);
        PendingFetch p2Call = await transport.NextCall();

        Task<GetReadIndexResponse> otherLeader = Read(fetcher, leader: "leader:2", partition: 1);
        PendingFetch otherCall = await transport.NextCall();

        Assert.Equal(3, transport.CallCount);
        Assert.Equal(2, p2Call.PartitionId);
        Assert.Equal("leader:2", otherCall.Node.Endpoint);

        p2Call.Reply.SetResult(new GetReadIndexResponse(true, 20));
        otherCall.Reply.SetResult(new GetReadIndexResponse(true, 30));
        p1Call.Reply.SetResult(new GetReadIndexResponse(true, 10));

        Assert.Equal(10, (await p1.WaitAsync(WaitBudget)).ReadIndex);
        Assert.Equal(20, (await p2.WaitAsync(WaitBudget)).ReadIndex);
        Assert.Equal(30, (await otherLeader.WaitAsync(WaitBudget)).ReadIndex);
    }
}
