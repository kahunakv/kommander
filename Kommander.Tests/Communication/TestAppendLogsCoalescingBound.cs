using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using Kommander;
using Kommander.Communication.Grpc;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.Communication;

/// <summary>
/// The bounded-outbound-queue behavior of the coalesced append path: once the pending queue is
/// saturated, further appends are dropped on the sender (safe by the fire-and-forget contract —
/// the heartbeat retry re-ships them) without ever touching the stream; and a failed coalesced
/// write faults the slot so the next fetch rebuilds the duplex call instead of reusing a dead one.
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
public sealed class TestAppendLogsCoalescingBound
{
    static TestAppendLogsCoalescingBound()
    {
        AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);
    }

    /// <summary>
    /// With the stream semaphore held by another writer, appends below the queue cap are queued
    /// for the active flusher; once the cap is reached the next append is dropped before it can
    /// grow the queue — the bound that keeps a stalled peer from ballooning sender memory.
    /// </summary>
    [Fact]
    public async Task SaturatedPendingQueue_DropsFurtherAppendsWithoutWriting()
    {
        string host = $"saturation-{Guid.NewGuid():N}.k.invalid";
        string url = $"http://{host}:9970";

        using RaftManager manager = CreateManager(
            peerEndpoint: $"{host}:9970",
            configure: configuration =>
            {
                configuration.GrpcEnableAppendLogsCoalescing = true;
                configuration.GrpcAppendLogsMaxCoalesceBatch = 2;
            });

        GrpcChannelPoolOptions options = new(1, false, null);
        GrpcInterSharedStreaming slot = SharedChannels.GetOrderedStreaming(url, null, options);

        const int cap = 2 * 4; // max batch × the documented headroom multiplier
        for (int i = 0; i < cap - 1; i++)
            slot.Pending.Enqueue(MakePending(i));

        await slot.Semaphore.WaitAsync(TestContext.Current.CancellationToken);
        try
        {
            GrpcCommunication grpc = new();
            RaftNode peer = new($"{host}:9970");

            // Below the cap: the append is enqueued for the (busy) flusher.
            AppendLogsResponse queued = await grpc.AppendLogs(
                manager, peer, MakeRequest(partition: 1));
            Assert.NotNull(queued);
            Assert.Equal(cap, slot.Pending.Count);

            // At the cap: the append is dropped on the sender; the queue does not grow.
            AppendLogsResponse dropped = await grpc.AppendLogs(
                manager, peer, MakeRequest(partition: 2));
            Assert.NotNull(dropped);
            Assert.Equal(cap, slot.Pending.Count);
        }
        finally
        {
            slot.Semaphore.Release();
        }
    }

    /// <summary>
    /// A coalesced write against a peer that refuses connections must surface its failure to the
    /// caller AND flag the slot, so the next fetch of the same stream hands out a rebuilt duplex
    /// call rather than the dead one.
    /// </summary>
    [Fact]
    public async Task FailedCoalescedWrite_FaultsSlot_NextFetchRebuildsTheStream()
    {
        int deadPort = ReserveRefusedPort();
        string peerEndpoint = $"localhost:{deadPort}";
        string url = $"http://{peerEndpoint}";

        using RaftManager manager = CreateManager(
            peerEndpoint: peerEndpoint,
            configure: configuration => configuration.GrpcEnableAppendLogsCoalescing = true);

        GrpcChannelPoolOptions options = new(1, false, null);
        GrpcInterSharedStreaming slot = SharedChannels.GetOrderedStreaming(url, null, options);
        object callBeforeFailure = slot.Streaming;

        GrpcCommunication grpc = new();
        RaftNode peer = new(peerEndpoint);

        await Assert.ThrowsAnyAsync<Exception>(
            () => grpc.AppendLogs(manager, peer, MakeRequest(partition: 1)));

        GrpcInterSharedStreaming refreshed = SharedChannels.GetOrderedStreaming(url, null, options);

        Assert.Same(slot, refreshed);
        Assert.NotSame(callBeforeFailure, refreshed.Streaming);
    }

    private static AppendLogsRequest MakeRequest(int partition) =>
        new(partition, 1, new HLCTimestamp(1, 1, 1), "node-a:7000");

    private static PendingAppendLogs MakePending(int partition)
    {
        GrpcAppendLogsRequest request = new() { Partition = partition, Term = 1, Endpoint = "node-a:7000" };
        GrpcBatchRequestsRequestItem item = new()
        {
            Type = GrpcBatchRequestsRequestType.AppendLogs,
            AppendLogs = request
        };
        return new PendingAppendLogs(item, request);
    }

    private static RaftManager CreateManager(string peerEndpoint, Action<RaftConfiguration> configure)
    {
        RaftConfiguration configuration = new()
        {
            NodeName = "node-a",
            NodeId = 7000,
            Host = "localhost",
            Port = 7000,
            InitialPartitions = 1,
            GrpcScheme = "http://",
            GrpcChannelsPerNode = 1,
            // These tests never join a cluster: keep every timer far outside the test's lifetime
            // so no background election traffic competes for the stream under test.
            HeartbeatInterval = TimeSpan.FromSeconds(30),
            VotingTimeout = TimeSpan.FromSeconds(60),
            StartElectionTimeout = 60_000,
            EnableQuiescence = false,
            EndElectionTimeout = 90_000,
            TimerInitialDelay = TimeSpan.FromSeconds(30),
            PingInterval = TimeSpan.Zero
        };

        configure(configuration);

        return new RaftManager(
            configuration,
            new StaticDiscovery([new RaftNode(peerEndpoint)]),
            new InMemoryWAL(NullLogger<IRaft>.Instance),
            new GrpcCommunication(),
            new HybridLogicalClock(),
            NullLogger<IRaft>.Instance);
    }

    /// <summary>
    /// Reserves a loopback port and releases it again, so nothing is listening: connections to it
    /// are refused immediately, which is the failure shape the write-failure test relies on.
    /// </summary>
    private static int ReserveRefusedPort()
    {
        TcpListener listener = new(IPAddress.Loopback, 0);
        listener.Start();
        int port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }
}
