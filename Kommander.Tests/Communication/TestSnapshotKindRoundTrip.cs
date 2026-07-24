
using Grpc.Core;
using Kommander;
using Kommander.Communication.Grpc;
using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.Communication;

/// <summary>
/// Verifies that <see cref="SnapshotKind"/> survives a round-trip through every
/// <c>ICommunication</c> implementation unchanged, and that a request with no kind
/// field set (simulated by using the default) deserializes as <see cref="SnapshotKind.Range"/>.
/// </summary>
public sealed class TestSnapshotKindRoundTrip
{
    static TestSnapshotKindRoundTrip()
    {
        AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);
    }

    // ── In-memory transport ────────────────────────────────────────────────────

    [Theory]
    [InlineData(SnapshotKind.Range)]
    [InlineData(SnapshotKind.SystemState)]
    public async Task InMemory_SnapshotKind_RoundTrips(SnapshotKind kind)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        CapturingSystemTransfer systemTransfer = new();
        CapturingRangeTransfer rangeTransfer = new();

        InMemoryCommunication comm = new();
        using RaftManager receiver = BuildInMemoryNode(comm, 19100, []);
        receiver.RegisterStateMachineTransfer(rangeTransfer);
        receiver.RegisterSystemStateTransfer(systemTransfer);
        comm.SetNodes(new Dictionary<string, IRaft> { [$"localhost:19100"] = receiver });

        using RaftManager sender = BuildInMemoryNode(comm, 19101, ["localhost:19100"]);

        SnapshotRequest request = new()
        {
            SessionId = "mem-test",
            PartitionId = 0,
            SnapshotIndex = 10,
            FollowerEndpoint = "localhost:19100",
            ChunkIndex = 0,
            IsLast = true,
            Data = new byte[] { 1, 2, 3 },
            Kind = kind,
        };

        SnapshotResponse response = await comm.SendInstallSnapshot(
            sender, new RaftNode("localhost:19100"), request, ct);

        Assert.True(response.Success);
        if (kind == SnapshotKind.SystemState)
            Assert.Equal(1, systemTransfer.ImportCount);
        else
            Assert.Equal(1, rangeTransfer.ImportCount);
    }

    [Fact]
    public async Task InMemory_DefaultKind_TreatedAsRange()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        CapturingRangeTransfer rangeTransfer = new();
        InMemoryCommunication comm = new();
        using RaftManager receiver = BuildInMemoryNode(comm, 19102, []);
        receiver.RegisterStateMachineTransfer(rangeTransfer);
        comm.SetNodes(new Dictionary<string, IRaft> { [$"localhost:19102"] = receiver });

        using RaftManager sender = BuildInMemoryNode(comm, 19103, ["localhost:19102"]);

        // No Kind set — should default to Range.
        SnapshotRequest request = new()
        {
            SessionId = "mem-default",
            PartitionId = 0,
            SnapshotIndex = 1,
            FollowerEndpoint = "localhost:19102",
            ChunkIndex = 0,
            IsLast = true,
            Data = new byte[] { 7 },
        };

        SnapshotResponse response = await comm.SendInstallSnapshot(
            sender, new RaftNode("localhost:19102"), request, ct);

        Assert.True(response.Success);
        Assert.Equal(1, rangeTransfer.ImportCount);
    }

    // ── gRPC transport ────────────────────────────────────────────────────────

    [Theory]
    [InlineData(SnapshotKind.Range)]
    [InlineData(SnapshotKind.SystemState)]
    public async Task Grpc_SnapshotKind_RoundTrips(SnapshotKind kind)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        using ILoggerFactory logFactory = LoggerFactory.Create(b => b.SetMinimumLevel(LogLevel.Warning));

        const int followerPort = 19200;
        const int leaderPort   = 19201;
        List<RaftNode> peers = [new($"localhost:{followerPort}"), new($"localhost:{leaderPort}")];

        CapturingSystemTransfer systemTransfer = new();
        CapturingRangeTransfer rangeTransfer = new();

        await using GrpcTestNode follower = GrpcTestNode.Create(followerPort, peers, logFactory);
        await using GrpcTestNode leader   = GrpcTestNode.Create(leaderPort,   peers, logFactory);

        await follower.StartAsync(ct);
        await leader.StartAsync(ct);

        follower.Manager.RegisterStateMachineTransfer(rangeTransfer);
        follower.Manager.RegisterSystemStateTransfer(systemTransfer);

        SnapshotRequest request = new()
        {
            SessionId = $"grpc-{kind}",
            PartitionId = 0,
            SnapshotIndex = 5,
            FollowerEndpoint = follower.Endpoint,
            ChunkIndex = 0,
            IsLast = true,
            Data = new byte[] { 0xAB },
            Kind = kind,
        };

        GrpcCommunication grpc = new();
        SnapshotResponse response = await grpc.SendInstallSnapshot(
            leader.Manager, new RaftNode(follower.Endpoint), request, ct);

        Assert.True(response.Success);
        if (kind == SnapshotKind.SystemState)
            Assert.Equal(1, systemTransfer.ImportCount);
        else
            Assert.Equal(1, rangeTransfer.ImportCount);
    }

    [Fact]
    public async Task Grpc_DefaultKind_TreatedAsRange()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        using ILoggerFactory logFactory = LoggerFactory.Create(b => b.SetMinimumLevel(LogLevel.Warning));

        const int followerPort = 19202;
        const int leaderPort   = 19203;
        List<RaftNode> peers = [new($"localhost:{followerPort}"), new($"localhost:{leaderPort}")];

        CapturingRangeTransfer rangeTransfer = new();

        await using GrpcTestNode follower = GrpcTestNode.Create(followerPort, peers, logFactory);
        await using GrpcTestNode leader   = GrpcTestNode.Create(leaderPort,   peers, logFactory);

        await follower.StartAsync(ct);
        await leader.StartAsync(ct);

        follower.Manager.RegisterStateMachineTransfer(rangeTransfer);

        SnapshotRequest request = new()
        {
            SessionId = "grpc-default",
            PartitionId = 0,
            SnapshotIndex = 1,
            FollowerEndpoint = follower.Endpoint,
            ChunkIndex = 0,
            IsLast = true,
            Data = new byte[] { 0xFF },
            // Kind intentionally omitted — must arrive as Range
        };

        GrpcCommunication grpc = new();
        SnapshotResponse response = await grpc.SendInstallSnapshot(
            leader.Manager, new RaftNode(follower.Endpoint), request, ct);

        Assert.True(response.Success);
        Assert.Equal(1, rangeTransfer.ImportCount);
    }

    // ── Helpers ────────────────────────────────────────────────────────────────

    private static RaftManager BuildInMemoryNode(InMemoryCommunication comm, int port, string[] peers) =>
        new(
            new RaftConfiguration { Host = "localhost", Port = port, InitialPartitions = 1 },
            new StaticDiscovery(peers.Select(p => new RaftNode(p)).ToList()),
            new InMemoryWAL(NullLogger<IRaft>.Instance),
            comm,
            new HybridLogicalClock(),
            NullLogger<IRaft>.Instance);

    private sealed class CapturingRangeTransfer : IRaftStateMachineTransfer
    {
        public int ImportCount;

        public Task<Stream> ExportRange(RaftSplitPlan plan, long upToIndex, CancellationToken ct) =>
            Task.FromResult<Stream>(new MemoryStream());

        public Task ImportRange(int targetPartitionId, Stream snapshot, CancellationToken ct)
        {
            Interlocked.Increment(ref ImportCount);
            return Task.CompletedTask;
        }
    }

    private sealed class CapturingSystemTransfer : IRaftSystemStateTransfer
    {
        public int ImportCount;

        public Task<Stream> ExportPartitionState(int partitionId, long upToIndex, CancellationToken ct) =>
            Task.FromResult<Stream>(new MemoryStream());

        public Task ImportPartitionState(int partitionId, Stream snapshot, CancellationToken ct)
        {
            Interlocked.Increment(ref ImportCount);
            return Task.CompletedTask;
        }
    }
}
