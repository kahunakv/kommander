using Grpc.Core;
using Kommander;
using Kommander.Communication.Grpc;
using Kommander.Data;
using Kommander.System;
using Microsoft.Extensions.Logging;

namespace Kommander.Tests.Communication;

public sealed class TestGrpcSnapshotCompression
{
    static TestGrpcSnapshotCompression()
    {
        AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);
    }

    [Fact]
    public void BuildInstallSnapshotCallOptions_WhenEnabled_AddsGzipRequestEncoding()
    {
        Metadata metadata = [];

        GrpcCommunication.BuildInstallSnapshotCallOptions(
            new RaftConfiguration { GrpcEnableSnapshotCompression = true },
            metadata,
            CancellationToken.None);

        Metadata.Entry encoding = Assert.Single(
            metadata,
            entry => entry.Key == GrpcCommunication.GrpcRequestEncodingHeader);
        Assert.Equal(GrpcCommunication.GzipRequestEncoding, encoding.Value);
    }

    [Fact]
    public void BuildInstallSnapshotCallOptions_WhenDisabled_OmitsRequestEncoding()
    {
        Metadata metadata = [];

        GrpcCommunication.BuildInstallSnapshotCallOptions(
            new RaftConfiguration { GrpcEnableSnapshotCompression = false },
            metadata,
            CancellationToken.None);

        Assert.DoesNotContain(
            metadata,
            entry => entry.Key == GrpcCommunication.GrpcRequestEncodingHeader);
    }

    [Fact]
    public async Task SendInstallSnapshot_WithCompressionEnabled_SucceedsOverWire()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        using ILoggerFactory logFactory = LoggerFactory.Create(b => b.SetMinimumLevel(LogLevel.Warning));

        const int followerPort = 8890;
        const int leaderPort = 8891;
        List<RaftNode> peers =
        [
            new($"localhost:{followerPort}"),
            new($"localhost:{leaderPort}")
        ];

        await using GrpcTestNode follower = GrpcTestNode.Create(
            followerPort,
            peers,
            logFactory,
            configure: c => c.GrpcEnableSnapshotCompression = false);
        await using GrpcTestNode leader = GrpcTestNode.Create(
            leaderPort,
            peers,
            logFactory,
            configure: c => c.GrpcEnableSnapshotCompression = true);

        await follower.StartAsync(ct);
        await leader.StartAsync(ct);

        follower.Manager.RegisterStateMachineTransfer(new NoopTransfer());

        byte[] payload = Enumerable.Repeat((byte)'x', 16_384).ToArray();

        SnapshotRequest request = new()
        {
            SessionId = "compress-test",
            PartitionId = 0,
            SnapshotIndex = 42,
            FollowerEndpoint = follower.Endpoint,
            ChunkIndex = 0,
            IsLast = true,
            Data = payload
        };

        GrpcCommunication grpc = new();
        SnapshotResponse response = await grpc.SendInstallSnapshot(
            leader.Manager,
            new RaftNode(follower.Endpoint),
            request,
            ct);

        Assert.True(response.Success);
    }

    private sealed class NoopTransfer : IRaftStateMachineTransfer
    {
        public Task<Stream> ExportRange(RaftSplitPlan plan, long upToIndex, CancellationToken ct) =>
            Task.FromResult<Stream>(new MemoryStream());

        public Task ImportRange(int targetPartitionId, Stream snapshot, CancellationToken ct) =>
            Task.CompletedTask;
    }
}
