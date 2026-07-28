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

    // Note: the over-the-wire SendInstallSnapshot_WithCompressionEnabled_SucceedsOverWire test was
    // retired in increment B. Post-B a successful install requires a live partition executor on the
    // follower (the receiver no longer imports on a bare, un-joined manager), and driving that onto a
    // real consensus partition purely to assert the gzip request header is decoded is both flaky and
    // redundant: the request-encoding header construction is covered by the two unit tests above, and
    // the full over-the-wire install path is exercised by TestSnapshotIntegration.
}
