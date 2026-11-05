
using Grpc.Core;
using Grpc.Net.Client;
using Kommander.Communication.Grpc;

namespace Kommander.Tests.Communication;

/// <summary>
/// The rebuild half of the stuck-channel repair in <see cref="SharedChannels"/>: a rebuilt slot
/// hands out a new channel, the stale one is disposed, the duplex stream pinned to the slot
/// re-opens on the replacement, and the health view counts it. The verdict that triggers a rebuild
/// in production is tested on its own in <see cref="TestChannelConnectivityWatchdog"/>.
/// </summary>
/// <remarks>
/// The URLs use the reserved <c>.invalid</c> TLD so no connection is ever established, and each
/// test uses its own URL because the pool is process-global.
/// </remarks>
public sealed class TestSharedChannelsRebuild
{
    private static GrpcChannelPoolOptions Options { get; } = new(2, false, null);

    [Fact]
    public async Task Rebuild_ReplacesTheSlotAndDisposesTheStaleChannel()
    {
        const string url = "https://rebuild-slot.k.invalid:9910";

        List<GrpcChannel> before = SharedChannels.GetAllChannels(url, Options);
        GrpcChannel stale = before[1];

        Assert.True(SharedChannels.RebuildChannel(url, 1, "test"));

        List<GrpcChannel> after = SharedChannels.GetAllChannels(url, Options);
        Assert.Same(before[0], after[0]);
        Assert.NotSame(stale, after[1]);

        // The stale channel is disposed: grpc-dotnet refuses its connectivity API from then on.
        await Assert.ThrowsAsync<ObjectDisposedException>(
            () => stale.WaitForStateChangedAsync(ConnectivityState.Idle, TestContext.Current.CancellationToken));

        // Round-robin hands out the replacement and never the stale channel again.
        HashSet<GrpcChannel> handedOut = [];
        for (int i = 0; i < 2 * Options.ChannelsPerNode; i++)
            handedOut.Add(SharedChannels.GetChannel(url, Options));

        Assert.Contains(after[1], handedOut);
        Assert.DoesNotContain(stale, handedOut);
    }

    [Fact]
    public void Rebuild_ReopensTheOrderedStreamOnTheReplacement()
    {
        const string url = "https://rebuild-stream.k.invalid:9911";

        GrpcInterSharedStreaming ordered = SharedChannels.GetOrderedStreaming(url, null, Options);
        AsyncDuplexStreamingCall<GrpcBatchRequestsRequest, GrpcBatchRequestsResponse> callBefore = ordered.Streaming;

        Assert.True(SharedChannels.RebuildChannel(url, 0, "test"));

        GrpcInterSharedStreaming again = SharedChannels.GetOrderedStreaming(url, null, Options);

        // Slot identity is what the ordering guarantee rests on, and it survives the rebuild; the
        // duplex call inside it does not, because the channel it was opened on is gone.
        Assert.Same(ordered, again);
        Assert.NotSame(callBefore, again.Streaming);
    }

    [Fact]
    public void Rebuild_IsCountedInTheHealthView()
    {
        const string url = "https://rebuild-health.k.invalid:9912";

        Assert.Empty(SharedChannels.GetChannelHealth(url));

        SharedChannels.GetAllChannels(url, Options);
        IReadOnlyList<PeerChannelHealth> fresh = SharedChannels.GetChannelHealth(url);
        Assert.Equal(Options.ChannelsPerNode, fresh.Count);
        Assert.All(fresh, h => Assert.Equal(0, h.Rebuilds));

        Assert.True(SharedChannels.RebuildChannel(url, 1, "test"));
        Assert.True(SharedChannels.RebuildChannel(url, 1, "test"));

        IReadOnlyList<PeerChannelHealth> after = SharedChannels.GetChannelHealth(url);
        Assert.Equal(0, after[0].Rebuilds);
        Assert.Equal(2, after[1].Rebuilds);
        Assert.Equal(1, after[1].Slot);
    }

    [Fact]
    public void Rebuild_OfAnUnknownPoolOrSlot_IsRefused()
    {
        Assert.False(SharedChannels.RebuildChannel("https://never-built.k.invalid:9913", 0, "test"));

        const string url = "https://rebuild-range.k.invalid:9914";
        SharedChannels.GetAllChannels(url, Options);

        Assert.False(SharedChannels.RebuildChannel(url, Options.ChannelsPerNode, "test"));
        Assert.False(SharedChannels.RebuildChannel(url, -1, "test"));
    }
}
