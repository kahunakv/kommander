using Kommander.Communication.Grpc;

namespace Kommander.Tests.Communication;

/// <summary>
/// How <see cref="SharedChannels"/> hands out duplex-streaming slots: the round-robin selector
/// spreads calls across the pool, while the ordered selector (used for append traffic, where a
/// peer's frames must arrive in send order) always returns the same slot.
/// </summary>
/// <remarks>
/// The URLs use the reserved <c>.invalid</c> TLD so no connection is ever established; the
/// assertions are about slot identity, which is stable even though a slot may internally rebuild
/// its duplex call after a transport fault. Slot identity — not the inner call — is what the
/// ordering guarantee rests on.
/// </remarks>
public sealed class TestSharedChannelsStreamingSelection
{
    private const int PoolSize = 3;

    private static GrpcChannelPoolOptions Options { get; } = new(PoolSize, false, null);

    [Fact]
    public void GetOrderedStreaming_AlwaysReturnsTheSameSlot()
    {
        string url = UniqueUrl("ordered-same-slot");

        GrpcInterSharedStreaming first = SharedChannels.GetOrderedStreaming(url, null, Options);

        for (int i = 0; i < 10; i++)
        {
            GrpcInterSharedStreaming next = SharedChannels.GetOrderedStreaming(url, null, Options);
            Assert.Same(first, next);
        }
    }

    /// <summary>
    /// Round-robin must cycle through the whole pool in order: with a fresh counter, three
    /// consecutive <see cref="SharedChannels.GetStreaming"/> calls return three distinct slots,
    /// and the fourth returns the first again.
    /// </summary>
    [Fact]
    public void GetStreaming_RoundRobinsThroughThePool()
    {
        string url = UniqueUrl("streaming-round-robin");

        GrpcInterSharedStreaming first = SharedChannels.GetStreaming(url, null, Options);
        GrpcInterSharedStreaming second = SharedChannels.GetStreaming(url, null, Options);
        GrpcInterSharedStreaming third = SharedChannels.GetStreaming(url, null, Options);
        GrpcInterSharedStreaming fourth = SharedChannels.GetStreaming(url, null, Options);

        Assert.NotSame(first, second);
        Assert.NotSame(first, third);
        Assert.NotSame(second, third);
        Assert.Same(first, fourth);
    }

    /// <summary>
    /// The streaming pool is sized to match the channel pool for the same URL — even when the
    /// channels were created first by another caller — so the channel and streaming indices can
    /// never disagree.
    /// </summary>
    [Fact]
    public void StreamingPoolSize_MatchesTheChannelPoolAlreadyCreatedForTheUrl()
    {
        string url = UniqueUrl("streaming-size-matches-channels");

        // Create the channel pool first with the same size, as the unary send path would.
        List<Grpc.Net.Client.GrpcChannel> channels = SharedChannels.GetAllChannels(url, Options);
        Assert.Equal(PoolSize, channels.Count);

        HashSet<GrpcInterSharedStreaming> distinct = [];
        for (int i = 0; i < PoolSize; i++)
            distinct.Add(SharedChannels.GetStreaming(url, null, Options));

        Assert.Equal(PoolSize, distinct.Count);
    }

    private static string UniqueUrl(string name) =>
        $"https://{name}-{Guid.NewGuid():N}.k.invalid:9960";
}
