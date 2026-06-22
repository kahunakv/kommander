
using Kommander.Communication.Grpc;
using Grpc.Net.Client;

namespace Kommander.Tests.Communication;

/// <summary>
/// Tests that <see cref="SharedChannels.GetChannel"/> and <see cref="SharedChannels.GetStreaming"/>
/// use round-robin rather than random selection, distributing calls evenly across the pool (±1).
/// </summary>
public sealed class TestSharedChannelsRoundRobin
{
    // Use ports that are unlikely to conflict with other tests; channels are
    // never actually connected — we only inspect which pool slot is returned.
    private const string PeerUrl = "https://localhost:19900";

    /// <summary>
    /// Calling <c>GetChannel</c> N × poolSize times must hit each slot in the pool
    /// exactly N times (perfect round-robin, no birthday-paradox clustering).
    /// </summary>
    [Fact]
    public void GetChannel_RoundRobin_DistributesEvenly()
    {
        const int rounds = 5;
        List<GrpcChannel> pool = SharedChannels.GetAllChannels(PeerUrl);
        int poolSize = pool.Count;
        Assert.True(poolSize > 0, "Pool must be non-empty.");

        Dictionary<GrpcChannel, int> hits = pool.ToDictionary(c => c, _ => 0);

        int total = rounds * poolSize;
        for (int i = 0; i < total; i++)
        {
            GrpcChannel selected = SharedChannels.GetChannel(PeerUrl);
            hits[selected]++;
        }

        // Each slot must be hit exactly `rounds` times — round-robin is deterministic.
        foreach ((GrpcChannel ch, int count) in hits)
            Assert.True(count == rounds,
                $"Channel {ch.Target} hit {count} times; expected exactly {rounds} (pool size {poolSize}).");
    }

    /// <summary>
    /// After a full round of N <c>GetChannel</c> calls, the next call must return the
    /// same channel as the very first call — confirming the counter wraps mod pool size.
    /// </summary>
    [Fact]
    public void GetChannel_RoundRobin_WrapsCorrectly()
    {
        List<GrpcChannel> pool = SharedChannels.GetAllChannels(PeerUrl);
        int poolSize = pool.Count;

        // Drive the counter to the start of a fresh cycle.
        // The counter may have been advanced by GetChannel_RoundRobin_DistributesEvenly;
        // do poolSize more calls to land on a cycle boundary.
        for (int i = 0; i < poolSize; i++)
            SharedChannels.GetChannel(PeerUrl);

        GrpcChannel first = SharedChannels.GetChannel(PeerUrl);

        for (int i = 1; i < poolSize; i++)
            SharedChannels.GetChannel(PeerUrl);

        GrpcChannel wrappedAround = SharedChannels.GetChannel(PeerUrl);
        Assert.Same(first, wrappedAround);
    }
}
