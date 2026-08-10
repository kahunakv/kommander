
using Kommander.Time;

namespace Kommander.Tests.Time;

public class TestHLC
{
    [Fact]
    public void TestAddOperation()
    {
        HLCTimestamp x = new(0, GetCurrentTime(), 0);
        HLCTimestamp p = x + 90000;
        
        Assert.True(p > x);
    }
    
    [Fact]
    public void TestAddOperation2()
    {
        HLCTimestamp x = new(0, GetCurrentTime(), 0);
        HLCTimestamp p = x + TimeSpan.FromMilliseconds(10000);
        
        Assert.True(p > x);
    }
    
    [Fact]
    public void TestHLC1()
    {
        HybridLogicalClock clock = new();
        
        Dictionary<HLCTimestamp, bool> hlc = new();
        
        for (int i = 0; i < 100; i++)
            hlc.Add(clock.SendOrLocalEvent(0), true);
            
        Assert.Equal(100, hlc.Count);
    }

    [Fact]
    public void TestHLCCompare()
    {
        HLCTimestamp t1 = new(0, 1744147995701, 1);
        HLCTimestamp t2 = new(0, 1744147995701, 2);
        
        Assert.Equal(-1, t1.CompareTo(t2));
    }
    
    [Fact]
    public void TestHLCCompare2()
    {
        HLCTimestamp t1 = new(0, 1744147995701, 1);
        HLCTimestamp t2 = new(0, 1744147995701, 1);
        
        Assert.Equal(0, t1.CompareTo(t2));
    }
    
    [Fact]
    public void TestHLCCompare3()
    {
        HLCTimestamp t1 = new(0, 1744147995701, 2);
        HLCTimestamp t2 = new(0, 1744147995701, 1);
        
        Assert.Equal(1, t1.CompareTo(t2));
    }
    
    [Fact]
    public void TestHLCCompare4()
    {
        HLCTimestamp t1 = new(3, 1745805885489, 11);
        HLCTimestamp t2 = new(2, 1745805885489, 3);

        Assert.Equal(1, t1.CompareTo(t2));
        Assert.Equal(-1, t2.CompareTo(t1));
    }

    [Fact]
    public void SamePhysicalAndCounter_DifferentNode_OrdersConsistentlyByNode()
    {
        // Same L and C, different node id. These are distinct timestamps (record equality
        // includes the node), so the comparison must be a consistent total order — exactly one
        // direction negative, the other positive — with the node id as the tie-breaker.
        HLCTimestamp lowerNode  = new(2, 1745805885489, 7);
        HLCTimestamp higherNode = new(5, 1745805885489, 7);

        Assert.Equal(-1, lowerNode.CompareTo(higherNode));
        Assert.Equal(1,  higherNode.CompareTo(lowerNode));
        Assert.NotEqual(0, lowerNode.CompareTo(higherNode));
        Assert.NotEqual(lowerNode, higherNode);
    }

    [Fact]
    public void CompareTo_IsAConsistentTotalOrder_AcrossNodesCountersAndPhysical()
    {
        // A list that includes same-(L,C) different-node entries must sort into a stable,
        // lexicographic (L, C, N) order without the comparator contradicting itself.
        List<HLCTimestamp> stamps =
        [
            new(5, 1000, 2),
            new(1, 1000, 2),
            new(2, 1000, 1),
            new(9, 1000, 2), // same (L,C) as the first, higher node
            new(1, 2000, 0),
        ];

        stamps.Sort();

        List<HLCTimestamp> expected =
        [
            new(2, 1000, 1),  // L=1000, C=1
            new(1, 1000, 2),  // L=1000, C=2, N=1
            new(5, 1000, 2),  // L=1000, C=2, N=5
            new(9, 1000, 2),  // L=1000, C=2, N=9
            new(1, 2000, 0),  // L=2000
        ];

        Assert.Equal(expected, stamps);
    }

    [Fact]
    public void ReceiveEvent_ResultDominatesBothClockAndMessage()
    {
        HybridLogicalClock clock = new();
        HLCTimestamp local = clock.SendOrLocalEvent(0);

        HLCTimestamp m = new(1, GetCurrentTime() + 5, 17);
        HLCTimestamp result = clock.ReceiveEvent(0, m);

        // HLC contract: the merged timestamp is strictly greater than both inputs.
        Assert.True(result.CompareTo(local) > 0);
        Assert.True(result.L > m.L || (result.L == m.L && result.C > m.C));
    }

    [Fact]
    public void ReceiveEvent_HugeWireCounter_RollsIntoLWithoutLosingCausality()
    {
        // The packed-state clock stores C in 22 bits; a wire message may carry any uint counter.
        // An increment that no longer fits must roll into (L+1, 0) — still strictly greater than
        // the message in HLC order — rather than silently truncating the counter.
        HybridLogicalClock clock = new();

        long messageL = GetCurrentTime() + 60_000; // future L so the message dominates physical time
        HLCTimestamp m = new(1, messageL, uint.MaxValue);

        HLCTimestamp result = clock.ReceiveEvent(0, m);

        Assert.True(result.L > messageL || (result.L == messageL && result.C > uint.MaxValue));
        Assert.Equal(messageL + 1, result.L);
        Assert.Equal(0u, result.C);

        // The clock keeps ticking correctly past the rollover.
        HLCTimestamp next = clock.SendOrLocalEvent(0);
        Assert.True(next.CompareTo(result) > 0);
    }

    [Fact]
    public async Task SendOrLocalEvent_ConcurrentCallers_ProduceUniqueMonotonicTimestamps()
    {
        // The lock-free packed CAS must hand out globally unique (L, C) pairs under contention —
        // duplicates would collide proposal ticket ids.
        HybridLogicalClock clock = new();
        const int threads = 8;
        const int perThread = 20_000;

        HLCTimestamp[][] results = new HLCTimestamp[threads][];

        await Task.WhenAll(Enumerable.Range(0, threads).Select(t => Task.Run(() =>
        {
            HLCTimestamp[] mine = new HLCTimestamp[perThread];
            for (int i = 0; i < perThread; i++)
                mine[i] = clock.SendOrLocalEvent(0);
            results[t] = mine;
        })));

        HashSet<HLCTimestamp> unique = [];
        foreach (HLCTimestamp[] batch in results)
        {
            HLCTimestamp previous = HLCTimestamp.Zero;
            foreach (HLCTimestamp ts in batch)
            {
                // Per-thread monotonicity: each successive call observes a strictly greater stamp.
                Assert.True(ts.CompareTo(previous) > 0);
                previous = ts;
                Assert.True(unique.Add(ts));
            }
        }

        Assert.Equal(threads * perThread, unique.Count);
    }

    [Fact]
    public async Task TrySendOrLocalEvent_ConcurrentCallers_ProduceUniqueMonotonicTimestamps()
    {
        // Regression guard: TrySendOrLocalEvent used to return a computed-but-not-installed stamp
        // when its single CAS lost, and the next event could then mint the SAME stamp — under
        // contention two calls returned equal (L, C). Consumers that order same-key mutations by
        // timestamp with >= (Kahuna's lock cache-coherence guard) then drop the later mutation.
        // Every returned stamp must be installed, so uniqueness must hold even when Try callers
        // race Send callers on the same clock.
        HybridLogicalClock clock = new();
        const int threads = 8;
        const int perThread = 20_000;

        HLCTimestamp[][] results = new HLCTimestamp[threads][];

        await Task.WhenAll(Enumerable.Range(0, threads).Select(t => Task.Run(() =>
        {
            HLCTimestamp[] mine = new HLCTimestamp[perThread];
            for (int i = 0; i < perThread; i++)
                mine[i] = (t & 1) == 0
                    ? clock.TrySendOrLocalEvent(0)
                    : clock.SendOrLocalEvent(0);
            results[t] = mine;
        })));

        HashSet<HLCTimestamp> unique = [];
        foreach (HLCTimestamp[] batch in results)
        {
            HLCTimestamp previous = HLCTimestamp.Zero;
            foreach (HLCTimestamp ts in batch)
            {
                Assert.True(ts.CompareTo(previous) > 0);
                previous = ts;
                Assert.True(unique.Add(ts));
            }
        }

        Assert.Equal(threads * perThread, unique.Count);
    }

    private static long GetCurrentTime()
    {
        return ((DateTimeOffset)DateTime.UtcNow).ToUnixTimeMilliseconds();
    }
}