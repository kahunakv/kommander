
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

    private static long GetCurrentTime()
    {
        return ((DateTimeOffset)DateTime.UtcNow).ToUnixTimeMilliseconds();
    }
}