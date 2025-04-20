
using System.Runtime.CompilerServices;
using Kommander.Data;
using Kommander.Time;

namespace Kommander.Tests.Time;

public class TestHLC
{
    [Fact]
    public void TestAddOperation()
    {
        HLCTimestamp x = new(GetCurrentTime(), 0);
        HLCTimestamp p = x + 90000;
        
        Assert.True(p > x);
    }
    
    [Fact]
    public void TestAddOperation2()
    {
        HLCTimestamp x = new(GetCurrentTime(), 0);
        HLCTimestamp p = x + TimeSpan.FromMilliseconds(10000);
        
        Assert.True(p > x);
    }
    
    [Fact]
    public void TestHLC1()
    {
        HybridLogicalClock clock = new();
        
        Dictionary<HLCTimestamp, bool> hlc = new();
        
        for (int i = 0; i < 100; i++)
            hlc.Add(clock.SendOrLocalEvent(), true);
            
        Assert.Equal(100, hlc.Count);
    }

    [Fact]
    public void TestHLCCompare()
    {
        HLCTimestamp t1 = new(1744147995701, 1);
        HLCTimestamp t2 = new(1744147995701, 2);
        
        Assert.Equal(-1, t1.CompareTo(t2));
    }
    
    [Fact]
    public void TestHLCCompare2()
    {
        HLCTimestamp t1 = new(1744147995701, 1);
        HLCTimestamp t2 = new(1744147995701, 1);
        
        Assert.Equal(0, t1.CompareTo(t2));
    }
    
    [Fact]
    public void TestHLCCompare3()
    {
        HLCTimestamp t1 = new(1744147995701, 2);
        HLCTimestamp t2 = new(1744147995701, 1);
        
        Assert.Equal(1, t1.CompareTo(t2));
    }
    
    private static long GetCurrentTime()
    {
        return ((DateTimeOffset)DateTime.UtcNow).ToUnixTimeMilliseconds();
    }
}