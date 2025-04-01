
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
    
    private static long GetCurrentTime()
    {
        return ((DateTimeOffset)DateTime.UtcNow).ToUnixTimeMilliseconds();
    }
}