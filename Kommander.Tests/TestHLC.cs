using Kommander.Time;

namespace Kommander.Tests.HLC;

public class TestHLC
{
    [Fact]
    public void AddX()
    {
        var x = new HLCTimestamp(GetCurrentTime(), 0);
        
        var p = x + 90000;
        
        Console.WriteLine(DateTime.UtcNow);
        Console.WriteLine(DateTimeOffset.FromUnixTimeMilliseconds(p.L).UtcDateTime);
    }
    
    public long GetCurrentTime()
    {
        return ((DateTimeOffset)DateTime.UtcNow).ToUnixTimeMilliseconds();
    }
}