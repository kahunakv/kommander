
namespace Kommander.Time;

/// <summary>
/// Logical clock
/// </summary>
public sealed class LC
{
    private ulong ticks; // logical clock

    public ulong Increment(int count)
    {
        ticks += (ulong)count;
        return ticks;
    }

    public ulong Increment()
    {
        ticks++;
        return ticks;
    }

    public ulong GetTicks()
    {
        return ticks;
    }
}