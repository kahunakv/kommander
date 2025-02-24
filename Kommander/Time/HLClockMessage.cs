
namespace Kommander.Time;

/// <summary>
/// A message class to hold the message's timestamp
/// </summary>
public record struct HLClockMessage
{
    public long L { get; }

    public uint C { get; }

    public HLClockMessage(long l, uint c)
    {
        L = l;
        C = c;
    }
}