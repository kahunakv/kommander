
using System.Text.Json.Serialization;

namespace Kommander.Time;

/// <summary>
/// Represents a unique point in time given by the Hybrid Logical Clock (HLC)
/// </summary>
public readonly record struct HLCTimestamp : IComparable<HLCTimestamp>
{
    public static readonly HLCTimestamp Zero = new(0, 0);

    public long L { get; }

    public uint C { get; }

    [JsonConstructor]
    public HLCTimestamp(long l, uint c)
    {
        L = l;
        C = c;
    }

    /// <summary>
    /// Compares the current instance of <see cref="HLCTimestamp"/> with another instance and determines their relative order.
    /// </summary>
    /// <param name="other">The <see cref="HLCTimestamp"/> instance to compare with the current instance.</param>
    /// <returns>
    /// A signed integer that indicates the relative order of the instances being compared.
    /// - Returns 0 if the current instance is equal to <paramref name="other"/>.
    /// - Returns -1 if the current instance is less than <paramref name="other"/>.
    /// - Returns 1 if the current instance is greater than <paramref name="other"/>.
    /// </returns>
    public int CompareTo(HLCTimestamp other)
    {
        if (L == other.L)
        {
            if (C == other.C)
                return 0;

            if (C < other.C)
                return -1;

            if (C > other.C)
                return 1;
        }

        if (L < other.L)
            return -1;

        return 1;
    }

    public bool IsNull()
    {
        return L == 0 && C == 0;
    }

    public override string ToString()
    {
        return $"HLC({L}:{C})";
    }

    public static HLCTimestamp operator +(HLCTimestamp a, int b) => new(a.L + b, a.C);
    
    public static HLCTimestamp operator +(HLCTimestamp a, TimeSpan b) => new(a.L + (long)b.TotalMilliseconds, a.C);

    public static HLCTimestamp operator -(HLCTimestamp a, int b) => new(a.L - b, a.C);
    
    public static HLCTimestamp operator -(HLCTimestamp a, TimeSpan b) => new(a.L - (long)b.TotalMilliseconds, a.C);
    
    public static TimeSpan operator -(HLCTimestamp a, HLCTimestamp b) => TimeSpan.FromMilliseconds(a.L - b.L);
    
    public static bool operator >(HLCTimestamp a, HLCTimestamp b) => a.CompareTo(b) > 0;
    
    public static bool operator >=(HLCTimestamp a, HLCTimestamp b) => a.CompareTo(b) >= 0;
    
    public static bool operator <=(HLCTimestamp a, HLCTimestamp b) => a.CompareTo(b) <= 0;
    
    public static bool operator <(HLCTimestamp a, HLCTimestamp b) => a.CompareTo(b) < 0;
}