
using System.Text.Json.Serialization;

namespace Kommander.Time;

/// <summary>
/// Represents a unique point in time given by the Hybrid Logical Clock (HLC)
/// </summary>
public readonly record struct HLCTimestamp : IComparable<HLCTimestamp>
{
    /// <summary>
    /// Represents the default or initial instance of the <see cref="HLCTimestamp"/> structure with all its components
    /// (logical time, physical timestamp, and counter) set to zero.
    /// </summary>
    /// <remarks>
    /// This property is useful as a baseline value or as a default timestamp when no other value is provided.
    /// </remarks>
    public static readonly HLCTimestamp Zero = new(0, 0, 0);

    /// <summary>
    /// Represents a component of the Hybrid Logical Clock (HLC) timestamp that contributes to the unique node id dimension.
    /// </summary>
    /// <remarks>
    /// This property prevents conflicts when the same HLC is generated on different nodes.
    /// </remarks>
    public int N { get; }

    /// <summary>
    /// Gets the physical timestamp component of the <see cref="HLCTimestamp"/> structure,
    /// representing the physical moment in time as a long value (typically in milliseconds since a defined epoch).
    /// </summary>
    /// <remarks>
    /// This value is a critical part of Hybrid Logical Clocks (HLC), used to maintain
    /// causality and partial order within distributed systems while incorporating physical time.
    /// </remarks>
    public long L { get; }

    /// <summary>
    /// Represents the counter component of the <see cref="HLCTimestamp"/> structure,
    /// which is used to differentiate between events with the same physical timestamp in the Hybrid Logical Clock system.
    /// </summary>
    /// <remarks>
    /// This property helps maintain causality and ensures uniqueness in scenarios where multiple events occur with identical physical timestamps.
    /// </remarks>
    public uint C { get; }

    [JsonConstructor]
    public HLCTimestamp(int n, long l, uint c)
    {
        N = n;
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
        if (N == other.N)
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
        }

        if (L == other.L)
        {
            if (C > other.C)
                return 1;            
            return -1;
        }
        
        if (L < other.L)
            return -1;

        return 1;
    }

    /// <summary>
    /// Determines whether the current instance of <see cref="HLCTimestamp"/> represents a null or default timestamp.
    /// </summary>
    /// <returns>
    /// <c>true</c> if the current instance has all its components (node id, physical timestamp, and counter) set to zero; otherwise, <c>false</c>.
    /// </returns>
    public bool IsNull()
    {
        return N == 0 && L == 0 && C == 0;
    }

    /// <summary>
    /// Returns a string representation of the current <see cref="HLCTimestamp"/> instance.
    /// </summary>
    /// <returns>
    /// A string in the format "HLC(N:L:C)", where N, L, and C represent the respective components of the timestamp:
    /// - N: Node ID
    /// - L: Physical timestamp
    /// - C: Counter
    /// </returns>
    public override string ToString()
    {
        return $"HLC({N}:{L}:{C})";
    }

    public static HLCTimestamp operator +(HLCTimestamp a, int b) => new(a.N, a.L + b, a.C);
    
    public static HLCTimestamp operator +(HLCTimestamp a, TimeSpan b) => new(a.N, a.L + (long)b.TotalMilliseconds, a.C);

    public static HLCTimestamp operator -(HLCTimestamp a, int b) => new(a.N, a.L - b, a.C);
    
    public static HLCTimestamp operator -(HLCTimestamp a, TimeSpan b) => new(a.N, a.L - (long)b.TotalMilliseconds, a.C);
    
    public static TimeSpan operator -(HLCTimestamp a, HLCTimestamp b) => TimeSpan.FromMilliseconds(a.L - b.L);
    
    public static bool operator >(HLCTimestamp a, HLCTimestamp b) => a.CompareTo(b) > 0;
    
    public static bool operator >=(HLCTimestamp a, HLCTimestamp b) => a.CompareTo(b) >= 0;
    
    public static bool operator <=(HLCTimestamp a, HLCTimestamp b) => a.CompareTo(b) <= 0;
    
    public static bool operator <(HLCTimestamp a, HLCTimestamp b) => a.CompareTo(b) < 0;
}