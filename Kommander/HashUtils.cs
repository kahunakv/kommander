
using Standart.Hash.xxHash;

namespace Kommander;

/// <summary>
/// Utility to generate a consistent hash for a given value.
/// </summary>
public static class HashUtils
{
    public static long ConsistentHash(string value, int divisor)
    {
        uint computed = xxHash32.ComputeHash(value);
        return computed % divisor;
    }
}