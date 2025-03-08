
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
    
    public static int GetHashInRange(string input, int min, int max)
    {
        // Compute the hash value for the string.
        uint hash = xxHash32.ComputeHash(input);
        int range = max - min + 1;
    
        // Map the hash to the desired range.
        return (int)((hash % range) + min);
    }
}