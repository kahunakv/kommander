
using Standart.Hash.xxHash;

namespace Kommander;

public static class HashUtils
{
    public static long ConsistentHash(string value, int divisor)
    {
        uint computed = xxHash32.ComputeHash(value);
        return computed % divisor;
    }
}