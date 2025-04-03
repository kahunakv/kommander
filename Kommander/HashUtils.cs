
using System.Text;
using Standart.Hash.xxHash;

namespace Kommander;

/// <summary>
/// Utility to generate a consistent hash for a given value.
/// </summary>
public static class HashUtils
{
    public static ulong SimpleHash(string key)
    {
        return xxHash64.ComputeHash(key);
    }
    
    public static ulong StaticHash(string key, ulong buckets)
    {
        if (buckets <= 0)
            throw new ArgumentException("buckets must be greater than zero", nameof(buckets));
        
        ulong computed = xxHash64.ComputeHash(key);
        return computed % buckets;
    }

    public static long PrefixedHash(string key, char separator, int buckets)
    {
        if (buckets <= 0)
            throw new ArgumentException("buckets must be greater than zero", nameof(buckets));
        
        int pointer = key.IndexOf(separator);
        if (pointer == -1)
            return ConsistentHash(key, buckets);
        
        string prefix = key[..pointer];
        return ConsistentHash(prefix, buckets);
    }
    
    public static long InversePrefixedHash(string key, char separator, int buckets)
    {
        if (buckets <= 0)
            throw new ArgumentException("buckets must be greater than zero", nameof(buckets));
        
        int pointer = key.LastIndexOf(separator);
        if (pointer == -1)
            return ConsistentHash(key, buckets);
        
        string prefix = key[..pointer];
        return ConsistentHash(prefix, buckets);
    }
    
    public static ulong GetHashInRange(string key, ulong min, ulong max)
    {
        // Compute the hash value for the string.
        ulong hash = xxHash64.ComputeHash(key);
        ulong range = max - min + 1;
    
        // Map the hash to the desired range.
        return ((hash % range) + min);
    }
    
    /// <summary>
    /// Returns a bucket index (0 to numBuckets-1) using Jump Consistent Hash.
    /// This version is optimized for many buckets and uses fixed seeds to ensure
    /// consistency across runs.
    /// </summary>
    /// <param name="key">The key to hash.</param>
    /// <param name="numBuckets">The total number of buckets.</param>
    /// <returns>The selected bucket index.</returns>
    public static int ConsistentHash(string key, int numBuckets)
    {
        if (numBuckets <= 0)
            throw new ArgumentException("numBuckets must be greater than 0", nameof(numBuckets));
        
        // Convert key to bytes once.
        byte[] keyBytes = Encoding.UTF8.GetBytes(key);

        // Use two fixed seeds to produce two 32-bit hashes.
        const uint seed1 = 0xAAAAAAAA;
        const uint seed2 = 0x55555555;

        ulong hash1 = xxHash32.ComputeHash(keyBytes, seed1);
        // Use the ArraySegment overload with a fixed seed for consistency.
        ulong hash2 = xxHash32.ComputeHash(new ArraySegment<byte>(keyBytes), seed2);

        // Combine the two 32-bit hashes to form a 64-bit hash.
        ulong combinedHash = ((ulong)hash1 << 32) | hash2;

        return JumpConsistentHash(combinedHash, numBuckets);
    }

    /// <summary>
    /// Implements the Jump Consistent Hash algorithm.
    /// </summary>
    /// <param name="key">The 64-bit hash key.</param>
    /// <param name="numBuckets">The total number of buckets.</param>
    /// <returns>The selected bucket index.</returns>
    private static int JumpConsistentHash(ulong key, int numBuckets)
    {
        long b = -1, j = 0;
        while (j < numBuckets)
        {
            b = j;
            key = key * 2862933555777941757UL + 1;
            j = (long)((b + 1) * (2147483648.0 / ((double)((key >> 33) + 1))));
        }
        return (int)b;
    }
}