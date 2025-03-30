
using System.Text;
using Standart.Hash.xxHash;

namespace Kommander;

/// <summary>
/// Utility to generate a consistent hash for a given value.
/// </summary>
public static class HashUtils
{
    public static long StaticHash(string key, int buckets)
    {
        if (buckets <= 0)
            throw new ArgumentException("buckets must be greater than zero", nameof(buckets));
        
        uint computed = xxHash32.ComputeHash(key);
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
    
    public static int GetHashInRange(string key, int min, int max)
    {
        // Compute the hash value for the string.
        uint hash = xxHash32.ComputeHash(key);
        int range = max - min + 1;
    
        // Map the hash to the desired range.
        return (int)((hash % range) + min);
    }
    
    /// <summary>
    /// Returns a bucket index (0 to numberOfBuckets-1) using rendezvous hashing.
    /// This minimizes key remapping when the number of buckets changes.
    /// </summary>
    /// <param name="key">The key to hash.</param>
    /// <param name="buckets">The total number of buckets.</param>
    /// <returns>The selected bucket index.</returns>
    public static int ConsistentHash(string key, int buckets)
    {
        if (buckets <= 0)
            throw new ArgumentException("numberOfBuckets must be greater than zero", nameof(buckets));

        int bestBucket = -1;
        uint bestHash = 0;
        byte[] keyBytes = Encoding.UTF8.GetBytes(key);

        // Evaluate every bucket to see which gives the highest hash score
        for (int bucket = 0; bucket < buckets; bucket++)
        {
            uint hash = xxHash32.ComputeHash(keyBytes, bucket);

            if (bucket == 0 || hash > bestHash)
            {
                bestHash = hash;
                bestBucket = bucket;
            }
        }
        
        return bestBucket;
    }
}