
using Standart.Hash.xxHash;

namespace Kommander;

/// <summary>
/// Utility to generate a consistent hash for a given value.
/// </summary>
public static class HashUtils
{
    public static long StaticHash(string value, int numberBuckets)
    {
        uint computed = xxHash32.ComputeHash(value);
        return computed % numberBuckets;
    }

    public static long PrefixedHash(string value, char separator, int divisor)
    {
        int pointer = value.IndexOf(separator);
        if (pointer == -1)
            return ConsistentHash(value, divisor);
        
        string prefix = value[..pointer];
        return ConsistentHash(prefix, divisor);
    }
    
    public static long InversePrefixedHash(string value, char separator, int divisor)
    {
        int pointer = value.LastIndexOf(separator);
        if (pointer == -1)
            return ConsistentHash(value, divisor);
        
        string prefix = value[..pointer];
        return ConsistentHash(prefix, divisor);
    }
    
    public static int GetHashInRange(string input, int min, int max)
    {
        // Compute the hash value for the string.
        uint hash = xxHash32.ComputeHash(input);
        int range = max - min + 1;
    
        // Map the hash to the desired range.
        return (int)((hash % range) + min);
    }
    
    /// <summary>
    /// Returns a bucket index (0 to numberOfBuckets-1) using rendezvous hashing.
    /// This minimizes key remapping when the number of buckets changes.
    /// </summary>
    /// <param name="key">The key to hash.</param>
    /// <param name="numberOfBuckets">The total number of buckets.</param>
    /// <returns>The selected bucket index.</returns>
    public static int ConsistentHash(string key, int numberOfBuckets)
    {
        if (numberOfBuckets <= 0)
            throw new ArgumentException("numberOfBuckets must be greater than zero", nameof(numberOfBuckets));

        int bestBucket = -1;
        uint bestHash = 0;

        // Evaluate every bucket to see which gives the highest hash score
        for (int bucket = 0; bucket < numberOfBuckets; bucket++)
        {
            // Combine the key with the bucket identifier to produce a unique hash per bucket
            string combinedKey = $"{key}:{bucket}";
            uint hash = xxHash32.ComputeHash(combinedKey);

            if (bucket == 0 || hash > bestHash)
            {
                bestHash = hash;
                bestBucket = bucket;
            }
        }
        
        return bestBucket;
    }
}