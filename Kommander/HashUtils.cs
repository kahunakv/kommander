
using System.Buffers;
using System.IO.Hashing;
using System.Text;

namespace Kommander;

/// <summary>
/// Provides various utilities for computing hash codes and bucket-based hash partitioning.
/// </summary>
public static class HashUtils
{
    /// <summary>
    /// Computes a simple hash value for the given key using the xxHash32 algorithm.
    /// </summary>
    /// <param name="key">The input string to compute the hash for.</param>
    /// <returns>The computed hash value as a 32-bit integer.</returns>
    public static int SmallSimpleHash(string key)
    {
        return (int)XxHash32Utf8(key);
    }

    /// <summary>
    /// Computes a hash value for the given key using xxHash64 algorithm.
    /// </summary>
    /// <param name="key">The input string to compute the hash for.</param>
    /// <returns>The computed hash value as an unsigned 64-bit integer.</returns>
    public static ulong SimpleHash(string key)
    {
        return XxHash64Utf8(key);
    }

    /// <summary>
    /// Computes a hash value for the given key using xxHash64 algorithm and
    /// maps it to a fixed number of buckets.
    /// </summary>
    /// <param name="key">The input string to compute the hash for.</param>
    /// <param name="buckets">The total number of buckets to map the hash value into. Must be greater than zero.</param>
    /// <returns>The computed hash value mapped to a bucket index as an unsigned 64-bit integer.</returns>
    /// <exception cref="ArgumentException">Thrown when the provided number of buckets is less than or equal to zero.</exception>
    public static ulong StaticHash(string key, ulong buckets)
    {
        if (buckets <= 0)
            throw new ArgumentException("buckets must be greater than zero", nameof(buckets));

        ulong computed = XxHash64Utf8(key);
        return computed % buckets;
    }

    /// <summary>
    /// Computes the hash of a specified key, utilizing the part of the key before the specified separator.
    /// If the separator is not found, the entire key is hashed.
    /// </summary>
    /// <param name="key">The input string to be hashed.</param>
    /// <param name="separator">The character used to separate the prefix from the rest of the key.</param>
    /// <param name="buckets">The total number of buckets for computing the hash partition.</param>
    /// <returns>The computed hash value mapped to the range of buckets.</returns>
    /// <exception cref="ArgumentException">Thrown if the number of buckets is less than or equal to zero.</exception>
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

    /// <summary>
    /// Computes a hash value for the prefix of the given key,
    /// separated by the specified character, using the xxHash64 algorithm.
    /// If the separator is not found, computes the hash for the entire key.
    /// </summary>
    /// <param name="key">The input string to compute the hash for.</param>
    /// <param name="separator">The character used to determine the prefix of the input string.</param>
    /// <returns>The computed hash value for the prefix or the whole key as an unsigned 64-bit integer.</returns>
    public static ulong PrefixedStaticHash(string key, char separator)
    {
        int pointer = key.IndexOf(separator);
        if (pointer == -1)
            return SimpleHash(key);

        string prefix = key[..pointer];
        return SimpleHash(prefix);
    }

    /// <summary>
    /// Computes a hash value for a string by extracting and hashing its prefix based on the specified separator.
    /// If the separator is not found in the string, the entire string is hashed.
    /// </summary>
    /// <param name="key">The input string from which the prefix will be extracted and hashed.</param>
    /// <param name="separator">The character used to determine the prefix boundary in the input string.</param>
    /// <returns>The computed hash value of the prefix as an unsigned 64-bit integer.</returns>
    public static ulong InversePrefixedStaticHash(string key, char separator)
    {
        int pointer = key.LastIndexOf(separator);
        if (pointer == -1)
            return SimpleHash(key);

        string prefix = key[..pointer];
        return SimpleHash(prefix);
    }

    /// <summary>
    /// Computes a hash value for the prefix of the given key, separated by the specified character,
    /// and distributes the value across a specified number of buckets.
    /// If the separator is not found in the key, the entire key is used for hashing.
    /// </summary>
    /// <param name="key">The input string used to generate the hash value.</param>
    /// <param name="separator">The character used to identify the prefix within the key.</param>
    /// <param name="buckets">The number of buckets to partition the hash value across. Must be greater than zero.</param>
    /// <returns>The computed bucket index as a signed 64-bit integer.</returns>
    /// <exception cref="ArgumentException">Thrown when the number of buckets is less than or equal to zero.</exception>
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

    /// <summary>
    /// Computes a hash value for the given key using the xxHash64 algorithm
    /// and maps it to a specified range between a minimum and maximum value (inclusive).
    /// </summary>
    /// <param name="key">The input string to compute the hash for.</param>
    /// <param name="min">The minimum value of the target range.</param>
    /// <param name="max">The maximum value of the target range. Must be greater than or equal to the minimum value.</param>
    /// <returns>The computed hash value mapped to the specified range as an unsigned 64-bit integer.</returns>
    public static ulong GetHashInRange(string key, ulong min, ulong max)
    {
        ulong hash = XxHash64Utf8(key);
        ulong range = max - min + 1;
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

        // Use two fixed seeds to produce two 32-bit hashes.
        const int seed1 = unchecked((int)0xAAAAAAAA);
        const int seed2 = unchecked((int)0x55555555);

        int maxBytes = Encoding.UTF8.GetMaxByteCount(key.Length);
        byte[]? rented = maxBytes > 256 ? ArrayPool<byte>.Shared.Rent(maxBytes) : null;
        Span<byte> buffer = rented ?? stackalloc byte[256];
        try
        {
            int written = Encoding.UTF8.GetBytes(key, buffer);

            uint hash1 = XxHash32.HashToUInt32(buffer[..written], seed1);
            uint hash2 = XxHash32.HashToUInt32(buffer[..written], seed2);

            // Combine the two 32-bit hashes to form a 64-bit hash.
            ulong combinedHash = ((ulong)hash1 << 32) | hash2;

            return JumpConsistentHash(combinedHash, numBuckets);
        }
        finally
        {
            if (rented is not null)
                ArrayPool<byte>.Shared.Return(rented);
        }
    }

    /// <summary>
    /// UTF-8 encodes <paramref name="key"/> into a stack buffer (falling back to a pooled array for
    /// keys whose worst-case byte count exceeds 256) and returns its xxHash32. Avoids the per-call
    /// <c>byte[]</c> allocation of <c>Encoding.UTF8.GetBytes(string)</c>; the span never escapes.
    /// </summary>
    private static uint XxHash32Utf8(string key, uint seed = 0)
    {
        int maxBytes = Encoding.UTF8.GetMaxByteCount(key.Length);
        byte[]? rented = maxBytes > 256 ? ArrayPool<byte>.Shared.Rent(maxBytes) : null;
        Span<byte> buffer = rented ?? stackalloc byte[256];
        try
        {
            int written = Encoding.UTF8.GetBytes(key, buffer);
            return XxHash32.HashToUInt32(buffer[..written], unchecked((int)seed));
        }
        finally
        {
            if (rented is not null)
                ArrayPool<byte>.Shared.Return(rented);
        }
    }

    /// <summary>
    /// UTF-8 encodes <paramref name="key"/> into a stack buffer (falling back to a pooled array for
    /// keys whose worst-case byte count exceeds 256) and returns its xxHash64. Avoids the per-call
    /// <c>byte[]</c> allocation of <c>Encoding.UTF8.GetBytes(string)</c>; the span never escapes.
    /// </summary>
    private static ulong XxHash64Utf8(string key)
    {
        int maxBytes = Encoding.UTF8.GetMaxByteCount(key.Length);
        byte[]? rented = maxBytes > 256 ? ArrayPool<byte>.Shared.Rent(maxBytes) : null;
        Span<byte> buffer = rented ?? stackalloc byte[256];
        try
        {
            int written = Encoding.UTF8.GetBytes(key, buffer);
            return XxHash64.HashToUInt64(buffer[..written]);
        }
        finally
        {
            if (rented is not null)
                ArrayPool<byte>.Shared.Return(rented);
        }
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
