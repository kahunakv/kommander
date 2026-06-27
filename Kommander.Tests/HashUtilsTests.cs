using Kommander;

namespace Kommander.Tests;

/// <summary>
/// <see cref="HashUtils.ConsistentHash"/> now encodes the
/// key into a <c>stackalloc</c>/<see cref="System.Buffers.ArrayPool{T}"/> buffer and hashes via the
/// <see cref="System.ReadOnlySpan{T}"/> xxHash overloads instead of allocating a <c>byte[]</c> +
/// <c>ArraySegment</c> per call. These are the drift guards: the selected bucket must be byte-for-byte
/// stable for a fixed corpus across several bucket counts (the golden values below were captured from the
/// pre-refactor implementation for every non-empty key), and the path must not allocate for short keys.
/// </summary>
public sealed class HashUtilsTests
{
    // Golden bucket assignments captured from the pre-refactor ConsistentHash for buckets {1,2,3,7,16,1024}.
    // Any change to these values means the hash drifted and must be treated as a regression.
    public static TheoryData<string, int[]> Golden() => new()
    {
        { "a", [0, 1, 1, 1, 14, 726] },
        { "hello", [0, 1, 1, 4, 7, 374] },
        { "partition-key-12345", [0, 0, 2, 6, 10, 659] },
        { "ünïcødé-Ω≈ç√", [0, 0, 0, 0, 0, 934] },
        { new string('x', 500), [0, 1, 1, 4, 11, 178] },
    };

    private static readonly int[] BucketCounts = [1, 2, 3, 7, 16, 1024];

    [Theory]
    [MemberData(nameof(Golden))]
    public void ConsistentHash_BucketAssignment_IsStable(string key, int[] expectedBuckets)
    {
        for (int i = 0; i < BucketCounts.Length; i++)
            Assert.Equal(expectedBuckets[i], HashUtils.ConsistentHash(key, BucketCounts[i]));
    }

    /// <summary>
    /// The empty key used to throw (the old xxHash <c>ArraySegment</c> overload indexes <c>[0]</c> without a
    /// length guard); the span overload hashes the empty span cleanly. This pins the now-stable assignment so
    /// the empty/empty-prefix case stays deterministic.
    /// </summary>
    [Fact]
    public void ConsistentHash_EmptyKey_ReturnsStableBucket_NoThrow()
    {
        int[] expected = [0, 1, 2, 2, 2, 225];
        for (int i = 0; i < BucketCounts.Length; i++)
            Assert.Equal(expected[i], HashUtils.ConsistentHash("", BucketCounts[i]));
    }

    [Fact]
    public void ConsistentHash_RejectsNonPositiveBuckets()
    {
        Assert.Throws<ArgumentException>(() => HashUtils.ConsistentHash("a", 0));
        Assert.Throws<ArgumentException>(() => HashUtils.ConsistentHash("a", -1));
    }

    /// <summary>
    /// A short key (well under the stackalloc threshold) must hash without any managed allocation — no
    /// per-call UTF-8 <c>byte[]</c> and no <c>ArraySegment</c> box.
    /// </summary>
    [Fact]
    public void ConsistentHash_ShortKey_DoesNotAllocate()
    {
        const string key = "partition-key-12345";
        const int iterations = 10_000;

        // Warm up the JIT before measuring.
        for (int i = 0; i < 100; i++)
            _ = HashUtils.ConsistentHash(key, 64);

        long before = GC.GetAllocatedBytesForCurrentThread();

        int sink = 0;
        for (int i = 0; i < iterations; i++)
            sink += HashUtils.ConsistentHash(key, 64);

        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.True(sink >= 0); // keep the loop from being optimized away
        Assert.True(
            allocated == 0,
            $"ConsistentHash allocated {allocated} bytes over {iterations} short-key calls; expected 0.");
    }
}
