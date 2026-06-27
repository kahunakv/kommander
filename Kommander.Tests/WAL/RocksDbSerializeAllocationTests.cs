using Kommander.Data;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.WAL;

/// <summary>
/// The RocksDB write path serializes each
/// <c>RaftLogMessage</c> into a <c>stackalloc</c>/<see cref="System.Buffers.ArrayPool{T}"/> buffer
/// instead of a per-entry <c>MemoryStream</c> + <c>byte[]</c>. These tests pin the two guarantees that
/// change must hold: the on-disk encoding round-trips byte-for-byte (Protobuf writes a canonical encoding
/// regardless of the sink), across the small/large size thresholds; and the serialize path no longer
/// allocates a managed payload-sized buffer per write.
/// </summary>
public sealed class RocksDbSerializeAllocationTests
{
    /// <summary>
    /// Write/read round-trip over the size thresholds that used to switch serialization strategy
    /// (<c>StackallocThreshold</c> = 256, <c>MaxMessageSize</c> = 1024). Each restored <see cref="RaftLog"/>
    /// must equal what was written, proving the span serializer produces identical bytes for tiny, mid, and
    /// large payloads — both single-log fast path and multi-log batch path.
    /// </summary>
    [Theory]
    [InlineData(0)]      // no payload
    [InlineData(16)]     // well below stackalloc threshold
    [InlineData(255)]    // just below stackalloc threshold
    [InlineData(256)]    // at stackalloc threshold
    [InlineData(257)]    // just above -> ArrayPool
    [InlineData(1023)]   // just below old MaxMessageSize branch
    [InlineData(1024)]   // at old MaxMessageSize branch
    [InlineData(8192)]   // large -> ArrayPool
    public void RoundTrips_AcrossSizeThresholds_SingleAndBatch(int payloadSize)
    {
        string path = CreateTempWalPath();

        try
        {
            using RocksDbWAL wal = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false);

            const int partitionId = 3;

            // Single-log fast path (one partition, one log) — exercises the inline Put serialize site.
            RaftLog single = CreateLog(partitionId, id: 1, term: 7, payloadSize, seed: 1);
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(partitionId, [single])]));

            // Multi-log batch path — exercises PutToBatch.
            RaftLog batchA = CreateLog(partitionId, id: 2, term: 7, payloadSize, seed: 2);
            RaftLog batchB = CreateLog(partitionId, id: 3, term: 7, payloadSize, seed: 3);
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(partitionId, [batchA, batchB])]));

            List<RaftLog> restored = wal.ReadLogs(partitionId).OrderBy(l => l.Id).ToList();
            Assert.Equal(3, restored.Count);

            AssertEqualLog(single, restored[0]);
            AssertEqualLog(batchA, restored[1]);
            AssertEqualLog(batchB, restored[2]);
        }
        finally
        {
            DeleteTempWalPath(path);
        }
    }

    /// <summary>
    /// The serialize path no longer allocates a payload-sized managed buffer per write. We write many
    /// mid-sized entries (large enough that the old <c>MemoryStream.ToArray()</c> would have allocated a
    /// fresh array each call) and assert total managed allocation stays well under what one
    /// payload-sized array per write would have cost. The pooled/stack buffer is reused, so the serialize
    /// contribution is effectively zero.
    /// </summary>
    [Fact]
    public void SerializePath_DoesNotAllocatePerWrite()
    {
        string path = CreateTempWalPath();

        try
        {
            using RocksDbWAL wal = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false);

            const int partitionId = 5;
            const int payloadSize = 900; // below old MaxMessageSize, so old path used MemoryStream.ToArray
            const int writes = 1000;

            // Reuse one input so the measured loop allocates nothing test-side (no per-iteration payload
            // array, log, or list). What remains is purely what Write itself allocates. Re-writing the same
            // id overwrites the same key, which still drives the single-log fast-path serialize site.
            RaftLog log = CreateLog(partitionId, id: 1, term: 1, payloadSize, seed: 42);
            List<(int, List<RaftLog>)> input = [(partitionId, [log])];

            // Warm up: JIT, RocksDB column-family handles, ArrayPool buckets.
            for (int i = 0; i < 50; i++)
                wal.Write(input);

            long before = GC.GetAllocatedBytesForCurrentThread();

            for (int i = 0; i < writes; i++)
                wal.Write(input);

            long allocated = GC.GetAllocatedBytesForCurrentThread() - before;

            // The old serialize path alone allocated >= payloadSize bytes per write (the ToArray copy).
            // Require the whole write loop to stay under that floor, proving the per-write payload buffer
            // is gone (remaining allocations are the RaftLogMessage envelope + interop, not the payload).
            long oldSerializeFloor = (long)writes * payloadSize;
            Assert.True(
                allocated < oldSerializeFloor,
                $"Allocated {allocated} bytes over {writes} writes; expected < {oldSerializeFloor} " +
                "(one payload-sized array per write would have exceeded this).");
        }
        finally
        {
            DeleteTempWalPath(path);
        }
    }

    /// <summary>
    /// The read path parses each entry directly from the RocksDB iterator span — no per-entry
    /// <c>RecyclableMemoryStream</c> wrapper. With a tiny payload the deserialized data is negligible, so
    /// the per-read managed allocation is dominated by fixed per-entry objects; the removed stream wrapper
    /// would have added a measurable constant on top. Pin a modest per-read ceiling that the stream-wrapped
    /// path would have exceeded.
    /// </summary>
    [Fact]
    public void DeserializePath_DoesNotAllocateStreamPerRead()
    {
        string path = CreateTempWalPath();

        try
        {
            using RocksDbWAL wal = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false);

            const int partitionId = 7;
            const int payloadSize = 8; // tiny: read allocation is dominated by fixed per-entry overhead
            const int count = 200;
            const int readPasses = 20;

            for (int i = 1; i <= count; i++)
                wal.Write([(partitionId, [CreateLog(partitionId, id: i, term: 1, payloadSize, seed: i)])]);

            // Warm up the read path (JIT, iterator setup).
            for (int p = 0; p < 3; p++)
                _ = wal.ReadLogs(partitionId).Count;

            long before = GC.GetAllocatedBytesForCurrentThread();

            long total = 0;
            for (int p = 0; p < readPasses; p++)
                total += wal.ReadLogs(partitionId).Count;

            long allocated = GC.GetAllocatedBytesForCurrentThread() - before;
            Assert.Equal((long)count * readPasses, total);

            // Per-read overhead is the iterator value, the RaftLogMessage, the RaftLog, its LogType string
            // and tiny payload — a few hundred bytes. A per-read RecyclableMemoryStream wrapper would push
            // this well past 1 KB/read; require it to stay under that bound.
            long reads = (long)count * readPasses;
            long perRead = allocated / reads;
            Assert.True(
                perRead < 1024,
                $"Allocated {perRead} bytes per read ({allocated} over {reads} reads); " +
                "expected < 1024 (a per-read stream wrapper would have exceeded this).");
        }
        finally
        {
            DeleteTempWalPath(path);
        }
    }

    private static void AssertEqualLog(RaftLog expected, RaftLog actual)
    {
        Assert.Equal(expected.Id, actual.Id);
        Assert.Equal(expected.Term, actual.Term);
        Assert.Equal(expected.Type, actual.Type);
        Assert.Equal(expected.LogType, actual.LogType);
        Assert.Equal(expected.Time, actual.Time);

        byte[] expectedData = expected.LogData ?? [];
        byte[] actualData = actual.LogData ?? [];
        Assert.Equal(expectedData, actualData);
    }

    private static RaftLog CreateLog(int partitionId, long id, long term, int payloadSize, int seed)
    {
        byte[]? data = null;
        if (payloadSize > 0)
        {
            data = new byte[payloadSize];
            for (int i = 0; i < payloadSize; i++)
                data[i] = (byte)((i * 31 + seed) & 0xFF);
        }

        return new()
        {
            Id = id,
            Term = term,
            Type = RaftLogType.Committed,
            Time = new HLCTimestamp((int)id, term, (uint)seed),
            LogType = $"partition-{partitionId}",
            LogData = data
        };
    }

    private static string CreateTempWalPath()
    {
        string path = Path.Combine(Path.GetTempPath(), $"kommander-rocksdb-serialize-{Guid.NewGuid():N}");
        Directory.CreateDirectory(path);
        return path;
    }

    private static void DeleteTempWalPath(string path)
    {
        if (Directory.Exists(path))
            Directory.Delete(path, recursive: true);
    }
}
