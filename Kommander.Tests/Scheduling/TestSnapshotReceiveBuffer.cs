
namespace Kommander.Tests.Scheduling;

/// <summary>
/// Unit tests for <see cref="SnapshotReceiveBuffer"/>, the segmented storage a snapshot-receive session
/// accumulates into.
/// </summary>
/// <remarks>
/// The buffer replaced a <see cref="MemoryStream"/>, so the tests that matter are the ones that pin the
/// behaviour an application importer relies on — exact bytes back, across segment boundaries, through
/// every read shape — and the bound that motivated the change: allocated capacity must track the payload
/// instead of doubling past it.
/// </remarks>
public sealed class TestSnapshotReceiveBuffer
{
    private const int SegmentSize = SnapshotReceiveBuffer.SegmentSize;

    private static byte[] Pattern(int length, int seed = 0)
    {
        byte[] data = new byte[length];

        for (int i = 0; i < length; i++)
            data[i] = (byte)((i * 31 + seed) & 0xFF);

        return data;
    }

    [Fact]
    public void EmptyBuffer_HasNoLengthAndNoCapacity()
    {
        using SnapshotReceiveBuffer buffer = new();

        Assert.Equal(0, buffer.Length);
        Assert.Equal(0, buffer.Position);
        Assert.Equal(0, buffer.AllocatedByteCount);
        Assert.Equal(-1, buffer.ReadByte());
    }

    [Theory]
    [InlineData(1)]
    [InlineData(SegmentSize - 1)]
    [InlineData(SegmentSize)]
    [InlineData(SegmentSize + 1)]
    [InlineData((SegmentSize * 3) + 17)]
    public void ChunkedWrites_ReadBackByteForByte(int totalLength)
    {
        byte[] expected = Pattern(totalLength);

        using SnapshotReceiveBuffer buffer = new();

        // Written in awkward slices so at least one write straddles a segment boundary.
        int offset = 0;
        int slice = 7919;
        while (offset < totalLength)
        {
            int take = Math.Min(slice, totalLength - offset);
            buffer.Write(expected.AsSpan(offset, take));
            offset += take;
        }

        Assert.Equal(totalLength, buffer.Length);

        buffer.Position = 0;
        byte[] actual = new byte[totalLength];
        int read = 0;
        while (read < totalLength)
        {
            int n = buffer.Read(actual.AsSpan(read));
            Assert.True(n > 0, "read returned no bytes before the end of the buffer");
            read += n;
        }

        Assert.Equal(expected, actual);
        Assert.Equal(0, buffer.Read(actual.AsSpan(0, 1)));
    }

    [Fact]
    public async Task CopyToAsync_ReproducesEveryByte()
    {
        byte[] expected = Pattern((SegmentSize * 2) + 1234, seed: 5);

        using SnapshotReceiveBuffer buffer = new();
        buffer.Write(expected);
        buffer.Position = 0;

        using MemoryStream destination = new();
        await buffer.CopyToAsync(destination, TestContext.Current.CancellationToken);

        Assert.Equal(expected, destination.ToArray());
    }

    [Fact]
    public void CopyTo_ReproducesEveryByte()
    {
        byte[] expected = Pattern(SegmentSize + 3, seed: 9);

        using SnapshotReceiveBuffer buffer = new();
        buffer.Write(expected);
        buffer.Position = 0;

        using MemoryStream destination = new();
        buffer.CopyTo(destination);

        Assert.Equal(expected, destination.ToArray());
    }

    [Fact]
    public void Seek_RepositionsReadsWithoutDisturbingContent()
    {
        byte[] expected = Pattern(SegmentSize + 500, seed: 3);

        using SnapshotReceiveBuffer buffer = new();
        buffer.Write(expected);

        Assert.Equal(0, buffer.Seek(0, SeekOrigin.Begin));
        Assert.Equal(expected[0], (byte)buffer.ReadByte());

        long midpoint = SegmentSize - 1;
        buffer.Seek(midpoint, SeekOrigin.Begin);

        byte[] tail = new byte[3];
        Assert.Equal(3, buffer.Read(tail));
        Assert.Equal(expected[(int)midpoint..((int)midpoint + 3)], tail);

        buffer.Seek(-1, SeekOrigin.End);
        Assert.Equal(expected[^1], (byte)buffer.ReadByte());
        Assert.Equal(-1, buffer.ReadByte());
    }

    /// <summary>
    /// The bound the change exists for: a <see cref="MemoryStream"/> holding this payload would have a
    /// backing array of the next power of two above it, and would have copied the whole content on every
    /// growth step. Segments overshoot by less than one segment and copy nothing.
    /// </summary>
    [Fact]
    public void AllocatedCapacity_ExceedsThePayloadByLessThanOneSegment()
    {
        using SnapshotReceiveBuffer buffer = new();

        byte[] chunk = Pattern(64 * 1024, seed: 11);
        for (int i = 0; i < 40; i++)
            buffer.Write(chunk);

        long payload = buffer.Length;

        Assert.True(
            buffer.AllocatedByteCount >= payload,
            $"capacity {buffer.AllocatedByteCount} must cover the payload {payload}");

        Assert.True(
            buffer.AllocatedByteCount - payload < SegmentSize,
            $"capacity {buffer.AllocatedByteCount} overshoots the payload {payload} by a whole segment");
    }

    [Fact]
    public void WriteAwayFromTheEnd_IsRefused()
    {
        using SnapshotReceiveBuffer buffer = new();
        buffer.Write(Pattern(128));
        buffer.Position = 0;

        Assert.Throws<NotSupportedException>(() => buffer.Write(Pattern(8)));
    }

    [Fact]
    public void SetLength_IsRefused()
    {
        using SnapshotReceiveBuffer buffer = new();

        Assert.Throws<NotSupportedException>(() => buffer.SetLength(10));
    }

    [Fact]
    public void SeekBeforeStart_IsRefused()
    {
        using SnapshotReceiveBuffer buffer = new();
        buffer.Write(Pattern(16));

        Assert.Throws<IOException>(() => buffer.Seek(-1, SeekOrigin.Begin));
    }

    [Fact]
    public void ReadPastTheEnd_ReturnsZeroRatherThanSegmentPadding()
    {
        using SnapshotReceiveBuffer buffer = new();
        buffer.Write(Pattern(10));

        // The tail of the segment is allocated but not written; a read must stop at the logical length
        // rather than hand the importer the zero padding behind it.
        buffer.Position = 0;
        byte[] destination = new byte[SegmentSize];

        Assert.Equal(10, buffer.Read(destination));
        Assert.Equal(0, buffer.Read(destination));
    }

    [Fact]
    public void DisposedBuffer_RefusesFurtherUse()
    {
        SnapshotReceiveBuffer buffer = new();
        buffer.Write(Pattern(4));
        buffer.Dispose();

        Assert.Throws<ObjectDisposedException>(() => buffer.Position);
        Assert.Throws<ObjectDisposedException>(() => buffer.Write(Pattern(4)));
    }

    // ── spilled to a file ──────────────────────────────────────────────────────

    /// <summary>
    /// After a spill the buffer reads, seeks and copies exactly as before — the bytes staged before the spill
    /// and those appended after it — while holding no segments in memory, and disposing it deletes the file.
    /// </summary>
    [Fact]
    public async Task Spilled_BehavesLikeTheInMemoryBuffer_AndDisposingDeletesTheFile()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string path = Path.Combine(Path.GetTempPath(), "kommander-spill-" + Guid.NewGuid().ToString("N") + ".snapshot-staging");

        byte[] before = Pattern(SnapshotReceiveBuffer.SegmentSize + 1000, seed: 1);
        byte[] after = Pattern(3 * SnapshotReceiveBuffer.SegmentSize + 7, seed: 2);
        byte[] expected = [.. before, .. after];

        SnapshotReceiveBuffer buffer = new();
        buffer.Write(before);
        Assert.Equal(2L * SnapshotReceiveBuffer.SegmentSize, buffer.AllocatedByteCount);

        buffer.SpillTo(path);
        Assert.True(buffer.IsSpilled);
        Assert.Equal(0, buffer.AllocatedByteCount);
        Assert.True(File.Exists(path));

        buffer.Write(after);
        Assert.Equal(expected.Length, buffer.Length);

        buffer.Position = 0;
        Assert.Equal(expected, await ReadAllAsync(buffer, ct));

        buffer.Seek(before.Length - 3, SeekOrigin.Begin);
        Assert.Equal(expected[before.Length - 3], buffer.ReadByte());
        byte[] window = new byte[10];
        Assert.Equal(10, buffer.Read(window, 0, 10));
        Assert.Equal(expected[(before.Length - 2)..(before.Length + 8)], window);

        buffer.Seek(-5, SeekOrigin.End);
        Assert.Equal(5, buffer.Read(new byte[100], 0, 100));
        Assert.Equal(-1, buffer.ReadByte());

        buffer.Position = 0;
        using (MemoryStream copy = new())
        {
            buffer.CopyTo(copy);
            Assert.Equal(expected, copy.ToArray());
        }

        buffer.Position = 0;
        using (MemoryStream copy = new())
        {
            await buffer.CopyToAsync(copy, ct);
            Assert.Equal(expected, copy.ToArray());
        }

        buffer.Dispose();
        Assert.False(File.Exists(path));
    }

    [Fact]
    public void SpillThatCannotCreateItsFile_LeavesTheBufferInMemoryAndIntact()
    {
        byte[] data = Pattern(1000, seed: 3);
        using SnapshotReceiveBuffer buffer = new();
        buffer.Write(data);

        string missingDirectory = Path.Combine(Path.GetTempPath(), "kommander-missing-" + Guid.NewGuid().ToString("N"), "f.snapshot-staging");
        Assert.ThrowsAny<IOException>(() => buffer.SpillTo(missingDirectory));

        Assert.False(buffer.IsSpilled);
        buffer.Position = 0;
        byte[] read = new byte[data.Length];
        Assert.Equal(data.Length, buffer.Read(read, 0, read.Length));
        Assert.Equal(data, read);
    }

    private static async Task<byte[]> ReadAllAsync(Stream stream, CancellationToken ct)
    {
        using MemoryStream copy = new();
        byte[] chunk = new byte[4096];
        int read;
        while ((read = await stream.ReadAsync(chunk.AsMemory(), ct)) > 0)
            copy.Write(chunk, 0, read);
        return copy.ToArray();
    }
}
