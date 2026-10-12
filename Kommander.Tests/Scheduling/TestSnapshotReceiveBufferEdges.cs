namespace Kommander.Tests.Scheduling;

/// <summary>
/// Boundary and failure edges of <see cref="SnapshotReceiveBuffer"/> beyond the round-trip and
/// spill happy paths: spilling an empty buffer, spilling onto an existing file, repeated spill
/// attempts, seeking past the end, single-byte writes at segment boundaries, reads at and past
/// the end in both storage modes, cancelled reads, and the flush no-ops. The importer and the
/// receiver both rely on the stream contract holding identically on both sides of every one of
/// these edges, so each test pins the observable shape rather than an implementation detail.
/// </summary>
public sealed class TestSnapshotReceiveBufferEdges
{
    private const int SegmentSize = SnapshotReceiveBuffer.SegmentSize;

    private static byte[] Pattern(int length, int seed = 0)
    {
        byte[] data = new byte[length];

        for (int i = 0; i < length; i++)
            data[i] = (byte)((i * 31 + seed) & 0xFF);

        return data;
    }

    private static string TempSpillPath() =>
        Path.Combine(Path.GetTempPath(), "kommander-spill-edge-" + Guid.NewGuid().ToString("N") + ".snapshot-staging");

    // ── spill edges ────────────────────────────────────────────────────────────

    /// <summary>
    /// A session can be chosen to spill before it has staged a single byte (its first chunk alone
    /// would cross the memory budget). The spill of an empty buffer must still move it to the file:
    /// an empty file is created, later appends land in it, and the whole content reads back.
    /// </summary>
    [Fact]
    public async Task SpillOfAnEmptyBuffer_MovesTheSessionToTheFile_AndLaterAppendsLandThere()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string path = TempSpillPath();

        byte[] appended = Pattern(SegmentSize + 17, seed: 4);

        SnapshotReceiveBuffer buffer = new();
        buffer.SpillTo(path);

        Assert.True(buffer.IsSpilled);
        Assert.Equal(0, buffer.Length);
        Assert.True(File.Exists(path));
        Assert.Equal(0, new FileInfo(path).Length);

        buffer.Write(appended);
        Assert.Equal(appended.Length, buffer.Length);
        Assert.Equal(0, buffer.AllocatedByteCount);

        buffer.Position = 0;
        using MemoryStream copy = new();
        await buffer.CopyToAsync(copy, ct);
        Assert.Equal(appended, copy.ToArray());

        buffer.Dispose();
        Assert.False(File.Exists(path));
    }

    /// <summary>
    /// The spill file is created with <c>FileMode.CreateNew</c>, so a path that already exists is
    /// refused. The documented contract on that failure: the buffer stays in memory and intact, and
    /// a later spill to a usable path carries everything staged — both before and after the refusal.
    /// </summary>
    [Fact]
    public void SpillOntoAnExistingFile_IsRefused_AndTheBufferKeepsWorkingInMemory()
    {
        string occupied = TempSpillPath();
        string valid = TempSpillPath();

        try
        {
            File.WriteAllBytes(occupied, [0xFF]);

            byte[] before = Pattern(500, seed: 1);
            using SnapshotReceiveBuffer buffer = new();
            buffer.Write(before);

            Assert.ThrowsAny<IOException>(() => buffer.SpillTo(occupied));
            Assert.False(buffer.IsSpilled);
            Assert.True(buffer.AllocatedByteCount >= before.Length, "the segments were not released");

            // The refused spill must not have truncated or consumed the pre-existing file either.
            Assert.Equal([0xFF], File.ReadAllBytes(occupied));

            byte[] after = Pattern(300, seed: 2);
            buffer.Write(after);

            buffer.SpillTo(valid);
            Assert.True(buffer.IsSpilled);

            buffer.Position = 0;
            byte[] read = new byte[before.Length + after.Length];
            Assert.Equal(read.Length, buffer.Read(read, 0, read.Length));
            Assert.Equal([.. before, .. after], read);
        }
        finally
        {
            TryDelete(occupied);
            TryDelete(valid);
        }
    }

    /// <summary>
    /// Spilling is one-way: a second <see cref="SnapshotReceiveBuffer.SpillTo"/> (a racing decision,
    /// or a retry of the budget check) must be a no-op rather than creating a second file or
    /// corrupting the first.
    /// </summary>
    [Fact]
    public void SecondSpill_IsANoOp_AndNeverTouchesTheSecondPath()
    {
        string first = TempSpillPath();
        string second = TempSpillPath();

        try
        {
            byte[] data = Pattern(10_000, seed: 6);

            using SnapshotReceiveBuffer buffer = new();
            buffer.Write(data);
            buffer.SpillTo(first);

            buffer.SpillTo(second);

            Assert.True(buffer.IsSpilled);
            Assert.False(File.Exists(second), "the second spill path must never be created");

            byte[] more = Pattern(50, seed: 7);
            buffer.Write(more);

            buffer.Position = 0;
            byte[] read = new byte[data.Length + more.Length];
            Assert.Equal(read.Length, buffer.Read(read, 0, read.Length));
            Assert.Equal([.. data, .. more], read);
        }
        finally
        {
            TryDelete(first);
            TryDelete(second);
        }
    }

    // ── seek past the end ──────────────────────────────────────────────────────

    /// <summary>
    /// A seek past the end is accepted (the position is a cursor, not a bound), but every read
    /// there yields nothing — and an append is still refused until the cursor returns to the end,
    /// so a stray position can never punch a hole into the staged bytes.
    /// </summary>
    [Fact]
    public void SeekPastTheEnd_ReadsNothing_AndStillRefusesAppends()
    {
        using SnapshotReceiveBuffer buffer = new();
        buffer.Write(Pattern(128, seed: 8));

        long pastEnd = buffer.Seek(1000, SeekOrigin.End);
        Assert.Equal(1128, pastEnd);

        Assert.Equal(0, buffer.Read(new byte[16], 0, 16));
        Assert.Equal(-1, buffer.ReadByte());
        Assert.Throws<NotSupportedException>(() => buffer.Write(Pattern(1)));

        // Returning the cursor to the end restores appends, and the content is untouched.
        buffer.Seek(0, SeekOrigin.End);
        buffer.Write([0xAB]);
        Assert.Equal(129, buffer.Length);

        buffer.Position = 0;
        byte[] all = new byte[129];
        Assert.Equal(129, buffer.Read(all, 0, all.Length));
        Assert.Equal(0xAB, all[^1]);
    }

    [Fact]
    public void SeekPastTheEnd_OnASpilledBuffer_ReadsNothingFromBeyondTheContent()
    {
        string path = TempSpillPath();

        try
        {
            using SnapshotReceiveBuffer buffer = new();
            buffer.Write(Pattern(300, seed: 9));
            buffer.SpillTo(path);

            buffer.Seek(1_000_000, SeekOrigin.Begin);
            Assert.Equal(0, buffer.Read(new byte[16], 0, 16));
            Assert.Equal(-1, buffer.ReadByte());

            // The file cursor is logical: seeking back inside the content reads real bytes, and
            // reading from the last-but-one position yields exactly the two remaining bytes.
            buffer.Seek(298, SeekOrigin.Begin);
            byte[] tail = new byte[5];
            Assert.Equal(2, buffer.Read(tail, 0, 5));
            Assert.Equal(0, buffer.Length - buffer.Position);
        }
        finally
        {
            TryDelete(path);
        }
    }

    // ── single-byte writes at segment boundaries ───────────────────────────────

    /// <summary>
    /// A single byte landing exactly on a segment boundary must open the next segment, not write
    /// past the end of the previous one — the receiver appends hash-sized and tail-sized fragments
    /// without any guarantee they line up with the boundaries.
    /// </summary>
    [Fact]
    public void WriteByte_AtASegmentBoundary_OpensTheNextSegment()
    {
        using SnapshotReceiveBuffer buffer = new();

        buffer.Write(Pattern(SegmentSize, seed: 10));
        Assert.Equal(SegmentSize, buffer.AllocatedByteCount);

        buffer.WriteByte(0x7E);
        Assert.Equal(2L * SegmentSize, buffer.AllocatedByteCount);
        Assert.Equal(SegmentSize + 1, buffer.Length);

        buffer.Position = SegmentSize - 1;
        Assert.Equal(Pattern(SegmentSize, seed: 10)[SegmentSize - 1], buffer.ReadByte());
        Assert.Equal(0x7E, buffer.ReadByte());
        Assert.Equal(-1, buffer.ReadByte());
    }

    [Fact]
    public void WriteByte_AsTheVeryFirstByte_CreatesTheFirstSegment()
    {
        using SnapshotReceiveBuffer buffer = new();

        buffer.WriteByte(0x42);

        Assert.Equal(1, buffer.Length);
        Assert.Equal(SegmentSize, buffer.AllocatedByteCount);
        buffer.Position = 0;
        Assert.Equal(0x42, buffer.ReadByte());
    }

    [Fact]
    public void WriteByte_WhenThePositionIsNotAtTheEnd_IsRefused()
    {
        using SnapshotReceiveBuffer buffer = new();
        buffer.Write(Pattern(10, seed: 11));
        buffer.Position = 0;

        Assert.Throws<NotSupportedException>(() => buffer.WriteByte(1));
        Assert.Equal(10, buffer.Length);
    }

    [Fact]
    public void WriteByte_OnASpilledBuffer_AppendsToTheFile()
    {
        string path = TempSpillPath();

        try
        {
            using SnapshotReceiveBuffer buffer = new();
            buffer.Write(Pattern(64, seed: 12));
            buffer.SpillTo(path);

            buffer.WriteByte(0x33);
            Assert.Equal(65, buffer.Length);

            buffer.Position = 0;
            byte[] all = new byte[65];
            Assert.Equal(65, buffer.Read(all, 0, all.Length));
            Assert.Equal(0x33, all[^1]);
        }
        finally
        {
            TryDelete(path);
        }
    }

    /// <summary>
    /// A zero-length append is a legitimate call (an empty terminal chunk) and must change nothing
    /// in either storage mode — in particular it must not advance the position away from the end,
    /// which would refuse the next real append.
    /// </summary>
    [Fact]
    public void ZeroLengthWrite_AtTheEnd_ChangesNothing_InMemoryOrInTheFile()
    {
        using SnapshotReceiveBuffer inMemory = new();
        inMemory.Write(Pattern(10, seed: 13));
        inMemory.Write(Array.Empty<byte>());
        Assert.Equal(10, inMemory.Length);
        Assert.Equal(10, inMemory.Position);
        inMemory.Write([1]);
        Assert.Equal(11, inMemory.Length);

        string path = TempSpillPath();
        try
        {
            using SnapshotReceiveBuffer spilled = new();
            spilled.Write(Pattern(10, seed: 14));
            spilled.SpillTo(path);
            spilled.Write(Array.Empty<byte>());
            Assert.Equal(10, spilled.Length);
            Assert.Equal(10, spilled.Position);
            spilled.Write([1]);
            Assert.Equal(11, spilled.Length);
        }
        finally
        {
            TryDelete(path);
        }
    }

    // ── ReadAsync edges ────────────────────────────────────────────────────────

    /// <summary>
    /// At the exact end an asynchronous read must complete synchronously with zero while the bytes
    /// are in memory — the importer's <c>ReadExactly</c> loops terminate on exactly this shape.
    /// </summary>
    [Fact]
    public void ReadAsync_InMemory_AtTheExactEnd_CompletesSynchronouslyWithZero()
    {
        using SnapshotReceiveBuffer buffer = new();
        buffer.Write(Pattern(32, seed: 15));

        buffer.Position = buffer.Length;
        ValueTask<int> read = buffer.ReadAsync(new byte[8], TestContext.Current.CancellationToken);

        Assert.True(read.IsCompletedSuccessfully, "an in-memory read at the end must not schedule anything");
        Assert.Equal(0, read.Result);
    }

    [Fact]
    public async Task ReadAsync_Spilled_AtTheExactEnd_ReturnsZero_PastTheEndToo()
    {
        string path = TempSpillPath();
        try
        {
            using SnapshotReceiveBuffer buffer = new();
            buffer.Write(Pattern(32, seed: 16));
            buffer.SpillTo(path);

            buffer.Position = buffer.Length;
            Assert.Equal(0, await buffer.ReadAsync(new byte[8], TestContext.Current.CancellationToken));

            buffer.Seek(5, SeekOrigin.Current);
            Assert.Equal(0, await buffer.ReadAsync(new byte[8], TestContext.Current.CancellationToken));
        }
        finally
        {
            TryDelete(path);
        }
    }

    /// <summary>
    /// A buffer larger than the remaining bytes yields only the remainder, and leaves the position
    /// at the end — the file-backed reader clips to the logical length exactly as the in-memory
    /// reader does, never exposing padding or stale file content.
    /// </summary>
    [Fact]
    public async Task ReadAsync_Spilled_WithABufferLargerThanTheRemainder_ReturnsOnlyTheRemainder()
    {
        string path = TempSpillPath();
        try
        {
            byte[] data = Pattern(1000, seed: 17);

            using SnapshotReceiveBuffer buffer = new();
            buffer.Write(data);
            buffer.SpillTo(path);

            buffer.Seek(-3, SeekOrigin.End);
            byte[] window = new byte[512];

            Assert.Equal(3, await buffer.ReadAsync(window, TestContext.Current.CancellationToken));
            Assert.Equal(data[^3..], window.AsSpan(0, 3).ToArray());
            Assert.Equal(buffer.Length, buffer.Position);

            Assert.Equal(0, await buffer.ReadAsync(window, TestContext.Current.CancellationToken));
        }
        finally
        {
            TryDelete(path);
        }
    }

    /// <summary>
    /// A cancelled token is honoured before anything is read in both storage modes, so a cancelled
    /// import cannot observe half a read.
    /// </summary>
    [Fact]
    public async Task ReadAsync_WithACancelledToken_ReturnsCancelled_InMemoryAndSpilled()
    {
        CancellationToken cancelled = new CancellationToken(canceled: true);

        using SnapshotReceiveBuffer inMemory = new();
        inMemory.Write(Pattern(16, seed: 18));
        inMemory.Position = 0;
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => inMemory.ReadAsync(new byte[4], cancelled).AsTask());

        string path = TempSpillPath();
        try
        {
            using SnapshotReceiveBuffer spilled = new();
            spilled.Write(Pattern(16, seed: 19));
            spilled.SpillTo(path);
            spilled.Position = 0;
            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                () => spilled.ReadAsync(new byte[4], cancelled).AsTask());
            // Nothing was consumed: a read with a live token still returns the first bytes.
            Assert.Equal(4, await spilled.ReadAsync(new byte[4], TestContext.Current.CancellationToken));
        }
        finally
        {
            TryDelete(path);
        }
    }

    /// <summary>
    /// The asynchronous read of a disposed buffer faults the returned task rather than throwing on
    /// the caller's thread — the shape the importer's await sees.
    /// </summary>
    [Fact]
    public async Task ReadAsync_OnADisposedBuffer_FaultsTheReturnedTask()
    {
        SnapshotReceiveBuffer buffer = new();
        buffer.Write(Pattern(8, seed: 20));
        buffer.Position = 0;
        buffer.Dispose();

        await Assert.ThrowsAsync<ObjectDisposedException>(
            () => buffer.ReadAsync(new byte[4], TestContext.Current.CancellationToken).AsTask());
    }

    // ── flush no-ops ───────────────────────────────────────────────────────────

    /// <summary>
    /// Flush is a no-op in both storage modes — the file is written as chunks arrive and the
    /// segments are always current — so it must neither throw nor, unlike a real file stream's
    /// flush, observe the cancellation token.
    /// </summary>
    [Fact]
    public async Task Flush_AndFlushAsync_DoNothing_EvenWithACancelledToken()
    {
        string path = TempSpillPath();
        try
        {
            byte[] data = Pattern(64, seed: 21);

            using SnapshotReceiveBuffer inMemory = new();
            inMemory.Write(data);
            inMemory.Flush();
            await inMemory.FlushAsync(TestContext.Current.CancellationToken);
            Assert.Equal(data.Length, inMemory.Length);

            using SnapshotReceiveBuffer spilled = new();
            spilled.Write(data);
            spilled.SpillTo(path);

            CancellationToken cancelled = new(canceled: true);
            await spilled.FlushAsync(cancelled);
            spilled.Flush();

            spilled.Position = 0;
            byte[] read = new byte[data.Length];
            Assert.Equal(data.Length, spilled.Read(read, 0, read.Length));
            Assert.Equal(data, read);
        }
        finally
        {
            TryDelete(path);
        }
    }

    private static void TryDelete(string path)
    {
        try
        {
            if (File.Exists(path))
                File.Delete(path);
        }
        catch
        {
            // Best-effort temp cleanup.
        }
    }
}
