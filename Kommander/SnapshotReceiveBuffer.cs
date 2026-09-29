
namespace Kommander;

/// <summary>
/// Append-then-read storage for one in-progress snapshot-receive session, exposed as a
/// <see cref="Stream"/> for the application importer.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why not a <see cref="MemoryStream"/>.</b> A <see cref="MemoryStream"/> grows by allocating a
/// larger contiguous array and copying the existing content into it. For a snapshot assembled from
/// many chunks that is quadratic copy work, the old array and the new array are both live for the
/// duration of each copy, and every array past the first is on the large-object heap. Worse, all of
/// it happened under the receiver-wide lock, so an unrelated session waited through the copy.
/// </para>
/// <para>
/// <b>What this does instead.</b> Bytes land in a list of fixed-size segments. A full segment is never
/// touched again, so appending is a copy of the incoming chunk and nothing else, and physical memory
/// tracks the payload rather than the next power of two above it. The overshoot is at most one
/// unfilled segment per session, which is what makes the receiver's byte budget — accounted in logical
/// bytes — a bound on physical memory as well, within that known slack.
/// </para>
/// <para>
/// <b>Usage contract.</b> Writes append, and only at the end of the stream; the receiver fills the
/// buffer, rewinds it once, and hands it to the importer, which reads. A write while the position sits
/// anywhere but the end throws rather than silently diverging from what a
/// <see cref="MemoryStream"/> would have done. Reading supports seeking and reports
/// <see cref="Length"/>, so an importer that does more than a forward pass still works.
/// </para>
/// <para>
/// <b>Spilling to disk.</b> <see cref="SpillTo"/> moves the bytes staged so far into a temporary file
/// (deleted when the buffer is disposed) and releases the segments; every later append and read goes to
/// the file. The receiver spills a session when keeping it in memory would exceed its staging memory
/// budget, so a snapshot of any size can be staged without being held in memory. The stream contract is
/// the same in both modes: seekable, <see cref="Length"/> reported, reads from any position.
/// </para>
/// <para>
/// Not thread-safe. The receiver lets one caller at a time append to a session, and the importer runs
/// after the buffer has been detached from the session map.
/// </para>
/// </remarks>
internal sealed class SnapshotReceiveBuffer : Stream
{
    /// <summary>
    /// Size of one storage segment.
    /// </summary>
    /// <remarks>
    /// Chosen to be uniform rather than small: every segment is the same size, so the large-object heap
    /// sees one repeated allocation size and reuses freed segments instead of fragmenting the way the
    /// doubling sizes of a growing <see cref="MemoryStream"/> did. It also bounds the wasted tail of a
    /// session to below this value, and keeps the segment count of a large snapshot in the thousands
    /// rather than the millions.
    /// </remarks>
    internal const int SegmentSize = 256 * 1024;

    private readonly List<byte[]> segments = [];

    /// <summary>The spill file once <see cref="SpillTo"/> ran; null while the bytes are in memory.</summary>
    private FileStream? file;

    private long length;
    private long position;
    private bool disposed;

    /// <summary>
    /// Bytes actually allocated for this buffer. Equal to <see cref="Length"/> rounded up to a whole
    /// number of segments, so it exceeds the payload by less than <see cref="SegmentSize"/>.
    /// </summary>
    internal long AllocatedByteCount => (long)segments.Count * SegmentSize;

    /// <summary>Whether the staged bytes live in a spill file rather than in memory.</summary>
    internal bool IsSpilled => file is not null;

    /// <summary>
    /// Moves the bytes staged so far into a new file at <paramref name="path"/> and releases the in-memory
    /// segments; later appends and reads use the file, which is deleted when this buffer is disposed. A
    /// no-op once spilled. On failure (for example a full disk) the buffer is left as it was, in memory, and
    /// the partial file is removed.
    /// </summary>
    internal void SpillTo(string path)
    {
        ObjectDisposedException.ThrowIf(disposed, this);

        if (file is not null)
            return;

        FileStream created = new(path, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.None, 64 * 1024, FileOptions.DeleteOnClose);

        try
        {
            long remaining = length;

            foreach (byte[] segment in segments)
            {
                int take = (int)Math.Min(SegmentSize, remaining);
                created.Write(segment, 0, take);
                remaining -= take;
            }
        }
        catch
        {
            created.Dispose();
            throw;
        }

        file = created;
        segments.Clear();
    }

    public override bool CanRead => !disposed;

    public override bool CanSeek => !disposed;

    public override bool CanWrite => !disposed;

    public override long Length
    {
        get
        {
            ObjectDisposedException.ThrowIf(disposed, this);
            return length;
        }
    }

    public override long Position
    {
        get
        {
            ObjectDisposedException.ThrowIf(disposed, this);
            return position;
        }

        set
        {
            ObjectDisposedException.ThrowIf(disposed, this);
            ArgumentOutOfRangeException.ThrowIfNegative(value);
            position = value;
        }
    }

    public override void Flush()
    {
    }

    public override Task FlushAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public override void SetLength(long value) =>
        throw new NotSupportedException("A snapshot receive buffer only grows by appending chunks.");

    public override long Seek(long offset, SeekOrigin origin)
    {
        ObjectDisposedException.ThrowIf(disposed, this);

        long target = origin switch
        {
            SeekOrigin.Begin => offset,
            SeekOrigin.Current => position + offset,
            SeekOrigin.End => length + offset,
            _ => throw new ArgumentOutOfRangeException(nameof(origin))
        };

        if (target < 0)
            throw new IOException("Cannot seek before the start of a snapshot receive buffer.");

        position = target;
        return position;
    }

    public override void Write(byte[] buffer, int offset, int count)
    {
        ArgumentNullException.ThrowIfNull(buffer);
        Write(buffer.AsSpan(offset, count));
    }

    public override void Write(ReadOnlySpan<byte> buffer)
    {
        ObjectDisposedException.ThrowIf(disposed, this);

        if (position != length)
        {
            throw new NotSupportedException(
                "A snapshot receive buffer only accepts appends; the position must be at the end.");
        }

        if (file is not null)
        {
            file.Position = length;
            file.Write(buffer);
            length += buffer.Length;
            position = length;
            return;
        }

        while (!buffer.IsEmpty)
        {
            int offsetInSegment = (int)(length % SegmentSize);

            if (offsetInSegment == 0)
                segments.Add(new byte[SegmentSize]);

            byte[] segment = segments[^1];
            int room = SegmentSize - offsetInSegment;
            int take = Math.Min(room, buffer.Length);

            buffer[..take].CopyTo(segment.AsSpan(offsetInSegment, take));

            buffer = buffer[take..];
            length += take;
            position = length;
        }
    }

    public override void WriteByte(byte value)
    {
        ReadOnlySpan<byte> single = new(in value);
        Write(single);
    }

    public override int Read(byte[] buffer, int offset, int count)
    {
        ArgumentNullException.ThrowIfNull(buffer);
        return Read(buffer.AsSpan(offset, count));
    }

    public override int Read(Span<byte> buffer)
    {
        ObjectDisposedException.ThrowIf(disposed, this);

        if (file is not null)
            return ReadFromFile(buffer);

        int total = 0;

        while (!buffer.IsEmpty && position < length)
        {
            int segmentIndex = (int)(position / SegmentSize);
            int offsetInSegment = (int)(position % SegmentSize);
            int available = (int)Math.Min(SegmentSize - offsetInSegment, length - position);
            int take = Math.Min(available, buffer.Length);

            segments[segmentIndex].AsSpan(offsetInSegment, take).CopyTo(buffer);

            buffer = buffer[take..];
            position += take;
            total += take;
        }

        return total;
    }

    public override int ReadByte()
    {
        ObjectDisposedException.ThrowIf(disposed, this);

        if (position >= length)
            return -1;

        if (file is not null)
        {
            Span<byte> single = stackalloc byte[1];
            return ReadFromFile(single) == 1 ? single[0] : -1;
        }

        byte value = segments[(int)(position / SegmentSize)][(int)(position % SegmentSize)];
        position++;
        return value;
    }

    /// <summary>
    /// Completes synchronously while the bytes are in memory, where the default <see cref="Stream"/>
    /// implementation would only add a thread hop per call; reads the spill file asynchronously once spilled.
    /// </summary>
    public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        if (cancellationToken.IsCancellationRequested)
            return ValueTask.FromCanceled<int>(cancellationToken);

        if (file is not null && !disposed)
            return ReadFromFileAsync(buffer, cancellationToken);

        try
        {
            return ValueTask.FromResult(Read(buffer.Span));
        }
        catch (Exception exception)
        {
            return ValueTask.FromException<int>(exception);
        }
    }

    public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(buffer);
        return ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();
    }

    /// <summary>
    /// Copies the remaining bytes segment by segment, so a caller that drains this buffer into another
    /// stream never allocates an intermediate copy buffer.
    /// </summary>
    public override void CopyTo(Stream destination, int bufferSize)
    {
        ArgumentNullException.ThrowIfNull(destination);
        ObjectDisposedException.ThrowIf(disposed, this);

        if (file is not null)
        {
            // The file holds exactly the staged bytes (appends only), so copying to its end copies to ours.
            file.Position = position;
            file.CopyTo(destination, bufferSize);
            position = length;
            return;
        }

        while (position < length)
        {
            ReadOnlySpan<byte> slice = NextReadableSlice();
            destination.Write(slice);
            position += slice.Length;
        }
    }

    /// <inheritdoc cref="CopyTo(Stream,int)"/>
    public override async Task CopyToAsync(Stream destination, int bufferSize, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(destination);
        ObjectDisposedException.ThrowIf(disposed, this);

        if (file is not null)
        {
            file.Position = position;
            await file.CopyToAsync(destination, bufferSize, cancellationToken).ConfigureAwait(false);
            position = length;
            return;
        }

        while (position < length)
        {
            (int segmentIndex, int offsetInSegment, int count) = NextReadableRange();

            await destination
                .WriteAsync(segments[segmentIndex].AsMemory(offsetInSegment, count), cancellationToken)
                .ConfigureAwait(false);

            position += count;
        }
    }

    private int ReadFromFile(Span<byte> buffer)
    {
        if (position >= length || buffer.IsEmpty)
            return 0;

        if (buffer.Length > length - position)
            buffer = buffer[..(int)(length - position)];

        file!.Position = position;
        int read = file.Read(buffer);
        position += read;
        return read;
    }

    private async ValueTask<int> ReadFromFileAsync(Memory<byte> buffer, CancellationToken cancellationToken)
    {
        if (position >= length || buffer.IsEmpty)
            return 0;

        if (buffer.Length > length - position)
            buffer = buffer[..(int)(length - position)];

        file!.Position = position;
        int read = await file.ReadAsync(buffer, cancellationToken).ConfigureAwait(false);
        position += read;
        return read;
    }

    private ReadOnlySpan<byte> NextReadableSlice()
    {
        (int segmentIndex, int offsetInSegment, int count) = NextReadableRange();
        return segments[segmentIndex].AsSpan(offsetInSegment, count);
    }

    /// <summary>
    /// The contiguous run of readable bytes at the current position, clipped to the end of its segment
    /// and to the logical length. Must only be called while <c>position &lt; length</c>.
    /// </summary>
    private (int SegmentIndex, int OffsetInSegment, int Count) NextReadableRange()
    {
        int segmentIndex = (int)(position / SegmentSize);
        int offsetInSegment = (int)(position % SegmentSize);
        int count = (int)Math.Min(SegmentSize - offsetInSegment, length - position);

        return (segmentIndex, offsetInSegment, count);
    }

    protected override void Dispose(bool disposing)
    {
        if (!disposed)
        {
            disposed = true;
            segments.Clear();
            file?.Dispose();
            file = null;
            length = 0;
            position = 0;
        }

        base.Dispose(disposing);
    }
}
