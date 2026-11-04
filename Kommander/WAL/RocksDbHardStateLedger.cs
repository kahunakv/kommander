
using System.Buffers.Binary;
using System.IO.Hashing;
using System.Text;

namespace Kommander.WAL;

/// <summary>
/// The hard-state ledger: an append-only file beside the RocksDB engine that holds, outside the engine's
/// write-ahead log, the last term and vote persisted for each partition.
///
/// <para><b>Why it exists.</b> <see cref="IWAL.PersistHardState"/> is a synchronous metadata put, which
/// makes the vote durable — inside the same log file as the rows. A log truncated after its fsync takes
/// the put with it, and the node restarts with an older term and no vote: it then grants a second
/// candidate a term it already voted in, which is the two-leaders-in-one-term outcome the cluster run
/// of 2026-11-04 showed. The sync ledger (<see cref="RocksDbSyncLedger"/>) detects that the log lost
/// synced writes, but detection alone cannot say <em>which</em> vote is missing. This file can: at open,
/// a partition whose ledger entry is ahead of the engine's gets its term and vote written back before
/// the Raft layer reads them, so no quarantine lift can ever let the node vote twice.</para>
///
/// <para><b>Ordering.</b> Appended after the engine's put returned, never before. A crash between the
/// two leaves the ledger behind the engine, which the merge ignores (the engine's newer state wins);
/// the ledger is never ahead of the engine unless the engine lost the put. A record is one write and
/// one fsync; elections are rare, so the cost is nothing measurable.</para>
///
/// <para><b>Format.</b> Records of <c>magic, partition, term, vote length, vote, crc32</c>, replayed
/// in order at open; the last record per partition wins. A torn tail record (the crash shape) fails its
/// checksum, and the file is cut back to the last whole record so later appends start clean. The file
/// is rewritten with one record per partition once it passes a size bound.</para>
///
/// <para><b>Thread-safety.</b> <see cref="Append"/> is serialised by a lock; the open-time members run
/// on the constructor's thread.</para>
/// </summary>
internal sealed class RocksDbHardStateLedger : IDisposable
{
    /// <summary>The ledger's file name inside the engine directory. RocksDB ignores names it does not recognise.</summary>
    public const string FileName = ".kommander-hard-state";

    private const uint Magic = 0x54534448; // "HDST"

    /// <summary>magic 4 + partition 4 + term 8 + vote length 2; the vote bytes and the crc32 follow.</summary>
    private const int FixedHeaderSize = 18;

    private const int MaxVoteBytes = 1024;

    /// <summary>Rewrite the file with one record per partition once it grows past this.</summary>
    private const long CompactAboveBytes = 256 * 1024;

    /// <summary>One partition's last persisted term and vote.</summary>
    public readonly record struct Entry(long Term, string? VotedFor);

    private readonly string filePath;

    private readonly object gate = new();

    private readonly Dictionary<int, Entry> latest = new();

    private FileStream stream;

    private bool disposed;

    private RocksDbHardStateLedger(string filePath, FileStream stream)
    {
        this.filePath = filePath;
        this.stream = stream;
    }

    /// <summary>The ledger file's full path, for log lines.</summary>
    public string FilePath => filePath;

    /// <summary>
    /// Opens the ledger in <paramref name="directory"/>, creating it when missing, replays every whole
    /// record and cuts a torn tail record off so the next append starts at a record boundary.
    /// </summary>
    public static RocksDbHardStateLedger Open(string directory)
    {
        string filePath = Path.Combine(directory, FileName);
        FileStream stream = new(filePath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read, bufferSize: 0);
        RocksDbHardStateLedger ledger = new(filePath, stream);

        try
        {
            ledger.Replay();
        }
        catch
        {
            ledger.Dispose();
            throw;
        }

        return ledger;
    }

    /// <summary>A copy of the last entry per partition, as replayed at open plus every append since.</summary>
    public IReadOnlyDictionary<int, Entry> Snapshot()
    {
        lock (gate)
            return new Dictionary<int, Entry>(latest);
    }

    /// <summary>
    /// Appends one record and syncs it. Throws <see cref="IOException"/> when the write or the sync
    /// failed, or when the vote exceeds the record bound; the caller must then report the hard-state
    /// write as not durable, since the engine's copy alone is what this ledger exists to back up.
    /// </summary>
    public void Append(int partitionId, long term, string? votedFor)
    {
        byte[] record = Encode(partitionId, term, votedFor);

        lock (gate)
        {
            if (disposed)
                throw new ObjectDisposedException(nameof(RocksDbHardStateLedger));

            if (stream.Length > CompactAboveBytes)
                Compact();

            stream.Seek(0, SeekOrigin.End);
            stream.Write(record);
            stream.Flush();
            DurableFileSync.Sync(stream.SafeFileHandle);

            latest[partitionId] = new Entry(term, string.IsNullOrEmpty(votedFor) ? null : votedFor);
        }
    }

    private static byte[] Encode(int partitionId, long term, string? votedFor)
    {
        int voteBytes = string.IsNullOrEmpty(votedFor) ? 0 : Encoding.UTF8.GetByteCount(votedFor);
        if (voteBytes > MaxVoteBytes)
            throw new IOException($"The hard-state ledger cannot record a vote of {voteBytes} bytes (bound {MaxVoteBytes}).");

        byte[] record = new byte[FixedHeaderSize + voteBytes + 4];
        Span<byte> span = record;
        BinaryPrimitives.WriteUInt32LittleEndian(span[..4], Magic);
        BinaryPrimitives.WriteInt32LittleEndian(span.Slice(4, 4), partitionId);
        BinaryPrimitives.WriteInt64LittleEndian(span.Slice(8, 8), term);
        BinaryPrimitives.WriteUInt16LittleEndian(span.Slice(16, 2), (ushort)voteBytes);
        if (voteBytes > 0)
            Encoding.UTF8.GetBytes(votedFor, span.Slice(FixedHeaderSize, voteBytes));
        int payload = FixedHeaderSize + voteBytes;
        BinaryPrimitives.WriteUInt32LittleEndian(span.Slice(payload, 4), Crc32.HashToUInt32(span[..payload]));
        return record;
    }

    /// <summary>Replays the file into <see cref="latest"/>; returns after cutting any torn tail.</summary>
    private void Replay()
    {
        long length = stream.Length;
        if (length == 0)
            return;

        byte[] bytes = new byte[length];
        stream.Seek(0, SeekOrigin.Begin);
        int read = 0;
        while (read < bytes.Length)
        {
            int n = stream.Read(bytes, read, bytes.Length - read);
            if (n <= 0)
                break;
            read += n;
        }

        int offset = 0;
        while (TryDecode(bytes.AsSpan(0, read), offset, out int partitionId, out Entry entry, out int size))
        {
            latest[partitionId] = entry;
            offset += size;
        }

        if (offset < read)
        {
            // A torn tail: the residue of a crash during an append. Nothing after a bad record can be
            // trusted (records are not self-synchronising), and nothing after it was ever reported durable.
            stream.SetLength(offset);
            stream.Flush();
            DurableFileSync.Sync(stream.SafeFileHandle);
        }
    }

    private static bool TryDecode(ReadOnlySpan<byte> bytes, int offset, out int partitionId, out Entry entry, out int size)
    {
        partitionId = 0;
        entry = default;
        size = 0;

        if (bytes.Length - offset < FixedHeaderSize + 4)
            return false;

        ReadOnlySpan<byte> span = bytes[offset..];
        if (BinaryPrimitives.ReadUInt32LittleEndian(span[..4]) != Magic)
            return false;

        int voteBytes = BinaryPrimitives.ReadUInt16LittleEndian(span.Slice(16, 2));
        if (voteBytes > MaxVoteBytes || span.Length < FixedHeaderSize + voteBytes + 4)
            return false;

        int payload = FixedHeaderSize + voteBytes;
        if (BinaryPrimitives.ReadUInt32LittleEndian(span.Slice(payload, 4)) != Crc32.HashToUInt32(span[..payload]))
            return false;

        partitionId = BinaryPrimitives.ReadInt32LittleEndian(span.Slice(4, 4));
        long term = BinaryPrimitives.ReadInt64LittleEndian(span.Slice(8, 8));
        string? votedFor = voteBytes == 0 ? null : Encoding.UTF8.GetString(span.Slice(FixedHeaderSize, voteBytes));
        entry = new Entry(term, votedFor);
        size = payload + 4;
        return true;
    }

    /// <summary>
    /// Rewrites the file with one record per partition: written to a sibling file, synced, then renamed
    /// over the old one. A crash leaves either the old file or the new one, both complete. Under <see cref="gate"/>.
    /// </summary>
    private void Compact()
    {
        string tmp = filePath + ".tmp";

        using (FileStream fresh = new(tmp, FileMode.Create, FileAccess.ReadWrite, FileShare.None, bufferSize: 0))
        {
            foreach ((int partitionId, Entry entry) in latest)
                fresh.Write(Encode(partitionId, entry.Term, entry.VotedFor));
            fresh.Flush();
            DurableFileSync.Sync(fresh.SafeFileHandle);
        }

        stream.Dispose();
        File.Move(tmp, filePath, overwrite: true);
        stream = new FileStream(filePath, FileMode.Open, FileAccess.ReadWrite, FileShare.Read, bufferSize: 0);
    }

    public void Dispose()
    {
        lock (gate)
        {
            if (disposed)
                return;

            disposed = true;
            stream.Dispose();
        }
    }
}
