
using System.Buffers.Binary;
using System.IO.Hashing;
using System.Runtime.InteropServices;
using Microsoft.Win32.SafeHandles;

namespace Kommander.WAL;

/// <summary>
/// The sync ledger: a small file beside the RocksDB engine that attests, <b>outside the engine's own
/// write-ahead log</b>, the highest RocksDB sequence number this process has seen complete a
/// synchronous write.
///
/// <para><b>Why it exists.</b> Every column family shares one RocksDB log file, and the engine records
/// the synced size of a log only once that log is closed (<c>track_and_verify_wals_in_manifest</c>).
/// The live log has no such record: a file truncated <em>after</em> its fsync replays short and the
/// engine opens as if the lost records had never been written. Any proof kept inside that log — the
/// acknowledged-presence watermark, the term and the vote — is truncated with the rows it protects,
/// so the surviving state is always self-consistent and the loss is invisible. The proof has to live
/// in a file the truncation cannot reach. This is that file.</para>
///
/// <para><b>What it records.</b> One number: the sequence of the last operation in a write batch
/// after the engine's synchronous write of that batch returned. RocksDB assigns sequence numbers in
/// log order and its fsync covers every record appended before it, so everything at or below the
/// attested sequence is on disk. At open, <see cref="RocksDbWAL"/> compares the attested sequence with
/// the sequence the engine replayed: a replay that stops short lost synced writes, and the engine
/// fail-stops (<see cref="IWAL.GetFailStopReason"/>) instead of voting or leading from a log it cannot
/// vouch for. One global number covers the rows of every partition, the watermark, the hard state,
/// the frontiers and the floors alike, which is why the check needs no per-partition bookkeeping and
/// no ordering rules at the paths that lower a watermark.</para>
///
/// <para><b>Why there is no false positive.</b> The ledger is written only <em>after</em> the engine's
/// synchronous write returned, with the sequence of that batch taken from the batch itself (never
/// from the engine's latest sequence, which a concurrent unsynced write may have raised). A crash
/// between the engine write and the ledger write leaves the ledger behind the engine, which is
/// harmless; the ledger can never be ahead of what the engine fsynced. Unsynced writes are never
/// attested: they ride the next fsync and are attested by the batch that performs it.</para>
///
/// <para><b>Durability of the ledger itself.</b> Two fixed 512-byte slots, written alternately and
/// each fsynced before the next write begins, so at most one slot is ever torn and the other holds
/// the previous attestation. A torn slot (bad checksum) is skipped; both slots unreadable is treated
/// as no ledger, with a warning, because that shape is not one a crash can produce and the rule is
/// to report a loss only on evidence. Each record carries the engine's identity so a ledger copied
/// from another directory is recognised and reset rather than believed. The file is preallocated and
/// overwritten in place, so an attestation is one sector write and one <c>fdatasync</c>, and a full
/// disk cannot refuse it.</para>
///
/// <para><b>Cost.</b> One extra fsync per synchronous engine write, group-committed: concurrent
/// writers raise a shared target and the first one through the gate writes it for all of them, the
/// rest return as soon as a write that covers their sequence has landed. The latency of a durable
/// write grows by one device round trip; throughput under load is preserved by the grouping.</para>
///
/// <para><b>Thread-safety.</b> <see cref="Attest"/> is safe from any thread. The open-time members
/// (<see cref="Open"/>, <see cref="Initialize"/>, <see cref="Adopt"/>) run on the constructor's
/// thread before any writer exists.</para>
/// </summary>
internal sealed class RocksDbSyncLedger : IDisposable
{
    /// <summary>The ledger's file name inside the engine directory. RocksDB ignores names it does not recognise.</summary>
    public const string FileName = ".kommander-sync-ledger";

    private const uint Magic = 0x4C59534B; // "KSYL"

    private const uint FormatVersion = 1;

    private const int SlotSize = 512;

    private const int SlotCount = 2;

    /// <summary>magic 4 + version 4 + generation 8 + sequence 8 + identity 16 + crc32 4.</summary>
    private const int RecordSize = 44;

    /// <summary>What <see cref="Open"/> found in the file.</summary>
    public enum ReadStatus
    {
        /// <summary>No file, or no slot was ever written.</summary>
        Absent,

        /// <summary>At least one slot holds a verified record; the newest is reported.</summary>
        Valid,

        /// <summary>Every written slot fails its checksum. Not a crash shape; treated as absent, with a warning.</summary>
        Unreadable,
    }

    /// <summary>One attestation as read from a slot.</summary>
    public readonly record struct Record(Guid Identity, long Sequence, long Generation);

    private readonly SafeFileHandle handle;

    private readonly byte[] slotBuffer = new byte[SlotSize];

    private readonly object gate = new();

    private Guid identity;

    private long generation;

    private long attested = -1;

    private long pending = -1;

    private bool disposed;

    private RocksDbSyncLedger(SafeFileHandle handle, string filePath)
    {
        this.handle = handle;
        FilePath = filePath;
    }

    /// <summary>The ledger file's full path, for log lines.</summary>
    public string FilePath { get; }

    /// <summary>The engine identity this ledger was initialised with or adopted from.</summary>
    public Guid Identity => identity;

    /// <summary>The highest sequence this ledger attests as durable; -1 before the first attestation.</summary>
    public long AttestedSequence => Volatile.Read(ref attested);

    /// <summary>
    /// Opens the ledger file in <paramref name="directory"/>, creating and preallocating it when it is
    /// missing, and reads the newest verified record. The caller decides what the record means against
    /// the engine (see <see cref="RocksDbWAL"/>) and then calls <see cref="Adopt"/> or
    /// <see cref="Initialize"/> before the first <see cref="Attest"/>.
    /// </summary>
    public static RocksDbSyncLedger Open(string directory, out ReadStatus status, out Record record)
    {
        string filePath = Path.Combine(directory, FileName);

        SafeFileHandle handle = File.OpenHandle(filePath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read, FileOptions.None);
        RocksDbSyncLedger ledger = new(handle, filePath);

        try
        {
            (status, record) = ledger.ReadNewest();

            if (RandomAccess.GetLength(handle) < SlotSize * SlotCount)
            {
                // Preallocate both slots so every later attestation overwrites allocated blocks in
                // place: one sector write, no size change, and nothing a full disk can refuse.
                RandomAccess.Write(handle, new byte[SlotSize * SlotCount], fileOffset: 0);
                DurableFileSync.Sync(handle);
            }
        }
        catch
        {
            ledger.Dispose();
            throw;
        }

        return ledger;
    }

    /// <summary>
    /// Continues a ledger that belongs to this engine: later attestations alternate slots from the
    /// record's generation and never lower its sequence.
    /// </summary>
    public void Adopt(in Record record)
    {
        identity = record.Identity;
        generation = record.Generation;
        Volatile.Write(ref attested, record.Sequence);
        Volatile.Write(ref pending, record.Sequence);
    }

    /// <summary>
    /// Starts the ledger over for <paramref name="engineIdentity"/> at <paramref name="sequence"/>: a new
    /// directory, a directory that predates the ledger, or a ledger that belonged to another engine.
    /// <paramref name="afterGeneration"/> is the generation of a record being superseded (a foreign
    /// ledger's), so the new record outranks it at the next read instead of losing to it; the second
    /// attestation then overwrites the foreign slot. The record is durable when this returns.
    /// </summary>
    public void Initialize(Guid engineIdentity, long sequence, long afterGeneration = 0)
    {
        lock (gate)
        {
            identity = engineIdentity;
            generation = Math.Max(afterGeneration, 0);
            WriteRecord(Math.Max(sequence, 0));
            Volatile.Write(ref attested, Math.Max(sequence, 0));
            Volatile.Write(ref pending, Math.Max(sequence, 0));
        }
    }

    /// <summary>
    /// Makes <paramref name="sequence"/> part of the ledger's durable attestation. Returns once a record
    /// at or above it is on disk. Group-committed: see the type summary. Throws <see cref="IOException"/>
    /// when the write or the sync failed; the caller must then report its own write as failed, because
    /// a promise the ledger does not cover must not be made.
    /// </summary>
    public void Attest(long sequence)
    {
        if (sequence <= Volatile.Read(ref attested))
            return;

        // Raise the shared target; whoever holds the gate next writes the highest one for everybody.
        long seen = Volatile.Read(ref pending);
        while (seen < sequence)
        {
            long prior = Interlocked.CompareExchange(ref pending, sequence, seen);
            if (prior == seen)
                break;
            seen = prior;
        }

        lock (gate)
        {
            if (sequence <= attested)
                return;

            long target = Volatile.Read(ref pending);
            WriteRecord(target);
            Volatile.Write(ref attested, target);
        }
    }

    /// <summary>Writes one record with the next generation into the other slot and syncs it. Under <see cref="gate"/>.</summary>
    private void WriteRecord(long sequence)
    {
        if (disposed)
            throw new ObjectDisposedException(nameof(RocksDbSyncLedger));

        long next = generation + 1;
        int slot = (int)(next % SlotCount);

        Span<byte> record = slotBuffer.AsSpan();
        record.Clear();
        BinaryPrimitives.WriteUInt32LittleEndian(record[..4], Magic);
        BinaryPrimitives.WriteUInt32LittleEndian(record.Slice(4, 4), FormatVersion);
        BinaryPrimitives.WriteInt64LittleEndian(record.Slice(8, 8), next);
        BinaryPrimitives.WriteInt64LittleEndian(record.Slice(16, 8), sequence);
        identity.TryWriteBytes(record.Slice(24, 16));
        BinaryPrimitives.WriteUInt32LittleEndian(record.Slice(40, 4), Crc32.HashToUInt32(record[..40]));

        RandomAccess.Write(handle, slotBuffer, fileOffset: (long)slot * SlotSize);
        DurableFileSync.Sync(handle);

        generation = next;
    }

    private (ReadStatus Status, Record Record) ReadNewest()
    {
        ReadStatus status = ReadStatus.Absent;
        Record newest = default;
        byte[] buffer = new byte[SlotSize];

        for (int slot = 0; slot < SlotCount; slot++)
        {
            Array.Clear(buffer);
            int read = RandomAccess.Read(handle, buffer, fileOffset: (long)slot * SlotSize);
            if (read < RecordSize)
                continue;

            ReadOnlySpan<byte> record = buffer;
            uint magic = BinaryPrimitives.ReadUInt32LittleEndian(record[..4]);
            if (magic == 0)
                continue; // never written

            bool valid = magic == Magic
                && BinaryPrimitives.ReadUInt32LittleEndian(record.Slice(4, 4)) == FormatVersion
                && BinaryPrimitives.ReadUInt32LittleEndian(record.Slice(40, 4)) == Crc32.HashToUInt32(record[..40]);

            if (!valid)
            {
                if (status != ReadStatus.Valid)
                    status = ReadStatus.Unreadable;
                continue;
            }

            Record candidate = new(
                new Guid(record.Slice(24, 16)),
                BinaryPrimitives.ReadInt64LittleEndian(record.Slice(16, 8)),
                BinaryPrimitives.ReadInt64LittleEndian(record.Slice(8, 8)));

            if (status != ReadStatus.Valid || candidate.Generation > newest.Generation)
                newest = candidate;

            status = ReadStatus.Valid;
        }

        return (status, newest);
    }

    public void Dispose()
    {
        lock (gate)
        {
            if (disposed)
                return;

            disposed = true;
            handle.Dispose();
        }
    }
}

/// <summary>
/// Syncs a file handle with the same primitive the storage engine uses for its own log on each platform
/// (<c>fdatasync</c> on Linux, <c>fsync</c> elsewhere on Unix, <c>FlushFileBuffers</c> on Windows), so
/// the ledger is exactly as durable as the log it vouches for — no weaker, and not the full-device
/// flush <see cref="FileStream.Flush(bool)"/> issues on macOS, which would make every synchronous write
/// several times slower on a developer machine without strengthening anything the engine itself promises.
/// </summary>
internal static class DurableFileSync
{
    public static void Sync(SafeFileHandle handle)
    {
        if (OperatingSystem.IsWindows())
        {
            if (!FlushFileBuffers(handle))
                throw new IOException($"FlushFileBuffers failed with error {Marshal.GetLastPInvokeError()}");
            return;
        }

        bool added = false;
        try
        {
            handle.DangerousAddRef(ref added);
            int fd = (int)handle.DangerousGetHandle();
            int rc = OperatingSystem.IsLinux() ? fdatasync(fd) : fsync(fd);
            if (rc != 0)
                throw new IOException($"{(OperatingSystem.IsLinux() ? "fdatasync" : "fsync")} failed with errno {Marshal.GetLastPInvokeError()}");
        }
        finally
        {
            if (added)
                handle.DangerousRelease();
        }
    }

    [DllImport("libc", SetLastError = true)]
    private static extern int fsync(int fd);

    [DllImport("libc", SetLastError = true)]
    private static extern int fdatasync(int fd);

    [DllImport("kernel32.dll", SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool FlushFileBuffers(SafeFileHandle hFile);
}
