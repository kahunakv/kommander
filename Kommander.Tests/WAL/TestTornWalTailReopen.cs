using Kommander.Data;
using Kommander.WAL;
using Microsoft.Extensions.Logging.Abstractions;
using RocksDbSharp;

namespace Kommander.Tests.WAL;

/// <summary>
/// Crash-restart coverage for <see cref="RocksDbWAL"/>: a node that is SIGKILLed must be able to open
/// its WAL again.
///
/// <para>A kill lands between the write syscall and its completion, so the last record in the RocksDB
/// log file is short. That torn tail record is the normal residue of an unclean shutdown, not damage.
/// If the WAL refuses to open on it, the node cannot restart at all — it crash-loops, stays a voter
/// that never returns, and the next fault takes the cluster below quorum.</para>
///
/// <para>The kill is staged on disk rather than by killing a process: the test snapshots the live data
/// directory while the WAL is still open (so the log file still holds the records — a clean dispose
/// would flush them to SST files and retire the log), then cuts bytes off the tail of that snapshot's
/// log file. That is byte-for-byte the shape a SIGKILL leaves.</para>
///
/// <para>The second test is the counterweight: damage in the middle of the log must still be refused.
/// Tolerating a torn tail must not become tolerating bit-rot.</para>
/// </summary>
public sealed class TestTornWalTailReopen
{
    private const int Partition = 1;

    /// <summary>
    /// A SIGKILLed node reopens its WAL and keeps the committed prefix that was fsynced before the
    /// kill. The torn tail record is dropped: it was never acknowledged to any caller, because
    /// <c>syncWrites: true</c> acknowledges a write only after its fsync completes.
    /// </summary>
    [Fact]
    public void TornTailRecord_WalReopens_AndKeepsCommittedPrefix()
    {
        string live = CreateTempWalPath();
        string crashed = CreateTempWalPath();

        try
        {
            RocksDbWAL wal = new(live, "wal", NullLogger<IRaft>.Instance, syncWrites: true);
            try
            {
                // Two separate writes, so the log file holds two separate records. Cutting the tail
                // must cost the second write only.
                Assert.Equal(
                    RaftOperationStatus.Success,
                    wal.Write([(Partition, [Log(1), Log(2), Log(3), Log(4)])]));

                Assert.Equal(
                    RaftOperationStatus.Success,
                    wal.Write([(Partition, [Log(5)])]));

                // Snapshot while open: this is the on-disk state a kill leaves behind.
                CopyDirectory(Path.Combine(live, "wal"), Path.Combine(crashed, "wal"));
            }
            finally
            {
                wal.Dispose();
            }

            long removed = TruncateNewestLogFile(Path.Combine(crashed, "wal"), bytesToCut: 4);
            Assert.Equal(4, removed);

            using RocksDbWAL reopened = new(crashed, "wal", NullLogger<IRaft>.Instance, syncWrites: true);

            List<long> ids = reopened.ReadLogs(Partition).Select(l => l.Id).ToList();

            // The prefix that was fsynced before the kill survives whole.
            Assert.Equal([1L, 2L, 3L, 4L], ids);
            Assert.Equal(4, reopened.GetMaxLog(Partition));
            Assert.Equal(4, reopened.GetCurrentTerm(Partition));
        }
        finally
        {
            DeleteTempWalPath(live);
            DeleteTempWalPath(crashed);
        }
    }

    /// <summary>
    /// Damage in the middle of the log is still fatal. A short record can only ever be the last one, so
    /// tolerating it costs nothing; a checksum mismatch with valid records after it is real corruption,
    /// and opening on it would replay a log with a hole in it.
    /// </summary>
    [Fact]
    public void CorruptionInTheMiddleOfTheLog_StillRefusesToOpen()
    {
        string live = CreateTempWalPath();
        string corrupted = CreateTempWalPath();

        try
        {
            RocksDbWAL wal = new(live, "wal", NullLogger<IRaft>.Instance, syncWrites: true);
            try
            {
                Assert.Equal(
                    RaftOperationStatus.Success,
                    wal.Write([(Partition, [Log(1), Log(2), Log(3), Log(4)])]));

                Assert.Equal(
                    RaftOperationStatus.Success,
                    wal.Write([(Partition, [Log(5)])]));

                CopyDirectory(Path.Combine(live, "wal"), Path.Combine(corrupted, "wal"));
            }
            finally
            {
                wal.Dispose();
            }

            // Offset 8 is inside the body of the first record, so its header stays intact and its
            // checksum fails while whole records still follow it.
            FlipByteInNewestLogFile(Path.Combine(corrupted, "wal"), offset: 8);

            RocksDbException error = Assert.Throws<RocksDbException>(
                () => new RocksDbWAL(corrupted, "wal", NullLogger<IRaft>.Instance, syncWrites: true));

            Assert.Contains("orruption", error.Message);
        }
        finally
        {
            DeleteTempWalPath(live);
            DeleteTempWalPath(corrupted);
        }
    }

    private static RaftLog Log(long id) => new()
    {
        Id = id,
        Term = id,
        Type = RaftLogType.Committed,
        LogType = "torn-tail"
    };

    /// <summary>Cuts <paramref name="bytesToCut"/> bytes off the newest RocksDB log file.</summary>
    private static long TruncateNewestLogFile(string dbDirectory, int bytesToCut)
    {
        FileInfo log = NewestLogFile(dbDirectory);
        long target = log.Length - bytesToCut;

        Assert.True(target > 0, $"log file {log.Name} is only {log.Length} bytes");

        using FileStream stream = File.Open(log.FullName, FileMode.Open, FileAccess.Write);
        stream.SetLength(target);

        return log.Length - target;
    }

    /// <summary>Flips one byte of the newest RocksDB log file, at <paramref name="offset"/>.</summary>
    private static void FlipByteInNewestLogFile(string dbDirectory, int offset)
    {
        FileInfo log = NewestLogFile(dbDirectory);

        Assert.True(log.Length > offset, $"log file {log.Name} is only {log.Length} bytes");

        using FileStream stream = File.Open(log.FullName, FileMode.Open, FileAccess.ReadWrite);
        stream.Seek(offset, SeekOrigin.Begin);

        int current = stream.ReadByte();

        stream.Seek(offset, SeekOrigin.Begin);
        stream.WriteByte((byte)(current ^ 0xFF));
    }

    private static FileInfo NewestLogFile(string dbDirectory)
    {
        FileInfo? log = new DirectoryInfo(dbDirectory)
            .GetFiles("*.log")
            .OrderByDescending(f => f.Name)
            .FirstOrDefault();

        Assert.NotNull(log);
        Assert.True(log.Length > 0, $"log file {log.Name} is empty");

        return log;
    }

    private static void CopyDirectory(string source, string destination)
    {
        Directory.CreateDirectory(destination);

        foreach (string file in Directory.GetFiles(source))
        {
            // RocksDB holds the LOCK file open exclusively while the database is open, and the copy
            // does not need it: the reopened snapshot makes its own.
            if (Path.GetFileName(file) == "LOCK")
                continue;

            // Shared read, because the source database is still open. File.Copy asks for exclusive
            // access and fails here.
            using FileStream from = new(file, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
            using FileStream to = new(Path.Combine(destination, Path.GetFileName(file)), FileMode.CreateNew, FileAccess.Write);

            from.CopyTo(to);
        }

        foreach (string directory in Directory.GetDirectories(source))
            CopyDirectory(directory, Path.Combine(destination, Path.GetFileName(directory)));
    }

    private static string CreateTempWalPath()
    {
        string path = Path.Combine(Path.GetTempPath(), $"kommander-torn-tail-{Guid.NewGuid():N}");
        Directory.CreateDirectory(path);
        return path;
    }

    private static void DeleteTempWalPath(string path)
    {
        if (Directory.Exists(path))
            Directory.Delete(path, recursive: true);
    }
}
