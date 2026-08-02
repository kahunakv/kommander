
using Kommander.Data;
using Kommander.WAL;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.WAL;

/// <summary>
/// Crash/reopen coverage for the "persist last-checkpoint id" feature on the durable backends. The
/// persisted checkpoint id must survive a dispose/reopen of the same data directory and drive the correct
/// <c>ReadLogs</c> replay floor after restart — the exact restore path that used to pay a reverse scan.
/// InMemoryWAL is excluded because it cannot reopen.
/// </summary>
public sealed class TestLastCheckpointReopen
{
    private static RaftLog Log(long id, long term = 1, RaftLogType type = RaftLogType.Committed) =>
        new() { Id = id, Term = term, Type = type, LogType = "reopen" };

    /// <summary>Opens a fresh durable WAL of the requested kind at <paramref name="dir"/>/<c>rev1</c>.</summary>
    private static IWAL Open(string kind, string dir) => kind switch
    {
        "rocksdb" => new RocksDbWAL(dir, "rev1", NullLogger<IRaft>.Instance, syncWrites: true),
        "sqlite" => new SqliteWAL(dir, "rev1", NullLogger<IRaft>.Instance, syncWrites: true),
        _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "unknown WAL kind"),
    };

    [Theory]
    [InlineData("rocksdb")]
    [InlineData("sqlite")]
    public void LastCheckpoint_SurvivesReopen_AndDrivesReplayFloor(string kind)
    {
        string dir = Path.Combine(Path.GetTempPath(), $"wal-cp-reopen-{kind}-{Guid.NewGuid():N}");
        Directory.CreateDirectory(dir);

        try
        {
            const int p = 3;

            IWAL wal = Open(kind, dir);
            try
            {
                wal.Write([(p, [Log(1), Log(2, type: RaftLogType.CommittedCheckpoint), Log(3)])]);
                Assert.Equal(2, wal.GetLastCheckpoint(p));
                // Replay floor before restart: from the checkpoint (inclusive).
                Assert.Equal([2L, 3L], wal.ReadLogs(p).Select(l => l.Id));
            }
            finally { wal.Dispose(); }

            // Reopen the same directory — simulates a process restart / crash recovery.
            IWAL reopened = Open(kind, dir);
            try
            {
                // The persisted checkpoint id survived the restart (no reverse scan needed to find it)...
                Assert.Equal(2, reopened.GetLastCheckpoint(p));
                // ...and restore uses it as the replay floor, returning the checkpoint entry and newer.
                Assert.Equal([2L, 3L], reopened.ReadLogs(p).Select(l => l.Id));
            }
            finally { reopened.Dispose(); }
        }
        finally
        {
            if (Directory.Exists(dir))
                Directory.Delete(dir, recursive: true);
        }
    }

    [Theory]
    [InlineData("rocksdb")]
    [InlineData("sqlite")]
    public void NoCheckpoint_AfterReopen_ReturnsNegativeOne(string kind)
    {
        string dir = Path.Combine(Path.GetTempPath(), $"wal-cp-reopen-none-{kind}-{Guid.NewGuid():N}");
        Directory.CreateDirectory(dir);

        try
        {
            const int p = 4;

            IWAL wal = Open(kind, dir);
            try
            {
                wal.Write([(p, [Log(1), Log(2)])]); // no checkpoint written
                Assert.Equal(-1, wal.GetLastCheckpoint(p));
            }
            finally { wal.Dispose(); }

            IWAL reopened = Open(kind, dir);
            try
            {
                Assert.Equal(-1, reopened.GetLastCheckpoint(p));
                // With no checkpoint, restore replays the whole log from the start.
                Assert.Equal([1L, 2L], reopened.ReadLogs(p).Select(l => l.Id));
            }
            finally { reopened.Dispose(); }
        }
        finally
        {
            if (Directory.Exists(dir))
                Directory.Delete(dir, recursive: true);
        }
    }
}
