
using Kommander.Data;
using Kommander.WAL;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.WAL;

/// <summary>
/// Reopen/restart tests for the durable snapshot-boundary install: a boundary written with
/// <c>sync: true</c> to a persistent backend must survive a close/reopen with the same checkpoint index,
/// term, and post-install max — i.e. recovery reconstructs the same boundary and cursors. Exercised for
/// both the retain-suffix (matching term) and truncate-suffix (conflicting term) cases across the two
/// durable backends. The in-memory backend has no on-disk state to reopen, so it is not parameterized.
/// </summary>
public sealed class TestSnapshotBoundaryReopen
{
    public enum WalBackend { RocksDb, Sqlite }

    private static readonly ILogger<IRaft> Logger = NullLogger<IRaft>.Instance;

    private static IWAL CreateWal(WalBackend backend, string path) => backend switch
    {
        WalBackend.RocksDb => new RocksDbWAL(path, "wal", Logger, syncWrites: true),
        WalBackend.Sqlite => new SqliteWAL(path, "wal", Logger, syncWrites: true),
        _ => throw new ArgumentOutOfRangeException(nameof(backend)),
    };

    private static RaftLog Log(long id, long term) =>
        new() { Id = id, Term = term, Type = RaftLogType.Committed, LogType = "reopen" };

    [Theory]
    [InlineData(WalBackend.RocksDb)]
    [InlineData(WalBackend.Sqlite)]
    public void MatchingTerm_BoundaryAndRetainedSuffix_SurviveReopen(WalBackend backend)
    {
        string path = CreateTempWalPath();
        try
        {
            using (IWAL wal = CreateWal(backend, path))
            {
                wal.Write([(1, [Log(1, 2), Log(2, 2), Log(3, 2), Log(4, 2), Log(5, 2)])]);
                (RaftOperationStatus status, bool truncated) = wal.InstallSnapshotBoundary(1, snapshotIndex: 3, lastIncludedTerm: 2, sync: true);
                Assert.Equal(RaftOperationStatus.Success, status);
                Assert.False(truncated);
            }

            // Reopen on the same path and assert the boundary + retained suffix reconstruct identically.
            using (IWAL reopened = CreateWal(backend, path))
            {
                Assert.Equal(3, reopened.GetLastCheckpoint(1));
                Assert.Equal(2, reopened.GetTermAt(1, 3));
                Assert.Equal(5, reopened.GetMaxLog(1));
                Assert.Equal([1L, 2L, 3L, 4L, 5L], reopened.ReadLogsRange(1, 1).Select(l => l.Id));
            }
        }
        finally { DeleteTempWalPath(path); }
    }

    [Theory]
    [InlineData(WalBackend.RocksDb)]
    [InlineData(WalBackend.Sqlite)]
    public void ConflictingTerm_BoundaryAndTruncatedSuffix_SurviveReopen(WalBackend backend)
    {
        string path = CreateTempWalPath();
        try
        {
            using (IWAL wal = CreateWal(backend, path))
            {
                wal.Write([(1, [Log(1, 2), Log(2, 2), Log(3, 2), Log(4, 2), Log(5, 2)])]);
                (RaftOperationStatus status, bool truncated) = wal.InstallSnapshotBoundary(1, snapshotIndex: 3, lastIncludedTerm: 9, sync: true);
                Assert.Equal(RaftOperationStatus.Success, status);
                Assert.True(truncated);
            }

            using (IWAL reopened = CreateWal(backend, path))
            {
                Assert.Equal(3, reopened.GetLastCheckpoint(1));
                Assert.Equal(9, reopened.GetTermAt(1, 3));
                Assert.Equal(3, reopened.GetMaxLog(1));  // suffix truncated durably
                Assert.Equal([1L, 2L, 3L], reopened.ReadLogsRange(1, 1).Select(l => l.Id));
            }
        }
        finally { DeleteTempWalPath(path); }
    }

    private static string CreateTempWalPath()
    {
        string path = Path.Combine(Path.GetTempPath(), $"kommander-snapboundary-{Guid.NewGuid():N}");
        Directory.CreateDirectory(path);
        return path;
    }

    private static void DeleteTempWalPath(string path)
    {
        try
        {
            if (Directory.Exists(path))
                Directory.Delete(path, recursive: true);
        }
        catch
        {
            // best-effort cleanup
        }
    }
}
