
using Kommander;
using Kommander.Data;
using Kommander.WAL;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.WAL;

/// <summary>
/// Verifies that <see cref="RocksDbWAL.InstallSnapshotBoundary"/> excludes concurrent appends. The boundary
/// enumerates the suffix keys and then deletes them in a later batch; it dispatches on the read scheduler
/// while appends dispatch on the WAL write scheduler, so without an explicit guard an append landing between
/// the scan and the delete batch would survive the truncation. The install now holds a shared write guard for
/// the whole scan+write, so a racing append is serialized behind it.
/// </summary>
public sealed class RocksDbSnapshotBoundaryConcurrencyTests
{
    private static RaftLog Log(long id, long term = 1, RaftLogType type = RaftLogType.Committed) =>
        new() { Id = id, Term = term, Type = type, LogType = "t" };

    [Fact]
    public void InstallSnapshotBoundary_RacingAppend_IsExcludedByWriteGuard()
    {
        string path = Path.Combine(Path.GetTempPath(), "kommander-rocks-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(path);
        try
        {
            bool appendCompletedWhileBoundaryHeldGuard;
            Thread? appendThread = null;

            using (RocksDbWAL wal = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: true))
            {
                // Seed a committed prefix 1..5 (term 1).
                wal.Write([(1, [Log(1), Log(2), Log(3), Log(4), Log(5)])]);

                bool completed = false;
                // While the boundary holds the write guard (after its suffix scan, before its batch write),
                // launch a racing append and confirm it CANNOT complete — it must block on the guard.
                wal.OnAfterBoundaryScanForTesting = () =>
                {
                    Thread t = new(() => wal.Write([(1, [Log(6)])])) { IsBackground = true };
                    t.Start();
                    // Bounded wait: with the guard held, the append blocks and does not finish in this window.
                    completed = t.Join(TimeSpan.FromMilliseconds(500));
                    appendThread = t;
                };

                // Conflicting term at the boundary index → the install truncates the suffix above index 5.
                (RaftOperationStatus status, _) = wal.InstallSnapshotBoundary(1, snapshotIndex: 5, lastIncludedTerm: 2, sync: true);
                Assert.Equal(RaftOperationStatus.Success, status);

                appendCompletedWhileBoundaryHeldGuard = completed;
                appendThread?.Join(TimeSpan.FromSeconds(5)); // released now; let the append finish
            }

            Assert.False(appendCompletedWhileBoundaryHeldGuard,
                "a racing append must be blocked by the write guard while the boundary install holds it");

            // Reopen: the boundary is durable at index 5 (term 2) and the racing append landed only AFTER the
            // boundary write (index 6), so it is a legitimate post-boundary entry — never a suffix that
            // survived the truncation window.
            using (RocksDbWAL reopened = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: true))
            {
                Assert.Equal(5, reopened.GetLastCheckpoint(1));
                Assert.Equal(2, reopened.GetTermAt(1, 5));   // checkpoint stamped with last-included term
                Assert.Equal(6, reopened.GetMaxLog(1));       // the post-boundary append
            }
        }
        finally
        {
            try { Directory.Delete(path, recursive: true); } catch { /* best effort */ }
        }
    }
}
