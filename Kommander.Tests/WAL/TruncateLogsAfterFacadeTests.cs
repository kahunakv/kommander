
using Kommander;
using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.Scheduling;
using Kommander.Time;
using Kommander.WAL;
using Kommander.WAL.IO;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.WAL;

/// <summary>
/// Unit tests for <see cref="IRaftWalFacade.TruncateLogsAfterAsync"/>.
/// Verifies:
/// <list type="bullet">
///   <item>GetMaxLog returns the true max above a hole; GetAnyTermAt returns -1 at the missing index.</item>
///   <item>TruncateLogsAfterAsync(N-1) removes the holey tail atomically and returns the new max.</item>
///   <item>The operation is no-op-safe when the cut point is at or above the current max.</item>
///   <item>GetCommitIndex() returns the highest committed id (commitIndex field - 1).</item>
///   <item>Safety-guard precondition: a hole above the committed frontier is safe to truncate and
///         never discards a committed entry.</item>
/// </list>
/// </summary>
public sealed class TruncateLogsAfterFacadeTests
{
    private const int PartitionId = 1;

    // ── helpers ───────────────────────────────────────────────────────────────

    private static RaftLog CommittedLog(long id) => new()
    {
        Id      = id,
        Type    = RaftLogType.Committed,
        Term    = 1,
        LogType = "",
        LogData = [],
    };

    /// <summary>
    /// Builds an InMemoryWAL-backed RaftWriteAhead and a RaftWalFacadeAdapter that wraps it.
    /// The ReadScheduler is started so TruncateLogsAfterAsync and GetAnyTermAtAsync (both
    /// scheduled on it) can complete.
    /// </summary>
    private static (IRaftWalFacade facade, RaftWriteAhead writeAhead, InMemoryWAL wal, RaftManager manager, RaftPartition partition) CreateFacade()
    {
        InMemoryWAL wal = new(NullLogger<IRaft>.Instance);

        RaftConfiguration config = new()
        {
            Host             = "localhost",
            Port             = 9000,
            InitialPartitions = 0,
        };

        RaftManager manager = new(
            config,
            new StaticDiscovery([]),
            wal,
            new InMemoryCommunication(),
            new HybridLogicalClock(),
            NullLogger<IRaft>.Instance);

        ((FairReadScheduler)manager.ReadScheduler).Start();

        RaftPartition partition = new(
            manager,
            wal,
            PartitionId,
            startRange: 0,
            endRange:   0,
            NullLogger<IRaft>.Instance);

        RaftWriteAhead writeAhead = new(manager, _ => { }, partition, wal);
        IRaftWalFacade facade = new RaftWalFacadeAdapter(writeAhead);

        return (facade, writeAhead, wal, manager, partition);
    }

    /// <summary>
    /// Writes entries 1..m to the WAL, skipping <paramref name="holeAt"/> to produce a hole.
    /// </summary>
    private static void SeedWithHole(InMemoryWAL wal, int m, long holeAt)
    {
        List<RaftLog> logs = [];
        for (long id = 1; id <= m; id++)
        {
            if (id == holeAt) continue;
            logs.Add(CommittedLog(id));
        }
        wal.Write([(PartitionId, logs)]);
    }

    // ── symptom pin ───────────────────────────────────────────────────────────

    /// <summary>
    /// GetMaxLogAsync must return M (the true max above the hole) and
    /// GetAnyTermAtAsync must return -1 at the missing index.
    /// These are the observable symptoms the hole-repair path keys on.
    /// </summary>
    [Fact]
    public async Task GetMaxLog_ReturnsMaxAboveHole_AndGetAnyTermAtHole_ReturnsMinusOne()
    {
        (IRaftWalFacade facade, _, InMemoryWAL wal, RaftManager manager, RaftPartition partition) = CreateFacade();

        try
        {
            const int m      = 8;
            const long holeAt = 4;

            SeedWithHole(wal, m, holeAt);

            long max  = await facade.GetMaxLogAsync();
            long term = await facade.GetAnyTermAtAsync(holeAt);

            Assert.Equal(m, max);
            Assert.Equal(-1, term);
        }
        finally
        {
            partition.Dispose();
            manager.Dispose();
        }
    }

    // ── truncate-and-measure semantics ────────────────────────────────────────

    /// <summary>
    /// TruncateLogsAfterAsync(N-1) removes everything ≥ N (the hole and all above it)
    /// and returns N-1 as the new max. The log is contiguous 1..N-1 afterward.
    /// </summary>
    [Fact]
    public async Task TruncateLogsAfterAsync_RemovesHoleTail_ReturnsNewMax()
    {
        (IRaftWalFacade facade, _, InMemoryWAL wal, RaftManager manager, RaftPartition partition) = CreateFacade();

        try
        {
            const int m      = 8;
            const long holeAt = 4;

            SeedWithHole(wal, m, holeAt);

            long newMax = await facade.TruncateLogsAfterAsync(holeAt - 1);

            Assert.Equal(holeAt - 1, newMax);

            // Reported max must match.
            Assert.Equal(holeAt - 1, await facade.GetMaxLogAsync());

            // Entries 1..holeAt-1 must survive.
            for (long id = 1; id <= holeAt - 1; id++)
                Assert.True(await facade.GetAnyTermAtAsync(id) >= 0,
                    $"Entry {id} must exist after truncation");

            // Everything from holeAt onward must be gone.
            for (long id = holeAt; id <= m; id++)
                Assert.Equal(-1, await facade.GetAnyTermAtAsync(id));
        }
        finally
        {
            partition.Dispose();
            manager.Dispose();
        }
    }

    /// <summary>
    /// TruncateLogsAfterAsync is no-op-safe: a cut point at or above the current max
    /// leaves the log unchanged and returns the existing max.
    /// </summary>
    [Fact]
    public async Task TruncateLogsAfterAsync_CutAboveMax_IsNoOp()
    {
        (IRaftWalFacade facade, _, InMemoryWAL wal, RaftManager manager, RaftPartition partition) = CreateFacade();

        try
        {
            const int m = 5;
            List<RaftLog> logs = Enumerable.Range(1, m).Select(i => CommittedLog(i)).ToList();
            wal.Write([(PartitionId, logs)]);

            long newMax = await facade.TruncateLogsAfterAsync(m + 10);

            Assert.Equal(m, newMax);
            Assert.Equal(m, await facade.GetMaxLogAsync());
        }
        finally
        {
            partition.Dispose();
            manager.Dispose();
        }
    }

    // ── GetCommitIndex convention ─────────────────────────────────────────────

    /// <summary>
    /// GetCommitIndex() returns commitIndex-1 (the highest committed id).
    /// Before any restore the default is 0 (commitIndex field starts at 1).
    /// After CompleteRestoreAsync with entries 1..C the result is C.
    /// </summary>
    [Fact]
    public async Task GetCommitIndex_ReturnsHighestCommittedId()
    {
        (IRaftWalFacade facade, _, InMemoryWAL wal, RaftManager manager, RaftPartition partition) = CreateFacade();

        try
        {
            // No entries restored yet: committed frontier is 0.
            Assert.Equal(0, facade.GetCommitIndex());

            const int commitFrontier = 5;
            List<RaftLog> logs = Enumerable.Range(1, commitFrontier).Select(i => CommittedLog(i)).ToList();
            wal.Write([(PartitionId, logs)]);

            // CompleteRestoreAsync advances the in-memory commitIndex to commitFrontier+1.
            await facade.CompleteRestoreAsync(logs);

            Assert.Equal(commitFrontier, facade.GetCommitIndex());
        }
        finally
        {
            partition.Dispose();
            manager.Dispose();
        }
    }

    // ── safety-guard precondition ─────────────────────────────────────────────

    /// <summary>
    /// Safety-guard precondition: a hole at N with committed frontier C &lt; N satisfies
    /// N > GetCommitIndex(), so truncating to N-1 discards no committed entry.
    /// The committed prefix 1..C must be present after truncation.
    /// </summary>
    [Fact]
    public async Task SafetyGuard_HoleAboveCommitFrontier_TruncationPreservesCommitted()
    {
        (IRaftWalFacade facade, _, InMemoryWAL wal, RaftManager manager, RaftPartition partition) = CreateFacade();

        try
        {
            const int commitFrontier = 3;  // entries 1..3 committed
            const int m      = 8;
            const long holeAt = 5;          // hole above committed frontier

            // Advance the in-memory commit frontier via CompleteRestoreAsync.
            List<RaftLog> committedLogs = Enumerable.Range(1, commitFrontier)
                .Select(i => CommittedLog(i)).ToList();
            await facade.CompleteRestoreAsync(committedLogs);
            Assert.Equal(commitFrontier, facade.GetCommitIndex());

            // Now seed WAL with a holey log (entries 1..4, 6..8).
            SeedWithHole(wal, m, holeAt);

            // Guard condition: holeAt > committed frontier → truncation is safe.
            Assert.True(holeAt > facade.GetCommitIndex(),
                "hole must be strictly above the committed frontier for the guard to permit truncation");

            long newMax = await facade.TruncateLogsAfterAsync(holeAt - 1);
            Assert.Equal(holeAt - 1, newMax);

            // Committed entries 1..commitFrontier must still be present.
            for (long id = 1; id <= commitFrontier; id++)
                Assert.True(await facade.GetAnyTermAtAsync(id) >= 0,
                    $"Committed entry {id} must survive truncation");
        }
        finally
        {
            partition.Dispose();
            manager.Dispose();
        }
    }
}
