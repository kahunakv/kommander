
using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Kommander.WAL.IO;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.WAL;

/// <summary>
/// The persisted last-checkpoint id is trusted as an applied/compacted certificate for its whole
/// prefix: restore frontier seeding jumps over it, the apply drains skip "compacted" ids at or
/// below it, and compaction deletes rows under it. A <see cref="RaftLogType.CommittedCheckpoint"/>
/// row that lands OVER a replication gap — the unanchored commit broadcast reaching a follower
/// that is still backfilling — must therefore land as a plain row without advancing the floor.
///
/// <para>Before this rule, a catching-up follower whose gap was still open would persist the
/// leader's checkpoint id, silently mark the gap "compacted" in the apply drain, let compaction
/// delete the backfilled rows before they were ever delivered, and then win an election and serve
/// a projection missing committed writes (the split-nemesis-indeterminate Caraxes run: 15 committed
/// transfers durably lost after a 20-second SIGKILL of one node).</para>
/// </summary>
public sealed class TestCheckpointOverGapFloor
{
    private const int P = 3;

    private static RaftLog Log(long id, RaftLogType type = RaftLogType.Committed, long term = 1) =>
        new() { Id = id, Term = term, Type = type, LogType = "gap-floor" };

    private static IWAL Open(string kind, string dir) => kind switch
    {
        "rocksdb" => new RocksDbWAL(dir, "rev1", NullLogger<IRaft>.Instance, syncWrites: true),
        "sqlite" => new SqliteWAL(dir, "rev1", NullLogger<IRaft>.Instance, syncWrites: true),
        "inmemory" => new InMemoryWAL(NullLogger<IRaft>.Instance),
        _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "unknown WAL kind"),
    };

    private static void RunWithWal(string kind, Action<IWAL> body)
    {
        string dir = Path.Combine(Path.GetTempPath(), $"wal-gap-floor-{kind}-{Guid.NewGuid():N}");
        Directory.CreateDirectory(dir);
        try
        {
            IWAL wal = Open(kind, dir);
            try
            {
                body(wal);
            }
            finally
            {
                wal.Dispose();
            }
        }
        finally
        {
            if (Directory.Exists(dir))
                Directory.Delete(dir, recursive: true);
        }
    }

    /// <summary>
    /// A checkpoint row above an unfilled gap stores the row but withholds the floor; once the gap
    /// is backfilled, the next checkpoint advances the floor over the whole (now contiguous) prefix.
    /// </summary>
    [Theory]
    [InlineData("rocksdb")]
    [InlineData("sqlite")]
    [InlineData("inmemory")]
    public void CheckpointOverGap_WithholdsFloor_UntilBackfilled(string kind)
    {
        RunWithWal(kind, wal =>
        {
            wal.Write([(P, [Log(1), Log(2), Log(3)])]);

            // The over-gap broadcast shape: ids 4..9 are absent when the checkpoint at 10 lands.
            wal.Write([(P, [Log(10, RaftLogType.CommittedCheckpoint)])]);
            Assert.Equal(-1, wal.GetLastCheckpoint(P));

            // Backfill closes the gap; the floor stays where it was until a checkpoint re-attests.
            wal.Write([(P, [Log(4), Log(5), Log(6), Log(7), Log(8), Log(9)])]);
            Assert.Equal(-1, wal.GetLastCheckpoint(P));

            // The next checkpoint finds the prefix contiguous and advances.
            wal.Write([(P, [Log(11, RaftLogType.CommittedCheckpoint)])]);
            Assert.Equal(11, wal.GetLastCheckpoint(P));
        });
    }

    /// <summary>
    /// A checkpoint whose prefix is contiguous — including rows staged in the same batch — advances
    /// the floor exactly as before.
    /// </summary>
    [Theory]
    [InlineData("rocksdb")]
    [InlineData("sqlite")]
    [InlineData("inmemory")]
    public void ContiguousCheckpoint_Advances_IncludingSameBatchRows(string kind)
    {
        RunWithWal(kind, wal =>
        {
            wal.Write([(P, [Log(1), Log(2), Log(3), Log(4, RaftLogType.CommittedCheckpoint)])]);
            Assert.Equal(4, wal.GetLastCheckpoint(P));

            // A duplicate/lower checkpoint never regresses the recorded id.
            wal.Write([(P, [Log(2, RaftLogType.CommittedCheckpoint)])]);
            Assert.Equal(4, wal.GetLastCheckpoint(P));
        });
    }

    /// <summary>A batch that carries its own interior gap withholds the floor too.</summary>
    [Theory]
    [InlineData("rocksdb")]
    [InlineData("sqlite")]
    [InlineData("inmemory")]
    public void CheckpointWithGapInsideBatch_WithholdsFloor(string kind)
    {
        RunWithWal(kind, wal =>
        {
            wal.Write([(P, [Log(1), Log(2), Log(5, RaftLogType.CommittedCheckpoint)])]);
            Assert.Equal(-1, wal.GetLastCheckpoint(P));
        });
    }

    /// <summary>
    /// Restore-side gating: a checkpoint row that landed over a gap (floor withheld) must not jump
    /// the reconstructed commit/presence frontiers over the missing range. Before the gating, the
    /// row certified ids 4..9 that this node never held, the node advertised the inflated frontier
    /// for election freshness, and the applied cursor was seeded past undelivered entries.
    /// </summary>
    [Fact]
    public async Task Restore_OverGapCheckpointRow_DoesNotJumpFrontiers()
    {
        RaftConfiguration config = new()
        {
            Host = "localhost",
            Port = 9001,
            InitialPartitions = 0,
            WalSingleFsyncCommit = true,
        };

        IWAL wal = new InMemoryWAL(NullLogger<IRaft>.Instance);

        // Committed 1..3, a hole at 4..9, the over-gap checkpoint row at 10 (floor withheld by the
        // WAL), and committed rows above it — the exact shape a killed follower restarts with.
        wal.Write([(P, [Log(1), Log(2), Log(3)])]);
        wal.Write([(P, [Log(10, RaftLogType.CommittedCheckpoint), Log(11), Log(12)])]);
        Assert.Equal(-1, wal.GetLastCheckpoint(P));

        RaftManager manager = new(
            config,
            new StaticDiscovery([]),
            wal,
            new InMemoryCommunication(),
            new HybridLogicalClock(),
            NullLogger<IRaft>.Instance);

        ((FairReadScheduler)manager.ReadScheduler).Start();
        ((FairWalScheduler)manager.WalScheduler).Start();

        RaftPartition partition = new(
            manager,
            wal,
            P,
            startRange: 0,
            endRange: 0,
            NullLogger<IRaft>.Instance);

        try
        {
            RaftWriteAhead writeAhead = new(manager, _ => { }, partition, wal);

            IReadOnlyList<RaftLog> logs = await writeAhead.LoadRestoreLogsAsync();
            await writeAhead.CompleteRestoreAsync(logs);

            // The frontiers stop at the last contiguous entry; nothing certifies 4..9 here.
            Assert.Equal(3, writeAhead.GetCommitIndex());
            Assert.Equal(3, writeAhead.GetPresentIndex());
        }
        finally
        {
            partition.Dispose();
            manager.Dispose();
        }
    }
}
