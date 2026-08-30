using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Kommander.WAL.IO;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.WAL;

/// <summary>
/// The scheduler's proposed-tail truncation must never delete rows this process accepted.
///
/// <para>The sweep's founding claim — "an append whose rows end at X proves Proposed rows above X
/// are a dead term's orphans" — only holds for a live tail append. A commit-marker re-ship of an
/// OLD id says nothing about the tail: with pipelined proposals the commit of id k always races
/// proposals k+1... Before the fix, a duplicate commit-marker batch for the committed prefix
/// (arriving as its own scheduler batch after higher proposes had landed) drove the truncation
/// cutoff down to its own max id and silently deleted the freshly-acked Proposed rows above it —
/// no log line, no frontier regression. That is what manufactured the shared hole at index 6 on
/// four of five voters in Jepsen run 32690955741 (register/pause): the SIGSTOPped leader's commit
/// fan-out for the prefix was redelivered after proposes 6..8 had landed and been acked, the rows
/// were deleted on every follower, the leader froze before ever re-shipping entry 6, and the
/// partition stayed leaderless for the rest of the run.</para>
///
/// <para>Drives the REAL pipeline: <see cref="RaftWriteAhead.EnqueueProposeOrCommit"/> →
/// <see cref="FairWalScheduler"/> (including its pre-batch truncation) → <see cref="InMemoryWAL"/>,
/// then asserts on the rows read back through the read scheduler.</para>
/// </summary>
public sealed class TestProposedTailTruncationSafety
{
    /// <summary>
    /// The exact Jepsen shape: proposes 6..8 land (separate batches, durably written and
    /// presence-certified), then a duplicate commit-marker batch for the already-committed id 5
    /// arrives. The marker op's max id is 5; before the fix its batch ran
    /// <c>TruncateProposedLogsAfter(5)</c> and deleted rows 6..8 while the presence frontier kept
    /// advertising 8. The rows must survive, as Proposed, with the frontier intact.
    /// </summary>
    [Fact]
    public async Task DuplicateCommitMarkerBatch_DoesNotDeleteAckedProposedRowsAboveIt()
    {
        RaftWriteAhead writeAhead = CreateWriteAhead(out RaftManager manager, out RaftPartition partition);

        try
        {
            Append(writeAhead, Committed(1), Committed(2), Committed(3), Committed(4), Committed(5));
            Assert.Equal(5, writeAhead.GetCommitIndex());

            Append(writeAhead, Proposed(6));
            Append(writeAhead, Proposed(7));
            Append(writeAhead, Proposed(8));

            // Let the write scheduler apply the proposes so their rows are durably on disk before
            // the marker batch arrives — the kill shape needs them in EARLIER batches.
            Assert.NotNull(await ReadRowAsync(writeAhead, 8));
            Assert.Equal(8, writeAhead.GetPresentIndex());

            // The redelivered commit marker of the already-committed prefix (max id 5).
            Append(writeAhead, Committed(5));

            RaftLog? row6 = await ReadRowAsync(writeAhead, 6);
            RaftLog? row7 = await ReadRowAsync(writeAhead, 7);
            RaftLog? row8 = await ReadRowAsync(writeAhead, 8);

            Assert.NotNull(row6);
            Assert.NotNull(row7);
            Assert.NotNull(row8);
            Assert.Equal(RaftLogType.Proposed, row6!.Type);
            Assert.Equal(RaftLogType.Proposed, row7!.Type);
            Assert.Equal(RaftLogType.Proposed, row8!.Type);
            Assert.Equal(8, writeAhead.GetPresentIndex());
        }
        finally
        {
            partition.Dispose();
            manager.Dispose();
        }
    }

    /// <summary>
    /// A late proposal RETRY of a low id (a deposed leader's re-send after a fault window) is a
    /// Proposed-carrying op, so it still drives the sweep — but its cutoff must be clamped to the
    /// enqueue-time accepted floor, so the higher rows this node already accepted survive it.
    /// </summary>
    [Fact]
    public async Task LateProposalRetryBelowAckedRows_DoesNotDeleteThem()
    {
        RaftWriteAhead writeAhead = CreateWriteAhead(out RaftManager manager, out RaftPartition partition);

        try
        {
            Append(writeAhead, Committed(1), Committed(2), Committed(3), Committed(4), Committed(5));
            Append(writeAhead, Proposed(6));
            Append(writeAhead, Proposed(7));
            Append(writeAhead, Proposed(8));

            Assert.NotNull(await ReadRowAsync(writeAhead, 8));

            // The retry of proposal 6 alone: op max id 6, Proposed-carrying.
            Append(writeAhead, Proposed(6));

            RaftLog? row7 = await ReadRowAsync(writeAhead, 7);
            RaftLog? row8 = await ReadRowAsync(writeAhead, 8);

            Assert.NotNull(row7);
            Assert.NotNull(row8);
            Assert.Equal(RaftLogType.Proposed, row7!.Type);
            Assert.Equal(RaftLogType.Proposed, row8!.Type);
            Assert.Equal(8, writeAhead.GetPresentIndex());
        }
        finally
        {
            partition.Dispose();
            manager.Dispose();
        }
    }

    /// <summary>
    /// The commit frontier must never certify past an unresolved gap. Before the fix,
    /// <see cref="RaftWriteAhead.EnqueueCommit"/> jumped the frontier monotonically to the highest
    /// committed id: on a node whose log had a hole, a promotion-barrier no-op commit poisoned the
    /// frontier past the hole, and every later promotion refused forever on a committed drain that
    /// could never reach it (the majority-hole leaderless wedge). The commit of an over-gap id is
    /// now buffered and only certified once the gap resolves.
    /// </summary>
    [Fact]
    public async Task EnqueueCommit_AboveAGap_BuffersInsteadOfJumpingTheFrontier()
    {
        RaftWriteAhead writeAhead = CreateWriteAhead(out RaftManager manager, out RaftPartition partition);

        try
        {
            Append(writeAhead, Committed(1), Committed(2), Committed(3), Committed(4), Committed(5));
            Assert.Equal(5, writeAhead.GetCommitIndex());

            // A lone high proposal over a gap at 6 (the unanchored live-propose shape).
            Append(writeAhead, Proposed(7));
            Assert.NotNull(await ReadRowAsync(writeAhead, 7));
            Assert.Equal(5, writeAhead.GetPresentIndex());

            // The leader-side commit of the over-gap entry (e.g. a barrier no-op committing on a
            // poisoned quorum). The frontier must NOT jump to 7 over the unresolved 6.
            writeAhead.EnqueueCommit([Proposed(7)]);
            Assert.Equal(5, writeAhead.GetCommitIndex());

            // Filling the gap resolves it: the buffered commit drains and the frontier reaches 7.
            Append(writeAhead, Committed(6));
            Assert.Equal(7, writeAhead.GetCommitIndex());
            Assert.Equal(7, writeAhead.GetPresentIndex());
        }
        finally
        {
            partition.Dispose();
            manager.Dispose();
        }
    }

    /// <summary>
    /// A rollback resolves its ids, exactly like a commit: with the gap-buffered commit advance, a
    /// leader-side rollback must advance the resolution frontier, or every later commit above the
    /// rolled-back band buffers forever and the reported commit index sticks below the applied
    /// prefix (the CommitMonotonicity violations of CI run 33195170707 — Scenario08/09 pause
    /// chaos, where proposal-timeout rollbacks are constant). The old monotonic commit jump
    /// absorbed rolled-back bands implicitly; the gap-aware frontier needs the explicit advance,
    /// mirroring the follower append path where RolledBack rows already count as resolutions.
    /// </summary>
    [Fact]
    public void LeaderRollback_ResolvesTheFrontier_SoLaterCommitsDoNotStick()
    {
        RaftWriteAhead writeAhead = CreateWriteAhead(out RaftManager manager, out RaftPartition partition);

        try
        {
            RaftLog l1 = new() { LogType = "test", LogData = [1] };
            RaftLog l2 = new() { LogType = "test", LogData = [1] };
            RaftLog l3 = new() { LogType = "test", LogData = [1] };
            writeAhead.EnqueuePropose(1, [l1], HLCTimestamp.Zero, autoCommit: true);
            writeAhead.EnqueuePropose(1, [l2], HLCTimestamp.Zero, autoCommit: true);
            writeAhead.EnqueuePropose(1, [l3], HLCTimestamp.Zero, autoCommit: true);
            Assert.Equal(3, l3.Id);

            writeAhead.EnqueueCommit([l1]);
            Assert.Equal(1, writeAhead.GetCommitIndex());

            // Proposal 2 times out and rolls back; proposal 3 commits. The frontier must reach 3
            // — the rolled-back id is resolved, not a gap.
            writeAhead.EnqueueRollback([l2]);
            writeAhead.EnqueueCommit([l3]);
            Assert.Equal(3, writeAhead.GetCommitIndex());
        }
        finally
        {
            partition.Dispose();
            manager.Dispose();
        }
    }

    /// <summary>
    /// The facade adapter must actually forward <c>MarkInheritedCommitted</c> to the WAL: the
    /// interface declares a default no-op body, and for as long as the adapter did not override
    /// it, every inherited-drain frontier advance was silently swallowed — masked by the old
    /// monotonic commit jump until the gap-buffered frontier exposed it as a permanently pinned
    /// commit index on every new leader with an inherited tail (CI run 33195170707).
    /// </summary>
    [Fact]
    public void FacadeAdapter_ForwardsMarkInheritedCommitted_ToTheFrontier()
    {
        RaftWriteAhead writeAhead = CreateWriteAhead(out RaftManager manager, out RaftPartition partition);

        try
        {
            Append(writeAhead, Committed(1), Committed(2));
            Assert.Equal(2, writeAhead.GetCommitIndex());
            Append(writeAhead, Proposed(3));

            Kommander.Scheduling.IRaftWalFacade facade = new RaftWalFacadeAdapter(writeAhead);
            facade.MarkInheritedCommitted(3);

            Assert.Equal(3, writeAhead.GetCommitIndex());
        }
        finally
        {
            partition.Dispose();
            manager.Dispose();
        }
    }

    /// <summary>
    /// Promotion-time frontier reconciliation: a follower stint can leave the in-memory frontier
    /// below ids the node already delivered (its applied cursor advanced over them), and a leader
    /// is never backfilled — <c>AbsorbResolvedPrefix</c> absorbs the proven-resolved prefix and
    /// drains any buffered resolutions that became contiguous, so a new leader's commits stop
    /// buffering above a phantom gap.
    /// </summary>
    [Fact]
    public void AbsorbResolvedPrefix_AdvancesTheFrontier_AndDrainsBufferedResolutions()
    {
        RaftWriteAhead writeAhead = CreateWriteAhead(out RaftManager manager, out RaftPartition partition);

        try
        {
            Append(writeAhead, Committed(1), Committed(2));
            Append(writeAhead, Committed(4));            // buffered over the gap at 3
            Assert.Equal(2, writeAhead.GetCommitIndex());

            writeAhead.AbsorbResolvedPrefix(3);          // 3 proven resolved by other bookkeeping
            Assert.Equal(4, writeAhead.GetCommitIndex());

            writeAhead.AbsorbResolvedPrefix(2);          // never moves backwards
            Assert.Equal(4, writeAhead.GetCommitIndex());
        }
        finally
        {
            partition.Dispose();
            manager.Dispose();
        }
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    private static void Append(RaftWriteAhead writeAhead, params RaftLog[] logs) =>
        writeAhead.EnqueueProposeOrCommit([.. logs], HLCTimestamp.Zero, "leader:1", 1);

    /// <summary>
    /// Reads row <paramref name="id"/> back through the read scheduler, polling briefly: the
    /// physical write (and the pre-batch truncation) run on the WAL scheduler's workers, so the
    /// row state settles asynchronously after the enqueue returns.
    /// </summary>
    private static async Task<RaftLog?> ReadRowAsync(RaftWriteAhead writeAhead, long id)
    {
        RaftLog? row = null;

        for (int attempt = 0; attempt < 100; attempt++)
        {
            await Task.Delay(10);

            List<RaftLog> rows = await writeAhead.GetRangeAllTypesAsync(id, 1);
            row = rows.FirstOrDefault(l => l.Id == id);

            if (row is not null && attempt >= 3)
                return row;
        }

        return row;
    }

    private static RaftLog Committed(long id) => new()
    {
        Id = id,
        Term = 1,
        Type = RaftLogType.Committed,
        LogType = "test",
        LogData = [1],
    };

    private static RaftLog Proposed(long id) => new()
    {
        Id = id,
        Term = 1,
        Type = RaftLogType.Proposed,
        LogType = "test",
        LogData = [1],
    };

    private static RaftWriteAhead CreateWriteAhead(out RaftManager manager, out RaftPartition partition)
    {
        const int partitionId = 1;

        RaftConfiguration config = new()
        {
            Host = "localhost",
            Port = 9000,
            InitialPartitions = 0,
        };

        InMemoryWAL wal = new(NullLogger<IRaft>.Instance);

        manager = new(
            config,
            new StaticDiscovery([]),
            wal,
            new InMemoryCommunication(),
            new HybridLogicalClock(),
            NullLogger<IRaft>.Instance);

        ((FairReadScheduler)manager.ReadScheduler).Start();
        ((FairWalScheduler)manager.WalScheduler).Start();

        partition = new(
            manager,
            wal,
            partitionId,
            startRange: 0,
            endRange: 0,
            NullLogger<IRaft>.Instance);

        return new RaftWriteAhead(manager, _ => { }, partition, wal);
    }
}
