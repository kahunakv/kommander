using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Kommander.WAL.IO;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.WAL;

/// <summary>
/// A locally RESOLVED id must never be writable as <c>Proposed</c> again.
///
/// <para>Resolution (commit or rollback) is terminal. The only sender of a Proposed copy for a
/// resolved id is a stale duplicate — a deposed leader's still-in-flight broadcast, or a proposal
/// retry racing its own commit. Before this fix the follower-append plan builder wrote every
/// incoming Proposed row unconditionally, regressing the on-disk row from Committed back to
/// Proposed; the write pipeline's post-append <c>TruncateProposedLogsAfter</c> then silently
/// DELETED the regressed row, while the in-memory commit/presence frontiers (advanced at enqueue,
/// never re-read from rows) sailed on past it. The follower then honestly reported a frontier
/// above a permanently absent row, the leader trusted the self-report (gap = 0, no backfill), and
/// the apply drain blocked on the hole forever — the Jepsen frozen-replica residue of run
/// 31800761220 (n3/p3 row 110 absent below a frontier of 432).</para>
///
/// <para>Drives the REAL pipeline: <see cref="RaftWriteAhead.EnqueueProposeOrCommit"/> →
/// <see cref="FairWalScheduler"/> (including its post-append truncation) → <see cref="InMemoryWAL"/>,
/// then asserts on the rows read back through the read scheduler.</para>
/// </summary>
public sealed class TestStaleProposedRegression
{
    /// <summary>
    /// The full kill chain: commit 1..3, deliver a stale Proposed duplicate of id 2, then complete
    /// a low-max append batch (which triggers the pipeline's proposed-tail truncation at id 1).
    /// Row 2 must still be present and Committed, and the frontier must still be 3 — before the
    /// fix the duplicate regressed row 2 to Proposed and the truncation deleted it, leaving a
    /// permanent hole below the advertised frontier.
    /// </summary>
    [Fact]
    public async Task StaleProposedDuplicate_DoesNotRegressOrLoseAResolvedRow()
    {
        RaftWriteAhead writeAhead = CreateWriteAhead(out RaftManager manager, out RaftPartition partition);

        try
        {
            Append(writeAhead, Committed(1));
            Append(writeAhead, Committed(2));
            Append(writeAhead, Committed(3));
            Assert.Equal(3, writeAhead.GetCommitIndex());

            // Stale duplicate: a deposed leader's in-flight retry of the already-committed id 2.
            Append(writeAhead, Proposed(2));

            // A low-max append batch: its completion runs TruncateProposedLogsAfter(pid, 1),
            // which deletes any row above id 1 that reads Proposed at that moment.
            Append(writeAhead, Committed(1));

            RaftLog? row2 = await ReadRowAsync(writeAhead, 2);
            Assert.NotNull(row2);
            Assert.Equal(RaftLogType.Committed, row2!.Type);
            Assert.Equal(3, writeAhead.GetCommitIndex());
        }
        finally
        {
            partition.Dispose();
            manager.Dispose();
        }
    }

    /// <summary>
    /// Same guard for an id resolved ABOVE the contiguous frontier (buffered in the
    /// resolved-over-gap set): with a hole at 2, id 3 is committed-and-buffered; a stale Proposed
    /// duplicate of 3 must not regress its row either.
    /// </summary>
    [Fact]
    public async Task StaleProposedDuplicate_OfABufferedResolvedId_DoesNotRegressTheRow()
    {
        RaftWriteAhead writeAhead = CreateWriteAhead(out RaftManager manager, out RaftPartition partition);

        try
        {
            Append(writeAhead, Committed(1));
            Append(writeAhead, Committed(3));            // hole at 2: buffered, frontier stays 1
            Assert.Equal(1, writeAhead.GetCommitIndex());

            Append(writeAhead, Proposed(3));             // stale duplicate of the buffered id

            RaftLog? row3 = await ReadRowAsync(writeAhead, 3);
            Assert.NotNull(row3);
            Assert.Equal(RaftLogType.Committed, row3!.Type);

            // Filling the hole must still drain the buffer to 3 — the guard only skips the
            // stale duplicate, never the resolution bookkeeping.
            Append(writeAhead, Committed(2));
            Assert.Equal(3, writeAhead.GetCommitIndex());
        }
        finally
        {
            partition.Dispose();
            manager.Dispose();
        }
    }

    /// <summary>
    /// The same-batch leg: when ONE wire batch carries both a Proposed copy and the Committed
    /// resolution of an id, the resolved row must be what lands — regardless of which action
    /// group happened to be created first in the reused write plan. The first-ever batch here is
    /// commit-only, which (before the deterministic flatten order) pinned the Commit group ahead
    /// of the Propose group for the life of the instance, so a same-batch duplicate was written
    /// LAST and left the row Proposed.
    /// </summary>
    [Fact]
    public async Task ProposeAndCommitOfTheSameId_InOneBatch_LandCommitted()
    {
        RaftWriteAhead writeAhead = CreateWriteAhead(out RaftManager manager, out RaftPartition partition);

        try
        {
            Append(writeAhead, Committed(1));            // commit-only first batch seeds the plan order
            Append(writeAhead, Proposed(2), Committed(2));

            RaftLog? row2 = await ReadRowAsync(writeAhead, 2);
            Assert.NotNull(row2);
            Assert.Equal(RaftLogType.Committed, row2!.Type);
            Assert.Equal(2, writeAhead.GetCommitIndex());
        }
        finally
        {
            partition.Dispose();
            manager.Dispose();
        }
    }

    /// <summary>
    /// Control: a genuinely NEW Proposed entry above the frontier is still written — the guard
    /// must reject only resolved ids, not fresh proposals.
    /// </summary>
    [Fact]
    public async Task FreshProposedEntryAboveTheFrontier_IsStillWritten()
    {
        RaftWriteAhead writeAhead = CreateWriteAhead(out RaftManager manager, out RaftPartition partition);

        try
        {
            Append(writeAhead, Committed(1));
            Append(writeAhead, Proposed(2));

            RaftLog? row2 = await ReadRowAsync(writeAhead, 2);
            Assert.NotNull(row2);
            Assert.Equal(RaftLogType.Proposed, row2!.Type);
        }
        finally
        {
            partition.Dispose();
            manager.Dispose();
        }
    }

    /// <summary>
    /// The propose-id ALLOCATOR must be monotonic: accepting a low-id Proposed row (an unresolved
    /// band from an earlier term, delivered late) must not drag it backwards. Before the fix the
    /// arm did <c>proposeIndex = log.Id + 1</c> unconditionally, so the next client write was
    /// stamped onto an index already durably occupied — committing two different values at the
    /// same index on whichever replicas had a hole there (the Jepsen Log Matching violation of
    /// run 31805148040, p2/211..218).
    /// </summary>
    [Fact]
    public void LowUnresolvedProposedRow_DoesNotRegressTheAllocator()
    {
        RaftWriteAhead writeAhead = CreateWriteAhead(out RaftManager manager, out RaftPartition partition);

        try
        {
            Append(writeAhead, Proposed(5));   // allocator → 6
            Append(writeAhead, Proposed(2));   // unresolved low id: accepted, must NOT regress it

            RaftLog stamped = new() { LogType = "test", LogData = [1] };
            writeAhead.EnqueuePropose(1, [stamped], HLCTimestamp.Zero, autoCommit: true);

            Assert.Equal(6, stamped.Id);
        }
        finally
        {
            partition.Dispose();
            manager.Dispose();
        }
    }

    /// <summary>
    /// The exact last-non-skipped-id-wins shape from the ticket: with the allocator at 9
    /// (Proposed 6 and 8 held), a late unresolved Proposed 7 arrives. Before the fix it dragged
    /// the allocator to 8, and the next client write was stamped 8 — reissuing the id the durable
    /// Proposed row 8 already occupies.
    /// </summary>
    [Fact]
    public void LateUnresolvedRowBelowHeldProposes_DoesNotCauseAnIdReissue()
    {
        RaftWriteAhead writeAhead = CreateWriteAhead(out RaftManager manager, out RaftPartition partition);

        try
        {
            Append(writeAhead, Committed(1), Committed(2), Committed(3), Committed(4), Committed(5));
            Append(writeAhead, Proposed(6));
            Append(writeAhead, Proposed(8));   // allocator → 9 (8 held durably)
            Append(writeAhead, Proposed(7));   // late fill of the gap: must not regress to 8

            RaftLog stamped = new() { LogType = "test", LogData = [1] };
            writeAhead.EnqueuePropose(1, [stamped], HLCTimestamp.Zero, autoCommit: true);

            Assert.Equal(9, stamped.Id);
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
    /// physical write (and the post-append truncation) run on the WAL scheduler's workers, so the
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

            // Wait until the write queue has fully drained (two settled reads in a row would be
            // stronger, but the 10ms cadence over a 1-2 op queue is already generous); keep
            // polling while the row is absent in case its write is still queued.
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
