using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Kommander.WAL.IO;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.WAL;

/// <summary>
/// Regression tests for the follower commit frontier (<see cref="RaftWriteAhead.GetCommitIndex"/>).
///
/// <para>The unanchored live-propose broadcast delivers committed entries to a follower out of
/// order under load. The frontier must only ever advance over a contiguous prefix: an out-of-order
/// committed entry that sits <b>above</b> an unfilled gap must be buffered, not applied, so the
/// frontier neither overshoots the hole (which applies entries before their predecessors) nor
/// freezes at the first reordered entry. When the gap fills, the buffered successors drain forward
/// in one step.</para>
///
/// <para>Drives the real follower-append path (<see cref="RaftWriteAhead.EnqueueProposeOrCommit"/>),
/// which advances the frontier synchronously after the WAL enqueue, then asserts on the public
/// <see cref="RaftWriteAhead.GetCommitIndex"/>.</para>
/// </summary>
public sealed class TestCommitFrontierDrain
{
    /// <summary>Baseline: in-order committed entries advance the frontier one slot at a time.</summary>
    [Fact]
    public void ContiguousCommits_AdvanceFrontier()
    {
        RaftWriteAhead writeAhead = CreateWriteAhead(out RaftManager manager, out RaftPartition partition);

        try
        {
            // No commits yet: commitIndex starts at 1, so GetCommitIndex() == 0.
            Assert.Equal(0, writeAhead.GetCommitIndex());

            Append(writeAhead, Committed(1));
            Assert.Equal(1, writeAhead.GetCommitIndex());

            Append(writeAhead, Committed(2));
            Assert.Equal(2, writeAhead.GetCommitIndex());

            Append(writeAhead, Committed(3));
            Assert.Equal(3, writeAhead.GetCommitIndex());
        }
        finally
        {
            partition.Dispose();
            manager.Dispose();
        }
    }

    /// <summary>
    /// THE regression: a committed entry above an unfilled hole must NOT advance the frontier.
    /// With the contiguous prefix ending at id 1 and a hole at id 2, receiving committed ids 3
    /// and 4 must leave the frontier at 1 (overshoot would report 4 and apply 3,4 before 2).
    /// </summary>
    [Fact]
    public void OutOfOrderCommitAboveHole_DoesNotAdvancePastGap()
    {
        RaftWriteAhead writeAhead = CreateWriteAhead(out RaftManager manager, out RaftPartition partition);

        try
        {
            Append(writeAhead, Committed(1));
            Assert.Equal(1, writeAhead.GetCommitIndex());

            // Hole at id 2; these arrive early and must be buffered, not applied.
            Append(writeAhead, Committed(3));
            Append(writeAhead, Committed(4));

            Assert.Equal(1, writeAhead.GetCommitIndex());
        }
        finally
        {
            partition.Dispose();
            manager.Dispose();
        }
    }

    /// <summary>
    /// Filling the hole drains every buffered successor in one step: after the frontier sits at 1
    /// with 3,4 buffered, receiving id 2 advances the frontier straight to 4.
    /// </summary>
    [Fact]
    public void FillingHole_DrainsBufferedSuccessors()
    {
        RaftWriteAhead writeAhead = CreateWriteAhead(out RaftManager manager, out RaftPartition partition);

        try
        {
            Append(writeAhead, Committed(1));
            Append(writeAhead, Committed(3));
            Append(writeAhead, Committed(4));
            Assert.Equal(1, writeAhead.GetCommitIndex());

            // The hole fills: 2 advances the frontier and 3,4 become contiguous and drain.
            Append(writeAhead, Committed(2));
            Assert.Equal(4, writeAhead.GetCommitIndex());
        }
        finally
        {
            partition.Dispose();
            manager.Dispose();
        }
    }

    /// <summary>
    /// The reordering can occur within a single append batch. Logs are sorted by id before
    /// processing, so [1,3,4] applies 1 (contiguous) and buffers 3,4 above the hole at 2; a later
    /// batch carrying 2 drains them.
    /// </summary>
    [Fact]
    public void OutOfOrderWithinSingleBatch_BuffersUntilContiguous()
    {
        RaftWriteAhead writeAhead = CreateWriteAhead(out RaftManager manager, out RaftPartition partition);

        try
        {
            Append(writeAhead, Committed(1), Committed(3), Committed(4));
            Assert.Equal(1, writeAhead.GetCommitIndex());

            Append(writeAhead, Committed(2));
            Assert.Equal(4, writeAhead.GetCommitIndex());
        }
        finally
        {
            partition.Dispose();
            manager.Dispose();
        }
    }

    /// <summary>
    /// A duplicate replay of an already-committed id (below the frontier) is ignored and must not
    /// disturb the frontier.
    /// </summary>
    [Fact]
    public void DuplicateBelowFrontier_IsIgnored()
    {
        RaftWriteAhead writeAhead = CreateWriteAhead(out RaftManager manager, out RaftPartition partition);

        try
        {
            Append(writeAhead, Committed(1), Committed(2), Committed(3));
            Assert.Equal(3, writeAhead.GetCommitIndex());

            // Re-deliver an entry already covered by the frontier.
            Append(writeAhead, Committed(1));
            Assert.Equal(3, writeAhead.GetCommitIndex());
        }
        finally
        {
            partition.Dispose();
            manager.Dispose();
        }
    }

    /// <summary>
    /// Truncating the log tail (the hole-repair path) drops buffered successors that now point at
    /// absent entries. After truncating above the frontier, filling the hole advances only one slot;
    /// the previously-buffered ids must be re-delivered, not resurrected from the stale buffer.
    /// </summary>
    [Fact]
    public async Task TruncateAfterHole_DropsBufferedSuccessors()
    {
        RaftWriteAhead writeAhead = CreateWriteAhead(out RaftManager manager, out RaftPartition partition);

        try
        {
            Append(writeAhead, Committed(1));
            Append(writeAhead, Committed(3));
            Append(writeAhead, Committed(4));
            Assert.Equal(1, writeAhead.GetCommitIndex());

            // Repair truncates everything above the contiguous prefix; buffered 3,4 must be discarded.
            await writeAhead.TruncateLogsAfterAsync(1).ConfigureAwait(true);

            // Filling the hole now advances ONLY to 2 — proof the stale 3,4 did not drain.
            Append(writeAhead, Committed(2));
            Assert.Equal(2, writeAhead.GetCommitIndex());

            // The re-delivered successors advance the frontier normally.
            Append(writeAhead, Committed(3));
            Append(writeAhead, Committed(4));
            Assert.Equal(4, writeAhead.GetCommitIndex());
        }
        finally
        {
            partition.Dispose();
            manager.Dispose();
        }
    }

    /// <summary>
    /// The frontier a follower reports as DURABLE trails the protocol frontier for exactly as long as
    /// the storage engine has not answered: accepting entries 1..3 into the queue advances
    /// <see cref="RaftWriteAhead.GetCommitIndex"/> to 3 at once, while
    /// <see cref="RaftWriteAhead.GetDurableCommitFrontier"/> stays at 0 until the completions land,
    /// and follows them contiguously. A leader holding WAL retention on the protocol frontier of a
    /// follower whose disk had stalled compacted the very range the follower later needed (CamusDB
    /// slow-disk run sd8); the durable frontier is what it must hold on instead.
    /// </summary>
    [Fact]
    public void DurableCommitFrontier_TrailsTheProtocolFrontier_UntilTheEngineAnswers()
    {
        RaftWriteAhead writeAhead = CreateWriteAhead(out RaftManager manager, out RaftPartition partition);

        try
        {
            Append(writeAhead, Committed(1), Committed(2), Committed(3));
            Assert.Equal(3, writeAhead.GetCommitIndex());
            Assert.Equal(0, writeAhead.GetDurableCommitFrontier());

            // The engine answers for 1..2 only: the durable frontier follows, still below the protocol one.
            // The rows arrived typed Committed in a synced batch, so their resolution is on disk too.
            writeAhead.MarkDurablyWritten(1, 2, null);
            writeAhead.MarkResolutionWritten(2, synced: true);
            Assert.Equal(2, writeAhead.GetDurableCommitFrontier());
            Assert.Equal(3, writeAhead.GetCommitIndex());

            // An answer above a hole certifies nothing below the hole.
            writeAhead.MarkDurablyWritten(5, 5, null);
            writeAhead.MarkResolutionWritten(5, synced: true);
            Assert.Equal(2, writeAhead.GetDurableCommitFrontier());

            writeAhead.MarkDurablyWritten(3, 3, null);
            writeAhead.MarkResolutionWritten(3, synced: true);
            Assert.Equal(3, writeAhead.GetDurableCommitFrontier());
        }
        finally
        {
            partition.Dispose();
            manager.Dispose();
        }
    }

    /// <summary>
    /// A resolution that rode sync-off does not count as durable until a later synced write on the
    /// partition completes (DST FINDING 7).
    ///
    /// <para>The single-fsync fast path writes a <c>Committed</c> marker over a present
    /// <c>Proposed</c> row without an fsync. A crash before the next synced write returns the row to
    /// <c>Proposed</c>, so the node restarts with a commit frontier below the id. A follower that
    /// reported the id as durable let the leader compact it, and the leader's backfill anchored on
    /// the compacted id was then refused forever. The reported frontier must therefore wait for the
    /// marker to be on disk, while the row's presence alone is not enough.</para>
    /// </summary>
    [Fact]
    public void DurableCommitFrontier_DoesNotCountAResolutionThatRodeSyncOff()
    {
        RaftWriteAhead writeAhead = CreateWriteAhead(out RaftManager manager, out RaftPartition partition);

        try
        {
            // Entries 1..3 are present on disk, and this node knows they are committed.
            Append(writeAhead, Committed(1), Committed(2), Committed(3));
            writeAhead.MarkDurablyWritten(1, 3, null);
            writeAhead.MarkResolutionWritten(2, synced: true);
            Assert.Equal(2, writeAhead.GetDurableCommitFrontier());

            // The marker for 3 rides sync-off: presence and memory both say 3, the disk does not.
            writeAhead.MarkResolutionWritten(3, synced: false);
            Assert.Equal(3, writeAhead.GetDurablePresentIndex());
            Assert.Equal(2, writeAhead.GetDurableResolvedIndex());
            Assert.Equal(2, writeAhead.GetDurableCommitFrontier());

            // Any later synced write on the partition carries the marker to disk, even one that
            // resolves nothing itself (a propose).
            writeAhead.MarkResolutionWritten(-1, synced: true);
            Assert.Equal(3, writeAhead.GetDurableResolvedIndex());
            Assert.Equal(3, writeAhead.GetDurableCommitFrontier());
        }
        finally
        {
            partition.Dispose();
            manager.Dispose();
        }
    }

    private static void Append(RaftWriteAhead writeAhead, params RaftLog[] logs) =>
        writeAhead.EnqueueProposeOrCommit(logs.ToList());

    private static RaftLog Committed(long id) => new()
    {
        Id = id,
        Term = 1,
        Type = RaftLogType.Committed,
        LogType = "frontier-test",
        LogData = [1, 2, 3],
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
