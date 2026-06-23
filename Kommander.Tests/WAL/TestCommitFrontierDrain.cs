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
