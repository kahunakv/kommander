using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Kommander.WAL.IO;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.WAL;

/// <summary>
/// Regression tests for the presence frontier's term-inversion guard and for the
/// truncating-snapshot frontier seed (nightly DST seed 745773478048735981).
///
/// <para>The failure shape: a deposed leader's unanchored broadcast writes an entry over a gap
/// (buffered by the presence frontier), a newer-term chain later fills the gap below it, and the
/// drain then absorbed the stale entry with no term check. The log read "contiguous" through a
/// row no valid Raft log contains — terms never decrease within one log — so the next promotion's
/// hole gate passed and the inherited-tail drain committed the orphan: committed index 5 carried
/// term 1 above index 4 at term 6.</para>
///
/// <para>Drives the real follower-append path (<see cref="RaftWriteAhead.EnqueueProposeOrCommit"/>)
/// and asserts on the public frontier reads, exactly as <see cref="TestCommitFrontierDrain"/> does
/// for the commit frontier.</para>
/// </summary>
public sealed class TestPresenceTermInversion
{
    /// <summary>
    /// THE regression: an entry buffered over a gap at an old term must not be absorbed after a
    /// newer-term chain fills the gap below it. The frontier must stop at the newest genuine
    /// entry, so the orphan can never pass the promotion hole gate as part of the contiguous log.
    /// </summary>
    [Fact]
    public void StaleBufferedEntry_BelowFrontierTerm_IsNotAbsorbed()
    {
        RaftWriteAhead writeAhead = CreateWriteAhead(out RaftManager manager, out RaftPartition partition);

        try
        {
            Append(writeAhead, Committed(1, term: 1));

            // A deposed term-1 leader lands entry 5 over the gap at 2..4; it is buffered.
            Append(writeAhead, Proposed(5, term: 1));
            Assert.Equal(1, writeAhead.GetPresentIndex());

            // The term-6 chain fills the gap. The drain reaches the buffered 5:t1 and must
            // refuse it: no valid log holds term 1 above term 6.
            Append(writeAhead, Committed(2, term: 6));
            Append(writeAhead, Committed(3, term: 6));
            Append(writeAhead, Committed(4, term: 6));

            Assert.Equal(4, writeAhead.GetPresentIndex());
            Assert.Equal(6, writeAhead.GetPresentTerm());
        }
        finally
        {
            partition.Dispose();
            manager.Dispose();
        }
    }

    /// <summary>
    /// The fast path has the same hole: an in-order arrival whose term is below the frontier term
    /// must be refused too, not only the buffered drain case.
    /// </summary>
    [Fact]
    public void InOrderArrival_BelowFrontierTerm_IsNotAbsorbed()
    {
        RaftWriteAhead writeAhead = CreateWriteAhead(out RaftManager manager, out RaftPartition partition);

        try
        {
            Append(writeAhead, Committed(1, term: 1));
            Append(writeAhead, Committed(2, term: 6));
            Assert.Equal(2, writeAhead.GetPresentIndex());

            Append(writeAhead, Proposed(3, term: 1));
            Assert.Equal(2, writeAhead.GetPresentIndex());
            Assert.Equal(6, writeAhead.GetPresentTerm());
        }
        finally
        {
            partition.Dispose();
            manager.Dispose();
        }
    }

    /// <summary>
    /// After a refusal, the current leader re-supplies the same index from its own chain. The
    /// legitimate entry (term at or above the frontier term) must absorb normally — the guard is
    /// about inverted terms, not about the index.
    /// </summary>
    [Fact]
    public void RefusedIndex_AbsorbsLaterLegitimateEntry()
    {
        RaftWriteAhead writeAhead = CreateWriteAhead(out RaftManager manager, out RaftPartition partition);

        try
        {
            Append(writeAhead, Committed(1, term: 1));
            Append(writeAhead, Proposed(5, term: 1));
            Append(writeAhead, Committed(2, term: 6));
            Append(writeAhead, Committed(3, term: 6));
            Append(writeAhead, Committed(4, term: 6));
            Assert.Equal(4, writeAhead.GetPresentIndex());

            // The term-8 leader writes its own entry at the refused index.
            Append(writeAhead, Committed(5, term: 8));
            Assert.Equal(5, writeAhead.GetPresentIndex());
            Assert.Equal(8, writeAhead.GetPresentTerm());
        }
        finally
        {
            partition.Dispose();
            manager.Dispose();
        }
    }

    /// <summary>
    /// No false positive: an out-of-order delivery within ONE term is the broadcast's documented
    /// shape and must keep draining exactly as before.
    /// </summary>
    [Fact]
    public void SameTermOutOfOrder_StillDrains()
    {
        RaftWriteAhead writeAhead = CreateWriteAhead(out RaftManager manager, out RaftPartition partition);

        try
        {
            Append(writeAhead, Committed(1, term: 1));
            Append(writeAhead, Committed(3, term: 1));
            Assert.Equal(1, writeAhead.GetPresentIndex());

            Append(writeAhead, Committed(2, term: 1));
            Assert.Equal(3, writeAhead.GetPresentIndex());
        }
        finally
        {
            partition.Dispose();
            manager.Dispose();
        }
    }

    /// <summary>
    /// A truncating snapshot install deletes every row above the boundary. The frontier seed must
    /// purge resolutions buffered above the boundary BEFORE it drains: without the purge, the seed
    /// absorbed a deposed leader's optimistically-resolved tail and certified a commit frontier
    /// over rows the truncation had just deleted.
    /// </summary>
    [Fact]
    public void TruncatingSnapshotSeed_DropsStaleBufferedResolutions()
    {
        RaftWriteAhead writeAhead = CreateWriteAhead(out RaftManager manager, out RaftPartition partition);

        try
        {
            Append(writeAhead, Committed(1, term: 1));

            // Old-term resolutions land over the gap at 2..3 and are buffered.
            Append(writeAhead, Committed(4, term: 1));
            Append(writeAhead, Committed(5, term: 1));
            Assert.Equal(1, writeAhead.GetCommitIndex());

            // A conflicting snapshot at (3, t6) truncated the suffix; the seed must not let the
            // stale buffered 4,5 drain the frontier past the boundary.
            writeAhead.SeedCommitFrontierFromSnapshot(3, 6, suffixTruncated: true);

            Assert.Equal(3, writeAhead.GetCommitIndex());
            Assert.Equal(3, writeAhead.GetPresentIndex());
            Assert.Equal(6, writeAhead.GetPresentTerm());
        }
        finally
        {
            partition.Dispose();
            manager.Dispose();
        }
    }

    /// <summary>
    /// The retaining install keeps the suffix, so resolutions buffered above the boundary remain
    /// valid and must still drain — the purge applies only to the truncating case.
    /// </summary>
    [Fact]
    public void RetainingSnapshotSeed_StillDrainsBufferedResolutions()
    {
        RaftWriteAhead writeAhead = CreateWriteAhead(out RaftManager manager, out RaftPartition partition);

        try
        {
            Append(writeAhead, Committed(1, term: 6));
            Append(writeAhead, Committed(4, term: 6));
            Append(writeAhead, Committed(5, term: 6));
            Assert.Equal(1, writeAhead.GetCommitIndex());

            writeAhead.SeedCommitFrontierFromSnapshot(3, 6);

            Assert.Equal(5, writeAhead.GetCommitIndex());
        }
        finally
        {
            partition.Dispose();
            manager.Dispose();
        }
    }

    private static void Append(RaftWriteAhead writeAhead, params RaftLog[] logs) =>
        writeAhead.EnqueueProposeOrCommit(logs.ToList());

    private static RaftLog Committed(long id, long term) => new()
    {
        Id = id,
        Term = term,
        Type = RaftLogType.Committed,
        LogType = "inversion-test",
        LogData = [1, 2, 3],
    };

    private static RaftLog Proposed(long id, long term) => new()
    {
        Id = id,
        Term = term,
        Type = RaftLogType.Proposed,
        LogType = "inversion-test",
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
