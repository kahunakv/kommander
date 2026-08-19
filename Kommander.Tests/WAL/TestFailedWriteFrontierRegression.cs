using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Kommander.WAL.IO;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.WAL;

/// <summary>
/// Regression tests for <see cref="RaftWriteAhead.RegressFrontiersAfterFailedWriteAsync"/>.
///
/// <para>The enqueue paths advance the presence/commit frontiers optimistically when the WAL
/// scheduler accepts an operation. A write that later completes with a failure (e.g. a full
/// disk) leaves the frontiers certifying entries that are not on disk: the node then advertises
/// election freshness for entries it does not hold and acks heartbeats with a commit frontier
/// the leader trusts to skip backfill. The regression must clamp both frontiers below the failed
/// range, drop the failed ids from the over-gap buffers so they can never re-certify the range,
/// and re-derive the advertised term from a durable entry — while re-delivery of the same ids
/// (the backfill self-heal) must re-advance the frontiers normally.</para>
/// </summary>
public sealed class TestFailedWriteFrontierRegression
{
    /// <summary>
    /// The core regression: after the optimistic advance over a batch whose write failed, both
    /// frontiers return below the failed range, and re-delivery re-advances them (backfill heals).
    /// </summary>
    [Fact]
    public async Task FailedFollowerAppend_RegressesBothFrontiers_AndRedeliveryHeals()
    {
        RaftWriteAhead writeAhead = CreateWriteAhead(out RaftManager manager, out RaftPartition partition);

        try
        {
            Append(writeAhead, Committed(1), Committed(2));
            Assert.Equal(2, writeAhead.GetCommitIndex());
            Assert.Equal(2, writeAhead.GetPresentIndex());

            // The enqueue for id 3 advances both frontiers optimistically...
            Append(writeAhead, Committed(3));
            Assert.Equal(3, writeAhead.GetCommitIndex());
            Assert.Equal(3, writeAhead.GetPresentIndex());

            // ...then its write completes with a failure: the frontiers must stop certifying it.
            await writeAhead.RegressFrontiersAfterFailedWriteAsync(3, 3, regressPresence: true, regressCommit: true);

            Assert.Equal(2, writeAhead.GetCommitIndex());
            Assert.Equal(2, writeAhead.GetPresentIndex());

            // Re-delivery (the leader re-ships after seeing the low ack) re-advances normally.
            Append(writeAhead, Committed(3));
            Assert.Equal(3, writeAhead.GetCommitIndex());
            Assert.Equal(3, writeAhead.GetPresentIndex());
        }
        finally
        {
            partition.Dispose();
            manager.Dispose();
        }
    }

    /// <summary>
    /// The advertised (term, index) freshness pair must describe a real durable entry after the
    /// regression: the term is re-read from the entry now at the frontier, not left describing
    /// the entry that failed to persist.
    /// </summary>
    [Fact]
    public async Task Regression_ReReadsTermAtTheNewFrontier()
    {
        RaftWriteAhead writeAhead = CreateWriteAhead(out RaftManager manager, out RaftPartition partition);

        try
        {
            Append(writeAhead, Committed(1, term: 1), Committed(2, term: 2));
            Assert.Equal(2, writeAhead.GetPresentTerm());

            // A term-3 entry advances the advertised term optimistically...
            Append(writeAhead, Committed(3, term: 3));
            Assert.Equal(3, writeAhead.GetPresentTerm());

            // ...but its write failed: the pair must return to the durable entry at id 2.
            await writeAhead.RegressFrontiersAfterFailedWriteAsync(3, 3, regressPresence: true, regressCommit: true);

            Assert.Equal(2, writeAhead.GetPresentIndex());
            Assert.Equal(2, writeAhead.GetPresentTerm());
        }
        finally
        {
            partition.Dispose();
            manager.Dispose();
        }
    }

    /// <summary>
    /// Failed ids buffered above a hole must be dropped by the regression: when the hole later
    /// fills, the frontier must advance only over what was actually re-delivered, never draining
    /// a buffered id whose write failed.
    /// </summary>
    [Fact]
    public async Task Regression_DropsFailedIdsFromTheOverGapBuffers()
    {
        RaftWriteAhead writeAhead = CreateWriteAhead(out RaftManager manager, out RaftPartition partition);

        try
        {
            Append(writeAhead, Committed(1));

            // Ids 3 and 4 arrive above the hole at 2 and are buffered — then their write fails.
            Append(writeAhead, Committed(3), Committed(4));
            Assert.Equal(1, writeAhead.GetCommitIndex());

            await writeAhead.RegressFrontiersAfterFailedWriteAsync(3, 4, regressPresence: true, regressCommit: true);

            // Filling the hole must advance ONLY to 2 — proof the failed 3,4 did not drain.
            Append(writeAhead, Committed(2));
            Assert.Equal(2, writeAhead.GetCommitIndex());
            Assert.Equal(2, writeAhead.GetPresentIndex());

            // Re-delivered, they advance normally.
            Append(writeAhead, Committed(3), Committed(4));
            Assert.Equal(4, writeAhead.GetCommitIndex());
            Assert.Equal(4, writeAhead.GetPresentIndex());
        }
        finally
        {
            partition.Dispose();
            manager.Dispose();
        }
    }

    /// <summary>
    /// A failed leader propose regresses only the presence frontier: the commit frontier tracks
    /// durable resolutions, which a propose batch never carries.
    /// </summary>
    [Fact]
    public async Task PresenceOnlyRegression_LeavesCommitFrontierUntouched()
    {
        RaftWriteAhead writeAhead = CreateWriteAhead(out RaftManager manager, out RaftPartition partition);

        try
        {
            Append(writeAhead, Committed(1), Committed(2), Committed(3));
            Assert.Equal(3, writeAhead.GetCommitIndex());
            Assert.Equal(3, writeAhead.GetPresentIndex());

            await writeAhead.RegressFrontiersAfterFailedWriteAsync(3, 3, regressPresence: true, regressCommit: false);

            Assert.Equal(3, writeAhead.GetCommitIndex());
            Assert.Equal(2, writeAhead.GetPresentIndex());
        }
        finally
        {
            partition.Dispose();
            manager.Dispose();
        }
    }

    /// <summary>
    /// Ranges the frontiers never covered are no-ops, as is an invalid (negative) range —
    /// the regression must never move a frontier forward or throw on the failure path.
    /// </summary>
    [Fact]
    public async Task RangesAboveTheFrontierOrInvalid_AreNoOps()
    {
        RaftWriteAhead writeAhead = CreateWriteAhead(out RaftManager manager, out RaftPartition partition);

        try
        {
            Append(writeAhead, Committed(1), Committed(2));

            await writeAhead.RegressFrontiersAfterFailedWriteAsync(10, 12, regressPresence: true, regressCommit: true);
            await writeAhead.RegressFrontiersAfterFailedWriteAsync(-1, -1, regressPresence: true, regressCommit: true);

            Assert.Equal(2, writeAhead.GetCommitIndex());
            Assert.Equal(2, writeAhead.GetPresentIndex());
        }
        finally
        {
            partition.Dispose();
            manager.Dispose();
        }
    }

    private static void Append(RaftWriteAhead writeAhead, params RaftLog[] logs) =>
        writeAhead.EnqueueProposeOrCommit(logs.ToList());

    private static RaftLog Committed(long id, long term = 1) => new()
    {
        Id = id,
        Term = term,
        Type = RaftLogType.Committed,
        LogType = "frontier-regression-test",
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
