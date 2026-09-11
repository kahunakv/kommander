using System.Collections.Concurrent;
using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Kommander.WAL.Data;
using Kommander.WAL.IO;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.WAL;

/// <summary>
/// Regression tests for the published commit index (<see cref="RaftWriteAhead.GetDurableCommitIndex"/>).
///
/// <para>The nightly random search (GA run 34577216505, seed 5040446807928117419, every night from
/// 2026-09-06) caught a follower with a starved disk reporting commit index 1 and then 0: the
/// enqueue path had advanced the protocol frontier over an entry the disk then refused, and the
/// failed-write repair lowered it again. Both moves are right for the replication protocol — the
/// leader must see the low value to re-ship — and wrong for an observer, to whom "committed
/// through 1, then through 0" is an acknowledged entry un-committed. The published index must cover
/// an id only once it is both resolved and durably held, and it must never move backwards in one
/// process lifetime.</para>
/// </summary>
public sealed class TestDurableCommitFrontier
{
    /// <summary>
    /// The core case from the finding: a resolved entry the disk never took is never published,
    /// and the failed-write repair that lowers the protocol frontier leaves the published one alone.
    /// </summary>
    [Fact]
    public async Task EntryTheDiskRefused_IsNeverPublished_AndTheRepairDoesNotRegressIt()
    {
        RaftWriteAhead writeAhead = CreateWriteAhead(out RaftManager manager, out RaftPartition partition);

        try
        {
            // Entry 1 accepted: the protocol frontier certifies it at enqueue time, the published
            // one waits for the durable completion.
            RaftWalCompletion first = await Append(writeAhead, Committed(1));
            Assert.Equal(1, writeAhead.GetCommitIndex());
            Assert.Equal(0, writeAhead.GetDurableCommitIndex());

            Durable(writeAhead, first);
            Assert.Equal(1, writeAhead.GetDurableCommitIndex());

            // Entry 2 accepted, then its write fails: the protocol frontier goes 2 -> 1, the
            // published index never reached 2 and stays at 1.
            await Append(writeAhead, Committed(2));
            Assert.Equal(2, writeAhead.GetCommitIndex());
            Assert.Equal(1, writeAhead.GetDurableCommitIndex());

            await writeAhead.RegressFrontiersAfterFailedWriteAsync(2, 2, regressPresence: true, regressCommit: true);
            Assert.Equal(1, writeAhead.GetCommitIndex());
            Assert.Equal(1, writeAhead.GetDurableCommitIndex());

            // The leader re-ships entry 2 and this time it lands.
            RaftWalCompletion retry = await Append(writeAhead, Committed(2));
            Durable(writeAhead, retry);
            Assert.Equal(2, writeAhead.GetCommitIndex());
            Assert.Equal(2, writeAhead.GetDurableCommitIndex());
        }
        finally
        {
            partition.Dispose();
            manager.Dispose();
        }
    }

    /// <summary>
    /// The repair is deliberately conservative — it clamps the protocol frontier to the LOWEST id
    /// of the failed range, below entries that are durable. A failed commit-marker write over
    /// present rows therefore lowers the protocol frontier under ids the node holds and knows
    /// committed. The published index keeps them: the knowledge came from the leader, the rows are
    /// on disk, and the missing marker only means the leader re-ships it.
    /// </summary>
    [Fact]
    public async Task FailedMarkerBelowDurableEntries_LowersTheProtocolFrontierOnly()
    {
        RaftWriteAhead writeAhead = CreateWriteAhead(out RaftManager manager, out RaftPartition partition);

        try
        {
            RaftWalCompletion batch = await Append(writeAhead, Committed(1), Committed(2), Committed(3));
            Durable(writeAhead, batch);
            Assert.Equal(3, writeAhead.GetDurableCommitIndex());

            await writeAhead.RegressFrontiersAfterFailedWriteAsync(2, 3, regressPresence: false, regressCommit: true);

            Assert.Equal(1, writeAhead.GetCommitIndex());
            Assert.Equal(3, writeAhead.GetDurableCommitIndex());
        }
        finally
        {
            partition.Dispose();
            manager.Dispose();
        }
    }

    /// <summary>
    /// A batch with a hole in its own span certifies only the ids it carried: the skipped id's own
    /// write may be the one that failed, and the published index must stop below it until that id
    /// lands on its own.
    /// </summary>
    [Fact]
    public async Task SparseBatch_DoesNotCertifyTheIdItSkipped()
    {
        RaftWriteAhead writeAhead = CreateWriteAhead(out RaftManager manager, out RaftPartition partition);

        try
        {
            await Append(writeAhead, Committed(1), Committed(2), Committed(3));
            Assert.Equal(3, writeAhead.GetCommitIndex());

            writeAhead.MarkDurablyWritten(1, 3, sparseLogIds: [1, 3]);
            Assert.Equal(1, writeAhead.GetDurablePresentIndex());
            Assert.Equal(1, writeAhead.GetDurableCommitIndex());

            writeAhead.MarkDurablyWritten(2, 2, sparseLogIds: null);
            Assert.Equal(3, writeAhead.GetDurablePresentIndex());
            Assert.Equal(3, writeAhead.GetDurableCommitIndex());
        }
        finally
        {
            partition.Dispose();
            manager.Dispose();
        }
    }

    /// <summary>
    /// Durability alone publishes nothing: a durable entry the node does not yet know resolved
    /// (a Proposed row) stays below the published index until its resolution arrives.
    /// </summary>
    [Fact]
    public async Task DurableButUnresolvedEntry_IsNotPublished()
    {
        RaftWriteAhead writeAhead = CreateWriteAhead(out RaftManager manager, out RaftPartition partition);

        try
        {
            RaftWalCompletion first = await Append(writeAhead, Committed(1));
            Durable(writeAhead, first);

            RaftWalCompletion proposed = await Append(writeAhead, Proposed(2));
            Durable(writeAhead, proposed);

            Assert.Equal(2, writeAhead.GetDurablePresentIndex());
            Assert.Equal(1, writeAhead.GetCommitIndex());
            Assert.Equal(1, writeAhead.GetDurableCommitIndex());

            RaftWalCompletion marker = await Append(writeAhead, Committed(2));
            Durable(writeAhead, marker);

            Assert.Equal(2, writeAhead.GetDurableCommitIndex());
        }
        finally
        {
            partition.Dispose();
            manager.Dispose();
        }
    }

    /// <summary>
    /// A restore re-derives both frontiers from the disk, so a node that restarts publishes what it
    /// holds without waiting for a live write to certify it.
    /// </summary>
    [Fact]
    public async Task Restore_SeedsThePublishedIndexFromTheDisk()
    {
        RaftWriteAhead writeAhead = CreateWriteAhead(out RaftManager manager, out RaftPartition partition);

        try
        {
            await Append(writeAhead, Committed(1), Committed(2), Committed(3));
            Assert.Equal(0, writeAhead.GetDurableCommitIndex());

            await writeAhead.CompleteRestoreAsync(await writeAhead.LoadRestoreLogsAsync());

            Assert.Equal(3, writeAhead.GetCommitIndex());
            Assert.Equal(3, writeAhead.GetDurablePresentIndex());
            Assert.Equal(3, writeAhead.GetDurableCommitIndex());
        }
        finally
        {
            partition.Dispose();
            manager.Dispose();
        }
    }

    /// <summary>
    /// A truncation lowers the durable presence frontier with the disk, and a later re-append of
    /// the same ids has to land again before it is published. The published index itself is not
    /// lowered: a truncation removes an uncommitted suffix only.
    /// </summary>
    [Fact]
    public async Task Truncation_LowersDurablePresence_NotThePublishedIndex()
    {
        RaftWriteAhead writeAhead = CreateWriteAhead(out RaftManager manager, out RaftPartition partition);

        try
        {
            RaftWalCompletion committed = await Append(writeAhead, Committed(1), Committed(2));
            Durable(writeAhead, committed);
            RaftWalCompletion proposed = await Append(writeAhead, Proposed(3), Proposed(4));
            Durable(writeAhead, proposed);

            Assert.Equal(4, writeAhead.GetDurablePresentIndex());
            Assert.Equal(2, writeAhead.GetDurableCommitIndex());

            await writeAhead.TruncateLogsAfterAsync(2);

            Assert.Equal(2, writeAhead.GetDurablePresentIndex());
            Assert.Equal(2, writeAhead.GetDurableCommitIndex());
        }
        finally
        {
            partition.Dispose();
            manager.Dispose();
        }
    }

    /// <summary>
    /// The scheduler reports what a write carried, not the operation's index field: the propose
    /// path stores the allocator's next id there, and a sparse batch must be named id by id.
    /// </summary>
    [Fact]
    public async Task Completion_CarriesTheWrittenIds()
    {
        RaftWriteAhead writeAhead = CreateWriteAhead(out RaftManager manager, out RaftPartition partition);

        try
        {
            RaftWalCompletion contiguous = await Append(writeAhead, Committed(1), Committed(2), Committed(3));
            Assert.Equal(1, contiguous.MinLogIndex);
            Assert.Equal(3, contiguous.WrittenMaxLogIndex);
            Assert.Null(contiguous.SparseLogIds);

            RaftWalCompletion sparse = await Append(writeAhead, Committed(5), Committed(7));
            Assert.Equal(5, sparse.MinLogIndex);
            Assert.Equal(7, sparse.WrittenMaxLogIndex);
            Assert.NotNull(sparse.SparseLogIds);
            Assert.Equal([5L, 7L], sparse.SparseLogIds);

            // Unordered but complete: the span is exact, so no id list is needed.
            RaftWalCompletion unordered = await Append(writeAhead, Committed(9), Committed(8));
            Assert.Equal(8, unordered.MinLogIndex);
            Assert.Equal(9, unordered.WrittenMaxLogIndex);
            Assert.Null(unordered.SparseLogIds);

            // A leader propose: MaxLogIndex is the allocator's NEXT id, WrittenMaxLogIndex the last written one.
            WALWriteOperation propose = writeAhead.EnqueuePropose(1, [Proposed(0), Proposed(0)], HLCTimestamp.Zero, autoCommit: false);
            RaftWalCompletion proposeCompletion = await CompletionOf(propose);
            Assert.Equal(RaftOperationStatus.Success, proposeCompletion.Status);
            Assert.Equal(proposeCompletion.WrittenMaxLogIndex + 1, proposeCompletion.MaxLogIndex);
            Assert.Equal(proposeCompletion.MinLogIndex + 1, proposeCompletion.WrittenMaxLogIndex);
            Assert.Null(proposeCompletion.SparseLogIds);
        }
        finally
        {
            partition.Dispose();
            manager.Dispose();
        }
    }

    // ── Helpers ────────────────────────────────────────────────────────────

    private readonly ConcurrentDictionary<long, TaskCompletionSource<RaftWalCompletion>> completions = new();

    private void OnWalComplete(RaftWalCompletion completion) =>
        completions.GetOrAdd(
                completion.OperationId,
                static _ => new TaskCompletionSource<RaftWalCompletion>(TaskCreationOptions.RunContinuationsAsynchronously))
            .TrySetResult(completion);

    private Task<RaftWalCompletion> CompletionOf(WALWriteOperation operation) =>
        completions.GetOrAdd(
                operation.OperationId,
                static _ => new TaskCompletionSource<RaftWalCompletion>(TaskCreationOptions.RunContinuationsAsynchronously))
            .Task.WaitAsync(TimeSpan.FromSeconds(30));

    /// <summary>
    /// Enqueues the batch and waits for its WAL completion, returning it. The completion is NOT
    /// applied to the durable frontier here: in production that is the completion router's job,
    /// and a test that wants the published index to move calls <see cref="Durable"/> explicitly, so
    /// the window between "accepted" and "durable" is observable.
    /// </summary>
    private async Task<RaftWalCompletion> Append(RaftWriteAhead writeAhead, params RaftLog[] logs)
    {
        WALWriteOperation? operation = writeAhead.EnqueueProposeOrCommit(logs.ToList());

        Assert.NotNull(operation);

        RaftWalCompletion completion = await CompletionOf(operation);

        Assert.Equal(RaftOperationStatus.Success, completion.Status);

        return completion;
    }

    /// <summary>What the completion router does for a successful completion.</summary>
    private static void Durable(RaftWriteAhead writeAhead, RaftWalCompletion completion) =>
        writeAhead.MarkDurablyWritten(completion.MinLogIndex, completion.WrittenMaxLogIndex, completion.SparseLogIds);

    private static RaftLog Committed(long id, long term = 1) => new()
    {
        Id = id,
        Term = term,
        Type = RaftLogType.Committed,
        LogType = "durable-frontier-test",
        LogData = [1, 2, 3],
    };

    private static RaftLog Proposed(long id, long term = 1) => new()
    {
        Id = id,
        Term = term,
        Type = RaftLogType.Proposed,
        LogType = "durable-frontier-test",
        LogData = [1, 2, 3],
    };

    private RaftWriteAhead CreateWriteAhead(out RaftManager manager, out RaftPartition partition)
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

        return new RaftWriteAhead(manager, OnWalComplete, partition, wal);
    }
}
