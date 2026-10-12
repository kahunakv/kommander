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
/// Rollback, frontier and restore edges of <see cref="RaftWriteAhead"/>:
///
/// <list type="bullet">
///   <item>Rollback of a null/empty batch is a successful no-op.</item>
///   <item>A mixed Proposed + ProposedCheckpoint batch is re-typed to its rollback markers and
///         advances the resolved frontier per entry — gap-buffered, so a rollback over a hole
///         buffers until the hole resolves.</item>
///   <item>A rollback the scheduler refuses restores the original entry types and leaves the
///         frontier untouched.</item>
///   <item>The failed-write frontier regression lowers the protocol-facing frontiers (what the
///         leader's re-ship reads) without ever lowering the published commit high-water (what an
///         outside observer reads).</item>
///   <item>Restore completion on a blank WAL reconstructs zero frontiers; a marker-only WAL
///         (nothing but a certified checkpoint) jumps both frontiers to the boundary and replays
///         nothing.</item>
/// </list>
/// </summary>
public sealed class TestRaftWriteAheadRollbackAndRestore
{
    // ── rollback ─────────────────────────────────────────────────────────────

    [Fact]
    public async Task Rollback_NullOrEmptyBatch_ReturnsSuccessWithNoIndex()
    {
        RaftWriteAhead writeAhead = CreateWriteAhead(startWalScheduler: true, out RaftManager manager, out RaftPartition partition);

        try
        {
            Assert.Equal((RaftOperationStatus.Success, -1L), await writeAhead.Rollback(null));
            Assert.Equal((RaftOperationStatus.Success, -1L), await writeAhead.Rollback([]));
            Assert.Equal(0, writeAhead.GetCommitIndex());
        }
        finally
        {
            partition.Dispose();
            manager.Dispose();
        }
    }

    /// <summary>
    /// The rollback resolution re-types every entry it resolves — Proposed to RolledBack,
    /// ProposedCheckpoint to RolledBackCheckpoint — and advances the commit frontier over each
    /// resolved id, because a rollback resolves its ids exactly like a commit. An id that was
    /// already resolved (Committed) is left untouched and not advanced again.
    /// </summary>
    [Fact]
    public async Task Rollback_MixedBatch_FlipsTypesPerKind_AndAdvancesTheFrontierPerEntry()
    {
        RaftWriteAhead writeAhead = CreateWriteAhead(startWalScheduler: true, out RaftManager manager, out RaftPartition partition);

        try
        {
            RaftLog proposed = Log(1, RaftLogType.Proposed);
            RaftLog checkpoint = Log(2, RaftLogType.ProposedCheckpoint);
            RaftLog alreadyCommitted = Log(3, RaftLogType.Committed);
            List<RaftLog> batch = [proposed, checkpoint, alreadyCommitted];

            (RaftOperationStatus status, _) = await writeAhead.Rollback(batch);
            Assert.Equal(RaftOperationStatus.Pending, status);

            Assert.Equal(RaftLogType.RolledBack, proposed.Type);
            Assert.Equal(RaftLogType.RolledBackCheckpoint, checkpoint.Type);
            Assert.Equal(RaftLogType.Committed, alreadyCommitted.Type);

            // Ids 1 and 2 were resolved by this rollback; id 3 was already resolved before it and
            // is not the rollback's to advance. The rollback re-writes already-present rows, so it
            // advances the commit frontier only — never presence.
            Assert.Equal(2, writeAhead.GetCommitIndex());
            Assert.Equal(0, writeAhead.GetPresentIndex());

            await WaitForCompletionsAsync(count: 1);
        }
        finally
        {
            partition.Dispose();
            manager.Dispose();
        }
    }

    /// <summary>
    /// The frontier advance is gap-buffered: a rollback whose ids sit above an unfilled gap
    /// buffers them until the gap resolves, exactly like a commit — certifying "resolved through
    /// N" over a hole would let a promotion drain skip the missing entries.
    /// </summary>
    [Fact]
    public async Task Rollback_OverAGap_BuffersUntilTheGapResolves()
    {
        RaftWriteAhead writeAhead = CreateWriteAhead(startWalScheduler: true, out RaftManager manager, out RaftPartition partition);

        try
        {
            RaftLog two = Log(2, RaftLogType.Proposed);
            RaftLog three = Log(3, RaftLogType.Proposed);
            await writeAhead.Rollback([two, three]);
            Assert.Equal(0, writeAhead.GetCommitIndex());

            // Filling the hole resolves the whole buffered run.
            RaftLog one = Log(1, RaftLogType.Proposed);
            await writeAhead.Commit([one]);
            Assert.Equal(3, writeAhead.GetCommitIndex());

            await WaitForCompletionsAsync(count: 2);
        }
        finally
        {
            partition.Dispose();
            manager.Dispose();
        }
    }

    /// <summary>
    /// A rollback whose write the scheduler refuses (here: never started, so Enqueue throws)
    /// must restore the original entry types and leave the commit frontier where it was — a
    /// caller that retries the rollback starts from the state it handed over, not a half-applied
    /// resolution.
    /// </summary>
    [Fact]
    public async Task Rollback_RejectedByTheScheduler_RestoresEntryTypesAndFrontier()
    {
        RaftWriteAhead writeAhead = CreateWriteAhead(startWalScheduler: false, out RaftManager manager, out RaftPartition partition);

        try
        {
            RaftLog proposed = Log(1, RaftLogType.Proposed);
            RaftLog checkpoint = Log(2, RaftLogType.ProposedCheckpoint);
            List<RaftLog> batch = [proposed, checkpoint];

            await Assert.ThrowsAsync<InvalidOperationException>(async () => await writeAhead.Rollback(batch));

            Assert.Equal(RaftLogType.Proposed, proposed.Type);
            Assert.Equal(RaftLogType.ProposedCheckpoint, checkpoint.Type);
            Assert.Equal(0, writeAhead.GetCommitIndex());
            Assert.Equal(0, writeAhead.GetPresentIndex());
        }
        finally
        {
            partition.Dispose();
            manager.Dispose();
        }
    }

    // ── failed-write regression vs the published high-water ──────────────────

    /// <summary>
    /// After a failed write the protocol-facing frontier (<see cref="RaftWriteAhead.GetCommitIndex"/>,
    /// which the leader's re-ship reads) and the reported durable frontier regress below the failed
    /// range — but the PUBLISHED commit index is a high-water mark and must not move backwards: an
    /// observer outside the replication protocol would otherwise read an acknowledged entry as
    /// un-committed.
    /// </summary>
    [Fact]
    public async Task FailedWriteRegression_LowersTheProtocolFrontier_NotThePublishedHighWater()
    {
        RaftWriteAhead writeAhead = CreateWriteAhead(startWalScheduler: true, out RaftManager manager, out RaftPartition partition);

        try
        {
            Written(writeAhead, await Append(writeAhead, Committed(1), Committed(2), Committed(3)));
            Assert.Equal(3, writeAhead.GetCommitIndex());
            Assert.Equal(3, writeAhead.GetDurableCommitIndex());
            Assert.Equal(3, writeAhead.GetDurableCommitFrontier());

            await writeAhead.RegressFrontiersAfterFailedWriteAsync(3, 3, regressPresence: true, regressCommit: true);

            // The protocol paths see the failed range as missing so the leader re-ships it...
            Assert.Equal(2, writeAhead.GetCommitIndex());
            Assert.Equal(2, writeAhead.GetDurableCommitFrontier());

            // ...while the published value outside the protocol never regressed.
            Assert.Equal(3, writeAhead.GetDurableCommitIndex());
        }
        finally
        {
            partition.Dispose();
            manager.Dispose();
        }
    }

    // ── restore-completion edges ─────────────────────────────────────────────

    /// <summary>
    /// A blank WAL reconstructs zero frontiers — nothing committed, nothing present, no term
    /// evidence — so a fresh node advertises an empty log rather than whatever it held in a
    /// previous process.
    /// </summary>
    [Fact]
    public async Task CompleteRestore_BlankLog_ReconstructsZeroFrontiers()
    {
        RaftWriteAhead writeAhead = CreateWriteAhead(startWalScheduler: true, out RaftManager manager, out RaftPartition partition);
        global::System.Collections.Concurrent.ConcurrentQueue<long> restoredIds = new();
        manager.OnLogRestored += (_, log) =>
        {
            restoredIds.Enqueue(log.Id);
            return Task.FromResult(true);
        };

        try
        {
            await writeAhead.CompleteRestoreAsync([]);

            Assert.Equal(0, writeAhead.GetCommitIndex());
            Assert.Equal(0, writeAhead.GetPresentIndex());
            Assert.Equal(0, writeAhead.GetPresentTerm());
            // The checkpoint marker itself carries no application payload and is never delivered;
            // the committed prefix it certifies IS replayed to the consumer on restore (the
            // in-memory backend replays everything present — it retains no snapshot).
            Assert.DoesNotContain(5, restoredIds);
            Assert.Subset(new HashSet<long> { 1, 2, 3, 4 }, restoredIds.ToHashSet());
        }
        finally
        {
            partition.Dispose();
            manager.Dispose();
        }
    }

    /// <summary>
    /// A WAL whose tail is a certified CommittedCheckpoint boundary restores both frontiers to the
    /// boundary and replays nothing: the checkpoint certifies its whole prefix, so the committed
    /// entries below the floor are proven, not re-delivered — and the marker itself carries no
    /// application payload.
    /// </summary>
    [Fact]
    public async Task CompleteRestore_CertifiedCheckpointBoundary_JumpsFrontiers_AndReplaysNothingBelowTheFloor()
    {
        InMemoryWAL wal = new(NullLogger<IRaft>.Instance);
        RaftWriteAhead writeAhead = CreateWriteAhead(startWalScheduler: true, out RaftManager manager, out RaftPartition partition, wal);
        global::System.Collections.Concurrent.ConcurrentQueue<long> restoredIds = new();
        manager.OnLogRestored += (_, log) =>
        {
            restoredIds.Enqueue(log.Id);
            return Task.FromResult(true);
        };

        try
        {
            const int partitionId = 1;
            // The checkpoint certificate only records when the prefix is present, so the marker
            // rides on the four committed entries it certifies.
            List<RaftLog> batch =
            [
                Committed(1, term: 2),
                Committed(2, term: 2),
                Committed(3, term: 2),
                Committed(4, term: 2),
                new RaftLog
                {
                    Id = 5,
                    Term = 2,
                    Type = RaftLogType.CommittedCheckpoint,
                    LogType = "restore-boundary-test",
                    LogData = [1, 2, 3],
                },
            ];
            Assert.Equal(RaftOperationStatus.Success, wal.Write(new List<(int, List<RaftLog>)> { (partitionId, batch) }));

            IReadOnlyList<RaftLog> logs = await writeAhead.LoadRestoreLogsAsync();
            Assert.Equal(5, logs.Count);
            Assert.Equal(5, logs[^1].Id);
            Assert.Equal(RaftLogType.CommittedCheckpoint, logs[^1].Type);

            await writeAhead.CompleteRestoreAsync(logs);

            Assert.Equal(5, writeAhead.GetCommitIndex());
            Assert.Equal(5, writeAhead.GetPresentIndex());
            Assert.Equal(2, writeAhead.GetPresentTerm());
            // The checkpoint marker itself carries no application payload and is never delivered;
            // the committed prefix it certifies IS replayed to the consumer on restore (the
            // in-memory backend replays everything present — it retains no snapshot).
            Assert.DoesNotContain(5, restoredIds);
            Assert.Subset(new HashSet<long> { 1, 2, 3, 4 }, restoredIds.ToHashSet());
        }
        finally
        {
            partition.Dispose();
            manager.Dispose();
        }
    }

    // ── harness (mirrors the failed-write-regression harness) ────────────────

    private readonly ConcurrentDictionary<long, TaskCompletionSource<RaftWalCompletion>> completions = new();

    private void OnWalComplete(RaftWalCompletion completion) =>
        completions.GetOrAdd(
                completion.OperationId,
                static _ => new TaskCompletionSource<RaftWalCompletion>(TaskCreationOptions.RunContinuationsAsynchronously))
            .TrySetResult(completion);

    /// <summary>
    /// Enqueues the batch and waits for its durable WAL completion, so the in-memory frontiers the
    /// assertions read have already absorbed the completion's durable-presence advance.
    /// </summary>
    private async Task<RaftWalCompletion> Append(RaftWriteAhead writeAhead, params RaftLog[] logs)
    {
        WALWriteOperation? operation = writeAhead.EnqueueProposeOrCommit([.. logs]);

        Assert.NotNull(operation);

        RaftWalCompletion completion = await completions.GetOrAdd(
                operation.OperationId,
                static _ => new TaskCompletionSource<RaftWalCompletion>(TaskCreationOptions.RunContinuationsAsynchronously))
            .Task.WaitAsync(TimeSpan.FromSeconds(30));

        Assert.Equal(RaftOperationStatus.Success, completion.Status);
        return completion;
    }

    /// <summary>
    /// Everything the completion router records for a successful completion: durable presence and
    /// resolution. The published index is "resolved AND durably held", so marking durability alone
    /// never publishes — resolution must be recorded too.
    /// </summary>
    private static void Written(RaftWriteAhead writeAhead, RaftWalCompletion completion)
    {
        writeAhead.MarkDurablyWritten(completion.MinLogIndex, completion.WrittenMaxLogIndex, completion.SparseLogIds);
        writeAhead.MarkResolutionWritten(completion.ResolvedMinLogIndex, completion.ResolvedMaxLogIndex, completion.Synced);
    }

    /// <summary>
    /// Waits until the storage worker has delivered <paramref name="count"/> completions through
    /// <see cref="OnWalComplete"/> — the write paths return before durability, and a test must not
    /// dispose the harness with a write still queued behind its assertions.
    /// </summary>
    private async Task WaitForCompletionsAsync(int count)
    {
        long deadline = Environment.TickCount64 + 30_000;
        while (completions.Count < count && Environment.TickCount64 < deadline)
            await Task.Delay(10, TestContext.Current.CancellationToken);

        Assert.True(completions.Count >= count, $"expected {count} WAL completions, saw {completions.Count}");
    }

    private static RaftLog Log(long id, RaftLogType type) => new()
    {
        Id = id,
        Term = 1,
        Type = type,
        LogType = "rollback-test",
        LogData = [1, 2, 3],
    };

    private static RaftLog Committed(long id, long term = 1) => new()
    {
        Id = id,
        Term = term,
        Type = RaftLogType.Committed,
        LogType = "frontier-regression-test",
        LogData = [1, 2, 3],
    };

    private RaftWriteAhead CreateWriteAhead(bool startWalScheduler, out RaftManager manager, out RaftPartition partition) =>
        CreateWriteAhead(startWalScheduler, out manager, out partition, new InMemoryWAL(NullLogger<IRaft>.Instance));

    private RaftWriteAhead CreateWriteAhead(bool startWalScheduler, out RaftManager manager, out RaftPartition partition, InMemoryWAL wal)
    {
        const int partitionId = 1;

        RaftConfiguration config = new()
        {
            Host = "localhost",
            Port = 9000,
            InitialPartitions = 0,
        };

        manager = new(
            config,
            new StaticDiscovery([]),
            wal,
            new InMemoryCommunication(),
            new HybridLogicalClock(),
            NullLogger<IRaft>.Instance);

        ((FairReadScheduler)manager.ReadScheduler).Start();
        if (startWalScheduler)
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
