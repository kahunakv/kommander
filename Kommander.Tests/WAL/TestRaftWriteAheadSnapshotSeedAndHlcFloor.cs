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
/// Frontier-seeding and high-water edges of <see cref="RaftWriteAhead"/>:
///
/// <list type="bullet">
///   <item>Seeding the commit frontier from an installed snapshot boundary jumps every frontier
///         to the boundary, drains buffered resolutions that became contiguous, and — for a
///         truncating install — purges the buffered bookkeeping of rows the truncation deleted.</item>
///   <item>The promotion-time prefix absorption reconciles the in-memory frontier with ids the
///         node has already delivered, draining buffered successors and never moving backwards.</item>
///   <item>The durable HLC high-water mark is persisted once per slack window ahead of the
///         observed timestamp, is not re-persisted for smaller timestamps inside the window, and
///         retries after its queued write failed.</item>
///   <item>A stale Proposed duplicate of an already-resolved id (contiguous or still buffered
///         over a gap) is skipped rather than written, and a first-durability resolved row flags
///         its operation for its own fsync.</item>
/// </list>
/// </summary>
public sealed class TestRaftWriteAheadSnapshotSeedAndHlcFloor
{
    // ── snapshot seed ────────────────────────────────────────────────────────

    /// <summary>
    /// A non-truncating boundary install certifies its whole prefix, so the seed jumps the commit,
    /// presence, durable and resolved frontiers to the boundary and then drains the ids that were
    /// buffered over the (now certified) gap: a follower that receives a snapshot while entries
    /// sit buffered above a hole must end up reporting both the boundary and those entries.
    /// </summary>
    [Fact]
    public async Task SeedCommitFrontierFromSnapshot_NonTruncatingInstall_JumpsTheFrontiersAndDrainsTheGap()
    {
        RaftWriteAhead writeAhead = CreateWriteAhead(out RaftManager manager, out RaftPartition partition);

        try
        {
            // Resolutions 4 and 5 land over the unfilled gap 1..3: they buffer everywhere.
            Written(writeAhead, await Append(writeAhead, Resolved(4), Resolved(5)));

            Assert.Equal(0, writeAhead.GetCommitIndex());
            Assert.Equal(0, writeAhead.GetPresentIndex());
            Assert.True(writeAhead.HasPresenceGap(), "ids 4 and 5 sit buffered above the gap");

            writeAhead.SeedCommitFrontierFromSnapshot(3, snapshotTerm: 2, suffixTruncated: false);

            Assert.Equal(5, writeAhead.GetCommitIndex());
            Assert.Equal(2, writeAhead.GetPresentTerm());

            Assert.Equal(5, writeAhead.GetDurableCommitIndex());
            Assert.Equal(5, writeAhead.GetDurableCommitFrontier());
            Assert.Equal(5, writeAhead.GetDurableResolvedIndex());
            Assert.Equal(5, writeAhead.GetReadableResolvedHighWater());
        }
        finally
        {
            partition.Dispose();
            manager.Dispose();
        }
    }

    /// <summary>
    /// A truncating install just deleted every row above the boundary, so the seed purges the
    /// buffered resolutions, presence and durability entries above it BEFORE draining: absorbing
    /// the stale ids would certify a commit frontier over entries the truncation removed (the
    /// deposed leader's optimistically-resolved tail is exactly what those buffers hold).
    /// </summary>
    [Fact]
    public async Task SeedCommitFrontierFromSnapshot_TruncatingInstall_PurgesBookkeepingAboveTheBoundary()
    {
        RaftWriteAhead writeAhead = CreateWriteAhead(out RaftManager manager, out RaftPartition partition);

        try
        {
            Written(writeAhead, await Append(writeAhead, Resolved(4), Resolved(5)));

            writeAhead.SeedCommitFrontierFromSnapshot(2, snapshotTerm: 9, suffixTruncated: true);

            // Everything buffered above the boundary is gone; the frontiers sit exactly at it.
            Assert.Equal(2, writeAhead.GetCommitIndex());
            Assert.Equal(2, writeAhead.GetPresentIndex());
            Assert.Equal(9, writeAhead.GetPresentTerm());
            Assert.False(writeAhead.HasPresenceGap());

            Assert.Equal(2, writeAhead.GetDurableCommitIndex());
            Assert.Equal(2, writeAhead.GetDurableCommitFrontier());

            // The synced resolved high-water never regresses within a process lifetime: the rows
            // WERE on disk when their synced write completed, and the report is capped by the
            // commit frontier above rather than by this high-water.
            Assert.Equal(5, writeAhead.GetDurableResolvedIndex());
        }
        finally
        {
            partition.Dispose();
            manager.Dispose();
        }
    }

    // ── promotion-time prefix absorption ─────────────────────────────────────

    /// <summary>
    /// The applied-cursor absorption at promotion advances the frontier over ids the node has
    /// already delivered (applied and present implies resolved) and then drains the buffered
    /// successors that became contiguous — without it, a leader stint freezes a follower-era dip
    /// below buffered commits for its whole tenure. Absorbing a lower id afterwards is a no-op:
    /// the frontier never moves backwards.
    /// </summary>
    [Fact]
    public async Task AbsorbResolvedPrefix_OverAnAppliedPrefix_DrainsBufferedSuccessorsAndNeverLowers()
    {
        RaftWriteAhead writeAhead = CreateWriteAhead(out RaftManager manager, out RaftPartition partition);

        try
        {
            // 5 and 6 buffer over the unfilled gap 1..4.
            Written(writeAhead, await Append(writeAhead, Resolved(5), Resolved(6)));
            Assert.Equal(0, writeAhead.GetCommitIndex());

            writeAhead.AbsorbResolvedPrefix(4);

            Assert.Equal(6, writeAhead.GetCommitIndex());

            // Durability is still buffered over the same gap: the absorption reconciles RESOLUTION
            // knowledge only, so nothing is published outside the protocol yet.
            Assert.Equal(0, writeAhead.GetDurableCommitIndex());

            // A stale lower absorption may not drag the frontier back down.
            writeAhead.AbsorbResolvedPrefix(2);
            Assert.Equal(6, writeAhead.GetCommitIndex());
        }
        finally
        {
            partition.Dispose();
            manager.Dispose();
        }
    }

    // ── durable HLC high-water mark ──────────────────────────────────────────

    /// <summary>
    /// The first observed entry timestamp persists a floor one slack window (10 s) ahead of it as
    /// one queued metadata write; a later, SMALLER timestamp inside the window persists nothing;
    /// and after the queued write is reported failed the cache regresses so the next observation
    /// above the previous bound retries the persist.
    /// </summary>
    [Fact]
    public async Task ObservedLeaderTimestamps_PersistOneHlcFloorPerSlackWindow_AndRetryAfterAFailedWrite()
    {
        InMemoryWAL wal = new(NullLogger<IRaft>.Instance);
        RaftWriteAhead writeAhead = CreateWriteAhead(out RaftManager manager, out RaftPartition partition, wal);

        try
        {
            // A follower append carrying the leader's timestamp: the batch's maximum stamps the floor.
            await AppendWithTimestamp(writeAhead, timestampL: 50_000, Resolved(1));
            RaftWalCompletion floorWrite = await WaitForHlcFloorCompletionAsync();

            Assert.Equal(WALWriteOperationType.HlcFloor, floorWrite.OperationType);
            Assert.Equal(50_000 + 10_000, floorWrite.MetadataValue);
            Assert.Equal((50_000 + 10_000).ToString(), wal.GetMetaData("raft_hlcfloor_p1"));

            // A smaller timestamp inside the persisted window enqueues nothing.
            await AppendWithTimestamp(writeAhead, timestampL: 20_000, Resolved(2));
            await Task.Delay(200, TestContext.Current.CancellationToken);
            Assert.Equal(1, CountHlcFloorCompletions());

            // The queued write failed: the cache regresses below the observed maximum so the next
            // observation retries instead of silently running with an unpersisted bound.
            writeAhead.NoteHlcFloorWriteFailed(50_000 + 10_000);

            await AppendWithTimestamp(writeAhead, timestampL: 55_000, Resolved(3));
            RaftWalCompletion retry = await WaitForHlcFloorCompletionAsync(minimumCount: 2);

            Assert.Equal(55_000 + 10_000, retry.MetadataValue);
            Assert.Equal((55_000 + 10_000).ToString(), wal.GetMetaData("raft_hlcfloor_p1"));
        }
        finally
        {
            partition.Dispose();
            manager.Dispose();
        }
    }

    // ── stale-duplicate and first-durability guards on the append path ──────

    /// <summary>
    /// An incoming Proposed (or ProposedCheckpoint) row whose id is already resolved locally —
    /// below the contiguous frontier or still buffered over a gap — is skipped, not written:
    /// writing it would regress the durable row to Proposed while the in-memory frontier stays
    /// past it, and the post-append tail sweep would then delete the row and leave a hole.
    /// </summary>
    [Fact]
    public async Task EnqueueProposeOrCommit_StaleProposedDuplicatesOfResolvedIds_AreSkippedNotWritten()
    {
        RaftWriteAhead writeAhead = CreateWriteAhead(out RaftManager manager, out RaftPartition partition);

        try
        {
            Written(writeAhead, await Append(writeAhead, Resolved(1), Resolved(2), Resolved(3)));
            Assert.Equal(3, writeAhead.GetCommitIndex());

            Assert.Null(writeAhead.EnqueueProposeOrCommit([Proposed(2)]));
            Assert.Equal(1, writeAhead.GetStaleProposedSkippedCount());

            Assert.Null(writeAhead.EnqueueProposeOrCommit([ProposedCheckpoint(3)]));
            Assert.Equal(2, writeAhead.GetStaleProposedSkippedCount());

            // Resolved-but-buffered (above a gap) is protected exactly like the contiguous prefix.
            await Append(writeAhead, Resolved(7));
            Assert.Equal(3, writeAhead.GetCommitIndex());

            Assert.Null(writeAhead.EnqueueProposeOrCommit([Proposed(7)]));
            Assert.Equal(3, writeAhead.GetStaleProposedSkippedCount());

            await DrainCompletionsAsync();
        }
        finally
        {
            partition.Dispose();
            manager.Dispose();
        }
    }

    /// <summary>
    /// A resolved row this node does not yet hold is its first durable write, whatever its type
    /// says, so the enqueued operation demands its own fsync and stamps the accepted-id floor at
    /// the batch's own maximum. A marker for an id already held (or buffered) rides the propose's
    /// fsync instead.
    /// </summary>
    [Fact]
    public async Task EnqueueProposeOrCommit_FirstDurabilityResolvedRow_RequiresItsOwnFsync()
    {
        RaftWriteAhead writeAhead = CreateWriteAhead(out RaftManager manager, out RaftPartition partition);

        try
        {
            WALWriteOperation? first = writeAhead.EnqueueProposeOrCommit([Resolved(5)]);

            Assert.NotNull(first);
            Assert.True(first!.RequiresSync, "a Committed row the partition never held is a first-durability write");
            Assert.Equal(5, first.TruncateFloor);

            Written(writeAhead, await WaitForCompletionAsync(first!));

            // The id is held now (buffered over the gap): its re-shipped marker needs no fsync.
            WALWriteOperation? second = writeAhead.EnqueueProposeOrCommit([Resolved(5)]);

            Assert.NotNull(second);
            Assert.False(second!.RequiresSync, "the id is buffered over a gap, so the marker rides its propose's fsync");
            Assert.Equal(5, second.TruncateFloor);

            await DrainCompletionsAsync();
        }
        finally
        {
            partition.Dispose();
            manager.Dispose();
        }
    }

    /// <summary>
    /// Empty or null propose/commit batches succeed without enqueuing anything, and a commit
    /// batch containing no unresolved entries reports no commit index (there is nothing to
    /// resolve).
    /// </summary>
    [Fact]
    public async Task ProposeAndCommit_EmptyOrAlreadyResolvedBatches_SucceedWithoutEnqueueing()
    {
        RaftWriteAhead writeAhead = CreateWriteAhead(out RaftManager manager, out RaftPartition partition);

        try
        {
            Assert.Equal((RaftOperationStatus.Success, -1L), await writeAhead.Propose(1, null));
            Assert.Equal((RaftOperationStatus.Success, -1L), await writeAhead.Propose(1, []));
            Assert.Equal((RaftOperationStatus.Success, -1L), await writeAhead.Commit(null));

            // Only Proposed/ProposedCheckpoint entries advance the commit index of the batch.
            Assert.Equal((RaftOperationStatus.Pending, -1L),
                await writeAhead.Commit([Resolved(1), RolledBack(2)]));

            await DrainCompletionsAsync();
        }
        finally
        {
            partition.Dispose();
            manager.Dispose();
        }
    }

    // ── harness (mirrors the rollback/restore harness) ───────────────────────

    private readonly ConcurrentDictionary<long, TaskCompletionSource<RaftWalCompletion>> completions = new();
    private readonly ConcurrentQueue<RaftWalCompletion> hlcFloorCompletions = new();

    private void OnWalComplete(RaftWalCompletion completion)
    {
        completions.GetOrAdd(
                completion.OperationId,
                static _ => new TaskCompletionSource<RaftWalCompletion>(TaskCreationOptions.RunContinuationsAsynchronously))
            .TrySetResult(completion);

        if (completion.OperationType == WALWriteOperationType.HlcFloor)
            hlcFloorCompletions.Enqueue(completion);
    }

    private int CountHlcFloorCompletions() => hlcFloorCompletions.Count;

    /// <summary>Waits for the n-th durable HLC-floor completion the scheduler delivered.</summary>
    private async Task<RaftWalCompletion> WaitForHlcFloorCompletionAsync(int minimumCount = 1)
    {
        long deadline = Environment.TickCount64 + 30_000;
        while (hlcFloorCompletions.Count < minimumCount && Environment.TickCount64 < deadline)
            await Task.Delay(10, TestContext.Current.CancellationToken);

        Assert.True(hlcFloorCompletions.Count >= minimumCount,
            $"expected {minimumCount} HLC-floor completion(s), saw {hlcFloorCompletions.Count}");
        return hlcFloorCompletions.ToArray()[minimumCount - 1];
    }

    /// <summary>
    /// Enqueues the batch and waits for its durable WAL completion, so the in-memory frontiers the
    /// assertions read have already absorbed the completion's durable-presence advance.
    /// </summary>
    private async Task<RaftWalCompletion> Append(RaftWriteAhead writeAhead, params RaftLog[] logs)
    {
        WALWriteOperation? operation = writeAhead.EnqueueProposeOrCommit([.. logs]);
        Assert.NotNull(operation);
        return await WaitForCompletionAsync(operation!);
    }

    /// <summary>
    /// Appends entries stamped with a leader timestamp (the follower append path stamps each entry
    /// from the batch ticket, and the high-water logic reads the entries' own timestamps), returning
    /// once the rows are durable.
    /// </summary>
    private async Task<RaftWalCompletion> AppendWithTimestamp(RaftWriteAhead writeAhead, long timestampL, params RaftLog[] logs)
    {
        HLCTimestamp timestamp = new(0, timestampL, 0);
        foreach (RaftLog log in logs)
            log.Time = timestamp;

        WALWriteOperation? enqueued = writeAhead.EnqueueProposeOrCommit([.. logs], timestamp);
        Assert.NotNull(enqueued);

        RaftWalCompletion completion = await WaitForCompletionAsync(enqueued!);
        Written(writeAhead, completion);
        return completion;
    }

    private async Task<RaftWalCompletion> WaitForCompletionAsync(WALWriteOperation operation)
    {
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

    /// <summary>Waits until every completion observed so far has settled (nothing queued behind the assertions).</summary>
    private async Task DrainCompletionsAsync()
    {
        await Task.Delay(100, TestContext.Current.CancellationToken);
    }

    private static RaftLog Resolved(long id, long term = 1) => new()
    {
        Id = id,
        Term = term,
        Type = RaftLogType.Committed,
        LogType = "seed-hlc-test",
        LogData = [1, 2, 3],
    };

    private static RaftLog Proposed(long id, long term = 1) => new()
    {
        Id = id,
        Term = term,
        Type = RaftLogType.Proposed,
        LogType = "seed-hlc-test",
        LogData = [1, 2, 3],
    };

    private static RaftLog ProposedCheckpoint(long id, long term = 1) => new()
    {
        Id = id,
        Term = term,
        Type = RaftLogType.ProposedCheckpoint,
        LogType = "seed-hlc-test",
        LogData = [1, 2, 3],
    };

    private static RaftLog RolledBack(long id, long term = 1) => new()
    {
        Id = id,
        Term = term,
        Type = RaftLogType.RolledBack,
        LogType = "seed-hlc-test",
        LogData = [1, 2, 3],
    };

    private RaftWriteAhead CreateWriteAhead(out RaftManager manager, out RaftPartition partition) =>
        CreateWriteAhead(out manager, out partition, new InMemoryWAL(NullLogger<IRaft>.Instance));

    private RaftWriteAhead CreateWriteAhead(out RaftManager manager, out RaftPartition partition, InMemoryWAL wal)
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
