using Kommander.Data;
using Kommander.Tests.Simulation.WAL;
using Kommander.WAL;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.Simulation;

/// <summary>
/// Tests for the simulated write-ahead log.
///
/// <para><b>Why each of these matters.</b> The store exists to lose data on purpose. A fault that
/// silently loses nothing would make every crash scenario pass while testing nothing, and a fault
/// that lost more than its model says would produce failures no production node could reach. So
/// each mode is checked from both sides: what it takes, and what it must leave alone.</para>
///
/// <para>No cluster runs here. The file costs milliseconds.</para>
/// </summary>
[Trait("Category", "DSTSmoke")]
public sealed class TestSimulatedWal
{
    private const int PartitionId = 1;

    // ── Transparency ──────────────────────────────────────────────────────

    /// <summary>
    /// With no fault and no latency the store behaves exactly like the one it wraps. This is the
    /// property that lets every existing scenario keep its meaning when the simulated store is
    /// wired in by default.
    /// </summary>
    [Fact]
    public void Default_BehavesLikeTheStoreItWraps()
    {
        using SimulatedWAL wal = NewWal(out _);

        Assert.Equal(RaftOperationStatus.Success, wal.Write([(PartitionId, [
            Entry(1, term: 1, RaftLogType.Committed),
            Entry(2, term: 1, RaftLogType.Committed),
            Entry(3, term: 2, RaftLogType.Proposed),
        ])]));

        List<RaftLog> stored = wal.ReadLogsRange(PartitionId, 0);

        Assert.Equal(3, stored.Count);
        Assert.Equal([1L, 2L, 3L], stored.Select(entry => entry.Id));
        Assert.Equal(3, wal.GetMaxLog(PartitionId));
        Assert.Equal(2, wal.GetCurrentTerm(PartitionId));
        Assert.Equal(1, wal.GetTermAt(PartitionId, 2));
    }

    // ── Injected faults ───────────────────────────────────────────────────

    /// <summary>
    /// A full disk refuses every write and stores nothing. A partial write would be a second failure
    /// model on top of the first, and the interface promises a batch is atomic per partition.
    /// </summary>
    [Fact]
    public void OutOfSpace_RefusesEveryWriteAndStoresNothing()
    {
        using SimulatedWAL wal = NewWal(out _);

        wal.Write([(PartitionId, [Entry(1, term: 1, RaftLogType.Committed)])]);
        wal.SetOutOfSpace(true);

        Assert.Equal(RaftOperationStatus.Errored, wal.Write([(PartitionId, [
            Entry(2, term: 1, RaftLogType.Committed),
            Entry(3, term: 1, RaftLogType.Committed),
        ])]));

        Assert.Single(wal.ReadLogsRange(PartitionId, 0));
        Assert.Equal(1, wal.GetMaxLog(PartitionId));

        wal.SetOutOfSpace(false);

        Assert.Equal(RaftOperationStatus.Success, wal.Write([(PartitionId, [Entry(2, term: 1, RaftLogType.Committed)])]));
        Assert.Equal(2, wal.GetMaxLog(PartitionId));
        Assert.Equal(1, wal.Counters.FailedWrites);
    }

    /// <summary>A transient fault refuses exactly the requested number of writes, then stops.</summary>
    [Fact]
    public void FailNextWrites_RefusesExactlyThatManyWrites()
    {
        using SimulatedWAL wal = NewWal(out _);

        wal.FailNextWrites(2);

        Assert.Equal(RaftOperationStatus.Errored, wal.Write([(PartitionId, [Entry(1, term: 1, RaftLogType.Committed)])]));
        Assert.Equal(RaftOperationStatus.Errored, wal.Write([(PartitionId, [Entry(2, term: 1, RaftLogType.Committed)])]));
        Assert.Equal(RaftOperationStatus.Success, wal.Write([(PartitionId, [Entry(3, term: 1, RaftLogType.Committed)])]));

        Assert.Equal([3L], wal.ReadLogsRange(PartitionId, 0).Select(entry => entry.Id));
        Assert.Equal(2, wal.Counters.FailedWrites);
    }

    // ── The durability window ─────────────────────────────────────────────

    /// <summary>A crash keeps everything that reached its fsync.</summary>
    [Fact]
    public void Crash_KeepsWhatWasAlreadyDurable()
    {
        using SimulatedWAL wal = NewWal(out _);

        wal.Write([(PartitionId, [Entry(1, term: 1, RaftLogType.Committed), Entry(2, term: 1, RaftLogType.Committed)])]);
        wal.Crash();

        Assert.Equal([1L, 2L], wal.ReadLogsRange(PartitionId, 0).Select(entry => entry.Id));
        Assert.Equal(0, wal.Counters.EntriesLostOnCrash);
    }

    /// <summary>
    /// A crash reverts a commit marker that no fsync carried, and the entry comes back in the form
    /// it last had on disk.
    ///
    /// <para>This is the headline case, and it is not hypothetical: with the single-fsync fast path
    /// on — which is the default — the write-ahead-log scheduler writes a batch of per-entry
    /// committed markers with <c>sync</c> off, on the argument that the entry is already
    /// quorum-durable from its propose fsync and the marker can be reconstructed. This test is what
    /// that argument has to survive.</para>
    /// </summary>
    [Fact]
    public void Crash_RevertsACommitMarkerThatNoFsyncCarried()
    {
        using SimulatedWAL wal = NewWal(out _);

        wal.Write([(PartitionId, [Entry(1, term: 7, RaftLogType.Proposed)])]);
        wal.Write([(PartitionId, [Entry(1, term: 7, RaftLogType.Committed)])], sync: false);

        Assert.Equal(RaftLogType.Committed, wal.ReadLogsRange(PartitionId, 0)[0].Type);

        wal.Crash();

        RaftLog restored = Assert.Single(wal.ReadLogsRange(PartitionId, 0));
        Assert.Equal(1, restored.Id);
        Assert.Equal(7, restored.Term);
        Assert.Equal(RaftLogType.Proposed, restored.Type);
        Assert.Equal(1, wal.Counters.EntriesLostOnCrash);
    }

    /// <summary>
    /// A commit marker that a later sync write carried survives the crash. Without this the model
    /// would lose every marker forever, which would be a fault nobody could pass rather than the
    /// window production actually has.
    /// </summary>
    [Fact]
    public void ACommitMarker_SurvivesOnceALaterSyncWriteCarriesIt()
    {
        using SimulatedWAL wal = NewWal(out _);

        wal.Write([(PartitionId, [Entry(1, term: 7, RaftLogType.Proposed)])]);
        wal.Write([(PartitionId, [Entry(1, term: 7, RaftLogType.Committed)])], sync: false);

        // Any later durable write on the same partition is the carrier.
        wal.Write([(PartitionId, [Entry(2, term: 7, RaftLogType.Proposed)])]);

        wal.Crash();

        List<RaftLog> stored = wal.ReadLogsRange(PartitionId, 0);
        Assert.Equal(2, stored.Count);
        Assert.Equal(RaftLogType.Committed, stored[0].Type);
        Assert.Equal(0, wal.Counters.EntriesLostOnCrash);
    }

    /// <summary>A crash inside the fsync window removes a brand-new entry, because it had no earlier version.</summary>
    [Fact]
    public void Crash_InsideTheWriteWindow_RemovesTheEntry()
    {
        using SimulatedWAL wal = NewWal(out Clock clock);
        wal.WriteLatencyMilliseconds = 50;

        wal.Write([(PartitionId, [Entry(1, term: 1, RaftLogType.Committed)])]);

        clock.Milliseconds = 10;
        wal.Crash();

        Assert.Empty(wal.ReadLogsRange(PartitionId, 0));
        Assert.Equal(1, wal.Counters.EntriesLostOnCrash);
    }

    /// <summary>The same write, crashed after its window has elapsed, survives.</summary>
    [Fact]
    public void Crash_AfterTheWriteWindow_KeepsTheEntry()
    {
        using SimulatedWAL wal = NewWal(out Clock clock);
        wal.WriteLatencyMilliseconds = 50;

        wal.Write([(PartitionId, [Entry(1, term: 1, RaftLogType.Committed)])]);

        clock.Milliseconds = 50;
        wal.Crash();

        Assert.Single(wal.ReadLogsRange(PartitionId, 0));
        Assert.Equal(0, wal.Counters.EntriesLostOnCrash);
    }

    /// <summary>A slow disk widens the window: the same crash time now loses a write that a fast disk kept.</summary>
    [Fact]
    public void SlowDisk_WidensWhatOneCrashTakes()
    {
        using SimulatedWAL fast = NewWal(out Clock fastClock);
        using SimulatedWAL slow = NewWal(out Clock slowClock);

        fast.WriteLatencyMilliseconds = 5;
        slow.WriteLatencyMilliseconds = 500;

        fast.Write([(PartitionId, [Entry(1, term: 1, RaftLogType.Committed)])]);
        slow.Write([(PartitionId, [Entry(1, term: 1, RaftLogType.Committed)])]);

        fastClock.Milliseconds = 100;
        slowClock.Milliseconds = 100;

        fast.Crash();
        slow.Crash();

        Assert.Single(fast.ReadLogsRange(PartitionId, 0));
        Assert.Empty(slow.ReadLogsRange(PartitionId, 0));
    }

    // ── Deletion is durable ───────────────────────────────────────────────

    /// <summary>
    /// A crash does not bring back a truncated tail. Deletion is modelled as immediately durable, so
    /// that a divergence check never has to reason about a resurrected entry as well as a lost one.
    /// </summary>
    [Fact]
    public void Truncation_IsDurable_ACrashDoesNotBringTheTailBack()
    {
        using SimulatedWAL wal = NewWal(out _);
        wal.WriteLatencyMilliseconds = 100;

        wal.Write([(PartitionId, [
            Entry(1, term: 1, RaftLogType.Committed),
            Entry(2, term: 1, RaftLogType.Proposed),
            Entry(3, term: 1, RaftLogType.Proposed),
        ])]);

        Assert.Equal(RaftOperationStatus.Success, wal.TruncateLogsAfter(PartitionId, 1));
        wal.Crash();

        Assert.Empty(wal.ReadLogsRange(PartitionId, 0));
        Assert.Equal(1, wal.Counters.Truncations);
    }

    // ── Compaction floors and retention holds ─────────────────────────────

    /// <summary>Without a hold, compaction reaches the checkpoint floor.</summary>
    [Fact]
    public void Compaction_WithoutAHold_ReachesTheCheckpointFloor()
    {
        using SimulatedWAL wal = NewWal(out _);
        WriteRun(wal, 1, 10);

        (RaftOperationStatus status, int removed) = wal.CompactLogsOlderThan(PartitionId, 6, 100);

        Assert.Equal(RaftOperationStatus.Success, status);
        Assert.Equal(5, removed);
        Assert.Equal([6L, 7L, 8L, 9L, 10L], wal.ReadLogsRange(PartitionId, 0).Select(entry => entry.Id));
    }

    /// <summary>
    /// A retention hold stops compaction at its own index even when the caller asks for a higher
    /// floor. This is the shape that pins a lagging follower's entries in place, and the disagreement
    /// between the hold and the checkpoint is where the interesting failures live.
    /// </summary>
    [Fact]
    public void RetentionHold_StopsCompactionAtItsOwnIndex()
    {
        using SimulatedWAL wal = NewWal(out _);
        WriteRun(wal, 1, 10);

        wal.SetRetentionHold(PartitionId, 3);

        (RaftOperationStatus status, int removed) = wal.CompactLogsOlderThan(PartitionId, 8, 100);

        Assert.Equal(RaftOperationStatus.Success, status);
        Assert.Equal(2, removed);
        Assert.Equal(3, wal.ReadLogsRange(PartitionId, 0)[0].Id);

        wal.ClearRetentionHold(PartitionId);

        (_, int removedAfterRelease) = wal.CompactLogsOlderThan(PartitionId, 8, 100);

        Assert.Equal(5, removedAfterRelease);
        Assert.Equal(8, wal.ReadLogsRange(PartitionId, 0)[0].Id);
    }

    // ── Hard state ────────────────────────────────────────────────────────

    /// <summary>
    /// Hard state written with no fsync behind it is lost by a crash. The interface documents this
    /// as a deliberate latency trade-off, so the harness must be able to produce it: a node that
    /// forgets its last vote and votes again in the same term is a real way to elect two leaders.
    /// </summary>
    [Fact]
    public void HardState_IsLostWhenNoFsyncCarriesIt()
    {
        using SimulatedWAL wal = NewWal(out _);
        IWAL store = wal;

        store.PersistHardState(PartitionId, currentTerm: 4, votedFor: "node2");
        Assert.True(store.TryGetHardState(PartitionId, out long term, out string? votedFor));
        Assert.Equal(4, term);
        Assert.Equal("node2", votedFor);

        wal.Crash();

        Assert.False(store.TryGetHardState(PartitionId, out _, out _));
        Assert.Equal(1, wal.Counters.MetadataKeysLostOnCrash);
    }

    /// <summary>Hard state that a later durable write carried survives the crash.</summary>
    [Fact]
    public void HardState_SurvivesOnceAFsyncCarriesIt()
    {
        using SimulatedWAL wal = NewWal(out _);
        IWAL store = wal;

        store.PersistHardState(PartitionId, currentTerm: 4, votedFor: "node2");
        wal.Write([(PartitionId, [Entry(1, term: 4, RaftLogType.Committed)])]);

        wal.Crash();

        Assert.True(store.TryGetHardState(PartitionId, out long term, out string? votedFor));
        Assert.Equal(4, term);
        Assert.Equal("node2", votedFor);
        Assert.Equal(0, wal.Counters.MetadataKeysLostOnCrash);
    }

    // ── Snapshots for the invariant checks ────────────────────────────────

    /// <summary>A missing id inside the retained range is reported as a hole.</summary>
    [Fact]
    public void Snapshot_ReportsAHoleInsideTheRetainedRange()
    {
        using SimulatedWAL wal = NewWal(out _);

        wal.Write([(PartitionId, [
            Entry(1, term: 1, RaftLogType.Committed),
            Entry(2, term: 1, RaftLogType.Committed),
            Entry(4, term: 1, RaftLogType.Committed),
        ])]);

        SimulatedWalPartitionSnapshot partition = Assert.IsType<SimulatedWalPartitionSnapshot>(
            wal.Snapshot().Partition(PartitionId));

        Assert.True(partition.HasHole);
        Assert.Equal([3L], partition.MissingIds);
        Assert.Equal(1, partition.FirstLogId);
        Assert.Equal(4, partition.MaxLogId);
        Assert.Equal(3, partition.Count(RaftLogType.Committed));
    }

    /// <summary>
    /// A compacted prefix is not a hole. Reporting it as one would raise a false alarm on every node
    /// that has ever compacted, which is every long-lived node.
    /// </summary>
    [Fact]
    public void Snapshot_DoesNotCallACompactedPrefixAHole()
    {
        using SimulatedWAL wal = NewWal(out _);
        WriteRun(wal, 1, 6);
        wal.CompactLogsOlderThan(PartitionId, 4, 100);

        SimulatedWalPartitionSnapshot partition = Assert.IsType<SimulatedWalPartitionSnapshot>(
            wal.Snapshot().Partition(PartitionId));

        Assert.False(partition.HasHole);
        Assert.Equal(4, partition.FirstLogId);
        Assert.Equal(6, partition.MaxLogId);
    }

    /// <summary>The snapshot names exactly the entries a crash at that instant would take.</summary>
    [Fact]
    public void Snapshot_NamesTheEntriesInsideTheWindow()
    {
        using SimulatedWAL wal = NewWal(out Clock clock);
        wal.WriteLatencyMilliseconds = 20;

        wal.Write([(PartitionId, [Entry(1, term: 1, RaftLogType.Committed)])]);
        Assert.Equal([1L], wal.Snapshot().Partition(PartitionId)!.NonDurableIds);

        clock.Milliseconds = 20;
        wal.Write([(PartitionId, [Entry(2, term: 1, RaftLogType.Committed)])]);

        SimulatedWalSnapshot snapshot = wal.Snapshot();
        Assert.Equal([2L], snapshot.Partition(PartitionId)!.NonDurableIds);
        Assert.Equal(1, snapshot.NonDurableEntryCount);
    }

    /// <summary>The counters distinguish a durable write from one that skipped its fsync.</summary>
    [Fact]
    public void Counters_SeparateSyncWritesFromSyncOffWrites()
    {
        using SimulatedWAL wal = NewWal(out _);

        wal.Write([(PartitionId, [Entry(1, term: 1, RaftLogType.Proposed)])]);
        wal.Write([(PartitionId, [Entry(1, term: 1, RaftLogType.Committed)])], sync: false);
        wal.Write([(PartitionId, [Entry(2, term: 1, RaftLogType.Proposed)])]);

        SimulatedWalCounters counters = wal.Counters;

        Assert.Equal(3, counters.Writes);
        Assert.Equal(3, counters.EntriesWritten);
        Assert.Equal(2, counters.SyncWrites);
        Assert.Equal(1, counters.NonSyncWrites);
    }

    // ── Helpers ───────────────────────────────────────────────────────────

    /// <summary>A settable simulated clock, so a test decides where a crash falls in the window.</summary>
    private sealed class Clock
    {
        public long Milliseconds { get; set; }
    }

    private static SimulatedWAL NewWal(out Clock clock)
    {
        Clock created = new();
        clock = created;
        return new SimulatedWAL(NullLogger<IRaft>.Instance, () => created.Milliseconds);
    }

    private static void WriteRun(SimulatedWAL wal, long firstId, long lastId)
    {
        List<RaftLog> entries = [];
        for (long id = firstId; id <= lastId; id++)
            entries.Add(Entry(id, term: 1, RaftLogType.Committed));

        wal.Write([(PartitionId, entries)]);
    }

    private static RaftLog Entry(long id, long term, RaftLogType type) =>
        new() { Id = id, Term = term, Type = type };
}
