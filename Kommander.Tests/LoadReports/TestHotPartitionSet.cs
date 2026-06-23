
using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.Scheduling;
using Kommander.Time;
using Kommander.WAL;
using Kommander.WAL.IO;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.LoadReports;

/// <summary>
/// Manager-level tests for the Phase 2 hot-partition set: verifies that
/// <list type="bullet">
///   <item>a newly created partition enters the hot set,</item>
///   <item>a quiesced partition leaves the hot set (quiesce callback → MarkPartitionCool),</item>
///   <item>an un-quiesced partition re-enters the hot set (quiesce callback → MarkPartitionHot),</item>
///   <item>a removed partition is evicted from the hot set (RemovePartition),</item>
///   <item>a subsequent <see cref="RaftTimerService.TriggerCheckLeader"/> call does not throw
///         on the next normal-interval tick even when a stale entry could have been left.</item>
/// </list>
///
/// These tests exercise the real <see cref="RaftManager"/> + <see cref="RaftPartition"/> wiring,
/// unlike the <c>TestRaftTimerService</c> tests that drive the timer service through empty stub hosts
/// and would not catch bookkeeping bugs such as the stale-entry-after-remove regression.
/// </summary>
public sealed class TestHotPartitionSet
{
    private static RaftManager MakeManager()
    {
        RaftConfiguration config = new()
        {
            Host = "localhost",
            Port = 9100,
            InitialPartitions = 0,
            EnableSharedExecutorPool = true,   // Phase 2 hot-set is gated on this
            EnableQuiescence = false,           // not needed; we drive quiesce directly
            CheckLeaderInterval = TimeSpan.FromMilliseconds(250),
            UpdateNodesInterval = TimeSpan.FromMilliseconds(5000),
        };
        return new RaftManager(
            config,
            new StaticDiscovery([]),
            new InMemoryWAL(NullLogger<IRaft>.Instance),
            new InMemoryCommunication(),
            new HybridLogicalClock(),
            NullLogger<IRaft>.Instance);
    }

    private static TimeSpan Timeout => TimeSpan.FromSeconds(5);

    /// <summary>
    /// Sanity: a partition added to manager.Partitions and _hotPartitions appears in HotPartitionIds.
    /// </summary>
    [Fact]
    public async Task Partition_WhenAdded_IsInHotSet()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        using RaftManager manager = MakeManager();
        ((FairReadScheduler)manager.ReadScheduler).Start();

        using InMemoryWAL wal = new(NullLogger<IRaft>.Instance);
        RaftPartition p = new(manager, wal, partitionId: 1, 0, 0, NullLogger<IRaft>.Instance);
        await p.Executor.RestoreTask.WaitAsync(Timeout, ct);

        try
        {
            manager.Partitions[1] = p;
            manager.MarkPartitionHot(1);   // simulates what StartUserPartitions does

            Assert.Contains(1, manager.HotPartitionIds);
        }
        finally
        {
            p.Stop();
            p.Dispose();
        }
    }

    /// <summary>
    /// Quiesce callback path: SetQuiescedForTesting(true) removes the partition from
    /// the hot set via the OnQuiesceChanged callback wired in RaftPartition's constructor.
    /// </summary>
    [Fact]
    public async Task Partition_WhenQuiesced_LeavesHotSet()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        using RaftManager manager = MakeManager();
        ((FairReadScheduler)manager.ReadScheduler).Start();

        using InMemoryWAL wal = new(NullLogger<IRaft>.Instance);
        RaftPartition p = new(manager, wal, partitionId: 1, 0, 0, NullLogger<IRaft>.Instance);
        await p.Executor.RestoreTask.WaitAsync(Timeout, ct);

        try
        {
            manager.Partitions[1] = p;
            manager.MarkPartitionHot(1);

            Assert.Contains(1, manager.HotPartitionIds);

            // Drive quiesce through the executor so the callback fires under the single-owner guarantee.
            await p.Executor.Ask(new RaftRequest(RaftRequestType.SetQuiescedForTesting, quiesce: true), ct)
                .WaitAsync(Timeout, ct);

            Assert.DoesNotContain(1, manager.HotPartitionIds);
        }
        finally
        {
            p.Stop();
            p.Dispose();
        }
    }

    /// <summary>
    /// Un-quiesce callback path: after quiescing, SetQuiescedForTesting(false) re-adds the
    /// partition to the hot set.
    /// </summary>
    [Fact]
    public async Task Partition_WhenUnquiesced_ReentersHotSet()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        using RaftManager manager = MakeManager();
        ((FairReadScheduler)manager.ReadScheduler).Start();

        using InMemoryWAL wal = new(NullLogger<IRaft>.Instance);
        RaftPartition p = new(manager, wal, partitionId: 1, 0, 0, NullLogger<IRaft>.Instance);
        await p.Executor.RestoreTask.WaitAsync(Timeout, ct);

        try
        {
            manager.Partitions[1] = p;
            manager.MarkPartitionHot(1);

            await p.Executor.Ask(new RaftRequest(RaftRequestType.SetQuiescedForTesting, quiesce: true), ct)
                .WaitAsync(Timeout, ct);

            Assert.DoesNotContain(1, manager.HotPartitionIds);

            await p.Executor.Ask(new RaftRequest(RaftRequestType.SetQuiescedForTesting, quiesce: false), ct)
                .WaitAsync(Timeout, ct);

            Assert.Contains(1, manager.HotPartitionIds);
        }
        finally
        {
            p.Stop();
            p.Dispose();
        }
    }

    /// <summary>
    /// Remove eviction path: RemovePartition clears the entry from both manager.Partitions
    /// and _hotPartitions atomically so no stale reference remains.
    /// This is the regression test for Finding 1: before the fix, manager.Partitions.TryRemove
    /// was called without touching _hotPartitions, leaving a stopped executor in the hot set.
    /// </summary>
    [Fact]
    public async Task Partition_WhenRemoved_LeavesHotSet()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        using RaftManager manager = MakeManager();
        ((FairReadScheduler)manager.ReadScheduler).Start();

        using InMemoryWAL wal = new(NullLogger<IRaft>.Instance);
        RaftPartition p = new(manager, wal, partitionId: 1, 0, 0, NullLogger<IRaft>.Instance);
        await p.Executor.RestoreTask.WaitAsync(Timeout, ct);

        manager.Partitions[1] = p;
        manager.MarkPartitionHot(1);

        Assert.Contains(1, manager.HotPartitionIds);

        p.Stop();
        manager.RemovePartition(1);
        p.Dispose();

        Assert.DoesNotContain(1, manager.HotPartitionIds);
        Assert.False(manager.Partitions.ContainsKey(1));
    }

    /// <summary>
    /// End-to-end: after a partition is removed via RemovePartition, TriggerCheckLeader
    /// on the next normal-interval tick must not throw.  Before the Finding 1 fix, a stopped
    /// executor still in _hotPartitions would have caused an InvalidOperationException inside
    /// the foreach, silently aborting the sweep for all survivors that follow it.
    /// </summary>
    [Fact]
    public async Task TriggerCheckLeader_AfterPartitionRemoved_DoesNotThrow()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        using RaftManager manager = MakeManager();
        ((FairReadScheduler)manager.ReadScheduler).Start();

        using InMemoryWAL wal = new(NullLogger<IRaft>.Instance);
        RaftPartition p = new(manager, wal, partitionId: 1, 0, 0, NullLogger<IRaft>.Instance);
        await p.Executor.RestoreTask.WaitAsync(Timeout, ct);

        manager.Partitions[1] = p;
        manager.MarkPartitionHot(1);

        // Stop + remove (simulates merge / explicit remove path).
        p.Stop();
        manager.RemovePartition(1);
        p.Dispose();

        // Build a timer service in hot-set mode pointed at the manager.
        RaftConfiguration cfg = new()
        {
            Host = "localhost", Port = 9100,
            StartElectionTimeout = 50, EndElectionTimeout = 100,
            EnableSharedExecutorPool = true,
            CheckLeaderInterval  = TimeSpan.FromMilliseconds(250),
            UpdateNodesInterval  = TimeSpan.FromMilliseconds(5000),
        };
        using RaftTimerService svc = new(
            (IRaftTimerHost)manager,
            NullLogger<IRaft>.Instance,
            cfg,
            TimeSpan.Zero);

        // Must not throw even though there are no partitions in the hot set.
        Exception? ex = Record.Exception(() => svc.TriggerCheckLeader());
        Assert.Null(ex);
    }

    /// <summary>
    /// Full lifecycle: create → quiesce → un-quiesce → remove.
    /// Asserts hot-set membership at every transition and that TriggerCheckLeader
    /// is clean after removal.
    /// </summary>
    [Fact]
    public async Task FullLifecycle_CreateQuiesceUnquiesceRemove_HotSetConsistent()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        using RaftManager manager = MakeManager();
        ((FairReadScheduler)manager.ReadScheduler).Start();

        using InMemoryWAL wal = new(NullLogger<IRaft>.Instance);
        RaftPartition p = new(manager, wal, partitionId: 42, 0, 0, NullLogger<IRaft>.Instance);
        await p.Executor.RestoreTask.WaitAsync(Timeout, ct);

        try
        {
            // 1. Create → hot
            manager.Partitions[42] = p;
            manager.MarkPartitionHot(42);
            Assert.Contains(42, manager.HotPartitionIds);

            // 2. Quiesce → leaves hot set
            await p.Executor.Ask(new RaftRequest(RaftRequestType.SetQuiescedForTesting, quiesce: true), ct)
                .WaitAsync(Timeout, ct);
            Assert.DoesNotContain(42, manager.HotPartitionIds);

            // 3. Un-quiesce → re-enters hot set
            await p.Executor.Ask(new RaftRequest(RaftRequestType.SetQuiescedForTesting, quiesce: false), ct)
                .WaitAsync(Timeout, ct);
            Assert.Contains(42, manager.HotPartitionIds);

            // 4. Remove → gone from both dicts; no throw on next tick
            p.Stop();
            manager.RemovePartition(42);

            Assert.DoesNotContain(42, manager.HotPartitionIds);
            Assert.False(manager.Partitions.ContainsKey(42));

            RaftConfiguration cfg = new()
            {
                Host = "localhost", Port = 9100,
                StartElectionTimeout = 50, EndElectionTimeout = 100,
                EnableSharedExecutorPool = true,
                CheckLeaderInterval = TimeSpan.FromMilliseconds(250),
                UpdateNodesInterval = TimeSpan.FromMilliseconds(5000),
            };
            using RaftTimerService svc = new(
                (IRaftTimerHost)manager,
                NullLogger<IRaft>.Instance,
                cfg,
                TimeSpan.Zero);

            Exception? ex = Record.Exception(() => svc.TriggerCheckLeader());
            Assert.Null(ex);
        }
        finally
        {
            if (manager.Partitions.ContainsKey(42))
            {
                p.Stop();
                p.Dispose();
            }
        }
    }
}
