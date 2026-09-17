using Kommander.Data;
using Kommander.Discovery;
using Kommander.Scheduling;
using Kommander.Time;
using Kommander.WAL;
using Kommander.WAL.IO;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.Scheduler;

/// <summary>
/// A partition disposed while its WAL restore is still loading must not deliver that restore to
/// the executor it no longer has.
///
/// <para><b>The crash this pins down.</b> <see cref="RaftPartition"/> starts restore Phase 1 in its
/// constructor, on a thread-pool task. A test that built a partition and disposed it at once left
/// that task running: <c>Dispose</c> stopped the executor, ran the cleanup drain and disposed the
/// token source, and then Phase 1 finished, posted <c>RestoreLogsLoaded</c> and scheduled one more
/// drain. Phase 2 ran on a pool thread against a partition whose schedulers were gone, failed, and
/// took the fail-stop path — which called <c>Cancel()</c> on the disposed token source. The
/// <see cref="ObjectDisposedException"/> escaped the pool thread, and an unhandled exception on a
/// dedicated thread ends the process: the whole test host died with 1994 tests passed (CI run of
/// 2026-09-12).</para>
///
/// <para><b>What is asserted.</b> Three things, each a separate layer of the fix: the restore is
/// reported as cancelled rather than left pending or faulted; Phase 2 never touches the WAL; and,
/// in pool mode, the pool still serves a second partition afterwards, which is the observable
/// difference between a thread that survived and one that did not.</para>
///
/// <para>The gate blocks the read scheduler's thread inside <c>ReadLogs</c>, which is exactly where
/// a slow disk would hold it. Both executor modes are covered: the shared pool is the production
/// default, and the dedicated thread has its own late-wake path through a disposed semaphore.</para>
/// </summary>
public sealed class TestExecutorStopDuringRestore
{
    private static RaftManager Build() => new(
        new RaftConfiguration { Host = "localhost", Port = 9000, InitialPartitions = 0 },
        new StaticDiscovery([]),
        new InMemoryWAL(NullLogger<IRaft>.Instance),
        new Kommander.Communication.Memory.InMemoryCommunication(),
        new HybridLogicalClock(),
        NullLogger<IRaft>.Instance);

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task RestoreThatFinishesAfterDispose_IsCancelledAndNeverReplayed(bool sharedPool)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        using RaftManager manager = Build();
        ((FairReadScheduler)manager.ReadScheduler).Start();
        ((FairWalScheduler)manager.WalScheduler).Start();

        using GatedWal wal = new(new InMemoryWAL(NullLogger<IRaft>.Instance));
        using RaftExecutorPool? pool = sharedPool ? new RaftExecutorPool(2) : null;
        pool?.Start();

        RaftPartition partition = new(
            manager, wal, partitionId: 1, startRange: 0, endRange: 0, NullLogger<IRaft>.Instance, pool);

        Assert.True(wal.ReadStarted.Wait(TimeSpan.FromSeconds(10), ct), "Phase 1 never reached the WAL.");

        // Stop, cleanup drain, and token-source disposal all happen here, with Phase 1 still held
        // inside the gate. This is the ordering the crash needed.
        partition.Dispose();

        wal.FailEveryCallAfterRelease = true;
        wal.Release();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => partition.RestoreTask.WaitAsync(TimeSpan.FromSeconds(10), ct));

        Assert.Equal(0, wal.CallsAfterRelease);

        if (pool is null)
            return;

        // The pool thread that used to die still serves work: a second partition on the same pool
        // restores to completion.
        RaftPartition survivor = new(
            manager, new InMemoryWAL(NullLogger<IRaft>.Instance),
            partitionId: 2, startRange: 0, endRange: 0, NullLogger<IRaft>.Instance, pool);

        await survivor.RestoreTask.WaitAsync(TimeSpan.FromSeconds(10), ct);

        survivor.Dispose();
    }

    /// <summary>
    /// Holds every log read until <see cref="Release"/>, then counts (and optionally fails) every
    /// call that arrives afterwards. A call after release can only come from restore Phase 2, so
    /// the count is the proof that the replay was skipped.
    /// </summary>
    private sealed class GatedWal(IWAL inner) : IWAL
    {
        private readonly ManualResetEventSlim gate = new(false);
        private int callsAfterRelease;

        public ManualResetEventSlim ReadStarted { get; } = new(false);

        public bool FailEveryCallAfterRelease { get; set; }

        public int CallsAfterRelease => Volatile.Read(ref callsAfterRelease);

        public void Release() => gate.Set();

        private void Track()
        {
            if (!gate.IsSet)
                return;

            Interlocked.Increment(ref callsAfterRelease);

            if (FailEveryCallAfterRelease)
                throw new InvalidOperationException("The WAL is gone; the partition was disposed.");
        }

        // The held read is the Phase 1 call that was already in flight, so it is not counted:
        // only calls that start after the release can come from Phase 2.
        private void HoldRead()
        {
            ReadStarted.Set();
            gate.Wait();
        }

        public List<RaftLog> ReadLogs(int partitionId)
        {
            HoldRead();
            return inner.ReadLogs(partitionId);
        }

        public List<RaftLog> ReadLogsRange(int partitionId, long startLogIndex, int maxEntries = int.MaxValue)
        {
            HoldRead();
            return inner.ReadLogsRange(partitionId, startLogIndex, maxEntries);
        }

        public RaftOperationStatus Write(List<(int, List<RaftLog>)> logs)
        {
            Track();
            return inner.Write(logs);
        }

        public long GetMaxLog(int partitionId)
        {
            Track();
            return inner.GetMaxLog(partitionId);
        }

        public long GetCurrentTerm(int partitionId)
        {
            Track();
            return inner.GetCurrentTerm(partitionId);
        }

        public long GetLastCheckpoint(int partitionId)
        {
            Track();
            return inner.GetLastCheckpoint(partitionId);
        }

        public int CountPersistedLogs(int partitionId)
        {
            Track();
            return inner.CountPersistedLogs(partitionId);
        }

        public int CountRemovableLogs(int partitionId)
        {
            Track();
            return inner.CountRemovableLogs(partitionId);
        }

        public string? GetMetaData(string key)
        {
            Track();
            return inner.GetMetaData(key);
        }

        public bool SetMetaData(string key, string value)
        {
            Track();
            return inner.SetMetaData(key, value);
        }

        public (RaftOperationStatus Status, int Removed) CompactLogsOlderThan(
            int partitionId, long lastCheckpoint, int compactNumberEntries, int? maxTotalEntries = null)
        {
            Track();
            return inner.CompactLogsOlderThan(partitionId, lastCheckpoint, compactNumberEntries, maxTotalEntries);
        }

        public RaftOperationStatus DeletePartitionWAL(int partitionId)
        {
            Track();
            return inner.DeletePartitionWAL(partitionId);
        }

        public RaftOperationStatus TruncateLogsAfter(int partitionId, long afterLogId)
        {
            Track();
            return inner.TruncateLogsAfter(partitionId, afterLogId);
        }

        public (RaftOperationStatus Status, long MaxLogId) TruncateLogsAfterAndGetMax(int partitionId, long afterLogId)
        {
            Track();
            return inner.TruncateLogsAfterAndGetMax(partitionId, afterLogId);
        }

        public void Dispose()
        {
            gate.Set();
            gate.Dispose();
            ReadStarted.Dispose();
            inner.Dispose();
        }
    }
}
