using Kommander.Data;
using Kommander.WAL;

namespace Kommander.Benchmark;

/// <summary>
/// The in-memory store behind a simulated device: a sync takes a fixed time and one sync runs at a
/// time, as on an engine with one log. It tells the scheduler so
/// (<see cref="IWAL.SerializesSyncedWrites"/>).
///
/// <para><b>Why it exists.</b> The nodes of a benchmark cluster share one process and one disk. With
/// a real engine their fsyncs share the device, so a three-node arm measures the device and not
/// three nodes that each have a disk. Here each node has its own device with a known sync cost, which
/// is the shape of a real cluster: the leader's sync and the followers' syncs run in parallel.</para>
///
/// <para><b>Group commit, as the engine does it.</b> Writers that arrive while a sync is in progress
/// wait, and the first of them then runs one sync for all of them. A scheduler that sends several
/// partitions to the device in separate calls therefore still shares a sync between the calls that
/// queued together, as on RocksDB; what it cannot share is a sync with a call that arrives a moment
/// after the sync started.</para>
///
/// <para>The sync is a sleep, so its duration has the granularity of the timer (about a millisecond)
/// and costs no CPU. A write that needs no sync waits for the sync in progress and then returns,
/// because a sync-off write issued behind a sync does that on a real engine.</para>
/// </summary>
internal sealed class SimulatedDiskWal(ILogger<IRaft> logger, TimeSpan syncDuration) : InMemoryWAL(logger), IWAL
{
    private readonly object device = new();

    /// <summary>Synced writers that have arrived, and the last of them a finished sync covers. Guarded by <see cref="device"/>.</summary>
    private long arrived, durable;

    /// <summary>True while a writer runs the sync of its group. Guarded by <see cref="device"/>.</summary>
    private bool syncing;

    bool IWAL.SerializesSyncedWrites => true;

    RaftOperationStatus IWAL.Write(List<(int, List<RaftLog>)> logs) => WriteThroughDevice(logs, sync: true);

    RaftOperationStatus IWAL.Write(List<(int, List<RaftLog>)> logs, bool sync) => WriteThroughDevice(logs, sync);

    RaftOperationStatus IWAL.Write(List<(int, List<RaftLog>)> logs, bool sync, IReadOnlyDictionary<int, long>? acknowledgedPresence) =>
        WriteThroughDevice(logs, sync);

    private RaftOperationStatus WriteThroughDevice(List<(int, List<RaftLog>)> logs, bool sync)
    {
        RaftOperationStatus status = Write(logs);

        lock (device)
        {
            if (!sync)
            {
                while (syncing)
                    Monitor.Wait(device);

                return status;
            }

            long ticket = ++arrived;

            while (durable < ticket)
            {
                if (syncing)
                {
                    Monitor.Wait(device);
                    continue;
                }

                // Lead the group: everyone who has arrived by now shares this sync.
                long covers = arrived;
                syncing = true;

                Monitor.Exit(device);
                try
                {
                    Thread.Sleep(syncDuration);
                }
                finally
                {
                    Monitor.Enter(device);
                }

                durable = covers;
                syncing = false;
                Monitor.PulseAll(device);
            }
        }

        return status;
    }
}
