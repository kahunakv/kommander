using Kommander.Data;
using Kommander.WAL;
using Microsoft.Extensions.Logging;

namespace Kommander.Tests.Simulation.WAL;

/// <summary>
/// A write-ahead log that stores what a real one stores and loses what a real one loses.
///
/// <para><b>Why a decorator.</b> Storage semantics are not the thing under test. This class wraps a
/// real <see cref="InMemoryWAL"/> and adds only the two behaviours a simulation needs and an
/// in-memory store cannot have: a durability window, so a crash loses a definite set of writes, and
/// injectable faults. Reimplementing the six hundred lines of the store would risk the simulation
/// disagreeing with production for reasons that have nothing to do with Raft.</para>
///
/// <para><b>The durability model.</b> A write that asks for its own fsync becomes durable
/// <see cref="WriteLatencyMilliseconds"/> of simulated time later. A write that does not ask — the
/// per-entry committed marker on the single-fsync fast path, and every metadata write, which the
/// interface documents as riding the backend's fsync cadence — becomes durable when the next sync
/// write on the same partition does. Until then a crash reverts it. That window is not an
/// approximation of production: it is what
/// <see cref="IWAL.Write(List{ValueTuple{int, List{RaftLog}}}, bool)"/> promises, and losing the
/// last vote to it is a case the interface explicitly warns about.</para>
///
/// <para><b>Latency is logical, not blocking.</b> A write returns immediately however large the
/// latency. This is deliberate. In driven mode the write-ahead log executes inline on the driving
/// thread, so a write that blocked would park the only thread able to release it. Latency therefore
/// changes what a crash loses, never when a caller resumes — which is the property the fault is for
/// and the one a driver can survive.</para>
///
/// <para><b>Deletion is durable.</b> Truncation, compaction and partition deletion take effect
/// immediately and no crash undoes them. A store that could resurrect a truncated entry would be a
/// second failure model layered on the first, and every replica-divergence check would have to
/// reason about both at once. This is a modelled limitation, stated so that a scenario does not
/// assume otherwise.</para>
///
/// <para>Every mutation is serialized on one lock so that reading the pre-write state and
/// performing the write cannot interleave. Reads go straight to the inner store, which is already
/// thread-safe.</para>
/// </summary>
public sealed class SimulatedWAL : IWAL
{
    private readonly object gate = new();
    private readonly Func<long> nowMilliseconds;
    private readonly ILogger<IRaft> logger;

    /// <summary>
    /// The real store. Replaced wholesale by a crash, which is why it is not readonly. Typed as the
    /// interface, not as <see cref="InMemoryWAL"/>: the sync-aware write is a default interface
    /// member that the in-memory store does not override, and a default member is reachable only
    /// through the interface.
    /// </summary>
    private IWAL inner;

    /// <summary>
    /// Pre-write content of every entry currently inside the fsync window, keyed by partition and id.
    /// A null value means the id did not exist before the write, so a crash removes it; a non-null
    /// value is the version a crash restores. The first capture for an id wins, because that is the
    /// version that was last durable.
    /// </summary>
    private readonly Dictionary<int, Dictionary<long, RaftLog?>> priorEntries = new();

    /// <summary>Pre-write value of every metadata key inside the window. Null means the key was absent.</summary>
    private readonly Dictionary<string, string?> priorMetadata = new();

    /// <summary>
    /// Current value of every metadata key ever written. The interface exposes metadata by key only,
    /// with no way to enumerate it, so a crash could not rebuild the store without this mirror.
    /// </summary>
    private readonly Dictionary<string, string> metadataMirror = new();

    /// <summary>Ids written without an fsync of their own, awaiting a carrier, per partition.</summary>
    private readonly Dictionary<int, HashSet<long>> ridingEntries = new();

    /// <summary>Metadata keys awaiting a carrier fsync.</summary>
    private readonly HashSet<string> ridingMetadata = [];

    /// <summary>Scheduled fsyncs that have not yet reached their simulated completion time.</summary>
    private readonly List<PendingFsync> inFlight = [];

    /// <summary>Partitions this store has ever written, so a crash knows what to rebuild.</summary>
    private readonly HashSet<int> knownPartitions = [];

    /// <summary>Compaction floors imposed by a scenario, per partition.</summary>
    private readonly Dictionary<int, long> retentionHolds = new();

    /// <summary>
    /// Partitions the out-of-space fault applies to, or null for every partition. A scenario
    /// almost always wants a scope: a fault that also hits partition 0 takes down the control plane,
    /// and the run then measures how a cluster dies rather than how Raft handles a bad disk.
    /// </summary>
    private HashSet<int>? outOfSpaceScope;

    private bool outOfSpace;
    private int failNextWrites;
    private HashSet<int>? failNextWritesScope;
    private bool disposed;

    private long writes;
    private long entriesWritten;
    private long syncWrites;
    private long nonSyncWrites;
    private long failedWrites;
    private long compactions;
    private long entriesCompacted;
    private long truncations;
    private long metadataWrites;
    private long crashes;
    private long entriesLostOnCrash;
    private long metadataKeysLostOnCrash;

    /// <summary>
    /// Builds a store over a fresh inner log.
    /// </summary>
    /// <param name="logger">Passed to the inner store.</param>
    /// <param name="nowMilliseconds">
    /// Reads simulated time. Supply the cluster's virtual clock so that the durability window is
    /// measured in the same time the rest of the run advances; a run that measured it in real time
    /// would not replay.
    /// </param>
    public SimulatedWAL(ILogger<IRaft> logger, Func<long> nowMilliseconds)
    {
        this.logger = logger;
        this.nowMilliseconds = nowMilliseconds;
        inner = new InMemoryWAL(logger);
    }

    /// <summary>
    /// Simulated milliseconds between a sync write and its durability. Zero, the default, makes the
    /// store behave like the inner one for every sync write: nothing is ever inside the window.
    /// Raise it to model a slow disk, which widens what a crash can take.
    /// </summary>
    public long WriteLatencyMilliseconds { get; set; }

    // ── Fault injection ───────────────────────────────────────────────────

    /// <summary>
    /// Refuses every write while set. This is the <c>ENOSPC</c> mode: the call returns
    /// <see cref="RaftOperationStatus.Errored"/> and stores nothing, which is what a full disk does
    /// and what the Raft write path must survive without losing a committed entry.
    /// </summary>
    /// <param name="value">Whether the disk is full.</param>
    /// <param name="partitionId">
    /// The partition to starve, or null for every partition. Prefer naming one: starving partition 0
    /// as well takes the control plane down with it.
    /// </param>
    public void SetOutOfSpace(bool value, int? partitionId = null)
    {
        lock (gate)
        {
            outOfSpace = value;
            outOfSpaceScope = value && partitionId.HasValue ? [partitionId.Value] : null;
        }
    }

    /// <summary>Refuses the next <paramref name="count"/> writes, then behaves normally again.</summary>
    /// <param name="count">How many writes to refuse.</param>
    /// <param name="partitionId">The partition to fail on, or null for every partition.</param>
    public void FailNextWrites(int count, int? partitionId = null)
    {
        lock (gate)
        {
            failNextWrites = count;
            failNextWritesScope = partitionId.HasValue ? [partitionId.Value] : null;
        }
    }

    /// <summary>
    /// Clears every injected fault, leaving the durability model untouched.
    ///
    /// <para>Teardown calls this. A node that is still refusing writes cannot commit the roster
    /// change a graceful leave waits for, so a fault left set turns every shutdown into a timeout —
    /// including the shutdown of a run that already failed for its own reasons.</para>
    /// </summary>
    public void ClearFaults()
    {
        lock (gate)
        {
            outOfSpace = false;
            outOfSpaceScope = null;
            failNextWrites = 0;
            failNextWritesScope = null;
            retentionHolds.Clear();
        }
    }

    /// <summary>
    /// Pins entries at or above <paramref name="index"/> against compaction, whatever checkpoint the
    /// caller passes. This is the retention hold a leader places while a follower is still behind:
    /// the interesting failure is what happens when the hold and the checkpoint floor disagree.
    /// </summary>
    public void SetRetentionHold(int partitionId, long index)
    {
        lock (gate)
            retentionHolds[partitionId] = index;
    }

    /// <summary>Removes a retention hold, letting compaction reach the checkpoint floor again.</summary>
    public void ClearRetentionHold(int partitionId)
    {
        lock (gate)
            retentionHolds.Remove(partitionId);
    }

    /// <summary>
    /// Loses everything inside the fsync window and comes back holding only durable state.
    ///
    /// <para>This is a crash and the restart that follows it, in one call: the caller's next read
    /// sees the store a restarted process would open. Entries written but not yet fsynced revert to
    /// their last durable version, or disappear when they had none. Deletions already applied stay
    /// applied.</para>
    /// </summary>
    public void Crash()
    {
        lock (gate)
        {
            AdvanceDurability(nowMilliseconds());

            Dictionary<int, List<RaftLog>> image = new();
            long lostEntries = 0;

            foreach (int partitionId in knownPartitions)
            {
                Dictionary<long, RaftLog> retained = new();
                foreach (RaftLog entry in inner.ReadLogsRange(partitionId, 0, int.MaxValue))
                    retained[entry.Id] = entry;

                if (priorEntries.TryGetValue(partitionId, out Dictionary<long, RaftLog?>? window))
                {
                    foreach ((long id, RaftLog? prior) in window)
                    {
                        lostEntries++;

                        if (prior is null)
                            retained.Remove(id);
                        else
                            retained[id] = prior;
                    }
                }

                if (retained.Count > 0)
                    image[partitionId] = retained.Values.OrderBy(entry => entry.Id).ToList();
            }

            Dictionary<string, string> durableMetadata = new(metadataMirror);
            foreach ((string key, string? prior) in priorMetadata)
            {
                metadataKeysLostOnCrash++;

                if (prior is null)
                    durableMetadata.Remove(key);
                else
                    durableMetadata[key] = prior;
            }

            inner.Dispose();
            inner = new InMemoryWAL(logger);

            foreach ((int partitionId, List<RaftLog> entries) in image)
                inner.Write([(partitionId, entries.Select(Copy).ToList())]);

            metadataMirror.Clear();
            foreach ((string key, string value) in durableMetadata)
            {
                inner.SetMetaData(key, value);
                metadataMirror[key] = value;
            }

            priorEntries.Clear();
            priorMetadata.Clear();
            ridingEntries.Clear();
            ridingMetadata.Clear();
            inFlight.Clear();

            crashes++;
            entriesLostOnCrash += lostEntries;
        }
    }

    /// <summary>
    /// The store's state now, for the invariant checks and for a failure report. Advances durability
    /// to the current simulated time first, so a window that has already elapsed is not reported as
    /// still open.
    /// </summary>
    public SimulatedWalSnapshot Snapshot()
    {
        lock (gate)
        {
            AdvanceDurability(nowMilliseconds());

            Dictionary<int, SimulatedWalPartitionSnapshot> partitions = new();
            int nonDurableEntries = 0;

            foreach (int partitionId in knownPartitions.OrderBy(id => id))
            {
                List<RaftLog> entries = inner.ReadLogsRange(partitionId, 0, int.MaxValue);

                List<long> nonDurableIds = priorEntries.TryGetValue(partitionId, out Dictionary<long, RaftLog?>? window)
                    ? window.Keys.OrderBy(id => id).ToList()
                    : [];

                nonDurableEntries += nonDurableIds.Count;

                if (entries.Count == 0 && nonDurableIds.Count == 0)
                    continue;

                Dictionary<RaftLogType, int> countByType = new();
                HashSet<long> present = [];

                foreach (RaftLog entry in entries)
                {
                    countByType[entry.Type] = countByType.GetValueOrDefault(entry.Type) + 1;
                    present.Add(entry.Id);
                }

                long firstId = entries.Count > 0 ? entries[0].Id : -1;
                long maxId = entries.Count > 0 ? entries[^1].Id : 0;

                List<long> missing = [];
                for (long id = firstId; firstId >= 0 && id <= maxId; id++)
                {
                    if (!present.Contains(id))
                        missing.Add(id);
                }

                partitions[partitionId] = new SimulatedWalPartitionSnapshot(
                    partitionId,
                    entries.Count,
                    firstId,
                    maxId,
                    inner.GetLastCheckpoint(partitionId),
                    countByType,
                    missing,
                    nonDurableIds);
            }

            return new SimulatedWalSnapshot(
                partitions,
                CountersLocked(),
                nonDurableEntries,
                ridingMetadata.Concat(priorMetadata.Keys).Distinct().OrderBy(key => key, StringComparer.Ordinal).ToList());
        }
    }

    /// <summary>What this store has done so far.</summary>
    public SimulatedWalCounters Counters
    {
        get
        {
            lock (gate)
                return CountersLocked();
        }
    }

    // ── Write paths ───────────────────────────────────────────────────────

    /// <inheritdoc />
    public RaftOperationStatus Write(List<(int, List<RaftLog>)> logs) => Write(logs, sync: true);

    /// <inheritdoc />
    public RaftOperationStatus Write(List<(int, List<RaftLog>)> logs, bool sync)
    {
        lock (gate)
        {
            long now = nowMilliseconds();
            AdvanceDurability(now);

            // A batch is atomic per partition, so a fault that covers any partition in the batch
            // refuses the whole call rather than storing part of it.
            if (outOfSpace && InScope(logs, outOfSpaceScope))
            {
                failedWrites++;
                return RaftOperationStatus.Errored;
            }

            if (failNextWrites > 0 && InScope(logs, failNextWritesScope))
            {
                failNextWrites--;
                failedWrites++;
                return RaftOperationStatus.Errored;
            }

            // The pre-write version of every touched id must be captured before the store changes,
            // because after the write it is gone and a crash would have nothing to restore.
            foreach ((int partitionId, List<RaftLog> entries) in logs)
            {
                knownPartitions.Add(partitionId);

                foreach (RaftLog entry in entries)
                    CapturePriorEntry(partitionId, entry.Id);
            }

            RaftOperationStatus status = inner.Write(logs, sync);
            if (status != RaftOperationStatus.Success)
                return status;

            writes++;
            if (sync)
                syncWrites++;
            else
                nonSyncWrites++;

            foreach ((int partitionId, List<RaftLog> entries) in logs)
            {
                entriesWritten += entries.Count;

                List<long> ids = entries.Select(entry => entry.Id).ToList();

                if (sync)
                    ScheduleFsync(now, partitionId, ids);
                else
                    Ride(partitionId, ids);
            }

            return status;
        }
    }

    /// <inheritdoc />
    public string? GetMetaData(string key)
    {
        lock (gate)
            return inner.GetMetaData(key);
    }

    /// <inheritdoc />
    public bool SetMetaData(string key, string value)
    {
        lock (gate)
        {
            AdvanceDurability(nowMilliseconds());

            if (!priorMetadata.ContainsKey(key))
                priorMetadata[key] = metadataMirror.GetValueOrDefault(key);

            if (!inner.SetMetaData(key, value))
                return false;

            metadataMirror[key] = value;
            ridingMetadata.Add(key);
            metadataWrites++;
            return true;
        }
    }

    /// <inheritdoc />
    public (RaftOperationStatus Status, int Removed) CompactLogsOlderThan(
        int partitionId,
        long lastCheckpoint,
        int compactNumberEntries,
        int? maxTotalEntries = null)
    {
        lock (gate)
        {
            AdvanceDurability(nowMilliseconds());

            // A retention hold pins entries at or above its index, so the effective floor is the
            // lower of the two. Clamping here rather than refusing the call models the production
            // behaviour: compaction still runs, it just stops earlier.
            long floor = retentionHolds.TryGetValue(partitionId, out long hold)
                ? Math.Min(lastCheckpoint, hold)
                : lastCheckpoint;

            (RaftOperationStatus status, int removed) =
                inner.CompactLogsOlderThan(partitionId, floor, compactNumberEntries, maxTotalEntries);

            if (status == RaftOperationStatus.Success)
            {
                compactions++;
                entriesCompacted += removed;

                if (removed > 0)
                    DropVanishedTracking(partitionId);
            }

            return (status, removed);
        }
    }

    /// <inheritdoc />
    public RaftOperationStatus DeletePartitionWAL(int partitionId)
    {
        lock (gate)
        {
            RaftOperationStatus status = inner.DeletePartitionWAL(partitionId);

            if (status == RaftOperationStatus.Success)
            {
                priorEntries.Remove(partitionId);
                ridingEntries.Remove(partitionId);

                foreach (PendingFsync pending in inFlight)
                {
                    if (pending.PartitionId == partitionId)
                        pending.LogIds.Clear();
                }
            }

            return status;
        }
    }

    /// <inheritdoc />
    public RaftOperationStatus TruncateLogsAfter(int partitionId, long afterLogId)
    {
        lock (gate)
        {
            AdvanceDurability(nowMilliseconds());

            RaftOperationStatus status = inner.TruncateLogsAfter(partitionId, afterLogId);

            if (status == RaftOperationStatus.Success)
            {
                truncations++;
                DropVanishedTracking(partitionId);
            }

            return status;
        }
    }

    /// <inheritdoc />
    public RaftOperationStatus TruncateProposedLogsAfter(int partitionId, long afterLogId)
    {
        lock (gate)
        {
            AdvanceDurability(nowMilliseconds());

            RaftOperationStatus status = inner.TruncateProposedLogsAfter(partitionId, afterLogId);

            if (status == RaftOperationStatus.Success)
            {
                truncations++;
                DropVanishedTracking(partitionId);
            }

            return status;
        }
    }

    /// <inheritdoc />
    public (RaftOperationStatus Status, long MaxLogId) TruncateLogsAfterAndGetMax(int partitionId, long afterLogId)
    {
        lock (gate)
        {
            AdvanceDurability(nowMilliseconds());

            (RaftOperationStatus status, long maxLogId) = inner.TruncateLogsAfterAndGetMax(partitionId, afterLogId);

            if (status == RaftOperationStatus.Success)
            {
                truncations++;
                DropVanishedTracking(partitionId);
            }

            return (status, maxLogId);
        }
    }

    /// <inheritdoc />
    public (RaftOperationStatus Status, bool SuffixTruncated) InstallSnapshotBoundary(
        int partitionId, long snapshotIndex, long lastIncludedTerm, bool sync)
    {
        lock (gate)
        {
            long now = nowMilliseconds();
            AdvanceDurability(now);

            knownPartitions.Add(partitionId);
            CapturePriorEntry(partitionId, snapshotIndex);

            (RaftOperationStatus status, bool suffixTruncated) =
                inner.InstallSnapshotBoundary(partitionId, snapshotIndex, lastIncludedTerm, sync);

            if (status != RaftOperationStatus.Success)
                return (status, suffixTruncated);

            writes++;
            entriesWritten++;

            if (suffixTruncated)
            {
                truncations++;
                DropVanishedTracking(partitionId);
            }

            if (sync)
            {
                syncWrites++;
                ScheduleFsync(now, partitionId, [snapshotIndex]);
            }
            else
            {
                nonSyncWrites++;
                Ride(partitionId, [snapshotIndex]);
            }

            return (status, suffixTruncated);
        }
    }

    // ── Read paths ────────────────────────────────────────────────────────

    /// <inheritdoc />
    public List<RaftLog> ReadLogs(int partitionId)
    {
        lock (gate)
            return inner.ReadLogs(partitionId);
    }

    /// <inheritdoc />
    public List<RaftLog> ReadLogsRange(int partitionId, long startLogIndex, int maxEntries = int.MaxValue)
    {
        lock (gate)
            return inner.ReadLogsRange(partitionId, startLogIndex, maxEntries);
    }

    /// <inheritdoc />
    public List<RaftLog> ReadLogsRange(int partitionId, long startLogIndex, int maxEntries, long maxBytes)
    {
        lock (gate)
            return inner.ReadLogsRange(partitionId, startLogIndex, maxEntries, maxBytes);
    }

    /// <inheritdoc />
    public long GetTermAt(int partitionId, long logIndex)
    {
        lock (gate)
            return inner.GetTermAt(partitionId, logIndex);
    }

    /// <inheritdoc />
    public long GetMaxLog(int partitionId)
    {
        lock (gate)
            return inner.GetMaxLog(partitionId);
    }

    /// <inheritdoc />
    public long GetCurrentTerm(int partitionId)
    {
        lock (gate)
            return inner.GetCurrentTerm(partitionId);
    }

    /// <inheritdoc />
    public long GetLastCheckpoint(int partitionId)
    {
        lock (gate)
            return inner.GetLastCheckpoint(partitionId);
    }

    /// <inheritdoc />
    public int CountPersistedLogs(int partitionId)
    {
        lock (gate)
            return inner.CountPersistedLogs(partitionId);
    }

    /// <inheritdoc />
    public int CountRemovableLogs(int partitionId)
    {
        lock (gate)
            return inner.CountRemovableLogs(partitionId);
    }

    /// <inheritdoc />
    public void Dispose()
    {
        lock (gate)
        {
            if (disposed)
                return;

            disposed = true;
            inner.Dispose();
        }
    }

    // ── Durability bookkeeping ────────────────────────────────────────────

    /// <summary>
    /// Records the version of an id that a crash would restore. Only the first capture counts: a
    /// second write inside the same window replaces a version that was never durable, so restoring
    /// it would invent a state the store never had.
    /// </summary>
    private void CapturePriorEntry(int partitionId, long logId)
    {
        if (!priorEntries.TryGetValue(partitionId, out Dictionary<long, RaftLog?>? window))
        {
            window = new Dictionary<long, RaftLog?>();
            priorEntries[partitionId] = window;
        }

        if (window.ContainsKey(logId))
            return;

        List<RaftLog> found = inner.ReadLogsRange(partitionId, logId, 1);
        window[logId] = found.Count > 0 && found[0].Id == logId ? Copy(found[0]) : null;
    }

    /// <summary>
    /// Schedules the fsync that makes <paramref name="logIds"/> durable, and sweeps every write that
    /// was waiting for a carrier into it. That sweep is the single-fsync contract: a sync-off batch
    /// is durable once some later durable write on its partition lands.
    /// </summary>
    private void ScheduleFsync(long now, int partitionId, List<long> logIds)
    {
        List<long> ids = new(logIds);

        if (ridingEntries.TryGetValue(partitionId, out HashSet<long>? waiting) && waiting.Count > 0)
        {
            ids.AddRange(waiting);
            waiting.Clear();
        }

        List<string> keys = ridingMetadata.ToList();
        ridingMetadata.Clear();

        inFlight.Add(new PendingFsync(now + WriteLatencyMilliseconds, partitionId, ids, keys));
    }

    /// <summary>Marks ids as written without an fsync of their own, awaiting a carrier.</summary>
    private void Ride(int partitionId, List<long> logIds)
    {
        if (!ridingEntries.TryGetValue(partitionId, out HashSet<long>? waiting))
        {
            waiting = [];
            ridingEntries[partitionId] = waiting;
        }

        foreach (long id in logIds)
            waiting.Add(id);
    }

    /// <summary>Completes every fsync whose simulated time has arrived, closing its window.</summary>
    private void AdvanceDurability(long now)
    {
        for (int index = inFlight.Count - 1; index >= 0; index--)
        {
            PendingFsync pending = inFlight[index];
            if (pending.DurableAtMilliseconds > now)
                continue;

            if (priorEntries.TryGetValue(pending.PartitionId, out Dictionary<long, RaftLog?>? window))
            {
                foreach (long id in pending.LogIds)
                {
                    // An id still waiting for a later carrier is not made durable by this fsync.
                    if (!IsRiding(pending.PartitionId, id))
                        window.Remove(id);
                }
            }

            foreach (string key in pending.MetadataKeys)
            {
                if (!ridingMetadata.Contains(key))
                    priorMetadata.Remove(key);
            }

            inFlight.RemoveAt(index);
        }
    }

    /// <summary>True when a fault with the given scope covers any partition in the batch.</summary>
    private static bool InScope(List<(int, List<RaftLog>)> logs, HashSet<int>? scope)
    {
        if (scope is null)
            return true;

        foreach ((int partitionId, List<RaftLog> _) in logs)
        {
            if (scope.Contains(partitionId))
                return true;
        }

        return false;
    }

    private bool IsRiding(int partitionId, long logId) =>
        ridingEntries.TryGetValue(partitionId, out HashSet<long>? waiting) && waiting.Contains(logId);

    /// <summary>
    /// Forgets the window for every tracked id the store no longer holds. Deletion is durable here,
    /// so a crash must not bring a truncated or compacted entry back.
    /// </summary>
    private void DropVanishedTracking(int partitionId)
    {
        if (!priorEntries.TryGetValue(partitionId, out Dictionary<long, RaftLog?>? window) || window.Count == 0)
            return;

        List<long> gone = [];
        foreach (long id in window.Keys)
        {
            List<RaftLog> found = inner.ReadLogsRange(partitionId, id, 1);
            if (found.Count == 0 || found[0].Id != id)
                gone.Add(id);
        }

        foreach (long id in gone)
        {
            window.Remove(id);

            if (ridingEntries.TryGetValue(partitionId, out HashSet<long>? waiting))
                waiting.Remove(id);

            foreach (PendingFsync pending in inFlight)
            {
                if (pending.PartitionId == partitionId)
                    pending.LogIds.Remove(id);
            }
        }
    }

    private SimulatedWalCounters CountersLocked() => new()
    {
        Writes = writes,
        EntriesWritten = entriesWritten,
        SyncWrites = syncWrites,
        NonSyncWrites = nonSyncWrites,
        FailedWrites = failedWrites,
        Compactions = compactions,
        EntriesCompacted = entriesCompacted,
        Truncations = truncations,
        MetadataWrites = metadataWrites,
        Crashes = crashes,
        EntriesLostOnCrash = entriesLostOnCrash,
        MetadataKeysLostOnCrash = metadataKeysLostOnCrash,
    };

    /// <summary>
    /// Copies an entry. The store hands out its own references, so a captured version must not be an
    /// alias of a row the library can still change.
    /// </summary>
    private static RaftLog Copy(RaftLog entry) => new()
    {
        Id = entry.Id,
        Type = entry.Type,
        Term = entry.Term,
        Time = entry.Time,
        LogType = entry.LogType,
        LogData = entry.LogData is null ? null : (byte[])entry.LogData.Clone(),
    };

    /// <summary>One scheduled fsync: what it makes durable, and when.</summary>
    private sealed record PendingFsync(
        long DurableAtMilliseconds,
        int PartitionId,
        List<long> LogIds,
        List<string> MetadataKeys);
}
