
using Kommander.Data;
using Microsoft.Extensions.Logging;

namespace Kommander.WAL;

/// <summary>
/// Keeps a log of all Raft operations in memory.
/// Useful for testing and debugging.
///
/// <para>Per-partition storage is a <see cref="SortedList{TKey,TValue}"/> rather than a
/// <see cref="SortedDictionary{TKey,TValue}"/>: its indexable key list allows the range reads on
/// the follower-backfill path (<see cref="ReadLogsRange"/>) to binary-search the start position
/// and the max-id recompute to read the last key in O(1), where the tree previously forced an
/// O(n) walk of the whole retained prefix under the read lock. The trade-off is O(n) element
/// shifts for mid-list inserts and head removals — appends land at the tail (O(1) amortized) and
/// the truncate paths remove in descending key order so suffix removal shifts nothing. Head
/// compaction is the one path that would pay a shift per removed entry, so above
/// <see cref="RebuildElementMoveThreshold"/> element moves it rebuilds the partition list from the
/// retained entries instead, which touches each retained entry exactly once.</para>
/// </summary>
public class InMemoryWAL : IWAL, IDisposable
{
    /// <summary>
    /// Element moves above which <see cref="CompactLogsOlderThan"/> rebuilds a partition's storage
    /// instead of removing the prefix entry by entry.
    /// </summary>
    /// <remarks>
    /// A rebuild costs one copy per retained entry plus two fresh arrays; a prefix removal costs about
    /// <c>removed x retained</c> element moves in each of the two arrays. The crossover therefore sits
    /// only a few entries in, and this value is set well above it so that a small compaction — where the
    /// allocation would dominate — keeps the cheaper removal path. It is a deliberately conservative
    /// bound, not a measured optimum; the backend it guards is documented for tests and debugging.
    /// </remarks>
    private const long RebuildElementMoveThreshold = 4096;

    private readonly ReaderWriterLockSlim rwLock = new(LockRecursionPolicy.NoRecursion);

    private readonly Dictionary<string, string> allConfigs = new();

    private readonly Dictionary<int, SortedList<long, RaftLog>> allLogs = new();

    /// <summary>
    /// Per-partition id of the highest <see cref="RaftLogType.CommittedCheckpoint"/> entry, maintained
    /// incrementally on every mutation (write / snapshot boundary / truncation) rather than scanned. This
    /// mirrors the durable backends (which persist the same value) so all three <see cref="IWAL"/> adapters
    /// behave identically — including under the conformance cross-check that compares this value against an
    /// independent full scan. A missing entry means "no checkpoint" → <see cref="GetLastCheckpoint"/>
    /// returns -1. Guarded by <see cref="rwLock"/> like <see cref="allLogs"/>.
    /// </summary>
    private readonly Dictionary<int, long> lastCheckpoints = new();

    /// <summary>
    /// Per-partition highest log id currently present, maintained incrementally on every mutation (same
    /// pattern as <see cref="lastCheckpoints"/>). <see cref="GetMaxLog"/> and <see cref="GetCurrentTerm"/>
    /// are per-append/per-heartbeat reads; without this cache they scanned the whole retained log
    /// (<c>Keys.Max()</c> is O(n)) while holding the read lock, extending the window writers must wait
    /// for as the log grows. A missing entry means the partition holds no logs (max = 0). Removal paths
    /// only recompute when the highest removed id equals the recorded max, so the O(n) rescan is confined
    /// to the rare truncation that actually removes the tail. Guarded by <see cref="rwLock"/> like
    /// <see cref="allLogs"/>.
    /// </summary>
    private readonly Dictionary<int, long> maxLogIds = new();

    private readonly ILogger<IRaft> logger;

    public InMemoryWAL(ILogger<IRaft> logger)
    {
        this.logger = logger;
    }

    public List<RaftLog> ReadLogs(int partitionId)
    {
        rwLock.EnterReadLock();
        try
        {
            List<RaftLog> result = [];

            if (allLogs.TryGetValue(partitionId, out SortedList<long, RaftLog>? partitionLogs))
            {
                foreach (KeyValuePair<long, RaftLog> keyValue in partitionLogs)
                    result.Add(keyValue.Value);
            }

            return result;
        }
        finally
        {
            rwLock.ExitReadLock();
        }
    }

    public List<RaftLog> ReadLogsRange(int partitionId, long startLogIndex, int maxEntries = int.MaxValue)
        => ReadLogsRange(partitionId, startLogIndex, maxEntries, long.MaxValue);

    public List<RaftLog> ReadLogsRange(int partitionId, long startLogIndex, int maxEntries, long maxBytes)
    {
        rwLock.EnterReadLock();
        try
        {
            List<RaftLog> result = [];

            if (allLogs.TryGetValue(partitionId, out SortedList<long, RaftLog>? partitionLogs))
            {
                // Binary-search the first key >= startLogIndex instead of walking the whole
                // retained prefix — this is the follower-backfill read, called repeatedly for a
                // small window near the tail while holding the read lock.
                IList<long> keys = partitionLogs.Keys;
                IList<RaftLog> values = partitionLogs.Values;

                long payloadBytes = 0;

                for (int i = LowerBound(keys, startLogIndex); i < keys.Count; i++)
                {
                    long entryBytes = values[i].LogData?.Length ?? 0;

                    // At-least-one-entry rule: the budget stops a batch, never the first entry.
                    if (result.Count > 0 && payloadBytes + entryBytes > maxBytes)
                        break;

                    payloadBytes += entryBytes;
                    result.Add(values[i]);
                    if (result.Count >= maxEntries)
                        break;
                }
            }

            return result;
        }
        finally
        {
            rwLock.ExitReadLock();
        }
    }

    public long GetTermAt(int partitionId, long logIndex)
    {
        rwLock.EnterReadLock();
        try
        {
            if (allLogs.TryGetValue(partitionId, out SortedList<long, RaftLog>? partitionLogs)
                && partitionLogs.TryGetValue(logIndex, out RaftLog? log))
                return log.Term;

            return -1;
        }
        finally
        {
            rwLock.ExitReadLock();
        }
    }

    public RaftOperationStatus Write(List<(int, List<RaftLog>)> logs)
    {
        rwLock.EnterWriteLock();
        try
        {
            foreach ((int partitionId, List<RaftLog> raftLogs) item in logs)
            {
                long batchMaxCheckpoint = -1;
                long batchMaxId = 0;
                foreach (RaftLog log in item.raftLogs)
                {
                    if (allLogs.TryGetValue(item.partitionId, out SortedList<long, RaftLog>? partitionLogs))
                        partitionLogs[log.Id] = log;
                    else
                        allLogs.Add(item.partitionId, new() { { log.Id, log } });

                    if (log.Id > batchMaxId)
                        batchMaxId = log.Id;

                    if (log.Type == RaftLogType.CommittedCheckpoint && log.Id > batchMaxCheckpoint)
                        batchMaxCheckpoint = log.Id;
                }

                // Writes only ever add or overwrite entries, so the max can only grow.
                if (batchMaxId > maxLogIds.GetValueOrDefault(item.partitionId, 0))
                    maxLogIds[item.partitionId] = batchMaxId;

                // max() so an out-of-order lower checkpoint never regresses the recorded id. Like the
                // durable backends, the recorded id advances only when its whole prefix above the
                // current floor is present — a checkpoint row landing over a replication gap must not
                // certify entries this node never held (the floor drives restore seeding, the apply
                // drains' compacted-below-floor skip, and compaction).
                if (batchMaxCheckpoint >= 0)
                {
                    long currentFloor = lastCheckpoints.GetValueOrDefault(item.partitionId, -1);
                    if (batchMaxCheckpoint > currentFloor
                        && allLogs.TryGetValue(item.partitionId, out SortedList<long, RaftLog>? checkLogs)
                        && CheckpointPrefixPresent(checkLogs, currentFloor, batchMaxCheckpoint))
                        lastCheckpoints[item.partitionId] = batchMaxCheckpoint;
                }
            }

            return RaftOperationStatus.Success;
        }
        finally
        {
            rwLock.ExitWriteLock();
        }
    }

    public long GetMaxLog(int partitionId)
    {
        rwLock.EnterReadLock();
        try
        {
            // O(1) read of the incrementally-maintained value (see maxLogIds); 0 when the partition
            // holds no logs.
            return maxLogIds.GetValueOrDefault(partitionId, 0);
        }
        finally
        {
            rwLock.ExitReadLock();
        }
    }

    public long GetCurrentTerm(int partitionId)
    {
        rwLock.EnterReadLock();
        try
        {
            if (allLogs.TryGetValue(partitionId, out SortedList<long, RaftLog>? partitionLogs)
                && maxLogIds.TryGetValue(partitionId, out long maxLogId)
                && partitionLogs.TryGetValue(maxLogId, out RaftLog? log))
                return log.Term;

            return 0;
        }
        finally
        {
            rwLock.ExitReadLock();
        }
    }

    public long GetLastCheckpoint(int partitionId)
    {
        rwLock.EnterReadLock();
        try
        {
            // O(1) read of the incrementally-maintained value (see lastCheckpoints); -1 when no checkpoint
            // has been recorded for the partition.
            return lastCheckpoints.GetValueOrDefault(partitionId, -1);
        }
        finally
        {
            rwLock.ExitReadLock();
        }
    }

    /// <summary>
    /// True when every log id in <c>(floorExclusive, checkpointId]</c> exists for the partition — the
    /// contiguity certificate a last-checkpoint advance requires. A floor of <c>-1</c> anchors at id 1.
    /// Must be called under the write lock, after the batch's rows were inserted (so they count).
    /// </summary>
    private static bool CheckpointPrefixPresent(SortedList<long, RaftLog> partitionLogs, long floorExclusive, long checkpointId)
    {
        for (long id = Math.Max(floorExclusive, 0) + 1; id <= checkpointId; id++)
        {
            if (!partitionLogs.ContainsKey(id))
                return false;
        }

        return true;
    }

    /// <summary>
    /// Recomputes the highest <see cref="RaftLogType.CommittedCheckpoint"/> id currently present for a
    /// partition (or -1 if none), used after a truncation removed the recorded checkpoint. Must be called
    /// under the write lock. Scans descending so it exits at the first (= highest) match.
    /// </summary>
    private static long RecomputeLastCheckpoint(SortedList<long, RaftLog> partitionLogs)
    {
        IList<long> keys = partitionLogs.Keys;
        IList<RaftLog> values = partitionLogs.Values;

        for (int i = keys.Count - 1; i >= 0; i--)
        {
            if (values[i].Type == RaftLogType.CommittedCheckpoint)
                return keys[i];
        }

        return -1;
    }

    /// <summary>
    /// Index of the first key in <paramref name="keys"/> that is ≥ <paramref name="value"/>
    /// (or <c>keys.Count</c> when none is). <paramref name="keys"/> must be sorted ascending,
    /// which <see cref="SortedList{TKey,TValue}.Keys"/> guarantees.
    /// </summary>
    private static int LowerBound(IList<long> keys, long value)
    {
        int lo = 0;
        int hi = keys.Count;

        while (lo < hi)
        {
            int mid = (lo + hi) >> 1;
            if (keys[mid] < value)
                lo = mid + 1;
            else
                hi = mid;
        }

        return lo;
    }

    /// <summary>
    /// Refreshes <see cref="maxLogIds"/> after entries were removed from a partition. Must run under the
    /// write lock. Only when <paramref name="highestRemovedId"/> equals the recorded max can the max have
    /// changed, and the surviving max is then the SortedList's last key, read in O(1). Callers that
    /// removed nothing must not call this: id 0 is also the "no entries" sentinel, so passing it for an
    /// empty removal set would recompute a max that could not have moved.
    /// </summary>
    private void UpdateMaxAfterRemoval(int partitionId, SortedList<long, RaftLog> partitionLogs, long highestRemovedId)
    {
        if (highestRemovedId != maxLogIds.GetValueOrDefault(partitionId, 0))
            return;

        long max = partitionLogs.Count > 0 ? partitionLogs.Keys[partitionLogs.Count - 1] : 0;

        if (max == 0)
            maxLogIds.Remove(partitionId);
        else
            maxLogIds[partitionId] = max;
    }

    public int CountPersistedLogs(int partitionId)
    {
        rwLock.EnterReadLock();
        try
        {
            if (!allLogs.TryGetValue(partitionId, out SortedList<long, RaftLog>? partitionLogs))
                return 0;

            return partitionLogs.Count;
        }
        finally
        {
            rwLock.ExitReadLock();
        }
    }

    public int CountRemovableLogs(int partitionId)
    {
        return 0;
    }

    public string? GetMetaData(string key)
    {
        rwLock.EnterReadLock();
        try
        {
            return allConfigs.GetValueOrDefault(key);
        }
        finally
        {
            rwLock.ExitReadLock();
        }
    }

    public bool SetMetaData(string key, string value)
    {
        rwLock.EnterWriteLock();
        try
        {
            allConfigs[key] = value;
            return true;
        }
        finally
        {
            rwLock.ExitWriteLock();
        }
    }

    public RaftOperationStatus DeletePartitionWAL(int partitionId)
    {
        rwLock.EnterWriteLock();
        try
        {
            allLogs.Remove(partitionId);
            // Drop the recorded checkpoint too, so a reused partition id does not inherit a stale floor.
            lastCheckpoints.Remove(partitionId);
            maxLogIds.Remove(partitionId);
            return RaftOperationStatus.Success;
        }
        finally
        {
            rwLock.ExitWriteLock();
        }
    }

    public RaftOperationStatus TruncateLogsAfter(int partitionId, long afterLogId)
    {
        rwLock.EnterWriteLock();
        try
        {
            if (!allLogs.TryGetValue(partitionId, out SortedList<long, RaftLog>? partitionLogs))
                return RaftOperationStatus.Success;

            List<long> toRemove = [];
            foreach (long id in partitionLogs.Keys)
            {
                if (id > afterLogId)
                    toRemove.Add(id);
            }

            // Descending so suffix removal always deletes the current last element — a SortedList
            // shifts every element after the removed index, so ascending order would be O(m²).
            for (int i = toRemove.Count - 1; i >= 0; i--)
                partitionLogs.Remove(toRemove[i]);

            AdjustCheckpointAfterTruncation(partitionId, partitionLogs, afterLogId);

            if (toRemove.Count > 0)
                UpdateMaxAfterRemoval(partitionId, partitionLogs, toRemove[^1]);

            return RaftOperationStatus.Success;
        }
        finally
        {
            rwLock.ExitWriteLock();
        }
    }

    /// <summary>
    /// If the recorded checkpoint sits above <paramref name="afterLogId"/> (a truncation just removed it),
    /// recompute the surviving checkpoint from the remaining entries. Must run under the write lock.
    /// </summary>
    private void AdjustCheckpointAfterTruncation(int partitionId, SortedList<long, RaftLog> partitionLogs, long afterLogId)
    {
        if (lastCheckpoints.GetValueOrDefault(partitionId, -1) <= afterLogId)
            return;

        long surviving = RecomputeLastCheckpoint(partitionLogs);
        if (surviving < 0)
            lastCheckpoints.Remove(partitionId);
        else
            lastCheckpoints[partitionId] = surviving;
    }

    /// <inheritdoc/>
    public RaftOperationStatus TruncateProposedLogsAfter(int partitionId, long afterLogId)
    {
        rwLock.EnterWriteLock();
        try
        {
            if (!allLogs.TryGetValue(partitionId, out SortedList<long, RaftLog>? partitionLogs))
                return RaftOperationStatus.Success;

            List<long> toRemove = [];
            foreach (KeyValuePair<long, RaftLog> entry in partitionLogs)
            {
                if (entry.Key <= afterLogId)
                    continue;
                // Only unresolved (Proposed) entries are removable; resolved entries are quorum-agreed
                // and are load-bearing for the commit frontier, so they must survive tail cleanup.
                if (entry.Value.Type is RaftLogType.Proposed or RaftLogType.ProposedCheckpoint)
                    toRemove.Add(entry.Key);
            }

            // Descending so suffix removal always deletes the current last element — a SortedList
            // shifts every element after the removed index, so ascending order would be O(m²).
            for (int i = toRemove.Count - 1; i >= 0; i--)
                partitionLogs.Remove(toRemove[i]);

            if (toRemove.Count > 0)
                UpdateMaxAfterRemoval(partitionId, partitionLogs, toRemove[^1]);

            // No checkpoint adjustment: only unresolved entries are removed, never a CommittedCheckpoint.
            return RaftOperationStatus.Success;
        }
        finally
        {
            rwLock.ExitWriteLock();
        }
    }

    /// <inheritdoc/>
    public (RaftOperationStatus Status, long MaxLogId) TruncateLogsAfterAndGetMax(int partitionId, long afterLogId)
    {
        // Truncate and read-max under one write-lock acquisition so the pair is atomic against the
        // WAL-scheduler write path (FollowerAppend), which also takes this same write lock.
        rwLock.EnterWriteLock();
        try
        {
            if (!allLogs.TryGetValue(partitionId, out SortedList<long, RaftLog>? partitionLogs))
                return (RaftOperationStatus.Success, 0);

            List<long> toRemove = [];
            foreach (long id in partitionLogs.Keys)
            {
                if (id > afterLogId)
                    toRemove.Add(id);
            }

            // Descending so suffix removal always deletes the current last element — a SortedList
            // shifts every element after the removed index, so ascending order would be O(m²).
            for (int i = toRemove.Count - 1; i >= 0; i--)
                partitionLogs.Remove(toRemove[i]);

            AdjustCheckpointAfterTruncation(partitionId, partitionLogs, afterLogId);

            if (toRemove.Count > 0)
                UpdateMaxAfterRemoval(partitionId, partitionLogs, toRemove[^1]);

            return (RaftOperationStatus.Success, maxLogIds.GetValueOrDefault(partitionId, 0));
        }
        finally
        {
            rwLock.ExitWriteLock();
        }
    }

    /// <inheritdoc/>
    /// <remarks>
    /// Performed under a single write-lock acquisition so the conflict check, suffix truncation, and
    /// checkpoint upsert are atomic against the WAL write path. Does not call the sibling lock-taking
    /// methods (the lock is <see cref="LockRecursionPolicy.NoRecursion"/>); the logic is inlined.
    /// The <paramref name="sync"/> flag is irrelevant for this non-durable backend.
    /// </remarks>
    public (RaftOperationStatus Status, bool SuffixTruncated) InstallSnapshotBoundary(
        int partitionId, long snapshotIndex, long lastIncludedTerm, bool sync)
    {
        rwLock.EnterWriteLock();
        try
        {
            if (!allLogs.TryGetValue(partitionId, out SortedList<long, RaftLog>? partitionLogs))
            {
                partitionLogs = new SortedList<long, RaftLog>();
                allLogs[partitionId] = partitionLogs;
            }

            bool matches = partitionLogs.TryGetValue(snapshotIndex, out RaftLog? existing)
                           && existing!.Term == lastIncludedTerm;

            bool suffixTruncated = false;
            if (!matches)
            {
                List<long> toRemove = [];
                foreach (long id in partitionLogs.Keys)
                {
                    if (id > snapshotIndex)
                        toRemove.Add(id);
                }

                foreach (long id in toRemove)
                    partitionLogs.Remove(id);

                suffixTruncated = toRemove.Count > 0;
            }

            partitionLogs[snapshotIndex] = new RaftLog
            {
                Id = snapshotIndex,
                Term = lastIncludedTerm,
                Type = RaftLogType.CommittedCheckpoint,
            };

            // When the suffix was truncated, every entry (and checkpoint) above snapshotIndex is gone, so
            // the new max is snapshotIndex; otherwise keep the greater of the existing recorded id and it.
            lastCheckpoints[partitionId] = suffixTruncated
                ? snapshotIndex
                : Math.Max(lastCheckpoints.GetValueOrDefault(partitionId, -1), snapshotIndex);

            // Same reasoning for the max-id cache; the boundary entry itself guarantees the partition is
            // non-empty at snapshotIndex.
            maxLogIds[partitionId] = suffixTruncated
                ? snapshotIndex
                : Math.Max(maxLogIds.GetValueOrDefault(partitionId, 0), snapshotIndex);

            return (RaftOperationStatus.Success, suffixTruncated);
        }
        finally
        {
            rwLock.ExitWriteLock();
        }
    }

    public (RaftOperationStatus Status, int Removed) CompactLogsOlderThan(
        int partitionId,
        long lastCheckpoint,
        int compactNumberEntries,
        int? maxTotalEntries = null)
    {
        // Floor of 1 preserves this backend's previous behaviour for a non-positive cap: the old loop
        // appended an id before it tested the cap, so a cap of 0 still removed one entry. The change
        // below computes the removal count up front, which would otherwise turn that into a no-op.
        int passCap = Math.Max(1, maxTotalEntries ?? compactNumberEntries);

        rwLock.EnterWriteLock();
        try
        {
            if (!allLogs.TryGetValue(partitionId, out SortedList<long, RaftLog>? partitionLogs))
                return (RaftOperationStatus.Success, 0);

            IList<long> keys = partitionLogs.Keys;
            int total = keys.Count;

            // The removal set is always the leading run of ids below lastCheckpoint, so only its size has
            // to be discovered, and a binary search finds it without walking the prefix. The ids
            // themselves stay addressable by index until the storage changes, which is why no list of
            // removed keys is built here.
            int removeCount = Math.Min(LowerBound(keys, lastCheckpoint), passCap);

            if (removeCount == 0)
                return (RaftOperationStatus.Success, 0);

            long highestRemovedId = keys[removeCount - 1];
            int retainedCount = total - removeCount;

            if ((long)removeCount * retainedCount >= RebuildElementMoveThreshold)
            {
                // Rebuild. Every removal from the head of a SortedList shifts the whole retained tail in
                // both the key array and the value array, so removing m entries in front of r retained
                // ones moves about m x r elements per array — quadratic in the log size when a large
                // prefix is compacted away. Copying the retained entries into a pre-sized list moves each
                // of them exactly once. Keys are added in ascending order, so every Add lands at the tail
                // and shifts nothing.
                SortedList<long, RaftLog> rebuilt = new(retainedCount);
                IList<RaftLog> values = partitionLogs.Values;

                for (int i = removeCount; i < total; i++)
                    rebuilt.Add(keys[i], values[i]);

                allLogs[partitionId] = rebuilt;
                partitionLogs = rebuilt;
            }
            else
            {
                // Below the threshold the shift work is smaller than the two arrays a rebuild would
                // allocate. Descending index order removes the last element of the prefix first, which is
                // the order the previous implementation used.
                for (int i = removeCount - 1; i >= 0; i--)
                    partitionLogs.RemoveAt(i);
            }

            UpdateMaxAfterRemoval(partitionId, partitionLogs, highestRemovedId);

            // No checkpoint adjustment: only entries with id < lastCheckpoint are removed, so the recorded
            // checkpoint (>= lastCheckpoint) is never affected.
            return (RaftOperationStatus.Success, removeCount);
        }
        finally
        {
            rwLock.ExitWriteLock();
        }
    }

    public void Dispose()
    {
        GC.SuppressFinalize(this);

        rwLock.Dispose();
    }
}
