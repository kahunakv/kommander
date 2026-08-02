
using Kommander.Data;
using Microsoft.Extensions.Logging;

namespace Kommander.WAL;

/// <summary>
/// Keeps a log of all Raft operations in memory.
/// Useful for testing and debugging.
/// </summary>
public class InMemoryWAL : IWAL, IDisposable
{
    private readonly ReaderWriterLockSlim rwLock = new(LockRecursionPolicy.NoRecursion);

    private readonly Dictionary<string, string> allConfigs = new();

    private readonly Dictionary<int, SortedDictionary<long, RaftLog>> allLogs = new();

    /// <summary>
    /// Per-partition id of the highest <see cref="RaftLogType.CommittedCheckpoint"/> entry, maintained
    /// incrementally on every mutation (write / snapshot boundary / truncation) rather than scanned. This
    /// mirrors the durable backends (which persist the same value) so all three <see cref="IWAL"/> adapters
    /// behave identically — including under the conformance cross-check that compares this value against an
    /// independent full scan. A missing entry means "no checkpoint" → <see cref="GetLastCheckpoint"/>
    /// returns -1. Guarded by <see cref="rwLock"/> like <see cref="allLogs"/>.
    /// </summary>
    private readonly Dictionary<int, long> lastCheckpoints = new();

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

            if (allLogs.TryGetValue(partitionId, out SortedDictionary<long, RaftLog>? partitionLogs))
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
    {
        rwLock.EnterReadLock();
        try
        {
            List<RaftLog> result = [];

            if (allLogs.TryGetValue(partitionId, out SortedDictionary<long, RaftLog>? partitionLogs))
            {
                foreach (KeyValuePair<long, RaftLog> keyValue in partitionLogs)
                {
                    if (keyValue.Key < startLogIndex)
                        continue;
                    result.Add(keyValue.Value);
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
            if (allLogs.TryGetValue(partitionId, out SortedDictionary<long, RaftLog>? partitionLogs)
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
                foreach (RaftLog log in item.raftLogs)
                {
                    if (allLogs.TryGetValue(item.partitionId, out SortedDictionary<long, RaftLog>? partitionLogs))
                        partitionLogs[log.Id] = log;
                    else
                        allLogs.Add(item.partitionId, new() { { log.Id, log } });

                    if (log.Type == RaftLogType.CommittedCheckpoint && log.Id > batchMaxCheckpoint)
                        batchMaxCheckpoint = log.Id;
                }

                // max() so an out-of-order lower checkpoint never regresses the recorded id.
                if (batchMaxCheckpoint >= 0)
                    lastCheckpoints[item.partitionId] =
                        Math.Max(lastCheckpoints.GetValueOrDefault(item.partitionId, -1), batchMaxCheckpoint);
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
            if (allLogs.TryGetValue(partitionId, out SortedDictionary<long, RaftLog>? partitionLogs))
            {
                if (partitionLogs.Count > 0)
                    return partitionLogs.Keys.Max();
            }

            return 0;
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
            if (allLogs.TryGetValue(partitionId, out SortedDictionary<long, RaftLog>? partitionLogs))
            {
                if (partitionLogs.Count > 0)
                    return partitionLogs[partitionLogs.Keys.Max()].Term;
            }

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
    /// Recomputes the highest <see cref="RaftLogType.CommittedCheckpoint"/> id currently present for a
    /// partition (or -1 if none), used after a truncation removed the recorded checkpoint. Must be called
    /// under the write lock. Keys are sorted ascending, so the last match is the highest.
    /// </summary>
    private static long RecomputeLastCheckpoint(SortedDictionary<long, RaftLog> partitionLogs)
    {
        long checkpoint = -1;
        foreach (KeyValuePair<long, RaftLog> entry in partitionLogs)
        {
            if (entry.Value.Type == RaftLogType.CommittedCheckpoint)
                checkpoint = entry.Key;
        }

        return checkpoint;
    }

    public int CountPersistedLogs(int partitionId)
    {
        rwLock.EnterReadLock();
        try
        {
            if (!allLogs.TryGetValue(partitionId, out SortedDictionary<long, RaftLog>? partitionLogs))
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
            if (!allLogs.TryGetValue(partitionId, out SortedDictionary<long, RaftLog>? partitionLogs))
                return RaftOperationStatus.Success;

            List<long> toRemove = [];
            foreach (long id in partitionLogs.Keys)
            {
                if (id > afterLogId)
                    toRemove.Add(id);
            }

            foreach (long id in toRemove)
                partitionLogs.Remove(id);

            AdjustCheckpointAfterTruncation(partitionId, partitionLogs, afterLogId);

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
    private void AdjustCheckpointAfterTruncation(int partitionId, SortedDictionary<long, RaftLog> partitionLogs, long afterLogId)
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
            if (!allLogs.TryGetValue(partitionId, out SortedDictionary<long, RaftLog>? partitionLogs))
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

            foreach (long id in toRemove)
                partitionLogs.Remove(id);

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
            if (!allLogs.TryGetValue(partitionId, out SortedDictionary<long, RaftLog>? partitionLogs))
                return (RaftOperationStatus.Success, 0);

            List<long> toRemove = [];
            foreach (long id in partitionLogs.Keys)
            {
                if (id > afterLogId)
                    toRemove.Add(id);
            }

            foreach (long id in toRemove)
                partitionLogs.Remove(id);

            AdjustCheckpointAfterTruncation(partitionId, partitionLogs, afterLogId);

            long max = partitionLogs.Count > 0 ? partitionLogs.Keys.Max() : 0;
            return (RaftOperationStatus.Success, max);
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
            if (!allLogs.TryGetValue(partitionId, out SortedDictionary<long, RaftLog>? partitionLogs))
            {
                partitionLogs = new SortedDictionary<long, RaftLog>();
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
        int passCap = maxTotalEntries ?? compactNumberEntries;

        rwLock.EnterWriteLock();
        try
        {
            if (!allLogs.TryGetValue(partitionId, out SortedDictionary<long, RaftLog>? partitionLogs))
                return (RaftOperationStatus.Success, 0);

            List<long> toRemove = [];

            foreach (long id in partitionLogs.Keys)
            {
                if (id >= lastCheckpoint)
                    break;

                toRemove.Add(id);

                if (toRemove.Count >= passCap)
                    break;
            }

            foreach (long id in toRemove)
                partitionLogs.Remove(id);

            // No checkpoint adjustment: only entries with id < lastCheckpoint are removed, so the recorded
            // checkpoint (>= lastCheckpoint) is never affected.
            return (RaftOperationStatus.Success, toRemove.Count);
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
