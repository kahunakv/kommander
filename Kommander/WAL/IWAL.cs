
using Kommander.Data;

namespace Kommander.WAL;

/// <summary>
/// Write-Ahead Log abstraction for Raft log storage.
///
/// <para><b>Concurrency contract:</b></para>
/// <list type="bullet">
///   <item>All methods must be safe to call from multiple threads concurrently on the same instance.</item>
///   <item>Reads for a given partition must observe all writes that completed before the read began.</item>
///   <item><see cref="Write"/> is atomic per partition: either all logs in a single partition's batch are
///         persisted or none are. Cross-partition atomicity is adapter-specific (RocksDB provides a single
///         atomic write batch; SQLite commits per partition).</item>
///   <item>Log ordering within a partition is determined by <see cref="RaftLog.Id"/>, not insertion order.
///         Adapters must return logs sorted by id ascending on all read paths.</item>
///   <item><see cref="GetCurrentTerm"/> returns the <see cref="RaftLog.Term"/> of the log entry with the
///         highest <see cref="RaftLog.Id"/> for the partition, not the global maximum term.</item>
///   <item><see cref="GetLastCheckpoint"/> returns -1 when no committed checkpoint exists.</item>
/// </list>
///
/// <para><b>Per-adapter thread-safety notes:</b></para>
/// <list type="bullet">
///   <item><b>RocksDbWAL</b>: Concurrent reads and writes are safe. RocksDB's internal locking serializes
///         write batches. Iterators and write batches are per-call and must not be shared across threads.</item>
///   <item><b>SqliteWAL</b>: Uses a per-partition exclusive <c>lock</c> that serializes all operations —
///         reads and writes — for that partition. <see cref="Microsoft.Data.Sqlite.SqliteConnection"/>
///         wraps a single <c>sqlite3*</c> handle and is not safe for concurrent command execution, so
///         concurrent reads on the same partition are not permitted. Cross-partition operations can
///         overlap freely because each partition has its own connection and lock.</item>
///   <item><b>InMemoryWAL</b>: Uses a single global <see cref="System.Threading.ReaderWriterLockSlim"/>. Suitable
///         for tests; does not persist checkpoints or removable log counts.</item>
/// </list>
/// </summary>
public interface IWAL : IDisposable
{
    /// <summary>
    /// Reads all logs for <paramref name="partitionId"/> starting from (and including) the last committed
    /// checkpoint. Returns logs sorted by id ascending.
    /// </summary>
    public List<RaftLog> ReadLogs(int partitionId);

    /// <summary>
    /// Reads up to <paramref name="maxEntries"/> logs for <paramref name="partitionId"/> with id ≥
    /// <paramref name="startLogIndex"/>, sorted ascending. The storage engine must stop reading after
    /// <paramref name="maxEntries"/> rows so that callers far behind the leader do not read the entire
    /// remaining tail only to discard most of it in memory.
    /// </summary>
    public List<RaftLog> ReadLogsRange(int partitionId, long startLogIndex, int maxEntries = int.MaxValue);

    /// <summary>
    /// Returns the <see cref="RaftLog.Term"/> of the single entry whose id equals
    /// <paramref name="logIndex"/>, or <c>-1</c> if no entry with that exact id exists.
    /// <para>
    /// This is the scalar term-lookup surface behind the follower Log Matching Property check and the
    /// leader backfill anchor. Unlike <see cref="ReadLogsRange"/> with a limit of 1, backends must not
    /// materialize the entry's payload or a full <see cref="RaftLog"/> merely to return one term — a
    /// point key/row lookup is expected. All entry types (Proposed, Committed, RolledBack, checkpoints)
    /// are considered, since the anchor entry may still be uncommitted.
    /// </para>
    /// <para>The default implementation delegates to <see cref="ReadLogsRange"/> (limit 1) with the exact
    /// same <c>id == logIndex ? term : -1</c> semantics, so non-durable adapters and test fakes need no
    /// override; the durable backends override it with a direct lookup.</para>
    /// </summary>
    public long GetTermAt(int partitionId, long logIndex)
    {
        List<RaftLog> entries = ReadLogsRange(partitionId, logIndex, 1);
        return entries.Count > 0 && entries[0].Id == logIndex ? entries[0].Term : -1;
    }

    /// <summary>
    /// Persists <paramref name="logs"/>. Atomic per partition. Overwrites any existing entry with the same
    /// (partitionId, id) key.
    /// </summary>
    public RaftOperationStatus Write(List<(int, List<RaftLog>)> logs);

    /// <summary>
    /// Persists <paramref name="logs"/> like <see cref="Write(List{ValueTuple{int, List{RaftLog}}})"/>, but
    /// when <paramref name="sync"/> is <see langword="false"/> the batch is written <b>without forcing its
    /// own fsync</b>, so it rides the next durable (sync) write rather than paying a fsync of its own.
    /// <para>
    /// This is the storage primitive behind the single-fsync commit fast path
    /// (<see cref="RaftConfiguration.WalSingleFsyncCommit"/>): the per-entry <c>Committed</c> marker is
    /// written sync-off because the entry it commits is already quorum-durable from its propose fsync, so
    /// losing the marker on a crash is recoverable by reconstruction (it never loses acknowledged data).
    /// <c>CommittedCheckpoint</c> and any first-durability write (proposed entries) must still be written
    /// with <paramref name="sync"/> = <see langword="true"/>.
    /// </para>
    /// <para>The default implementation ignores <paramref name="sync"/> and delegates to the durable
    /// <see cref="Write(List{ValueTuple{int, List{RaftLog}}})"/>, which is correct for non-durable adapters
    /// (<see cref="InMemoryWAL"/>, test fakes) that never fsync. Durable backends override it.</para>
    /// </summary>
    public RaftOperationStatus Write(List<(int, List<RaftLog>)> logs, bool sync) => Write(logs);

    /// <summary>Returns the largest log id persisted for <paramref name="partitionId"/>, or 0 if empty.</summary>
    public long GetMaxLog(int partitionId);

    /// <summary>
    /// Returns the <see cref="RaftLog.Term"/> of the log entry with the highest id for
    /// <paramref name="partitionId"/>, or 0 if no logs exist.
    /// </summary>
    public long GetCurrentTerm(int partitionId);

    /// <summary>Returns the id of the last <see cref="RaftLogType.CommittedCheckpoint"/> log, or -1 if none.</summary>
    public long GetLastCheckpoint(int partitionId);

    /// <summary>
    /// Returns the total number of persisted log rows for the partition.
    /// Unlike <see cref="ReadLogsRange"/>, this is not capped by a range limit.
    /// </summary>
    public int CountPersistedLogs(int partitionId);

    /// <summary>
    /// Returns the number of persisted log rows with id strictly below the partition's
    /// last committed checkpoint. Returns 0 when no checkpoint exists.
    /// </summary>
    public int CountRemovableLogs(int partitionId);

    public string? GetMetaData(string key);

    public bool SetMetaData(string key, string value);

    /// <param name="compactNumberEntries">Maximum entries removed per internal delete batch.</param>
    /// <param name="maxTotalEntries">
    /// When set, removes up to this many entries in one storage transaction by issuing
    /// multiple internal batches of <paramref name="compactNumberEntries"/>. When <see langword="null"/>,
    /// only one batch is removed.
    /// </param>
    public (RaftOperationStatus Status, int Removed) CompactLogsOlderThan(
        int partitionId,
        long lastCheckpoint,
        int compactNumberEntries,
        int? maxTotalEntries = null);

    /// <summary>
    /// Permanently removes all WAL entries for the given partition id.
    /// Safe to call multiple times (idempotent): calling on an already-deleted
    /// or non-existent partition returns <see cref="RaftOperationStatus.Success"/>.
    /// Must not touch any other partition's data.
    /// </summary>
    RaftOperationStatus DeletePartitionWAL(int partitionId);

    /// <summary>
    /// Deletes all log entries for <paramref name="partitionId"/> whose id is strictly greater than
    /// <paramref name="afterLogId"/>. Called by the follower path after a successful append to discard
    /// any conflicting tail left over from a previous term's replication.
    /// Idempotent: no-op when no entries exist beyond <paramref name="afterLogId"/>.
    /// </summary>
    RaftOperationStatus TruncateLogsAfter(int partitionId, long afterLogId);

    /// <summary>
    /// Atomically deletes all log entries for <paramref name="partitionId"/> whose id is strictly
    /// greater than <paramref name="afterLogId"/> and returns the partition's max log id as observed
    /// <b>after</b> the deletion.
    /// <para>
    /// The delete and the max-log read are performed under a <b>single</b> acquisition of the backend's
    /// per-partition (or per-shard) write guard, so no concurrent write to the same partition can be
    /// interleaved between them. This is what makes the hole-repair frontier reported to the leader
    /// exact: a separate <c>TruncateLogsAfter</c> followed by <c>GetMaxLog</c> would expose a window in
    /// which a concurrent <c>FollowerAppend</c> (running on the WAL scheduler, a different thread pool
    /// from the read scheduler that drives repair) re-grows the tail and corrupts the reported max.
    /// </para>
    /// <para>Returns <c>(Success, 0)</c> when the partition has no entries after truncation.</para>
    /// </summary>
    (RaftOperationStatus Status, long MaxLogId) TruncateLogsAfterAndGetMax(int partitionId, long afterLogId);
}