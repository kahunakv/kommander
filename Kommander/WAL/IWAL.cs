
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
    /// Persists <paramref name="logs"/>. Atomic per partition. Overwrites any existing entry with the same
    /// (partitionId, id) key.
    /// </summary>
    public RaftOperationStatus Write(List<(int, List<RaftLog>)> logs);

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

    public (RaftOperationStatus Status, int Removed) CompactLogsOlderThan(int partitionId, long lastCheckpoint, int compactNumberEntries);

    /// <summary>
    /// Permanently removes all WAL entries for the given partition id.
    /// Safe to call multiple times (idempotent): calling on an already-deleted
    /// or non-existent partition returns <see cref="RaftOperationStatus.Success"/>.
    /// Must not touch any other partition's data.
    /// </summary>
    RaftOperationStatus DeletePartitionWAL(int partitionId);
}