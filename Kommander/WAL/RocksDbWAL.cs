
using System.Buffers;
using System.Text;
using Google.Protobuf;
using Kommander.Data;
using Kommander.Logging;
using Kommander.WAL.Protos;
using RocksDbSharp;
using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace Kommander.WAL;

/// <summary>
/// The RocksDbWAL class provides an implementation of a Write-Ahead Log (WAL) using RocksDB as the storage backend.
/// It supports functionality for reading, writing, compacting, and managing log metadata in a partitioned setup.
/// </summary>
public class RocksDbWAL : IWAL, IDisposable
{
    /// <summary>
    /// A static, reusable instance of <see cref="WriteOptions"/> configured to enable synchronous operations.
    /// This ensures data integrity by guaranteeing that write operations are fully persisted to disk
    /// before returning, at the potential cost of reduced write performance.
    /// </summary>
    private static readonly WriteOptions SynchronousWriteOptions = new WriteOptions().SetSync(true);

    private static readonly WriteOptions NonSynchronousWriteOptions = new WriteOptions().SetSync(false);

    /// <summary>
    /// Specifies the version of the format used for the RocksDb Write-Ahead Log (WAL) implementation.
    /// This constant is used to ensure compatibility by identifying the version of the metadata structure
    /// and log format maintained within the storage system.
    /// </summary>
    private const string FormatVersion = "2.0.0";

    /// <summary>
    /// Specifies the maximum number of shards supported by the Write-Ahead Log (WAL) implementation,
    /// representing the logical partitions used for segregating and managing log entries efficiently.
    ///
    /// Each shard represent a column family in RocksDB, allowing for concurrent access and
    /// concurrent compaction of the memtables in each shard.
    /// </summary>
    private const int MaxShards = 8;

    /// <summary>
    /// Specifies the fixed width, in bytes, used for encoding and storing
    /// unique identifiers within the Write-Ahead Log (WAL).
    /// This value is utilized to ensure consistent byte representation of IDs
    /// across various operations, such as reading, writing, and seeking log entries.
    ///
    /// This shouldn't be changed without a proper migration plan, as it would break the ordering of the logs.
    /// </summary>
    private const int IdWidth = 20;

    /// <summary>
    /// Specifies the fixed width, in bytes, used for encoding partition IDs into
    /// RocksDB keys. The width keeps all keys for a partition contiguous and
    /// lexicographically sorted by log ID within that partition.
    /// </summary>
    private const int PartitionIdWidth = 10;

    private const int LogKeyWidth = PartitionIdWidth + 1 + IdWidth;

    private const byte LogKeySeparator = (byte)':';

    private const byte PartitionUpperBoundSeparator = (byte)';';

    /// <summary>
    /// Represents a RocksDB instance used as the underlying storage engine
    /// for write-ahead logging (WAL). Provides efficient operations for
    /// persisting and retrieving logs with support for multiple column
    /// families and partitioning.
    /// </summary>
    private readonly RocksDb db;

    private readonly string path;
    
    private readonly string revision;
    
    private readonly ConcurrentDictionary<int, Lazy<ColumnFamilyHandle>> families = new();
    
    private readonly ILogger<IRaft> logger;

    private readonly WriteOptions writeOptions;

    private readonly bool syncWrites;

    internal bool SyncWritesEnabled => syncWrites;

    /// <summary>For tests: number of <c>db.Write</c> calls issued by the last compaction call.</summary>
    internal int LastCompactionWriteCount { get; private set; }
    
    /// <summary>
    /// Opens a RocksDB WAL at <paramref name="path"/>/<paramref name="revision"/>.
    ///
    /// <para>
    /// When <paramref name="sharedResources"/> is non-null, the block cache and WriteBufferManager it
    /// carries are applied to this database before <c>RocksDb.Open</c> — the block cache to every column
    /// family, the WBM to the <see cref="DbOptions"/> — so this WAL shares a unified memory budget with
    /// any other database that received the same bundle. The bundle is <em>borrowed</em>: this constructor
    /// does not take ownership and must not dispose it; the caller owns the bundle's lifetime.
    /// </para>
    ///
    /// <para>
    /// When <paramref name="sharedResources"/> is null, behavior is byte-for-byte identical to the
    /// no-arg default: no block cache is configured and RocksDB's built-in defaults apply.
    /// </para>
    /// </summary>
    public RocksDbWAL(
        string path,
        string revision,
        ILogger<IRaft> logger,
        bool syncWrites = true,
        RocksDbSharedResources? sharedResources = null)
    {
        this.path = path;
        this.revision = revision;
        this.logger = logger;
        this.syncWrites = syncWrites;
        this.writeOptions = syncWrites ? SynchronousWriteOptions : NonSynchronousWriteOptions;

        DbOptions dbOptions = new DbOptions()
            .SetCreateIfMissing(true)
            .SetCreateMissingColumnFamilies(true)
            .SetAllowConcurrentMemtableWrite(true)
            .SetWalRecoveryMode(Recovery.AbsoluteConsistency);

        // When sharing resources, apply the WBM to the DbOptions before opening. The block cache is
        // applied per-CF below. Both must be set before RocksDb.Open — they cannot be changed afterward.
        if (sharedResources is not null)
            Native.Instance.rocksdb_options_set_write_buffer_manager(
                dbOptions.Handle, sharedResources.WriteBufferManagerHandle);

        // One shared BlockBasedTableOptions referencing the shared block cache, applied to every CF so
        // none are excluded from the shared budget. When sharedResources is null, CFs keep defaults.
        //
        // Write-buffer sizing when sharing: this WAL has ~10 CFs. With the WBM bounding total memtable
        // memory across all sharing databases, per-CF write_buffer_size × max_write_buffer_number × CF
        // count should be a modest fraction of memtableBudgetBytes to leave headroom for the host DB.
        // The RocksDB defaults (64 MB write_buffer_size, 2 max_write_buffer_number) give ~1.28 GB for
        // 10 CFs — far above any typical WBM budget. Hosts sharing a WBM should configure lower values
        // on both this WAL and their own DB, or accept frequent cross-CF/cross-DB flush coupling.
        BlockBasedTableOptions? sharedBbto = sharedResources is not null
            ? new BlockBasedTableOptions().SetBlockCache(sharedResources.BlockCache)
            : null;

        ColumnFamilies columnFamilies = new()
        {
            { "default", ApplyCfOptions(new(), sharedBbto) },
            { "metadata", ApplyCfOptions(new(), sharedBbto) }
        };

        for (int i = 0; i < MaxShards; i++)
            columnFamilies.Add("shard" + i, ApplyCfOptions(new(), sharedBbto));

        string completePath = $"{path}/{revision}";
        
        bool firstTime = !Directory.Exists(completePath);
        
        db = RocksDb.Open(dbOptions, completePath, columnFamilies);
        
        if (firstTime)
            SetMetaData("version", FormatVersion);
        else
        {
            string? currentVersion = GetMetaData("version");

            if (!string.Equals(currentVersion, FormatVersion, StringComparison.Ordinal))
            {
                db.Dispose();
                throw new InvalidOperationException(
                    $"RocksDB WAL format version '{currentVersion ?? "<missing>"}' is not compatible with '{FormatVersion}'. " +
                    "Create a fresh WAL directory or migrate the existing data before opening it."
                );
            }
        }
    }

    /// <summary>
    /// Retrieves the column family handle for the specified partition ID from the internal collection,
    /// creating and storing it lazily if it does not already exist.
    /// </summary>
    /// <param name="partitionId">
    /// The ID of the partition for which the column family handle is to be retrieved.
    /// </param>
    /// <returns>
    /// The instance of <see cref="ColumnFamilyHandle"/> corresponding to the specified partition.
    /// </returns>
    private ColumnFamilyHandle GetColumnFamily(int partitionId)
    {
        Lazy<ColumnFamilyHandle> lazy = families.GetOrAdd(partitionId, GetColumnFamilyHandle);
        return lazy.Value;
    }

    /// <summary>
    /// Retrieves a lazy-loaded column family handle for the specified partition ID.
    /// </summary>
    /// <param name="arg">
    /// The ID of the partition for which the column family handle is to be retrieved.
    /// </param>
    /// <returns>
    /// A lazy-loaded instance of <see cref="ColumnFamilyHandle"/> corresponding to the specified partition.
    /// </returns>
    private Lazy<ColumnFamilyHandle> GetColumnFamilyHandle(int arg)
    {
        return new(() =>
        {
            int shardId = arg % MaxShards;
            return db.GetColumnFamily("shard" + shardId);
        });
    }

    /// <summary>
    /// Reads all logs from the Write-Ahead Log (WAL) for the specified partition.
    /// </summary>
    /// <param name="partitionId">
    /// The ID of the partition from which to read the logs.
    /// </param>
    /// <returns>
    /// A list of <see cref="RaftLog"/> instances representing the logs read from the specified partition.
    /// </returns>
    public List<RaftLog> ReadLogs(int partitionId)
    {
        List<RaftLog> result = [];

        ColumnFamilyHandle columnFamilyHandle = GetColumnFamily(partitionId);
        
        long lastCheckpoint = GetLastCheckpointInternal(partitionId, columnFamilyHandle);
        
        //Console.WriteLine($"Last checkpoint {partitionId} {lastCheckpoint}");

        using Iterator? iterator = db.NewIterator(cf: columnFamilyHandle);
        
        long startLogId = Math.Max(0, lastCheckpoint);
        Span<byte> seekKey = stackalloc byte[LogKeyWidth];
        BuildLogKey(seekKey, partitionId, startLogId);
        iterator.Seek(seekKey);

        while (iterator.Valid())
        {
            if (!KeyBelongsToPartition(iterator.GetKeySpan(), partitionId))
                break;

            ReadLogFromWire(iterator.GetValueSpan(), out RaftLogWireView view);

            if (view.Partition != partitionId || view.Id < lastCheckpoint)
            {
                iterator.Next();
                continue;
            }

            //if (partitionId == 1)
            //    Console.WriteLine("{0} {1}", iterator.StringKey(), view.Id);

            result.Add(ToRaftLog(view));

            iterator.Next();
        }

        return result;
    }

    /// <summary>
    /// Reads up to <paramref name="maxEntries"/> logs for <paramref name="partitionId"/> with id ≥
    /// <paramref name="startLogIndex"/>, sorted ascending. The iterator stops advancing once
    /// <paramref name="maxEntries"/> rows have been read so large tails are not scanned in full.
    /// </summary>
    public List<RaftLog> ReadLogsRange(int partitionId, long startLogIndex, int maxEntries = int.MaxValue)
    {
        // Presize when the caller supplied a sane bound (the common case: a bounded backfill batch),
        // so a full batch does not walk the doubling sequence. An unbounded/absurd limit falls back
        // to the default growth rather than reserving for entries that may not exist.
        List<RaftLog> result = maxEntries is > 0 and <= 1024 ? new(maxEntries) : [];

        ColumnFamilyHandle columnFamilyHandle = GetColumnFamily(partitionId);

        using Iterator? iterator = db.NewIterator(cf: columnFamilyHandle);

        Span<byte> seekKey = stackalloc byte[LogKeyWidth];
        BuildLogKey(seekKey, partitionId, Math.Max(0, startLogIndex));
        iterator.Seek(seekKey);

        while (iterator.Valid())
        {
            if (!KeyBelongsToPartition(iterator.GetKeySpan(), partitionId))
                break;

            ReadLogFromWire(iterator.GetValueSpan(), out RaftLogWireView view);

            if (view.Partition != partitionId || view.Id < startLogIndex)
            {
                iterator.Next();
                continue;
            }

            result.Add(ToRaftLog(view));

            iterator.Next();

            if (result.Count >= maxEntries)
                break;
        }

        return result;
    }

    /// <summary>
    /// Writes a set of logs to the Write-Ahead Log (WAL) for the specified partitions.
    /// </summary>
    /// <param name="logs">
    /// A list of tuples where each tuple contains a partition ID (int) and a list of
    /// <see cref="RaftLog"/> instances to be written to that partition.
    /// </param>
    /// <returns>
    /// Returns a <see cref="RaftOperationStatus"/> indicating the status of the write operation:
    /// <see cref="RaftOperationStatus.Success"/> if the operation succeeds,
    /// <see cref="RaftOperationStatus.Errored"/> if there is an issue during the operation.
    /// </returns>
    public RaftOperationStatus Write(List<(int, List<RaftLog>)> logs) => Write(logs, sync: true);

    /// <inheritdoc/>
    /// <remarks>
    /// When <paramref name="sync"/> is <see langword="false"/> the batch is written with
    /// <see cref="NonSynchronousWriteOptions"/> (<c>SetSync(false)</c>), so RocksDB appends it to the WAL
    /// without an fsync; the next <c>SetSync(true)</c> write flushes the shared WAL up to and including it,
    /// making prior sync-off writes durable. When this instance was constructed with <c>syncWrites=false</c>
    /// every write is already non-sync, so <paramref name="sync"/> has no additional effect.
    /// </remarks>
    public RaftOperationStatus Write(List<(int, List<RaftLog>)> logs, bool sync)
    {
        WriteOptions effectiveOptions = sync ? writeOptions : NonSynchronousWriteOptions;

        try
        {
            if (logs is [{ Item2.Count: 1 } _]) // fast path
            {
                RaftLog log = logs[0].Item2[0];
                int partitionId = logs[0].Item1;
                
                RaftLogMessage message = new()
                {
                    Partition = partitionId,
                    Id = log.Id,
                    Term = log.Term,
                    Type = (int)log.Type,
                    TimeNode = log.Time.N,
                    TimePhysical = log.Time.L,
                    TimeCounter = log.Time.C
                };

                if (log.LogType != null)
                    message.LogType = log.LogType;

                if (log.LogData != null)
                    message.Log = UnsafeByteOperations.UnsafeWrap(log.LogData);

                Span<byte> buffer = stackalloc byte[LogKeyWidth];
                BuildLogKey(buffer, partitionId, message.Id);
                
                ColumnFamilyHandle columnFamilyHandle = GetColumnFamily(partitionId);

                // RocksDB copies the value synchronously inside Put, so the rented/stack buffer is safe to
                // release as soon as the call returns. The stackalloc is evaluated only when nothing was
                // rented (?? short-circuits), so small and large messages never both reserve a buffer.
                int size = message.CalculateSize();
                byte[]? rented = size > StackallocThreshold ? ArrayPool<byte>.Shared.Rent(size) : null;
                Span<byte> valueBuffer = (rented ?? stackalloc byte[StackallocThreshold])[..size];
                try
                {
                    SerializeInto(message, valueBuffer);
                    db.Put(buffer, valueBuffer, columnFamilyHandle, effectiveOptions);
                }
                finally
                {
                    if (rented is not null)
                        ArrayPool<byte>.Shared.Return(rented);
                }

                return RaftOperationStatus.Success;
            }
            
            Dictionary<ColumnFamilyHandle, Dictionary<int, List<RaftLog>>> plan = new();
            
            foreach ((int partitionId, List<RaftLog> raftLog) log in logs)
            {
                ColumnFamilyHandle columnFamilyHandle = GetColumnFamily(log.partitionId);

                if (plan.TryGetValue(columnFamilyHandle, out Dictionary<int, List<RaftLog>>? raftLogsPerPartition))
                {
                    if (raftLogsPerPartition.TryGetValue(log.partitionId, out List<RaftLog>? raftLogs))
                        raftLogs.AddRange(log.raftLog);
                    else
                    {
                        raftLogs = new(log.raftLog.Count);
                        raftLogs.AddRange(log.raftLog);
                        raftLogsPerPartition.Add(log.partitionId, raftLogs);
                    }
                }
                else
                {
                    List<RaftLog> raftLogs = new(log.raftLog.Count);
                    raftLogs.AddRange(log.raftLog);
                    raftLogsPerPartition = new() { { log.partitionId, raftLogs } };
                    plan.Add(columnFamilyHandle, raftLogsPerPartition);
                }
            }
            
            using WriteBatch writeBatch = new();
            
            foreach ((ColumnFamilyHandle key, Dictionary<int, List<RaftLog>> raftLogs) in plan)
            {
                //int count = 0;

                foreach (KeyValuePair<int, List<RaftLog>> kv in raftLogs)
                {
                    foreach (RaftLog log in kv.Value)
                    {                                               
                        RaftLogMessage message = new()
                        {
                            Partition = kv.Key,
                            Id = log.Id,
                            Term = log.Term,
                            Type = (int)log.Type,
                            TimeNode = log.Time.N,
                            TimePhysical = log.Time.L,
                            TimeCounter = log.Time.C
                        };

                        if (log.LogType != null)
                            message.LogType = log.LogType;

                        if (log.LogData != null)
                            message.Log = UnsafeByteOperations.UnsafeWrap(log.LogData);

                        PutToBatch(writeBatch, message, key);

                        //count++;
                    }
                }

                //Console.WriteLine("Batch of {0}", count);
            }
            
            db.Write(writeBatch, effectiveOptions);

            return RaftOperationStatus.Success;
        } 
        catch (Exception ex)
        {
            logger.LogError("Error during write: {Message}\n{StackTrace}", ex.Message, ex.StackTrace);
                    
            return RaftOperationStatus.Errored;
        }
    }

    /// <summary>
    /// Adds a serialized RaftLogMessage to the write batch within a specified column family.
    /// </summary>
    /// <param name="writeBatch">
    /// The write batch object used to batch database operations.
    /// </param>
    /// <param name="message">
    /// The RaftLogMessage instance containing the details to be added to the batch.
    /// </param>
    /// <param name="columnFamilyHandle">
    /// The handle to the column family in which the record should be stored.
    /// </param>
    private static void PutToBatch(WriteBatch writeBatch, RaftLogMessage message, ColumnFamilyHandle columnFamilyHandle)
    {
        Span<byte> buffer = stackalloc byte[LogKeyWidth];
        BuildLogKey(buffer, message.Partition, message.Id);

        // WriteBatch.Put copies the value synchronously, so the rented/stack buffer is safe to release
        // as soon as the call returns. The stackalloc is evaluated only when nothing was rented
        // (?? short-circuits), so small and large messages never both reserve a buffer.
        int size = message.CalculateSize();
        byte[]? rented = size > StackallocThreshold ? ArrayPool<byte>.Shared.Rent(size) : null;
        Span<byte> valueBuffer = (rented ?? stackalloc byte[StackallocThreshold])[..size];
        try
        {
            SerializeInto(message, valueBuffer);
            writeBatch.Put(buffer, valueBuffer, cf: columnFamilyHandle);
        }
        finally
        {
            if (rented is not null)
                ArrayPool<byte>.Shared.Return(rented);
        }
    }

    /// <summary>
    /// Retrieves the maximum log ID from the Write-Ahead Log (WAL) for the specified partition.
    /// </summary>
    /// <param name="partitionId">
    /// The ID of the partition from which to retrieve the maximum log ID.
    /// </param>
    /// <returns>
    /// The ID of the maximum log stored in the specified partition. Returns 0 if no logs exist for the partition.
    /// </returns>
    public long GetMaxLog(int partitionId)
    {
        ColumnFamilyHandle columnFamilyHandle = GetColumnFamily(partitionId);
        
        using Iterator? iterator = db.NewIterator(cf: columnFamilyHandle);
        SeekToLastPartitionKey(iterator, partitionId);

        // The key alone carries the id, so the last key in the partition answers this without reading
        // (and copying) a single value.
        if (iterator.Valid() && KeyBelongsToPartition(iterator.GetKeySpan(), partitionId))
            return ParseLogIdFromKey(iterator.GetKeySpan());

        return 0;
    }

    /// <summary>
    /// Retrieves the current term of the specified partition by examining the last log entry.
    /// </summary>
    /// <param name="partitionId">
    /// The ID of the partition for which to retrieve the current term.
    /// </param>
    /// <returns>
    /// The current term of the specified partition, or 0 if no logs are found.
    /// </returns>
    /// <summary>
    /// Point lookup of a single entry's term by exact id. Builds the fixed-width log key and issues a
    /// direct <c>db.Get</c> instead of creating an iterator, then parses only the protobuf message to
    /// read <c>Term</c> — it does not build a <see cref="RaftLog"/> or copy the entry's payload
    /// <c>LogData</c>. Returns -1 when the key is absent (or, defensively, when the stored message does
    /// not match the requested partition/id). A further optimization — a protobuf wire scan that reads
    /// the <c>term</c> field and skips the payload <c>ByteString</c> entirely — is left as a
    /// benchmark-gated follow-up.
    /// </summary>
    public long GetTermAt(int partitionId, long logIndex)
    {
        ColumnFamilyHandle columnFamilyHandle = GetColumnFamily(partitionId);

        byte[] key = new byte[LogKeyWidth];
        BuildLogKey(key, partitionId, logIndex);

        // Point lookup on the exact key: a hit is unambiguously the entry for (partitionId, logIndex),
        // so no field re-validation is needed. `value` is the full serialized message (payload included)
        // — that read cost is inherent to RocksDB storing the payload inline — but ReadTermFromWire
        // scans only far enough to read the `term` field (field 3), which protobuf serializes before the
        // `log` payload (field 6), so the multi-megabyte payload ByteString is never materialized.
        byte[]? value = db.Get(key, cf: columnFamilyHandle);
        return value is null ? -1 : ReadTermFromWire(value);
    }

    /// <summary>
    /// Reads only the <c>term</c> field (field 3, varint) from a serialized <c>RaftLogMessage</c>,
    /// skipping every other field — crucially the length-delimited <c>log</c> payload (field 6) —
    /// without allocating. Returns the proto3 default (<c>0</c>) when the field is absent, which is the
    /// correct value for an entry whose term is 0 (proto3 omits default-valued fields on the wire). The
    /// caller has already established, via the exact-key <c>db.Get</c> hit, that the entry exists.
    /// </summary>
    private static long ReadTermFromWire(ReadOnlySpan<byte> data)
    {
        int pos = 0;
        while (pos < data.Length)
        {
            ulong tag = ReadVarint(data, ref pos);
            int fieldNumber = (int)(tag >> 3);
            int wireType = (int)(tag & 0x7);

            if (fieldNumber == 3 && wireType == 0) // term
                return (long)ReadVarint(data, ref pos);

            switch (wireType)
            {
                case 0: ReadVarint(data, ref pos); break;                             // varint
                case 1: pos += 8; break;                                              // 64-bit
                case 2: pos += (int)ReadVarint(data, ref pos); break;                 // length-delimited (skip payload)
                case 5: pos += 4; break;                                              // 32-bit
                default: return 0;                                                    // unknown wire type — treat as default
            }
        }

        return 0; // term field omitted => proto3 default
    }

    /// <summary>
    /// Reads the scalar "header" fields of a serialized <c>RaftLogMessage</c> — <c>partition</c> (1),
    /// <c>id</c> (2), <c>term</c> (3) and <c>type</c> (4) — without allocating and without materializing
    /// the length-delimited <c>logType</c> (5) / <c>log</c> (6) fields, which are skipped by advancing
    /// past their declared length. Scan cost is therefore bounded by the number of fields, not by the
    /// payload size, so a backwards scan over a partition with multi-megabyte entries no longer allocates
    /// (and copies) one <c>ByteString</c> per entry visited.
    ///
    /// Absent fields yield the proto3 default (<c>0</c>), which is the correct value for an entry that
    /// genuinely stored 0. Use this only where the payload is not needed; callers that must return a
    /// <see cref="RaftLog"/> go through <see cref="ReadLogFromWire"/>, which decodes every field.
    /// </summary>
    private static void ReadHeaderFromWire(ReadOnlySpan<byte> data, out int partition, out long id, out long term, out int type)
    {
        partition = 0;
        id = 0;
        term = 0;
        type = 0;

        int pos = 0;
        while (pos < data.Length)
        {
            ulong tag = ReadVarint(data, ref pos);
            int fieldNumber = (int)(tag >> 3);
            int wireType = (int)(tag & 0x7);

            if (wireType == 0)
            {
                long value = (long)ReadVarint(data, ref pos);

                switch (fieldNumber)
                {
                    case 1: partition = (int)value; break;
                    case 2: id = value; break;
                    case 3: term = value; break;
                    case 4: type = (int)value; break;
                }

                continue;
            }

            switch (wireType)
            {
                case 1: pos += 8; break;                                              // 64-bit
                case 2: pos += (int)ReadVarint(data, ref pos); break;                 // length-delimited (skip payload)
                case 5: pos += 4; break;                                              // 32-bit
                default: return;                                                      // unknown wire type — stop, keep defaults
            }
        }
    }

    /// <summary>
    /// A borrowed, non-owning view over a serialized <c>RaftLogMessage</c>. The two length-delimited
    /// fields point straight into the caller's buffer (a live RocksDB iterator value), so a view is only
    /// valid until that buffer moves — it must be converted with <see cref="ToRaftLog"/>, which performs
    /// the single unavoidable payload copy, before <c>iterator.Next()</c> is called.
    /// </summary>
    private ref struct RaftLogWireView
    {
        public int Partition;
        public long Id;
        public long Term;
        public int Type;
        public ReadOnlySpan<byte> LogType;
        public ReadOnlySpan<byte> Log;
        public bool HasLog;
        public int TimeNode;
        public long TimePhysical;
        public uint TimeCounter;
    }

    /// <summary>
    /// Decodes every field of a serialized <c>RaftLogMessage</c> into a <see cref="RaftLogWireView"/>
    /// without allocating. This is the read-path counterpart to <see cref="ReadHeaderFromWire"/>: it
    /// replaces <see cref="Unserializer"/> on the hot range-scan path, where going through the generated
    /// parser allocated a <c>RaftLogMessage</c>, a <c>ByteString</c> wrapper and a fresh <c>logType</c>
    /// string for every entry visited — overhead that dwarfed the payload for small entries and was
    /// paid again on each re-read of an overlapping follower backfill range.
    ///
    /// <para>Decoding is deliberately separated from materialization so that callers can apply their
    /// partition/id filter against the view and skip non-matching entries without ever copying a
    /// payload.</para>
    ///
    /// <para>Absent fields keep their proto3 default, matching the generated parser: in particular an
    /// unset <c>logType</c> (5) yields <see cref="string.Empty"/>, not <see langword="null"/>, because
    /// the generated property substitutes the default for a missing value.</para>
    /// </summary>
    /// <exception cref="InvalidDataException">
    /// The value is truncated or otherwise malformed. The generated parser signalled this by throwing
    /// <c>InvalidProtocolBufferException</c>; corrupt entries must keep failing loudly rather than being
    /// silently decoded to defaults, which would present a torn entry as a legitimate log record.
    /// </exception>
    private static void ReadLogFromWire(ReadOnlySpan<byte> data, out RaftLogWireView view)
    {
        view = default;

        int pos = 0;
        while (pos < data.Length)
        {
            ulong tag = ReadVarint(data, ref pos);
            int fieldNumber = (int)(tag >> 3);
            int wireType = (int)(tag & 0x7);

            switch (wireType)
            {
                case 0: // varint
                {
                    ulong value = ReadVarint(data, ref pos);

                    switch (fieldNumber)
                    {
                        case 1: view.Partition = (int)value; break;
                        case 2: view.Id = (long)value; break;
                        case 3: view.Term = (long)value; break;
                        case 4: view.Type = (int)value; break;
                        case 7: view.TimeNode = (int)value; break;
                        case 8: view.TimePhysical = (long)value; break;
                        case 9: view.TimeCounter = (uint)value; break;
                    }

                    break;
                }

                case 2: // length-delimited
                {
                    int length = (int)ReadVarint(data, ref pos);
                    if (length < 0 || pos + length > data.Length)
                        throw new InvalidDataException($"Truncated RaftLogMessage: field {fieldNumber} declares {length} bytes but only {data.Length - pos} remain.");

                    switch (fieldNumber)
                    {
                        case 5: view.LogType = data.Slice(pos, length); break;
                        case 6: view.Log = data.Slice(pos, length); view.HasLog = true; break;
                    }

                    pos += length;
                    break;
                }

                case 1: // 64-bit
                    if (pos + 8 > data.Length)
                        throw new InvalidDataException("Truncated RaftLogMessage: incomplete 64-bit field.");
                    pos += 8;
                    break;

                case 5: // 32-bit
                    if (pos + 4 > data.Length)
                        throw new InvalidDataException("Truncated RaftLogMessage: incomplete 32-bit field.");
                    pos += 4;
                    break;

                default:
                    throw new InvalidDataException($"Malformed RaftLogMessage: unsupported wire type {wireType} for field {fieldNumber}.");
            }
        }
    }

    /// <summary>
    /// Materializes <paramref name="view"/> into a <see cref="RaftLog"/>, copying the payload out of the
    /// borrowed buffer. This copy is unavoidable — <see cref="RaftLog.LogData"/> is a <c>byte[]</c> that
    /// outlives the iterator position the bytes came from — and after this change it is the only
    /// per-entry allocation on the range-scan path apart from the <see cref="RaftLog"/> itself, since
    /// <c>logType</c> is served from <see cref="InternLogType"/>'s cache.
    /// </summary>
    private static RaftLog ToRaftLog(in RaftLogWireView view)
    {
        return new()
        {
            Id = view.Id,
            Term = view.Term,
            Type = (RaftLogType)view.Type,
            Time = new(view.TimeNode, view.TimePhysical, view.TimeCounter),
            LogType = InternLogType(view.LogType),
            LogData = view.HasLog ? (view.Log.IsEmpty ? [] : view.Log.ToArray()) : null
        };
    }

    /// <summary>
    /// The per-thread cache used by <see cref="InternLogType"/>. Log types form a tiny closed set (a
    /// handful of operation names for the whole cluster), so a short linear scan comparing raw UTF-8
    /// bytes resolves nearly every entry without decoding a new string. Kept <c>[ThreadStatic]</c>
    /// rather than shared because range scans run concurrently on the read scheduler's worker threads
    /// and a shared cache would need locking to publish entries safely.
    /// </summary>
    [ThreadStatic]
    private static (byte[] Utf8, string Value)[]? logTypeCache;

    /// <summary>The number of populated slots in <see cref="logTypeCache"/>.</summary>
    [ThreadStatic]
    private static int logTypeCacheCount;

    /// <summary>The next slot <see cref="InternLogType"/> will write, wrapping round-robin once the cache is full.</summary>
    [ThreadStatic]
    private static int logTypeCacheCursor;

    /// <summary>The maximum number of distinct log types cached per thread before slots are recycled.</summary>
    private const int LogTypeCacheSize = 8;

    /// <summary>
    /// Returns the string form of a UTF-8 <c>logType</c>, reusing a previously decoded instance whenever
    /// the same bytes are seen again. A range scan over a partition is overwhelmingly homogeneous in log
    /// type, so this turns one string allocation per entry into one per distinct type per thread.
    /// A miss costs an extra <c>byte[]</c> for the cached key; with more than
    /// <see cref="LogTypeCacheSize"/> live types the oldest slot is recycled rather than growing.
    /// </summary>
    private static string InternLogType(ReadOnlySpan<byte> utf8)
    {
        if (utf8.IsEmpty)
            return "";

        (byte[] Utf8, string Value)[] cache = logTypeCache ??= new (byte[], string)[LogTypeCacheSize];

        for (int i = 0; i < logTypeCacheCount; i++)
        {
            if (utf8.SequenceEqual(cache[i].Utf8))
                return cache[i].Value;
        }

        string value = Encoding.UTF8.GetString(utf8);

        cache[logTypeCacheCursor] = (utf8.ToArray(), value);
        logTypeCacheCursor = (logTypeCacheCursor + 1) % LogTypeCacheSize;
        if (logTypeCacheCount < LogTypeCacheSize)
            logTypeCacheCount++;

        return value;
    }

    /// <summary>Decodes a base-128 varint at <paramref name="pos"/>, advancing it past the value.</summary>
    private static ulong ReadVarint(ReadOnlySpan<byte> data, ref int pos)
    {
        ulong result = 0;
        int shift = 0;
        while (pos < data.Length)
        {
            byte b = data[pos++];
            result |= (ulong)(b & 0x7F) << shift;
            if ((b & 0x80) == 0)
                break;
            shift += 7;
        }

        return result;
    }

    public long GetCurrentTerm(int partitionId)
    {
        ColumnFamilyHandle columnFamilyHandle = GetColumnFamily(partitionId);

        using Iterator? iterator = db.NewIterator(cf: columnFamilyHandle);
        SeekToLastPartitionKey(iterator, partitionId);

        if (iterator.Valid() && KeyBelongsToPartition(iterator.GetKeySpan(), partitionId))
        {
            ReadHeaderFromWire(iterator.GetValueSpan(), out _, out _, out long term, out _);
            return term;
        }

        return 0;
    }

    /// <summary>
    /// Retrieves the last checkpoint log index from the specified partition.
    /// </summary>
    /// <param name="partitionId">
    /// The ID of the partition for which to retrieve the last checkpoint.
    /// </param>
    /// <returns>
    /// The log index of the last checkpoint for the specified partition.
    /// </returns>
    public long GetLastCheckpoint(int partitionId)
    {
        ColumnFamilyHandle columnFamilyHandle = GetColumnFamily(partitionId);
        
        return GetLastCheckpointInternal(partitionId, columnFamilyHandle);
    }

    /// <inheritdoc/>
    public int CountPersistedLogs(int partitionId)
    {
        ColumnFamilyHandle columnFamilyHandle = GetColumnFamily(partitionId);

        using Iterator? iterator = db.NewIterator(cf: columnFamilyHandle);
        Span<byte> seekKey = stackalloc byte[LogKeyWidth];
        BuildLogKey(seekKey, partitionId, 0);
        iterator.Seek(seekKey);

        int count = 0;

        // Counting only needs partition membership, which the key prefix already establishes — no value read.
        while (iterator.Valid() && KeyBelongsToPartition(iterator.GetKeySpan(), partitionId))
        {
            count++;
            iterator.Next();
        }

        return count;
    }

    /// <inheritdoc/>
    public int CountRemovableLogs(int partitionId)
    {
        long lastCheckpoint = GetLastCheckpoint(partitionId);

        if (lastCheckpoint <= 0)
            return 0;

        ColumnFamilyHandle columnFamilyHandle = GetColumnFamily(partitionId);

        using Iterator? iterator = db.NewIterator(cf: columnFamilyHandle);
        Span<byte> seekKey = stackalloc byte[LogKeyWidth];
        BuildLogKey(seekKey, partitionId, 0);
        iterator.Seek(seekKey);

        int count = 0;

        while (iterator.Valid() && KeyBelongsToPartition(iterator.GetKeySpan(), partitionId))
        {
            if (ParseLogIdFromKey(iterator.GetKeySpan()) >= lastCheckpoint)
                break;

            count++;
            iterator.Next();
        }

        return count;
    }

    /// <summary>
    /// Retrieves the last checkpoint ID for the specified partition using the provided column family handle.
    /// </summary>
    /// <param name="partitionId">
    /// The ID of the partition for which to retrieve the last checkpoint.
    /// </param>
    /// <param name="columnFamilyHandle">
    /// The column family handle associated with the specified partition in the database.
    /// </param>
    /// <returns>
    /// The ID of the last checkpoint for the specified partition, or -1 if no checkpoint is found.
    /// </returns>
    private long GetLastCheckpointInternal(int partitionId, ColumnFamilyHandle columnFamilyHandle)
    {
        using Iterator? iterator = db.NewIterator(cf: columnFamilyHandle);
        SeekToLastPartitionKey(iterator, partitionId);

        while (iterator.Valid() && KeyBelongsToPartition(iterator.GetKeySpan(), partitionId))
        {
            // `type` is only available in the value, but GetValueSpan borrows RocksDB's buffer rather than
            // copying it out, and the wire scan skips the payload — so a long backwards scan is allocation-free.
            ReadHeaderFromWire(iterator.GetValueSpan(), out _, out _, out _, out int type);

            if (type == (int)RaftLogType.CommittedCheckpoint)
                return ParseLogIdFromKey(iterator.GetKeySpan());

            iterator.Prev();
        }

        return -1;
    }

    /// <summary>
    /// Retrieves metadata value associated with the specified key from the database.
    /// </summary>
    /// <param name="key">
    /// The key for which the metadata value is to be retrieved.
    /// </param>
    /// <returns>
    /// The metadata value as a string if the key exists; otherwise, null.
    /// </returns>
    public string? GetMetaData(string key)
    {
        ColumnFamilyHandle? metaDataColumnFamily = db.GetColumnFamily("metadata");

        byte[] value = db.Get(Encoding.UTF8.GetBytes(key), cf: metaDataColumnFamily);
        
        return value is not null ? Encoding.UTF8.GetString(value) : null;
    }

    /// <summary>
    /// Sets a metadata key-value pair in the underlying storage.
    /// </summary>
    /// <param name="key">
    /// The key of the metadata to set.
    /// </param>
    /// <param name="value">
    /// The value of the metadata to associate with the specified key.
    /// </param>
    /// <returns>
    /// A boolean indicating whether the metadata was successfully set.
    /// </returns>
    public bool SetMetaData(string key, string value)
    {
        ColumnFamilyHandle? metaDataColumnFamily = db.GetColumnFamily("metadata");

        db.Put(Encoding.UTF8.GetBytes(key), Encoding.UTF8.GetBytes(value), cf: metaDataColumnFamily);

        return true;
    }

    /// <inheritdoc/>
    public RaftOperationStatus DeletePartitionWAL(int partitionId)
    {
        try
        {
            ColumnFamilyHandle columnFamilyHandle = GetColumnFamily(partitionId);

            List<byte[]> keysToDelete = [];

            using (Iterator? iterator = db.NewIterator(cf: columnFamilyHandle))
            {
                Span<byte> seekKey = stackalloc byte[LogKeyWidth];
                BuildLogKey(seekKey, partitionId, 0);
                iterator.Seek(seekKey);

                while (iterator.Valid() && KeyBelongsToPartition(iterator.GetKeySpan(), partitionId))
                {
                    keysToDelete.Add(iterator.Key());
                    iterator.Next();
                }
            }

            if (keysToDelete.Count > 0)
            {
                using WriteBatch writeBatch = new();
                foreach (byte[] key in keysToDelete)
                    writeBatch.Delete(key, cf: columnFamilyHandle);
                db.Write(writeBatch, writeOptions);
            }

            return RaftOperationStatus.Success;
        }
        catch (Exception ex)
        {
            logger.LogError("Error during DeletePartitionWAL({PartitionId}): {Message}", partitionId, ex.Message);
            return RaftOperationStatus.Errored;
        }
    }

    /// <inheritdoc/>
    public RaftOperationStatus TruncateLogsAfter(int partitionId, long afterLogId)
    {
        try
        {
            ColumnFamilyHandle columnFamilyHandle = GetColumnFamily(partitionId);

            List<byte[]> keysToDelete = [];

            using (Iterator? iterator = db.NewIterator(cf: columnFamilyHandle))
            {
                Span<byte> seekKey = stackalloc byte[LogKeyWidth];
                BuildLogKey(seekKey, partitionId, afterLogId + 1);
                iterator.Seek(seekKey);

                while (iterator.Valid() && KeyBelongsToPartition(iterator.GetKeySpan(), partitionId))
                {
                    keysToDelete.Add(iterator.Key());
                    iterator.Next();
                }
            }

            if (keysToDelete.Count > 0)
            {
                using WriteBatch writeBatch = new();
                foreach (byte[] key in keysToDelete)
                    writeBatch.Delete(key, cf: columnFamilyHandle);
                db.Write(writeBatch, writeOptions);
            }

            return RaftOperationStatus.Success;
        }
        catch (Exception ex)
        {
            logger.LogError("TruncateLogsAfter({PartitionId}, {AfterLogId}): {Message}", partitionId, afterLogId, ex.Message);
            return RaftOperationStatus.Errored;
        }
    }

    /// <inheritdoc/>
    /// <remarks>
    /// RocksDB exposes no app-level per-partition write guard (it is internally thread-safe per
    /// operation), so unlike the in-memory and SQLite backends the delete and max-read here are two
    /// distinct operations rather than one locked section. This is acceptable: holes are effectively
    /// absent on the fsync-paced persistent path (the storm this repair targets is in-memory only), so
    /// this method almost never fires on RocksDB, and the single-writer-per-partition WAL-scheduler
    /// invariant serializes appends for a given partition regardless.
    /// </remarks>
    public (RaftOperationStatus Status, long MaxLogId) TruncateLogsAfterAndGetMax(int partitionId, long afterLogId)
    {
        RaftOperationStatus status = TruncateLogsAfter(partitionId, afterLogId);
        if (status != RaftOperationStatus.Success)
            return (status, 0);

        return (status, GetMaxLog(partitionId));
    }

    /// <inheritdoc/>
    /// <remarks>
    /// The suffix deletes and the checkpoint put are staged into a single <see cref="WriteBatch"/> and
    /// applied with one <c>db.Write</c>, so the boundary install is atomic (RocksDB applies a batch
    /// all-or-nothing). Durability follows <paramref name="sync"/>: <c>sync:true</c> uses the instance's
    /// sync write options, else the non-sync options. The per-partition WAL-scheduler single-writer
    /// invariant serializes this against appends for the same partition.
    /// </remarks>
    public (RaftOperationStatus Status, bool SuffixTruncated) InstallSnapshotBoundary(
        int partitionId, long snapshotIndex, long lastIncludedTerm, bool sync)
    {
        WriteOptions effectiveOptions = sync ? writeOptions : NonSynchronousWriteOptions;

        try
        {
            ColumnFamilyHandle columnFamilyHandle = GetColumnFamily(partitionId);

            byte[] boundaryKey = new byte[LogKeyWidth];
            BuildLogKey(boundaryKey, partitionId, snapshotIndex);
            byte[]? existing = db.Get(boundaryKey, cf: columnFamilyHandle);
            long localTerm = existing is null ? -1 : ReadTermFromWire(existing);

            using WriteBatch writeBatch = new();

            bool suffixTruncated = false;
            if (localTerm != lastIncludedTerm)
            {
                List<byte[]> keysToDelete = [];
                using (Iterator? iterator = db.NewIterator(cf: columnFamilyHandle))
                {
                    Span<byte> seekKey = stackalloc byte[LogKeyWidth];
                    BuildLogKey(seekKey, partitionId, snapshotIndex + 1);
                    iterator.Seek(seekKey);

                    while (iterator.Valid() && KeyBelongsToPartition(iterator.GetKeySpan(), partitionId))
                    {
                        keysToDelete.Add(iterator.Key());
                        iterator.Next();
                    }
                }

                foreach (byte[] key in keysToDelete)
                    writeBatch.Delete(key, cf: columnFamilyHandle);

                suffixTruncated = keysToDelete.Count > 0;
            }

            // Upsert the checkpoint marker at the boundary index (same key overwrites any existing entry).
            RaftLogMessage checkpoint = new()
            {
                Partition = partitionId,
                Id = snapshotIndex,
                Term = lastIncludedTerm,
                Type = (int)RaftLogType.CommittedCheckpoint,
            };
            PutToBatch(writeBatch, checkpoint, columnFamilyHandle);

            db.Write(writeBatch, effectiveOptions);

            return (RaftOperationStatus.Success, suffixTruncated);
        }
        catch (Exception ex)
        {
            logger.LogError("InstallSnapshotBoundary({PartitionId}, {SnapshotIndex}): {Message}",
                partitionId, snapshotIndex, ex.Message);
            return (RaftOperationStatus.Errored, false);
        }
    }

    /// <summary>
    /// Compacts and removes logs in the Write-Ahead Log (WAL) for a specific partition that are older than the given checkpoint.
    /// </summary>
    /// <param name="partitionId">
    /// The ID of the partition whose logs are to be compacted.
    /// </param>
    /// <param name="lastCheckpoint">
    /// The log index up to which logs will be considered for compaction. Logs with an ID less than this checkpoint will be removed.
    /// </param>
    /// <param name="compactNumberEntries">
    /// The maximum number of entries removed per internal iterator batch when building the write batch.
    /// </param>
    /// <param name="maxTotalEntries">
    /// When set, multiple internal batches are accumulated into one <c>db.Write</c> so a compaction
    /// pass costs a single fsync.
    /// </param>
    /// <returns>
    /// A tuple of <see cref="RaftOperationStatus"/> and the number of entries removed.
    /// </returns>
    public (RaftOperationStatus Status, int Removed) CompactLogsOlderThan(
        int partitionId,
        long lastCheckpoint,
        int compactNumberEntries,
        int? maxTotalEntries = null)
    {
        int passCap = maxTotalEntries ?? compactNumberEntries;

        try
        {
            ColumnFamilyHandle columnFamilyHandle = GetColumnFamily(partitionId);

            using WriteBatch writeBatch = new();
            int removed = 0;

            using Iterator? iterator = db.NewIterator(cf: columnFamilyHandle);
            Span<byte> seekKey = stackalloc byte[LogKeyWidth];
            BuildLogKey(seekKey, partitionId, 0);
            iterator.Seek(seekKey);

            // Deletion is decided entirely from the key (partition prefix + id), so this pass never reads
            // an entry's value — the payload of every compacted entry stays in RocksDB's own buffers.
            while (iterator.Valid() && KeyBelongsToPartition(iterator.GetKeySpan(), partitionId) && removed < passCap)
            {
                if (ParseLogIdFromKey(iterator.GetKeySpan()) >= lastCheckpoint)
                    break;

                writeBatch.Delete(iterator.GetKeySpan(), cf: columnFamilyHandle);
                removed++;

                iterator.Next();
            }

            if (removed > 0)
            {
                db.Write(writeBatch, writeOptions);
                LastCompactionWriteCount = 1;
                logger.LogDebugRemovedFromWal(removed, partitionId);
            }
            else
            {
                LastCompactionWriteCount = 0;
            }

            return (RaftOperationStatus.Success, removed);
        }
        catch (Exception ex)
        {
            logger.LogError("Error during compact: {Message}", ex.Message);

            return (RaftOperationStatus.Errored, 0);
        }
    }
    
    /// <summary>
    /// The largest serialized message size that is serialized onto a <c>stackalloc</c> buffer before
    /// falling back to an <see cref="ArrayPool{T}"/> rental. Bounds stack usage on the write path while
    /// keeping the common (small) entry copy-free of any managed allocation.
    /// </summary>
    private const int StackallocThreshold = 256;

    /// <summary>
    /// Serializes <paramref name="message"/> into <paramref name="destination"/>, which must be exactly
    /// <see cref="MessageExtensions.CalculateSize"/> bytes long (sized by the caller). The bytes produced
    /// are byte-for-byte identical to the previous <c>MemoryStream.ToArray()</c> path — Protobuf writes a
    /// canonical encoding regardless of the sink — so on-disk format is unchanged.
    /// </summary>
    private static void SerializeInto(RaftLogMessage message, Span<byte> destination)
    {
        message.WriteTo(destination);
    }

    /// <summary>
    /// Applies a shared <see cref="BlockBasedTableOptions"/> to <paramref name="cfOptions"/> when
    /// <paramref name="sharedBbto"/> is non-null, then returns <paramref name="cfOptions"/>. A no-op
    /// when <paramref name="sharedBbto"/> is null so the caller does not need a branch at each CF site.
    /// </summary>
    private static ColumnFamilyOptions ApplyCfOptions(ColumnFamilyOptions cfOptions, BlockBasedTableOptions? sharedBbto)
    {
        if (sharedBbto is not null)
            cfOptions.SetBlockBasedTableFactory(sharedBbto);
        return cfOptions;
    }

    private static void BuildLogKey(Span<byte> result, int partitionId, long logId)
    {
        if (result.Length != LogKeyWidth)
            throw new ArgumentException($"RocksDB WAL log keys must be {LogKeyWidth} bytes.", nameof(result));

        ToDecimalBytes(result[..PartitionIdWidth], partitionId);
        result[PartitionIdWidth] = LogKeySeparator;
        ToDecimalBytes(result[(PartitionIdWidth + 1)..], logId);
    }

    private static void BuildPartitionUpperBoundKey(Span<byte> result, int partitionId)
    {
        if (result.Length != PartitionIdWidth + 1)
            throw new ArgumentException($"RocksDB WAL partition upper-bound keys must be {PartitionIdWidth + 1} bytes.", nameof(result));

        ToDecimalBytes(result[..PartitionIdWidth], partitionId);
        result[PartitionIdWidth] = PartitionUpperBoundSeparator;
    }

    /// <summary>
    /// Decodes the log id from a fixed-width WAL key without touching the stored value. The key is
    /// authoritative for both partition and id — <see cref="BuildLogKey"/> derives it from the message's
    /// own <c>Partition</c>/<c>Id</c> on every write — so scans that need only those two fields can skip
    /// reading (and copying out of native memory) the entry's value entirely.
    /// </summary>
    private static long ParseLogIdFromKey(ReadOnlySpan<byte> key)
    {
        long id = 0;

        for (int i = PartitionIdWidth + 1; i < key.Length; i++)
            id = (id * 10) + (key[i] - (byte)'0');

        return id;
    }

    private static bool KeyBelongsToPartition(ReadOnlySpan<byte> key, int partitionId)
    {
        Span<byte> prefix = stackalloc byte[PartitionIdWidth + 1];
        ToDecimalBytes(prefix[..PartitionIdWidth], partitionId);
        prefix[PartitionIdWidth] = LogKeySeparator;

        return key.StartsWith(prefix);
    }

    private static void SeekToLastPartitionKey(Iterator iterator, int partitionId)
    {
        Span<byte> upperBoundKey = stackalloc byte[PartitionIdWidth + 1];
        BuildPartitionUpperBoundKey(upperBoundKey, partitionId);

        iterator.Seek(upperBoundKey);

        if (iterator.Valid())
            iterator.Prev();
        else
            iterator.SeekToLast();
    }

    /// <summary>
    /// Converts the specified long value into its decimal representation as a sequence of ASCII bytes
    /// and stores it into the provided span buffer. The resulting buffer is left-padded with '0'
    /// characters to reach a fixed width.
    ///
    /// This ensures the logs will be written and ordered in lexicographical order.
    /// </summary>
    /// <param name="result">
    /// The span of bytes where the decimal ASCII representation will be stored. The span must
    /// have sufficient space to accommodate the fixed width.
    /// </param>
    /// <param name="value">
    /// The long value to be converted into its decimal ASCII byte representation.
    /// </param>
    private static void ToDecimalBytes(Span<byte> result, long value)
    {
        // 1) Pre‑fill with ASCII '0'
        for (int i = 0; i < result.Length; i++)
            result[i] = (byte)'0';

        // 2) Write digits right‑to‑left
        int pos = result.Length - 1;
        do
        {
            result[pos--] = (byte)('0' + (value % 10));
            value /= 10;
        }
        while (value > 0);        
    }

    public void Dispose()
    {
        GC.SuppressFinalize(this);
        
        db.Dispose();        
    }
}
