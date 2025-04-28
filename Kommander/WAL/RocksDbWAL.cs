
using System.Collections.Concurrent;
using System.Runtime.InteropServices;
using System.Text;
using Google.Protobuf;
using Kommander.Data;
using Kommander.WAL.Protos;
using Microsoft.IO;
using RocksDbSharp;

namespace Kommander.WAL;

/// <summary>
/// The RocksDbWAL class provides an implementation of a Write-Ahead Log (WAL) using RocksDB as the storage backend.
/// It supports functionality for reading, writing, compacting, and managing log metadata in a partitioned setup.
/// </summary>
public class RocksDbWAL : IWAL, IDisposable
{
    /// <summary>
    /// A shared, reusable memory stream manager that provides efficient
    /// memory allocation and deallocation for stream-based operations,
    /// reducing memory fragmentation and improving performance, especially
    /// for scenarios involving frequent and large memory allocations.
    /// </summary>
    private static readonly RecyclableMemoryStreamManager streamManager = new();

    /// <summary>
    /// A static, reusable instance of <see cref="WriteOptions"/> configured to enable synchronous operations.
    /// This ensures data integrity by guaranteeing that write operations are fully persisted to disk
    /// before returning, at the potential cost of reduced write performance.
    /// </summary>
    private static readonly WriteOptions DefaultWriteOptions = new WriteOptions().SetSync(true);

    /// <summary>
    /// Specifies the version of the format used for the RocksDb Write-Ahead Log (WAL) implementation.
    /// This constant is used to ensure compatibility by identifying the version of the metadata structure
    /// and log format maintained within the storage system.
    /// </summary>
    private const string FormatVersion = "1.0.0";

    /// <summary>
    /// Represents the maximum allowable size, in bytes, for a serialized message
    /// within the system. Messages exceeding this size require additional processing,
    /// such as usage of recyclable memory streams, to ensure efficient handling and adherence
    /// to size constraints.
    /// </summary>
    private const int MaxMessageSize = 1024;

    /// <summary>
    /// Specifies the maximum number of shards supported by the Write-Ahead Log (WAL) implementation,
    /// representing the logical partitions used for segregating and managing log entries efficiently.
    ///
    /// Each shard represent a column family in RocksDB, allowing for concurrent access and
    /// concurrent compaction of the memtables in each shard.
    /// </summary>
    private const int MaxShards = 8;

    /// <summary>
    /// Defines the maximum number of log entries that can be retrieved in a ranged query
    /// when reading logs from the Write-Ahead Log (WAL). This constant is used to limit
    /// the number of entries returned in a single operation to ensure performance and
    /// prevent excessive resource usage during log read operations.
    /// </summary>
    private const int MaxNumberOfRangedEntries = 100;

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
    
    public RocksDbWAL(string path, string revision, ILogger<IRaft> logger)
    {
        this.path = path;
        this.revision = revision;
        this.logger = logger;

        DbOptions dbOptions = new DbOptions()
            .SetCreateIfMissing(true)
            .SetCreateMissingColumnFamilies(true)
            .SetAllowConcurrentMemtableWrite(true)
            .SetWalRecoveryMode(Recovery.AbsoluteConsistency);
        
        ColumnFamilies columnFamilies = new()
        {
            { "default", new() },
            { "metadata", new() }
        };
        
        for (int i = 0; i < MaxShards; i++)
            columnFamilies.Add("shard" + i, new());

        string completePath = $"{path}/{revision}";
        
        bool firstTime = !Directory.Exists(completePath);
        
        db = RocksDb.Open(dbOptions, completePath, columnFamilies);
        
        if (firstTime)
            SetMetaData("version", FormatVersion);
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
        
        if (lastCheckpoint <= 0)
            iterator.SeekToFirst();  // Move to the first key
        else
        {
            Span<byte> buffer = stackalloc byte[IdWidth];
            
            ToDecimalBytes(buffer, lastCheckpoint);
           
            iterator.Seek(buffer);
        }

        while (iterator.Valid())
        {
            RaftLogMessage message = Unserializer(iterator.Value());
            
            if (message.Partition != partitionId || message.Id < lastCheckpoint)
            {
                iterator.Next();
                continue;
            }

            //if (partitionId == 1)
            //    Console.WriteLine("{0} {1}", iterator.StringKey(), message.Id);

            RaftLog raftLog = new()
            {
                Id = message.Id,
                Term = message.Term,
                Type = (RaftLogType)message.Type,
                Time = new(message.TimeNode, message.TimePhysical, message.TimeCounter),
                LogType = message.LogType
            };
            
            if (message.Log is not null)
            {
                if (MemoryMarshal.TryGetArray(message.Log.Memory, out ArraySegment<byte> segment))
                    raftLog.LogData = segment.Array;
                else
                    raftLog.LogData = message.Log.ToByteArray();
            }

            result.Add(raftLog);
            
            iterator.Next();
        }

        return result;
    }

    /// <summary>
    /// Reads a range of logs from the Write-Ahead Log (WAL) for the specified partition,
    /// starting from the given log index.
    /// </summary>
    /// <param name="partitionId">
    /// The ID of the partition from which to read the logs.
    /// </param>
    /// <param name="startLogIndex">
    /// The starting index from which logs should be read. Logs with a lower index will be excluded.
    /// </param>
    /// <returns>
    /// A list of <see cref="RaftLog"/> instances representing the logs read from the specified partition,
    /// starting from the specified log index.
    /// </returns>
    public List<RaftLog> ReadLogsRange(int partitionId, long startLogIndex)
    {
        List<RaftLog> result = [];

        ColumnFamilyHandle columnFamilyHandle = GetColumnFamily(partitionId);
        
        using Iterator? iterator = db.NewIterator(cf: columnFamilyHandle);
        
        if (startLogIndex <= 0)
            iterator.SeekToFirst();  // Move to the first key
        else
        {
            Span<byte> buffer = stackalloc byte[IdWidth];
            
            ToDecimalBytes(buffer, startLogIndex);
            
            //Span<byte> buffer = stackalloc byte[Encoding.UTF8.GetByteCount(index)];
            //Encoding.UTF8.GetBytes(index.AsSpan(), buffer);
            
            iterator.Seek(buffer);
        }

        int counter = 0;

        while (iterator.Valid())
        {
            RaftLogMessage message = Unserializer(iterator.Value());
            
            if (message.Partition != partitionId || message.Id < startLogIndex)
            {
                iterator.Next();
                continue;
            }

            RaftLog raftLog = new()
            {
                Id = message.Id,
                Term = message.Term,
                Type = (RaftLogType)message.Type,
                Time = new(message.TimeNode, message.TimePhysical, message.TimeCounter),
                LogType = message.LogType
            };

            if (message.Log is not null)
            {
                if (MemoryMarshal.TryGetArray(message.Log.Memory, out ArraySegment<byte> segment))
                    raftLog.LogData = segment.Array;
                else
                    raftLog.LogData = message.Log.ToByteArray();
            }

            result.Add(raftLog);
            
            iterator.Next();

            counter++;

            if (counter > MaxNumberOfRangedEntries)
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
    public RaftOperationStatus Write(List<(int, List<RaftLog>)> logs)
    {
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

                Span<byte> buffer = stackalloc byte[IdWidth];
                
                ToDecimalBytes(buffer, message.Id);
                
                ColumnFamilyHandle columnFamilyHandle = GetColumnFamily(partitionId);
                
                db.Put(buffer, Serialize(message), columnFamilyHandle);
                
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
            
            db.Write(writeBatch, DefaultWriteOptions);

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
        Span<byte> buffer = stackalloc byte[IdWidth];
                
        ToDecimalBytes(buffer, message.Id);
        
        writeBatch.Put(buffer, Serialize(message), cf: columnFamilyHandle);
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
        iterator.SeekToLast();  // Move to the last key

        while (iterator.Valid())
        {
            RaftLogMessage message = Unserializer(iterator.Value());
            
            if (message.Partition != partitionId)
            {
                iterator.Prev();
                continue;
            }
            
            return message.Id;
        }

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
    public long GetCurrentTerm(int partitionId)
    {
        ColumnFamilyHandle columnFamilyHandle = GetColumnFamily(partitionId);
        
        using Iterator? iterator = db.NewIterator(cf: columnFamilyHandle);
        iterator.SeekToLast();  // Move to the last key

        while (iterator.Valid())
        {
            RaftLogMessage message = Unserializer(iterator.Value());
            
            if (message.Partition != partitionId)
            {
                iterator.Prev();
                continue;
            }
            
            return message.Term;
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
        iterator.SeekToLast();  // Move to the last key

        while (iterator.Valid())
        {
            RaftLogMessage message = Unserializer(iterator.Value());
            
            //if (partitionId == 6)
            //    Console.WriteLine("{0} {1} {2} {3} {4}", partitionId, message.Partition, message.Type, iterator.StringKey(), message.Id);
            
            if (message.Partition != partitionId)
            {
                iterator.Prev();
                continue;
            }
            
            if (message.Type == (int)RaftLogType.CommittedCheckpoint)
                return message.Id;
            
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
    /// The maximum number of entries to process during the compaction operation.
    /// </param>
    /// <returns>
    /// A <see cref="RaftOperationStatus"/> indicating the result of the compaction operation.
    /// </returns>
    public RaftOperationStatus CompactLogsOlderThan(int partitionId, long lastCheckpoint, int compactNumberEntries)
    {
        try
        {
            ColumnFamilyHandle columnFamilyHandle = GetColumnFamily(partitionId);

            HashSet<byte[]> logsToRemove = new(compactNumberEntries);

            using Iterator? iterator = db.NewIterator(cf: columnFamilyHandle);
            iterator.SeekToFirst(); // Move to the last key

            while (iterator.Valid())
            {
                RaftLogMessage message = Unserializer(iterator.Value());

                if (message.Partition != partitionId)
                {
                    iterator.Next();
                    continue;
                }

                if (message.Id < lastCheckpoint)
                {
                    logsToRemove.Add(iterator.Key());

                    if (logsToRemove.Count >= compactNumberEntries)
                        break;
                }

                iterator.Next();
            }

            if (logsToRemove.Count > 0)
            {
                using WriteBatch writeBatch = new();

                foreach (byte[] log in logsToRemove)
                    writeBatch.Delete(log, cf: columnFamilyHandle);

                db.Write(writeBatch); // No need to be synchronous here
                
                logger.LogDebug("Removed {Count} from WAL for partition {PartitionId}", logsToRemove.Count, partitionId);
            }
            
            return RaftOperationStatus.Success;
        } 
        catch (Exception ex)
        {
            logger.LogError("Error during compact: {Message}", ex.Message);
                
            return RaftOperationStatus.Errored;
        }
    }
    
    private static byte[] Serialize(RaftLogMessage message)
    {
        if (!message.Log.IsEmpty && message.Log.Length >= MaxMessageSize)
        {
            using RecyclableMemoryStream recyclableMemoryStream = streamManager.GetStream();
            message.WriteTo((Stream)recyclableMemoryStream);
            return recyclableMemoryStream.ToArray();
        }
        
        using MemoryStream memoryStream = streamManager.GetStream();
        message.WriteTo(memoryStream);
        return memoryStream.ToArray();
    }

    /// <summary>
    /// Deserializes a binary representation of a <see cref="RaftLogMessage"/> into its object form.
    /// </summary>
    /// <param name="serializedData">
    /// The binary data representing a serialized <see cref="RaftLogMessage"/>.
    /// </param>
    /// <returns>
    /// An instance of <see cref="RaftLogMessage"/> deserialized from the provided binary data.
    /// </returns>
    private static RaftLogMessage Unserializer(ReadOnlySpan<byte> serializedData)
    {
        if (serializedData.Length >= MaxMessageSize)
        {
            using RecyclableMemoryStream recyclableMemoryStream = streamManager.GetStream(serializedData);
            return RaftLogMessage.Parser.ParseFrom(recyclableMemoryStream);
        }
        
        using MemoryStream memoryStream = streamManager.GetStream(serializedData);
        return RaftLogMessage.Parser.ParseFrom(memoryStream);
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
        for (int i = 0; i < IdWidth; i++)
            result[i] = (byte)'0';

        // 2) Write digits right‑to‑left
        int pos = IdWidth - 1;
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