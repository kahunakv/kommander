
using System.Text;
using Google.Protobuf;
using Kommander.Data;
using Kommander.WAL.Protos;
using Microsoft.IO;
using RocksDbSharp;
using System.Collections.Concurrent;
using System.Runtime.InteropServices;
using Microsoft.Extensions.Logging;

namespace Kommander.WAL;

public class RocksDbWAL : IWAL, IDisposable
{
    private static readonly RecyclableMemoryStreamManager streamManager = new();
    
    private static readonly WriteOptions DefaultWriteOptions = new WriteOptions().SetSync(true);

    private const string FormatVersion = "1.0.0";

    private const int MaxMessageSize = 1024;
    
    private const int MaxShards = 8;
    
    private const int MaxNumberOfRangedEntries = 100;
    
    private const int IdWidth = 20;
    
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
    
    private ColumnFamilyHandle GetColumnFamily(int partitionId)
    {
        Lazy<ColumnFamilyHandle> lazy = families.GetOrAdd(partitionId, GetColumnFamilyHandle);
        return lazy.Value;
    }

    private Lazy<ColumnFamilyHandle> GetColumnFamilyHandle(int arg)
    {
        return new(() =>
        {
            int shardId = arg % MaxShards;
            return db.GetColumnFamily("shard" + shardId);
        });
    }

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
    
    private static void PutToBatch(WriteBatch writeBatch, RaftLogMessage message, ColumnFamilyHandle columnFamilyHandle)
    {
        Span<byte> buffer = stackalloc byte[IdWidth];
                
        ToDecimalBytes(buffer, message.Id);
        
        writeBatch.Put(buffer, Serialize(message), cf: columnFamilyHandle);
    }

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
    
    public long GetLastCheckpoint(int partitionId)
    {
        ColumnFamilyHandle columnFamilyHandle = GetColumnFamily(partitionId);
        
        return GetLastCheckpointInternal(partitionId, columnFamilyHandle);
    }

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
    
    public string? GetMetaData(string key)
    {
        ColumnFamilyHandle? metaDataColumnFamily = db.GetColumnFamily("metadata");

        byte[] value = db.Get(Encoding.UTF8.GetBytes(key), cf: metaDataColumnFamily);
        
        return value is not null ? Encoding.UTF8.GetString(value) : null;
    }

    public bool SetMetaData(string key, string value)
    {
        ColumnFamilyHandle? metaDataColumnFamily = db.GetColumnFamily("metadata");

        db.Put(Encoding.UTF8.GetBytes(key), Encoding.UTF8.GetBytes(value), cf: metaDataColumnFamily);

        return true;
    }

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