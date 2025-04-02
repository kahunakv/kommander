
using System.Text;
using Google.Protobuf;
using Kommander.Data;
using Kommander.WAL.Protos;
using RocksDbSharp;

namespace Kommander.WAL;

public class RocksDbWAL : IWAL
{
    private static readonly WriteOptions DefaultWriteOptions = new WriteOptions().SetSync(true);

    private const string FormatVersion = "1.0.0";
    
    private const int MaxShards = 16;
    
    private const int MaxNumberOfRangedEntries = 100;
    
    private readonly RocksDb db;
    
    private readonly string path;
    
    private readonly string revision;
    
    private readonly Dictionary<int, ColumnFamilyHandle> families = new();
    
    private readonly ILogger<IRaft> logger;
    
    public RocksDbWAL(string path, string revision, ILogger<IRaft> logger)
    {
        this.path = path;
        this.revision = revision;
        this.logger = logger;

        DbOptions dbOptions = new DbOptions()
            .SetCreateIfMissing(true)
            .SetCreateMissingColumnFamilies(true)
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
        if (!families.TryGetValue(partitionId, out ColumnFamilyHandle? columnFamily))
        {
            int shardId = partitionId % MaxShards;
            columnFamily = db.GetColumnFamily("shard" + shardId);
            families.Add(partitionId, columnFamily);
        }
        
        return columnFamily;
    }

    public List<RaftLog> ReadLogs(int partitionId)
    {
        List<RaftLog> result = [];
        
        ColumnFamilyHandle columnFamilyHandle = GetColumnFamily(partitionId);
        
        long lastCheckpoint = GetLastCheckpointInternal(partitionId, columnFamilyHandle);
        
        // Console.WriteLine($"Last checkpoint {partitionId} {lastCheckpoint}");

        using Iterator? iterator = db.NewIterator(cf: columnFamilyHandle);
        
        if (lastCheckpoint <= 0)
            iterator.SeekToFirst();  // Move to the first key
        else
        {
            string index = lastCheckpoint.ToString("D20");
            iterator.Seek(Encoding.UTF8.GetBytes(index));
        }

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
                iterator.Next();
                continue;
            }

            //if (partitionId == 1)
            //    Console.WriteLine("{0} {1}", iterator.StringKey(), message.Id);

            result.Add(new()
            {
                Id = message.Id,
                Term = message.Term,
                Type = (RaftLogType)message.Type,
                Time = new(message.TimePhysical, message.TimeCounter),
                LogType = message.LogType,
                LogData = message.Log.ToByteArray(),
            });
            
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
            string index = startLogIndex.ToString("D20");
            iterator.Seek(Encoding.UTF8.GetBytes(index));
        }

        int counter = 0;

        while (iterator.Valid())
        {
            RaftLogMessage message = Unserializer(iterator.Value());
            
            if (message.Partition != partitionId)
            {
                iterator.Next();
                continue;
            }

            if (message.Id < startLogIndex)
            {
                iterator.Next();
                continue;
            }

            result.Add(new()
            {
                Id = message.Id,
                Term = message.Term,
                Type = (RaftLogType)message.Type,
                Time = new(message.TimePhysical, message.TimeCounter),
                LogType = message.LogType,
                LogData = message.Log?.ToByteArray(),
            });
            
            iterator.Next();

            counter++;

            if (counter > MaxNumberOfRangedEntries)
                break;
        }

        return result;
    }

    public RaftOperationStatus Propose(int partitionId, RaftLog log)
    {
        try 
        {
            ColumnFamilyHandle columnFamilyHandle = GetColumnFamily(partitionId);
            
            string index = log.Id.ToString("D20");

            RaftLogMessage message = new()
            {
                Partition = partitionId,
                Id = log.Id,
                Term = log.Term,
                Type = (int)log.Type,
                TimePhysical = log.Time.L,
                TimeCounter = log.Time.C
            };

            if (log.LogType != null)
                message.LogType = log.LogType;
            
            if (log.LogData != null)
                message.Log = UnsafeByteOperations.UnsafeWrap(log.LogData);
            
            db.Put(Encoding.UTF8.GetBytes(index), Serialize(message), cf: columnFamilyHandle, DefaultWriteOptions);

            return RaftOperationStatus.Success;
        } 
        catch (Exception ex)
        {
            logger.LogError("Error during proposal: {Message}\n{StackTrace}", ex.Message, ex.StackTrace);
                    
            return RaftOperationStatus.Errored;
        }
    }
    
    public RaftOperationStatus Commit(int partitionId, RaftLog log)
    {
        try 
        {
            ColumnFamilyHandle columnFamilyHandle = GetColumnFamily(partitionId);
            
            string index = log.Id.ToString("D20");
            
            RaftLogMessage message = new()
            {
                Partition = partitionId,
                Id = log.Id,
                Term = log.Term,
                Type = (int)log.Type,
                TimePhysical = log.Time.L,
                TimeCounter = log.Time.C
            };
            
            if (log.LogType != null)
                message.LogType = log.LogType;
            
            if (log.LogData != null)
                message.Log = UnsafeByteOperations.UnsafeWrap(log.LogData);
            
            db.Put(Encoding.UTF8.GetBytes(index), Serialize(message), cf: columnFamilyHandle, DefaultWriteOptions);

            return RaftOperationStatus.Success;
        } 
        catch (Exception ex)
        {
            logger.LogError("Error during commit: {Message}\n{StackTrace}", ex.Message, ex.StackTrace);
                    
            return RaftOperationStatus.Errored;
        }
    }
    
    public RaftOperationStatus Rollback(int partitionId, RaftLog log)
    {
        try
        {
            ColumnFamilyHandle columnFamilyHandle = GetColumnFamily(partitionId);
            
            string index = log.Id.ToString("D20");
            
            RaftLogMessage message = new()
            {
                Partition = partitionId,
                Id = log.Id,
                Term = log.Term,
                Type = (int)log.Type,
                TimePhysical = log.Time.L,
                TimeCounter = log.Time.C
            };
            
            if (log.LogType != null)
                message.LogType = log.LogType;
            
            if (log.LogData != null)
                message.Log = UnsafeByteOperations.UnsafeWrap(log.LogData);
            
            db.Put(Encoding.UTF8.GetBytes(index), Serialize(message), cf: columnFamilyHandle, DefaultWriteOptions);

            return RaftOperationStatus.Success;
        } 
        catch (Exception ex)
        {
            logger.LogError("Error during rollback: {Message}\n{StackTrace}", ex.Message, ex.StackTrace);
                
            return RaftOperationStatus.Errored;
        }
    }

    public RaftOperationStatus ProposeMany(int partitionId, List<RaftLog> logs)
    {
        try
        {
            ColumnFamilyHandle columnFamilyHandle = GetColumnFamily(partitionId);
            
            using WriteBatch writeBatch = new();
            
            foreach (RaftLog log in logs)
            {
                string index = log.Id.ToString("D20");
                
                RaftLogMessage message = new()
                {
                    Partition = partitionId,
                    Id = log.Id,
                    Term = log.Term,
                    Type = (int)log.Type,
                    TimePhysical = log.Time.L,
                    TimeCounter = log.Time.C
                };
                
                if (log.LogType != null)
                    message.LogType = log.LogType;
            
                if (log.LogData != null)
                    message.Log = UnsafeByteOperations.UnsafeWrap(log.LogData);
            
                writeBatch.Put(Encoding.UTF8.GetBytes(index), Serialize(message), cf: columnFamilyHandle);
            }
            
            db.Write(writeBatch, DefaultWriteOptions);

            return RaftOperationStatus.Success;
        } 
        catch (Exception ex)
        {
            logger.LogError("Error during proposal: {Message}\n{StackTrace}", ex.Message, ex.StackTrace);
                    
            return RaftOperationStatus.Errored;
        }
    }

    public RaftOperationStatus CommitMany(int partitionId, List<RaftLog> logs)
    {
        if (logs.Count == 0)
            return RaftOperationStatus.Success;

        try
        {
            ColumnFamilyHandle columnFamilyHandle = GetColumnFamily(partitionId);

            using WriteBatch writeBatch = new();

            foreach (RaftLog log in logs)
            {
                string index = log.Id.ToString("D20");
                
                RaftLogMessage message = new()
                {
                    Partition = partitionId,
                    Id = log.Id,
                    Term = log.Term,
                    Type = (int)log.Type,
                    TimePhysical = log.Time.L,
                    TimeCounter = log.Time.C
                };
                
                if (log.LogType != null)
                    message.LogType = log.LogType;
            
                if (log.LogData != null)
                    message.Log = UnsafeByteOperations.UnsafeWrap(log.LogData);

                writeBatch.Put(Encoding.UTF8.GetBytes(index), Serialize(message), cf: columnFamilyHandle);
            }

            db.Write(writeBatch, DefaultWriteOptions);

            return RaftOperationStatus.Success;
        } 
        catch (Exception ex)
        {
            logger.LogError("Error during commit: {Message}\n{StackTrace}", ex.Message, ex.StackTrace);
            
            return RaftOperationStatus.Errored;
        }
    }

    public RaftOperationStatus RollbackMany(int partitionId, List<RaftLog> logs)
    {
        try
        {
            ColumnFamilyHandle columnFamilyHandle = GetColumnFamily(partitionId);
            
            using WriteBatch writeBatch = new();

            foreach (RaftLog log in logs)
            {
                string index = log.Id.ToString("D20");
                
                RaftLogMessage message = new()
                {
                    Partition = partitionId,
                    Id = log.Id,
                    Term = log.Term,
                    Type = (int)log.Type,
                    TimePhysical = log.Time.L,
                    TimeCounter = log.Time.C
                };
                
                if (log.LogType != null)
                    message.LogType = log.LogType;
            
                if (log.LogData != null)
                    message.Log = UnsafeByteOperations.UnsafeWrap(log.LogData);

                writeBatch.Put(Encoding.UTF8.GetBytes(index), Serialize(message), cf: columnFamilyHandle);
            }
            
            db.Write(writeBatch, DefaultWriteOptions);

            return RaftOperationStatus.Success;
        } 
        catch (Exception ex)
        {
            logger.LogError("Error during rollback: {Message}\n{StackTrace}", ex.Message, ex.StackTrace);
                
            return RaftOperationStatus.Errored;
        }
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
                iterator.Next();
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
                iterator.Next();
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
            
            if (message.Partition != partitionId)
            {
                iterator.Next();
                continue;
            }
            
            if (message.Type == (int)RaftLogType.CommittedCheckpoint)
                return message.Id;
            
            iterator.Next();
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

            HashSet<string> logsToRemove = new((int)compactNumberEntries);

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
                    logsToRemove.Add(iterator.StringKey());

                    if (logsToRemove.Count >= compactNumberEntries)
                        break;
                }

                iterator.Next();
            }

            if (logsToRemove.Count > 0)
            {
                using WriteBatch writeBatch = new();

                foreach (string log in logsToRemove)
                    writeBatch.Delete(Encoding.UTF8.GetBytes(log), cf: columnFamilyHandle);

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
        using MemoryStream memoryStream = new();
        message.WriteTo(memoryStream);
        return memoryStream.ToArray();
    }

    private static RaftLogMessage Unserializer(byte[] serializedData)
    {
        using MemoryStream memoryStream = new(serializedData);
        return RaftLogMessage.Parser.ParseFrom(memoryStream);
    }
}