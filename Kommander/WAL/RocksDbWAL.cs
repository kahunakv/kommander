
using System.Text;
using Google.Protobuf;
using Kommander.Data;
using Kommander.WAL.Protos;
using RocksDbSharp;

namespace Kommander.WAL;

public class RocksDbWAL : IWAL
{
    private static readonly WriteOptions DefaultWriteOptions = new WriteOptions().SetSync(true);
    
    private const int MaxShards = 16;
    
    private const int MaxNumberOfRangedEntries = 100;
    
    private readonly RocksDb db;
    
    private readonly string path;
    
    private readonly string revision;
    
    public RocksDbWAL(string path, string revision)
    {
        this.path = path;
        this.revision = revision;

        DbOptions dbOptions = new DbOptions()
            .SetCreateIfMissing(true)
            .SetCreateMissingColumnFamilies(true)
            .SetWalRecoveryMode(Recovery.AbsoluteConsistency);
        
        ColumnFamilies columnFamilies = new()
        {
            { "default", new() }
        };
        
        for (int i = 0; i < MaxShards; i++)
            columnFamilies.Add("shard" + i, new());
        
        this.db = RocksDb.Open(dbOptions, $"{path}/{revision}", columnFamilies);
    }

    public List<RaftLog> ReadLogs(int partitionId)
    {
        List<RaftLog> result = [];
        
        int shardId = partitionId % MaxShards;
        ColumnFamilyHandle? columnFamilyHandle = db.GetColumnFamily("shard" + shardId);
        
        long lastCheckpoint = GetLastCheckpoint(partitionId, columnFamilyHandle);

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
        
        int shardId = partitionId % MaxShards;
        ColumnFamilyHandle? columnFamilyHandle = db.GetColumnFamily("shard" + shardId);
        
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
                LogData = message.Log.ToByteArray(),
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
        int shardId = partitionId % MaxShards;
        ColumnFamilyHandle? columnFamilyHandle = db.GetColumnFamily("shard" + shardId);
        
        string index = log.Id.ToString("D20");
        
        db.Put(Encoding.UTF8.GetBytes(index), Serialize(new()
        {
            Partition = partitionId,
            Id = log.Id,
            Term = log.Term,
            Type = (int)log.Type,
            LogType = log.LogType,
            Log = UnsafeByteOperations.UnsafeWrap(log.LogData),
            TimePhysical = log.Time.L,
            TimeCounter = log.Time.C
        }), cf: columnFamilyHandle, DefaultWriteOptions);

        return RaftOperationStatus.Success;
    }
    
    public RaftOperationStatus Commit(int partitionId, RaftLog log)
    {
        int shardId = partitionId % MaxShards;
        ColumnFamilyHandle? columnFamilyHandle = db.GetColumnFamily("shard" + shardId);
        
        string index = log.Id.ToString("D20");
        
        db.Put(Encoding.UTF8.GetBytes(index), Serialize(new()
        {
            Partition = partitionId,
            Id = log.Id,
            Term = log.Term,
            Type = (int)log.Type,
            LogType = log.LogType,
            Log = UnsafeByteOperations.UnsafeWrap(log.LogData),
            TimePhysical = log.Time.L,
            TimeCounter = log.Time.C
        }), cf: columnFamilyHandle, DefaultWriteOptions);

        return RaftOperationStatus.Success;
    }
    
    public RaftOperationStatus Rollback(int partitionId, RaftLog log)
    {
        int shardId = partitionId % MaxShards;
        ColumnFamilyHandle? columnFamilyHandle = db.GetColumnFamily("shard" + shardId);
        
        string index = log.Id.ToString("D20");
        
        db.Put(Encoding.UTF8.GetBytes(index), Serialize(new()
        {
            Partition = partitionId,
            Id = log.Id,
            Term = log.Term,
            Type = (int)log.Type,
            LogType = log.LogType,
            Log = UnsafeByteOperations.UnsafeWrap(log.LogData),
            TimePhysical = log.Time.L,
            TimeCounter = log.Time.C
        }), cf: columnFamilyHandle, DefaultWriteOptions);

        return RaftOperationStatus.Success;
    }

    public RaftOperationStatus ProposeMany(int partitionId, List<RaftLog> logs)
    {
        int shardId = partitionId % MaxShards;
        ColumnFamilyHandle? columnFamilyHandle = db.GetColumnFamily("shard" + shardId);
        
        using WriteBatch writeBatch = new();
        
        foreach (RaftLog log in logs)
        {
            string index = log.Id.ToString("D20");
        
            writeBatch.Put(Encoding.UTF8.GetBytes(index), Serialize(new()
            {
                Partition = partitionId,
                Id = log.Id,
                Term = log.Term,
                Type = (int)log.Type,
                LogType = log.LogType,
                Log = UnsafeByteOperations.UnsafeWrap(log.LogData),
                TimePhysical = log.Time.L,
                TimeCounter = log.Time.C
            }), cf: columnFamilyHandle);
        }
        
        db.Write(writeBatch, DefaultWriteOptions);

        return RaftOperationStatus.Success;
    }

    public RaftOperationStatus CommitMany(int partitionId, List<RaftLog> logs)
    {
        if (logs.Count == 0)
            return RaftOperationStatus.Success;
        
        int shardId = partitionId % MaxShards;
        ColumnFamilyHandle? columnFamilyHandle = db.GetColumnFamily("shard" + shardId);
        
        using WriteBatch writeBatch = new();
        
        foreach (RaftLog log in logs)
        {
            string index = log.Id.ToString("D20");

            writeBatch.Put(Encoding.UTF8.GetBytes(index), Serialize(new()
            {
                Partition = partitionId,
                Id = log.Id,
                Term = log.Term,
                Type = (int)log.Type,
                LogType = log.LogType,
                Log = UnsafeByteOperations.UnsafeWrap(log.LogData),
                TimePhysical = log.Time.L,
                TimeCounter = log.Time.C
            }), cf: columnFamilyHandle);
        }
        
        db.Write(writeBatch, DefaultWriteOptions);

        return RaftOperationStatus.Success;
    }

    public RaftOperationStatus RollbackMany(int partitionId, List<RaftLog> logs)
    {
        int shardId = partitionId % MaxShards;
        ColumnFamilyHandle? columnFamilyHandle = db.GetColumnFamily("shard" + shardId);
        
        using WriteBatch writeBatch = new();

        foreach (RaftLog log in logs)
        {
            string index = log.Id.ToString("D20");

            writeBatch.Put(Encoding.UTF8.GetBytes(index), Serialize(new()
            {
                Partition = partitionId,
                Id = log.Id,
                Term = log.Term,
                Type = (int)log.Type,
                LogType = log.LogType,
                Log = UnsafeByteOperations.UnsafeWrap(log.LogData),
                TimePhysical = log.Time.L,
                TimeCounter = log.Time.C
            }), cf: columnFamilyHandle);
        }
        
        db.Write(writeBatch, DefaultWriteOptions);

        return RaftOperationStatus.Success;
    }

    public long GetMaxLog(int partitionId)
    {
        int shardId = partitionId % MaxShards;
        ColumnFamilyHandle? columnFamilyHandle = db.GetColumnFamily("shard" + shardId);
        
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
        int shardId = partitionId % MaxShards;
        ColumnFamilyHandle? columnFamilyHandle = db.GetColumnFamily("shard" + shardId);
        
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

    private long GetLastCheckpoint(int partitionId, ColumnFamilyHandle? columnFamilyHandle)
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