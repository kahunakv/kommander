
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

    public async IAsyncEnumerable<RaftLog> ReadLogs(int partitionId)
    {
        int shardId = partitionId % MaxShards;
        ColumnFamilyHandle? columnFamilyHandle = db.GetColumnFamily("shard" + shardId);
        
        long lastCheckpoint = await GetLastCheckpoint(partitionId, columnFamilyHandle).ConfigureAwait(false);

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

            yield return new()
            {
                Id = message.Id,
                Term = message.Term,
                Type = (RaftLogType)message.Type,
                Time = new(message.TimePhysical, message.TimeCounter),
                LogType = message.LogType,
                LogData = message.Log.ToByteArray(),
            };
            
            iterator.Next();
        }
    }

    public async IAsyncEnumerable<RaftLog> ReadLogsRange(int partitionId, long startLogIndex)
    {
        await Task.CompletedTask;
        
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

            yield return new()
            {
                Id = message.Id,
                Term = message.Term,
                Type = (RaftLogType)message.Type,
                Time = new(message.TimePhysical, message.TimeCounter),
                LogType = message.LogType,
                LogData = message.Log.ToByteArray(),
            };
            
            iterator.Next();

            counter++;

            if (counter > MaxNumberOfRangedEntries)
                break;
        }
    }

    public Task Propose(int partitionId, RaftLog log)
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

        return Task.CompletedTask;
    }
    
    public Task Commit(int partitionId, RaftLog log)
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

        return Task.CompletedTask;
    }
    
    public Task Rollback(int partitionId, RaftLog log)
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

        return Task.CompletedTask;
    }

    public Task ProposeMany(int partitionId, List<RaftLog> logs)
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
        
        return Task.CompletedTask;
    }

    public Task CommitMany(int partitionId, List<RaftLog> logs)
    {
        if (logs.Count == 0)
            return Task.CompletedTask;
        
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
        
        return Task.CompletedTask;
    }

    public Task RollbackMany(int partitionId, List<RaftLog> logs)
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
        
        return Task.CompletedTask;
    }

    public Task<long> GetMaxLog(int partitionId)
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
            
            return Task.FromResult(message.Id);
        }

        return Task.FromResult<long>(0);
    }

    public Task<long> GetCurrentTerm(int partitionId)
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
            
            return Task.FromResult(message.Term);
        }

        return Task.FromResult<long>(0);
    }

    private Task<long> GetLastCheckpoint(int partitionId, ColumnFamilyHandle? columnFamilyHandle)
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
                return Task.FromResult(message.Id);
            
            iterator.Next();
        }

        return Task.FromResult<long>(-1);
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