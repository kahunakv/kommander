
using System.Text;
using Google.Protobuf;
using Kommander.Data;
using Kommander.WAL.Protos;
using RocksDbSharp;

namespace Kommander.WAL;

public class RocksDbWAL : IWAL
{
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
        
        for (int i = 0; i < 32; i++)
            columnFamilies.Add("shard" + i, new());
        
        this.db = RocksDb.Open(dbOptions, $"{path}/{revision}", columnFamilies);
    }

    public async IAsyncEnumerable<RaftLog> ReadLogs(int partitionId)
    {
        int shardId = partitionId % 32;
        ColumnFamilyHandle? columnFamilyHandle = db.GetColumnFamily("shard" + shardId);
        
        long maxId = await GetLastCheckpoint(partitionId, columnFamilyHandle).ConfigureAwait(false);

        using Iterator? iterator = db.NewIterator(cf: columnFamilyHandle);
        iterator.SeekToFirst();  // Move to the first key

        while (iterator.Valid())
        {
            RaftLogMessage message = Unserializer(iterator.Value());
            
            if (message.Partition != partitionId)
            {
                iterator.Next();
                continue;
            }

            if (message.Id < maxId)
            {
                iterator.Next();
                continue;
            }

            yield return new RaftLog()
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
        
        int shardId = partitionId % 32;
        ColumnFamilyHandle? columnFamilyHandle = db.GetColumnFamily("shard" + shardId);
        
        using Iterator? iterator = db.NewIterator(cf: columnFamilyHandle);
        iterator.SeekToFirst();  // Move to the first key

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

            yield return new RaftLog()
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

    public Task Append(int partitionId, RaftLog log)
    {
        int shardId = partitionId % 32;
        ColumnFamilyHandle? columnFamilyHandle = db.GetColumnFamily("shard" + shardId);
        
        string x = log.Id.ToString("D20");
        Console.WriteLine($"Appending {x}");
        
        db.Put(Encoding.UTF8.GetBytes(x), Serialize(new RaftLogMessage()
        {
            Partition = partitionId,
            Id = log.Id,
            Term = log.Term,
            Type = (int)log.Type,
            LogType = log.LogType,
            Log = ByteString.CopyFrom(log.LogData),
            TimePhysical = log.Time.L,
            TimeCounter = log.Time.C
        }), cf: columnFamilyHandle);

        return Task.CompletedTask;
    }

    public async Task AppendUpdate(int partitionId, RaftLog log)
    {
        await Append(partitionId, log).ConfigureAwait(false);
    }

    public Task<long> GetMaxLog(int partitionId)
    {
        int shardId = partitionId % 32;
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
        int shardId = partitionId % 32;
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
            
            if (message.Type == (int)RaftLogType.Checkpoint)
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