
using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using MartinCostello.Logging.XUnit;
using Microsoft.Extensions.Logging;
using Nixie;

namespace Kommander.Tests;

[SuppressMessage("Performance", "CA1859:Use concrete types when possible for improved performance")]
public sealed class TestThreeNodeCluster
{
    private readonly ILogger<IRaft> logger;
    
    private int totalLeaderReceived;

    private int totalFollowersReceived;
    
    public TestThreeNodeCluster(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory1 = LoggerFactory.Create(builder =>
        {
            builder
                .AddXUnit(outputHelper)
                .SetMinimumLevel(LogLevel.Debug);
        });

        logger = loggerFactory1.CreateLogger<IRaft>();
    }

    [Theory, CombinatorialData]
    public async Task TestJoinClusterAndDecideLeaderOnManyPartitions(
        [CombinatorialValues("memory", "sqlite", "rocksdb")] string walStorage,
        [CombinatorialValues(1, 8, 16)] int partitions
    )
    {
        (IRaft node1, IRaft node2, IRaft node3) = await AssembleThreNodeCluster(walStorage, partitions);
        
        await node1.LeaveCluster(true);
        await node2.LeaveCluster(true);
        await node3.LeaveCluster(true);
    }
    
    [Theory, CombinatorialData]
    public async Task TestJoinClusterAndMultiReplicateLogs(
        [CombinatorialValues("memory", "sqlite", "rocksdb")] string walStorage,
        [CombinatorialValues(1, 8, 16)] int partitions,
        [CombinatorialValues(100, 250)] int entries
        
        //[CombinatorialValues("sqlite")] string walStorage,
        //[CombinatorialValues(8)] int partitions,
        //[CombinatorialValues(250)] int entries
    )
    {
        (IRaft node1, IRaft node2, IRaft node3) = await AssembleThreNodeCluster(walStorage, partitions);

        IRaft? leader = await GetLeader(1, [node1, node2, node3]);
        Assert.NotNull(leader);

        List<IRaft> followers = await GetFollowers([node1, node2, node3]);
        Assert.NotEmpty(followers);
        Assert.Equal(2, followers.Count);
        
        leader.OnReplicationReceived += (_, _) =>
        {
            Interlocked.Increment(ref totalLeaderReceived);
            return Task.FromResult(true);
        };

        ConcurrentBag<long> logsReceived = [];
        
        foreach (IRaft follower in followers)
            follower.OnReplicationReceived += (_, log) =>
            {
                logsReceived.Add(log.Id);

                Interlocked.Increment(ref totalFollowersReceived);
                return Task.FromResult(true);
            };

        long maxId = node1.WalAdapter.GetMaxLog(1);
        Assert.Equal(0, maxId);
        
        maxId = node2.WalAdapter.GetMaxLog(1);
        Assert.Equal(0, maxId);
        
        maxId = node3.WalAdapter.GetMaxLog(1);
        Assert.Equal(0, maxId);

        byte[] data = "Hello World"u8.ToArray();

        int expectedId = 1;

        for (int i = 0; i < entries; i++)
        {
            RaftReplicationResult response = await leader.ReplicateLogs(
                1, 
                "Greeting", 
                data, 
                cancellationToken: TestContext.Current.CancellationToken
            );
            
            Assert.Equal(RaftOperationStatus.Success, response.Status);
            Assert.Equal(expectedId++, response.LogIndex);

            if (expectedId % 50 == 0)
            {
                response = await leader.ReplicateCheckpoint(
                    1, 
                    cancellationToken: TestContext.Current.CancellationToken
                );
            
                Assert.Equal(RaftOperationStatus.Success, response.Status);
                Assert.Equal(expectedId++, response.LogIndex);
            }
        }
        
        maxId = node1.WalAdapter.GetMaxLog(1);
        Assert.Equal(entries + (entries / 50), maxId);
        
        maxId = node2.WalAdapter.GetMaxLog(1);
        Assert.Equal(entries + (entries / 50), maxId);
        
        maxId = node3.WalAdapter.GetMaxLog(1);
        Assert.Equal(entries + (entries / 50), maxId);

        await Task.Delay(5000, cancellationToken: TestContext.Current.CancellationToken);
        
        //Assert.Equal(200, totalFollowersReceived);
        //Assert.Equal(0, totalLeaderReceived);

        //if (totalLeaderReceived > (entries + (entries / 50)))
        //    await File.AppendAllTextAsync("/tmp/u.txt", string.Join(",", logsReceived) + "\n", cancellationToken: TestContext.Current.CancellationToken);
        
        await node1.LeaveCluster(true);
        await node2.LeaveCluster(true);
        await node3.LeaveCluster(true);
    }
    
    [Theory, CombinatorialData]
    public async Task TestJoinClusterAndProposeReplicateLogs(
        [CombinatorialValues("memory", "sqlite", "rocksdb")] string walStorage,
        [CombinatorialValues(1, 8, 16)] int partitions
    )
    {
        (IRaft node1, IRaft node2, IRaft node3) = await AssembleThreNodeCluster(walStorage, partitions);

        IRaft? leader = await GetLeader(1, [node1, node2, node3]);
        Assert.NotNull(leader);

        List<IRaft> followers = await GetFollowers([node1, node2, node3]);
        Assert.NotEmpty(followers);
        Assert.Equal(2, followers.Count);
        
        byte[] data = "Hello World"u8.ToArray();
        
        leader.OnReplicationReceived += (_, log) =>
        {
            Assert.Equal("Greeting", log.LogType);
            Assert.Equal(data, log.LogData);
            
            Interlocked.Increment(ref totalLeaderReceived);
            return Task.FromResult(true);
        };
        
        foreach (IRaft follower in followers)
            follower.OnReplicationReceived += (_, _) =>
            {
                  Interlocked.Increment(ref totalFollowersReceived);
                  return Task.FromResult(true);
            };

        long maxId = node1.WalAdapter.GetMaxLog(1);
        Assert.Equal(0, maxId);
        
        maxId = node2.WalAdapter.GetMaxLog(1);
        Assert.Equal(0, maxId);
        
        maxId = node3.WalAdapter.GetMaxLog(1);
        Assert.Equal(0, maxId);

        for (int i = 1; i <= partitions; i++)
        {
            leader = await GetLeader(i, [node1, node2, node3]);
            Assert.NotNull(leader);
            
            RaftReplicationResult response = await leader.ReplicateLogs(i, "Greeting", data, false, TestContext.Current.CancellationToken);
            
            Assert.True(response.Success);

            Assert.Equal(RaftOperationStatus.Success, response.Status);
            Assert.Equal(1, response.LogIndex);

            Assert.Equal(0, totalFollowersReceived);
            Assert.Equal(0, totalLeaderReceived);
        }

        await node1.LeaveCluster(true);
        await node2.LeaveCluster(true);
        await node3.LeaveCluster(true);
    }
    
    [Theory, CombinatorialData]
    public async Task TestJoinClusterAndProposeReplicateLogsRace(
        [CombinatorialValues("memory", "sqlite", "rocksdb")] string walStorage,
        [CombinatorialValues(1, 8, 16)] int partitions
    )
    {
        (IRaft node1, IRaft node2, IRaft node3) = await AssembleThreNodeCluster(walStorage, partitions);

        IRaft? leader = await GetLeader(0, [node1, node2, node3]);
        Assert.NotNull(leader);

        List<IRaft> followers = await GetFollowers([node1, node2, node3]);
        Assert.NotEmpty(followers);
        Assert.Equal(2, followers.Count);
        
        byte[] data = "Hello World"u8.ToArray();
        
        leader.OnReplicationReceived +=  (_, log) =>
        {
            Assert.Equal("Greeting", log.LogType);
            Assert.Equal(data, log.LogData);
            
            Interlocked.Increment(ref totalLeaderReceived);
            return Task.FromResult(true);
        };
        
        foreach (IRaft follower in followers)
            follower.OnReplicationReceived += (_, _) =>
            {
                  Interlocked.Increment(ref totalFollowersReceived);
                  return Task.FromResult(true);
            };

        long maxId = node1.WalAdapter.GetMaxLog(1);
        Assert.Equal(0, maxId);
        
        maxId = node2.WalAdapter.GetMaxLog(1);
        Assert.Equal(0, maxId);
        
        maxId = node3.WalAdapter.GetMaxLog(1);
        Assert.Equal(0, maxId);

        for (int i = 1; i <= partitions; i++)
        {
            leader = await GetLeader(i, [node1, node2, node3]);
            Assert.NotNull(leader);

            RaftReplicationResult[] responses = await Task.WhenAll(
                leader.ReplicateLogs(i, "Greeting", data, cancellationToken: TestContext.Current.CancellationToken),
                leader.ReplicateLogs(i, "Greeting", data, cancellationToken: TestContext.Current.CancellationToken),
                leader.ReplicateLogs(i, "Greeting", data, cancellationToken: TestContext.Current.CancellationToken)
            );

            foreach (RaftReplicationResult response in responses)
            {
                if (!response.Success)
                    throw new Exception(response.Status.ToString());

                Assert.True(response.Success);
                Assert.Equal(RaftOperationStatus.Success, response.Status);
            }
        }
        
        await node1.LeaveCluster(true);
        await node2.LeaveCluster(true);
        await node3.LeaveCluster(true);
    }
    
    [Theory, CombinatorialData]
    public async Task TestJoinClusterAndProposeReplicateLogsRace2(
        [CombinatorialValues("memory", "sqlite", "rocksdb")] string walStorage,
        [CombinatorialValues(1, 8, 16)] int partitions
    )
    {
        (IRaft node1, IRaft node2, IRaft node3) = await AssembleThreNodeCluster(walStorage, partitions);

        IRaft? leader = await GetLeader(0, [node1, node2, node3]);
        Assert.NotNull(leader);

        List<IRaft> followers = await GetFollowers([node1, node2, node3]);
        Assert.NotEmpty(followers);
        Assert.Equal(2, followers.Count);
        
        byte[] data = "Hello World"u8.ToArray();
        
        leader.OnReplicationReceived += (_, log) =>
        {
            Assert.Equal("Greeting", log.LogType);
            Assert.Equal(data, log.LogData);
            
            Interlocked.Increment(ref totalLeaderReceived);
            return Task.FromResult(true);
        };
        
        foreach (IRaft follower in followers)
            follower.OnReplicationReceived += (_, _) =>
            {
                  Interlocked.Increment(ref totalFollowersReceived);
                  return Task.FromResult(true);
            };

        long maxId = node1.WalAdapter.GetMaxLog(1);
        Assert.Equal(0, maxId);
        
        maxId = node2.WalAdapter.GetMaxLog(1);
        Assert.Equal(0, maxId);
        
        maxId = node3.WalAdapter.GetMaxLog(1);
        Assert.Equal(0, maxId);

        for (int i = 1; i <= partitions; i++)
        {
            leader = await GetLeader(i, [node1, node2, node3]);
            Assert.NotNull(leader);

            RaftReplicationResult[] responses = await Task.WhenAll(
                leader.ReplicateLogs(i, "Greeting", data, cancellationToken: TestContext.Current.CancellationToken),
                leader.ReplicateLogs(i, "Greeting", data, cancellationToken: TestContext.Current.CancellationToken),
                leader.ReplicateLogs(i, "Greeting", data, cancellationToken: TestContext.Current.CancellationToken),
                leader.ReplicateLogs(i, "Greeting", data, cancellationToken: TestContext.Current.CancellationToken),
                leader.ReplicateLogs(i, "Greeting", data, cancellationToken: TestContext.Current.CancellationToken),
                leader.ReplicateLogs(i, "Greeting", data, cancellationToken: TestContext.Current.CancellationToken),
                leader.ReplicateLogs(i, "Greeting", data, cancellationToken: TestContext.Current.CancellationToken),
                leader.ReplicateLogs(i, "Greeting", data, cancellationToken: TestContext.Current.CancellationToken),
                leader.ReplicateLogs(i, "Greeting", data, cancellationToken: TestContext.Current.CancellationToken)
            );

            foreach (RaftReplicationResult response in responses)
            {
                if (!response.Success)
                    throw new Exception(response.Status.ToString());

                Assert.True(response.Success);
                Assert.Equal(RaftOperationStatus.Success, response.Status);
            }
        }
        
        await node1.LeaveCluster(true);
        await node2.LeaveCluster(true);
        await node3.LeaveCluster(true);
    }
    
    [Theory, CombinatorialData]
    public async Task TestJoinClusterAndProposeReplicateLogsRace3(
        [CombinatorialValues("memory", "sqlite", "rocksdb")] string walStorage,
        [CombinatorialValues(1, 8, 16)] int partitions
    )
    {
        (IRaft node1, IRaft node2, IRaft node3) = await AssembleThreNodeCluster(walStorage, partitions);

        IRaft? leader = await GetLeader(0, [node1, node2, node3]);
        Assert.NotNull(leader);

        List<IRaft> followers = await GetFollowers([node1, node2, node3]);
        Assert.NotEmpty(followers);
        Assert.Equal(2, followers.Count);
        
        byte[] data = "Hello World"u8.ToArray();
        
        leader.OnReplicationReceived += (_, log) =>
        {
            Assert.Equal("Greeting", log.LogType);
            Assert.Equal(data, log.LogData);
            
            Interlocked.Increment(ref totalLeaderReceived);
            return Task.FromResult(true);
        };
        
        foreach (IRaft follower in followers)
            follower.OnReplicationReceived += (_, _) =>
            {
                  Interlocked.Increment(ref totalFollowersReceived);
                  return Task.FromResult(true);
            };

        long maxId = node1.WalAdapter.GetMaxLog(1);
        Assert.Equal(0, maxId);
        
        maxId = node2.WalAdapter.GetMaxLog(1);
        Assert.Equal(0, maxId);
        
        maxId = node3.WalAdapter.GetMaxLog(1);
        Assert.Equal(0, maxId);

        for (int i = 1; i <= partitions; i++)
        {
            leader = await GetLeader(i, [node1, node2, node3]);
            Assert.NotNull(leader);

            RaftReplicationResult[] responses = await Task.WhenAll(
                leader.ReplicateLogs(i, "Greeting", data, cancellationToken: TestContext.Current.CancellationToken),
                leader.ReplicateLogs(i, "Greeting", data, cancellationToken: TestContext.Current.CancellationToken),
                leader.ReplicateLogs(i, "Greeting", data, cancellationToken: TestContext.Current.CancellationToken),
                leader.ReplicateCheckpoint(i, cancellationToken: TestContext.Current.CancellationToken),
                leader.ReplicateLogs(i, "Greeting", data, cancellationToken: TestContext.Current.CancellationToken),
                leader.ReplicateLogs(i, "Greeting", data, cancellationToken: TestContext.Current.CancellationToken),
                leader.ReplicateLogs(i, "Greeting", data, cancellationToken: TestContext.Current.CancellationToken),
                leader.ReplicateLogs(i, "Greeting", data, cancellationToken: TestContext.Current.CancellationToken),
                leader.ReplicateCheckpoint(i, cancellationToken: TestContext.Current.CancellationToken),
                leader.ReplicateLogs(i, "Greeting", data, cancellationToken: TestContext.Current.CancellationToken),
                leader.ReplicateLogs(i, "Greeting", data, cancellationToken: TestContext.Current.CancellationToken),
                leader.ReplicateCheckpoint(i, cancellationToken: TestContext.Current.CancellationToken)
            );

            foreach (RaftReplicationResult response in responses)
            {
                if (!response.Success)
                    throw new Exception(response.Status.ToString());

                Assert.True(response.Success);
                Assert.Equal(RaftOperationStatus.Success, response.Status);
            }
        }
        
        await node1.LeaveCluster(true);
        await node2.LeaveCluster(true);
        await node3.LeaveCluster(true);
    }

    private async Task<(IRaft, IRaft, IRaft)> AssembleThreNodeCluster(string walStorage, int partitions)
    {
        InMemoryCommunication communication = new();
        
        IRaft node1 = GetNode1(communication, walStorage, partitions, logger);
        IRaft node2 = GetNode2(communication, walStorage, partitions, logger);
        IRaft node3 = GetNode3(communication, walStorage, partitions, logger);

        communication.SetNodes(new()
        {
            { "localhost:8001", node1 }, 
            { "localhost:8002", node2 },
            { "localhost:8003", node3 }
        });

        await Task.WhenAll([node1.JoinCluster(), node2.JoinCluster(), node3.JoinCluster()]);

        for (int i = 1; i <= partitions; i++)
        {
            while (true)
            {
                if (await node1.AmILeader(i, cancellationToken: TestContext.Current.CancellationToken) ||
                    await node2.AmILeader(i, cancellationToken: TestContext.Current.CancellationToken) ||
                    await node3.AmILeader(i, cancellationToken: TestContext.Current.CancellationToken))
                    break;

                await Task.Delay(100, cancellationToken: TestContext.Current.CancellationToken);
            }
        }
        
        return (node1, node2, node3);
    }
    
    private static IRaft GetNode1(InMemoryCommunication communication, string walStorage, int partitions, ILogger<IRaft> logger)
    {
        IWAL wal = GetWAL(walStorage, logger);
        
        ActorSystem actorSystem = new(logger: logger);
        
        RaftConfiguration config = new()
        {
            NodeId = "node1",
            Host = "localhost",
            Port = 8001,
            InitialPartitions = partitions,
            CompactEveryOperations = 100,
            CompactNumberEntries = 50,
            StartElectionTimeout = 500,
            EndElectionTimeout = 1000,
        };
        
        RaftManager node = new(
            actorSystem, 
            config, 
            new StaticDiscovery([new("localhost:8002"), new("localhost:8003")]),
            wal,
            communication,
            new HybridLogicalClock(),
            logger
        );

        return node;
    }
    
    private static RaftManager GetNode2(InMemoryCommunication communication, string walStorage, int partitions, ILogger<IRaft> logger)
    {
        IWAL wal = GetWAL(walStorage, logger);
        
        ActorSystem actorSystem = new(logger: logger);
        
        RaftConfiguration config = new()
        {
            NodeId = "node2",
            Host = "localhost",
            Port = 8002,
            InitialPartitions = partitions,
            CompactEveryOperations = 100,
            CompactNumberEntries = 50
        };
        
        RaftManager node = new(
            actorSystem, 
            config, 
            new StaticDiscovery([new("localhost:8001"), new("localhost:8003")]),
            wal,
            communication,
            new HybridLogicalClock(),
            logger
        );

        return node;
    }
    
    private static RaftManager GetNode3(InMemoryCommunication communication, string walStorage, int partitions, ILogger<IRaft> logger)
    {
        IWAL wal = GetWAL(walStorage, logger);
        
        ActorSystem actorSystem = new(logger: logger);
        
        RaftConfiguration config = new()
        {
            NodeId = "node3",
            Host = "localhost",
            Port = 8003,
            InitialPartitions = partitions,
            CompactEveryOperations = 100,
            CompactNumberEntries = 50
        };
        
        RaftManager node = new(
            actorSystem, 
            config, 
            new StaticDiscovery([new("localhost:8001"), new("localhost:8002")]),
            wal,
            communication,
            new HybridLogicalClock(),
            logger
        );

        return node;
    }

    private static IWAL GetWAL(string walStorage, ILogger<IRaft> logger)
    {
        return walStorage switch
        {
            "memory" => new InMemoryWAL(logger),
            "sqlite" => new SqliteWAL("/tmp", Guid.NewGuid().ToString(), logger),
            "rocksdb" => new RocksDbWAL("/tmp", Guid.NewGuid().ToString(), logger),
            _ => throw new ArgumentException($"Unknown wal: {walStorage}")
        };
    }
    
    private static async Task<IRaft?> GetLeader(int partitionId, IRaft[] nodes)
    {
        foreach (IRaft node in nodes)
        {
            if (await node.AmILeader(partitionId, CancellationToken.None))
                return node;
        }

        return null;
    }
    
    private static async Task<List<IRaft>> GetFollowers(IRaft[] nodes)
    {
        List<IRaft> followers = [];
        
        foreach (IRaft node in nodes)
        {
            if (!await node.AmILeader(1, CancellationToken.None))
                followers.Add(node);
        }

        return followers;
    }
}
