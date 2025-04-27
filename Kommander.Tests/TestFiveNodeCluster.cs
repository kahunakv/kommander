
using Kommander.Communication.Memory;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;
using Nixie;

namespace Kommander.Tests;

public class TestFiveNodeCluster
{
    private readonly ILogger<IRaft> logger;
    
    private int totalLeaderReceived;

    private int totalFollowersReceived;
    
    public TestFiveNodeCluster(ITestOutputHelper outputHelper)
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
        (IRaft node1, IRaft node2, IRaft node3, IRaft node4, IRaft node5) = await AssembleFiveNodeCluster(walStorage, partitions);
        
        await node1.LeaveCluster(true);
        await node2.LeaveCluster(true);
        await node3.LeaveCluster(true);
        await node4.LeaveCluster(true);
        await node5.LeaveCluster(true);
    }
    
    [Theory, CombinatorialData]
    public async Task TestJoinClusterAndMultiReplicateLogs(
        [CombinatorialValues("memory", "sqlite", "rocksdb")] string walStorage,
        [CombinatorialValues(1, 8, 16)] int partitions
    )
    {
        (IRaft node1, IRaft node2, IRaft node3, IRaft node4, IRaft node5) = await AssembleFiveNodeCluster(walStorage, partitions);
        
        IRaft? leader = await GetLeader(1, [node1, node2, node3, node4, node5]);
        Assert.NotNull(leader);

        List<IRaft> followers = await GetFollowers([node1, node2, node3, node4, node5]);
        Assert.NotEmpty(followers);
        Assert.Equal(4, followers.Count);
        
        await node1.LeaveCluster(true);
        await node2.LeaveCluster(true);
        await node3.LeaveCluster(true);
        await node4.LeaveCluster(true);
        await node5.LeaveCluster(true);
    }
    
    private async Task<(IRaft, IRaft, IRaft, IRaft, IRaft)> AssembleFiveNodeCluster(string walStorage, int partitions)
    {
        InMemoryCommunication communication = new();
        
        IRaft node1 = GetNode1(communication, walStorage, partitions, logger);
        IRaft node2 = GetNode2(communication, walStorage, partitions, logger);
        IRaft node3 = GetNode3(communication, walStorage, partitions, logger);
        IRaft node4 = GetNode4(communication, walStorage, partitions, logger);
        IRaft node5 = GetNode5(communication, walStorage, partitions, logger);

        communication.SetNodes(new()
        {
            { "localhost:8001", node1 }, 
            { "localhost:8002", node2 },
            { "localhost:8003", node3 },
            { "localhost:8004", node4 },
            { "localhost:8005", node5 }
        });

        await Task.WhenAll([node1.JoinCluster(), node2.JoinCluster(), node3.JoinCluster(), node4.JoinCluster(), node5.JoinCluster()]);

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
        
        return (node1, node2, node3, node4, node5);
    }
    
    private static IRaft GetNode1(InMemoryCommunication communication, string walStorage, int partitions, ILogger<IRaft> logger)
    {
        IWAL wal = GetWAL(walStorage, logger);
        
        ActorSystem actorSystem = new(logger: logger);
        
        RaftConfiguration config = new()
        {
            NodeName = "node1",
            NodeId = 1,
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
            new StaticDiscovery([new("localhost:8002"), new("localhost:8003"), new("localhost:8004"), new("localhost:8005")]),
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
            NodeName = "node2",
            NodeId = 2,
            Host = "localhost",
            Port = 8002,
            InitialPartitions = partitions,
            CompactEveryOperations = 100,
            CompactNumberEntries = 50
        };
        
        RaftManager node = new(
            actorSystem, 
            config, 
            new StaticDiscovery([new("localhost:8001"), new("localhost:8003"), new("localhost:8004"), new("localhost:8005")]),
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
            NodeName = "node3",
            NodeId = 3,
            Host = "localhost",
            Port = 8003,
            InitialPartitions = partitions,
            CompactEveryOperations = 100,
            CompactNumberEntries = 50
        };
        
        RaftManager node = new(
            actorSystem, 
            config, 
            new StaticDiscovery([new("localhost:8001"), new("localhost:8002"), new("localhost:8004"), new("localhost:8005")]),
            wal,
            communication,
            new HybridLogicalClock(),
            logger
        );

        return node;
    }
    
    private static RaftManager GetNode4(InMemoryCommunication communication, string walStorage, int partitions, ILogger<IRaft> logger)
    {
        IWAL wal = GetWAL(walStorage, logger);
        
        ActorSystem actorSystem = new(logger: logger);
        
        RaftConfiguration config = new()
        {
            NodeName = "node4",
            NodeId = 4,
            Host = "localhost",
            Port = 8004,
            InitialPartitions = partitions,
            CompactEveryOperations = 100,
            CompactNumberEntries = 50
        };
        
        RaftManager node = new(
            actorSystem, 
            config, 
            new StaticDiscovery([new("localhost:8001"), new("localhost:8002"), new("localhost:8003"), new("localhost:8005")]),
            wal,
            communication,
            new HybridLogicalClock(),
            logger
        );

        return node;
    }
    
    private static RaftManager GetNode5(InMemoryCommunication communication, string walStorage, int partitions, ILogger<IRaft> logger)
    {
        IWAL wal = GetWAL(walStorage, logger);
        
        ActorSystem actorSystem = new(logger: logger);
        
        RaftConfiguration config = new()
        {
            NodeName = "node5",
            NodeId = 5,
            Host = "localhost",
            Port = 8005,
            InitialPartitions = partitions,
            CompactEveryOperations = 100,
            CompactNumberEntries = 50
        };
        
        RaftManager node = new(
            actorSystem, 
            config, 
            new StaticDiscovery([new("localhost:8001"), new("localhost:8002"), new("localhost:8003"), new("localhost:8004")]),
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