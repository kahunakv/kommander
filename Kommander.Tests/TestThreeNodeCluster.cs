
using Nixie;
using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;

namespace Kommander.Tests;

public class TestThreeNodeCluster
{
    private readonly ILogger<IRaft> logger;
    
    private int totalLeaderReceived;

    private int totalFollowersReceived;
    
    public TestThreeNodeCluster()
    {
        ILoggerFactory loggerFactory1 = LoggerFactory.Create(builder =>
        {
            builder
                //.AddFilter("Kommander.IRaft", LogLevel.Debug)
                //.AddFilter("Kommander", LogLevel.Debug)
                .SetMinimumLevel(LogLevel.Debug)
                .AddConsole();
        });

        logger = loggerFactory1.CreateLogger<IRaft>();
    }
    
    private static IRaft GetNode1(InMemoryCommunication communication, ILogger<IRaft> logger)
    {
        ActorSystem actorSystem = new(logger: logger);
        
        RaftConfiguration config = new()
        {
            NodeId = "node1",
            Host = "localhost",
            Port = 8001,
            MaxPartitions = 1
        };
        
        RaftManager node = new(
            actorSystem, 
            config, 
            new StaticDiscovery([new("localhost:8002"), new("localhost:8003")]),
            new InMemoryWAL(),
            communication,
            new HybridLogicalClock(),
            logger
        );

        return node;
    }
    
    private static RaftManager GetNode2(InMemoryCommunication communication, ILogger<IRaft> logger)
    {
        ActorSystem actorSystem = new(logger: logger);
        
        RaftConfiguration config = new()
        {
            NodeId = "node2",
            Host = "localhost",
            Port = 8002,
            MaxPartitions = 1
        };
        
        RaftManager node = new(
            actorSystem, 
            config, 
            new StaticDiscovery([new("localhost:8001"), new("localhost:8003")]),
            new InMemoryWAL(),
            communication,
            new HybridLogicalClock(),
            logger
        );

        return node;
    }
    
    private static RaftManager GetNode3(InMemoryCommunication communication, ILogger<IRaft> logger)
    {
        ActorSystem actorSystem = new(logger: logger);
        
        RaftConfiguration config = new()
        {
            NodeId = "node3",
            Host = "localhost",
            Port = 8003,
            MaxPartitions = 1
        };
        
        RaftManager node = new(
            actorSystem, 
            config, 
            new StaticDiscovery([new("localhost:8001"), new("localhost:8002")]),
            new InMemoryWAL(),
            communication,
            new HybridLogicalClock(),
            logger
        );

        return node;
    }
    
    /*[Fact]
    public async Task TestJoinCluster()
    {
        InMemoryCommunication communication = new();
        
        RaftManager node1 = GetNode1(communication);
        RaftManager node2 = GetNode2(communication);
        RaftManager node3 = GetNode3(communication);

        await node1.JoinCluster();
        await node2.JoinCluster();
        await node3.JoinCluster();
    }*/
    
    [Fact]
    public async Task TestJoinClusterAndDecideLeader()
    {
        InMemoryCommunication communication = new();
        
        IRaft node1 = GetNode1(communication, logger);
        IRaft node2 = GetNode2(communication, logger);
        IRaft node3 = GetNode3(communication, logger);

        await node1.JoinCluster();
        await node2.JoinCluster();
        await node3.JoinCluster();

        await node1.UpdateNodes();
        await node2.UpdateNodes();
        await node3.UpdateNodes();
        
        communication.SetNodes(new()
        {
            { "localhost:8001", node1 }, 
            { "localhost:8002", node2 },
            { "localhost:8003", node3 }
        });

        while (true)
        {
            if (await node1.AmILeader(0, CancellationToken.None) || await node2.AmILeader(0, CancellationToken.None) || await node3.AmILeader(0, CancellationToken.None))
                break;
            
            await Task.Delay(1000);
        }

        IRaft? leader = await GetLeader([node1, node2, node3]);
        Assert.NotNull(leader);
        
        List<IRaft> followers = await GetFollowers([node1, node2, node3]);
        Assert.NotEmpty(followers);
        Assert.Equal(2, followers.Count);

        node1.ActorSystem.Dispose();
        node2.ActorSystem.Dispose();
        node3.ActorSystem.Dispose();
    }
    
    [Fact]
    public async Task TestJoinClusterSimulAndDecideLeader()
    {
        InMemoryCommunication communication = new();
        
        IRaft node1 = GetNode1(communication, logger);
        IRaft node2 = GetNode2(communication, logger);
        IRaft node3 = GetNode3(communication, logger);

        await Task.WhenAll([node1.JoinCluster(), node2.JoinCluster(), node3.JoinCluster()]);

        await node1.UpdateNodes();
        await node2.UpdateNodes();
        await node3.UpdateNodes();
        
        communication.SetNodes(new()
        {
            { "localhost:8001", node1 }, 
            { "localhost:8002", node2 },
            { "localhost:8003", node3 }
        });

        while (true)
        {
            if (await node1.AmILeader(0, CancellationToken.None) || await node2.AmILeader(0, CancellationToken.None) || await node3.AmILeader(0, CancellationToken.None))
                break;
            
            await Task.Delay(1000);
        }

        IRaft? leader = await GetLeader([node1, node2, node3]);
        Assert.NotNull(leader);
        
        List<IRaft> followers = await GetFollowers([node1, node2, node3]);
        Assert.NotEmpty(followers);
        Assert.Equal(2, followers.Count);

        long maxId = await node1.WalAdapter.GetMaxLog(0);
        Assert.Equal(0, maxId);
        
        maxId = await node2.WalAdapter.GetMaxLog(0);
        Assert.Equal(0, maxId);
        
        maxId = await node3.WalAdapter.GetMaxLog(0);
        Assert.Equal(0, maxId);
        
        node1.ActorSystem.Dispose();
        node2.ActorSystem.Dispose();
        node3.ActorSystem.Dispose();
    }
    
    [Fact]
    public async Task TestJoinClusterAndReplicateLogs()
    {
        InMemoryCommunication communication = new();
        
        IRaft node1 = GetNode1(communication, logger);
        IRaft node2 = GetNode2(communication, logger);
        IRaft node3 = GetNode3(communication, logger);

        await Task.WhenAll([node1.JoinCluster(), node2.JoinCluster(), node3.JoinCluster()]);

        await node1.UpdateNodes();
        await node2.UpdateNodes();
        await node3.UpdateNodes();
        
        communication.SetNodes(new()
        {
            { "localhost:8001", node1 }, 
            { "localhost:8002", node2 },
            { "localhost:8003", node3 }
        });

        while (true)
        {
            if (await node1.AmILeader(0, CancellationToken.None) || await node2.AmILeader(0, CancellationToken.None) || await node3.AmILeader(0, CancellationToken.None))
                break;
            
            await Task.Delay(1000);
        }

        IRaft? leader = await GetLeader([node1, node2, node3]);
        Assert.NotNull(leader);

        List<IRaft> followers = await GetFollowers([node1, node2, node3]);
        Assert.NotEmpty(followers);
        Assert.Equal(2, followers.Count);
        
        leader.OnReplicationReceived += log =>
        {
            Interlocked.Increment(ref totalLeaderReceived);
            return Task.FromResult(true);
        };
        
        foreach (IRaft follower in followers)
            follower.OnReplicationReceived += log =>
            {
                  Interlocked.Increment(ref totalFollowersReceived);
                  return Task.FromResult(true);
            };

        long maxId = await node1.WalAdapter.GetMaxLog(0);
        Assert.Equal(0, maxId);
        
        maxId = await node2.WalAdapter.GetMaxLog(0);
        Assert.Equal(0, maxId);
        
        maxId = await node3.WalAdapter.GetMaxLog(0);
        Assert.Equal(0, maxId);
        
        (bool success, RaftOperationStatus status, long commitLogId) response = await leader.ReplicateLogs(0, "Greeting", "Hello World"u8.ToArray());
        Assert.True(response.success);
        
        Assert.Equal(RaftOperationStatus.Success, response.status);
        Assert.Equal(1, response.commitLogId);
        
        node1.ActorSystem.Dispose();
        node2.ActorSystem.Dispose();
        node3.ActorSystem.Dispose();
    }

    private static async Task<IRaft?> GetLeader(IRaft[] nodes)
    {
        foreach (IRaft node in nodes)
        {
            if (await node.AmILeader(0, CancellationToken.None))
                return node;
        }

        return null;
    }
    
    private static async Task<List<IRaft>> GetFollowers(IRaft[] nodes)
    {
        List<IRaft> followers = [];
        
        foreach (IRaft node in nodes)
        {
            if (!await node.AmILeader(0, CancellationToken.None))
                followers.Add(node);
        }

        return followers;
    }
}