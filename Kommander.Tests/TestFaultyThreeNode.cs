
using Nixie;
using Microsoft.Extensions.Logging;

using Kommander.Communication.Memory;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;

namespace Kommander.Tests;

public class TestFaultyThreeNode
{
    private readonly ILogger<IRaft> logger;
    
    public TestFaultyThreeNode()
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
            new DynamicDiscovery([new("localhost:8002"), new("localhost:8003")]),
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
            new DynamicDiscovery([new("localhost:8001"), new("localhost:8003")]),
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
            new DynamicDiscovery([new("localhost:8001"), new("localhost:8002")]),
            new InMemoryWAL(),
            communication,
            new HybridLogicalClock(),
            logger
        );

        return node;
    }
    
    [Fact]
    public async Task TestJoinClusterDecideLeaderAndRemoveNode()
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

        IRaft? leader = await GetLeader(0,[node1, node2, node3]);
        Assert.NotNull(leader);
        
        List<IRaft> followers = await GetFollowers(0,[node1, node2, node3]);
        Assert.NotEmpty(followers);
        Assert.Equal(2, followers.Count);
        
        leader.ActorSystem.Dispose();

        RaftNode nodeToRemove = new(leader.GetLocalEndpoint());
        
        foreach (IRaft follower in followers)
            ((DynamicDiscovery)follower.Discovery).RemoveNode(nodeToRemove);
        
        IRaft[] newFollowers = followers.ToArray();
        Assert.Equal(2, newFollowers.Length);
        
        while (true)
        {
            List<Task<bool>> p = followers.Select(node => node.AmILeader(0, CancellationToken.None).AsTask()).ToList();
            
            bool[] results = await Task.WhenAll(p);
            if (results.Any(isLeader => isLeader))
                break;

            await Task.Delay(1000);
        }
        
        IRaft? newLeader = await GetLeader(0, newFollowers);
        Assert.NotNull(newLeader);
        
        Assert.NotEqual(leader.GetLocalEndpoint(), newLeader.GetLocalEndpoint());

        node1.ActorSystem.Dispose();
        node2.ActorSystem.Dispose();
    }
    
    [Fact]
    public async Task TestJoinClusterDecideLeaderRemoveNodeAndRejoin()
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

        IRaft? leader = await GetLeader(0,[node1, node2, node3]);
        Assert.NotNull(leader);
        
        List<IRaft> followers = await GetFollowers(0,[node1, node2, node3]);
        Assert.NotEmpty(followers);
        Assert.Equal(2, followers.Count);
        
        leader.ActorSystem.Dispose();

        RaftNode nodeToRemove = new(leader.GetLocalEndpoint());
        
        //string x = Guid.NewGuid().ToString();

        foreach (IRaft follower in followers)
        {
            //Console.WriteLine($"> [{x}] {follower.GetLocalEndpoint()}");
            ((DynamicDiscovery)follower.Discovery).RemoveNode(nodeToRemove);
        }

        while (true)
        {
            List<Task<bool>> p = followers.Select(node => node.AmILeader(0, CancellationToken.None).AsTask()).ToList();
            
            bool[] results = await Task.WhenAll(p);
            if (results.Any(isLeader => isLeader))
                break;

            await Task.Delay(500);
        }
        
        IRaft[] newFollowers = followers.ToArray();
        Assert.Equal(2, newFollowers.Length);
        
        IRaft? newLeader = await GetLeader(0, newFollowers);
        Assert.NotNull(newLeader);
        
        Assert.NotEqual(leader.GetLocalEndpoint(), newLeader.GetLocalEndpoint());
        
        followers = await GetFollowers(0, newFollowers);
        Assert.NotEmpty(followers);
        Assert.Single(followers);

        IRaft? newNode = null;

        switch (nodeToRemove.Endpoint)
        {
            case "localhost:8001":
                node1 = GetNode1(communication, logger);
                newNode = node1;
                break;
            
            case "localhost:8002":
                node2 = GetNode2(communication, logger);
                newNode = node2;
                break;
            
            case "localhost:8003":
                node3 = GetNode3(communication, logger);
                newNode = node2;
                break;
        }
        
        Assert.NotNull(newNode);

        foreach (IRaft follower in followers)
        {
            Console.WriteLine("@ follower {0}", follower.GetLocalEndpoint());
            ((DynamicDiscovery)follower.Discovery).AddNode(new(newNode.GetLocalEndpoint()));
        }

        await Task.Delay(10000);

        await newNode.JoinCluster();

        await node1.UpdateNodes();
        await node2.UpdateNodes();
        await node3.UpdateNodes();

        IRaft? currentLeader = await GetLeader(0,[node1, node2, node3]);
        Assert.NotNull(currentLeader);

        //Assert.NotEqual(leader.GetLocalEndpoint(), newLeader.GetLocalEndpoint());
        Assert.Equal(currentLeader.GetLocalEndpoint(), newLeader.GetLocalEndpoint());

        followers = await GetFollowers(0,[node1, node2, node3]);
        Assert.NotEmpty(followers);
        Assert.Equal(2, followers.Count);

        node1.ActorSystem.Dispose();
        node2.ActorSystem.Dispose();
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
    
    private static async Task<List<IRaft>> GetFollowers(int partitionId, IRaft[] nodes)
    {
        //string uuid = Guid.NewGuid().ToString();
        
        //Console.WriteLine("[{0}] checking followers {1}", uuid, nodes.Length);
        
        List<IRaft> followers = [];
        
        foreach (IRaft node in nodes)
        {
            //Console.WriteLine("[{0}] checking if follower {1}", uuid, node.GetLocalEndpoint());
            
            if (!await node.AmILeader(partitionId, CancellationToken.None))
                followers.Add(node);
        }

        return followers;
    }
}