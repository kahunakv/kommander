
using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;
using Nixie;

namespace Kommander.Tests;

public sealed class TestThreeNodeClusterManyPartitions
{
    private readonly ILogger<IRaft> logger;
    
    private int totalLeaderReceived;

    private int totalFollowersReceived;
    
    public TestThreeNodeClusterManyPartitions()
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
    
    private static IRaft GetNode1(InMemoryCommunication communication, int partitions, ILogger<IRaft> logger)
    {
        ActorSystem actorSystem = new(logger: logger);
        
        RaftConfiguration config = new()
        {
            NodeId = "node1",
            Host = "localhost",
            Port = 8001,
            MaxPartitions = partitions
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
    
    private static RaftManager GetNode2(InMemoryCommunication communication, int partitions, ILogger<IRaft> logger)
    {
        ActorSystem actorSystem = new(logger: logger);
        
        RaftConfiguration config = new()
        {
            NodeId = "node2",
            Host = "localhost",
            Port = 8002,
            MaxPartitions = partitions
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
    
    private static RaftManager GetNode3(InMemoryCommunication communication, int partitions, ILogger<IRaft> logger)
    {
        ActorSystem actorSystem = new(logger: logger);
        
        RaftConfiguration config = new()
        {
            NodeId = "node3",
            Host = "localhost",
            Port = 8003,
            MaxPartitions = partitions
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
    
    [Theory]
    [InlineData(4)]
    [InlineData(8)]
    [InlineData(16)]
    public async Task TestJoinClusterAndDecideLeaderOnManyPartitions(int partitions)
    {
        InMemoryCommunication communication = new();
        
        IRaft node1 = GetNode1(communication, partitions, logger);
        IRaft node2 = GetNode2(communication, partitions, logger);
        IRaft node3 = GetNode3(communication, partitions, logger);

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

        for (int i = 0; i < partitions; i++)
        {
            while (true)
            {
                bool result = await node1.AmILeader(i, CancellationToken.None) ||
                              await node2.AmILeader(i, CancellationToken.None) ||
                              await node3.AmILeader(i, CancellationToken.None);

                if (result)
                    break;
                
                await Task.Delay(500);
            }
        }

        node1.ActorSystem.Dispose();
        node2.ActorSystem.Dispose();
        node3.ActorSystem.Dispose();
    }
    
    [Theory]
    [InlineData(4)]
    [InlineData(8)]
    [InlineData(16)]
    public async Task TestJoinClusterAndMultiReplicateLogs(int partitions)
    {
        InMemoryCommunication communication = new();
        
        IRaft node1 = GetNode1(communication, partitions, logger);
        IRaft node2 = GetNode2(communication, partitions, logger);
        IRaft node3 = GetNode3(communication, partitions, logger);

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

        for (int i = 0; i < partitions; i++)
        {
            while (true)
            {
                bool result = await node1.AmILeader(i, CancellationToken.None) ||
                              await node2.AmILeader(i, CancellationToken.None) ||
                              await node3.AmILeader(i, CancellationToken.None);

                if (result)
                    break;
                
                await Task.Delay(500);
            }
        }

        IRaft? leader = await GetLeader(0, [node1, node2, node3]);
        Assert.NotNull(leader);

        List<IRaft> followers = await GetFollowers([node1, node2, node3]);
        Assert.NotEmpty(followers);
        Assert.Equal(2, followers.Count);
        
        leader.OnReplicationReceived += _ =>
        {
            Interlocked.Increment(ref totalLeaderReceived);
            return Task.FromResult(true);
        };
        
        foreach (IRaft follower in followers)
            follower.OnReplicationReceived += _ =>
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

        byte[] data = "Hello World"u8.ToArray();

        for (int i = 0; i < 100; i++)
        {
            RaftReplicationResult response = await leader.ReplicateLogs(0, "Greeting", data);
            
            Assert.True(response.Success);
            Assert.Equal(RaftOperationStatus.Success, response.Status);
            
            Assert.Equal(i + 1, response.LogIndex);
        }
        
        maxId = await node1.WalAdapter.GetMaxLog(0);
        Assert.Equal(100, maxId);
        
        maxId = await node2.WalAdapter.GetMaxLog(0);
        Assert.Equal(100, maxId);
        
        maxId = await node3.WalAdapter.GetMaxLog(0);
        Assert.Equal(100, maxId);
        
        Assert.Equal(200, totalFollowersReceived);
        Assert.Equal(0, totalLeaderReceived);
        
        node1.ActorSystem.Dispose();
        node2.ActorSystem.Dispose();
        node3.ActorSystem.Dispose();
    }
    
    [Theory]
    [InlineData(4)]
    [InlineData(8)]
    public async Task TestJoinClusterAndProposeReplicateLogs(int partitions)
    {
        InMemoryCommunication communication = new();
        
        IRaft node1 = GetNode1(communication, partitions, logger);
        IRaft node2 = GetNode2(communication, partitions, logger);
        IRaft node3 = GetNode3(communication, partitions, logger);

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

        for (int i = 0; i < partitions; i++)
        {
            while (true)
            {
                bool result = await node1.AmILeader(i, CancellationToken.None) ||
                              await node2.AmILeader(i, CancellationToken.None) ||
                              await node3.AmILeader(i, CancellationToken.None);

                if (result)
                    break;
                
                await Task.Delay(500);
            }
        }

        IRaft? leader = await GetLeader(0, [node1, node2, node3]);
        Assert.NotNull(leader);

        List<IRaft> followers = await GetFollowers([node1, node2, node3]);
        Assert.NotEmpty(followers);
        Assert.Equal(2, followers.Count);
        
        byte[] data = "Hello World"u8.ToArray();
        
        leader.OnReplicationReceived += log =>
        {
            Assert.Equal("Greeting", log.LogType);;
            Assert.Equal(data, log.LogData);
            
            Interlocked.Increment(ref totalLeaderReceived);
            return Task.FromResult(true);
        };
        
        foreach (IRaft follower in followers)
            follower.OnReplicationReceived += _ =>
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

        for (int i = 0; i < partitions; i++)
        {
            leader = await GetLeader(i, [node1, node2, node3]);
            Assert.NotNull(leader);
            
            RaftReplicationResult response = await leader.ReplicateLogs(i, "Greeting", data, false);
            
            if (!response.Success)
                throw new Exception(response.Status.ToString());
            
            Assert.True(response.Success);

            Assert.Equal(RaftOperationStatus.Success, response.Status);
            Assert.Equal(1, response.LogIndex);

            Assert.Equal(0, totalFollowersReceived);
            Assert.Equal(0, totalLeaderReceived);
        }

        node1.ActorSystem.Dispose();
        node2.ActorSystem.Dispose();
        node3.ActorSystem.Dispose();
    }
    
    [Theory]
    [InlineData(4)]
    [InlineData(8)]
    public async Task TestJoinClusterAndProposeReplicateLogsRace(int partitions)
    {
        InMemoryCommunication communication = new();
        
        IRaft node1 = GetNode1(communication, partitions, logger);
        IRaft node2 = GetNode2(communication, partitions, logger);
        IRaft node3 = GetNode3(communication, partitions, logger);

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

        for (int i = 0; i < partitions; i++)
        {
            while (true)
            {
                bool result = await node1.AmILeader(i, CancellationToken.None) ||
                              await node2.AmILeader(i, CancellationToken.None) ||
                              await node3.AmILeader(i, CancellationToken.None);

                if (result)
                    break;
                
                await Task.Delay(500);
            }
        }

        IRaft? leader = await GetLeader(0, [node1, node2, node3]);
        Assert.NotNull(leader);

        List<IRaft> followers = await GetFollowers([node1, node2, node3]);
        Assert.NotEmpty(followers);
        Assert.Equal(2, followers.Count);
        
        byte[] data = "Hello World"u8.ToArray();
        
        leader.OnReplicationReceived += log =>
        {
            Assert.Equal("Greeting", log.LogType);;
            Assert.Equal(data, log.LogData);
            
            Interlocked.Increment(ref totalLeaderReceived);
            return Task.FromResult(true);
        };
        
        foreach (IRaft follower in followers)
            follower.OnReplicationReceived += _ =>
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

        for (int i = 0; i < partitions; i++)
        {
            leader = await GetLeader(i, [node1, node2, node3]);
            Assert.NotNull(leader);

            RaftReplicationResult[] responses = await Task.WhenAll(
                leader.ReplicateLogs(i, "Greeting", data, false),
                leader.ReplicateLogs(i, "Greeting", data, false),
                leader.ReplicateLogs(i, "Greeting", data, false)
            );

            foreach (RaftReplicationResult response in responses)
            {
                if (!response.Success)
                    throw new Exception(response.Status.ToString());

                Assert.True(response.Success);

                Assert.Equal(RaftOperationStatus.Success, response.Status);
            }

            Assert.Equal(0, totalFollowersReceived);
            Assert.Equal(0, totalLeaderReceived);
        }

        node1.ActorSystem.Dispose();
        node2.ActorSystem.Dispose();
        node3.ActorSystem.Dispose();
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
            if (!await node.AmILeader(0, CancellationToken.None))
                followers.Add(node);
        }

        return followers;
    }
}