
using Kommander.Communication.Memory;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;
using Moq;
using Nixie;

namespace Kommander.Tests;

public class TestThreeNodeCluster
{
    private static IRaft GetNode1(InMemoryCommunication communication)
    {
        ActorSystem actorSystem = new();
        
        RaftConfiguration config = new()
        {
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
            new Mock<ILogger<IRaft>>().Object
        );

        return node;
    }
    
    private static RaftManager GetNode2(InMemoryCommunication communication)
    {
        ActorSystem actorSystem = new();
        
        RaftConfiguration config = new()
        {
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
            new Mock<ILogger<IRaft>>().Object
        );

        return node;
    }
    
    private static RaftManager GetNode3(InMemoryCommunication communication)
    {
        ActorSystem actorSystem = new();
        
        RaftConfiguration config = new()
        {
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
            new Mock<ILogger<IRaft>>().Object
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
        
        IRaft node1 = GetNode1(communication);
        IRaft node2 = GetNode2(communication);
        IRaft node3 = GetNode3(communication);

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
    }
    
    [Fact]
    public async Task TestJoinClusterSimulAndDecideLeader()
    {
        InMemoryCommunication communication = new();
        
        IRaft node1 = GetNode1(communication);
        IRaft node2 = GetNode2(communication);
        IRaft node3 = GetNode3(communication);

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

        long r = await node1.WalAdapter.GetMaxLog(0);
        Assert.Equal(0, r);
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
}