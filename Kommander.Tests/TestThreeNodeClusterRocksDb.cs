
using System.Diagnostics.CodeAnalysis;
using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;
using Nixie;
// ReSharper disable AccessToModifiedClosure

namespace Kommander.Tests;

[SuppressMessage("Performance", "CA1859:Use concrete types when possible for improved performance")]
public class TestThreeNodeClusterRocksDb
{
    private readonly ILogger<IRaft> logger;
    
    //private int totalLeaderReceived;
    //private int totalFollowersReceived;
    
    public TestThreeNodeClusterRocksDb()
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
            new RocksDbWAL("/tmp", Guid.NewGuid().ToString()),
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
            new RocksDbWAL("/tmp", Guid.NewGuid().ToString()),
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
            new RocksDbWAL("/tmp", Guid.NewGuid().ToString()),
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
            
            await Task.Delay(100);
        }

        IRaft? leader = await GetLeader([node1, node2, node3]);
        Assert.NotNull(leader);
        
        List<IRaft> followers = await GetFollowers([node1, node2, node3]);
        Assert.NotEmpty(followers);
        Assert.Equal(2, followers.Count);

        await node1.LeaveCluster(true);
        await node2.LeaveCluster(true);
        await node3.LeaveCluster(true);
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
            
            await Task.Delay(100);
        }

        IRaft? leader = await GetLeader([node1, node2, node3]);
        Assert.NotNull(leader);
        
        List<IRaft> followers = await GetFollowers([node1, node2, node3]);
        Assert.NotEmpty(followers);
        Assert.Equal(2, followers.Count);

        long maxId = node1.WalAdapter.GetMaxLog(0);
        Assert.Equal(0, maxId);
        
        maxId = node2.WalAdapter.GetMaxLog(0);
        Assert.Equal(0, maxId);
        
        maxId = node3.WalAdapter.GetMaxLog(0);
        Assert.Equal(0, maxId);
        
        await node1.LeaveCluster(true);
        await node2.LeaveCluster(true);
        await node3.LeaveCluster(true);
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
            
            await Task.Delay(100);
        }

        IRaft? leader = await GetLeader([node1, node2, node3]);
        Assert.NotNull(leader);

        List<IRaft> followers = await GetFollowers([node1, node2, node3]);
        Assert.NotEmpty(followers);
        Assert.Equal(2, followers.Count);
        
        byte[] data = "Hello World"u8.ToArray();

        int totalLeaderReceived = 0;
        int totalFollowersReceived = 0;
        
        leader.OnReplicationReceived += (_, _) =>
        {
            totalLeaderReceived++;
            return Task.FromResult(true);
        };

        foreach (IRaft follower in followers)
        {
            Console.Error.WriteLine(follower.GetHashCode());
            
            follower.OnReplicationReceived += (_, log) =>
            {
                totalFollowersReceived++;

                return Task.FromResult(true);
            };
            
            follower.OnReplicationError += _ =>
            {
                Console.Error.WriteLine("ERROR");
            };
        }

        long maxId = node1.WalAdapter.GetMaxLog(0);
        Assert.Equal(0, maxId);
        
        maxId = node2.WalAdapter.GetMaxLog(0);
        Assert.Equal(0, maxId);
        
        maxId = node3.WalAdapter.GetMaxLog(0);
        Assert.Equal(0, maxId);
        
        RaftReplicationResult response = await leader.ReplicateLogs(0, "Greeting", data);
        Assert.True(response.Success);
        
        Assert.Equal(RaftOperationStatus.Success, response.Status);
        Assert.Equal(1, response.LogIndex);
        
        response = await leader.ReplicateLogs(0, "Greeting", data);
        Assert.True(response.Success);
        
        Assert.Equal(RaftOperationStatus.Success, response.Status);
        Assert.Equal(2, response.LogIndex);
        
        maxId = node1.WalAdapter.GetMaxLog(0);
        Assert.Equal(2, maxId);
        
        maxId = node2.WalAdapter.GetMaxLog(0);
        Assert.Equal(2, maxId);
        
        maxId = node3.WalAdapter.GetMaxLog(0);
        Assert.Equal(2, maxId);

        await Task.Delay(100);
        
        Assert.Equal(4, totalFollowersReceived);
        Assert.Equal(0, totalLeaderReceived);
        
        await node1.LeaveCluster(true);
        await node2.LeaveCluster(true);
        await node3.LeaveCluster(true);
    }
    
    [Fact]
    public async Task TestJoinClusterAndProposeReplicateLogs()
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
            
            await Task.Delay(100);
        }

        IRaft? leader = await GetLeader([node1, node2, node3]);
        Assert.NotNull(leader);

        List<IRaft> followers = await GetFollowers([node1, node2, node3]);
        Assert.NotEmpty(followers);
        Assert.Equal(2, followers.Count);
        
        byte[] data = "Hello World"u8.ToArray();
        
        int totalLeaderReceived = 0;
        int totalFollowersReceived = 0;
        
        leader.OnReplicationReceived += (_, _) =>
        {
            Interlocked.Increment(ref totalLeaderReceived);
            return Task.FromResult(true);
        };

        foreach (IRaft follower in followers)
        {
            follower.OnReplicationReceived += (_, log) =>
            {
                Assert.Equal("Greeting", log.LogType);;
                Assert.Equal(data, log.LogData);
                
                Interlocked.Increment(ref totalFollowersReceived);
                return Task.FromResult(true);
            };
        }

        long maxId = node1.WalAdapter.GetMaxLog(0);
        Assert.Equal(0, maxId);
        
        maxId = node2.WalAdapter.GetMaxLog(0);
        Assert.Equal(0, maxId);
        
        maxId = node3.WalAdapter.GetMaxLog(0);
        Assert.Equal(0, maxId);
        
        RaftReplicationResult response = await leader.ReplicateLogs(0, "Greeting", data, false);
        Assert.True(response.Success);
        
        Assert.Equal(RaftOperationStatus.Success, response.Status);
        Assert.Equal(1, response.LogIndex);
        
        await Task.Delay(100);
        
        Assert.Equal(0, totalFollowersReceived);
        Assert.Equal(0, totalLeaderReceived);
        
        await node1.LeaveCluster(true);
        await node2.LeaveCluster(true);
        await node3.LeaveCluster(true);
    }
    
    [Fact]
    public async Task TestJoinClusterAndProposeReplicateLogsAndCommit()
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
            
            await Task.Delay(100);
        }

        IRaft? leader = await GetLeader([node1, node2, node3]);
        Assert.NotNull(leader);

        List<IRaft> followers = await GetFollowers([node1, node2, node3]);
        Assert.NotEmpty(followers);
        Assert.Equal(2, followers.Count);
        
        byte[] data = "Hello World"u8.ToArray();
        
        int totalLeaderReceived = 0;
        int totalFollowersReceived = 0;
        
        leader.OnReplicationReceived += (_, _) =>
        {
            Interlocked.Increment(ref totalLeaderReceived);
            return Task.FromResult(true);
        };

        foreach (IRaft follower in followers)
        {
            follower.OnReplicationReceived += (_, log) =>
            {
                Assert.Equal("Greeting", log.LogType);;
                Assert.Equal(data, log.LogData);
                
                Interlocked.Increment(ref totalFollowersReceived);
                return Task.FromResult(true);
            };
        }

        long maxId = node1.WalAdapter.GetMaxLog(0);
        Assert.Equal(0, maxId);
        
        maxId = node2.WalAdapter.GetMaxLog(0);
        Assert.Equal(0, maxId);
        
        maxId = node3.WalAdapter.GetMaxLog(0);
        Assert.Equal(0, maxId);
        
        RaftReplicationResult response = await leader.ReplicateLogs(0, "Greeting", data, false);
        Assert.True(response.Success);
        
        Assert.Equal(RaftOperationStatus.Success, response.Status);
        Assert.Equal(1, response.LogIndex);
        
        Assert.Equal(0, totalFollowersReceived);
        Assert.Equal(0, totalLeaderReceived);

        (bool success, RaftOperationStatus status, long commitLogId) = await leader.CommitLogs(0, response.TicketId);
        Assert.True(success);
        Assert.Equal(RaftOperationStatus.Success, status);
        
        await Task.Delay(100);
        
        Assert.Equal(2, totalFollowersReceived);
        Assert.Equal(0, totalLeaderReceived);
        
        await node1.LeaveCluster(true);
        await node2.LeaveCluster(true);
        await node3.LeaveCluster(true);
    }
    
    [Fact]
    public async Task TestJoinClusterAndProposeReplicateLogsAndRollback()
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
            
            await Task.Delay(100);
        }

        IRaft? leader = await GetLeader([node1, node2, node3]);
        Assert.NotNull(leader);

        List<IRaft> followers = await GetFollowers([node1, node2, node3]);
        Assert.NotEmpty(followers);
        Assert.Equal(2, followers.Count);
        
        byte[] data = "Hello World"u8.ToArray();
        
        int totalLeaderReceived = 0;
        int totalFollowersReceived = 0;
        
        leader.OnReplicationReceived += (_, _) =>
        {
            Interlocked.Increment(ref totalLeaderReceived);
            return Task.FromResult(true);
        };

        foreach (IRaft follower in followers)
        {
            follower.OnReplicationReceived += (_, log) =>
            {
                Assert.Equal("Greeting", log.LogType);;
                Assert.Equal(data, log.LogData);
                
                totalFollowersReceived++;
                
                return Task.FromResult(true);
            };
        }

        long maxId = node1.WalAdapter.GetMaxLog(0);
        Assert.Equal(0, maxId);
        
        maxId = node2.WalAdapter.GetMaxLog(0);
        Assert.Equal(0, maxId);
        
        maxId = node3.WalAdapter.GetMaxLog(0);
        Assert.Equal(0, maxId);
        
        RaftReplicationResult response = await leader.ReplicateLogs(0, "Greeting", data, false);
        Assert.True(response.Success);
        
        Assert.Equal(RaftOperationStatus.Success, response.Status);
        Assert.Equal(1, response.LogIndex);
        
        Assert.Equal(0, totalFollowersReceived);
        Assert.Equal(0, totalLeaderReceived);

        (bool success, RaftOperationStatus status, long commitLogId) = await leader.RollbackLogs(0, response.TicketId);
        Assert.True(success);
        Assert.Equal(RaftOperationStatus.Success, status);
        
        await Task.Delay(100);
        
        Assert.Equal(0, totalFollowersReceived);
        Assert.Equal(0, totalLeaderReceived);
        
        await node1.LeaveCluster(true);
        await node2.LeaveCluster(true);
        await node3.LeaveCluster(true);
    }
    
    [Fact]
    public async Task TestJoinClusterAndMultiReplicateLogs()
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
            
            await Task.Delay(100);
        }

        IRaft? leader = await GetLeader([node1, node2, node3]);
        Assert.NotNull(leader);

        List<IRaft> followers = await GetFollowers([node1, node2, node3]);
        Assert.NotEmpty(followers);
        Assert.Equal(2, followers.Count);

        byte[] data = "Hello World"u8.ToArray();
        
        int totalLeaderReceived = 0;
        int totalFollowersReceived = 0;
        
        leader.OnReplicationReceived += (_, _) =>
        {
            Interlocked.Increment(ref totalLeaderReceived);
            return Task.FromResult(true);
        };

        foreach (IRaft follower in followers)
        {
            follower.OnReplicationReceived += (_, log) =>
            {
                Assert.Equal("Greeting", log.LogType);;
                Assert.Equal(data, log.LogData);
                
                Interlocked.Increment(ref totalFollowersReceived);
                
                return Task.FromResult(true);
            };
        }

        long maxId = node1.WalAdapter.GetMaxLog(0);
        Assert.Equal(0, maxId);
        
        maxId = node2.WalAdapter.GetMaxLog(0);
        Assert.Equal(0, maxId);
        
        maxId = node3.WalAdapter.GetMaxLog(0);
        Assert.Equal(0, maxId);

        for (int i = 0; i < 100; i++)
        {
            RaftReplicationResult response = await leader.ReplicateLogs(0, "Greeting", data);
            
            Assert.True(response.Success);
            Assert.Equal(RaftOperationStatus.Success, response.Status);
            
            Assert.Equal(i + 1, response.LogIndex);
        }
        
        maxId = node1.WalAdapter.GetMaxLog(0);
        Assert.Equal(100, maxId);
        
        maxId = node2.WalAdapter.GetMaxLog(0);
        Assert.Equal(100, maxId);
        
        maxId = node3.WalAdapter.GetMaxLog(0);
        Assert.Equal(100, maxId);
        
        await Task.Delay(100);
        
        Assert.Equal(200, totalFollowersReceived);
        Assert.Equal(0, totalLeaderReceived);
        
        await node1.LeaveCluster(true);
        await node2.LeaveCluster(true);
        await node3.LeaveCluster(true);
    }
    
    [Fact]
    public async Task TestJoinClusterAndReplicateLogsMulti()
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
            
            await Task.Delay(100);
        }

        IRaft? leader = await GetLeader([node1, node2, node3]);
        Assert.NotNull(leader);

        List<IRaft> followers = await GetFollowers([node1, node2, node3]);
        Assert.NotEmpty(followers);
        Assert.Equal(2, followers.Count);
        
        byte[] data = "Hello World"u8.ToArray();
        
        int totalLeaderReceived = 0;
        int totalFollowersReceived = 0;
        
        leader.OnReplicationReceived += (_, _) =>
        {
            Interlocked.Increment(ref totalLeaderReceived);
            return Task.FromResult(true);
        };

        foreach (IRaft follower in followers)
        {
            follower.OnReplicationReceived += (_, log) =>
            {
                Assert.Equal("Greeting", log.LogType);;
                //Assert.Equal(data, log.LogData);

                Interlocked.Increment(ref totalFollowersReceived);
                
                return Task.FromResult(true);
            };
        }

        long maxId = node1.WalAdapter.GetMaxLog(0);
        Assert.Equal(0, maxId);
        
        maxId = node2.WalAdapter.GetMaxLog(0);
        Assert.Equal(0, maxId);
        
        maxId = node3.WalAdapter.GetMaxLog(0);
        Assert.Equal(0, maxId);

        List<byte[]> multiLogs =
        [
            data,
            "Foo Bar"u8.ToArray(),
            "Come Undone"u8.ToArray()
        ];
        
        RaftReplicationResult response = await leader.ReplicateLogs(0, "Greeting", multiLogs);
        Assert.True(response.Success);
        
        Assert.Equal(RaftOperationStatus.Success, response.Status);
        Assert.Equal(3, response.LogIndex);
        
        response = await leader.ReplicateLogs(0, "Greeting", multiLogs);
        Assert.True(response.Success);
        
        Assert.Equal(RaftOperationStatus.Success, response.Status);
        Assert.Equal(6, response.LogIndex);
        
        maxId = node1.WalAdapter.GetMaxLog(0);
        Assert.Equal(6, maxId);
        
        maxId = node2.WalAdapter.GetMaxLog(0);
        Assert.Equal(6, maxId);
        
        maxId = node3.WalAdapter.GetMaxLog(0);
        Assert.Equal(6, maxId);
        
        await Task.Delay(500);
        
        Assert.Equal(12, totalFollowersReceived);
        Assert.Equal(0, totalLeaderReceived);
        
        await node1.LeaveCluster(true);
        await node2.LeaveCluster(true);
        await node3.LeaveCluster(true);
    }
    
    [Fact]
    public async Task TestJoinClusterRestoreWalAndDecideLeader()
    {
        InMemoryCommunication communication = new();
        
        IRaft node1 = GetNode1(communication, logger);
        IRaft node2 = GetNode2(communication, logger);
        IRaft node3 = GetNode3(communication, logger);

        byte[] data = "Hello"u8.ToArray();

        for (int i = 0; i < 10; i++)
        {
            foreach (IRaft node in new List<IRaft> { node1, node2, node3 })
            {
                node.WalAdapter.Propose(0,
                    new()
                    {
                        Id = i + 1,
                        Term = 1,
                        LogData = data,
                        Time = HLCTimestamp.Zero,
                        Type = RaftLogType.Proposed
                    });

                node.WalAdapter.Commit(0,
                    new()
                    {
                        Id = i + 1,
                        Term = 1,
                        LogData = data,
                        Time = HLCTimestamp.Zero,
                        Type = RaftLogType.Committed
                    });
            }
        }

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
            
            await Task.Delay(100);
        }

        IRaft? leader = await GetLeader([node1, node2, node3]);
        Assert.NotNull(leader);
        
        List<IRaft> followers = await GetFollowers([node1, node2, node3]);
        Assert.NotEmpty(followers);
        Assert.Equal(2, followers.Count);

        long maxId = node1.WalAdapter.GetMaxLog(0);
        Assert.Equal(10, maxId);
        
        maxId = node2.WalAdapter.GetMaxLog(0);
        Assert.Equal(10, maxId);
        
        maxId = node3.WalAdapter.GetMaxLog(0);
        Assert.Equal(10, maxId);
        
        node1.ActorSystem.Dispose();
        node2.ActorSystem.Dispose();
        node3.ActorSystem.Dispose();
    }
    
    [Fact]
    public async Task TestJoinClusterRestoreWalAtDifferentIndexDecideLeader()
    {
        InMemoryCommunication communication = new();
        
        IRaft node1 = GetNode1(communication, logger);
        IRaft node2 = GetNode2(communication, logger);
        IRaft node3 = GetNode3(communication, logger);

        byte[] data = "Hello"u8.ToArray();

        for (int i = 0; i < 10; i++)
        {
            foreach (IRaft node in new List<IRaft> { node1, node2 })
            {
                node.WalAdapter.Propose(0,
                    new()
                    {
                        Id = i + 1,
                        Term = 1,
                        LogData = data,
                        Time = HLCTimestamp.Zero,
                        Type = RaftLogType.Proposed
                    });

                node.WalAdapter.Commit(0,
                    new()
                    {
                        Id = i + 1,
                        Term = 1,
                        LogData = data,
                        Time = HLCTimestamp.Zero,
                        Type = RaftLogType.Committed
                    });
            }
        }

        for (int i = 0; i < 25; i++)
        {
            node3.WalAdapter.Propose(0,
                new()
                {
                    Id = i + 1,
                    Term = 1,
                    LogData = data,
                    Time = HLCTimestamp.Zero,
                    Type = RaftLogType.Proposed
                });

            node3.WalAdapter.Commit(0,
                new()
                {
                    Id = i + 1,
                    Term = 1,
                    LogData = data,
                    Time = HLCTimestamp.Zero,
                    Type = RaftLogType.Committed
                });
        }

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
            
            await Task.Delay(100);
        }

        IRaft? leader = await GetLeader([node1, node2, node3]);
        Assert.NotNull(leader);
        
        Assert.Equal(leader.GetLocalEndpoint(), node3.GetLocalEndpoint());
        
        List<IRaft> followers = await GetFollowers([node1, node2, node3]);
        Assert.NotEmpty(followers);
        Assert.Equal(2, followers.Count);

        long maxId = node1.WalAdapter.GetMaxLog(0);
        Assert.Equal(10, maxId);
        
        maxId = node2.WalAdapter.GetMaxLog(0);
        Assert.Equal(10, maxId);
        
        maxId = node3.WalAdapter.GetMaxLog(0);
        Assert.Equal(25, maxId);
        
        await node1.LeaveCluster(true);
        await node2.LeaveCluster(true);
        await node3.LeaveCluster(true);
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
