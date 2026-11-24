using System.Diagnostics.CodeAnalysis;
using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;

namespace Kommander.Tests;

/// <summary>
/// Covers <see cref="IRaft.ConfirmLeadershipLeaseAsync"/> on real in-memory clusters, through the public
/// <see cref="IRaft"/> surface.
///
/// <list type="bullet">
///   <item>It answers while committed entries wait for their delivery, where
///         <see cref="IRaft.ConfirmLeadershipAsync"/> waits for that delivery.</item>
///   <item>Every entry of an earlier leadership is delivered before it answers true, which is what
///         lets a caller reason about its own proposals alone.</item>
///   <item>A follower, a leader that stepped down and a leader that lost its quorum are never
///         confirmed.</item>
/// </list>
/// </summary>
[SuppressMessage("Performance", "CA1859:Use concrete types when possible for improved performance")]
[Collection(ClusterIntegrationCollection.Name)]
public class TestLeadershipLeaseConfirmation
{
    private const int UserPartition = 1;

    private readonly ILogger<IRaft> logger;

    public TestLeadershipLeaseConfirmation()
    {
        ILoggerFactory loggerFactory = LoggerFactory.Create(builder => builder.SetMinimumLevel(LogLevel.Warning));
        logger = loggerFactory.CreateLogger<IRaft>();
    }

    /// <param name="checkQuorum">False keeps a leader that reaches nobody in its role, so the lease alone decides.</param>
    private IRaft NewNode(InMemoryCommunication communication, string name, int id, int port, bool checkQuorum, TimeSpan confirmationTimeout, string[] peers) =>
        new RaftManager(
            new RaftConfiguration
            {
                NodeName = name,
                NodeId = id,
                Host = "localhost",
                Port = port,
                InitialPartitions = 1,
                HeartbeatInterval = TimeSpan.FromMilliseconds(50),
                RecentHeartbeat = TimeSpan.FromMilliseconds(25),
                VotingTimeout = TimeSpan.FromMilliseconds(250),
                CheckLeaderInterval = TimeSpan.FromMilliseconds(25),
                UpdateNodesInterval = TimeSpan.FromMilliseconds(100),
                TimerInitialDelay = TimeSpan.FromMilliseconds(25),
                StartElectionTimeout = 100,
                EndElectionTimeout = 250,
                EnableQuiescence = false,
                EnableCheckQuorum = checkQuorum,
                LeadershipConfirmationTimeout = confirmationTimeout
            },
            new StaticDiscovery([.. peers.Select(p => new RaftNode(p))]),
            new InMemoryWAL(logger),
            communication,
            new HybridLogicalClock(),
            logger);

    private async Task<IRaft[]> AssembleCluster(InMemoryCommunication communication, int count, bool checkQuorum = true, TimeSpan? confirmationTimeout = null)
    {
        int[] ports = [.. Enumerable.Range(0, count).Select(i => 8001 + i)];
        IRaft[] nodes =
        [
            .. ports.Select((port, i) => NewNode(
                communication, $"node{i + 1}", i + 1, port, checkQuorum,
                // Long enough that the full confirmation of a held delivery is still waiting when a test looks at it.
                confirmationTimeout ?? TimeSpan.FromSeconds(20),
                [.. ports.Where(other => other != port).Select(other => $"localhost:{other}")]))
        ];

        communication.SetNodes(nodes.ToDictionary(node => node.GetLocalEndpoint()));

        foreach (IRaft node in nodes)
            await node.UpdateNodes();

        await Task.WhenAll(nodes.Select(node => node.JoinCluster(TestContext.Current.CancellationToken)));
        return nodes;
    }

    private static async Task<IRaft> WaitForLeaderAsync(IRaft[] nodes, IRaft? excluding = null)
    {
        for (int attempt = 0; attempt < 400; attempt++)
        {
            foreach (IRaft node in nodes)
            {
                if (!ReferenceEquals(node, excluding) && await node.AmILeaderQuick(UserPartition))
                    return node;
            }

            await Task.Delay(25, TestContext.Current.CancellationToken);
        }

        throw new InvalidOperationException("No leader elected");
    }

    /// <summary>Waits until the lease of <paramref name="leader"/> answers, as it does once its acks arrived.</summary>
    private static async Task WaitForLeaseAsync(IRaft leader)
    {
        for (int attempt = 0; attempt < 400; attempt++)
        {
            if (await leader.ConfirmLeadershipLeaseAsync(UserPartition, TestContext.Current.CancellationToken))
                return;

            await Task.Delay(25, TestContext.Current.CancellationToken);
        }

        throw new InvalidOperationException("The leader never confirmed its lease");
    }

    private static async Task DisposeAsync(IRaft[] nodes)
    {
        foreach (IRaft node in nodes)
            await node.LeaveCluster(true, CancellationToken.None);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(3)]
    public async Task UndeliveredEntries_HoldBackTheFullConfirmation_AndNotTheLease(int nodeCount)
    {
        InMemoryCommunication communication = new();
        IRaft[] nodes = await AssembleCluster(communication, nodeCount);

        try
        {
            IRaft leader = await WaitForLeaderAsync(nodes);
            await WaitForLeaseAsync(leader);

            Assert.Equal(RaftOperationStatus.Success,
                await leader.HoldConsumerAppliesForTesting(UserPartition, TestContext.Current.CancellationToken));

            // The proposer is answered at quorum, and the entry is not delivered on the leader.
            RaftReplicationResult write = await leader.ReplicateLogs(
                UserPartition, "Greeting", "Hello World"u8.ToArray(), cancellationToken: TestContext.Current.CancellationToken);
            Assert.True(write.Success, $"write failed with {write.Status}");

            Task<bool> full = leader.ConfirmLeadershipAsync(UserPartition, TestContext.Current.CancellationToken).AsTask();

            // A lease answers for as long as the delivery is held, not only right after the write.
            await Task.Delay(TimeSpan.FromMilliseconds(500), TestContext.Current.CancellationToken);

            Assert.True(await LeaseAnswersAtOnce(leader), "a fresh lease must answer while the delivery is held");
            Assert.False(full.IsCompleted, "the full confirmation must wait for the delivery");

            Assert.Equal(RaftOperationStatus.Success,
                await leader.ResumeConsumerAppliesForTesting(UserPartition, TestContext.Current.CancellationToken));

            Assert.True(await full.WaitAsync(TimeSpan.FromSeconds(15), TestContext.Current.CancellationToken));
        }
        finally
        {
            await DisposeAsync(nodes);
        }
    }

    /// <summary>Whether a lease confirmation completes synchronously within a second. A lapse between two heartbeats
    /// sends one call through a round, so a single call proves nothing.</summary>
    private static async Task<bool> LeaseAnswersAtOnce(IRaft leader)
    {
        for (int attempt = 0; attempt < 200; attempt++)
        {
            Task<bool> lease = leader.ConfirmLeadershipLeaseAsync(UserPartition, TestContext.Current.CancellationToken).AsTask();
            if (lease.IsCompleted)
                return await lease;

            await Task.Delay(5, TestContext.Current.CancellationToken);
        }

        return false;
    }

    [Fact]
    public async Task EntriesOfAnEarlierLeadership_AreDeliveredBeforeTheLeaseAnswers()
    {
        InMemoryCommunication communication = new();
        IRaft[] nodes = await AssembleCluster(communication, 3);

        try
        {
            IRaft leader = await WaitForLeaderAsync(nodes);

            Dictionary<IRaft, List<long>> delivered = nodes.ToDictionary(node => node, _ => new List<long>());
            foreach (IRaft node in nodes)
            {
                node.OnReplicationReceived += (partitionId, log) =>
                {
                    if (partitionId == UserPartition)
                    {
                        lock (delivered[node])
                            delivered[node].Add(log.Id);
                    }

                    return Task.FromResult(true);
                };
            }

            List<long> written = [];
            for (int i = 0; i < 20; i++)
            {
                RaftReplicationResult write = await leader.ReplicateLogs(
                    UserPartition, "Greeting", "Hello World"u8.ToArray(), cancellationToken: TestContext.Current.CancellationToken);
                Assert.True(write.Success, $"write {i} failed with {write.Status}");
                written.Add(write.LogIndex);
            }

            Assert.Equal(RaftOperationStatus.Success, leader.SetCandidacyWithheld(UserPartition, true));
            await leader.StepDownAsync(UserPartition, TestContext.Current.CancellationToken);

            IRaft next = await WaitForLeaderAsync(nodes, excluding: leader);
            await WaitForLeaseAsync(next);

            // Read right after the first true answer, with no other wait.
            long[] seen;
            lock (delivered[next])
                seen = [.. delivered[next]];

            foreach (long index in written)
                Assert.Contains(index, seen);

            Assert.False(await leader.ConfirmLeadershipLeaseAsync(UserPartition, TestContext.Current.CancellationToken));
        }
        finally
        {
            await DisposeAsync(nodes);
        }
    }

    [Fact]
    public async Task AFollowerIsNotConfirmed()
    {
        InMemoryCommunication communication = new();
        IRaft[] nodes = await AssembleCluster(communication, 3);

        try
        {
            IRaft leader = await WaitForLeaderAsync(nodes);
            await WaitForLeaseAsync(leader);

            foreach (IRaft node in nodes.Where(node => !ReferenceEquals(node, leader)))
                Assert.False(await node.ConfirmLeadershipLeaseAsync(UserPartition, TestContext.Current.CancellationToken));
        }
        finally
        {
            await DisposeAsync(nodes);
        }
    }

    [Fact]
    public async Task ALeaderThatLostItsQuorum_StopsBeingConfirmedWhenTheLeaseLapses()
    {
        InMemoryCommunication communication = new();
        IRaft[] nodes = await AssembleCluster(communication, 3, checkQuorum: false, confirmationTimeout: TimeSpan.FromSeconds(1));

        try
        {
            IRaft leader = await WaitForLeaderAsync(nodes);
            await WaitForLeaseAsync(leader);

            // The leader reaches nobody: no ack renews its lease, and no round can.
            communication.SetNodes(new Dictionary<string, IRaft> { { leader.GetLocalEndpoint(), leader } });

            // One heartbeat interval is the window of the lease. The slack covers the ack in flight.
            await Task.Delay(TimeSpan.FromMilliseconds(250), TestContext.Current.CancellationToken);

            // It still believes that it leads, so belief would confirm it. The round that the lapsed lease falls back
            // to finds no quorum.
            Assert.True(await leader.AmILeaderQuick(UserPartition));
            Assert.False(await leader.ConfirmLeadershipLeaseAsync(UserPartition, TestContext.Current.CancellationToken));
        }
        finally
        {
            communication.SetNodes(nodes.ToDictionary(node => node.GetLocalEndpoint()));
            await DisposeAsync(nodes);
        }
    }
}
