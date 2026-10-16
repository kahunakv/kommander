using System.Collections.Concurrent;
using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Diagnostics;
using Kommander.Discovery;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;

namespace Kommander.Tests;

/// <summary>
/// A new P0 leader has installed every system entry committed before its term when it learns it
/// leads.
///
/// <para>
/// The coordinator's stale-base fence refuses a whole-map write when a committed entry is
/// installed while the handler runs. It can only see entries the log applicator has handed to the
/// coordinator. For the entries of earlier terms, the guarantee comes from the promotion barrier:
/// leadership is published, and <see cref="RaftSystemRequestType.LeaderChanged"/> sent, only after
/// every prior-term entry was applied, and the applicator sends each system entry to the
/// coordinator before it moves the applied frontier. If that ordering broke, a new leader could
/// write the map from a base that misses committed entries, and the in-order install guard would
/// then refuse them for good (the Kahuna nightly regression of 2026-10-16). This test pins it on a
/// real in-process cluster: the P0 leader commits several map entries and is cut off at once, and
/// the node that takes over must report them installed at its own <c>LeaderChanged</c>.
/// </para>
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
public sealed class TestSystemLeaderChangeDeliveryOrder
{
    private readonly ILogger<IRaft> logger;

    public TestSystemLeaderChangeDeliveryOrder(ITestOutputHelper outputHelper)
    {
        ILoggerFactory lf = LoggerFactory.Create(b => b
            .AddXUnit(outputHelper)
            .SetMinimumLevel(LogLevel.Warning));
        logger = lf.CreateLogger<IRaft>();
    }

    private RaftManager MakeNode(InMemoryCommunication communication, int port, int nodeId, IEnumerable<string> peers)
    {
        RaftConfiguration config = new()
        {
            NodeName = $"node{nodeId}",
            NodeId = nodeId,
            Host = "localhost",
            Port = port,
            InitialPartitions = 1,
            HeartbeatInterval = TimeSpan.FromMilliseconds(50),
            RecentHeartbeat = TimeSpan.FromMilliseconds(25),
            VotingTimeout = TimeSpan.FromMilliseconds(250),
            CheckLeaderInterval = TimeSpan.FromMilliseconds(25),
            UpdateNodesInterval = TimeSpan.FromMilliseconds(50),
            TimerInitialDelay = TimeSpan.FromMilliseconds(25),
            StartElectionTimeout = 100,
            EnableQuiescence = false,
            EndElectionTimeout = 250,
        };

        return new RaftManager(
            config,
            new StaticDiscovery(peers.Select(e => new RaftNode(e)).ToList()),
            new InMemoryWAL(logger),
            communication,
            new HybridLogicalClock(),
            logger);
    }

    private static async Task WaitForCondition(Func<bool> cond, CancellationToken ct, int timeoutMs = 10_000)
    {
        timeoutMs = TestTimeouts.Scale(timeoutMs);
        ValueStopwatch sw = ValueStopwatch.StartNew();
        while (sw.GetElapsedMilliseconds() < timeoutMs)
        {
            ct.ThrowIfCancellationRequested();
            if (cond()) return;
            await Task.Delay(25, ct);
        }
        throw new TimeoutException("Condition not satisfied within timeout.");
    }

    private static async Task<RaftManager> SystemLeaderOf(IEnumerable<RaftManager> nodes, CancellationToken ct)
    {
        ValueStopwatch sw = ValueStopwatch.StartNew();
        while (sw.GetElapsedMilliseconds() < TestTimeouts.Scale(15_000))
        {
            ct.ThrowIfCancellationRequested();
            foreach (RaftManager n in nodes)
            {
                if (await n.AmILeaderQuick(RaftSystemConfig.SystemPartition))
                    return n;
            }
            await Task.Delay(25, ct);
        }
        throw new TimeoutException("No P0 leader elected.");
    }

    [Fact]
    public async Task NewSystemLeader_HasInstalledEveryEarlierCommit_WhenItLearnsItLeads()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        const int basePort = 8780;
        string Ep(int i) => $"localhost:{basePort + i}";

        InMemoryCommunication comm = new();
        RaftManager[] nodes =
        [
            MakeNode(comm, basePort + 1, 1, [Ep(2), Ep(3)]),
            MakeNode(comm, basePort + 2, 2, [Ep(1), Ep(3)]),
            MakeNode(comm, basePort + 3, 3, [Ep(1), Ep(2)]),
        ];

        comm.SetNodes(new Dictionary<string, IRaft>
        {
            [Ep(1)] = nodes[0],
            [Ep(2)] = nodes[1],
            [Ep(3)] = nodes[2],
        });

        try
        {
            foreach (RaftManager n in nodes)
                await n.UpdateNodes();

            await Task.WhenAll(nodes.Select(n => n.JoinCluster(ct)));
            await WaitForCondition(
                () => nodes.All(n => n.IsInitialized && n.SystemCoordinator.GetMembership().MembershipVersion > 0),
                ct, timeoutMs: 20_000);

            RaftManager oldLeader = await SystemLeaderOf(nodes, ct);

            // Every LeaderChanged that names its own node, with the installed index at dispatch.
            ConcurrentQueue<(string Endpoint, long Applied)> selfLeaderChanges = new();
            foreach (RaftManager n in nodes)
            {
                RaftManager node = n;
                node.SystemCoordinator.LeaderChangedForTest = (leader, applied) =>
                {
                    if (leader == node.LocalEndpoint)
                        selfLeaderChanges.Enqueue((node.LocalEndpoint, applied));
                };
            }

            // Three committed map entries in the old leader's term.
            foreach (int partitionId in new[] { 11, 12, 13 })
            {
                RaftPartitionLifecycleResult created = await oldLeader.CreatePartitionAsync(partitionId, RaftRoutingMode.Unrouted, ct: ct);
                Assert.True(created.Success, $"Create {partitionId} failed: {created.Status}");
            }

            long committedThrough = oldLeader.SystemCoordinator.AppliedSystemLogIndexForTest;
            Assert.True(committedThrough > 0);

            // Cut the old leader off at once: the followers may hold its last entries without
            // having heard their commit, which is the inherited tail the barrier must drain.
            comm.PartitionNode(oldLeader.LocalEndpoint);

            RaftManager[] survivors = nodes.Where(n => n != oldLeader).ToArray();
            RaftManager newLeader = await SystemLeaderOf(survivors, ct);

            await WaitForCondition(() => selfLeaderChanges.Any(c => c.Endpoint == newLeader.LocalEndpoint), ct);

            (string _, long appliedAtLeaderChange) = selfLeaderChanges.First(c => c.Endpoint == newLeader.LocalEndpoint);
            Assert.True(appliedAtLeaderChange >= committedThrough,
                $"{newLeader.LocalEndpoint} learned it leads P0 with system entries installed through {appliedAtLeaderChange}, " +
                $"but the previous leader committed through {committedThrough}");

            foreach (int partitionId in new[] { 11, 12, 13 })
                Assert.Contains(newLeader.GetPartitionMap(), r => r.PartitionId == partitionId);
        }
        finally
        {
            comm.HealAll();
            foreach (RaftManager n in nodes)
                await n.LeaveCluster(true, CancellationToken.None);
        }
    }
}
