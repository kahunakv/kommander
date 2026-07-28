
using Kommander;
using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Diagnostics;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.Chaos;

/// <summary>
/// Attaches the hash-chain oracle to a real 3-node in-process cluster (no faults) and asserts that all
/// nodes converge to the identical applied prefix. This exercises <see cref="HashChainStateMachine"/> and
/// <see cref="HashChainAssert"/> against genuine <see cref="RaftManager.OnReplicationReceived"/> delivery,
/// and is the healthy baseline the chaos scenarios build on. It uses its own consumers, so no existing
/// test's <c>OnReplicationReceived</c> handler is affected.
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
public class TestHashChainCluster
{
    private static readonly ILogger<IRaft> Logger = NullLogger<IRaft>.Instance;

    [Fact]
    public async Task ThreeNodeCluster_Replication_ConvergesWithNoDivergence()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        InMemoryCommunication comm = new();

        RaftManager n1 = BuildNode(comm, 8450, 1, ["localhost:8451", "localhost:8452"]);
        RaftManager n2 = BuildNode(comm, 8451, 2, ["localhost:8450", "localhost:8452"]);
        RaftManager n3 = BuildNode(comm, 8452, 3, ["localhost:8450", "localhost:8451"]);

        comm.SetNodes(new Dictionary<string, IRaft>
        {
            ["localhost:8450"] = n1,
            ["localhost:8451"] = n2,
            ["localhost:8452"] = n3,
        });

        try
        {
            await Task.WhenAll(n1.JoinCluster(ct), n2.JoinCluster(ct), n3.JoinCluster(ct));
            await WaitForAsync(() => n1.IsInitialized && n2.IsInitialized && n3.IsInitialized, ct);

            RaftManager leader = await FindLeaderAsync([n1, n2, n3], ct);
            int partitionId = leader.Partitions.Keys.First(k => k != 0);

            // Attach one hash chain per node BEFORE replicating, so every observer sees the same prefix.
            (RaftManager Node, HashChainStateMachine Chain)[] chains =
            [
                (n1, new HashChainStateMachine("localhost:8450", partitionId)),
                (n2, new HashChainStateMachine("localhost:8451", partitionId)),
                (n3, new HashChainStateMachine("localhost:8452", partitionId)),
            ];
            foreach ((RaftManager node, HashChainStateMachine chain) in chains)
                node.OnReplicationReceived += chain.OnReplicationReceived;

            // Replicate distinct, uniquely-identified entries through the leader.
            const int count = 25;
            long lastIndex = 0;
            for (int i = 0; i < count; i++)
            {
                RaftReplicationResult r = await leader.ReplicateLogs(
                    partitionId, "chaos", BitConverter.GetBytes(i), cancellationToken: ct);
                Assert.Equal(RaftOperationStatus.Success, r.Status);
                lastIndex = r.LogIndex;
            }

            HashChainStateMachine[] observers = chains.Select(c => c.Chain).ToArray();

            // Wait until every node has applied through the last committed index.
            await WaitForAsync(
                () => observers.All(o => o.Snapshot().LastAppliedIndex >= lastIndex),
                ct);

            HashChainAssert.NoDivergence(observers, partitionId);
            HashChainAssert.ConvergedToIndex(observers, partitionId, lastIndex);
            // Delivery is exactly-once: no node applies any index (identical or conflicting) more than
            // once. This is the strict form; it holds now that ApplyLogToConsumerAsync /
            // CompleteFollowerAppend gate consumer delivery on log.Id > lastAppliedIndex.
            HashChainAssert.NoDuplicateApply(observers, partitionId);
        }
        finally
        {
            n1.Dispose(); n2.Dispose(); n3.Dispose();
        }
    }

    // ── helpers ────────────────────────────────────────────────────────────────

    private static RaftManager BuildNode(InMemoryCommunication comm, int port, int nodeId, string[] peers)
    {
        RaftConfiguration cfg = new()
        {
            NodeId = nodeId, Host = "localhost", Port = port,
            InitialPartitions = 1,
            HeartbeatInterval = TimeSpan.FromMilliseconds(50),
            RecentHeartbeat = TimeSpan.FromMilliseconds(25),
            VotingTimeout = TimeSpan.FromMilliseconds(500),
            CheckLeaderInterval = TimeSpan.FromMilliseconds(25),
            UpdateNodesInterval = TimeSpan.FromMilliseconds(200),
            TimerInitialDelay = TimeSpan.FromMilliseconds(25),
            StartElectionTimeout = 100,
            EnableQuiescence = false,
            EndElectionTimeout = 300,
            BackfillThreshold = 0,
            MaxBackfillEntriesPerRound = 128,
        };
        return new RaftManager(cfg,
            new StaticDiscovery(peers.Select(e => new RaftNode(e)).ToList()),
            new InMemoryWAL(Logger), comm, new HybridLogicalClock(), Logger);
    }

    private static async Task WaitForAsync(Func<bool> cond, CancellationToken ct, int timeoutMs = 15_000)
    {
        ValueStopwatch sw = ValueStopwatch.StartNew();
        while (sw.GetElapsedMilliseconds() < timeoutMs)
        {
            ct.ThrowIfCancellationRequested();
            if (cond()) return;
            await Task.Delay(50, ct);
        }
        throw new TimeoutException($"Condition not met within {timeoutMs} ms.");
    }

    private static async Task<RaftManager> FindLeaderAsync(RaftManager[] nodes, CancellationToken ct)
    {
        ValueStopwatch sw = ValueStopwatch.StartNew();
        while (sw.GetElapsedMilliseconds() < 15_000)
        {
            ct.ThrowIfCancellationRequested();
            foreach (RaftManager n in nodes)
                foreach (int partId in n.Partitions.Keys)
                    if (partId != 0 && await n.AmILeaderQuick(partId))
                        return n;
            await Task.Delay(50, ct);
        }
        throw new TimeoutException("No leader for user partition within 15 s.");
    }
}
