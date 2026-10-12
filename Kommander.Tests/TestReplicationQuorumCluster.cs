using Kommander.Data;
using Kommander.Communication.Memory;
using Kommander.Diagnostics;
using Kommander.Discovery;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;

namespace Kommander.Tests;

/// <summary>
/// The write path's quorum contract on a real in-process three-node cluster: a batch whose
/// quorum is unreachable fails every admitted entry (nothing is reported committed or left
/// pending) and the partition is not wedged afterwards; a single unreachable follower costs no
/// availability — the write commits on the remaining quorum and the lagging follower catches up
/// once reachable again; and a checkpoint replicates and advances the durable checkpoint floor
/// on every replica.
/// <para>
/// Check-quorum stays off and the proposal timeout is shortened so the leader keeps its role
/// while its followers are cut off, making the quorum-wait timeout deterministic instead of
/// racing a leadership step-down.</para>
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
public sealed class TestReplicationQuorumCluster
{
    private readonly ILogger<IRaft> logger;

    public TestReplicationQuorumCluster(ITestOutputHelper outputHelper)
    {
        ILoggerFactory lf = LoggerFactory.Create(b => b
            .AddXUnit(outputHelper)
            .SetMinimumLevel(LogLevel.Warning));
        logger = lf.CreateLogger<IRaft>();
    }

    // ── Cluster assembly ──────────────────────────────────────────────────────

    private RaftManager MakeNode(
        InMemoryCommunication communication,
        int port, int nodeId,
        IEnumerable<string> peers)
    {
        RaftConfiguration config = new()
        {
            NodeName = $"node{nodeId}",
            NodeId = nodeId,
            Host = "localhost",
            Port = port,
            InitialPartitions = 1,
            ProposalTimeout = TimeSpan.FromMilliseconds(1_500),
            EnableCheckQuorum = false,
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

    private async Task<(RaftManager[] Nodes, InMemoryCommunication Comm)> AssembleCluster(
        int basePort, CancellationToken ct)
    {
        InMemoryCommunication comm = new();
        string Ep(int i) => $"localhost:{basePort + i}";

        RaftManager[] nodes =
        {
            MakeNode(comm, basePort + 1, 1, [Ep(2), Ep(3)]),
            MakeNode(comm, basePort + 2, 2, [Ep(1), Ep(3)]),
            MakeNode(comm, basePort + 3, 3, [Ep(1), Ep(2)]),
        };

        comm.SetNodes(new Dictionary<string, IRaft>
        {
            [Ep(1)] = nodes[0],
            [Ep(2)] = nodes[1],
            [Ep(3)] = nodes[2],
        });

        foreach (RaftManager n in nodes)
            await n.UpdateNodes();

        await Task.WhenAll(nodes.Select(n => n.JoinCluster(ct)));

        await WaitForCondition(
            () => nodes.All(n => n.IsInitialized &&
                                 n.SystemCoordinator.GetMembership().MembershipVersion > 0),
            ct, timeoutMs: 20_000);

        return (nodes, comm);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

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

    private static async Task<RaftManager> LeaderOf(RaftManager[] nodes, int partitionId, CancellationToken ct)
    {
        ValueStopwatch sw = ValueStopwatch.StartNew();
        while (sw.GetElapsedMilliseconds() < TestTimeouts.Scale(15_000))
        {
            ct.ThrowIfCancellationRequested();
            foreach (RaftManager n in nodes)
            {
                if (n.HostsPartition(partitionId) && await n.AmILeaderQuick(partitionId))
                    return n;
            }
            await Task.Delay(25, ct);
        }
        throw new TimeoutException($"No leader elected for partition {partitionId} within 15 s.");
    }

    // ── Quorum loss fails the batch ───────────────────────────────────────────

    /// <summary>
    /// With both followers cut off, a per-entry batch is admitted by the leader (its entries
    /// reach the leader's WAL) but can never commit: the quorum wait expires, and the failure is
    /// stamped on every admitted entry — auto-commit and manual alike — with no log index and no
    /// ticket left behind for the caller. Once the links heal, the stuck entry settles and a
    /// fresh batch commits — the failed wait leaked nothing that wedges the partition.
    /// </summary>
    [Fact]
    public async Task ReplicateEntries_QuorumUnreachable_FailsEveryAdmittedEntryAndRecoversAfterHeal()
    {
        const int partitionId = 1;
        CancellationToken ct = TestContext.Current.CancellationToken;
        (RaftManager[] nodes, InMemoryCommunication comm) = await AssembleCluster(basePort: 8430, ct);

        try
        {
            RaftManager leader = await LeaderOf(nodes, partitionId, ct);
            RaftManager[] followers = [.. nodes.Where(n => !ReferenceEquals(n, leader))];
            long maxLogBefore = leader.WalAdapter.GetMaxLog(partitionId);

            comm.PartitionNode(followers[0].LocalEndpoint);
            comm.PartitionNode(followers[1].LocalEndpoint);

            RaftProposalEntry[] entries =
            [
                new("batch", "auto-a"u8.ToArray(), AutoCommit: true),
                new("batch", "auto-b"u8.ToArray(), AutoCommit: true),
                new("batch", "manual"u8.ToArray(), AutoCommit: false),
            ];

            RaftBatchReplicationResult failed = await leader.ReplicateEntries(partitionId, entries, ct)
                .WaitAsync(TimeSpan.FromMilliseconds(TestTimeouts.Scale(20_000)), ct);

            Assert.False(failed.Success);
            Assert.Equal(RaftOperationStatus.ProposalTimeout, failed.Status);
            Assert.Equal(HLCTimestamp.Zero, failed.TicketId);
            Assert.Equal(3, failed.Entries.Count);
            Assert.All(failed.Entries, entry =>
            {
                Assert.Equal(RaftOperationStatus.ProposalTimeout, entry.Status);
                Assert.Equal(-1, entry.LogIndex);
                Assert.Equal(HLCTimestamp.Zero, entry.Ticket);
            });

            // The batch really was admitted first — the leader appended it — so the failure is
            // the quorum wait, not an up-front rejection.
            long maxLogDuringFailure = leader.WalAdapter.GetMaxLog(partitionId);
            Assert.True(maxLogDuringFailure > maxLogBefore,
                "The batch never reached the leader's WAL; the path under test did not run.");

            // Heal: the stranded entry settles everywhere and the partition takes new writes.
            comm.HealAll();
            await WaitForCondition(
                () => followers.All(f => f.WalAdapter.GetMaxLog(partitionId) >= maxLogDuringFailure),
                ct, timeoutMs: 20_000);

            RaftManager settledLeader = await LeaderOf(nodes, partitionId, ct);
            RaftBatchReplicationResult recovered = await settledLeader.ReplicateEntries(
                partitionId, [new("batch", "after-heal"u8.ToArray())], ct);
            Assert.True(recovered.Success, $"Batch after healing failed: {recovered.Status}");
            Assert.Equal(RaftOperationStatus.Success, recovered.Entries[0].Status);
            Assert.True(recovered.Entries[0].LogIndex > 0);
        }
        finally
        {
            comm.HealAll();
            foreach (RaftManager n in nodes)
                await n.LeaveCluster(true, CancellationToken.None);
        }
    }

    // ── One unreachable follower ──────────────────────────────────────────────

    /// <summary>
    /// A single unreachable follower must not block writes: the leader commits on the remaining
    /// quorum, the reachable follower ends up durably ahead of the cut one, and once the cut
    /// follower is reachable again it catches up to the committed index on its own.
    /// </summary>
    [Fact]
    public async Task ReplicateLogs_OneFollowerUnreachable_CommitsOnRemainingQuorumAndCatchUpFollows()
    {
        const int partitionId = 1;
        CancellationToken ct = TestContext.Current.CancellationToken;
        (RaftManager[] nodes, InMemoryCommunication comm) = await AssembleCluster(basePort: 8440, ct);

        try
        {
            RaftManager leader = await LeaderOf(nodes, partitionId, ct);
            RaftManager[] followers = [.. nodes.Where(n => !ReferenceEquals(n, leader))];
            RaftManager cut = followers[0];
            RaftManager healthy = followers[1];

            comm.PartitionNode(cut.LocalEndpoint);

            RaftReplicationResult write = await leader.ReplicateLogs(
                partitionId, "degraded-write", "degraded"u8.ToArray(), cancellationToken: ct);
            Assert.True(write.Success, $"Write with one follower down failed: {write.Status}");
            Assert.True(write.LogIndex > 0);

            await WaitForCondition(
                () => healthy.WalAdapter.GetMaxLog(partitionId) >= write.LogIndex, ct);

            // The cut follower has none of it yet — the commit really rode the remaining quorum.
            Assert.True(cut.WalAdapter.GetMaxLog(partitionId) < write.LogIndex,
                "The isolated follower already had the entry; the cut did not take effect.");

            comm.HealPartition(cut.LocalEndpoint);
            await WaitForCondition(
                () => cut.WalAdapter.GetMaxLog(partitionId) >= write.LogIndex, ct, timeoutMs: 20_000);
        }
        finally
        {
            comm.HealAll();
            foreach (RaftManager n in nodes)
                await n.LeaveCluster(true, CancellationToken.None);
        }
    }

    // ── Checkpoint replication ────────────────────────────────────────────────

    /// <summary>
    /// A checkpoint proposed after writes commits with a quorum and every replica's durable
    /// checkpoint floor ends up at the checkpoint's log index; a second checkpoint advances the
    /// floor further rather than resetting it.
    /// </summary>
    [Fact]
    public async Task ReplicateCheckpoint_AfterWrites_CommitsAndRaisesTheCheckpointFloorEverywhere()
    {
        const int partitionId = 1;
        CancellationToken ct = TestContext.Current.CancellationToken;
        (RaftManager[] nodes, _) = await AssembleCluster(basePort: 8450, ct);

        try
        {
            RaftManager leader = await LeaderOf(nodes, partitionId, ct);

            foreach (string payload in new[] { "before-checkpoint-1", "before-checkpoint-2" })
            {
                RaftReplicationResult write = await leader.ReplicateLogs(
                    partitionId, "checkpoint-data", global::System.Text.Encoding.UTF8.GetBytes(payload), cancellationToken: ct);
                Assert.True(write.Success, $"Seed write failed: {write.Status}");
            }

            RaftReplicationResult checkpoint = await leader.ReplicateCheckpoint(partitionId, ct);
            Assert.True(checkpoint.Success, $"Checkpoint failed: {checkpoint.Status}");
            Assert.True(checkpoint.LogIndex > 0);

            await WaitForCondition(
                () => nodes.All(n => n.WalAdapter.GetLastCheckpoint(partitionId) >= checkpoint.LogIndex),
                ct, timeoutMs: 20_000);

            RaftReplicationResult checkpointAgain = await leader.ReplicateCheckpoint(partitionId, ct);
            Assert.True(checkpointAgain.Success, $"Second checkpoint failed: {checkpointAgain.Status}");
            Assert.True(checkpointAgain.LogIndex > checkpoint.LogIndex);

            await WaitForCondition(
                () => nodes.All(n => n.WalAdapter.GetLastCheckpoint(partitionId) >= checkpointAgain.LogIndex),
                ct, timeoutMs: 20_000);
        }
        finally
        {
            foreach (RaftManager n in nodes)
                await n.LeaveCluster(true, CancellationToken.None);
        }
    }
}
