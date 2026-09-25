using System.Diagnostics.CodeAnalysis;
using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;

namespace Kommander.Tests;

/// <summary>
/// The two seams an application needs when it finds one of its replicas holding an incomplete
/// projection: withholding that replica's candidacy so it never leads (nor accepts a leadership
/// transfer) until released, and asking the leader to re-seed it with a whole-partition snapshot
/// that installs over a log the replica already holds.
/// </summary>
[SuppressMessage("Performance", "CA1859:Use concrete types when possible for improved performance")]
[Collection(ClusterIntegrationCollection.Name)]
public class TestReseedAndCandidacyWithhold
{
    private const int UserPartition = 1;

    private readonly ILogger<IRaft> logger;

    public TestReseedAndCandidacyWithhold()
    {
        ILoggerFactory loggerFactory = LoggerFactory.Create(builder => builder.SetMinimumLevel(LogLevel.Warning));
        logger = loggerFactory.CreateLogger<IRaft>();
    }

    private static RaftConfiguration NodeConfig(string name, int id, int port) => new()
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
        ReseedRequestTimeout = TimeSpan.FromSeconds(15),
    };

    private IRaft NewNode(InMemoryCommunication communication, string name, int id, int port, params string[] peers) =>
        new RaftManager(
            NodeConfig(name, id, port),
            new StaticDiscovery([.. peers.Select(p => new RaftNode(p))]),
            new InMemoryWAL(logger),
            communication,
            new HybridLogicalClock(),
            logger);

    private async Task<IRaft[]> AssembleThreeNodeCluster(InMemoryCommunication communication, Action<IRaft>? configure = null)
    {
        IRaft node1 = NewNode(communication, "node1", 1, 8001, "localhost:8002", "localhost:8003");
        IRaft node2 = NewNode(communication, "node2", 2, 8002, "localhost:8001", "localhost:8003");
        IRaft node3 = NewNode(communication, "node3", 3, 8003, "localhost:8001", "localhost:8002");

        communication.SetNodes(new()
        {
            { "localhost:8001", node1 },
            { "localhost:8002", node2 },
            { "localhost:8003", node3 },
        });

        IRaft[] nodes = [node1, node2, node3];
        foreach (IRaft node in nodes)
            configure?.Invoke(node);

        await node1.UpdateNodes();
        await node2.UpdateNodes();
        await node3.UpdateNodes();

        await Task.WhenAll(
            node1.JoinCluster(TestContext.Current.CancellationToken),
            node2.JoinCluster(TestContext.Current.CancellationToken),
            node3.JoinCluster(TestContext.Current.CancellationToken));

        return nodes;
    }

    private static async Task<IRaft> WaitForLeaderAsync(IRaft[] nodes, int partitionId, IRaft? excluding = null)
    {
        for (int attempt = 0; attempt < 400; attempt++)
        {
            foreach (IRaft node in nodes)
            {
                if (excluding is not null && ReferenceEquals(node, excluding))
                    continue;

                if (await node.AmILeaderQuick(partitionId).ConfigureAwait(false))
                    return node;
            }

            await Task.Delay(25).ConfigureAwait(false);
        }

        throw new InvalidOperationException($"No leader elected for partition {partitionId}");
    }

    private static async Task WaitUntilAsync(Func<bool> condition, int timeoutMs, string what)
    {
        long deadline = Environment.TickCount64 + timeoutMs;
        while (Environment.TickCount64 < deadline)
        {
            if (condition())
                return;

            await Task.Delay(25).ConfigureAwait(false);
        }

        throw new TimeoutException($"Timed out after {timeoutMs} ms waiting for {what}");
    }

    private static async Task WaitUntilAsync(Func<Task<bool>> condition, int timeoutMs, string what)
    {
        long deadline = Environment.TickCount64 + timeoutMs;
        while (Environment.TickCount64 < deadline)
        {
            if (await condition().ConfigureAwait(false))
                return;

            await Task.Delay(25).ConfigureAwait(false);
        }

        throw new TimeoutException($"Timed out after {timeoutMs} ms waiting for {what}");
    }

    private static async Task LeaveAllAsync(IRaft[] nodes)
    {
        foreach (IRaft node in nodes)
        {
            try
            {
                await node.LeaveCluster(true).ConfigureAwait(false);
            }
            catch (Exception)
            {
                // Teardown only.
            }
        }
    }

    private static async Task<int> ReplicateAsync(IRaft leader, int count, CancellationToken ct)
    {
        int accepted = 0;
        for (int i = 0; i < count; i++)
        {
            RaftReplicationResult result = await leader.ReplicateLogs(UserPartition, "test", [1, 2, 3, (byte)i], cancellationToken: ct);
            if (result.Success)
                accepted++;
        }

        return accepted;
    }

    /// <summary>
    /// A replica whose candidacy is withheld never leads: a leadership transfer addressed to it is
    /// ignored (the leader keeps the partition), repeated step-downs of whoever leads elect one of the
    /// other replicas every time, and a release makes it electable again.
    /// </summary>
    [Fact]
    public async Task WithheldCandidacy_IsNeverElected_UntilReleased()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        InMemoryCommunication communication = new();
        IRaft[] nodes = await AssembleThreeNodeCluster(communication);

        try
        {
            IRaft leader = await WaitForLeaderAsync(nodes, UserPartition);
            IRaft withheld = nodes.First(n => !ReferenceEquals(n, leader));

            Assert.Equal(RaftOperationStatus.Success, withheld.SetCandidacyWithheld(UserPartition, true));
            Assert.True(withheld.IsCandidacyWithheld(UserPartition));
            Assert.Equal(RaftOperationStatus.Errored, withheld.SetCandidacyWithheld(9_999, true));

            // A transfer to the withheld replica is refused on its side: the leader keeps leading.
            RaftOperationStatus transfer = await leader.TransferLeadershipAsync(UserPartition, withheld.GetLocalEndpoint(), ct);
            Assert.NotEqual(RaftOperationStatus.Success, transfer);
            await Task.Delay(500, ct);
            Assert.False(await withheld.AmILeaderQuick(UserPartition));

            // Whoever leads steps down, three times: the withheld replica never wins the term.
            for (int round = 0; round < 3; round++)
            {
                IRaft current = await WaitForLeaderAsync(nodes, UserPartition);
                Assert.NotSame(withheld, current);

                await current.StepDownAsync(UserPartition, ct);

                IRaft next = await WaitForLeaderAsync(nodes, UserPartition);
                Assert.NotSame(withheld, next);
            }

            await Task.Delay(500, ct);
            Assert.False(await withheld.AmILeaderQuick(UserPartition));

            // Released: a transfer to it lands.
            Assert.Equal(RaftOperationStatus.Success, withheld.SetCandidacyWithheld(UserPartition, false));
            Assert.False(withheld.IsCandidacyWithheld(UserPartition));

            IRaft finalLeader = await WaitForLeaderAsync(nodes, UserPartition);
            transfer = await finalLeader.TransferLeadershipAsync(UserPartition, withheld.GetLocalEndpoint(), ct);
            Assert.True(transfer is RaftOperationStatus.Success or RaftOperationStatus.Pending, $"transfer after release: {transfer}");
            await WaitUntilAsync(async () => await withheld.AmILeaderQuick(UserPartition), 10_000, "the released replica to lead");
        }
        finally
        {
            await LeaveAllAsync(nodes);
        }
    }

    /// <summary>
    /// A follower that asks to be re-seeded gets a whole-partition snapshot from the leader at a
    /// checkpoint newer than its request, installed over the log it already holds, and resumes
    /// delivering committed entries above the installed boundary afterwards. A leader cannot ask.
    /// </summary>
    [Fact]
    public async Task ReseedRequest_ShipsAFreshCheckpointSnapshot_InstallsOverTheHeldLog_AndResumesApplies()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        InMemoryCommunication communication = new();
        RecordingPartitionTransfer transfer = new();
        Dictionary<string, int> delivered = [];

        IRaft[] nodes = await AssembleThreeNodeCluster(communication, node =>
        {
            node.RegisterPartitionStateTransfer(transfer);
            string endpoint = node.GetLocalEndpoint();
            node.OnReplicationReceived += (partition, log) =>
            {
                if (partition == UserPartition)
                    lock (delivered)
                        delivered[endpoint] = delivered.GetValueOrDefault(endpoint) + 1;

                return Task.FromResult(true);
            };
        });

        int Delivered(IRaft node)
        {
            lock (delivered)
                return delivered.GetValueOrDefault(node.GetLocalEndpoint());
        }

        try
        {
            IRaft leader = await WaitForLeaderAsync(nodes, UserPartition);
            IRaft follower = nodes.First(n => !ReferenceEquals(n, leader));

            Assert.Equal(10, await ReplicateAsync(leader, 10, ct));
            foreach (IRaft node in nodes)
                await WaitUntilAsync(() => Delivered(node) >= 10, 10_000, $"{node.GetLocalEndpoint()} to apply the first batch");

            long baselineCheckpoint = transfer.LastExportIndex;
            int followerDeliveredBefore = Delivered(follower);

            // A leader cannot be re-seeded; a follower can.
            Assert.Equal(RaftOperationStatus.NodeIsNotLeader, await leader.RequestReseedAsync(UserPartition, ct));
            Assert.Equal(RaftOperationStatus.Success, await follower.RequestReseedAsync(UserPartition, ct));
            Assert.Equal(RaftOperationStatus.Success, await follower.RequestReseedAsync(UserPartition, ct));

            // The leader takes a checkpoint and ships the snapshot at it; the follower imports it.
            await WaitUntilAsync(() => transfer.ImportCount >= 1, 15_000, "the follower to import the re-seed snapshot");
            Assert.True(transfer.ExportCount >= 1, "the leader exported nothing");
            Assert.True(transfer.LastExportIndex > baselineCheckpoint, $"export index {transfer.LastExportIndex} is not above the baseline {baselineCheckpoint}");
            Assert.True(transfer.LastExportIndex >= leader.GetCommitIndex(UserPartition) - 1 || transfer.LastExportIndex > 10,
                $"the export index {transfer.LastExportIndex} does not sit above the entries the follower had applied");

            // Applies resumed: entries replicated after the install reach the follower's consumer.
            await WaitUntilAsync(() => leader.GetSnapshotStatuses(UserPartition).Count == 0, 10_000, "the transfer status to clear");
            int afterInstall = Delivered(follower);
            Assert.Equal(5, await ReplicateAsync(leader, 5, ct));
            await WaitUntilAsync(() => Delivered(follower) >= afterInstall + 5, 10_000, "the follower to apply entries after the re-seed");
            Assert.True(Delivered(follower) >= followerDeliveredBefore, "the follower lost delivered entries");

            // A repeat re-seed after completion starts a new cycle rather than being confused with the old one.
            int importsBefore = transfer.ImportCount;
            Assert.Equal(RaftOperationStatus.Success, await follower.RequestReseedAsync(UserPartition, ct));
            await WaitUntilAsync(() => transfer.ImportCount > importsBefore, 15_000, "the second re-seed to install");
        }
        finally
        {
            await LeaveAllAsync(nodes);
        }
    }

    /// <summary>
    /// A re-seed request no snapshot ever answers is bounded: the follower resumes delivering on its
    /// own once <see cref="RaftConfiguration.ReseedRequestTimeout"/> elapses, so an application that
    /// asked at the wrong moment does not leave the replica silently behind.
    /// </summary>
    [Fact]
    public async Task ReseedRequest_WithoutATransferRegistered_ExpiresAndResumesApplies()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        InMemoryCommunication communication = new();
        Dictionary<string, int> delivered = [];

        IRaft[] nodes = await AssembleThreeNodeCluster(communication, node =>
        {
            string endpoint = node.GetLocalEndpoint();
            node.OnReplicationReceived += (partition, log) =>
            {
                if (partition == UserPartition)
                    lock (delivered)
                        delivered[endpoint] = delivered.GetValueOrDefault(endpoint) + 1;

                return Task.FromResult(true);
            };
        });

        int Delivered(IRaft node)
        {
            lock (delivered)
                return delivered.GetValueOrDefault(node.GetLocalEndpoint());
        }

        try
        {
            IRaft leader = await WaitForLeaderAsync(nodes, UserPartition);
            IRaft follower = nodes.First(n => !ReferenceEquals(n, leader));

            Assert.Equal(4, await ReplicateAsync(leader, 4, ct));
            await WaitUntilAsync(() => Delivered(follower) >= 4, 10_000, "the follower to apply the first batch");

            Assert.Equal(RaftOperationStatus.Success, await follower.RequestReseedAsync(UserPartition, ct));

            // No transfer is registered anywhere, so the leader drops the request and nothing installs.
            // Entries replicated meanwhile are held back from the follower's consumer ...
            Assert.Equal(4, await ReplicateAsync(leader, 4, ct));
            await WaitUntilAsync(() => Delivered(leader) >= 8, 10_000, "the leader to apply the second batch");
            await Task.Delay(1_000, ct);
            Assert.Equal(4, Delivered(follower));

            // ... until the bound elapses and delivery resumes where it stopped.
            await WaitUntilAsync(() => Delivered(follower) >= 8, 30_000, "the follower to resume applies after the bound");
        }
        finally
        {
            await LeaveAllAsync(nodes);
        }
    }

    private sealed class RecordingPartitionTransfer : IRaftPartitionStateTransfer
    {
        private int exportCount;

        private int importCount;

        private long lastExportIndex = -1;

        public int ExportCount => Volatile.Read(ref exportCount);

        public int ImportCount => Volatile.Read(ref importCount);

        public long LastExportIndex => Volatile.Read(ref lastExportIndex);

        public Task<Stream> ExportPartitionState(int partitionId, long upToIndex, CancellationToken ct)
        {
            Interlocked.Increment(ref exportCount);
            Volatile.Write(ref lastExportIndex, upToIndex);
            return Task.FromResult<Stream>(new MemoryStream([0xCA, 0xFE, 0xBA, 0xBE]));
        }

        public Task ImportPartitionState(int partitionId, Stream snapshot, CancellationToken ct)
        {
            Interlocked.Increment(ref importCount);
            return Task.CompletedTask;
        }
    }
}
