using System.Collections.Concurrent;
using System.Text;
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
/// End-to-end partition-lifecycle operations on a real in-process three-node cluster (legacy
/// full replication, so every node hosts every range): a split that actually moves data through
/// the snapshot transfer, a create/write/remove cycle, and a merge that absorbs a whole range.
/// Each operation is driven through the <see cref="RaftManager"/> public API by the node that
/// legitimately leads the partitions involved, and every assertion reads observable cluster
/// state (committed map, delivered writes, reclaimed WALs) rather than coordinator internals.
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
public sealed class TestPartitionLifecycleCluster
{
    private readonly ILogger<IRaft> logger;

    public TestPartitionLifecycleCluster(ITestOutputHelper outputHelper)
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
        IEnumerable<string> peers,
        int initialPartitions)
    {
        RaftConfiguration config = new()
        {
            NodeName = $"node{nodeId}",
            NodeId = nodeId,
            Host = "localhost",
            Port = port,
            InitialPartitions = initialPartitions,
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

    private async Task<RaftManager[]> AssembleCluster(int basePort, int initialPartitions, CancellationToken ct)
    {
        InMemoryCommunication comm = new();
        string Ep(int i) => $"localhost:{basePort + i}";

        RaftManager[] nodes =
        [
            MakeNode(comm, basePort + 1, 1, [Ep(2), Ep(3)], initialPartitions),
            MakeNode(comm, basePort + 2, 2, [Ep(1), Ep(3)], initialPartitions),
            MakeNode(comm, basePort + 3, 3, [Ep(1), Ep(2)], initialPartitions),
        ];

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

        return nodes;
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

    /// <summary>
    /// Returns the node currently leading <paramref name="partitionId"/>, or null. The
    /// leadership probe runs only on nodes that host the range: on a non-host the probe is the
    /// documented typed exception, not a "no".
    /// </summary>
    private static async Task<RaftManager?> FindLeaderOnce(RaftManager[] nodes, int partitionId)
    {
        foreach (RaftManager n in nodes)
        {
            if (n.HostsPartition(partitionId) && await n.AmILeaderQuick(partitionId))
                return n;
        }

        return null;
    }

    private static async Task<RaftManager> LeaderOf(RaftManager[] nodes, int partitionId, CancellationToken ct)
    {
        ValueStopwatch sw = ValueStopwatch.StartNew();
        while (sw.GetElapsedMilliseconds() < TestTimeouts.Scale(15_000))
        {
            ct.ThrowIfCancellationRequested();
            RaftManager? leader = await FindLeaderOnce(nodes, partitionId);
            if (leader is not null)
                return leader;
            await Task.Delay(25, ct);
        }
        throw new TimeoutException($"No leader elected for partition {partitionId} within 15 s.");
    }

    /// <summary>
    /// Moves leadership of <paramref name="partitionId"/> onto <paramref name="target"/> so a
    /// single node can legitimately drive an operation that needs leadership of several
    /// partitions at once (a merge needs both sides; every map mutation needs the system
    /// partition). The transfer is retried while the target has not replicated the leader's
    /// latest no-op yet — the same transient the four-node join test tolerates.
    /// </summary>
    private static async Task TransferLeadershipTo(
        RaftManager currentLeader, int partitionId, RaftManager target, CancellationToken ct)
    {
        long deadline = Environment.TickCount64 + TestTimeouts.Scale(15_000);
        while (Environment.TickCount64 < deadline)
        {
            ct.ThrowIfCancellationRequested();
            RaftOperationStatus st = await currentLeader.TransferLeadershipAsync(
                partitionId, target.LocalEndpoint, ct);
            if (st != RaftOperationStatus.ReplicationFailed)
                break;
            await Task.Delay(200, ct);
        }

        ValueStopwatch wait = ValueStopwatch.StartNew();
        while (wait.GetElapsedMilliseconds() < TestTimeouts.Scale(15_000))
        {
            ct.ThrowIfCancellationRequested();
            if (target.HostsPartition(partitionId) && await target.AmILeaderQuick(partitionId))
                return;
            await Task.Delay(25, ct);
        }
        throw new TimeoutException($"Leadership of partition {partitionId} never moved to {target.LocalEndpoint}.");
    }

    /// <summary>
    /// Key routing the way a hash-range consumer does it: a key belongs to the range whose
    /// [start, end] contains its unsigned-masked xxHash32.
    /// </summary>
    private static int KeyHash(string key) => HashUtils.SmallSimpleHash(key) & 0x7FFFFFFF;

    private static (string Left, string Right) KeysAroundBoundary(int boundary)
    {
        string? left = null, right = null;
        for (int i = 0; i < 1_000_000 && (left is null || right is null); i++)
        {
            string key = $"range-key-{i}";
            int h = KeyHash(key);
            if (h < boundary && left is null) left = key;
            else if (h >= boundary && right is null) right = key;
        }

        if (left is null || right is null)
            throw new InvalidOperationException("Could not find keys on both sides of the boundary.");
        return (left, right);
    }

    private static byte[] Payload(string key) => Encoding.UTF8.GetBytes($"{key}::value");

    /// <summary>Records every committed entry each node applies, as (endpoint, partition, key).</summary>
    private sealed class DeliveryLog
    {
        private readonly ConcurrentBag<(string Endpoint, int PartitionId, string Key)> delivered = [];

        public Task<bool> Record(string endpoint, int partitionId, RaftLog? log)
        {
            string? key = DecodeKey(log?.LogData);
            if (key is not null)
                delivered.Add((endpoint, partitionId, key));
            return Task.FromResult(true);
        }

        public void Attach(RaftManager node)
        {
            string endpoint = node.LocalEndpoint;
            node.OnReplicationReceived += (partitionId, log) => Record(endpoint, partitionId, log);
        }

        public int DistinctEndpoints(int partitionId, string key)
        {
            HashSet<string> endpoints = new(StringComparer.Ordinal);
            foreach ((string endpoint, int pid, string deliveredKey) in delivered)
            {
                if (pid == partitionId && deliveredKey == key)
                    endpoints.Add(endpoint);
            }
            return endpoints.Count;
        }

        public List<string> KeysOn(int partitionId) =>
            delivered.Where(d => d.PartitionId == partitionId)
                     .Select(d => d.Key)
                     .Distinct()
                     .ToList();

        private static string? DecodeKey(byte[]? data)
        {
            if (data is null)
                return null;
            string text = Encoding.UTF8.GetString(data);
            int separator = text.IndexOf("::", StringComparison.Ordinal);
            return separator > 0 ? text[..separator] : null;
        }
    }

    /// <summary>
    /// A state-machine transfer that behaves like a real consumer store would: it exports the
    /// keys that will belong to the new (right) half of the split range and records exactly what
    /// the coordinator asked it to export and where the snapshot was imported.
    /// </summary>
    private sealed class RecordingStateTransfer : IRaftStateMachineTransfer
    {
        private readonly Func<int, List<string>> keysInRightHalfOf;

        public RaftSplitPlan? ExportedPlan { get; private set; }
        public long ExportedUpToIndex { get; private set; }
        public (int TargetPartitionId, List<string> ImportedKeys)? Import { get; private set; }

        public RecordingStateTransfer(Func<int, List<string>> keysInRightHalfOf) =>
            this.keysInRightHalfOf = keysInRightHalfOf;

        public Task<Stream> ExportRange(RaftSplitPlan plan, long upToIndex, CancellationToken ct)
        {
            ExportedPlan = plan;
            ExportedUpToIndex = upToIndex;
            string payload = string.Join(',', keysInRightHalfOf(plan.HashBoundary ?? 0));
            return Task.FromResult<Stream>(new MemoryStream(Encoding.UTF8.GetBytes(payload)));
        }

        public Task ImportRange(int targetPartitionId, Stream snapshot, CancellationToken ct)
        {
            // Reading to the end closes the stream; the coordinator's own disposal is idempotent.
            using StreamReader reader = new(snapshot, Encoding.UTF8);
            string payload = reader.ReadToEnd();
            Import = (targetPartitionId, payload.Length == 0 ? [] : [.. payload.Split(',')]);
            return Task.CompletedTask;
        }
    }

    // ── Split with real data ───────────────────────────────────────────────────

    /// <summary>
    /// A split of a partition that holds data must move that data: the snapshot transfer runs
    /// between the source and the freshly created target (export bounded by the source's
    /// committed log index, import landing on the target partition id), the committed map ends
    /// up as two adjacent Active halves across the whole cluster, and new writes route to the
    /// half their key hashes into — including the new target partition.
    /// </summary>
    [Fact]
    public async Task SplitPartition_WithData_MovesKeysToTargetAndRoutesWritesThroughBothHalves()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        RaftManager[] nodes = await AssembleCluster(basePort: 8310, initialPartitions: 1, ct);

        try
        {
            const int sourcePartition = 1;
            const int targetPartition = 7;
            const int boundary = 1_000_000_000;

            DeliveryLog delivery = new();
            foreach (RaftManager n in nodes)
                delivery.Attach(n);

            // Seed data on both sides of the future boundary so the snapshot has something to move.
            (string leftKey, string rightKey) = KeysAroundBoundary(boundary);
            RaftManager writer = await LeaderOf(nodes, sourcePartition, ct);

            foreach (string key in new[] { leftKey, rightKey, $"{leftKey}-b", $"{rightKey}-b" })
            {
                RaftReplicationResult write = await writer.ReplicateLogs(
                    sourcePartition, "split-data", Payload(key), cancellationToken: ct);
                Assert.True(write.Success, $"Seed write failed: {write.Status}");
            }

            long seededMaxLog = writer.WalAdapter.GetMaxLog(sourcePartition);
            await WaitForCondition(
                () => delivery.DistinctEndpoints(sourcePartition, leftKey) >= 2 &&
                      delivery.DistinctEndpoints(sourcePartition, rightKey) >= 2,
                ct);

            // The map mutation is committed on the system partition, so the driver must lead it
            // as well as the source range: hand the system partition to the source's leader.
            RaftManager systemLeader = await LeaderOf(nodes, RaftSystemConfig.SystemPartition, ct);
            if (!ReferenceEquals(systemLeader, writer))
                await TransferLeadershipTo(systemLeader, RaftSystemConfig.SystemPartition, writer, ct);

            RecordingStateTransfer transfer = new(b =>
                delivery.KeysOn(sourcePartition).Where(k => KeyHash(k) >= b).ToList());
            foreach (RaftManager n in nodes)
                n.RegisterStateMachineTransfer(transfer);

            RaftPartitionLifecycleResult result = await writer.SplitPartitionAsync(
                sourcePartition, targetPartition,
                new RaftSplitPlan
                {
                    TargetPartitionId = targetPartition,
                    TargetRoutingMode = RaftRoutingMode.HashRange,
                    HashBoundary = boundary,
                },
                ct);

            Assert.True(result.Success, $"Split failed: {result.Status}");
            Assert.Equal(RaftOperationStatus.Success, result.Status);
            Assert.Equal(2, result.Generation); // the target's generation after both commits

            // The snapshot step ran against the real data: the export covered the committed log
            // and carried the split plan, and the import landed on the new partition with the
            // keys that now belong to its half.
            Assert.NotNull(transfer.ExportedPlan);
            Assert.Equal(targetPartition, transfer.ExportedPlan!.TargetPartitionId);
            Assert.Equal(boundary, transfer.ExportedPlan!.HashBoundary);
            Assert.True(transfer.ExportedUpToIndex >= seededMaxLog,
                $"Export was bounded by {transfer.ExportedUpToIndex}, below the seeded log index {seededMaxLog}.");
            Assert.NotNull(transfer.Import);
            Assert.Equal(targetPartition, transfer.Import!.Value.TargetPartitionId);
            Assert.Contains(rightKey, transfer.Import!.Value.ImportedKeys);
            Assert.Contains($"{rightKey}-b", transfer.Import!.Value.ImportedKeys);
            Assert.DoesNotContain(leftKey, transfer.Import!.Value.ImportedKeys);

            // The committed map settles on every node: two adjacent Active halves.
            await WaitForCondition(
                () => nodes.All(n =>
                {
                    IReadOnlyList<RaftPartitionRange> map = n.GetPartitionMap();
                    RaftPartitionRange? source = map.FirstOrDefault(r => r.PartitionId == sourcePartition);
                    RaftPartitionRange? target = map.FirstOrDefault(r => r.PartitionId == targetPartition);
                    return source is { State: RaftPartitionState.Active, StartRange: 0, EndRange: boundary - 1, Generation: 3 } &&
                           target is { State: RaftPartitionState.Active, StartRange: boundary, EndRange: int.MaxValue, Generation: 2 };
                }),
                ct, timeoutMs: 20_000);

            // New writes route to whichever half owns their key, and commit.
            RaftManager leftLeader = await LeaderOf(nodes, sourcePartition, ct);
            RaftReplicationResult leftWrite = await leftLeader.ReplicateLogs(
                sourcePartition, "split-write", Payload($"{leftKey}-post"), cancellationToken: ct);
            Assert.True(leftWrite.Success, $"Write to the shrunken source failed: {leftWrite.Status}");

            RaftManager rightLeader = await LeaderOf(nodes, targetPartition, ct);
            RaftReplicationResult rightWrite = await rightLeader.ReplicateLogs(
                targetPartition, "split-write", Payload($"{rightKey}-post"), cancellationToken: ct);
            Assert.True(rightWrite.Success, $"Write to the new target failed: {rightWrite.Status}");

            await WaitForCondition(
                () => delivery.DistinctEndpoints(sourcePartition, $"{leftKey}-post") >= 2 &&
                      delivery.DistinctEndpoints(targetPartition, $"{rightKey}-post") >= 2,
                ct);
        }
        finally
        {
            foreach (RaftManager n in nodes)
                await n.LeaveCluster(true, CancellationToken.None);
        }
    }

    // ── Create → write → remove ───────────────────────────────────────────────

    /// <summary>
    /// A created partition is a first-class citizen cluster-wide — every node hosts it under
    /// full replication, it elects a leader and serves committed writes — and a removed
    /// partition is gone for good: its host stops it, reclaims its WAL, the committed map drops
    /// it everywhere, the node that committed the removal refuses further proposals, and a
    /// repeated remove is an idempotent success.
    /// </summary>
    [Fact]
    public async Task CreatePartition_ServesWrites_ThenRemoveStopsItEverywhere()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        RaftManager[] nodes = await AssembleCluster(basePort: 8320, initialPartitions: 1, ct);

        try
        {
            const int newPartition = 9;

            RaftManager systemLeader = await LeaderOf(nodes, RaftSystemConfig.SystemPartition, ct);
            RaftPartitionLifecycleResult created = await systemLeader.CreatePartitionAsync(
                newPartition, RaftRoutingMode.Unrouted, ct: ct);
            Assert.True(created.Success, $"Create failed: {created.Status}");
            Assert.Equal(1, created.Generation);

            await WaitForCondition(() => nodes.All(n => n.HostsPartition(newPartition)), ct, timeoutMs: 20_000);

            RaftManager partitionLeader = await LeaderOf(nodes, newPartition, ct);
            RaftReplicationResult write = await partitionLeader.ReplicateLogs(
                newPartition, "created-write", "created-payload"u8.ToArray(), cancellationToken: ct);
            Assert.True(write.Success, $"Write to the created partition failed: {write.Status}");
            Assert.True(write.LogIndex > 0);

            // Removal is again a system-partition mutation; re-resolve the leader rather than
            // assuming it survived the election churn a fresh partition can cause.
            RaftManager remover = await LeaderOf(nodes, RaftSystemConfig.SystemPartition, ct);
            RaftPartitionLifecycleResult removed = await remover.RemovePartitionAsync(newPartition, ct);
            Assert.True(removed.Success, $"Remove failed: {removed.Status}");

            // On the node that committed the removal the partition stopped synchronously and its
            // WAL was reclaimed.
            Assert.False(remover.HostsPartition(newPartition));
            Assert.False(remover.Partitions.ContainsKey(newPartition));
            Assert.Equal(0, remover.WalAdapter.GetMaxLog(newPartition));

            // Everywhere else the live map drops the range (the tombstone never re-creates it).
            await WaitForCondition(
                () => nodes.All(n => n.GetPartitionMap().All(r => r.PartitionId != newPartition)),
                ct, timeoutMs: 20_000);

            // The removed range refuses proposals: no node treats it as a target any more, so
            // the caller gets the invalid-partition diagnosis rather than a silent success.
            await Assert.ThrowsAsync<RaftException>(() =>
                remover.ReplicateLogs(newPartition, "after-remove", "x"u8.ToArray(), cancellationToken: ct));

            // A re-delivered remove is idempotent — it reports success without churning the map.
            RaftManager stillLeader = await LeaderOf(nodes, RaftSystemConfig.SystemPartition, ct);
            RaftPartitionLifecycleResult removedAgain = await stillLeader.RemovePartitionAsync(newPartition, ct);
            Assert.True(removedAgain.Success, $"Idempotent remove failed: {removedAgain.Status}");
            Assert.DoesNotContain(stillLeader.GetPartitionMap(), r => r.PartitionId == newPartition);
        }
        finally
        {
            foreach (RaftManager n in nodes)
                await n.LeaveCluster(true, CancellationToken.None);
        }
    }

    // ── Merge ─────────────────────────────────────────────────────────────────

    /// <summary>
    /// A merge collapses two adjacent ranges into the survivor: the survivor's range grows to
    /// cover the whole hash space on every node, the node that committed the merge stops the
    /// source partition and reclaims its WAL, writes to keys that used to belong to the source
    /// now commit on the survivor, and the source partition id is no longer a valid target.
    /// </summary>
    [Fact]
    public async Task MergePartitions_AbsorbsSourceRangeIntoSurvivorClusterWide()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        RaftManager[] nodes = await AssembleCluster(basePort: 8330, initialPartitions: 2, ct);

        try
        {
            const int survivorPartition = 1;
            const int sourcePartition = 2;

            DeliveryLog delivery = new();
            foreach (RaftManager n in nodes)
                delivery.Attach(n);

            // The merge driver must lead both sides of the merge and the system partition.
            RaftManager survivorLeader = await LeaderOf(nodes, survivorPartition, ct);
            RaftManager sourceLeader = await LeaderOf(nodes, sourcePartition, ct);
            RaftManager systemLeader = await LeaderOf(nodes, RaftSystemConfig.SystemPartition, ct);

            if (!ReferenceEquals(sourceLeader, survivorLeader))
                await TransferLeadershipTo(sourceLeader, sourcePartition, survivorLeader, ct);

            if (!ReferenceEquals(systemLeader, survivorLeader))
                await TransferLeadershipTo(systemLeader, RaftSystemConfig.SystemPartition, survivorLeader, ct);

            // Seed the source range so the merge happens to a partition with real content.
            RaftPartitionRange sourceRange = nodes[0].GetPartitionMap().Single(r => r.PartitionId == sourcePartition);
            (string _, string rightKey) = KeysAroundBoundary(sourceRange.StartRange);
            RaftReplicationResult seeded = await survivorLeader.ReplicateLogs(
                sourcePartition, "merge-data", Payload(rightKey), cancellationToken: ct);
            Assert.True(seeded.Success, $"Seed write to the source range failed: {seeded.Status}");
            await WaitForCondition(() => delivery.DistinctEndpoints(sourcePartition, rightKey) >= 2, ct);

            RaftPartitionLifecycleResult merged = await survivorLeader.MergePartitionsAsync(
                survivorPartition, sourcePartition, ct: ct);

            Assert.True(merged.Success, $"Merge failed: {merged.Status}");
            Assert.Equal(RaftOperationStatus.Success, merged.Status);

            // The driver stopped the source and reclaimed its WAL; the survivor covers the whole
            // hash space on every node.
            Assert.False(survivorLeader.Partitions.ContainsKey(sourcePartition));
            Assert.Equal(0, survivorLeader.WalAdapter.GetMaxLog(sourcePartition));

            await WaitForCondition(
                () => nodes.All(n =>
                {
                    IReadOnlyList<RaftPartitionRange> map = n.GetPartitionMap();
                    return map.All(r => r.PartitionId != sourcePartition) &&
                           map.Single(r => r.PartitionId == survivorPartition) is
                               { State: RaftPartitionState.Active, StartRange: 0, EndRange: int.MaxValue };
                }),
                ct, timeoutMs: 20_000);

            // A key that hashed into the absorbed range now commits on the survivor and is
            // delivered as survivor traffic.
            RaftManager survivorWriter = await LeaderOf(nodes, survivorPartition, ct);
            RaftReplicationResult absorbedWrite = await survivorWriter.ReplicateLogs(
                survivorPartition, "merge-write", Payload($"{rightKey}-post"), cancellationToken: ct);
            Assert.True(absorbedWrite.Success, $"Write to the absorbed range failed: {absorbedWrite.Status}");

            await WaitForCondition(
                () => delivery.DistinctEndpoints(survivorPartition, $"{rightKey}-post") >= 2, ct);

            // The absorbed id is no longer a target on the node that committed the merge.
            await Assert.ThrowsAsync<RaftException>(() =>
                survivorLeader.ReplicateLogs(sourcePartition, "after-merge", "x"u8.ToArray(), cancellationToken: ct));
        }
        finally
        {
            foreach (RaftManager n in nodes)
                await n.LeaveCluster(true, CancellationToken.None);
        }
    }
}
