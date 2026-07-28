
using Kommander;
using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Diagnostics;
using Kommander.Discovery;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.Chaos.Scenarios;

/// <summary>
/// Fixed scenario 2 — deposed leader snapshot. A stale install-snapshot chunk (as a deposed leader would emit,
/// for an index the cluster has already moved past) is put on the wire and <b>held</b> in transport by the
/// nemesis: it parks unapplied, demonstrating bounded receiver resources. Meanwhile the live cluster commits
/// further and advances, superseding the chunk's index. When the stale chunk is finally released, the receiver
/// must reject it <b>without a second application import</b> — the Rule-7 already-installed / stale guard.
///
/// <para>Deterministic by construction: the stale chunk is crafted for an index at or below the receiver's WAL
/// frontier and driven through the real transport (<see cref="NemesisCommunication.SendInstallSnapshot"/>) with
/// a scripted hold/release, so there is no dependence on live re-election or learner-promotion timing. The
/// term-based rejection variants (stale-term, conflicting-leader-same-term) are covered deterministically by
/// the executor-level Rule-7 tests.</para>
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
[Trait("Category", "ChaosSmoke")]
public sealed class Scenario02_DeposedLeaderSnapshot
{
    [Fact]
    public async Task HeldStaleChunk_ReleasedAfterSupersede_RejectedWithoutImport()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        NemesisCommunication nemesis = new(new InMemoryCommunication(), seed: 2002);

        string[] all = ["localhost:8660", "localhost:8661", "localhost:8662"];
        RaftManager n1 = BuildNode(nemesis, 8660, 1, ["localhost:8661", "localhost:8662"]);
        RaftManager n2 = BuildNode(nemesis, 8661, 2, ["localhost:8660", "localhost:8662"]);
        RaftManager n3 = BuildNode(nemesis, 8662, 3, ["localhost:8660", "localhost:8661"]);
        RaftManager[] nodes = [n1, n2, n3];

        RecordingTransfer transfer = new();
        foreach (RaftManager n in nodes)
            n.RegisterStateMachineTransfer(transfer);

        nemesis.SetNodes(new Dictionary<string, IRaft>
        {
            ["localhost:8660"] = n1, ["localhost:8661"] = n2, ["localhost:8662"] = n3,
        });

        try
        {
            await Task.WhenAll(nodes.Select(n => n.JoinCluster(ct)));
            await WaitForAsync(() => nodes.All(n => n.IsInitialized), ct);

            RaftManager leader = await FindLeaderAsync(nodes, ct);
            int partition = leader.Partitions.Keys.First(k => k != 0);

            for (int i = 0; i < 5; i++)
                await leader.ReplicateLogs(partition, "chaos", [1, 2, 3], cancellationToken: ct);

            // Receiver is a follower; the deposed leader's stale chunk targets an index it already holds.
            RaftManager receiver = nodes.First(n => n != leader);
            await WaitForAsync(() => receiver.GetPartitionViewAsync(partition, ct).GetAwaiter().GetResult() is { MaxWalIndex: >= 5 }, ct);
            long staleIndex = (await receiver.GetPartitionViewAsync(partition, ct))!.MaxWalIndex;

            SnapshotRequest stale = new()
            {
                SessionId = "deposed-session", PartitionId = partition,
                SnapshotIndex = staleIndex,                 // already superseded → import must not re-run
                FollowerEndpoint = receiver.LocalEndpoint,
                LeaderEndpoint = "deposed:9999", LeaderTerm = 1, LastIncludedTerm = 1,
                ChunkIndex = 0, IsLast = true, Data = new byte[] { 0xFF },
            };

            // Hold the stale chunk in transport: it parks, unapplied (bounded receiver resources).
            nemesis.Hold(leader.LocalEndpoint, receiver.LocalEndpoint, verb: NemesisVerb.InstallSnapshot, name: "hold-stale");
            Task<SnapshotResponse> heldSend = Task.Run(
                () => nemesis.SendInstallSnapshot(leader, new RaftNode(receiver.LocalEndpoint), stale, ct), ct);

            await WaitForAsync(() => nemesis.HeldCount >= 1, ct);
            Assert.False(transfer.ImportWasCalled, "held stale chunk must not have been imported");

            // The live cluster keeps advancing while the stale chunk is parked (the "higher term" moves on).
            for (int i = 0; i < 5; i++)
                await leader.ReplicateLogs(partition, "chaos", [4, 5, 6], cancellationToken: ct);

            // Release the stale chunk: the receiver already holds this index, so it is rejected without import.
            int releasedCount = await nemesis.ReleaseHeldAsync(destination: receiver.LocalEndpoint, verb: NemesisVerb.InstallSnapshot);
            Assert.Equal(1, releasedCount);
            SnapshotResponse resp = await heldSend.WaitAsync(TimeSpan.FromSeconds(10), ct);

            // Rejection without application import: the state machine transfer was never invoked.
            Assert.False(transfer.ImportWasCalled, "stale chunk for an already-held index must not import");
            Assert.Equal(0, nemesis.HeldCount);
        }
        finally
        {
            foreach (RaftManager n in nodes)
                n.Dispose();
        }
    }

    private static RaftManager BuildNode(NemesisCommunication comm, int port, int nodeId, string[] peers)
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
            StartElectionTimeout = 100, EndElectionTimeout = 300, ElectionTimeoutSeed = 2002,
            EnableQuiescence = false, BackfillThreshold = 0, MaxBackfillEntriesPerRound = 128,
        };
        return new RaftManager(cfg, new StaticDiscovery(peers.Select(e => new RaftNode(e)).ToList()),
            new InMemoryWAL(NullLogger<IRaft>.Instance), comm, new HybridLogicalClock(), NullLogger<IRaft>.Instance);
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
