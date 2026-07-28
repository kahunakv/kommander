
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
/// Snapshot chunks under partition. A learner joins below the compaction floor, so the
/// leader ships an install-snapshot. The nemesis drops the first snapshot chunk on the leader→learner link;
/// because the sender re-ships on the next heartbeat, the receiver must recover: exactly one import runs, the
/// learner is promoted to Voter, and no snapshot session or held message is leaked (bounded receiver
/// resources). Uses scripted selectors (occurrence-scoped drop) and an event barrier on the first chunk.
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
[Trait("Category", "ChaosSmoke")]
public sealed class Scenario01_SnapshotChunksUnderPartition
{
    private readonly ITestOutputHelper _out;
    public Scenario01_SnapshotChunksUnderPartition(ITestOutputHelper output) => _out = output;

    [Fact]
    public async Task DroppedFirstChunk_ResendsAndConverges_WithBoundedResources()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        NemesisCommunication nemesis = new(new InMemoryCommunication(), seed: 1001);

        string[] voters = ["localhost:8640", "localhost:8641", "localhost:8642"];
        RaftManager n1 = BuildNode(nemesis, 8640, 1, ["localhost:8641", "localhost:8642"], 1);
        RaftManager n2 = BuildNode(nemesis, 8641, 2, ["localhost:8640", "localhost:8642"], 1);
        RaftManager n3 = BuildNode(nemesis, 8642, 3, ["localhost:8640", "localhost:8641"], 1);
        RaftManager n4 = BuildNode(nemesis, 8643, 4, voters, 0);

        RecordingTransfer transfer = new();
        foreach (RaftManager n in new[] { n1, n2, n3, n4 })
            n.RegisterStateMachineTransfer(transfer);

        nemesis.SetNodes(new Dictionary<string, IRaft>
        {
            ["localhost:8640"] = n1, ["localhost:8641"] = n2,
            ["localhost:8642"] = n3, ["localhost:8643"] = n4,
        });

        ChaosSafetyMonitor? monitor = null;
        try
        {
            await Task.WhenAll(n1.JoinCluster(ct), n2.JoinCluster(ct), n3.JoinCluster(ct));
            await WaitForAsync(() => n1.IsInitialized && n2.IsInitialized && n3.IsInitialized, ct);

            RaftManager leader = await FindLeaderAsync([n1, n2, n3], ct);
            int partition = leader.Partitions.Keys.First(k => k != 0);

            // Attach the safety oracles (hash-chain observers + continuous invariant checker) over all four
            // nodes for this partition, BEFORE the first write, so every applied entry is recorded and safety
            // is evaluated continuously across the dropped-chunk fault window. The learner (n4) has not joined
            // the partition yet; its chain simply stays empty until it applies its post-snapshot tail.
            monitor = new ChaosSafetyMonitor([n1, n2, n3, n4], [partition], nemesis, seed: 1001, scenario: "snapshot-chunks-under-partition");

            for (int i = 0; i < 5; i++)
                await leader.ReplicateLogs(partition, "chaos", [1, 2, 3], cancellationToken: ct);

            RaftReplicationResult cp = await leader.ReplicateCheckpoint(partition, ct);
            Assert.Equal(RaftOperationStatus.Success, cp.Status);
            await WaitForAsync(() => n1.Partitions.ContainsKey(partition), ct);

            // Scripted: drop the FIRST install-snapshot chunk the leader sends to the learner. The sender
            // re-ships on the next heartbeat, so the session must recover rather than wedge.
            nemesis.AddRule(new NemesisRule
            {
                Source = leader.LocalEndpoint, Destination = "localhost:8643",
                Verb = NemesisVerb.InstallSnapshot, Action = FaultAction.Drop, Occurrence = 1, Name = "chunk-drop",
            });

            await n4.JoinCluster(voters, ct);

            // The learner recovered from the dropped chunk: promoted, applied exactly once.
            Assert.Equal(ClusterMemberRole.Voter, n4.LocalRole);
            Assert.True(transfer.ImportWasCalled, "learner should have imported the snapshot after resend");

            // The scripted drop actually fired, and no receiver resources leaked.
            Assert.Contains(nemesis.AllEvents(),
                e => e.Kind == NemesisEventKind.Drop && e.Envelope.Verb == NemesisVerb.InstallSnapshot);
            Assert.Equal(0, nemesis.HeldCount);

            // No safety invariant (two leaders, commit regression, conflicting applied prefixes, quorum
            // discipline) was violated at any point during the fault window.
            monitor.ThrowIfViolated();
        }
        catch (Exception ex)
        {
            if (monitor is not null)
                _out.WriteLine(await monitor.BuildFailureReportAsync($"scenario-failure: {ex.Message}", ct));
            throw;
        }
        finally
        {
            if (monitor is not null)
                await monitor.DisposeAsync();
            foreach (RaftManager n in new[] { n1, n2, n3, n4 })
                n.Dispose();
        }
    }

    private static RaftManager BuildNode(NemesisCommunication comm, int port, int nodeId, string[] peers, int initialPartitions)
    {
        IWAL wal = initialPartitions == 0
            ? new InMemoryWAL(NullLogger<IRaft>.Instance)
            : new CompactableWAL(new InMemoryWAL(NullLogger<IRaft>.Instance));
        RaftConfiguration cfg = new()
        {
            NodeId = nodeId, Host = "localhost", Port = port,
            InitialPartitions = initialPartitions,
            HeartbeatInterval = TimeSpan.FromMilliseconds(50),
            RecentHeartbeat = TimeSpan.FromMilliseconds(25),
            VotingTimeout = TimeSpan.FromMilliseconds(500),
            CheckLeaderInterval = TimeSpan.FromMilliseconds(25),
            UpdateNodesInterval = TimeSpan.FromMilliseconds(200),
            TimerInitialDelay = TimeSpan.FromMilliseconds(25),
            StartElectionTimeout = 100, EndElectionTimeout = 300, ElectionTimeoutSeed = 1001,
            EnableQuiescence = false, BackfillThreshold = 0, MaxBackfillEntriesPerRound = 128,
            LearnerPromotionLag = 5, LearnerPromotionStableWindow = TimeSpan.FromMilliseconds(500),
        };
        return new RaftManager(cfg, new StaticDiscovery(peers.Select(e => new RaftNode(e)).ToList()),
            wal, comm, new HybridLogicalClock(), NullLogger<IRaft>.Instance);
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
