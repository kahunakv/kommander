
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
/// Deposed leader snapshot. The scenario drives a real leadership change and asserts that a genuinely stale
/// install-snapshot chunk — one a leader had in flight at the moment it was deposed — is rejected through the
/// executor's term/leader validation once the cluster has moved to a higher term.
///
/// <para>Sequence: elect a leader at term T and commit some entries; craft the leader's final install-snapshot
/// chunk (its own endpoint + term T) and <b>hold</b> it in transport so it parks unapplied (bounded receiver
/// resources). Isolate that leader from the other two voters, who form a quorum and elect a new leader at a
/// higher term T' &gt; T; the receiver adopts T'. Release the parked chunk: it now carries a superseded
/// leader/term, no matching snapshot boundary is installed at its index, so the Rule-7 stale-term guard rejects
/// it with <b>no application import</b> (the boundary-identity idempotency shortcut does not fire on an
/// unrelated WAL id). Throughout, the safety oracles run continuously: the deliberate deposed-leader-at-T /
/// new-leader-at-T' coexistence is not an election-safety violation (that invariant keys on same-term
/// duplicate leaders), and no conflicting applied prefix or commit regression is permitted.</para>
///
/// <para>Determinism: per-node election timeouts are seeded, and the hold/release is scripted, so the term
/// change and the rejection reproduce. Election timing does vary the exact higher term value, but the assertion
/// only requires <c>T' &gt; T</c>, which the isolation guarantees.</para>
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
[Trait("Category", "ChaosSmoke")]
public sealed class Scenario02_DeposedLeaderSnapshot
{
    private readonly ITestOutputHelper _out;
    public Scenario02_DeposedLeaderSnapshot(ITestOutputHelper output) => _out = output;

    [Fact]
    public async Task DeposedLeadersInflightChunk_RejectedAfterHigherTermElection_WithoutImport()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        NemesisCommunication nemesis = new(new InMemoryCommunication(), seed: 2002);

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

        ChaosSafetyMonitor? monitor = null;
        try
        {
            await Task.WhenAll(nodes.Select(n => n.JoinCluster(ct)));
            await WaitForAsync(() => nodes.All(n => n.IsInitialized), ct);

            RaftManager deposed = await FindLeaderAsync(nodes, ct);
            int partition = deposed.Partitions.Keys.First(k => k != 0);

            monitor = new ChaosSafetyMonitor(nodes, [partition], nemesis, seed: 2002, scenario: "deposed-leader-snapshot");

            for (int i = 0; i < 5; i++)
                await deposed.ReplicateLogs(partition, "chaos", [1, 2, 3], cancellationToken: ct);

            RaftManager[] others = nodes.Where(n => n != deposed).ToArray();
            RaftManager receiver = others[0];
            // Wait on the receiver's durable APPLIED frontier, not its raw WAL max: the term-based rejection
            // below only needs a committed index the receiver already holds, and the applied prefix is the
            // stable signal (a follower's WAL max can transiently trail its applied frontier under the
            // unanchored live-propose path, which would make a MaxWalIndex wait flake).
            await WaitForAsync(() => receiver.GetPartitionViewAsync(partition, ct).GetAwaiter().GetResult() is { LastAppliedIndex: >= 5 }, ct);
            RaftPartitionView deposedView = (await deposed.GetPartitionViewAsync(partition, ct))!;
            long oldTerm = deposedView.Term;
            long staleIndex = (await receiver.GetPartitionViewAsync(partition, ct))!.LastAppliedIndex;

            // The chunk the deposed leader had in flight when it lost leadership: its OWN endpoint and term T,
            // for an index the receiver already holds. Held in transport so it parks unapplied.
            SnapshotRequest stale = new()
            {
                SessionId = "deposed-session", PartitionId = partition,
                SnapshotIndex = staleIndex,
                FollowerEndpoint = receiver.LocalEndpoint,
                LeaderEndpoint = deposed.LocalEndpoint, LeaderTerm = oldTerm, LastIncludedTerm = oldTerm,
                ChunkIndex = 0, IsLast = true, Data = new byte[] { 0xFF },
            };

            nemesis.Hold(deposed.LocalEndpoint, receiver.LocalEndpoint, verb: NemesisVerb.InstallSnapshot, name: "hold-stale");
            Task<SnapshotResponse> heldSend = Task.Run(
                () => nemesis.SendInstallSnapshot(deposed, new RaftNode(receiver.LocalEndpoint), stale, ct), ct);

            await WaitForAsync(() => nemesis.HeldCount >= 1, ct);
            Assert.False(transfer.ImportWasCalled, "held stale chunk must not have been imported");

            // Depose the leader for real: isolate it from both other voters, who retain a quorum and elect a new
            // leader at a strictly higher term. The receiver adopts that higher term, so the parked chunk's term
            // is now stale.
            nemesis.PartitionSymmetric(deposed.LocalEndpoint, others[0].LocalEndpoint, name: "isolate-a");
            nemesis.PartitionSymmetric(deposed.LocalEndpoint, others[1].LocalEndpoint, name: "isolate-b");

            RaftManager newLeader = await WaitForHigherTermLeaderAsync(others, partition, oldTerm, ct);
            long newTerm = (await newLeader.GetPartitionViewAsync(partition, ct))!.Term;
            Assert.True(newTerm > oldTerm, $"a higher-term leader must have been elected (old={oldTerm}, new={newTerm})");
            await WaitForAsync(() => receiver.GetPartitionViewAsync(partition, ct).GetAwaiter().GetResult() is { Term: var t } && t > oldTerm, ct);

            // Release the parked chunk. It carries the deposed leader's endpoint and the old term, below the
            // receiver's current term, and no snapshot boundary is installed at its index, so the executor's
            // Rule-7 stale-term validation rejects it — the idempotency shortcut is boundary-identity-based and
            // does not apply to an unrelated WAL id.
            int releasedCount = await nemesis.ReleaseHeldAsync(destination: receiver.LocalEndpoint, verb: NemesisVerb.InstallSnapshot);
            Assert.Equal(1, releasedCount);
            SnapshotResponse resp = await heldSend.WaitAsync(TimeSpan.FromSeconds(10), ct);

            Assert.False(resp.Success, "the deposed leader's stale-term chunk must be rejected");
            Assert.False(transfer.ImportWasCalled, "a rejected chunk must not import application state");
            Assert.Equal(0, nemesis.HeldCount);

            // No safety invariant was violated across the isolation + higher-term election.
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
            foreach (RaftManager n in nodes)
                n.Dispose();
        }
    }

    /// <summary>Waits for a single node among <paramref name="candidates"/> to report leadership for the
    /// partition at a term strictly greater than <paramref name="oldTerm"/>, and returns it.</summary>
    private static async Task<RaftManager> WaitForHigherTermLeaderAsync(
        RaftManager[] candidates, int partition, long oldTerm, CancellationToken ct)
    {
        RaftManager? found = null;
        ValueStopwatch sw = ValueStopwatch.StartNew();
        while (sw.GetElapsedMilliseconds() < 20_000)
        {
            ct.ThrowIfCancellationRequested();
            foreach (RaftManager n in candidates)
            {
                if (!await n.AmILeaderQuick(partition)) continue;
                RaftPartitionView? v = await n.GetPartitionViewAsync(partition, ct);
                if (v is { Term: var t } && t > oldTerm) { found = n; return found; }
            }
            await Task.Delay(50, ct);
        }
        throw new TimeoutException($"No higher-term (> {oldTerm}) leader elected within 20 s.");
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
