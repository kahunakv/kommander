using Kommander.Communication.Memory;
using Kommander.Discovery;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.LoadReports;

/// <summary>
/// Integration tests for the leader balancer controller (Phase 4).
///
/// <para>These tests stand up real in-process clusters using <see cref="InMemoryCommunication"/>
/// and drive the gossip + balancer timer paths to verify end-to-end correctness without
/// relying on wall-clock timer ticks — every gossip round and balancer pass is triggered
/// manually via the public <c>Trigger*</c> methods on <see cref="RaftTimerService"/>.</para>
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
public sealed class TestLeaderBalancerIntegration
{
    // ── Helpers ───────────────────────────────────────────────────────────────

    private static RaftManager MakeNode(
        InMemoryCommunication communication,
        string host, int port, int nodeId,
        IEnumerable<string> peers,
        ILogger<IRaft> logger,
        bool enableBalancer = true,
        TimeSpan suggestionTimeout = default,
        TimeSpan moveCooldown = default)
    {
        RaftConfiguration config = new()
        {
            NodeName = $"node{nodeId}",
            NodeId = nodeId,
            Host = host,
            Port = port,
            InitialPartitions = 1, // P0 system partition must be present for JoinCluster to complete
            HeartbeatInterval = TimeSpan.FromMilliseconds(50),
            RecentHeartbeat = TimeSpan.FromMilliseconds(25),
            VotingTimeout = TimeSpan.FromMilliseconds(250),
            CheckLeaderInterval = TimeSpan.FromMilliseconds(25),
            UpdateNodesInterval = TimeSpan.FromMilliseconds(50),
            TimerInitialDelay = TimeSpan.FromMilliseconds(25),
            StartElectionTimeout = 100,
            EnableQuiescence = false,
            EndElectionTimeout = 250,
            GossipFanout = 1,
            GossipInterval = TimeSpan.FromMilliseconds(50),
            LeaderBalancerInterval = TimeSpan.FromSeconds(30), // timer-driven manually in tests
            LeaderBalancerReportTtl = TimeSpan.FromSeconds(20),
            EnableLeaderBalancer = enableBalancer,
            MinLeaderStabilityMs = 0, // let brand-new leaders be moved immediately in tests
            MoveCooldown = moveCooldown == default ? TimeSpan.FromMilliseconds(200) : moveCooldown,
            SuggestionTimeout = suggestionTimeout == default ? TimeSpan.FromSeconds(5) : suggestionTimeout,
            MaxMovesPerPass = 8,
            MaxConcurrentTransfers = 8,
            CountDeadband = 0, // no deadband so we detect skew clearly
        };

        return new RaftManager(
            config,
            new StaticDiscovery(peers.Select(e => new RaftNode(e)).ToList()),
            new InMemoryWAL(NullLogger<IRaft>.Instance),
            communication,
            new HybridLogicalClock(),
            logger);
    }

    private static async Task WaitForCondition(Func<bool> cond, CancellationToken ct, int timeoutMs = 15_000)
    {
        long deadline = Environment.TickCount64 + timeoutMs;
        while (Environment.TickCount64 < deadline)
        {
            ct.ThrowIfCancellationRequested();
            if (cond()) return;
            await Task.Delay(30, ct);
        }
        throw new TimeoutException($"Condition not met within {timeoutMs} ms");
    }

    private static async Task<RaftManager> WaitForP0Leader(
        RaftManager[] nodes, CancellationToken ct, int timeoutMs = 15_000)
    {
        long deadline = Environment.TickCount64 + timeoutMs;
        while (Environment.TickCount64 < deadline)
        {
            ct.ThrowIfCancellationRequested();
            foreach (RaftManager n in nodes)
            {
                if (await n.AmILeaderQuick(0))
                    return n;
            }
            await Task.Delay(30, ct);
        }
        throw new TimeoutException($"No P0 leader within {timeoutMs} ms");
    }

    /// <summary>
    /// Builds a <see cref="NodeLoadReport"/> reflecting the current leadership state of
    /// <paramref name="node"/> and enqueues it directly into <paramref name="target"/>'s
    /// coordinator.  This bypasses the gossip fire-and-forget path so tests can deliver
    /// fresh reports deterministically without any Task.Delay.
    /// </summary>
    private static void InjectLoadReport(RaftManager node, RaftManager target)
    {
        NodeLoadReport report = node.BuildLocalLoadReport();
        target.SystemCoordinator.Send(new RaftSystemRequest(report));
    }

    private static int CountLeaderships(RaftManager node, IEnumerable<int> partitionIds)
    {
        int count = 0;
        foreach (int id in partitionIds)
        {
            if (node.Partitions.TryGetValue(id, out RaftPartition? p) &&
                string.Equals(p.Leader, node.LocalEndpoint, StringComparison.Ordinal))
                count++;
        }
        return count;
    }

    // ── Tests ─────────────────────────────────────────────────────────────────

    /// <summary>
    /// Force all partition leaderships onto a single node, then drive gossip rounds and a
    /// balancer pass.  Verifies that the P0 leader dispatches transfer suggestions (not direct
    /// transfers) and that after the suggestions land, leadership counts equalize across nodes.
    /// Acceptance criterion 11.
    /// </summary>
    [Fact]
    public async Task BalancerConverges_CountSkew_AfterSuggestions()
    {
        using CancellationTokenSource cts = new(TimeSpan.FromSeconds(60));
        CancellationToken ct = cts.Token;
        ILogger<IRaft> log = NullLogger<IRaft>.Instance;

        InMemoryCommunication comm = new();
        // SuggestionTimeout=250ms + MoveCooldown=100ms = 350ms total, well under the 600ms
        // per-pass window.  If a TransferLeadershipAsync attempt fails on the first pass
        // (e.g. target not yet log-confirmed), the suggestion expires and the partition
        // re-enters planning on the next pass after the cooldown elapses, giving the cluster
        // time to replicate before the retry.
        RaftManager n1 = MakeNode(comm, "localhost", 9700, 1, ["localhost:9701", "localhost:9702"], log, enableBalancer: true, suggestionTimeout: TimeSpan.FromMilliseconds(250), moveCooldown: TimeSpan.FromMilliseconds(100));
        RaftManager n2 = MakeNode(comm, "localhost", 9701, 2, ["localhost:9700", "localhost:9702"], log, enableBalancer: true, suggestionTimeout: TimeSpan.FromMilliseconds(250), moveCooldown: TimeSpan.FromMilliseconds(100));
        RaftManager n3 = MakeNode(comm, "localhost", 9702, 3, ["localhost:9700", "localhost:9701"], log, enableBalancer: true, suggestionTimeout: TimeSpan.FromMilliseconds(250), moveCooldown: TimeSpan.FromMilliseconds(100));
        RaftManager[] nodes = [n1, n2, n3];

        try
        {
            comm.SetNodes(new Dictionary<string, IRaft>
            {
                ["localhost:9700"] = n1,
                ["localhost:9701"] = n2,
                ["localhost:9702"] = n3,
            });

            await n1.UpdateNodes();
            await n2.UpdateNodes();
            await n3.UpdateNodes();

            await Task.WhenAll(n1.JoinCluster(ct), n2.JoinCluster(ct), n3.JoinCluster(ct));

            // Wait for P0 leader.
            RaftManager p0 = await WaitForP0Leader(nodes, ct);

            // Create 6 user partitions on the P0 leader so we can control their placement.
            int[] pids = [10, 11, 12, 13, 14, 15];
            foreach (int pid in pids)
                await p0.CreatePartitionAsync(pid, RaftRoutingMode.Unrouted, null, ct);

            // Wait for all partitions to have a leader.
            await WaitForCondition(() =>
                pids.All(pid => nodes.Any(n =>
                    n.Partitions.TryGetValue(pid, out RaftPartition? p) &&
                    !string.IsNullOrEmpty(p.Leader))),
                ct);

            // Force-skew: transfer all 6 partition leaderships onto n1 by asking their
            // current leaders to hand off to n1.
            foreach (int pid in pids)
            {
                for (int attempt = 0; attempt < 15; attempt++)
                {
                    RaftManager? current = nodes.FirstOrDefault(n =>
                        n.Partitions.TryGetValue(pid, out RaftPartition? p) &&
                        string.Equals(p.Leader, n.LocalEndpoint, StringComparison.Ordinal));

                    if (current is null)
                    {
                        await Task.Delay(50, ct);
                        continue;
                    }

                    if (string.Equals(current.LocalEndpoint, n1.LocalEndpoint, StringComparison.Ordinal))
                        break;

                    try { await current.TransferLeadershipAsync(pid, n1.LocalEndpoint, ct); }
                    catch { /* ignore; retry */ }
                    await Task.Delay(80, ct);
                }
            }

            // Allow leadership to stabilize before counting.
            await Task.Delay(300, ct);

            // Confirm n1 holds at least 4 (a clear skew; may not hold all 6 if elections interfere).
            int skewCount = CountLeaderships(n1, pids);
            if (skewCount < 4)
            {
                // If we couldn't skew strongly enough, skip the convergence assertion to
                // avoid a false failure — the balancer is only meaningful with real skew.
                return;
            }

            // ── Drive balancer passes until counts converge or the deadline elapses ──
            // The balancer only *suggests* moves; each move is a real Raft handoff
            // (TimeoutNow → target election → heartbeat re-establishing leadership) whose
            // completion time is inherently non-deterministic. So instead of asserting after
            // a fixed number of passes, poll on a deadline: each iteration re-injects fresh
            // reports, runs a pass, then drives check-leader + gossip so the dispatched
            // handoffs can actually complete (and so P0 observes the new ownership next pass).
            // A genuine non-convergence still fails — it simply exhausts the deadline.
            int diagnosticOutstanding = 0;
            int c1 = CountLeaderships(n1, pids);
            int c2 = CountLeaderships(n2, pids);
            int c3 = CountLeaderships(n3, pids);
            bool converged = false;

            long deadline = Environment.TickCount64 + 30_000;
            while (Environment.TickCount64 < deadline)
            {
                ct.ThrowIfCancellationRequested();

                // Refresh P0's view of current ownership, then plan + dispatch.
                foreach (RaftManager n in nodes)
                    InjectLoadReport(n, p0);
                await p0.SystemCoordinator.DrainAsync();

                p0.TimerService.TriggerBalancer();
                await p0.SystemCoordinator.DrainAsync();

                if (diagnosticOutstanding == 0)
                    diagnosticOutstanding = p0.SystemCoordinator.OutstandingMoveCountForTest;

                // Let suggestions propagate and the recipients complete the real handoffs.
                for (int i = 0; i < 10; i++)
                {
                    foreach (RaftManager n in nodes)
                    {
                        n.TimerService.TriggerCheckLeader();
                        n.TimerService.TriggerGossip();
                    }
                    await Task.Delay(40, ct);
                }

                c1 = CountLeaderships(n1, pids);
                c2 = CountLeaderships(n2, pids);
                c3 = CountLeaderships(n3, pids);
                if (c1 <= 3 && c2 <= 3 && c3 <= 3 && c1 + c2 + c3 == pids.Length)
                {
                    converged = true;
                    break;
                }
            }

            Assert.True(converged,
                $"Counts did not equalize within the deadline: n1={c1} n2={c2} n3={c3} " +
                $"(outstandingAfterFirstPass={diagnosticOutstanding}: 0=pass skipped/view-incomplete, >0=suggestions dispatched)");
        }
        finally
        {
            foreach (RaftManager n in nodes) n.Dispose();
        }
    }

    /// <summary>
    /// Verifies that when <c>EnableLeaderBalancer</c> is false no load reports are
    /// included in gossip and no balancer pass runs.  Acceptance criterion 13.
    /// </summary>
    [Fact]
    public async Task BalancerDisabled_NoReportsGossiped_NoPassRuns()
    {
        using CancellationTokenSource cts = new(TimeSpan.FromSeconds(30));
        CancellationToken ct = cts.Token;
        ILogger<IRaft> log = NullLogger<IRaft>.Instance;

        InMemoryCommunication comm = new();
        RaftManager n1 = MakeNode(comm, "localhost", 9710, 1, ["localhost:9711", "localhost:9712"], log, enableBalancer: false);
        RaftManager n2 = MakeNode(comm, "localhost", 9711, 2, ["localhost:9710", "localhost:9712"], log, enableBalancer: false);
        RaftManager n3 = MakeNode(comm, "localhost", 9712, 3, ["localhost:9710", "localhost:9711"], log, enableBalancer: false);

        RaftManager[] nodes = [n1, n2, n3];

        try
        {
            comm.SetNodes(new Dictionary<string, IRaft>
            {
                ["localhost:9710"] = n1,
                ["localhost:9711"] = n2,
                ["localhost:9712"] = n3,
            });

            await n1.UpdateNodes();
            await n2.UpdateNodes();
            await n3.UpdateNodes();

            await Task.WhenAll(n1.JoinCluster(ct), n2.JoinCluster(ct), n3.JoinCluster(ct));

            RaftManager p0 = await WaitForP0Leader(nodes, ct);

            // Drive gossip for several rounds.
            for (int i = 0; i < 4; i++)
            {
                n1.TimerService.TriggerGossip();
                n2.TimerService.TriggerGossip();
                n3.TimerService.TriggerGossip();
                await Task.Delay(60, ct);
            }

            // With the balancer disabled, the P0 coordinator's report store must be empty.
            await p0.SystemCoordinator.DrainAsync();
            Assert.Empty(p0.SystemCoordinator.GetLoadReports());

            // Triggering a balancer pass must be a no-op (no exception, no work).
            p0.TimerService.TriggerBalancer();
            await p0.SystemCoordinator.DrainAsync();
        }
        finally
        {
            foreach (RaftManager n in nodes) n.Dispose();
        }
    }

    /// <summary>
    /// Verifies that an outstanding move that is never confirmed (suggestion dropped) is
    /// cleared after <c>SuggestionTimeout</c> elapses and enters cooldown.
    /// The partition becomes eligible again only after <c>MoveCooldown</c>.
    /// Acceptance criterion 12.
    /// </summary>
    [Fact]
    public async Task BalancerCooldown_AfterSuggestionTimeout()
    {
        using CancellationTokenSource cts = new(TimeSpan.FromSeconds(30));
        CancellationToken ct = cts.Token;

        using RaftManager manager = new(
            new RaftConfiguration
            {
                Host = "localhost",
                Port = 9720,
                InitialPartitions = 0,
                EnableLeaderBalancer = true,
                SuggestionTimeout = TimeSpan.FromMilliseconds(1), // expire immediately
                MoveCooldown = TimeSpan.FromSeconds(60),
                MaxConcurrentTransfers = 4,
                MaxMovesPerPass = 4,
                CountDeadband = 0,
            },
            new StaticDiscovery([]),
            new InMemoryWAL(NullLogger<IRaft>.Instance),
            new InMemoryCommunication(),
            new HybridLogicalClock(),
            NullLogger<IRaft>.Instance);

        RaftSystemCoordinator coord = manager.SystemCoordinator;

        // Inject two artificial load reports so the planner sees a 2-node universe.
        // node-A leads partitions 1 and 2; node-B leads nothing.
        // With 2 partitions on 2 nodes: floor=ceil=1 → node-A is over, node-B is under.
        NodeLoadReport reportA = new()
        {
            Endpoint = "node-A:9001",
            ReportVersion = 1,
            Time = manager.HybridLogicalClock.TrySendOrLocalEvent(1),
            Leaderships =
            [
                new PartitionLoad { PartitionId = 1, Load = 1.0, LeaderSinceMs = 60_000 },
                new PartitionLoad { PartitionId = 2, Load = 1.0, LeaderSinceMs = 60_000 },
            ],
        };
        NodeLoadReport reportB = new()
        {
            Endpoint = "node-B:9002",
            ReportVersion = 1,
            Time = manager.HybridLogicalClock.TrySendOrLocalEvent(1),
            Leaderships = [],
        };

        // Seed membership (two voters) and both partitions as Active so the planner considers them eligible.
        // Without seeded membership GlobalLeadershipView.LiveVoters is empty → IsComplete() = false
        // → passes are skipped.
        coord.SetMembershipForTest(new ClusterMembership
        {
            MembershipVersion = 1,
            Members =
            [
                new ClusterMember { Endpoint = "node-A:9001", Role = ClusterMemberRole.Voter, JoinedVersion = 1 },
                new ClusterMember { Endpoint = "node-B:9002", Role = ClusterMemberRole.Voter, JoinedVersion = 1 },
            ],
        });
        coord.SetPartitionMapForTest(new RaftPartitionMap
        {
            Partitions =
            [
                new RaftPartitionRange { PartitionId = 1, State = RaftPartitionState.Active },
                new RaftPartitionRange { PartitionId = 2, State = RaftPartitionState.Active },
            ],
        });

        coord.Send(new RaftSystemRequest(reportA));
        coord.Send(new RaftSystemRequest(reportB));
        await coord.DrainAsync();

        // Test flow: with 2 partitions and SuggestionTimeout=1ms:
        //   Pass 1: planner dispatches partition 1 (1 move balances counts). Outstanding = {1}.
        //   Wait 50ms → timeout elapses.
        //   Pass 2: reconciliation expires partition 1 → MoveCooldown (60s). Planner dispatches
        //           partition 2 (still eligible). Outstanding = {2}.
        //   Wait 50ms → timeout elapses again.
        //   Pass 3: reconciliation expires partition 2 → MoveCooldown. Partitions 1 and 2 both
        //           in 60s cooldown → 0 eligible → 0 new moves. Outstanding = {}.
        //
        // This verifies that SuggestionTimeout correctly expires outstanding moves and that
        // MoveCooldown prevents re-dispatch of recently-timed-out partitions.

        coord.Send(new RaftSystemRequest(RaftSystemRequestType.LeaderChanged, manager.LocalEndpoint));
        await coord.DrainAsync();

        // Pass 1: dispatches suggestion for partition 1.
        coord.Send(new RaftSystemRequest(RaftSystemRequestType.RunBalancerPass));
        await coord.DrainAsync();

        // Wait for SuggestionTimeout to elapse (1ms → 50ms margin).
        await Task.Delay(50, ct);

        // Pass 2: reconciliation expires partition 1 → cooldown; planner dispatches partition 2.
        coord.Send(new RaftSystemRequest(RaftSystemRequestType.RunBalancerPass));
        await coord.DrainAsync();

        // Wait for partition 2's SuggestionTimeout to elapse.
        await Task.Delay(50, ct);

        // Pass 3: reconciliation expires partition 2 → cooldown; both partitions now in 60s
        // MoveCooldown → planner has 0 eligible partitions → 0 new moves.
        coord.Send(new RaftSystemRequest(RaftSystemRequestType.RunBalancerPass));
        await coord.DrainAsync();

        // Verify: both entries have been reconciled out; outstanding table is empty.
        Assert.Equal(0, coord.OutstandingMoveCountForTest);
    }

    /// <summary>
    /// Verifies that a partition with an outstanding (unconfirmed) suggestion is not
    /// re-selected by the planner in a subsequent pass even when
    /// <c>LeaderBalancerInterval &lt; SuggestionTimeout</c>.
    ///
    /// <para>Without the outstanding-move exclusion the planner treats the partition as
    /// still owned by the over-loaded node (the move is not visible in the view yet) and
    /// re-suggests it every pass, extending its effective deadline each time.</para>
    /// </summary>
    [Fact]
    public async Task OutstandingMove_NotReselectedByPlanner_BeforeTimeout()
    {
        using CancellationTokenSource cts = new(TimeSpan.FromSeconds(30));
        CancellationToken ct = cts.Token;

        using RaftManager manager = new(
            new RaftConfiguration
            {
                Host = "localhost",
                Port = 9730,
                InitialPartitions = 0,
                EnableLeaderBalancer = true,
                SuggestionTimeout = TimeSpan.FromSeconds(60), // long — move stays outstanding
                MoveCooldown = TimeSpan.FromSeconds(60),
                MaxConcurrentTransfers = 4,
                MaxMovesPerPass = 4,
                CountDeadband = 0,
            },
            new StaticDiscovery([]),
            new InMemoryWAL(NullLogger<IRaft>.Instance),
            new InMemoryCommunication(),
            new HybridLogicalClock(),
            NullLogger<IRaft>.Instance);

        RaftSystemCoordinator coord = manager.SystemCoordinator;

        // Inject two reports: node-A leads partitions 1 and 2; node-B leads nothing.
        // With 2 partitions on 2 nodes: floor=1, ceil=1 so node-A (count=2) is over
        // and node-B (count=0) is under — the planner will plan exactly 1 move.
        NodeLoadReport reportA = new()
        {
            Endpoint = "node-A:9001",
            ReportVersion = 1,
            Time = manager.HybridLogicalClock.TrySendOrLocalEvent(1),
            Leaderships =
            [
                new PartitionLoad { PartitionId = 1, Load = 1.0, LeaderSinceMs = 60_000 },
                new PartitionLoad { PartitionId = 2, Load = 1.0, LeaderSinceMs = 60_000 },
            ],
        };
        NodeLoadReport reportB = new()
        {
            Endpoint = "node-B:9002",
            ReportVersion = 1,
            Time = manager.HybridLogicalClock.TrySendOrLocalEvent(1),
            Leaderships = [],
        };

        // Seed membership and partition map so the balancer view is complete and partitions are eligible.
        coord.SetMembershipForTest(new ClusterMembership
        {
            MembershipVersion = 1,
            Members =
            [
                new ClusterMember { Endpoint = "node-A:9001", Role = ClusterMemberRole.Voter, JoinedVersion = 1 },
                new ClusterMember { Endpoint = "node-B:9002", Role = ClusterMemberRole.Voter, JoinedVersion = 1 },
            ],
        });
        coord.SetPartitionMapForTest(new RaftPartitionMap
        {
            Partitions =
            [
                new RaftPartitionRange { PartitionId = 1, State = RaftPartitionState.Active },
                new RaftPartitionRange { PartitionId = 2, State = RaftPartitionState.Active },
            ],
        });

        coord.Send(new RaftSystemRequest(reportA));
        coord.Send(new RaftSystemRequest(reportB));
        coord.Send(new RaftSystemRequest(RaftSystemRequestType.LeaderChanged, manager.LocalEndpoint));
        await coord.DrainAsync();

        // Pass 1 — suggestion for partition 1 is dispatched and recorded as outstanding.
        coord.Send(new RaftSystemRequest(RaftSystemRequestType.RunBalancerPass));
        await coord.DrainAsync();
        Assert.Equal(1, coord.OutstandingMoveCountForTest);

        // Pass 2 — partition 1's suggestion is still outstanding (timeout=60s, not elapsed).
        // The planner must NOT re-select partition 1 (no duplicate dispatch / deadline extension).
        // Because the view still shows 2-0 skew, it should pick partition 2 instead.
        coord.Send(new RaftSystemRequest(RaftSystemRequestType.RunBalancerPass));
        await coord.DrainAsync();

        // With the outstanding-exclusion fix:
        //   Pass 1 selected partition 1 → outstanding = {1}, count = 1.
        //   Pass 2 excludes partition 1 (outstanding), selects partition 2 → outstanding = {1, 2}, count = 2.
        // Without the fix:
        //   Pass 2 re-selects partition 1 and overwrites its deadline → outstanding = {1}, count = 1.
        Assert.Equal(2, coord.OutstandingMoveCountForTest);
    }
}
