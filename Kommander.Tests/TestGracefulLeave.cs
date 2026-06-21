
using Kommander.Communication.Memory;
using Kommander.Diagnostics;
using Kommander.Discovery;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;

namespace Kommander.Tests;

/// <summary>
/// Regression tests for the graceful-leave single-voter short-circuit (G1/G2/G3).
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
public sealed class TestGracefulLeave
{
    private readonly ILogger<IRaft> _logger;

    public TestGracefulLeave(ITestOutputHelper outputHelper)
    {
        ILoggerFactory lf = LoggerFactory.Create(b => b
            .AddXUnit(outputHelper)
            .SetMinimumLevel(LogLevel.Warning));
        _logger = lf.CreateLogger<IRaft>();
    }

    private static RaftManager MakeNode(
        InMemoryCommunication communication,
        string host, int port, int nodeId,
        IEnumerable<string> peers,
        ILogger<IRaft> logger)
    {
        RaftConfiguration config = new()
        {
            NodeName = $"node{nodeId}",
            NodeId = nodeId,
            Host = host,
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

    private static async Task WaitForLeader(RaftManager[] nodes, int partitionId, CancellationToken ct)
    {
        ValueStopwatch sw = ValueStopwatch.StartNew();
        while (sw.GetElapsedMilliseconds() < 15_000)
        {
            ct.ThrowIfCancellationRequested();
            foreach (RaftManager n in nodes)
            {
                if (await n.AmILeaderQuick(partitionId))
                    return;
            }
            await Task.Delay(25, ct);
        }
        throw new TimeoutException($"No leader elected for partition {partitionId} within 15 s.");
    }

    private static async Task WaitForCondition(Func<bool> cond, CancellationToken ct, int timeoutMs = 10_000)
    {
        ValueStopwatch sw = ValueStopwatch.StartNew();
        while (sw.GetElapsedMilliseconds() < timeoutMs)
        {
            ct.ThrowIfCancellationRequested();
            if (cond()) return;
            await Task.Delay(25, ct);
        }
        throw new TimeoutException("Condition not satisfied within timeout.");
    }

    // ── G1: Single-voter short-circuit ───────────────────────────────────────

    /// <summary>
    /// A single-node (embedded/standalone) cluster must complete <c>LeaveCluster</c> in
    /// well under 1 s. Before the fix this would spin for the full 10 s deadline because
    /// there is no quorum peer to commit the removal.
    /// </summary>
    [Fact]
    public async Task SingleVoterCluster_LeaveCluster_ReturnsImmediately()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        InMemoryCommunication comm = new();
        RaftManager node = MakeNode(comm, "localhost", 9200, 1, [], _logger);
        comm.SetNodes(new Dictionary<string, IRaft> { ["localhost:9200"] = node });

        await node.UpdateNodes();
        await node.JoinCluster(ct);

        // Wait until the node has a committed roster (MembershipVersion > 0) so the
        // graceful-leave guard `roster.MembershipVersion > 0 && clusterHandler.Joined`
        // would normally be true.
        await WaitForCondition(
            () => node.SystemCoordinator.GetMembership().MembershipVersion > 0, ct);

        // Verify the local node is the only voter.
        ClusterMembership roster = node.SystemCoordinator.GetMembership();
        int voterCount = roster.Members.Count(m => m.Role == ClusterMemberRole.Voter);
        Assert.Equal(1, voterCount);

        // LeaveCluster must complete in << 1 s (we allow 2 s as a generous upper bound
        // to account for CI variability; the fix returns in ~0 ms).
        ValueStopwatch sw = ValueStopwatch.StartNew();
        await node.LeaveCluster(dispose: true, cancellationToken: ct)
                  .WaitAsync(TimeSpan.FromSeconds(2), ct);

        long elapsedMs = sw.GetElapsedMilliseconds();
        Assert.True(elapsedMs < 1_000,
            $"LeaveCluster on a single-voter cluster took {elapsedMs} ms (expected << 1 s).");
    }

    // ── G2: Cancellation ─────────────────────────────────────────────────────

    /// <summary>
    /// Cancelling the token passed to <c>LeaveCluster</c> while the graceful-leave
    /// loop is spinning must cause the method to return promptly instead of waiting
    /// up to <c>deadlineMs</c>.
    ///
    /// We simulate this by passing an already-cancelled token to a 3-node cluster
    /// whose multi-node graceful path would otherwise be invoked.
    /// </summary>
    [Fact]
    public async Task CancelledToken_LeaveCluster_ReturnsPromptly()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        InMemoryCommunication comm = new();
        RaftManager n1 = MakeNode(comm, "localhost", 9210, 1, ["localhost:9211", "localhost:9212"], _logger);
        RaftManager n2 = MakeNode(comm, "localhost", 9211, 2, ["localhost:9210", "localhost:9212"], _logger);
        RaftManager n3 = MakeNode(comm, "localhost", 9212, 3, ["localhost:9210", "localhost:9211"], _logger);

        comm.SetNodes(new Dictionary<string, IRaft>
        {
            ["localhost:9210"] = n1,
            ["localhost:9211"] = n2,
            ["localhost:9212"] = n3,
        });

        await n1.UpdateNodes();
        await n2.UpdateNodes();
        await n3.UpdateNodes();

        await Task.WhenAll(n1.JoinCluster(ct), n2.JoinCluster(ct), n3.JoinCluster(ct));
        await WaitForLeader([n1, n2, n3], partitionId: 1, ct);

        await WaitForCondition(
            () => n1.SystemCoordinator.GetMembership().MembershipVersion > 0 &&
                  n2.SystemCoordinator.GetMembership().MembershipVersion > 0 &&
                  n3.SystemCoordinator.GetMembership().MembershipVersion > 0, ct);

        // Pre-cancel the token so CommitGracefulLeaveAsync observes it immediately.
        using CancellationTokenSource cts = new();
        cts.Cancel();

        // Must return very quickly; allow 2 s for CI overhead.
        ValueStopwatch sw = ValueStopwatch.StartNew();
        await n1.LeaveCluster(dispose: false, cancellationToken: cts.Token)
                .WaitAsync(TimeSpan.FromSeconds(2), ct);

        long elapsedMs = sw.GetElapsedMilliseconds();
        Assert.True(elapsedMs < 1_000,
            $"LeaveCluster with cancelled token took {elapsedMs} ms (expected << 1 s).");

        // Clean up remaining nodes.
        await n3.LeaveCluster(dispose: true, cancellationToken: ct);
        await n2.LeaveCluster(dispose: true, cancellationToken: ct);
        n1.Dispose();
    }

    // ── G3: Multi-node graceful leave is unchanged ────────────────────────────

    /// <summary>
    /// On a 3-node cluster the graceful leave must commit and surviving nodes must
    /// eventually drop the leaver from their committed roster.
    /// </summary>
    [Fact]
    public async Task MultiNodeCluster_LeaveCluster_CommitsAndPropagates()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        InMemoryCommunication comm = new();
        RaftManager n1 = MakeNode(comm, "localhost", 9220, 1, ["localhost:9221", "localhost:9222"], _logger);
        RaftManager n2 = MakeNode(comm, "localhost", 9221, 2, ["localhost:9220", "localhost:9222"], _logger);
        RaftManager n3 = MakeNode(comm, "localhost", 9222, 3, ["localhost:9220", "localhost:9221"], _logger);

        comm.SetNodes(new Dictionary<string, IRaft>
        {
            ["localhost:9220"] = n1,
            ["localhost:9221"] = n2,
            ["localhost:9222"] = n3,
        });

        await n1.UpdateNodes();
        await n2.UpdateNodes();
        await n3.UpdateNodes();

        await Task.WhenAll(n1.JoinCluster(ct), n2.JoinCluster(ct), n3.JoinCluster(ct));
        await WaitForLeader([n1, n2, n3], partitionId: 1, ct);

        await WaitForCondition(
            () => n1.SystemCoordinator.GetMembership().MembershipVersion > 0 &&
                  n2.SystemCoordinator.GetMembership().MembershipVersion > 0 &&
                  n3.SystemCoordinator.GetMembership().MembershipVersion > 0, ct);

        string n3Endpoint = n3.LocalEndpoint;

        // n3 leaves gracefully.
        await n3.LeaveCluster(dispose: true, cancellationToken: ct);

        // Surviving nodes must eventually drop n3 from their committed roster.
        await WaitForCondition(
            () => !n1.SystemCoordinator.GetMembership().Members.Any(m => m.Endpoint == n3Endpoint) &&
                  !n2.SystemCoordinator.GetMembership().Members.Any(m => m.Endpoint == n3Endpoint),
            ct, timeoutMs: 15_000);

        ClusterMembership n1Roster = n1.SystemCoordinator.GetMembership();
        Assert.DoesNotContain(n1Roster.Members, m => m.Endpoint == n3Endpoint);

        ClusterMembership n2Roster = n2.SystemCoordinator.GetMembership();
        Assert.DoesNotContain(n2Roster.Members, m => m.Endpoint == n3Endpoint);

        await n2.LeaveCluster(dispose: true, cancellationToken: ct);
        await n1.LeaveCluster(dispose: true, cancellationToken: ct);
    }
}
