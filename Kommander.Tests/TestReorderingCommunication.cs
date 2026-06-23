
using Kommander.Communication;
using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.Tests.Communication;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests;

/// <summary>
/// Smoke-tests for <see cref="ReorderingCommunication"/>.
///
/// Verifies that the shim can intercept exactly one outbound <see cref="AppendLogsRequest"/>
/// and that deferring it produces a genuine log hole on the target follower — i.e.
/// <c>GetMaxLog</c> reports an index above the range covered by the held batch while
/// that batch is still delayed.
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
public sealed class TestReorderingCommunication
{
    private readonly ILogger<IRaft> logger = NullLoggerFactory.Instance.CreateLogger<IRaft>();

    // ── helpers ───────────────────────────────────────────────────────────────

    private IRaft MakeNode(int id, string endpoint, string[] peers, ICommunication comm)
    {
        string host = endpoint.Split(':')[0];
        int port    = int.Parse(endpoint.Split(':')[1]);

        RaftConfiguration config = new()
        {
            NodeId   = id,
            Host     = host,
            Port     = port,
            InitialPartitions    = 1,
            PingInterval         = TimeSpan.Zero,
            HeartbeatInterval    = TimeSpan.FromMilliseconds(50),
            RecentHeartbeat      = TimeSpan.FromMilliseconds(25),
            VotingTimeout        = TimeSpan.FromMilliseconds(250),
            CheckLeaderInterval  = TimeSpan.FromMilliseconds(25),
            UpdateNodesInterval  = TimeSpan.FromMilliseconds(100),
            TimerInitialDelay    = TimeSpan.FromMilliseconds(25),
            StartElectionTimeout = 100,
            EnableQuiescence     = false,
            EndElectionTimeout   = 250,
        };

        return new RaftManager(
            config,
            new StaticDiscovery(peers.Select(p => new RaftNode(p)).ToList()),
            new InMemoryWAL(logger),
            comm,
            new HybridLogicalClock(),
            logger);
    }

    private static async Task WaitForCondition(Func<bool> condition, TimeSpan timeout, CancellationToken ct)
    {
        using CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(timeout);
        while (!cts.Token.IsCancellationRequested)
        {
            if (condition()) return;
            await Task.Delay(20, cts.Token).ConfigureAwait(false);
        }
        Assert.Fail("Condition not met within timeout.");
    }

    // ── tests ─────────────────────────────────────────────────────────────────

    /// <summary>
    /// A held AppendLogs batch leaves the target follower with a log hole:
    /// <c>GetMaxLog</c> is above the held range because a later batch (injected
    /// directly) was accepted first, while the held entries are still missing.
    /// </summary>
    [Fact]
    public async Task Shim_InducesFollowerHole_MaxAboveMissingRange()
    {
        const string ep1 = "localhost:9101";
        const string ep2 = "localhost:9102";
        const string ep3 = "localhost:9103";

        CancellationToken ct = TestContext.Current.CancellationToken;

        InMemoryCommunication inner = new();
        ReorderingCommunication shim = new(inner);

        // 3-node cluster ensures quorum is reachable even when one follower is partitioned.
        IRaft n1 = MakeNode(1, ep1, [ep2, ep3], shim);
        IRaft n2 = MakeNode(2, ep2, [ep1, ep3], shim);
        IRaft n3 = MakeNode(3, ep3, [ep1, ep2], shim);

        inner.SetNodes(new Dictionary<string, IRaft>
        {
            { ep1, n1 }, { ep2, n2 }, { ep3, n3 }
        });

        await Task.WhenAll(n1.UpdateNodes(), n2.UpdateNodes(), n3.UpdateNodes());

        await Task.WhenAll(
            n1.JoinCluster(ct),
            n2.JoinCluster(ct),
            n3.JoinCluster(ct));

        string leaderEndpoint = await n1.WaitForLeaderStableAsync(
            1, TimeSpan.FromMilliseconds(200), ct);

        // Identify leader and a non-leader follower.
        IRaft leaderNode   = new[] { n1, n2, n3 }.First(n => n.GetLocalEndpoint() == leaderEndpoint);
        IRaft followerNode = new[] { n1, n2, n3 }.First(n => n.GetLocalEndpoint() != leaderEndpoint);
        string followerEndpoint = followerNode.GetLocalEndpoint();

        // Configure the hold now that we know which node is the follower.
        // Match any non-empty AppendLogs — this catches both the leadership NoOp entry
        // and any subsequent committed entry.
        shim.ConfigureHold(1, followerEndpoint, r => r.Logs?.Count > 0);

        // Commit one entry through the cluster. In a 3-node cluster the leader can reach
        // quorum via the third node even if the follower never ACKs.
        byte[] data = "hole-test"u8.ToArray();
        Task<RaftReplicationResult> commitTask = leaderNode.ReplicateLogs(
            1, "HoleTest", data, cancellationToken: ct);

        // Wait for the shim to capture the first non-empty AppendLogs to the follower.
        await shim.WaitForHeldAsync(ct).WaitAsync(TimeSpan.FromSeconds(10), ct);

        Assert.True(shim.HasHeld, "Shim must have captured a non-empty AppendLogs to the follower.");

        // Partition the follower immediately so leader retries cannot fill the hole before we can
        // assert. Retries bypass the shim (_holdFired=true) and would go straight through the inner
        // transport without the partition guard. Do this first, before any other work, to keep the
        // hold→partition window as small as possible.
        inner.PartitionNode(followerEndpoint);

        // Assert against the entries that were actually held — not entry 1. By the time the hold is
        // armed (after leadership stabilises) the leadership NoOp may already have replicated to the
        // follower, so the first held non-empty batch is whatever the leader ships next, not
        // necessarily id 1. The held entries are the ones that must be missing to form the hole.
        // HeldRequest remains valid after PartitionNode — it is cleared only by ReleaseHeld.
        AppendLogsRequest heldReq = shim.HeldRequest!;
        long heldMinId = heldReq.Logs!.Min(l => l.Id);
        long heldMaxId = heldReq.Logs!.Max(l => l.Id);

        // Inject entries strictly above the held range so a contiguous gap is guaranteed.
        long injectLow  = heldMaxId + 3;
        long injectHigh = heldMaxId + 4;

        try
        {
            // The commit must succeed — the third node provides quorum.
            RaftReplicationResult commitResult = await commitTask.WaitAsync(TimeSpan.FromSeconds(10), ct);
            Assert.Equal(RaftOperationStatus.Success, commitResult.Status);

            // Inject a later batch directly to the follower, bypassing the shim and
            // the partition (the test calls IRaft.AppendLogs directly, not via transport).
            long term = leaderNode.WalAdapter.GetCurrentTerm(1);
            followerNode.AppendLogs(new AppendLogsRequest(
                partition: 1,
                term: term,
                time: leaderNode.HybridLogicalClock.TrySendOrLocalEvent(1),
                endpoint: leaderNode.GetLocalEndpoint(),
                logs:
                [
                    new RaftLog { Id = injectLow,  Term = term, Type = RaftLogType.Committed, LogType = "hole-inject" },
                    new RaftLog { Id = injectHigh, Term = term, Type = RaftLogType.Committed, LogType = "hole-inject" }
                ],
                prevLogIndex: 0,
                prevLogTerm: 0));

            await WaitForCondition(
                () => followerNode.WalAdapter.GetMaxLog(1) >= injectLow,
                TimeSpan.FromSeconds(5),
                ct);

            long maxAfter = followerNode.WalAdapter.GetMaxLog(1);
            Assert.True(maxAfter >= injectLow,
                $"Follower must have accepted the injected entries (got max={maxAfter})");

            // The held entries must be absent — they are still delayed in the shim — while the
            // injected higher ids are present. That non-contiguity is the hole.
            HashSet<long> ids = followerNode.WalAdapter.ReadLogsRange(1, 1).Select(l => l.Id).ToHashSet();
            Assert.Contains(injectLow, ids);
            for (long id = heldMinId; id <= heldMaxId; id++)
                Assert.DoesNotContain(id, ids);
        }
        finally
        {
            inner.HealPartition(followerEndpoint);
            shim.ReleaseHeld();

            await n1.LeaveCluster(true, CancellationToken.None);
            await n2.LeaveCluster(true, CancellationToken.None);
            await n3.LeaveCluster(true, CancellationToken.None);
        }
    }

    /// <summary>
    /// The shim captures the first matching AppendLogs and then passes all subsequent
    /// messages through unchanged.  After <see cref="ReorderingCommunication.ReleaseHeld"/>
    /// the cluster can resume normal operation.
    /// </summary>
    [Fact]
    public async Task Shim_CapturesOneMessage_ThenPassesThrough()
    {
        const string ep1 = "localhost:9111";
        const string ep2 = "localhost:9112";

        CancellationToken ct = TestContext.Current.CancellationToken;

        InMemoryCommunication inner = new();
        ReorderingCommunication shim = new(inner);

        IRaft n1 = MakeNode(1, ep1, [ep2], shim);
        IRaft n2 = MakeNode(2, ep2, [ep1], shim);

        inner.SetNodes(new Dictionary<string, IRaft>
        {
            { ep1, n1 }, { ep2, n2 }
        });

        await Task.WhenAll(n1.UpdateNodes(), n2.UpdateNodes());

        await Task.WhenAll(
            n1.JoinCluster(ct),
            n2.JoinCluster(ct));

        string leaderEndpoint = await n1.WaitForLeaderStableAsync(
            1, TimeSpan.FromMilliseconds(200), ct);

        // Configure the hold for the follower (not the leader) so messages will actually flow there.
        string followerEndpoint = leaderEndpoint == ep1 ? ep2 : ep1;

        // Match any AppendLogs — even empty heartbeats count.
        shim.ConfigureHold(1, followerEndpoint);

        // Wait for the first AppendLogs to the follower to be captured.
        await shim.WaitForHeldAsync(ct).WaitAsync(TimeSpan.FromSeconds(10), ct);

        Assert.True(shim.HasHeld, "Shim must have captured an AppendLogs to the follower.");

        shim.ReleaseHeld();

        // Hold must be cleared after release.
        Assert.False(shim.HasHeld);

        await n1.LeaveCluster(true, CancellationToken.None);
        await n2.LeaveCluster(true, CancellationToken.None);
    }
}
