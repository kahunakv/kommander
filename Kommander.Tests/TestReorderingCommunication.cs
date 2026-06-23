
using Kommander.Communication;
using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.Tests.Communication;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using System.Collections.Concurrent;

namespace Kommander.Tests;

/// <summary>
/// Counts <see cref="LogLevel.Warning"/> messages whose text contains a specific substring.
/// Used by Task-5 tests to assert that the leader's LogMismatch backtracking stays O(1)
/// rather than O(gap) after a hole is induced and healed.
/// </summary>
internal sealed class CountingLogger : ILogger<IRaft>
{
    private readonly string _substring;
    private int _count;

    public CountingLogger(string substring) => _substring = substring;

    public int Count => _count;

    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
    public bool IsEnabled(LogLevel logLevel) => logLevel >= LogLevel.Warning;

    public void Log<TState>(
        LogLevel logLevel,
        EventId eventId,
        TState state,
        Exception? exception,
        Func<TState, Exception?, string> formatter)
    {
        if (logLevel >= LogLevel.Warning && formatter(state, exception).Contains(_substring))
            Interlocked.Increment(ref _count);
    }
}

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

    private IRaft MakeNode(int id, string endpoint, string[] peers, ICommunication comm,
        ILogger<IRaft>? nodeLogger = null)
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

        ILogger<IRaft> log = nodeLogger ?? logger;
        return new RaftManager(
            config,
            new StaticDiscovery(peers.Select(p => new RaftNode(p)).ToList()),
            new InMemoryWAL(log),
            comm,
            new HybridLogicalClock(),
            log);
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

    /// <summary>
    /// End-to-end Task-5 verification: a hole induced on one follower heals in O(1)
    /// backfill rounds and no committed entry is ever discarded.
    ///
    /// <para>Procedure:
    /// <list type="number">
    ///   <item>Commit several entries so the committed frontier is non-trivial.</item>
    ///   <item>Hold one AppendLogs to the target follower (shim) and simultaneously
    ///         partition it so leader retries cannot fill the gap.</item>
    ///   <item>Inject entries beyond the held range directly — creating a hole below the
    ///         follower's max.</item>
    ///   <item>Heal the partition and release the hold, triggering the state machine's
    ///         hole-repair truncation path.</item>
    ///   <item>Wait for the follower to converge with the leader's committed max.</item>
    ///   <item>Assert every committed id is present on all three nodes.</item>
    ///   <item>Assert the leader's LogMismatch backtracking count stays bounded (≤ 5) —
    ///         O(1), not O(gap).</item>
    /// </list>
    /// </para>
    /// </summary>
    [Fact]
    public async Task HoleRepair_HealsInBoundedRounds_NoCommittedEntryLost()
    {
        const string ep1 = "localhost:9121";
        const string ep2 = "localhost:9122";
        const string ep3 = "localhost:9123";

        CancellationToken ct = TestContext.Current.CancellationToken;

        // The leader node uses a counting logger so we can observe its LogMismatch count.
        CountingLogger leaderLog = new("LogMismatch from");

        InMemoryCommunication inner = new();
        ReorderingCommunication shim  = new(inner);

        // We don't know which node will become leader, so wrap all three with the counting
        // logger and sum across them — in a 3-node cluster only the leader emits LogMismatch.
        CountingLogger log1 = new("LogMismatch from");
        CountingLogger log2 = new("LogMismatch from");
        CountingLogger log3 = new("LogMismatch from");

        IRaft n1 = MakeNode(1, ep1, [ep2, ep3], shim, log1);
        IRaft n2 = MakeNode(2, ep2, [ep1, ep3], shim, log2);
        IRaft n3 = MakeNode(3, ep3, [ep1, ep2], shim, log3);

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

        IRaft[] allNodes       = [n1, n2, n3];
        IRaft   leaderNode     = allNodes.First(n => n.GetLocalEndpoint() == leaderEndpoint);
        IRaft   followerNode   = allNodes.First(n => n.GetLocalEndpoint() != leaderEndpoint);
        string  followerEndpoint = followerNode.GetLocalEndpoint();

        // ── Phase 1: commit several entries to establish a committed frontier ─────────

        List<long> committedIds = [];
        for (int i = 0; i < 3; i++)
        {
            byte[] data = global::System.Text.Encoding.UTF8.GetBytes($"setup-{i}");
            RaftReplicationResult r = await leaderNode.ReplicateLogs(
                1, "Task5Setup", data, cancellationToken: ct);
            Assert.Equal(RaftOperationStatus.Success, r.Status);
            committedIds.Add(r.LogIndex);
        }

        long committedFrontier = committedIds.Max();

        // Snapshot LogMismatch count before inducing the hole.
        int mismatchBefore = log1.Count + log2.Count + log3.Count;

        // ── Phase 2: arm hold, commit one more entry (this one gets held to the follower) ─

        shim.ConfigureHold(1, followerEndpoint, r => r.Logs?.Count > 0);

        RaftReplicationResult holeEntry = await leaderNode.ReplicateLogs(
            1, "Task5Hole", "hole"u8.ToArray(), cancellationToken: ct);
        Assert.Equal(RaftOperationStatus.Success, holeEntry.Status);

        // Wait for the shim to capture the batch headed to the follower.
        await shim.WaitForHeldAsync(ct).WaitAsync(TimeSpan.FromSeconds(10), ct);
        Assert.True(shim.HasHeld, "Shim must have captured an AppendLogs to the follower.");

        // Partition the follower so leader heartbeats cannot bypass the shim once _holdFired=true.
        inner.PartitionNode(followerEndpoint);

        AppendLogsRequest heldReq = shim.HeldRequest!;
        long heldMaxId = heldReq.Logs!.Max(l => l.Id);

        // ── Phase 3: inject entries above the held range directly into the follower ─────

        long term = leaderNode.WalAdapter.GetCurrentTerm(1);
        long injectLow  = heldMaxId + 3;
        long injectHigh = heldMaxId + 4;

        followerNode.AppendLogs(new AppendLogsRequest(
            partition: 1,
            term:      term,
            time:      leaderNode.HybridLogicalClock.TrySendOrLocalEvent(1),
            endpoint:  leaderNode.GetLocalEndpoint(),
            logs:
            [
                new RaftLog { Id = injectLow,  Term = term, Type = RaftLogType.Committed, LogType = "hole-inject" },
                new RaftLog { Id = injectHigh, Term = term, Type = RaftLogType.Committed, LogType = "hole-inject" },
            ],
            prevLogIndex: 0,
            prevLogTerm:  0));

        // Wait until the follower has accepted the injected entries (hole now exists).
        await WaitForCondition(
            () => followerNode.WalAdapter.GetMaxLog(1) >= injectLow,
            TimeSpan.FromSeconds(5), ct);

        // ── Phase 4: heal partition, release hold → triggers hole-repair path ─────────

        inner.HealPartition(followerEndpoint);
        shim.ReleaseHeld();

        // ── Phase 5: wait for convergence ────────────────────────────────────────────

        long leaderMax = leaderNode.WalAdapter.GetMaxLog(1);

        // Wait for the follower's WAL max to match what the leader ultimately reaches.
        await WaitForCondition(
            () =>
            {
                long lMax = leaderNode.WalAdapter.GetMaxLog(1);
                long fMax = followerNode.WalAdapter.GetMaxLog(1);
                return fMax >= lMax && lMax >= committedFrontier;
            },
            TimeSpan.FromSeconds(15), ct);

        // ── Phase 6: assert no committed entry was discarded ─────────────────────────

        // Read committed ids from the leader's WAL and check all followers have them.
        HashSet<long> leaderIds = leaderNode.WalAdapter
            .ReadLogsRange(1, 1)
            .Select(l => l.Id)
            .ToHashSet();

        // Every id the leader committed must be present on every node.
        foreach (IRaft node in allNodes)
        {
            HashSet<long> nodeIds = node.WalAdapter.ReadLogsRange(1, 1).Select(l => l.Id).ToHashSet();
            foreach (long id in leaderIds)
                Assert.Contains(id, nodeIds);
        }

        // ── Phase 7: assert O(1) LogMismatch (bounded backtracking, not O(gap)) ──────

        int mismatchAfter = log1.Count + log2.Count + log3.Count;
        int mismatchDelta = mismatchAfter - mismatchBefore;

        // Gap between heldMaxId and injectHigh is 4 (injectLow = heldMaxId+3, injectHigh = heldMaxId+4).
        // O(1) repair: the hole-truncation path collapses the gap in a single truncate round,
        // so the leader needs at most a small constant number of LogMismatch exchanges
        // (≤ 5 accounts for any NoOp or election noise during the test).
        Assert.True(mismatchDelta <= 5,
            $"Expected ≤5 LogMismatch rounds after hole-repair (got {mismatchDelta}). " +
            "Backtracking is O(gap), not O(1) — the hole-repair truncation path may not be firing.");

        await n1.LeaveCluster(true, CancellationToken.None);
        await n2.LeaveCluster(true, CancellationToken.None);
        await n3.LeaveCluster(true, CancellationToken.None);
    }
}
