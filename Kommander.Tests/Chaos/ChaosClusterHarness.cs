
using System.Text;
using Kommander;
using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Diagnostics;
using Kommander.Discovery;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.Chaos;

/// <summary>Options for <see cref="ChaosClusterHarness.BuildAsync"/>.</summary>
public sealed record ChaosClusterOptions
{
    public int BasePort { get; init; } = 8600;
    public TimeSpan InvariantPollInterval { get; init; } = TimeSpan.FromMilliseconds(100);
    public string Scenario { get; init; } = "unnamed";

    /// <summary>
    /// Optional per-node configuration hook, applied after the harness defaults. A scenario uses
    /// this to opt into production-shaped settings the fixed defaults turn off (for example
    /// quiescence), without forking the harness.
    /// </summary>
    public Action<RaftConfiguration>? ConfigureNode { get; init; }
}

/// <summary>
/// Real-cluster chaos harness: builds N <see cref="RaftManager"/>s over a shared
/// <see cref="NemesisCommunication"/> decorator, then attaches a <see cref="ChaosSafetyMonitor"/> that wires
/// the safety oracles — a <see cref="HashChainStateMachine"/> per (node, user partition), the commit-ack
/// subscription, and a background <see cref="ClusterInvariantChecker"/> continuously sampling an immutable
/// <see cref="ClusterView"/>. The harness owns node/nemesis lifecycle and the writer helpers; the monitor owns
/// the sampling/checker/report machinery (shared with the fixed scenarios). Disposal stops the monitor's
/// checker first, then heals/drops nemesis state, disposes the nodes, and reports any leaked held messages.
///
/// <para><b>Live observability.</b> Partition views come from the executor thread (never a torn field
/// read); entries "observed committed" are derived at sample time from a voter-majority of nodes agreeing on
/// the applied digest at an index. Per-commit voter acknowledgements populate
/// <see cref="ClusterView.CommitAcks"/> from each node's <see cref="RaftManager.OnCommitAcksObserved"/> hook,
/// so the quorum-discipline invariant evaluates against real acks.</para>
/// </summary>
public sealed class ChaosClusterHarness : IAsyncDisposable
{
    private readonly ChaosClusterOptions _options;
    private readonly int _seed;
    private readonly List<RaftManager> _nodes = [];
    private ChaosSafetyMonitor _monitor = null!;
    private int _writerSeq;
    private bool _disposed;

    private ChaosClusterHarness(int seed, ChaosClusterOptions options)
    {
        _seed = seed;
        _options = options;
    }

    public NemesisCommunication Nemesis { get; private set; } = null!;
    public IReadOnlyList<int> UserPartitions { get; private set; } = [];
    public IReadOnlyList<RaftManager> Nodes => _nodes;

    /// <summary>The continuous safety-invariant checker (owned by the attached <see cref="ChaosSafetyMonitor"/>).</summary>
    public ClusterInvariantChecker Checker => _monitor.Checker;

    /// <summary>The hash-chain observer attached to a specific (node endpoint, partition) pair.</summary>
    public HashChainStateMachine ChainFor(string endpoint, int partition) => _monitor.ChainFor(endpoint, partition);

    /// <summary>Every hash-chain observer for a partition, one per node.</summary>
    public IEnumerable<HashChainStateMachine> ChainsFor(int partition) => _monitor.ChainsFor(partition);

    /// <summary>
    /// Builds and starts the cluster. Returns only after endpoints/discovery are registered, all nodes share
    /// the nemesis decorator, a hash chain is attached per user partition, the cluster has joined and user
    /// partitions exist, and background invariant evaluation has started.
    /// </summary>
    public static async Task<ChaosClusterHarness> BuildAsync(
        int nodeCount, int userPartitionCount, int seed, ChaosClusterOptions? options = null, CancellationToken ct = default)
    {
        ChaosClusterHarness h = new(seed, options ?? new ChaosClusterOptions());
        await h.InitializeAsync(nodeCount, userPartitionCount, ct).ConfigureAwait(false);
        return h;
    }

    private async Task InitializeAsync(int nodeCount, int userPartitionCount, CancellationToken ct)
    {
        InMemoryCommunication inner = new();
        Nemesis = new NemesisCommunication(inner, _seed);

        string[] endpoints = Enumerable.Range(0, nodeCount)
            .Select(i => $"localhost:{_options.BasePort + i}").ToArray();

        for (int i = 0; i < nodeCount; i++)
        {
            string[] peers = endpoints.Where((_, idx) => idx != i).ToArray();
            _nodes.Add(BuildNode(_options.BasePort + i, i + 1, peers, userPartitionCount));
        }

        Nemesis.SetNodes(_nodes.ToDictionary(n => n.LocalEndpoint, n => (IRaft)n));

        await Task.WhenAll(_nodes.Select(n => n.JoinCluster(ct))).ConfigureAwait(false);
        await WaitAsync(() => _nodes.All(n => n.IsInitialized), 20_000, ct).ConfigureAwait(false);

        // Discover user partitions and attach a hash chain per (node, user partition) BEFORE writing.
        RaftManager any = _nodes[0];
        await WaitAsync(() => any.Partitions.Keys.Count(k => k != 0) >= userPartitionCount, 20_000, ct).ConfigureAwait(false);
        UserPartitions = any.Partitions.Keys.Where(k => k != 0).OrderBy(k => k).ToArray();

        // Attach the safety oracles — a hash-chain observer per (node, user partition), the commit-ack
        // subscription (so the quorum-discipline invariant sees real acks), and the continuous invariant
        // checker wired to the nemesis event stream — over the joined cluster BEFORE any write, so the
        // recorded history is complete. The monitor owns this machinery; the harness owns node/nemesis
        // lifecycle and disposes the monitor first on teardown.
        _monitor = new ChaosSafetyMonitor(_nodes, UserPartitions, Nemesis, _seed, _options.Scenario, _options.InvariantPollInterval);
    }

    private RaftManager BuildNode(int port, int nodeId, string[] peers, int initialPartitions)
    {
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
            StartElectionTimeout = 100,
            EndElectionTimeout = 300,
            // Deterministic per-node election timeouts: repeated runs of a scenario make the same election
            // decisions, so a failure reproduces exactly. Derived per (partition, node) internally.
            ElectionTimeoutSeed = _seed,
            EnableQuiescence = false,
            BackfillThreshold = 0,
            MaxBackfillEntriesPerRound = 128,
        };
        _options.ConfigureNode?.Invoke(cfg);
        return new RaftManager(cfg, new StaticDiscovery(peers.Select(e => new RaftNode(e)).ToList()),
            new InMemoryWAL(NullLogger<IRaft>.Instance), Nemesis, new HybridLogicalClock(),
            Environment.GetEnvironmentVariable("CHAOS_DIAG_LOG") is not null ? new TempFileLogger<IRaft>() : NullLogger<IRaft>.Instance);
    }

    // ── driving ─────────────────────────────────────────────────────────────────────

    /// <summary>Finds the current leader for a partition within a bounded time, or throws.</summary>
    public async Task<RaftManager> FindLeaderAsync(int partition, CancellationToken ct = default)
    {
        RaftManager? leader = null;
        await WaitAsync(async () =>
        {
            foreach (RaftManager n in _nodes)
                if (await n.AmILeaderQuick(partition).ConfigureAwait(false)) { leader = n; return true; }
            return false;
        }, 15_000, ct).ConfigureAwait(false);
        return leader!;
    }

    /// <summary>
    /// Writes a uniquely-identified <c>(writer, sequence)</c> payload through the partition leader and, on a
    /// successful commit, records it as expected history. Returns the committed log index, or -1 on failure.
    /// </summary>
    public async Task<long> WriteAsync(int partition, CancellationToken ct = default)
    {
        int seq = Interlocked.Increment(ref _writerSeq);
        byte[] payload = Encoding.UTF8.GetBytes($"w{_seed}:{seq}");
        RaftManager leader = await FindLeaderAsync(partition, ct).ConfigureAwait(false);
        RaftReplicationResult r = await leader.ReplicateLogs(partition, "chaos", payload, cancellationToken: ct).ConfigureAwait(false);
        if (r.Status != RaftOperationStatus.Success)
            return -1;
        return r.LogIndex;
    }

    /// <summary>
    /// Submits a uniquely-identified write through a <b>specific</b> node (not the discovered leader). Used by
    /// scenarios that deliberately write to an isolated/minority node and assert the write does not commit.
    /// Returns the raw replication result so the caller can inspect <see cref="RaftOperationStatus"/>.
    /// </summary>
    public async Task<RaftReplicationResult> WriteViaAsync(RaftManager node, int partition, CancellationToken ct = default)
    {
        int seq = Interlocked.Increment(ref _writerSeq);
        byte[] payload = Encoding.UTF8.GetBytes($"w{_seed}:{seq}");
        return await node.ReplicateLogs(partition, "chaos", payload, cancellationToken: ct).ConfigureAwait(false);
    }

    /// <summary>
    /// A liveness oracle for a single stable leader: waits until exactly one node reports leadership for the
    /// partition and that view stays single across a short confirmation window, then returns that leader.
    /// Only meaningful after all withholding rules are healed and held messages released/dropped.
    /// </summary>
    public async Task<RaftManager> WaitForSingleLeaderAsync(int partition, CancellationToken ct = default)
    {
        RaftManager? leader = null;
        await WaitAsync(async () =>
        {
            RaftManager? found = null;
            int count = 0;
            foreach (RaftManager n in _nodes)
                if (await n.AmILeaderQuick(partition).ConfigureAwait(false)) { found = n; count++; }
            if (count != 1) return false;
            // Confirmation window: the same single leader must still hold leadership shortly after. A
            // transiently-stale peer view is tolerated here (two-leader safety is caught continuously by the
            // invariant checker); this oracle only needs one stable leader to make progress against.
            leader = found;
            await Task.Delay(200, ct).ConfigureAwait(false);
            return await found!.AmILeaderQuick(partition).ConfigureAwait(false);
        }, 20_000, ct).ConfigureAwait(false);
        return leader!;
    }

    public async Task WaitForAppliedIndexAsync(int partition, long index, CancellationToken ct = default) =>
        await WaitAsync(() => _monitor.ChainsFor(partition)
            .All(c => c.Snapshot().LastAppliedIndex >= index), 15_000, ct).ConfigureAwait(false);

    public async Task WaitForConvergenceAsync(int partition, long index, CancellationToken ct = default)
    {
        await WaitForAppliedIndexAsync(partition, index, ct).ConfigureAwait(false);
        HashChainAssert.NoDivergence(_monitor.ChainsFor(partition), partition, _seed);
        HashChainAssert.ConvergedToIndex(_monitor.ChainsFor(partition), partition, index, _seed);
    }

    // ── sampling / failure report (delegated to the attached monitor) ────────────────

    /// <summary>Builds an immutable point-in-time cluster view. Never throws (transient sampling errors are swallowed).</summary>
    public Task<ClusterView> SampleAsync(CancellationToken ct = default) => _monitor.SampleAsync(ct);

    /// <summary>Builds the standard failure report (seed, nemesis event tail, per-node views).</summary>
    public Task<string> BuildFailureReportAsync(string violated, CancellationToken ct = default) =>
        _monitor.BuildFailureReportAsync(violated, ct);

    // ── helpers ──────────────────────────────────────────────────────────────────────

    private static async Task WaitAsync(Func<bool> cond, int timeoutMs, CancellationToken ct)
    {
        ValueStopwatch sw = ValueStopwatch.StartNew();
        while (sw.GetElapsedMilliseconds() < timeoutMs)
        {
            ct.ThrowIfCancellationRequested();
            if (cond()) return;
            await Task.Delay(25, ct).ConfigureAwait(false);
        }
        throw new TimeoutException($"Condition not met within {timeoutMs} ms.");
    }

    private static async Task WaitAsync(Func<Task<bool>> cond, int timeoutMs, CancellationToken ct)
    {
        ValueStopwatch sw = ValueStopwatch.StartNew();
        while (sw.GetElapsedMilliseconds() < timeoutMs)
        {
            ct.ThrowIfCancellationRequested();
            if (await cond().ConfigureAwait(false)) return;
            await Task.Delay(25, ct).ConfigureAwait(false);
        }
        throw new TimeoutException($"Condition not met within {timeoutMs} ms.");
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        // 1. Stop invariant polling first so it does not sample disposing nodes.
        if (_monitor is not null)
            await _monitor.DisposeAsync().ConfigureAwait(false);

        // 2. Heal / release nemesis state, and drain any in-flight delayed deliveries so none can land after
        //    disposal (ClearRules signals cancellation; await the drain here so nothing leaks or delivers late).
        Nemesis?.ClearRules();
        Nemesis?.DropAllHeld();
        int delayedCanceled = Nemesis is not null ? await Nemesis.CancelDelayedDeliveriesAsync().ConfigureAwait(false) : 0;

        // 3. Dispose nodes.
        foreach (RaftManager node in _nodes)
        {
            try { node.Dispose(); } catch { /* best effort */ }
        }

        // 4. Report leaked background work.
        if (Nemesis is not null && (Nemesis.HeldCount > 0 || Nemesis.DelayedDeliveryCount > 0))
            throw new InvalidOperationException(
                $"Chaos harness leaked {Nemesis.HeldCount} held + {Nemesis.DelayedDeliveryCount} delayed messages on dispose " +
                $"(canceled {delayedCanceled} delayed during teardown).");
    }
}
