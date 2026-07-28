
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
}

/// <summary>
/// Real-cluster chaos harness: builds N <see cref="RaftManager"/>s over a shared
/// <see cref="NemesisCommunication"/> decorator, attaches a <see cref="HashChainStateMachine"/> to each
/// (node, user partition), and runs a background <see cref="ClusterInvariantChecker"/> that continuously
/// samples an immutable <see cref="ClusterView"/> and evaluates the safety invariants. It owns all
/// background tasks and a cancellation token; disposal cancels the checker, heals/drops nemesis state,
/// disposes the nodes, and reports any leaked held messages.
///
/// <para><b>Live observability.</b> Partition views come from the executor thread (never a torn field
/// read); entries "observed committed" are derived at sample time from a voter-majority of nodes agreeing
/// on the applied digest at an index. Per-commit voter acknowledgements (for the quorum-discipline
/// invariant) are not observable from outside the cluster without deeper instrumentation, so
/// <see cref="ClusterView.CommitAcks"/> is left empty in the live harness; that invariant is covered by its
/// synthetic unit test.</para>
/// </summary>
public sealed class ChaosClusterHarness : IAsyncDisposable
{
    private readonly ChaosClusterOptions _options;
    private readonly int _seed;
    private readonly List<RaftManager> _nodes = [];
    private readonly Dictionary<(string Endpoint, int Partition), HashChainStateMachine> _chains = new();
    private readonly List<CommitObservation> _recordedCommits = [];
    private readonly Dictionary<(int Partition, long Index, string Acker), CommitAck> _recordedCommitAcks = new();
    private readonly object _recordLock = new();
    private long _sampleSeq;
    private int _writerSeq;
    private bool _disposed;

    private ChaosClusterHarness(int seed, ChaosClusterOptions options)
    {
        _seed = seed;
        _options = options;
    }

    public NemesisCommunication Nemesis { get; private set; } = null!;
    public ClusterInvariantChecker Checker { get; private set; } = null!;
    public IReadOnlyList<int> UserPartitions { get; private set; } = [];
    public IReadOnlyList<RaftManager> Nodes => _nodes;

    /// <summary>The hash-chain observer attached to a specific (node endpoint, partition) pair.</summary>
    public HashChainStateMachine ChainFor(string endpoint, int partition) => _chains[(endpoint, partition)];

    /// <summary>Every hash-chain observer for a partition, one per node.</summary>
    public IEnumerable<HashChainStateMachine> ChainsFor(int partition) =>
        _chains.Where(kv => kv.Key.Partition == partition).Select(kv => kv.Value);

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

        foreach (RaftManager node in _nodes)
        foreach (int partition in UserPartitions)
        {
            HashChainStateMachine chain = new(node.LocalEndpoint, partition);
            _chains[(node.LocalEndpoint, partition)] = chain;
            node.OnReplicationReceived += chain.OnReplicationReceived;
        }

        // Subscribe to each node's commit-quorum acknowledgements so the live quorum-discipline invariant sees
        // real facts (who acked each commit, their voter role, the voter count) rather than an empty set.
        foreach (RaftManager node in _nodes)
            node.OnCommitAcksObserved += RecordCommitAcks;

        Checker = new ClusterInvariantChecker(SampleAsync, ClusterInvariants.All, _options.InvariantPollInterval);
        Nemesis.OnEvent = Checker.Notify;
        Checker.Start();
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
        return new RaftManager(cfg, new StaticDiscovery(peers.Select(e => new RaftNode(e)).ToList()),
            new InMemoryWAL(NullLogger<IRaft>.Instance), Nemesis, new HybridLogicalClock(), NullLogger<IRaft>.Instance);
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
        await WaitAsync(() => _chains.Where(kv => kv.Key.Partition == partition)
            .All(kv => kv.Value.Snapshot().LastAppliedIndex >= index), 15_000, ct).ConfigureAwait(false);

    public async Task WaitForConvergenceAsync(int partition, long index, CancellationToken ct = default)
    {
        await WaitForAppliedIndexAsync(partition, index, ct).ConfigureAwait(false);
        HashChainAssert.NoDivergence(_chains.Where(kv => kv.Key.Partition == partition).Select(kv => kv.Value), partition, _seed);
        HashChainAssert.ConvergedToIndex(_chains.Where(kv => kv.Key.Partition == partition).Select(kv => kv.Value), partition, index, _seed);
    }

    // ── sampling ────────────────────────────────────────────────────────────────────

    /// <summary>Builds an immutable point-in-time cluster view. Never throws (transient sampling errors are swallowed).</summary>
    public async Task<ClusterView> SampleAsync(CancellationToken ct = default)
    {
        long seq = Interlocked.Increment(ref _sampleSeq);

        List<RaftPartitionView> views = [];
        foreach (RaftManager node in _nodes)
        foreach (int partition in UserPartitions)
        {
            try
            {
                RaftPartitionView? v = await node.GetPartitionViewAsync(partition, ct).ConfigureAwait(false);
                if (v is not null) views.Add(v);
            }
            catch { /* node mid-dispose or partition gone */ }
        }

        List<HashChainSnapshot> chains = _chains.Values.Select(c => c.Snapshot()).ToList();
        List<CommitObservation> observed = DeriveObservedCommits(chains);

        // Record newly-observed commits cumulatively so history persists even if a node later diverges.
        List<CommitAck> acks;
        lock (_recordLock)
        {
            HashSet<(int, long)> known = _recordedCommits.Select(c => (c.Partition, c.Index)).ToHashSet();
            foreach (CommitObservation c in observed)
                if (known.Add((c.Partition, c.Index)))
                    _recordedCommits.Add(c);
            observed = [.. _recordedCommits];
            acks = [.. _recordedCommitAcks.Values];
        }

        return new ClusterView(seq, views, chains, observed, acks, new Dictionary<string, long>());
    }

    /// <summary>Records the acknowledgements that carried a commit to quorum (deduplicated per (partition, index, acker)).</summary>
    private void RecordCommitAcks(IReadOnlyList<Kommander.Data.RaftCommitAckObservation> observations)
    {
        lock (_recordLock)
        {
            foreach (Kommander.Data.RaftCommitAckObservation o in observations)
                _recordedCommitAcks[(o.Partition, o.Index, o.Acker)] =
                    new CommitAck(o.Partition, o.Index, o.Acker, o.AckerIsVoter, o.VotersTotal);
        }
    }

    /// <summary>An index is "observed committed" when a majority of nodes agree on its applied digest.</summary>
    private List<CommitObservation> DeriveObservedCommits(IReadOnlyList<HashChainSnapshot> chains)
    {
        List<CommitObservation> result = [];
        int majority = _nodes.Count / 2 + 1;

        foreach (IGrouping<int, HashChainSnapshot> byPartition in chains.GroupBy(c => c.PartitionId))
        {
            IEnumerable<long> allIndexes = byPartition.SelectMany(c => c.MetaByIndex.Keys).Distinct();
            foreach (long index in allIndexes)
            {
                var agree = byPartition
                    .Where(c => c.MetaByIndex.ContainsKey(index))
                    .GroupBy(c => c.MetaByIndex[index].EntryDigest)
                    .OrderByDescending(g => g.Count())
                    .FirstOrDefault();
                if (agree is not null && agree.Count() >= majority)
                {
                    EntryMeta meta = agree.First().MetaByIndex[index];
                    result.Add(new CommitObservation(byPartition.Key, index, meta.Term, meta.EntryDigest));
                }
            }
        }
        return result;
    }

    // ── failure report ────────────────────────────────────────────────────────────────

    public async Task<string> BuildFailureReportAsync(string violated, CancellationToken ct = default)
    {
        ClusterView view = await SampleAsync(ct).ConfigureAwait(false);
        StringBuilder sb = new();
        sb.AppendLine($"=== Chaos failure report ===");
        sb.AppendLine($"scenario={_options.Scenario} seed={_seed} violated={violated}");
        sb.AppendLine($"nemesis events={Nemesis.TotalEventCount} held={Nemesis.HeldCount} delayed={Nemesis.DelayedDeliveryCount}");
        sb.AppendLine("-- last 20 nemesis events --");
        foreach (NemesisEvent e in Nemesis.RecentEvents())
            sb.AppendLine("  " + e);
        sb.AppendLine("-- per-node views --");
        foreach (RaftPartitionView v in view.PartitionViews.OrderBy(v => v.Partition).ThenBy(v => v.Endpoint))
            sb.AppendLine("  " + v);
        return sb.ToString();
    }

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
        if (Checker is not null)
            await Checker.DisposeAsync().ConfigureAwait(false);

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
