
using System.Text;
using Kommander;
using Kommander.Data;

namespace Kommander.Tests.Chaos;

/// <summary>
/// Attaches the chaos safety oracles to an already-built set of <see cref="RaftManager"/> nodes: a
/// <see cref="HashChainStateMachine"/> per (node, partition) that records each node's applied history, a
/// background <see cref="ClusterInvariantChecker"/> that continuously samples an immutable
/// <see cref="ClusterView"/> and evaluates the safety invariants during fault activity, and the commit-ack
/// subscription that lets the quorum-discipline invariant see real facts. It also builds the standard
/// failure report.
///
/// <para>Unlike <see cref="ChaosClusterHarness"/>, this monitor does NOT build nodes, own the nemesis
/// lifecycle, or heal/dispose the cluster — the caller owns all of that. It exists so the fixed scenarios,
/// which construct specialised clusters directly (learner joins below the compaction floor, a deposed-leader
/// snapshot, custom WALs and promotion tuning), can still evaluate safety continuously and emit the standard
/// report on failure, without duplicating the sampling logic in each scenario. Attach it AFTER the cluster
/// has joined but BEFORE the first write, so every applied entry is observed.</para>
///
/// <para>Disposal stops the checker only; the caller disposes the nodes and nemesis. It is intentionally
/// idempotent so a scenario can dispose it in a <c>finally</c> after also asserting on it.</para>
/// </summary>
public sealed class ChaosSafetyMonitor : IAsyncDisposable
{
    private readonly IReadOnlyList<RaftManager> _nodes;
    private readonly IReadOnlyList<int> _partitions;
    private readonly NemesisCommunication _nemesis;
    private readonly int _seed;
    private readonly string _scenario;
    private readonly Dictionary<(string Endpoint, int Partition), HashChainStateMachine> _chains = new();
    private readonly List<CommitObservation> _recordedCommits = [];
    private readonly Dictionary<(int Partition, long Index, string Acker), CommitAck> _recordedCommitAcks = new();
    private readonly object _recordLock = new();
    private long _sampleSeq;
    private bool _disposed;

    public ClusterInvariantChecker Checker { get; }

    /// <summary>
    /// Wires the oracles over <paramref name="nodes"/>/<paramref name="partitions"/> and starts continuous
    /// evaluation. Hash chains are subscribed to <see cref="RaftManager.OnReplicationReceived"/> and the
    /// commit-ack recorder to <see cref="RaftManager.OnCommitAcksObserved"/> immediately, so construct this
    /// before any replication so the recorded history is complete.
    /// </summary>
    public ChaosSafetyMonitor(
        IReadOnlyList<RaftManager> nodes,
        IReadOnlyList<int> partitions,
        NemesisCommunication nemesis,
        int seed,
        string scenario,
        TimeSpan? pollInterval = null,
        IReadOnlyList<IClusterInvariant>? invariants = null)
    {
        _nodes = nodes;
        _partitions = partitions;
        _nemesis = nemesis;
        _seed = seed;
        _scenario = scenario;

        foreach (RaftManager node in _nodes)
        foreach (int partition in _partitions)
        {
            HashChainStateMachine chain = new(node.LocalEndpoint, partition);
            _chains[(node.LocalEndpoint, partition)] = chain;
            node.OnReplicationReceived += chain.OnReplicationReceived;
        }

        foreach (RaftManager node in _nodes)
            node.OnCommitAcksObserved += RecordCommitAcks;

        Checker = new ClusterInvariantChecker(SampleAsync, invariants ?? ClusterInvariants.All, pollInterval);
        _nemesis.OnEvent = Checker.Notify;
        Checker.Start();
    }

    /// <summary>The hash-chain observer attached to a specific (node endpoint, partition) pair.</summary>
    public HashChainStateMachine ChainFor(string endpoint, int partition) => _chains[(endpoint, partition)];

    /// <summary>Every hash-chain observer for a partition, one per node.</summary>
    public IEnumerable<HashChainStateMachine> ChainsFor(int partition) =>
        _chains.Where(kv => kv.Key.Partition == partition).Select(kv => kv.Value);

    /// <summary>Throws if a safety invariant has been confirmed violated during the run.</summary>
    public void ThrowIfViolated() => Checker.ThrowIfViolated();

    /// <summary>Builds an immutable point-in-time cluster view. Never throws (transient sampling errors are swallowed).</summary>
    public async Task<ClusterView> SampleAsync(CancellationToken ct = default)
    {
        long seq = Interlocked.Increment(ref _sampleSeq);

        List<RaftPartitionView> views = [];
        foreach (RaftManager node in _nodes)
        foreach (int partition in _partitions)
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
    private void RecordCommitAcks(IReadOnlyList<RaftCommitAckObservation> observations)
    {
        lock (_recordLock)
        {
            foreach (RaftCommitAckObservation o in observations)
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

    /// <summary>Builds the standard failure report (seed, nemesis event tail, per-node views) for a scenario.</summary>
    public async Task<string> BuildFailureReportAsync(string violated, CancellationToken ct = default)
    {
        ClusterView view = await SampleAsync(ct).ConfigureAwait(false);
        StringBuilder sb = new();
        sb.AppendLine("=== Chaos failure report ===");
        sb.AppendLine($"scenario={_scenario} seed={_seed} violated={violated}");
        sb.AppendLine($"nemesis events={_nemesis.TotalEventCount} held={_nemesis.HeldCount} delayed={_nemesis.DelayedDeliveryCount}");
        sb.AppendLine("-- last 20 nemesis events --");
        foreach (NemesisEvent e in _nemesis.RecentEvents())
            sb.AppendLine("  " + e);
        sb.AppendLine("-- per-node views --");
        foreach (RaftPartitionView v in view.PartitionViews.OrderBy(v => v.Partition).ThenBy(v => v.Endpoint))
            sb.AppendLine("  " + v);
        return sb.ToString();
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;
        await Checker.DisposeAsync().ConfigureAwait(false);
    }
}
