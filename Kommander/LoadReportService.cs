
using Kommander.System;
using Kommander.Time;
using Kommander.WAL.IO;
using Microsoft.Extensions.Logging;

namespace Kommander;

/// <summary>
/// Owns the <c>_reportVersion</c> counter and provides local and gossip-based
/// load-report queries for <see cref="RaftManager"/>. Reads partition state through
/// <see cref="IPartitionProvider"/> — the manager keeps sole ownership of the registry,
/// so this service never holds a reference to the live dictionary — and delegates
/// remote-node fallback to the coordinator's <see cref="LoadReportStore"/> snapshot.
/// </summary>
internal sealed class LoadReportService
{
    private long _reportVersion;

    private readonly IPartitionProvider partitionProvider;
    private readonly FairWalScheduler walScheduler;
    private readonly Func<IReadOnlyList<NodeLoadReport>> getLoadReports;
    private readonly Func<HLCTimestamp> getHlcNow;
    private readonly Func<int, string?> getPartitionLeaderEndpoint;
    private readonly RaftConfiguration configuration;
    private readonly string localEndpoint;

    internal LoadReportService(
        IPartitionProvider partitionProvider,
        FairWalScheduler walScheduler,
        Func<IReadOnlyList<NodeLoadReport>> getLoadReports,
        Func<HLCTimestamp> getHlcNow,
        Func<int, string?> getPartitionLeaderEndpoint,
        RaftConfiguration configuration,
        string localEndpoint)
    {
        this.partitionProvider = partitionProvider;
        this.walScheduler = walScheduler;
        this.getLoadReports = getLoadReports;
        this.getHlcNow = getHlcNow;
        this.getPartitionLeaderEndpoint = getPartitionLeaderEndpoint;
        this.configuration = configuration;
        this.localEndpoint = localEndpoint;
    }

    internal NodeLoadReport BuildLocalLoadReport()
    {
        double wOps = configuration.LeaderBalancerOpsWeight;
        double wQueue = configuration.LeaderBalancerQueueWeight;
        double ticksToMs = 1000.0 / configuration.TickSource.Frequency;
        long now = configuration.TickSource.GetTimestamp();

        List<PartitionLoad> leaderships = [];

        foreach (RaftPartition p in partitionProvider.DataPartitions)
        {
            if (!string.Equals(p.Leader, localEndpoint, StringComparison.Ordinal))
                continue;

            long leaderSinceMs = (long)((now - p.LeaderChangedTicks) * ticksToMs);

            leaderships.Add(new PartitionLoad
            {
                PartitionId = p.PartitionId,
                Load = p.GetCurrentLoad(wOps, wQueue),
                LeaderSinceMs = leaderSinceMs > 0 ? leaderSinceMs : 0,
                LogOpsPerSecond = p.GetLogOpsPerSecond(),
                WalQueueDepth = walScheduler.GetPartitionDepth(p.PartitionId),
                CommitWaitMs = walScheduler.GetPartitionCommitWaitMs(p.PartitionId),
            });
        }

        // Node-wide disk health is read outside the leadership loop on purpose: a node that leads
        // nothing must still advertise it, because that is exactly the node the balancer has to
        // judge before it hands over a leadership.
        return new NodeLoadReport
        {
            Endpoint = localEndpoint,
            ReportVersion = Interlocked.Increment(ref _reportVersion),
            Time = getHlcNow(),
            Zone = configuration.Zone,
            NodeCommitWaitMs = walScheduler.GetNodeCommitWaitMs(),
            NodeCommitWaitSamples = walScheduler.GetNodeCommitWaitSamples(),
            NodeCommitWaitAgeMs = (long)walScheduler.GetNodeCommitWaitAgeMs(),
            Leaderships = leaderships,
        };
    }

    /// <summary>
    /// Best-effort leader hint for <paramref name="partitionId"/> — see
    /// <see cref="IRaft.GetPartitionLeaderHint"/> for the contract. Local belief wins when the
    /// partition is hosted here; otherwise the newest fresh gossiped
    /// <see cref="NodeLoadReport"/> claiming the partition provides the answer.
    /// <para>Freshness is enforced here with <see cref="RaftConfiguration.LeaderBalancerReportTtl"/>
    /// rather than left to <see cref="LoadReportStore.EvictStale"/>, because eviction runs only on
    /// the P0 leader's balancer pass — on every other node reports accumulate forever, and an
    /// unfiltered scan would keep returning a dead node's endpoint indefinitely.</para>
    /// </summary>
    internal string? GetPartitionLeaderHint(int partitionId)
    {
        string? local = getPartitionLeaderEndpoint(partitionId);
        if (!string.IsNullOrEmpty(local))
            return local;

        HLCTimestamp now = getHlcNow();
        TimeSpan ttl = configuration.LeaderBalancerReportTtl;

        NodeLoadReport? best = null;
        foreach (NodeLoadReport r in getLoadReports())
        {
            if ((now - r.Time) > ttl)
                continue;

            foreach (PartitionLoad l in r.Leaderships)
            {
                if (l.PartitionId == partitionId && (best is null || r.Time > best.Time))
                {
                    best = r;
                    break;
                }
            }
        }

        return best?.Endpoint;
    }

    /// <summary>
    /// Finds the remote <see cref="PartitionLoad"/> entry that the three metric getters read, or null
    /// when no gossiped report carries one for <paramref name="partitionId"/>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Returns the entry rather than the owning <see cref="NodeLoadReport"/> so a getter does not scan
    /// the winning report's leadership list a second time to reach the one field it wants. Each getter
    /// is a separate call, so the duplicated scan was paid once per metric per partition.
    /// </para>
    /// <para>
    /// Selection order is unchanged: a known leader endpoint wins outright — including when its report
    /// carries no entry for the partition, which still reads as "no value" rather than falling back to
    /// another node's claim — and otherwise the newest report claiming the partition wins.
    /// </para>
    /// </remarks>
    private PartitionLoad? FindPartitionLoad(int partitionId)
    {
        string? leaderEndpoint = getPartitionLeaderEndpoint(partitionId);
        IReadOnlyList<NodeLoadReport> reports = getLoadReports();

        if (leaderEndpoint is not null)
        {
            foreach (NodeLoadReport r in reports)
            {
                if (!string.Equals(r.Endpoint, leaderEndpoint, StringComparison.Ordinal))
                    continue;

                foreach (PartitionLoad l in r.Leaderships)
                {
                    if (l.PartitionId == partitionId)
                        return l;
                }

                return null;
            }
        }

        NodeLoadReport? best = null;
        PartitionLoad? bestLoad = null;

        foreach (NodeLoadReport r in reports)
        {
            foreach (PartitionLoad l in r.Leaderships)
            {
                if (l.PartitionId != partitionId || (best is not null && r.Time <= best.Time))
                    continue;

                best = r;
                bestLoad = l;
                break;
            }
        }

        return bestLoad;
    }

    internal double GetPartitionLogOpsPerSecond(int partitionId)
    {
        if (partitionProvider.TryGetDataPartition(partitionId, out RaftPartition? p) && p is not null &&
            string.Equals(p.Leader, localEndpoint, StringComparison.Ordinal))
            return p.GetLogOpsPerSecond();

        return FindPartitionLoad(partitionId)?.LogOpsPerSecond ?? 0.0;
    }

    internal int GetPartitionWalQueueDepth(int partitionId)
    {
        if (partitionProvider.TryGetDataPartition(partitionId, out RaftPartition? p) && p is not null &&
            string.Equals(p.Leader, localEndpoint, StringComparison.Ordinal))
            return walScheduler.GetPartitionDepth(partitionId);

        return FindPartitionLoad(partitionId)?.WalQueueDepth ?? 0;
    }

    internal double GetPartitionCommitWaitMs(int partitionId)
    {
        if (partitionProvider.TryGetDataPartition(partitionId, out RaftPartition? p) && p is not null &&
            string.Equals(p.Leader, localEndpoint, StringComparison.Ordinal))
            return walScheduler.GetPartitionCommitWaitMs(partitionId);

        return FindPartitionLoad(partitionId)?.CommitWaitMs ?? 0.0;
    }
}
