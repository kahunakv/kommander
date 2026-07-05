
using System.Collections.Concurrent;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL.IO;
using Microsoft.Extensions.Logging;

namespace Kommander;

/// <summary>
/// Owns the <c>_reportVersion</c> counter and provides local and gossip-based
/// load-report queries for <see cref="RaftManager"/>. Reads partition state
/// directly from the <c>partitions</c> dictionary via <see cref="RaftPartition"/>
/// references and delegates remote-node fallback to the coordinator's
/// <see cref="LoadReportStore"/> snapshot.
/// </summary>
internal sealed class LoadReportService
{
    private long _reportVersion;

    private readonly ConcurrentDictionary<int, RaftPartition> partitions;
    private readonly FairWalScheduler walScheduler;
    private readonly Func<IReadOnlyList<NodeLoadReport>> getLoadReports;
    private readonly Func<HLCTimestamp> getHlcNow;
    private readonly Func<int, string?> getPartitionLeaderEndpoint;
    private readonly RaftConfiguration configuration;
    private readonly string localEndpoint;

    internal LoadReportService(
        ConcurrentDictionary<int, RaftPartition> partitions,
        FairWalScheduler walScheduler,
        Func<IReadOnlyList<NodeLoadReport>> getLoadReports,
        Func<HLCTimestamp> getHlcNow,
        Func<int, string?> getPartitionLeaderEndpoint,
        RaftConfiguration configuration,
        string localEndpoint)
    {
        this.partitions = partitions;
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
        double ticksToMs = 1000.0 / global::System.Diagnostics.Stopwatch.Frequency;
        long now = global::System.Diagnostics.Stopwatch.GetTimestamp();

        List<PartitionLoad> leaderships = [];

        foreach (KeyValuePair<int, RaftPartition> kv in partitions)
        {
            RaftPartition p = kv.Value;
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

        return new NodeLoadReport
        {
            Endpoint = localEndpoint,
            ReportVersion = Interlocked.Increment(ref _reportVersion),
            Time = getHlcNow(),
            Leaderships = leaderships,
        };
    }

    private NodeLoadReport? FindBestLoadReport(int partitionId)
    {
        string? leaderEndpoint = getPartitionLeaderEndpoint(partitionId);
        IReadOnlyList<NodeLoadReport> reports = getLoadReports();

        NodeLoadReport? best = null;

        if (leaderEndpoint is not null)
        {
            foreach (NodeLoadReport r in reports)
            {
                if (string.Equals(r.Endpoint, leaderEndpoint, StringComparison.Ordinal))
                {
                    best = r;
                    break;
                }
            }
        }

        if (best is null)
        {
            foreach (NodeLoadReport r in reports)
            {
                foreach (PartitionLoad l in r.Leaderships)
                {
                    if (l.PartitionId == partitionId && (best is null || r.Time > best.Time))
                    {
                        best = r;
                        break;
                    }
                }
            }
        }

        return best;
    }

    internal double GetPartitionLogOpsPerSecond(int partitionId)
    {
        if (partitions.TryGetValue(partitionId, out RaftPartition? p) &&
            string.Equals(p.Leader, localEndpoint, StringComparison.Ordinal))
            return p.GetLogOpsPerSecond();

        NodeLoadReport? best = FindBestLoadReport(partitionId);
        if (best is null) return 0.0;

        foreach (PartitionLoad l in best.Leaderships)
        {
            if (l.PartitionId == partitionId)
                return l.LogOpsPerSecond;
        }

        return 0.0;
    }

    internal int GetPartitionWalQueueDepth(int partitionId)
    {
        if (partitions.TryGetValue(partitionId, out RaftPartition? p) &&
            string.Equals(p.Leader, localEndpoint, StringComparison.Ordinal))
            return walScheduler.GetPartitionDepth(partitionId);

        NodeLoadReport? best = FindBestLoadReport(partitionId);
        if (best is null) return 0;

        foreach (PartitionLoad l in best.Leaderships)
        {
            if (l.PartitionId == partitionId)
                return l.WalQueueDepth;
        }

        return 0;
    }

    internal double GetPartitionCommitWaitMs(int partitionId)
    {
        if (partitions.TryGetValue(partitionId, out RaftPartition? p) &&
            string.Equals(p.Leader, localEndpoint, StringComparison.Ordinal))
            return walScheduler.GetPartitionCommitWaitMs(partitionId);

        NodeLoadReport? best = FindBestLoadReport(partitionId);
        if (best is null) return 0.0;

        foreach (PartitionLoad l in best.Leaderships)
        {
            if (l.PartitionId == partitionId)
                return l.CommitWaitMs;
        }

        return 0.0;
    }
}
