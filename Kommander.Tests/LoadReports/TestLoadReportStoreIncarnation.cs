
using System.Diagnostics;
using Kommander.System;
using Kommander.Time;

namespace Kommander.Tests.LoadReports;

/// <summary>
/// <see cref="LoadReportStore.Apply"/> orders a sender's reports by incarnation first and by
/// version within an incarnation. These tests pin the rule that the Kahuna 2026-10-09 learner
/// stall was missing: a restarted sender's version counter restarts at 1, so a version-only
/// store rejected every post-restart report, the retained pre-restart entry aged past the hint
/// TTL, and the placement controller lost the leader hint for every range the restarted node led.
/// </summary>
public sealed class TestLoadReportStoreIncarnation
{
    private const string Sender = "node-a:5000";

    private static readonly TimeSpan Ttl = TimeSpan.FromSeconds(20);

    private static RaftSystemRequest Report(long incarnation, long version, params int[] ledPartitions) =>
        new(new NodeLoadReport
        {
            Endpoint = Sender,
            Incarnation = incarnation,
            ReportVersion = version,
            Time = new HLCTimestamp(1, version, 0),
            Leaderships = ledPartitions.Select(p => new PartitionLoad { PartitionId = p }).ToList(),
        });

    private static NodeLoadReport Retained(LoadReportStore store) =>
        Assert.Single(store.GetAll(), r => r.Endpoint == Sender);

    private static long Ticks(double seconds) => (long)(Stopwatch.Frequency * seconds);

    [Fact]
    public void RestartedSender_LowerVersionNewerIncarnation_ReplacesTheRetainedEntry()
    {
        LoadReportStore store = new(staleAfter: Ttl);

        // The dead lifetime reported many times; its last entry is retained.
        store.Apply(Report(incarnation: 1_000, version: 300, 7));
        Assert.Equal(300, Retained(store).ReportVersion);

        // First report of the restarted process: version 1, newer incarnation.
        store.Apply(Report(incarnation: 2_000, version: 1, 7));

        NodeLoadReport retained = Retained(store);
        Assert.Equal(2_000, retained.Incarnation);
        Assert.Equal(1, retained.ReportVersion);
    }

    [Fact]
    public void SameIncarnation_LowerOrEqualVersion_IsRejected()
    {
        LoadReportStore store = new(staleAfter: Ttl);

        store.Apply(Report(incarnation: 1_000, version: 5, 7));
        store.Apply(Report(incarnation: 1_000, version: 4, 8));
        store.Apply(Report(incarnation: 1_000, version: 5, 8));

        NodeLoadReport retained = Retained(store);
        Assert.Equal(5, retained.ReportVersion);
        Assert.Equal(7, Assert.Single(retained.Leaderships).PartitionId);
    }

    [Fact]
    public void SameIncarnation_HigherVersion_IsAccepted()
    {
        LoadReportStore store = new(staleAfter: Ttl);

        store.Apply(Report(incarnation: 1_000, version: 5, 7));
        store.Apply(Report(incarnation: 1_000, version: 6, 8));

        Assert.Equal(8, Assert.Single(Retained(store).Leaderships).PartitionId);
    }

    /// <summary>
    /// An in-flight report of the dead lifetime that lands after the restarted process's first
    /// report must not flip the entry back while the retained one is fresh.
    /// </summary>
    [Fact]
    public void OlderIncarnation_IsRejectedWhileTheRetainedEntryIsFresh()
    {
        LoadReportStore store = new(staleAfter: Ttl);

        store.Apply(Report(incarnation: 2_000, version: 1, 7));
        store.Apply(Report(incarnation: 1_000, version: 999, 8));

        NodeLoadReport retained = Retained(store);
        Assert.Equal(2_000, retained.Incarnation);
        Assert.Equal(7, Assert.Single(retained.Leaderships).PartitionId);
    }

    /// <summary>
    /// The backstop for any ordering failure (a restart whose clock stepped backwards gives the
    /// new lifetime a LOWER incarnation): once the retained entry is older than the TTL it is
    /// invisible to every freshness-filtered consumer, so any report replaces it.
    /// </summary>
    [Fact]
    public void OlderIncarnation_ReplacesARetainedEntryOlderThanTheTtl()
    {
        long now = Ticks(100);
        LoadReportStore store = new(() => now, Ttl);

        store.Apply(Report(incarnation: 2_000, version: 50, 7));

        now += Ticks(Ttl.TotalSeconds + 1);
        store.Apply(Report(incarnation: 1_000, version: 1, 8));

        NodeLoadReport retained = Retained(store);
        Assert.Equal(1_000, retained.Incarnation);
        Assert.Equal(8, Assert.Single(retained.Leaderships).PartitionId);
        Assert.Equal(now, retained.ReceivedAtTicks);
    }

    /// <summary>
    /// A sender too old to carry the field reports incarnation 0 forever; two zero incarnations
    /// must keep the version-only ordering so a mixed-version cluster behaves as before.
    /// </summary>
    [Fact]
    public void LegacySender_NoIncarnation_OrdersByVersionAlone()
    {
        LoadReportStore store = new(staleAfter: Ttl);

        store.Apply(Report(incarnation: 0, version: 3, 7));
        store.Apply(Report(incarnation: 0, version: 2, 8));
        Assert.Equal(7, Assert.Single(Retained(store).Leaderships).PartitionId);

        store.Apply(Report(incarnation: 0, version: 4, 9));
        Assert.Equal(9, Assert.Single(Retained(store).Leaderships).PartitionId);
    }

    /// <summary>
    /// Rolling upgrade: the first report that carries an incarnation beats a retained legacy
    /// entry whatever its version.
    /// </summary>
    [Fact]
    public void UpgradedSender_FirstIncarnatedReport_BeatsALegacyEntry()
    {
        LoadReportStore store = new(staleAfter: Ttl);

        store.Apply(Report(incarnation: 0, version: 300, 7));
        store.Apply(Report(incarnation: 1_000, version: 1, 8));

        Assert.Equal(8, Assert.Single(Retained(store).Leaderships).PartitionId);
    }
}
