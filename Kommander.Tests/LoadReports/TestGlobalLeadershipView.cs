
using Kommander.System;
using Kommander.Time;

namespace Kommander.Tests.LoadReports;

/// <summary>
/// Unit tests for <see cref="GlobalLeadershipView"/>: stale-report expiry,
/// conflicting-claim resolution, and the <c>IsComplete</c> predicate.
/// </summary>
public sealed class TestGlobalLeadershipView
{
    private static readonly HLCTimestamp T0 = new(0, 1_000_000, 0); // epoch ms=1_000_000

    private static ClusterMember Voter(string endpoint) =>
        new() { Endpoint = endpoint, Role = ClusterMemberRole.Voter };

    private static NodeLoadReport Report(string endpoint, long version, HLCTimestamp time,
        params (int partitionId, double load, long sinceMs)[] leaderships) =>
        new()
        {
            Endpoint = endpoint,
            ReportVersion = version,
            Time = time,
            Leaderships = leaderships
                .Select(l => new PartitionLoad { PartitionId = l.partitionId, Load = l.load, LeaderSinceMs = l.sinceMs })
                .ToList(),
        };

    // ── IsComplete ────────────────────────────────────────────────────────────

    [Fact]
    public void IsComplete_FalseWhenSilentLiveVoter()
    {
        // 2 live voters but only 1 fresh report → incomplete.
        HLCTimestamp now = T0 + TimeSpan.FromSeconds(1);
        TimeSpan ttl = TimeSpan.FromSeconds(20);

        var view = GlobalLeadershipView.Build(
            [Report("a:1", 1, T0, (1, 1.0, 6000))],
            [Voter("a:1"), Voter("b:1")],
            aliveEndpoints: new global::System.Collections.Generic.HashSet<string> { "a:1", "b:1" },
            ttl, now);

        Assert.Equal(1, view.FreshReportCount);
        Assert.False(view.IsComplete());
    }

    [Fact]
    public void IsComplete_FalseWhenDeadNodeReportMasksLiveButSilentNode()
    {
        // Voters A, B, C. B is alive but silent. C is dead but its report is still within TTL.
        // FreshReportCount = 2 (A and C), LiveVoters = {A, B} (count also 2).
        // A count-based check would incorrectly return true; the set-membership check catches it.
        HLCTimestamp now = T0 + TimeSpan.FromSeconds(5);
        TimeSpan ttl = TimeSpan.FromSeconds(20);

        var view = GlobalLeadershipView.Build(
            [
                Report("a:1", 1, T0, (1, 1.0, 6000)),
                // C's report is still fresh but C is not in aliveEndpoints (dead).
                Report("c:1", 1, T0, (2, 1.0, 6000)),
                // B is alive (in aliveEndpoints) but has sent no report.
            ],
            [Voter("a:1"), Voter("b:1"), Voter("c:1")],
            aliveEndpoints: new global::System.Collections.Generic.HashSet<string> { "a:1", "b:1" },
            ttl, now);

        // FreshReportCount = 2, LiveVoters = {"a:1", "b:1"}, but "b:1" has no fresh report.
        Assert.Equal(2, view.FreshReportCount);
        Assert.Equal(2, view.LiveVoters.Count);
        Assert.False(view.IsComplete(),
            "Dead node C's lingering report must not substitute for silent live voter B.");
    }

    [Fact]
    public void IsComplete_TrueWhenAllLiveVotersReported()
    {
        HLCTimestamp now = T0 + TimeSpan.FromSeconds(1);
        TimeSpan ttl = TimeSpan.FromSeconds(20);

        var view = GlobalLeadershipView.Build(
            [
                Report("a:1", 1, T0, (1, 1.0, 6000)),
                Report("b:1", 1, T0, (2, 0.5, 6000)),
            ],
            [Voter("a:1"), Voter("b:1")],
            aliveEndpoints: new global::System.Collections.Generic.HashSet<string> { "a:1", "b:1" },
            ttl, now);

        Assert.True(view.IsComplete());
    }

    [Fact]
    public void IsComplete_FalseWithNoLiveVoters()
    {
        // No live voters → IsComplete returns false (nothing to balance).
        HLCTimestamp now = T0 + TimeSpan.FromSeconds(1);
        TimeSpan ttl = TimeSpan.FromSeconds(20);

        var view = GlobalLeadershipView.Build(
            [Report("a:1", 1, T0, (1, 1.0, 6000))],
            [Voter("a:1")],
            aliveEndpoints: new global::System.Collections.Generic.HashSet<string>(),
            ttl, now);

        Assert.False(view.IsComplete());
    }

    // ── Stale-report expiry ────────────────────────────────────────────────────

    [Fact]
    public void StaleReportsAreExcluded()
    {
        TimeSpan ttl = TimeSpan.FromSeconds(20);
        // T0 is 30 s before now → older than TTL.
        HLCTimestamp now = T0 + TimeSpan.FromSeconds(30);

        var view = GlobalLeadershipView.Build(
            [Report("a:1", 1, T0, (1, 2.0, 10_000))],
            [Voter("a:1")],
            aliveEndpoints: new global::System.Collections.Generic.HashSet<string> { "a:1" },
            ttl, now);

        Assert.Equal(0, view.FreshReportCount);
        Assert.Empty(view.PartitionOwner);
        Assert.Empty(view.LeadersByNode);
    }

    [Fact]
    public void FreshReportsAreIncluded()
    {
        TimeSpan ttl = TimeSpan.FromSeconds(20);
        // T0 is 5 s before now → within TTL.
        HLCTimestamp now = T0 + TimeSpan.FromSeconds(5);

        var view = GlobalLeadershipView.Build(
            [Report("a:1", 1, T0, (1, 2.0, 10_000))],
            [Voter("a:1")],
            aliveEndpoints: new global::System.Collections.Generic.HashSet<string> { "a:1" },
            ttl, now);

        Assert.Equal(1, view.FreshReportCount);
        Assert.Equal("a:1", view.PartitionOwner[1]);
    }

    // ── Duplicate / out-of-order deduplication ────────────────────────────────

    [Fact]
    public void HigherReportVersionWins()
    {
        HLCTimestamp old = T0;
        HLCTimestamp fresh = T0 + TimeSpan.FromSeconds(1);
        HLCTimestamp now = T0 + TimeSpan.FromSeconds(5);
        TimeSpan ttl = TimeSpan.FromSeconds(20);

        // Older version arrives after newer; should be discarded.
        var view = GlobalLeadershipView.Build(
            [
                Report("a:1", 5, fresh, (1, 9.0, 7000)),
                Report("a:1", 3, old,   (1, 1.0, 7000)), // older version
            ],
            [Voter("a:1")],
            aliveEndpoints: new global::System.Collections.Generic.HashSet<string> { "a:1" },
            ttl, now);

        Assert.Equal(9.0, view.PartitionLoad[1], precision: 6);
    }

    // ── Conflicting-claim resolution ──────────────────────────────────────────

    [Fact]
    public void ConflictingClaimResolvedByNewerTime()
    {
        HLCTimestamp t1 = T0;
        HLCTimestamp t2 = T0 + TimeSpan.FromSeconds(2); // newer
        HLCTimestamp now = T0 + TimeSpan.FromSeconds(5);
        TimeSpan ttl = TimeSpan.FromSeconds(20);

        // Both nodes claim partition 1; node b has the newer Time → wins.
        var view = GlobalLeadershipView.Build(
            [
                Report("a:1", 1, t1, (1, 3.0, 8000)),
                Report("b:1", 1, t2, (1, 7.0, 8000)),
            ],
            [Voter("a:1"), Voter("b:1")],
            aliveEndpoints: new global::System.Collections.Generic.HashSet<string> { "a:1", "b:1" },
            ttl, now);

        Assert.Equal("b:1", view.PartitionOwner[1]);
        Assert.Equal(7.0, view.PartitionLoad[1], precision: 6);
    }

    [Fact]
    public void ConflictingClaimFirstSeenWinsOnEqualTime()
    {
        HLCTimestamp sameTime = T0;
        HLCTimestamp now = T0 + TimeSpan.FromSeconds(5);
        TimeSpan ttl = TimeSpan.FromSeconds(20);

        // Equal Time → first seen (a) wins (CompareTo returns 0, strict > fails).
        var view = GlobalLeadershipView.Build(
            [
                Report("a:1", 1, sameTime, (1, 3.0, 8000)),
                Report("b:1", 1, sameTime, (1, 7.0, 8000)),
            ],
            [Voter("a:1"), Voter("b:1")],
            aliveEndpoints: new global::System.Collections.Generic.HashSet<string> { "a:1", "b:1" },
            ttl, now);

        Assert.Equal("a:1", view.PartitionOwner[1]);
    }

    // ── LiveVoters filtering ──────────────────────────────────────────────────

    [Fact]
    public void LiveVotersExcludesDeadAndLearners()
    {
        HLCTimestamp now = T0 + TimeSpan.FromSeconds(5);
        TimeSpan ttl = TimeSpan.FromSeconds(20);

        var view = GlobalLeadershipView.Build(
            [],
            [
                Voter("a:1"),
                new() { Endpoint = "b:1", Role = ClusterMemberRole.Voter },
                new() { Endpoint = "c:1", Role = ClusterMemberRole.Learner },
            ],
            // b:1 is absent from alive set → excluded despite being a Voter in the roster.
            aliveEndpoints: new global::System.Collections.Generic.HashSet<string> { "a:1" },
            ttl, now);

        Assert.Contains("a:1", view.LiveVoters);
        Assert.DoesNotContain("b:1", view.LiveVoters);
        Assert.DoesNotContain("c:1", view.LiveVoters);
    }

    // ── Load aggregation ──────────────────────────────────────────────────────

    [Fact]
    public void LoadByNodeSumsOwnedPartitionLoads()
    {
        HLCTimestamp now = T0 + TimeSpan.FromSeconds(5);
        TimeSpan ttl = TimeSpan.FromSeconds(20);

        var view = GlobalLeadershipView.Build(
            [
                Report("a:1", 1, T0, (1, 3.0, 6000), (2, 5.0, 6000)),
            ],
            [Voter("a:1")],
            aliveEndpoints: new global::System.Collections.Generic.HashSet<string> { "a:1" },
            ttl, now);

        Assert.Equal(8.0, view.LoadByNode["a:1"], precision: 6);
    }
}
