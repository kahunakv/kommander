
namespace Kommander.Tests;

/// <summary>
/// Unit tests for <see cref="RaftConfiguration.Validate"/> and the quiescence config knobs.
/// </summary>
public class TestRaftConfigurationValidation
{
    // ── Validate passes at defaults ───────────────────────────────────────────

    [Fact]
    public void Validate_DefaultConfiguration_DoesNotThrow()
    {
        RaftConfiguration cfg = new();
        // Default: EnableQuiescence=false, so the timing invariant is not checked → valid.
        cfg.Validate();
    }

    // ── LeadershipBarrierTimeout must be positive ─────────────────────────────

    [Fact]
    public void Validate_NonPositiveLeadershipBarrierTimeout_Throws()
    {
        RaftConfiguration cfg = new() { LeadershipBarrierTimeout = TimeSpan.Zero };
        RaftException ex = Assert.Throws<RaftException>(cfg.Validate);
        Assert.Contains("LeadershipBarrierTimeout", ex.Message);
    }

    // ── PingInterval >= StartElectionTimeout with quiescence on → throws ──────

    [Fact]
    public void Validate_PingIntervalEqualsElectionTimeout_Throws()
    {
        RaftConfiguration cfg = new()
        {
            EnableQuiescence = true,
            PingInterval = TimeSpan.FromMilliseconds(2000),
            StartElectionTimeout = 2000,
        };
        RaftException ex = Assert.Throws<RaftException>(cfg.Validate);
        Assert.Contains("PingInterval", ex.Message);
        Assert.Contains("StartElectionTimeout", ex.Message);
    }

    [Fact]
    public void Validate_PingIntervalAboveElectionTimeout_Throws()
    {
        RaftConfiguration cfg = new()
        {
            EnableQuiescence = true,
            PingInterval = TimeSpan.FromSeconds(5),
            StartElectionTimeout = 2000,
        };
        Assert.Throws<RaftException>(cfg.Validate);
    }

    // ── Same misconfig with quiescence off → does NOT throw ──────────────────

    [Fact]
    public void Validate_PingIntervalAboveElectionTimeout_QuiescenceDisabled_DoesNotThrow()
    {
        RaftConfiguration cfg = new()
        {
            EnableQuiescence = false,
            PingInterval = TimeSpan.FromSeconds(5),
            StartElectionTimeout = 2000,
        };
        cfg.Validate(); // must not throw
    }

    // ── Quiescence on with SWIM disabled (PingInterval = 0) → throws ──────────

    [Fact]
    public void Validate_QuiescenceEnabled_PingIntervalZero_Throws()
    {
        RaftConfiguration cfg = new()
        {
            EnableQuiescence = true,
            PingInterval = TimeSpan.Zero,
            StartElectionTimeout = 2000,
        };
        RaftException ex = Assert.Throws<RaftException>(cfg.Validate);
        Assert.Contains("PingInterval", ex.Message);
    }

    [Fact]
    public void Validate_QuiescenceDisabled_PingIntervalZero_DoesNotThrow()
    {
        RaftConfiguration cfg = new()
        {
            EnableQuiescence = false,
            PingInterval = TimeSpan.Zero,
            StartElectionTimeout = 2000,
        };
        cfg.Validate(); // SWIM-off is fine when quiescence is off
    }

    // ── Heartbeat cadence vs election timeout (always enforced) ───────────────
    // A heartbeat cadence at or above StartElectionTimeout guarantees followers time out
    // before the next heartbeat arrives → perpetual re-elections on every partition.
    // Origin: a downstream harness set StartElectionTimeout=50/EndElectionTimeout=150 but
    // left HeartbeatInterval (500 ms) and CheckLeaderInterval (250 ms) at their defaults;
    // the cluster "assembled" (a leader existed at any instant) but never held a leader.

    [Fact]
    public void Validate_HeartbeatIntervalAtOrAboveElectionTimeout_Throws()
    {
        RaftConfiguration cfg = new()
        {
            EnableQuiescence = false,
            StartElectionTimeout = 50,
            EndElectionTimeout = 150,
            CheckLeaderInterval = TimeSpan.FromMilliseconds(25),
        };
        // HeartbeatInterval left at its 500 ms default → >= StartElectionTimeout.
        RaftException ex = Assert.Throws<RaftException>(cfg.Validate);
        Assert.Contains("HeartbeatInterval", ex.Message);
        Assert.Contains("StartElectionTimeout", ex.Message);
    }

    [Fact]
    public void Validate_CheckLeaderIntervalAtOrAboveElectionTimeout_Throws()
    {
        RaftConfiguration cfg = new()
        {
            EnableQuiescence = false,
            StartElectionTimeout = 50,
            EndElectionTimeout = 150,
            HeartbeatInterval = TimeSpan.FromMilliseconds(25),
        };
        // CheckLeaderInterval left at its 250 ms default → >= StartElectionTimeout, and it
        // gates when heartbeats are actually sent regardless of HeartbeatInterval.
        RaftException ex = Assert.Throws<RaftException>(cfg.Validate);
        Assert.Contains("CheckLeaderInterval", ex.Message);
    }

    [Fact]
    public void Validate_FastElectionTimersWithMatchingHeartbeatCadence_DoesNotThrow()
    {
        RaftConfiguration cfg = new()
        {
            EnableQuiescence = false,
            StartElectionTimeout = 100,
            EndElectionTimeout = 300,
            HeartbeatInterval = TimeSpan.FromMilliseconds(50),
            RecentHeartbeat = TimeSpan.FromMilliseconds(25),
            CheckLeaderInterval = TimeSpan.FromMilliseconds(25),
        };
        cfg.Validate(); // must not throw
    }

    // ── Quiescence knob defaults ──────────────────────────────────────────────

    [Fact]
    public void EnableQuiescence_DefaultsToTrue()
    {
        RaftConfiguration cfg = new();
        Assert.True(cfg.EnableQuiescence);
    }

    [Fact]
    public void QuiesceAfter_DefaultsTo1500ms()
    {
        RaftConfiguration cfg = new();
        Assert.Equal(TimeSpan.FromMilliseconds(1500), cfg.QuiesceAfter);
    }

    // ── Degraded-node avoidance knobs ─────────────────────────────────────────
    // These are validated unconditionally, not only when EnableSlowNodeAvoidance is on: a knob
    // that is wrong while the feature is off is still wrong the moment someone switches it on.

    [Fact]
    public void Validate_SlowNodeMultiplierAtOne_Throws()
    {
        // At 1.0 every node at or above the median is a candidate — at least half the cluster.
        RaftConfiguration cfg = new() { SlowNodeMultiplier = 1.0 };
        RaftException ex = Assert.Throws<RaftException>(cfg.Validate);
        Assert.Contains("SlowNodeMultiplier", ex.Message);
    }

    [Fact]
    public void Validate_NegativeSlowNodeFloor_Throws()
    {
        RaftConfiguration cfg = new() { SlowNodeFloorMs = -1.0 };
        RaftException ex = Assert.Throws<RaftException>(cfg.Validate);
        Assert.Contains("SlowNodeFloorMs", ex.Message);
    }

    [Fact]
    public void Validate_SlowNodeMinSamplesBelowOne_Throws()
    {
        // Zero would let a node with no observations be judged on a zero commit wait.
        RaftConfiguration cfg = new() { SlowNodeMinSamples = 0 };
        RaftException ex = Assert.Throws<RaftException>(cfg.Validate);
        Assert.Contains("SlowNodeMinSamples", ex.Message);
    }

    [Fact]
    public void Validate_NonPositiveSlowNodeObservationTtl_Throws()
    {
        RaftConfiguration cfg = new() { SlowNodeObservationTtl = TimeSpan.Zero };
        RaftException ex = Assert.Throws<RaftException>(cfg.Validate);
        Assert.Contains("SlowNodeObservationTtl", ex.Message);
    }

    [Fact]
    public void Validate_SlowNodePassCountsBelowOne_Throw()
    {
        RaftException enter = Assert.Throws<RaftException>(new RaftConfiguration { SlowNodeEnterPasses = 0 }.Validate);
        Assert.Contains("SlowNodeEnterPasses", enter.Message);

        RaftException exit = Assert.Throws<RaftException>(new RaftConfiguration { SlowNodeExitPasses = 0 }.Validate);
        Assert.Contains("SlowNodeExitPasses", exit.Message);
    }

    [Fact]
    public void Validate_SlowNodeAvoidanceEnabledWithBalancerOff_DoesNotThrow()
    {
        // A no-op combination, not a misconfiguration: the node-health fields still ride the load
        // report, and a deployment may want them for the placement path alone.
        RaftConfiguration cfg = new() { EnableSlowNodeAvoidance = true, EnableLeaderBalancer = false };
        cfg.Validate();
    }
}
