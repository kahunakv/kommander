
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

    // ── Quiescence knob defaults ──────────────────────────────────────────────

    [Fact]
    public void EnableQuiescence_DefaultsToFalse()
    {
        RaftConfiguration cfg = new();
        Assert.False(cfg.EnableQuiescence);
    }

    [Fact]
    public void QuiesceAfter_DefaultsTo1500ms()
    {
        RaftConfiguration cfg = new();
        Assert.Equal(TimeSpan.FromMilliseconds(1500), cfg.QuiesceAfter);
    }
}
