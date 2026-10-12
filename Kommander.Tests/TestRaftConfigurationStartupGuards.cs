namespace Kommander.Tests;

/// <summary>
/// Startup guards of <see cref="RaftConfiguration.Validate"/> that fence node-level and
/// snapshot-subsystem misconfiguration: the monotonic tick source, the externally-driven
/// scheduling combination, duration floors where zero is the documented disable value, the
/// sender-budget vs gRPC-receive-limit boundaries, the heartbeat de-dup window, and the
/// snapshot receive/transfer bounds. Also covers the zone-hint normalization and the cached
/// transport authenticator invalidation.
/// </summary>
public class TestRaftConfigurationStartupGuards
{
    // ── Tick source ───────────────────────────────────────────────────────────

    [Fact]
    public void Validate_NullTickSource_ThrowsNamingTheOption()
    {
        RaftConfiguration cfg = new() { TickSource = null! };

        RaftException ex = Assert.Throws<RaftException>(cfg.Validate);

        Assert.Contains("TickSource", ex.Message);
    }

    // ── Externally driven scheduling ──────────────────────────────────────────

    [Fact]
    public void Validate_DedicatedThreadExecutorsWithExternalScheduling_Throws()
    {
        // A partition executor on its own dedicated thread has no pump, so a configuration
        // asking for external driving would start a node whose partitions never run.
        RaftConfiguration cfg = new()
        {
            EnableInternalSchedulingThreads = false,
            EnableSharedExecutorPool = false
        };

        RaftException ex = Assert.Throws<RaftException>(cfg.Validate);

        Assert.Contains("EnableInternalSchedulingThreads", ex.Message);
        Assert.Contains("EnableSharedExecutorPool", ex.Message);
    }

    [Fact]
    public void Validate_ExternalSchedulingWithSharedPool_DoesNotThrow()
    {
        RaftConfiguration cfg = new()
        {
            EnableInternalSchedulingThreads = false,
            EnableSharedExecutorPool = true
        };

        cfg.Validate();
    }

    // ── Durations where zero disables and negatives are refused ───────────────

    public static TheoryData<string, Action<RaftConfiguration>> NegativeDurationGuards => new()
    {
        // A negative watchdog/grace/retention window has no meaning: each of these treats
        // TimeSpan.Zero as "disable" and any negative value as a misconfiguration.
        {
            nameof(RaftConfiguration.WalStallStepDownTimeout),
            c => c.WalStallStepDownTimeout = TimeSpan.FromMilliseconds(-1)
        },
        {
            nameof(RaftConfiguration.BackfillSeededCatchUpGraceCap),
            c => c.BackfillSeededCatchUpGraceCap = TimeSpan.FromMilliseconds(-1)
        },
        {
            nameof(RaftConfiguration.CompactionSilentPeerRetentionWindow),
            c => c.CompactionSilentPeerRetentionWindow = TimeSpan.FromMilliseconds(-1)
        },
        {
            nameof(RaftConfiguration.WalStallWarnThreshold),
            c => c.WalStallWarnThreshold = TimeSpan.FromMilliseconds(-1)
        },
        {
            nameof(RaftConfiguration.SelfRepairPeerDownGrace),
            c => c.SelfRepairPeerDownGrace = TimeSpan.FromMilliseconds(-1)
        }
    };

    [Theory]
    [MemberData(nameof(NegativeDurationGuards))]
    public void Validate_NegativeDuration_ThrowsNamingTheOption(string optionName, Action<RaftConfiguration> mutate)
    {
        RaftConfiguration cfg = new();
        mutate(cfg);

        RaftException ex = Assert.Throws<RaftException>(cfg.Validate);

        Assert.Contains(optionName, ex.Message);
    }

    [Fact]
    public void Validate_ZeroDuration_DisablesEachWatchdogAndDoesNotThrow()
    {
        // Every guard above documents TimeSpan.Zero as its off switch, so a config with all
        // five disabled at once must still validate.
        RaftConfiguration cfg = new()
        {
            WalStallStepDownTimeout = TimeSpan.Zero,
            BackfillSeededCatchUpGraceCap = TimeSpan.Zero,
            CompactionSilentPeerRetentionWindow = TimeSpan.Zero,
            WalStallWarnThreshold = TimeSpan.Zero,
            SelfRepairPeerDownGrace = TimeSpan.Zero
        };

        cfg.Validate();
    }

    // ── Positive-only durations ───────────────────────────────────────────────

    public static TheoryData<string, Action<RaftConfiguration>> PositiveDurationGuards => new()
    {
        {
            nameof(RaftConfiguration.LeadershipConfirmationTimeout),
            c => c.LeadershipConfirmationTimeout = TimeSpan.Zero
        },
        {
            nameof(RaftConfiguration.LeadershipConfirmationTimeout),
            c => c.LeadershipConfirmationTimeout = TimeSpan.FromSeconds(-1)
        },
        {
            nameof(RaftConfiguration.ProposalTimeout),
            c => c.ProposalTimeout = TimeSpan.Zero
        },
        {
            nameof(RaftConfiguration.ProposalTimeout),
            c => c.ProposalTimeout = TimeSpan.FromSeconds(-1)
        },
        {
            nameof(RaftConfiguration.SnapshotReceiveSessionTtl),
            c => c.SnapshotReceiveSessionTtl = TimeSpan.Zero
        },
        {
            nameof(RaftConfiguration.SnapshotTransferStepTimeout),
            c => c.SnapshotTransferStepTimeout = TimeSpan.Zero
        }
    };

    [Theory]
    [MemberData(nameof(PositiveDurationGuards))]
    public void Validate_NonPositiveDuration_ThrowsNamingTheOption(string optionName, Action<RaftConfiguration> mutate)
    {
        // Unlike the watchdog family, these have no "disable" value: a zero would fail every
        // confirmation/write immediately or retain a receive session forever.
        RaftConfiguration cfg = new();
        mutate(cfg);

        RaftException ex = Assert.Throws<RaftException>(cfg.Validate);

        Assert.Contains(optionName, ex.Message);
    }

    // ── Snapshot receive bounds ───────────────────────────────────────────────

    public static TheoryData<string, Action<RaftConfiguration>> SnapshotReceiveBoundsGuards => new()
    {
        { nameof(RaftConfiguration.SnapshotMaxPendingSessions), c => c.SnapshotMaxPendingSessions = 0 },
        { nameof(RaftConfiguration.SnapshotMaxPendingSessions), c => c.SnapshotMaxPendingSessions = -1 },
        { nameof(RaftConfiguration.SnapshotMaxPendingBytes), c => c.SnapshotMaxPendingBytes = 0 },
        { nameof(RaftConfiguration.SnapshotMaxPendingBytes), c => c.SnapshotMaxPendingBytes = -1 },
        { nameof(RaftConfiguration.SnapshotStagingMemoryBytes), c => c.SnapshotStagingMemoryBytes = -1 }
    };

    [Theory]
    [MemberData(nameof(SnapshotReceiveBoundsGuards))]
    public void Validate_InvalidSnapshotReceiveBound_ThrowsNamingTheOption(
        string optionName,
        Action<RaftConfiguration> mutate)
    {
        RaftConfiguration cfg = new();
        mutate(cfg);

        RaftException ex = Assert.Throws<RaftException>(cfg.Validate);

        Assert.Contains(optionName, ex.Message);
    }

    [Fact]
    public void Validate_ZeroStagingMemoryBudget_StagesOnDiskAndDoesNotThrow()
    {
        // Zero is the documented "stage every session on disk from the first chunk" value.
        RaftConfiguration cfg = new()
        {
            SnapshotStagingDirectory = "/tmp/kommander-staging",
            SnapshotStagingMemoryBytes = 0
        };

        cfg.Validate();
    }

    [Fact]
    public void Validate_BlankSnapshotStagingDirectory_Throws()
    {
        RaftConfiguration cfg = new() { SnapshotStagingDirectory = "   " };

        RaftException ex = Assert.Throws<RaftException>(cfg.Validate);

        Assert.Contains("SnapshotStagingDirectory", ex.Message);
    }

    [Fact]
    public void Validate_NullSnapshotStagingDirectory_KeepsInMemoryStaging()
    {
        RaftConfiguration cfg = new() { SnapshotStagingDirectory = null };

        cfg.Validate();
    }

    // ── Sender budgets vs gRPC receive limit ──────────────────────────────────

    [Fact]
    public void Validate_SenderBudgetsExactlyAtReceiveLimit_DoesNotThrow()
    {
        // The guard is strict: only a budget ABOVE the receive limit is refused, because a
        // frame at exactly the limit still fits the peer's MaxReceiveMessageSize.
        RaftConfiguration cfg = new()
        {
            GrpcMaxMessageBytes = 4 * 1024 * 1024,
            MaxOutboundBatchBytes = 4 * 1024 * 1024,
            MaxBackfillBytesPerRound = 4 * 1024 * 1024
        };

        cfg.Validate();
    }

    [Fact]
    public void Validate_OutboundBatchBytesOneByteAboveReceiveLimit_Throws()
    {
        RaftConfiguration cfg = new()
        {
            GrpcMaxMessageBytes = 4 * 1024 * 1024,
            MaxOutboundBatchBytes = 4L * 1024 * 1024 + 1
        };

        RaftException ex = Assert.Throws<RaftException>(cfg.Validate);

        Assert.Contains("MaxOutboundBatchBytes", ex.Message);
    }

    [Fact]
    public void Validate_BackfillBudgetOneByteAboveReceiveLimit_Throws()
    {
        RaftConfiguration cfg = new()
        {
            GrpcMaxMessageBytes = 4 * 1024 * 1024,
            MaxOutboundBatchBytes = 1024 * 1024,
            MaxBackfillBytesPerRound = 4 * 1024 * 1024 + 1
        };

        RaftException ex = Assert.Throws<RaftException>(cfg.Validate);

        Assert.Contains("MaxBackfillBytesPerRound", ex.Message);
    }

    [Fact]
    public void Validate_NegativeGrpcMaxMessageBytes_Throws()
    {
        RaftConfiguration cfg = new() { GrpcMaxMessageBytes = -1 };

        RaftException ex = Assert.Throws<RaftException>(cfg.Validate);

        Assert.Contains("GrpcMaxMessageBytes", ex.Message);
    }

    // ── Heartbeat de-dup window ───────────────────────────────────────────────

    [Fact]
    public void Validate_RecentHeartbeatEqualToHeartbeatInterval_Throws()
    {
        // The skip is strictly-less-than, so a window equal to the cadence skips every
        // timer-driven heartbeat round — the boundary itself is invalid.
        RaftConfiguration cfg = new()
        {
            RecentHeartbeat = TimeSpan.FromMilliseconds(500),
            HeartbeatInterval = TimeSpan.FromMilliseconds(500)
        };

        RaftException ex = Assert.Throws<RaftException>(cfg.Validate);

        Assert.Contains("RecentHeartbeat", ex.Message);
        Assert.Contains("HeartbeatInterval", ex.Message);
    }

    [Fact]
    public void Validate_RecentHeartbeatJustBelowHeartbeatInterval_DoesNotThrow()
    {
        RaftConfiguration cfg = new()
        {
            RecentHeartbeat = TimeSpan.FromMilliseconds(499),
            HeartbeatInterval = TimeSpan.FromMilliseconds(500)
        };

        cfg.Validate();
    }

    [Fact]
    public void Validate_ZeroRecentHeartbeat_DisablesTheWindow()
    {
        RaftConfiguration cfg = new()
        {
            RecentHeartbeat = TimeSpan.Zero,
            HeartbeatInterval = TimeSpan.FromMilliseconds(500)
        };

        cfg.Validate();
    }

    // ── Slow-node threshold boundary ──────────────────────────────────────────

    [Fact]
    public void Validate_SlowNodeMultiplierJustAboveOne_DoesNotThrow()
    {
        RaftConfiguration cfg = new() { SlowNodeMultiplier = 1.000001 };

        cfg.Validate();
    }

    [Fact]
    public void Validate_SlowNodeMultiplierBelowOne_Throws()
    {
        RaftConfiguration cfg = new() { SlowNodeMultiplier = 0.5 };

        RaftException ex = Assert.Throws<RaftException>(cfg.Validate);

        Assert.Contains("SlowNodeMultiplier", ex.Message);
    }

    // ── Zone hint normalization ───────────────────────────────────────────────

    [Theory]
    [InlineData(null, null)]
    [InlineData("", null)]
    [InlineData("   ", null)]
    [InlineData("\t", null)]
    [InlineData("zone-1", "zone-1")]
    [InlineData("  zone-1  ", "zone-1")]
    [InlineData(" zone-1", "zone-1")]
    public void Validate_NormalizesZoneHint(string? configured, string? expected)
    {
        // The placement planner guards with IsNullOrEmpty, so the canonical form of "no zone"
        // is null: a whitespace-only value must not survive validation looking configured.
        RaftConfiguration cfg = new() { Zone = configured };

        cfg.Validate();

        Assert.Equal(expected, cfg.Zone);
    }

    // ── Cached transport authenticator ────────────────────────────────────────

    [Fact]
    public void GetTransportAuthenticator_BuildsOnceAndCaches()
    {
        RaftConfiguration cfg = new()
        {
            TransportSecurity = new RaftTransportSecurityOptions
            {
                NodeAuthenticationMode = RaftNodeAuthenticationMode.SharedSecret,
                SharedSecret = "first-secret"
            }
        };

        RaftTransportAuthenticator first = cfg.GetTransportAuthenticator();
        RaftTransportAuthenticator second = cfg.GetTransportAuthenticator();

        Assert.Same(first, second);
        Assert.Equal("first-secret", first.Options.SharedSecret);
    }

    [Fact]
    public void GetTransportAuthenticator_AfterMutationWithoutInvalidate_KeepsStaleCache()
    {
        // There is intentionally no automatic invalidation: a security-sensitive object is
        // replaced explicitly and atomically. A silent rebuild would mask when the swap took.
        RaftConfiguration cfg = new()
        {
            TransportSecurity = new RaftTransportSecurityOptions
            {
                NodeAuthenticationMode = RaftNodeAuthenticationMode.SharedSecret,
                SharedSecret = "first-secret"
            }
        };

        RaftTransportAuthenticator cached = cfg.GetTransportAuthenticator();
        cfg.TransportSecurity.SharedSecret = "rotated-secret";

        Assert.Same(cached, cfg.GetTransportAuthenticator());
        Assert.Equal("first-secret", cfg.GetTransportAuthenticator().Options.SharedSecret);
    }

    [Fact]
    public void InvalidateTransportAuthenticator_ForcesRebuildFromCurrentSecurity()
    {
        RaftConfiguration cfg = new()
        {
            TransportSecurity = new RaftTransportSecurityOptions
            {
                NodeAuthenticationMode = RaftNodeAuthenticationMode.SharedSecret,
                SharedSecret = "first-secret"
            }
        };

        RaftTransportAuthenticator stale = cfg.GetTransportAuthenticator();
        cfg.TransportSecurity.SharedSecret = "rotated-secret";

        cfg.InvalidateTransportAuthenticator();

        RaftTransportAuthenticator rebuilt = cfg.GetTransportAuthenticator();
        Assert.NotSame(stale, rebuilt);
        Assert.Equal("rotated-secret", rebuilt.Options.SharedSecret);
    }

    [Fact]
    public void InvalidateTransportAuthenticator_Repeatedly_IsHarmless()
    {
        RaftConfiguration cfg = new();

        cfg.InvalidateTransportAuthenticator();
        cfg.InvalidateTransportAuthenticator();

        Assert.NotNull(cfg.GetTransportAuthenticator());
    }
}
