using System.Reflection;
using Kommander;
using Microsoft.Extensions.Logging;

namespace Kommander.Tests;

/// <summary>
/// Covers <see cref="RaftSafetyOptionAudit"/> (fragility analysis, recommendation 5).
///
/// <para>Two jobs. First, pin the classification: every public option on
/// <see cref="RaftConfiguration"/> must be classified, so a new option cannot be added without
/// someone deciding whether it is a safety fence, a liveness bound, tuning, deployment, or
/// diagnostics. Nothing in C# can force that decision at the declaration, so this test is the
/// enforcement point — a new option fails here until it is classified.</para>
///
/// <para>Second, check that each disabled fence is actually reported. A fence that an option can
/// turn off will eventually run off somewhere; what makes that recoverable is the node saying so
/// at startup instead of at the incident.</para>
/// </summary>
public sealed class TestRaftSafetyOptionAudit
{
    private static IEnumerable<PropertyInfo> PublicOptions() =>
        typeof(RaftConfiguration)
            .GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.GetMethod is not null && p.GetMethod.IsPublic);

    [Fact]
    public void EveryPublicOptionIsClassified()
    {
        List<string> unclassified = PublicOptions()
            .Select(p => p.Name)
            .Where(name => !RaftSafetyOptionAudit.Classification.ContainsKey(name))
            .OrderBy(name => name, StringComparer.Ordinal)
            .ToList();

        Assert.True(
            unclassified.Count == 0,
            "These RaftConfiguration options are not classified in RaftSafetyOptionAudit.Classification: "
            + string.Join(", ", unclassified)
            + ". Decide what each option is for (Safety, Liveness, Performance, Deployment, "
            + "Diagnostics, Derived) and add it to the table.");
    }

    [Fact]
    public void TheClassificationNamesNoOptionThatNoLongerExists()
    {
        HashSet<string> live = PublicOptions().Select(p => p.Name).ToHashSet(StringComparer.Ordinal);

        List<string> stale = RaftSafetyOptionAudit.Classification.Keys
            .Where(name => !live.Contains(name))
            .OrderBy(name => name, StringComparer.Ordinal)
            .ToList();

        Assert.True(
            stale.Count == 0,
            "These names are classified but no longer exist on RaftConfiguration: " + string.Join(", ", stale));
    }

    [Fact]
    public void EveryDerivedOptionIsReadOnly()
    {
        foreach (PropertyInfo option in PublicOptions())
        {
            if (RaftSafetyOptionAudit.Classification[option.Name] != RaftOptionKind.Derived)
                continue;

            Assert.False(
                option.CanWrite,
                $"{option.Name} is classified Derived but has a setter; classify it as a real option instead.");
        }
    }

    [Fact]
    public void ADefaultConfigurationReportsOnlyShippedDefaultDeviations()
    {
        RaftConfiguration configuration = new() { Host = "localhost", Port = 8001 };

        IReadOnlyList<RaftSafetyOptionDeviation> deviations = configuration.GetSafetyOptionDeviations();

        Assert.All(deviations, d => Assert.True(
            d.IsShippedDefault,
            $"{d.Option} is reported as a chosen deviation, but nothing was set on this configuration."));

        // The two fences that ship off. Each is a deliberate compatibility choice, and each is
        // reported at Information so an operator can see it without being warned every startup.
        // Check-quorum ships ON: an isolated leader that keeps acknowledging writes is the
        // two-leader window behind lost acknowledged writes, so leaving it off is a chosen deviation.
        Assert.DoesNotContain(deviations, d => d.Option == nameof(RaftConfiguration.EnableCheckQuorum));
        Assert.Contains(deviations, d => d.Option == nameof(RaftConfiguration.ApplicationDurabilityProvider));
        Assert.Contains(deviations, d => d.Option == "TransportSecurity.NodeAuthenticationMode");
    }

    [Fact]
    public void ADisabledFenceIsReportedAsAChosenDeviation()
    {
        RaftConfiguration configuration = new()
        {
            Host = "localhost",
            Port = 8001,
            BackfillEnabled = false,
            EnableCheckQuorum = false,
            AllowLegacySnapshotSenders = true,
            SnapshotRescueMaxConsecutiveCycles = 0,
            CompactionLiveReplicaLagBudget = 0,
            MaxOutboundQueueBytesPerPeer = 0,
        };
        configuration.TransportSecurity.RequireTls = false;
        configuration.TransportSecurity.AllowInsecureCertificateValidation = true;

        IReadOnlyList<RaftSafetyOptionDeviation> deviations = configuration.GetSafetyOptionDeviations();

        string[] expected =
        [
            "TransportSecurity.RequireTls",
            "TransportSecurity.AllowInsecureCertificateValidation",
            nameof(RaftConfiguration.AllowLegacySnapshotSenders),
            nameof(RaftConfiguration.EnableCheckQuorum),
            nameof(RaftConfiguration.BackfillEnabled),
            nameof(RaftConfiguration.SnapshotRescueMaxConsecutiveCycles),
            nameof(RaftConfiguration.CompactionLiveReplicaLagBudget),
            nameof(RaftConfiguration.MaxOutboundQueueBytesPerPeer),
        ];

        foreach (string option in expected)
        {
            RaftSafetyOptionDeviation deviation = Assert.Single(deviations, d => d.Option == option);
            Assert.False(deviation.IsShippedDefault, $"{option} was set explicitly, so it is not a shipped default.");
            Assert.NotEmpty(deviation.Hazard);
            Assert.NotEmpty(deviation.SafeValue);
        }
    }

    [Fact]
    public void EveryReportedDeviationIsASafetyOrLivenessConcern()
    {
        RaftConfiguration configuration = new() { Host = "localhost", Port = 8001, BackfillEnabled = false };

        Assert.All(
            configuration.GetSafetyOptionDeviations(),
            d => Assert.True(
                d.Kind is RaftOptionKind.Safety or RaftOptionKind.Liveness,
                $"{d.Option} is reported as a fence but classified {d.Kind}; only Safety and Liveness belong here."));
    }

    [Fact]
    public void EveryOptionTheAuditReportsIsClassifiedTheSameWay()
    {
        // The report and the table must agree. A nested option (TransportSecurity.X) is classified
        // through its owning property, so it is checked against that entry.
        RaftConfiguration configuration = new() { Host = "localhost", Port = 8001, BackfillEnabled = false };

        foreach (RaftSafetyOptionDeviation deviation in configuration.GetSafetyOptionDeviations())
        {
            string owner = deviation.Option.Split('.')[0];

            Assert.True(
                RaftSafetyOptionAudit.Classification.TryGetValue(owner, out RaftOptionKind kind),
                $"{owner} is reported by the audit but missing from the classification table.");

            if (deviation.Option == owner)
                Assert.Equal(kind, deviation.Kind);
        }
    }

    private const string WitnessReason = "the other voters are in-process witnesses that store nothing";

    /// <summary>
    /// A standalone node with witness voters: check-quorum and backfill are off on purpose and
    /// marked, while RequireTls is off without a mark.
    /// </summary>
    private static RaftConfiguration WitnessTopologyConfiguration()
    {
        RaftConfiguration configuration = new()
        {
            Host = "localhost",
            Port = 8001,
            EnableCheckQuorum = false,
            BackfillEnabled = false,
            IntendedFenceDeviations = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                [nameof(RaftConfiguration.EnableCheckQuorum)] = WitnessReason,
                [nameof(RaftConfiguration.BackfillEnabled)] = WitnessReason,
            },
        };
        configuration.TransportSecurity.RequireTls = false;
        return configuration;
    }

    [Fact]
    public void AMarkedDeviationCarriesItsReason_AndStaysInTheReport()
    {
        IReadOnlyList<RaftSafetyOptionDeviation> deviations =
            WitnessTopologyConfiguration().GetSafetyOptionDeviations();

        RaftSafetyOptionDeviation checkQuorum =
            Assert.Single(deviations, d => d.Option == nameof(RaftConfiguration.EnableCheckQuorum));
        RaftSafetyOptionDeviation backfill =
            Assert.Single(deviations, d => d.Option == nameof(RaftConfiguration.BackfillEnabled));
        RaftSafetyOptionDeviation requireTls =
            Assert.Single(deviations, d => d.Option == "TransportSecurity.RequireTls");

        Assert.Equal(WitnessReason, checkQuorum.IntendedReason);
        Assert.Equal(WitnessReason, backfill.IntendedReason);
        Assert.Null(requireTls.IntendedReason);

        // The mark changes nothing else about the entry.
        Assert.False(checkQuorum.IsShippedDefault);
        Assert.Equal("false", checkQuorum.CurrentValue);
        Assert.Equal("true", checkQuorum.SafeValue);
    }

    [Fact]
    public void AMarkOnAFenceThatIsOn_ReportsNoDeviationForIt()
    {
        RaftConfiguration configuration = new()
        {
            Host = "localhost",
            Port = 8001,
            IntendedFenceDeviations = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                [nameof(RaftConfiguration.EnableCheckQuorum)] = WitnessReason,
            },
        };

        Assert.DoesNotContain(
            configuration.GetSafetyOptionDeviations(),
            d => d.Option == nameof(RaftConfiguration.EnableCheckQuorum));
    }

    [Fact]
    public void MarkedDeviationsLogAtInformationWithTheReason_UnmarkedChosenOnesStillWarn()
    {
        CapturingLogger logger = new();

        RaftManager.LogSafetyOptionDeviations(WitnessTopologyConfiguration(), logger);

        foreach (string option in new[] { "EnableCheckQuorum=false", "BackfillEnabled=false" })
        {
            (LogLevel Level, string Message) line = Assert.Single(logger.Messages, m => m.Message.Contains(option));
            Assert.Equal(LogLevel.Information, line.Level);
            Assert.Contains("fence off by host intent", line.Message);
            Assert.Contains("Host reason: " + WitnessReason, line.Message);
        }

        (LogLevel Level, string Message) tls =
            Assert.Single(logger.Messages, m => m.Message.Contains("TransportSecurity.RequireTls=false"));
        Assert.Equal(LogLevel.Warning, tls.Level);
        Assert.DoesNotContain("Host reason", tls.Message);
    }

    [Fact]
    public void AMarkOnAShippedDefaultDeviation_LogsItsReason()
    {
        // Peer authentication ships off and already logs at Information. Marking it adds the reason.
        RaftConfiguration configuration = new()
        {
            Host = "localhost",
            Port = 8001,
            IntendedFenceDeviations = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["TransportSecurity.NodeAuthenticationMode"] = "the node listens on loopback only",
            },
        };
        CapturingLogger logger = new();

        RaftManager.LogSafetyOptionDeviations(configuration, logger);

        (LogLevel Level, string Message) line =
            Assert.Single(logger.Messages, m => m.Message.Contains("TransportSecurity.NodeAuthenticationMode"));
        Assert.Equal(LogLevel.Information, line.Level);
        Assert.Contains("Host reason: the node listens on loopback only", line.Message);
    }

    [Fact]
    public void Validate_AcceptsAMarkForEveryOptionTheAuditReports()
    {
        Dictionary<string, string> marks = new(StringComparer.Ordinal);
        foreach (string option in RaftSafetyOptionAudit.ReportableOptions)
            marks[option] = "reason for " + option;

        RaftConfiguration configuration = new() { IntendedFenceDeviations = marks };

        configuration.Validate();
    }

    [Theory]
    [InlineData("EnableCheckquorum")]
    [InlineData("RequireTls")]
    [InlineData(nameof(RaftConfiguration.HeartbeatInterval))]
    public void Validate_AMarkTheAuditNeverReports_Throws(string option)
    {
        RaftConfiguration configuration = new()
        {
            IntendedFenceDeviations = new Dictionary<string, string>(StringComparer.Ordinal) { [option] = WitnessReason },
        };

        RaftException ex = Assert.Throws<RaftException>(configuration.Validate);
        Assert.Contains($"'{option}'", ex.Message);
    }

    [Fact]
    public void Validate_AMarkWithACaseInsensitiveMapStillNeedsTheExactName()
    {
        // The audit looks names up ordinally, so a key that only matches ignoring case would never
        // lower the Warning. Validate checks the spelling whatever comparer the host's map uses.
        RaftConfiguration configuration = new()
        {
            IntendedFenceDeviations = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["enablecheckquorum"] = WitnessReason,
            },
        };

        Assert.Throws<RaftException>(configuration.Validate);
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    public void Validate_AMarkWithABlankReason_Throws(string reason)
    {
        RaftConfiguration configuration = new()
        {
            IntendedFenceDeviations = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                [nameof(RaftConfiguration.BackfillEnabled)] = reason,
            },
        };

        RaftException ex = Assert.Throws<RaftException>(configuration.Validate);
        Assert.Contains(nameof(RaftConfiguration.BackfillEnabled), ex.Message);
    }

    [Fact]
    public void Validate_ANullMarkMap_Throws_AndTheAuditReadsItAsEmpty()
    {
        RaftConfiguration configuration = new() { BackfillEnabled = false, IntendedFenceDeviations = null! };

        Assert.Throws<RaftException>(configuration.Validate);

        // The audit runs on the startup log path and must not throw.
        RaftSafetyOptionDeviation backfill = Assert.Single(
            configuration.GetSafetyOptionDeviations(),
            d => d.Option == nameof(RaftConfiguration.BackfillEnabled));
        Assert.Null(backfill.IntendedReason);
    }

    /// <summary>Records each log line with its level.</summary>
    private sealed class CapturingLogger : ILogger<IRaft>
    {
        public List<(LogLevel Level, string Message)> Messages { get; } = [];

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => logLevel != LogLevel.None;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter) =>
            Messages.Add((logLevel, formatter(state, exception)));
    }
}
