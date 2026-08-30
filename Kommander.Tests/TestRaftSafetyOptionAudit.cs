using System.Reflection;
using Kommander;

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

        // The three fences that ship off. Each is a deliberate compatibility choice, and each is
        // reported at Information so an operator can see it without being warned every startup.
        Assert.Contains(deviations, d => d.Option == nameof(RaftConfiguration.EnableCheckQuorum));
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
}
