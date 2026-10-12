using Kommander;

namespace Kommander.Tests;

/// <summary>
/// The remaining fence families of <see cref="RaftSafetyOptionAudit"/>: the liveness fences whose
/// weakened values are valid configuration (zero disables them) must each be reported as a chosen
/// deviation, a configuration with every fence this audit knows about turned on must report
/// nothing, Safety concerns must be reported ahead of Liveness ones, and a null configuration is
/// rejected rather than crashing the startup log path.
/// </summary>
public sealed class TestRaftSafetyOptionAuditWeakenedFences
{
    /// <summary>
    /// Each of these options accepts a "disabled" value that Validate allows: zero (or a zero
    /// time window) turns the fence off without being an error. The audit must still report it —
    /// the deployment's own logs are the only place the choice is visible.
    /// </summary>
    [Fact]
    public void RetentionAndRescueFences_WeakenedToTheirOffValue_AreReportedAsChosenDeviations()
    {
        RaftConfiguration configuration = new()
        {
            Host = "localhost",
            Port = 8001,
            SnapshotRescueProbeInterval = TimeSpan.Zero,
            SnapshotExportRetryCacheMaxBytes = 0,
            CompactionSilentPeerRetentionWindow = TimeSpan.Zero,
        };

        IReadOnlyList<RaftSafetyOptionDeviation> deviations = RaftSafetyOptionAudit.Inspect(configuration);

        string[] weakened =
        [
            nameof(RaftConfiguration.SnapshotRescueProbeInterval),
            nameof(RaftConfiguration.SnapshotExportRetryCacheMaxBytes),
            nameof(RaftConfiguration.CompactionSilentPeerRetentionWindow),
        ];

        foreach (string option in weakened)
        {
            RaftSafetyOptionDeviation deviation = Assert.Single(deviations, d => d.Option == option);
            Assert.Equal(RaftOptionKind.Liveness, deviation.Kind);
            Assert.False(deviation.IsShippedDefault, $"{option} was weakened explicitly, not by default");
            Assert.NotEmpty(deviation.Hazard);
            Assert.NotEmpty(deviation.SafeValue);
        }
    }

    /// <summary>
    /// A configuration that turns on every fence the audit knows about reports nothing: the empty
    /// result is the audit's "every fence is on" answer and must be reachable, not just promised.
    /// </summary>
    [Fact]
    public void AConfigurationWithEveryFenceOn_ReportsNoDeviations()
    {
        RaftConfiguration configuration = new()
        {
            Host = "localhost",
            Port = 8001,
            EnableCheckQuorum = true,
            ApplicationDurabilityProvider = new FixedDurabilityFloorProvider(-1),
        };
        configuration.TransportSecurity.NodeAuthenticationMode = RaftNodeAuthenticationMode.SharedSecret;
        configuration.TransportSecurity.RequireTls = true;

        Assert.Empty(RaftSafetyOptionAudit.Inspect(configuration));
    }

    /// <summary>
    /// The report is ordered most-protective-concern-first: every Safety deviation precedes every
    /// Liveness deviation, so an operator scanning the top of the startup output sees the fences
    /// whose absence can lose data before the ones whose absence can wedge progress.
    /// </summary>
    [Fact]
    public void TheReportListsSafetyConcernsBeforeLivenessOnes()
    {
        RaftConfiguration configuration = new()
        {
            Host = "localhost",
            Port = 8001,
            EnableCheckQuorum = false,
            BackfillEnabled = false,
        };
        configuration.TransportSecurity.RequireTls = false;

        IReadOnlyList<RaftSafetyOptionDeviation> deviations = RaftSafetyOptionAudit.Inspect(configuration);

        Assert.Contains(deviations, d => d.Kind == RaftOptionKind.Safety && !d.IsShippedDefault);
        Assert.Contains(deviations, d => d.Kind == RaftOptionKind.Liveness);

        int firstLiveness = IndexOfKind(deviations, RaftOptionKind.Liveness);
        for (int i = 0; i < deviations.Count; i++)
        {
            if (deviations[i].Kind == RaftOptionKind.Liveness)
                Assert.True(i >= firstLiveness);
            else
                Assert.True(i < firstLiveness,
                    $"{deviations[i].Option} (Safety) must be reported before the first Liveness deviation");
        }
    }

    /// <summary>A null configuration is a caller bug, not an empty report: it must throw.</summary>
    [Fact]
    public void Inspect_NullConfiguration_Throws()
    {
        Assert.Throws<ArgumentNullException>(() => RaftSafetyOptionAudit.Inspect(null!));
    }

    private static int IndexOfKind(IReadOnlyList<RaftSafetyOptionDeviation> deviations, RaftOptionKind kind)
    {
        for (int i = 0; i < deviations.Count; i++)
        {
            if (deviations[i].Kind == kind)
                return i;
        }

        return deviations.Count;
    }

    /// <summary>
    /// A durability provider answering "no opinion", which is the legitimate starting state of
    /// every consumer: the audit only cares that a provider is configured.
    /// </summary>
    private sealed class FixedDurabilityFloorProvider(long floor) : IApplicationDurabilityProvider
    {
        public long GetDurablyAppliedIndex(int partitionId) => floor;
    }
}
