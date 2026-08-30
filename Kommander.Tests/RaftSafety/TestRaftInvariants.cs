using System.Diagnostics.Metrics;
using Kommander.Consensus;
using Kommander.Diagnostics;

namespace Kommander.Tests.RaftSafety;

/// <summary>
/// Covers the always-on consensus invariant checks (fragility analysis, recommendation 2).
///
/// <para>The checks live in <see cref="RaftPartitionCoreState"/>, at the one place each value is
/// written, so these tests drive that type directly rather than a cluster. What is under test is
/// the rule and its declared exceptions, not the protocol: a regression of the term or of either
/// frontier is reported, and each declared regression — the demotion reset, the promotion anchor,
/// the test-only setter — is silent.</para>
///
/// <para>In the <c>Cluster integration</c> collection because
/// <see cref="RaftInvariants.Policy"/> is process-wide. Lowering it while a cluster test runs
/// would silence a real violation in that test, so these cases must not run beside one.</para>
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
public sealed class TestRaftInvariants : IDisposable
{
    private readonly RaftInvariantPolicy originalPolicy = RaftInvariants.Policy;

    public void Dispose() => RaftInvariants.Policy = originalPolicy;

    private static RaftPartitionCoreState NewState()
    {
        RaftPartitionCoreState state = new();
        state.SetIdentity(7, "node-a");
        return state;
    }

    // ── Term (Raft §5.1) ──────────────────────────────────────────────────────

    [Fact]
    public void TermMayRise()
    {
        RaftInvariants.Policy = RaftInvariantPolicy.Throw;
        RaftPartitionCoreState state = NewState();

        state.CurrentTerm = 1;
        state.CurrentTerm = 2;
        state.CurrentTerm = 2;

        Assert.Equal(2, state.CurrentTerm);
    }

    [Fact]
    public void TermMayNotFall()
    {
        RaftInvariants.Policy = RaftInvariantPolicy.Throw;
        RaftPartitionCoreState state = NewState();
        state.CurrentTerm = 5;

        RaftInvariantViolationException ex =
            Assert.Throws<RaftInvariantViolationException>(() => state.CurrentTerm = 4);

        Assert.Equal(RaftInvariants.TermMonotonic, ex.Invariant);
        Assert.Contains("node-a/7", ex.Message);
    }

    // ── Committed frontier ────────────────────────────────────────────────────

    [Fact]
    public void CommittedFrontierMayNotRegress()
    {
        RaftInvariants.Policy = RaftInvariantPolicy.Throw;
        RaftPartitionCoreState state = NewState();
        state.LocalCommittedIndex = 10;

        RaftInvariantViolationException ex =
            Assert.Throws<RaftInvariantViolationException>(() => state.LocalCommittedIndex = 9);

        Assert.Equal(RaftInvariants.CommittedFrontierMonotonic, ex.Invariant);
    }

    [Fact]
    public void DemotionResetIsADeclaredRegression()
    {
        RaftInvariants.Policy = RaftInvariantPolicy.Throw;
        RaftPartitionCoreState state = NewState();
        state.LocalCommittedIndex = 10;

        state.ResetLocalCommittedIndexOnDemotion();

        Assert.Equal(-1, state.LocalCommittedIndex);
    }

    [Fact]
    public void PromotionAnchorIsADeclaredRegression()
    {
        RaftInvariants.Policy = RaftInvariantPolicy.Throw;
        RaftPartitionCoreState state = NewState();
        state.LocalCommittedIndex = 10;

        state.SeedLocalCommittedIndexOnPromotion(4);

        Assert.Equal(4, state.LocalCommittedIndex);
    }

    // ── Applied frontier ──────────────────────────────────────────────────────

    [Fact]
    public void AppliedFrontierMayNotRegress()
    {
        RaftInvariants.Policy = RaftInvariantPolicy.Throw;
        RaftPartitionCoreState state = NewState();
        state.LastAppliedIndex = 12;

        RaftInvariantViolationException ex =
            Assert.Throws<RaftInvariantViolationException>(() => state.LastAppliedIndex = 11);

        Assert.Equal(RaftInvariants.AppliedFrontierMonotonic, ex.Invariant);
    }

    // ── Policy ────────────────────────────────────────────────────────────────

    [Fact]
    public void LogPolicyRecordsTheViolationAndLetsTheCallerContinue()
    {
        RaftInvariants.Policy = RaftInvariantPolicy.Log;

        long observed = 0;
        string? observedInvariant = null;

        using MeterListener listener = new();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == KommanderMetrics.MeterName
                && instrument.Name == "raft.invariant.violations_total")
                l.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>((_, measurement, tags, _) =>
        {
            Interlocked.Add(ref observed, measurement);
            foreach (KeyValuePair<string, object?> tag in tags)
            {
                if (tag.Key == "invariant")
                    observedInvariant = tag.Value as string;
            }
        });
        listener.Start();

        RaftPartitionCoreState state = NewState();
        state.LastAppliedIndex = 12;
        state.LastAppliedIndex = 11;

        listener.RecordObservableInstruments();

        Assert.Equal(1, Interlocked.Read(ref observed));
        Assert.Equal(RaftInvariants.AppliedFrontierMonotonic, observedInvariant);

        // The write still lands: Log policy reports, it does not repair.
        Assert.Equal(11, state.LastAppliedIndex);
    }

    [Fact]
    public void OffPolicyRecordsNothing()
    {
        RaftInvariants.Policy = RaftInvariantPolicy.Off;

        long observed = 0;

        using MeterListener listener = new();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == KommanderMetrics.MeterName
                && instrument.Name == "raft.invariant.violations_total")
                l.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>((_, measurement, _, _) => Interlocked.Add(ref observed, measurement));
        listener.Start();

        RaftPartitionCoreState state = NewState();
        state.LastAppliedIndex = 12;
        state.LastAppliedIndex = 11;

        Assert.Equal(0, Interlocked.Read(ref observed));
    }

    [Fact]
    public void DefaultPolicyThrowsInADebugBuild()
    {
#if DEBUG
        Assert.Equal(RaftInvariantPolicy.Throw, RaftInvariants.DefaultPolicy);
#else
        Assert.Equal(RaftInvariantPolicy.Log, RaftInvariants.DefaultPolicy);
#endif
    }
}
