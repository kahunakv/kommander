
using Kommander.System;
using Kommander.Time;

namespace Kommander.Tests.LoadReports;

/// <summary>
/// <see cref="LoadReportStore.GetAll"/> caches its snapshot per store version. These tests pin the two
/// properties that caching must not break: a reader never sees a report that a later mutation added or
/// removed under it, and the shared result cannot be mutated by one caller on behalf of the others.
/// </summary>
public sealed class TestLoadReportStoreSnapshot
{
    private static RaftSystemRequest ReportRequest(string endpoint, long version, HLCTimestamp time) =>
        new(new NodeLoadReport
        {
            Endpoint = endpoint,
            ReportVersion = version,
            Time = time,
        });

    private static HLCTimestamp At(long milliseconds) => new(1, milliseconds, 0);

    [Fact]
    public void RepeatedReads_ReuseOneSnapshot()
    {
        LoadReportStore store = new();
        store.Apply(ReportRequest("node-a:5000", 1, At(100)));

        IReadOnlyList<NodeLoadReport> first = store.GetAll();
        IReadOnlyList<NodeLoadReport> second = store.GetAll();

        Assert.Same(first, second);
        Assert.Single(first);
    }

    [Fact]
    public void AnAppliedReport_InvalidatesTheSnapshot()
    {
        LoadReportStore store = new();
        store.Apply(ReportRequest("node-a:5000", 1, At(100)));

        IReadOnlyList<NodeLoadReport> before = store.GetAll();

        store.Apply(ReportRequest("node-b:5001", 1, At(100)));

        IReadOnlyList<NodeLoadReport> after = store.GetAll();

        Assert.NotSame(before, after);
        Assert.Single(before);
        Assert.Equal(2, after.Count);
    }

    /// <summary>
    /// A report that loses the version check changes nothing, so the previously published snapshot stays
    /// correct — but it must not be observably wrong either, which is what this asserts.
    /// </summary>
    [Fact]
    public void AStaleReport_LeavesTheContentsUnchanged()
    {
        LoadReportStore store = new();
        store.Apply(ReportRequest("node-a:5000", 5, At(100)));

        store.Apply(ReportRequest("node-a:5000", 2, At(200)));

        IReadOnlyList<NodeLoadReport> reports = store.GetAll();

        Assert.Single(reports);
        Assert.Equal(5, reports[0].ReportVersion);
    }

    [Fact]
    public void Eviction_InvalidatesTheSnapshot()
    {
        LoadReportStore store = new();
        store.Apply(ReportRequest("node-a:5000", 1, At(0)));
        store.Apply(ReportRequest("node-b:5001", 1, At(100_000)));

        Assert.Equal(2, store.GetAll().Count);

        store.EvictStale(TimeSpan.FromSeconds(1), At(100_000));

        IReadOnlyList<NodeLoadReport> after = store.GetAll();

        Assert.Single(after);
        Assert.Equal("node-b:5001", after[0].Endpoint);
    }

    /// <summary>
    /// The snapshot reaches many callers now, so a caller that reached through the interface to the
    /// backing collection would corrupt every other reader rather than only its own copy.
    /// </summary>
    [Fact]
    public void TheSharedSnapshot_IsNotMutableThroughItsInterface()
    {
        LoadReportStore store = new();
        store.Apply(ReportRequest("node-a:5000", 1, At(100)));

        IReadOnlyList<NodeLoadReport> reports = store.GetAll();

        Assert.IsNotType<NodeLoadReport[]>(reports);
        Assert.IsNotType<List<NodeLoadReport>>(reports);

        ICollection<NodeLoadReport> asCollection = Assert.IsAssignableFrom<ICollection<NodeLoadReport>>(reports);
        Assert.True(asCollection.IsReadOnly);
    }

    [Fact]
    public void EmptyStore_ReturnsAnEmptySnapshot()
    {
        LoadReportStore store = new();

        Assert.Empty(store.GetAll());
    }
}
