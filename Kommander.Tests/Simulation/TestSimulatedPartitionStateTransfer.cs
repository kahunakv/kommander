using Kommander.Tests.Simulation.Cluster;

namespace Kommander.Tests.Simulation;

/// <summary>
/// The simulated state transfer on its own, with no cluster.
///
/// <para><b>Why the fault is tested from both sides.</b> A hang that did not really hang would let
/// the rescue sweep pass while testing nothing. A hang that ended on cancellation would be ended by
/// the library's own timeout, and the defect class it exists for — a transfer nothing can end —
/// would stay out of reach. A hang that outlived its release would leave detached tasks behind for
/// the next test.</para>
/// </summary>
[Trait("Category", "DSTSmoke")]
public sealed class TestSimulatedPartitionStateTransfer
{
    private const int PartitionId = 1;

    /// <summary>With no fault, an export round-trips through an import. This is the rescue path.</summary>
    [Fact]
    public async Task Default_ExportsASnapshotThatImports()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;
        SimulatedPartitionStateTransfer transfer = new();

        Stream snapshot = await transfer.ExportPartitionState(PartitionId, upToIndex: 11, cancellationToken);
        await transfer.ImportPartitionState(PartitionId, snapshot, cancellationToken);

        Assert.Equal(1, transfer.ExportsServed);
        Assert.Equal(0, transfer.ExportsHung);
    }

    /// <summary>
    /// An armed hang takes exactly as many exports as it was armed for, and ignores cancellation.
    /// The export after it is served normally.
    /// </summary>
    [Fact]
    public async Task HangNextExports_HangsThatManyExports_AndIgnoresCancellation()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;
        SimulatedPartitionStateTransfer transfer = new();
        transfer.HangNextExports(1);

        using CancellationTokenSource cancelled = new();
        await cancelled.CancelAsync();

        Task<Stream> hung = transfer.ExportPartitionState(PartitionId, upToIndex: 11, cancelled.Token);

        // A short real wait is enough to show the task does not complete on its own; a cancelled
        // token was passed, so a token-honouring export would already have finished.
        await Task.Delay(50, cancellationToken);
        Assert.False(hung.IsCompleted);

        Stream served = await transfer.ExportPartitionState(PartitionId, upToIndex: 11, cancellationToken);
        Assert.NotNull(served);

        Assert.Equal(1, transfer.ExportsHung);
        Assert.Equal(1, transfer.ExportsServed);
        Assert.Equal(0, transfer.ExportsArmedToHang);

        transfer.ReleaseHungExports();
    }

    /// <summary>
    /// A release ends every hung export with an exception and disarms any hang still waiting. This
    /// is what a crash and a teardown do; a runner never calls it to heal a run.
    /// </summary>
    [Fact]
    public async Task ReleaseHungExports_EndsTheHungExports_AndDisarms()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;
        SimulatedPartitionStateTransfer transfer = new();
        transfer.HangNextExports(3);

        Task<Stream> hung = transfer.ExportPartitionState(PartitionId, upToIndex: 11, cancellationToken);

        transfer.ReleaseHungExports();

        await Assert.ThrowsAsync<InvalidOperationException>(() => hung);
        Assert.Equal(0, transfer.ExportsArmedToHang);

        Stream served = await transfer.ExportPartitionState(PartitionId, upToIndex: 11, cancellationToken);
        Assert.NotNull(served);
        Assert.Equal(1, transfer.ExportsServed);
    }
}
