namespace Kommander.Tests.Simulation.Cluster;

/// <summary>
/// The application-state transfer every simulated node registers, so a follower that falls below
/// the leader's WAL compaction floor can actually be rescued.
///
/// <para><b>Why the harness needs one at all.</b> Without a registered transfer the leader still
/// escalates correctly — it reaches <c>ReportUnproducible</c> and says so through
/// <c>GetSnapshotStatuses</c> — but no rescue can ever complete. A follower under the leader's first
/// available index is then unrepairable <em>by construction</em>, whatever the library does. That
/// made the whole compaction family untestable: a wedge caused by a real defect and a wedge caused
/// by the harness's own missing registration produce the same stuck cluster, and no oracle can tell
/// them apart. Measured, not assumed — a hunt over a reintroduced escalation defect wedged
/// identically on the fixed build, twenty replays out of twenty each way.</para>
///
/// <para><b>Why the blob is empty, and why that is honest.</b> A simulated node holds no application
/// state: its state machine is the log itself, which Kommander ships through its own paths. So there
/// is nothing to export, and an empty export truthfully "reflects everything applied at
/// <c>upToIndex</c>". What the transfer buys is the part that repairs the follower — the receiver
/// installs its WAL boundary at <c>upToIndex</c>, which lifts it back above the floor so ordinary
/// replication can carry it forward. A scenario family that gives simulated nodes real state must
/// replace this, not extend it.</para>
///
/// <para>The header bytes are not decoration. An import that received a truncated or foreign stream
/// would otherwise succeed silently, and a rescue that "worked" while transferring nothing is
/// exactly the kind of quiet pass this harness exists to prevent.</para>
///
/// <para>Both methods run off the executor thread on background transfer tasks, and one instance is
/// shared by every partition on a node. The only mutable state is the hang fault below, and every
/// field it uses is read and written through <see cref="Interlocked"/> or under a lock.</para>
///
/// <para><b>The hang fault, and why a simulated application needs one.</b>
/// <see cref="HangNextExports"/> makes the next exports never return, and they ignore their
/// cancellation token. That is the shape of the Caraxes anchor-1 wedge (vorpal <c>d11fd5f9</c>,
/// round 2): the application's export began with a persistence drain that could not be cancelled,
/// the transfer task parked forever, its in-flight guard never released, and the leader silently
/// vetoed every later rescue of that follower. <c>SnapshotTransferStepTimeout</c> is the library's
/// repair. Without a fault that can hang, no simulated run can reach the state that repair exists
/// for, and a search over it cannot fail.</para>
/// </summary>
public sealed class SimulatedPartitionStateTransfer : IRaftPartitionStateTransfer
{
    /// <summary>Marks a blob as one this type produced, so an import can reject anything else.</summary>
    private const uint Magic = 0x5349_4D53; // "SIMS"

    private readonly object hungLock = new();

    /// <summary>Exports still owed a hang. Consumed one per export, never below zero.</summary>
    private int exportsToHang;

    private int exportsHung;
    private int exportsServed;

    /// <summary>
    /// The hung exports, kept so that <see cref="ReleaseHungExports"/> can end them. A hung task
    /// nobody ends keeps its transfer's continuation alive for the rest of the test process.
    /// </summary>
    private readonly List<TaskCompletionSource<Stream>> hung = [];

    /// <summary>Exports that hung, over the life of this instance.</summary>
    public int ExportsHung => Volatile.Read(ref exportsHung);

    /// <summary>Exports that returned a snapshot, over the life of this instance.</summary>
    public int ExportsServed => Volatile.Read(ref exportsServed);

    /// <summary>Exports still armed to hang. Zero when the fault is spent or was never set.</summary>
    public int ExportsArmedToHang => Volatile.Read(ref exportsToHang);

    /// <summary>
    /// Called with the partition id at the start of every import, before the node's log changes.
    /// The cluster installs it to judge whether the snapshot was needed at all (see
    /// <see cref="SimulationCluster.UnnecessarySnapshotImports"/>). Runs on the importing
    /// partition's executor thread.
    /// </summary>
    public Action<int>? ImportObserver { get; set; }

    /// <summary>
    /// Makes the next <paramref name="count"/> exports on this node never complete.
    ///
    /// <para><b>Armed until used, not until a step passes.</b> The fault is a latent defect in the
    /// application, and it bites on the next export whenever that export happens. A fault that
    /// expired after a fixed number of steps would miss every rescue that starts later, and a
    /// rescue usually starts later: the follower must first fall below a floor.</para>
    ///
    /// <para><b>A hung export ignores its token.</b> A hang that ended on cancellation would be
    /// ended by the library's own timeout, and the defect class this fault exists for is a
    /// transfer that nothing can end. The library must abandon the step, not wait for it.</para>
    /// </summary>
    public void HangNextExports(int count)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(count);
        Interlocked.Add(ref exportsToHang, count);
    }

    /// <summary>
    /// Disarms the fault and ends every export that is still hung, with an exception.
    ///
    /// <para>For a crash and for teardown only. A crash ends the process that owned the hung
    /// call. Teardown must not leave detached tasks behind for the next test. A runner must not
    /// call this to heal a run: the library's step timeout is the repair under test, and a runner
    /// that ended the hang itself would repair the state it is looking for.</para>
    /// </summary>
    public void ReleaseHungExports()
    {
        Interlocked.Exchange(ref exportsToHang, 0);

        List<TaskCompletionSource<Stream>> released;

        lock (hungLock)
        {
            released = [.. hung];
            hung.Clear();
        }

        foreach (TaskCompletionSource<Stream> export in released)
            export.TrySetException(new InvalidOperationException("The simulated export was released by the harness."));
    }

    public Task<Stream> ExportPartitionState(int partitionId, long upToIndex, CancellationToken ct)
    {
        if (TryConsumeHang())
        {
            // RunContinuationsAsynchronously: a release from the harness thread must not run the
            // abandoned transfer's continuation inline on that thread.
            TaskCompletionSource<Stream> export = new(TaskCreationOptions.RunContinuationsAsynchronously);

            lock (hungLock)
                hung.Add(export);

            Interlocked.Increment(ref exportsHung);
            return export.Task;
        }

        Interlocked.Increment(ref exportsServed);

        MemoryStream blob = new();

        using (BinaryWriter writer = new(blob, global::System.Text.Encoding.UTF8, leaveOpen: true))
        {
            writer.Write(Magic);
            writer.Write(partitionId);
            writer.Write(upToIndex);
        }

        blob.Position = 0;
        return Task.FromResult<Stream>(blob);
    }

    public async Task ImportPartitionState(int partitionId, Stream snapshot, CancellationToken ct)
    {
        ImportObserver?.Invoke(partitionId);

        MemoryStream buffer = new();
        await snapshot.CopyToAsync(buffer, ct).ConfigureAwait(false);
        buffer.Position = 0;

        // A short read is a broken transfer, and a wrong partition is a routing defect. Both must
        // fail loudly: an import that shrugged would report a successful rescue that moved nothing.
        if (buffer.Length < sizeof(uint) + sizeof(int) + sizeof(long))
            throw new InvalidOperationException(
                $"Snapshot for partition {partitionId} is {buffer.Length} bytes, shorter than its own header.");

        using BinaryReader reader = new(buffer, global::System.Text.Encoding.UTF8, leaveOpen: true);

        uint magic = reader.ReadUInt32();
        if (magic != Magic)
            throw new InvalidOperationException(
                $"Snapshot for partition {partitionId} does not carry this harness's marker (read 0x{magic:X8}).");

        int exportedPartition = reader.ReadInt32();
        if (exportedPartition != partitionId)
            throw new InvalidOperationException(
                $"Snapshot was exported for partition {exportedPartition} and delivered to partition {partitionId}.");

        // The index is read for the same reason: a blob that cannot be read to its end is not a
        // blob this node should call an installed state.
        reader.ReadInt64();
    }

    /// <summary>Takes one armed hang, if any is left. Lock-free, so two exports never take one.</summary>
    private bool TryConsumeHang()
    {
        while (true)
        {
            int armed = Volatile.Read(ref exportsToHang);

            if (armed <= 0)
                return false;

            if (Interlocked.CompareExchange(ref exportsToHang, armed - 1, armed) == armed)
                return true;
        }
    }
}
