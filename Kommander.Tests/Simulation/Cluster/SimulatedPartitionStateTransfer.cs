using Kommander.Data;
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
/// <para><b>What the blob carries.</b> The node's application state at the boundary: every entry
/// the library handed the node for application, up to <c>upToIndex</c>, as id, term and payload
/// hash (see <see cref="Apply"/>). It used to be a header only, which was honest while the nodes kept
/// no state, but it made a receiver that acknowledged an install and imported nothing look exactly
/// like one that imported everything (<c>14b564a</c> item 6, the Kahuna run of Sept 20). The run-level
/// rule <c>applied-state-agrees</c> now tells them apart (DST-20).</para>
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

        // The applied entries at or below the boundary: the state a snapshot at upToIndex stands
        // for. Entries above it arrive afterwards through ordinary replication.
        List<KeyValuePair<long, AppliedEntry>> exported;
        lock (appliedLock)
        {
            exported = applied.TryGetValue(partitionId, out SortedDictionary<long, AppliedEntry>? state)
                ? state.Where(pair => pair.Key <= upToIndex).ToList()
                : [];
        }

        MemoryStream blob = new();

        using (BinaryWriter writer = new(blob, global::System.Text.Encoding.UTF8, leaveOpen: true))
        {
            writer.Write(Magic);
            writer.Write(partitionId);
            writer.Write(upToIndex);
            writer.Write(exported.Count);

            foreach ((long id, AppliedEntry entry) in exported)
            {
                writer.Write(id);
                writer.Write(entry.Term);
                writer.Write(entry.Hash);
            }
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

        long upToIndex = reader.ReadInt64();
        int count = reader.ReadInt32();

        SortedDictionary<long, AppliedEntry> imported = [];
        for (int index = 0; index < count; index++)
        {
            long id = reader.ReadInt64();
            long term = reader.ReadInt64();
            ulong hash = reader.ReadUInt64();
            imported[id] = new AppliedEntry(term, hash);
        }

        // The Control A hook of DST-20: a receiver that reads the blob, reports success, and keeps
        // its old state. Everything above ran, so the transfer itself looks complete.
        if (SkipImports)
        {
            Interlocked.Increment(ref importsSkipped);
            return;
        }

        // A snapshot is the whole state at the boundary, so it replaces what the node had. Entries
        // above the boundary are applied again as replication delivers them.
        lock (appliedLock)
            applied[partitionId] = imported;

        Interlocked.Increment(ref importsApplied);
        LastImportBoundary = upToIndex;
    }


    /// <summary>Takes one armed hang, if any is left. Lock-free, so two exports never take one.</summary>
    // ── The application state ─────────────────────────────────────────────

    /// <summary>One applied entry, reduced to what two nodes must agree on.</summary>
    /// <param name="Term">The entry's term.</param>
    /// <param name="Hash">A hash of the entry's type name and payload.</param>
    public readonly record struct AppliedEntry(long Term, ulong Hash);

    private readonly object appliedLock = new();
    private readonly Dictionary<int, SortedDictionary<long, AppliedEntry>> applied = [];
    private int importsApplied;
    private int importsSkipped;

    /// <summary>
    /// When true, an import reads the whole blob and then keeps the old state: the receiver reports
    /// success and imports nothing. For Control A only.
    /// </summary>
    public bool SkipImports { get; set; }

    /// <summary>Imports that replaced the state.</summary>
    public int ImportsApplied => Volatile.Read(ref importsApplied);

    /// <summary>Imports that <see cref="SkipImports"/> discarded.</summary>
    public int ImportsSkipped => Volatile.Read(ref importsSkipped);

    /// <summary>The boundary of the last import that replaced the state, or 0.</summary>
    public long LastImportBoundary { get; private set; }

    /// <summary>
    /// Records an entry the library delivered for application, from a live commit or a restore
    /// replay. Idempotent by id, so a replay after a restart changes nothing it already held.
    ///
    /// <para><b>What survives a crash.</b> This object is not rebuilt on restart, so the state
    /// outlives a crash: it models an application that persists what it applied, which is what
    /// Kahuna and CamusDB do. A wiped node loses it (<see cref="ClearApplied"/>).</para>
    /// </summary>
    public void Apply(int partitionId, RaftLog log)
    {
        AppliedEntry entry = new(log.Term, Hash(log));

        lock (appliedLock)
        {
            if (!applied.TryGetValue(partitionId, out SortedDictionary<long, AppliedEntry>? state))
            {
                state = [];
                applied[partitionId] = state;
            }

            state[log.Id] = entry;
        }
    }

    /// <summary>Forgets every applied entry: the application's own store was lost with the disk.</summary>
    public void ClearApplied()
    {
        lock (appliedLock)
            applied.Clear();
    }

    /// <summary>A copy of the applied entries of one partition, by id.</summary>
    public IReadOnlyDictionary<long, AppliedEntry> GetApplied(int partitionId)
    {
        lock (appliedLock)
        {
            return applied.TryGetValue(partitionId, out SortedDictionary<long, AppliedEntry>? state)
                ? new Dictionary<long, AppliedEntry>(state)
                : new Dictionary<long, AppliedEntry>();
        }
    }

    /// <summary>The hash two nodes compare for one entry: FNV-1a over the type name and the payload.</summary>
    public static ulong Hash(RaftLog log)
    {
        const ulong offset = 14695981039346656037;
        const ulong prime = 1099511628211;

        ulong hash = offset;

        foreach (char character in log.LogType ?? string.Empty)
        {
            hash ^= character;
            hash *= prime;
        }

        foreach (byte value in log.LogData ?? [])
        {
            hash ^= value;
            hash *= prime;
        }

        return hash;
    }

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
