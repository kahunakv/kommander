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
/// shared by every partition on a node, so this type holds no mutable state.</para>
/// </summary>
internal sealed class SimulatedPartitionStateTransfer : IRaftPartitionStateTransfer
{
    /// <summary>Marks a blob as one this type produced, so an import can reject anything else.</summary>
    private const uint Magic = 0x5349_4D53; // "SIMS"

    public Task<Stream> ExportPartitionState(int partitionId, long upToIndex, CancellationToken ct)
    {
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
}
