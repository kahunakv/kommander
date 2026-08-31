using Kommander.Data;

namespace Kommander.Tests.Simulation.WAL;

/// <summary>
/// The state of one partition's log in a simulated write-ahead log, at one instant.
///
/// <para>This is the shape the write-ahead-log invariants read. A partition view reports how far a
/// node believes it has committed; only the store itself can say what is actually on disk under
/// that belief, and the difference between the two is where several shipped defects lived.</para>
/// </summary>
/// <param name="PartitionId">The partition this describes.</param>
/// <param name="EntryCount">Entries retained, of every type.</param>
/// <param name="FirstLogId">Lowest retained id, or -1 when the partition is empty.</param>
/// <param name="MaxLogId">Highest retained id, or 0 when the partition is empty.</param>
/// <param name="LastCheckpoint">Id of the last committed checkpoint, or -1 when there is none.</param>
/// <param name="CountByType">Entries retained, per log type.</param>
/// <param name="MissingIds">
/// Ids absent between <paramref name="FirstLogId"/> and <paramref name="MaxLogId"/>. These are holes
/// and nothing else: an id below <paramref name="FirstLogId"/> may simply have been compacted, so it
/// is not reported here.
/// </param>
/// <param name="NonDurableIds">
/// Ids written but not yet fsynced. A crash at this instant would revert or remove exactly these.
/// </param>
/// <param name="CompactionsAboveFloor">
/// Times the caller asked to compact past the checkpoint that was certified at that moment.
///
/// <para>Recorded at the moment of the request rather than derived afterwards, because afterwards
/// the two cases are indistinguishable: a log that starts above its first id may have been
/// compacted, or may simply never have received its head. Only the request separates them.</para>
/// </param>
/// <param name="WorstCompactionRequest">The highest floor ever asked for, or -1 if none was too high.</param>
/// <param name="WorstCompactionCertifiedFloor">The checkpoint certified when that request arrived.</param>
/// <param name="CompactedThrough">
/// Highest id compaction actually removed, or 0 where none was. This is what makes a missing head
/// readable: a log whose lowest id is 5 has either compacted 1 through 4 or never received them, and
/// nothing observable afterwards separates those two.
/// </param>
public sealed record SimulatedWalPartitionSnapshot(
    int PartitionId,
    int EntryCount,
    long FirstLogId,
    long MaxLogId,
    long LastCheckpoint,
    IReadOnlyDictionary<RaftLogType, int> CountByType,
    IReadOnlyList<long> MissingIds,
    IReadOnlyList<long> NonDurableIds,
    int CompactionsAboveFloor = 0,
    long WorstCompactionRequest = -1,
    long WorstCompactionCertifiedFloor = -1,
    long CompactedThrough = 0)
{
    /// <summary>True when an id is absent inside the retained range.</summary>
    public bool HasHole => MissingIds.Count > 0;

    /// <summary>Entries of the given type, or zero.</summary>
    public int Count(RaftLogType type) => CountByType.TryGetValue(type, out int count) ? count : 0;
}

/// <summary>
/// Every partition of one simulated write-ahead log, plus the run counters. Taken at a settled step
/// boundary, so nothing here is half-written.
/// </summary>
/// <param name="Partitions">Per-partition state, keyed by partition id.</param>
/// <param name="Counters">What the store did up to this instant.</param>
/// <param name="NonDurableEntryCount">
/// Entries inside the fsync window across every partition. A crash loses these.
/// </param>
/// <param name="NonDurableMetadataKeys">
/// Metadata keys inside the fsync window, the per-partition Raft hard state among them.
/// </param>
public sealed record SimulatedWalSnapshot(
    IReadOnlyDictionary<int, SimulatedWalPartitionSnapshot> Partitions,
    SimulatedWalCounters Counters,
    int NonDurableEntryCount,
    IReadOnlyList<string> NonDurableMetadataKeys)
{
    /// <summary>One partition's state, or null when the partition holds nothing.</summary>
    public SimulatedWalPartitionSnapshot? Partition(int partitionId) =>
        Partitions.TryGetValue(partitionId, out SimulatedWalPartitionSnapshot? partition) ? partition : null;
}
