namespace Kommander.Tests.Simulation.WAL;

/// <summary>
/// What a simulated write-ahead log did during a run.
///
/// <para>These numbers are the evidence a scenario asserts on. A crash test that only checks the
/// surviving entries cannot tell a store that lost nothing from a store that was never asked to
/// lose anything, so the counters record both what was written and what a crash took away.</para>
/// </summary>
public sealed record SimulatedWalCounters
{
    /// <summary>Successful calls to a write method. One call can carry many entries.</summary>
    public long Writes { get; init; }

    /// <summary>Entries accepted across every successful write.</summary>
    public long EntriesWritten { get; init; }

    /// <summary>Writes that asked for their own fsync.</summary>
    public long SyncWrites { get; init; }

    /// <summary>
    /// Writes that did not ask for an fsync. These ride the next sync write on the same partition,
    /// which is the window a crash can catch.
    /// </summary>
    public long NonSyncWrites { get; init; }

    /// <summary>Writes refused by an injected fault. Nothing was stored.</summary>
    public long FailedWrites { get; init; }

    /// <summary>Successful compaction calls.</summary>
    public long Compactions { get; init; }

    /// <summary>Entries removed by compaction.</summary>
    public long EntriesCompacted { get; init; }

    /// <summary>Successful truncation calls, of every kind.</summary>
    public long Truncations { get; init; }

    /// <summary>Metadata writes, which include the per-partition Raft hard state.</summary>
    public long MetadataWrites { get; init; }

    /// <summary>Simulated crashes.</summary>
    public long Crashes { get; init; }

    /// <summary>
    /// Entries a crash reverted or removed, summed over every crash. An entry that a crash rolled
    /// back to an earlier durable version counts once, the same as one that vanished.
    /// </summary>
    public long EntriesLostOnCrash { get; init; }

    /// <summary>Metadata keys a crash reverted or removed, summed over every crash.</summary>
    public long MetadataKeysLostOnCrash { get; init; }
}
