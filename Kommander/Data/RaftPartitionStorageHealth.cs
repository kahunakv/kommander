namespace Kommander.Data;

/// <summary>
/// One partition's storage verdict on this node, for the host's readiness probe and for operators.
/// A point-in-time read of volatile fields written on the partition executor, so it may be one
/// transition stale but is never torn.
/// </summary>
/// <param name="State">The verdict. Anything but <see cref="RaftPartitionStorageState.Healthy"/> or
/// <see cref="RaftPartitionStorageState.Restoring"/> means the partition should not be reported ready.</param>
/// <param name="Reason">A human-readable explanation naming the evidence (the storage engine's
/// message, the watermark and frontier, the floors), or <see langword="null"/> when healthy.</param>
/// <param name="AcknowledgedThrough">For <see cref="RaftPartitionStorageState.LostAcknowledgedEntries"/>:
/// the highest id this node had acknowledged as durably held before the restart; -1 otherwise.</param>
/// <param name="PresentThrough">For <see cref="RaftPartitionStorageState.LostAcknowledgedEntries"/>:
/// the contiguous log the node currently holds, which must reach <paramref name="AcknowledgedThrough"/>
/// again before the quarantine lifts; -1 otherwise.</param>
public sealed record RaftPartitionStorageHealth(
    RaftPartitionStorageState State,
    string? Reason = null,
    long AcknowledgedThrough = -1,
    long PresentThrough = -1)
{
    /// <summary>The verdict every healthy, restored partition reports.</summary>
    public static RaftPartitionStorageHealth Healthy { get; } = new(RaftPartitionStorageState.Healthy);

    /// <summary>The verdict while the WAL restore is still running.</summary>
    public static RaftPartitionStorageHealth Restoring { get; } = new(RaftPartitionStorageState.Restoring);

    /// <summary>True for <see cref="RaftPartitionStorageState.Healthy"/> only.</summary>
    public bool IsHealthy => State == RaftPartitionStorageState.Healthy;

    /// <summary>
    /// True while the verdict withholds this replica's candidacy — every state but Healthy and
    /// Restoring. (Restoring withholds too, through the executor's restore gates.)
    /// </summary>
    public bool WithholdsCandidacy => State is not (RaftPartitionStorageState.Healthy or RaftPartitionStorageState.Restoring);

    public override string ToString() =>
        Reason is null ? State.ToString() : $"{State}: {Reason}";
}
