using System.ComponentModel;

namespace Kommander.Data;

/// <summary>
/// What a suspended snapshot install reports to its gate: enough of the request and of this
/// node's state to assert that a log tail exists above the boundary and that the consumer's apply
/// cursor is where the phase says it should be.
///
/// <para>Read-only. The gate acts on the node through the ordinary API (or through the other test
/// hooks); it never mutates the install through this object.</para>
/// </summary>
[EditorBrowsable(EditorBrowsableState.Never)]
public sealed class SnapshotInstallSignal
{
    internal SnapshotInstallSignal(
        int partitionId,
        long snapshotIndex,
        long boundaryTerm,
        SnapshotKind kind,
        SnapshotInstallPhase phase,
        long localMaxLogId,
        long lastAppliedIndex)
    {
        PartitionId = partitionId;
        SnapshotIndex = snapshotIndex;
        BoundaryTerm = boundaryTerm;
        Kind = kind;
        Phase = phase;
        LocalMaxLogId = localMaxLogId;
        LastAppliedIndex = lastAppliedIndex;
    }

    /// <summary>Partition being installed into.</summary>
    public int PartitionId { get; }

    /// <summary>Last index the snapshot includes — the boundary the suffix is judged against.</summary>
    public long SnapshotIndex { get; }

    /// <summary>
    /// Term the boundary is stamped with: the request's <c>LastIncludedTerm</c>, or this node's
    /// current term for a legacy sender with no authoritative last-included term.
    /// </summary>
    public long BoundaryTerm { get; }

    /// <summary>Which importer the payload is routed to.</summary>
    public SnapshotKind Kind { get; }

    /// <summary>The phase this gate is registered for.</summary>
    public SnapshotInstallPhase Phase { get; }

    /// <summary>Highest log id this node holds, so a test can assert a tail exists above the boundary.</summary>
    public long LocalMaxLogId { get; }

    /// <summary>Consumer apply cursor at this phase.</summary>
    public long LastAppliedIndex { get; }
}
