
using Kommander.System;

namespace Kommander.Data;

/// <summary>
/// An immutable, point-in-time snapshot of one partition's consensus state on one node, captured on the
/// partition executor's single-writer thread so no mutable <see cref="RaftPartitionStateMachine"/> field
/// is ever read by a polling thread. Used by the chaos harness to build a <c>ClusterView</c> for the
/// continuous invariant checker.
/// </summary>
public sealed record RaftPartitionView(
    string Endpoint,
    int Partition,
    RaftNodeState Role,
    long Term,
    string Leader,
    long CommitIndex,
    long LastAppliedIndex,
    long MaxWalIndex,
    bool Quiesced,
    ClusterMemberRole MemberRole)
{
    public override string ToString() =>
        $"{Endpoint} p{Partition} {Role}/{MemberRole} term={Term} leader={Leader} commit={CommitIndex} " +
        $"applied={LastAppliedIndex} maxWal={MaxWalIndex}{(Quiesced ? " quiesced" : "")}";
}
