
namespace Kommander.System;

/// <summary>
/// Versioned cluster roster stored under <see cref="RaftSystemConfigKeys.Members"/> on P0.
/// Mutated only through committed Raft entries — never by gossip or discovery snapshots —
/// so it inherits P0's linearizability. Each change increments <see cref="MembershipVersion"/>
/// by exactly 1; callers carry the version they read so the coordinator can detect stale writes.
/// </summary>
public sealed class ClusterMembership
{
    /// <summary>
    /// Monotonically increasing counter, bumped on every Add/Promote/Remove.
    /// Zero means no roster has been committed yet (pre-seed transient).
    /// </summary>
    public long MembershipVersion { get; set; }

    public List<ClusterMember> Members { get; set; } = [];
}
