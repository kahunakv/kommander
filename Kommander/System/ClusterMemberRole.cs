
namespace Kommander.System;

public enum ClusterMemberRole
{
    Learner,
    Voter,
    Leaving,
    /// <summary>
    /// The local node is not present in the committed roster at all.
    /// Used only as a return value from <see cref="RaftManager.LocalRole"/>;
    /// never stored in a <see cref="ClusterMember"/> entry.
    /// </summary>
    NotMember
}
