
namespace Kommander.System;

/// <summary>
/// Provides configuration constants specifically for the Raft system partitions.
/// </summary>
/// <remarks>
/// This class is used throughout the system to define constant values associated with
/// the Raft system, such as default partition information and log type identifiers.
/// </remarks>
public static class RaftSystemConfig
{
    public const int SystemPartition = 0;
    
    public const string RaftLogType = "_RaftSystem";

    /// <summary>
    /// Log type stamped on P0 checkpoint entries whose <c>LogData</c> carries a serialized
    /// <c>RaftSystemCheckpointSnapshot</c> (the full system-configuration map at checkpoint time).
    /// Restore uses it to rebuild the membership roster and partition map even after WAL
    /// compaction has removed the original config delta entries. Checkpoints with an empty
    /// log type (older WALs, non-P0 partitions) carry no payload and are skipped as before.
    /// </summary>
    public const string CheckpointLogType = "_RaftSystemCheckpoint";

    /// <summary>
    /// Log type stamped on the no-op entry a newly elected leader commits in its own term before
    /// publishing leadership (the promotion barrier). Committing it forces the inherited-entry
    /// drain, proving the consumer projection covers every entry committed by the previous leader.
    /// Entries of this type are internal to the consensus layer: every consumer delivery path
    /// (leader apply, follower apply, inherited drain, WAL restore) skips them, so consumers never
    /// observe the type and need no handling for it.
    /// </summary>
    public const string LeadershipBarrierLogType = "_RaftLeadershipBarrier";
}