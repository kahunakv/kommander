
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
}