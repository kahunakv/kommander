
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
}