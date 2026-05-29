
namespace Kommander.System;

public enum RaftSystemRequestType
{
    LeaderChanged,
    RestoreCompleted,
    ConfigRestored,
    ConfigReplicated,
    SplitPartition,
    /// <summary>
    /// Test-only sentinel. When the loop processes this it completes the
    /// corresponding <see cref="TaskCompletionSource"/> registered via
    /// <see cref="RaftSystemCoordinator.DrainAsync"/>, letting tests wait for
    /// all previously-enqueued work to finish without a fixed delay.
    /// </summary>
    DrainSentinel
}