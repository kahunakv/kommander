
namespace Kommander.Data;

/// <summary>
/// What happened to the optional state transfer of a split. A split stays valid when its transfer
/// fails, because the map change is already committed, so <see cref="RaftPartitionLifecycleResult.Success"/>
/// cannot report this: an application that relies on the transfer must read this value and move the
/// state itself when it is not <see cref="Completed"/>.
/// </summary>
public enum RaftStateTransferOutcome
{
    /// <summary>
    /// No transfer ran: the operation is not a split, or no
    /// <see cref="IRaftStateMachineTransfer"/> is registered on the coordinator node.
    /// </summary>
    NotAttempted = 0,

    /// <summary>
    /// The coordinator node exported and imported the range, and the checkpoint on the target
    /// partition committed. The import ran on the coordinator node only, and the checkpoint carries
    /// no application state, so the other replicas of the target do not hold the imported data.
    /// </summary>
    Completed = 1,

    /// <summary><see cref="IRaftStateMachineTransfer.ExportRange"/> failed. Nothing was imported.</summary>
    ExportFailed = 2,

    /// <summary><see cref="IRaftStateMachineTransfer.ImportRange"/> failed. The import may be partial if the implementation is not atomic.</summary>
    ImportFailed = 3,

    /// <summary>
    /// The import finished on the coordinator node, but the checkpoint on the target partition did not
    /// commit, for example because the coordinator node does not lead the target.
    /// </summary>
    CheckpointFailed = 4,
}
