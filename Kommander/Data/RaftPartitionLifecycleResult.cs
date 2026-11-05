
namespace Kommander.Data;

public sealed class RaftPartitionLifecycleResult
{
    public bool Success { get; init; }
    public RaftOperationStatus Status { get; init; }
    public long Generation { get; init; }

    /// <summary>
    /// The outcome of the state transfer of a split. <see cref="Success"/> does not cover it, because
    /// a failed transfer does not fail the split. See <see cref="RaftStateTransferOutcome"/>.
    /// </summary>
    public RaftStateTransferOutcome StateTransfer { get; init; }
}
