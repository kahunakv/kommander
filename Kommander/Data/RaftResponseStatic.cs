
using Kommander.Time;

namespace Kommander.Data;

internal static class RaftResponseStatic
{
    public static RaftResponse NoneResponse = new(RaftResponseType.None);

    /// <summary>
    /// Pre-allocated rejection response for when the client proposal queue is saturated.
    /// Callers receiving this should back off and retry.
    /// </summary>
    public static RaftResponse ProposalQueueFullResponse = new(
        RaftResponseType.None,
        RaftOperationStatus.ProposalQueueFull,
        HLCTimestamp.Zero);

    /// <summary>
    /// Pre-allocated rejection response for client proposals arriving before the
    /// partition has completed its WAL restore.  Callers should retry after a delay.
    /// </summary>
    public static RaftResponse RestoreInProgressResponse = new(
        RaftResponseType.None,
        RaftOperationStatus.RestoreInProgress,
        HLCTimestamp.Zero);

    /// <summary>
    /// Pre-allocated rejection response returned when the proposal's
    /// <c>ExpectedGeneration</c> does not match the partition's current committed
    /// generation. Callers should refresh the partition map and retry.
    /// </summary>
    public static RaftResponse PartitionMovedResponse = new(
        RaftResponseType.None,
        RaftOperationStatus.PartitionMoved,
        HLCTimestamp.Zero);
}