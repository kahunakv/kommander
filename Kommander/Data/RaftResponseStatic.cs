
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
}