
using Kommander.Time;

namespace Kommander.Data;

/// <summary>
/// Advisory message sent from the P0 balancer controller to the current leader of a partition,
/// asking it to transfer leadership to <see cref="TargetEndpoint"/>.
///
/// <para><b>Never committed to the Raft log.</b>  The recipient performs its own validation
/// before acting (checks current leadership, partition <c>Active</c> state, and that the
/// target is a live voter).  A dropped or rejected suggestion is always safe — the worst
/// outcome is a missed rebalancing opportunity that the next planning pass can retry.</para>
/// </summary>
public sealed class TransferLeadershipSuggestionRequest
{
    /// <summary>Partition whose leadership should be transferred.</summary>
    public int Partition { get; set; }

    /// <summary>Raft term at which the sender believed it was the P0 leader when it planned this move.</summary>
    public long Term { get; set; }

    /// <summary>HLC timestamp at which the suggestion was created.</summary>
    public HLCTimestamp Time { get; set; }

    /// <summary>Endpoint of the node that produced this suggestion (the P0 leader).</summary>
    public string SuggestedBy { get; set; } = "";

    /// <summary>Endpoint that should receive leadership of <see cref="Partition"/>.</summary>
    public string TargetEndpoint { get; set; } = "";

    public TransferLeadershipSuggestionRequest() { }

    public TransferLeadershipSuggestionRequest(
        int partition, long term, HLCTimestamp time, string suggestedBy, string targetEndpoint)
    {
        Partition      = partition;
        Term           = term;
        Time           = time;
        SuggestedBy    = suggestedBy;
        TargetEndpoint = targetEndpoint;
    }
}
