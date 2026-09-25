using Kommander.Time;

namespace Kommander.Data;

/// <summary>
/// A follower's request for a whole-partition snapshot from the partition's leader, sent through
/// <see cref="IRaft.RequestReseedAsync"/>. The requester has stopped delivering committed entries to
/// its application, so its apply cursor cannot move past the checkpoint the leader takes for it, and
/// the install that follows replaces its application state whatever its log already covers.
/// </summary>
public sealed class ReseedRequest
{
    public int Partition { get; set; }

    /// <summary>The requester's current term: a leader in an older term drops the request.</summary>
    public long Term { get; set; }

    public HLCTimestamp Time { get; set; }

    /// <summary>The follower asking to be re-seeded.</summary>
    public string Endpoint { get; set; } = "";

    public ReseedRequest() { }

    public ReseedRequest(int partition, long term, HLCTimestamp time, string endpoint)
    {
        Partition = partition;
        Term      = term;
        Time      = time;
        Endpoint  = endpoint;
    }
}
