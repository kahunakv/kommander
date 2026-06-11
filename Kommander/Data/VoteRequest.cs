
using Kommander.Time;

namespace Kommander.Data;

public sealed class VoteRequest
{
    public int Partition { get; set; }

    public long Term { get; set; }
    
    public long MaxLogId { get; set; }
    
    public HLCTimestamp Time { get; set; }

    public string Endpoint { get; set; }

    /// <summary>
    /// When true this Vote is a side-effect-free pre-election grant (Raft §9.6);
    /// the peer does not persist term/vote state.
    /// </summary>
    public bool PreVote { get; set; }

    public VoteRequest(int partition, long term, long maxLogId, HLCTimestamp time, string endpoint, bool preVote = false)
    {
        Partition = partition;
        Term = term;
        MaxLogId = maxLogId;
        Time = time;
        Endpoint = endpoint;
        PreVote = preVote;
    }
}
