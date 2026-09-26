
using Kommander.Time;

namespace Kommander.Data;

public sealed class VoteRequest
{
    public int Partition { get; set; }

    public long Term { get; set; }
    
    public long MaxLogId { get; set; }

    /// <summary>
    /// Term of the granter's last log entry (the entry at <see cref="MaxLogId"/>), the other half of
    /// its Raft §5.4.1 position. The candidate tallying the grant re-checks that its own log is not
    /// behind this pair with the same lexicographic rule the voter applied (a fence against its log
    /// having changed since the request), so the term must travel with the index: an index-only
    /// re-check rejected grants a voter with an older-term, longer tail had correctly given.
    /// <c>0</c> from peers predating the field, which makes the candidate fall back to index-only.
    /// </summary>
    public long LastLogTerm { get; set; }

    public HLCTimestamp Time { get; set; }

    public string Endpoint { get; set; }

    /// <summary>
    /// When true this Vote is a side-effect-free pre-election grant (Raft §9.6);
    /// the peer does not persist term/vote state.
    /// </summary>
    public bool PreVote { get; set; }

    public VoteRequest(int partition, long term, long maxLogId, long lastLogTerm, HLCTimestamp time, string endpoint, bool preVote = false)
    {
        Partition = partition;
        Term = term;
        MaxLogId = maxLogId;
        LastLogTerm = lastLogTerm;
        Time = time;
        Endpoint = endpoint;
        PreVote = preVote;
    }
}
