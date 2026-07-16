
using Kommander.Time;

namespace Kommander.Data;

public sealed class VoteRequest
{
    public int Partition { get; set; }

    public long Term { get; set; }
    
    public long MaxLogId { get; set; }

    /// <summary>
    /// Term of the granter's last log entry (the entry at <see cref="MaxLogId"/>). Carried for wire
    /// symmetry with <see cref="RequestVotesRequest.LastLogTerm"/> and future use; the grant reply's
    /// freshness key is not consulted by the candidate tallying it (the §5.4.1 comparison happens on
    /// the voter side against the candidate's advertised key). <c>0</c> from peers predating the field.
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
