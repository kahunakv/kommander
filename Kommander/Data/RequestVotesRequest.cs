
using Kommander.Time;

namespace Kommander.Data;

/// <summary>
/// Represents a request used in a distributed consensus algorithm for requesting votes between nodes.
/// This request is typically sent during the election process in a cluster to gather support
/// from peer nodes for leadership candidacy.
/// </summary>
public sealed class RequestVotesRequest
{
    public int Partition { get; set; }

    public long Term { get; set; }
    
    public long MaxLogId { get; set; }

    /// <summary>
    /// Term of the candidate's last log entry (the entry at <see cref="MaxLogId"/>). Paired with
    /// <see cref="MaxLogId"/> this is the §5.4.1 log-freshness key voters compare lexicographically
    /// (term first, then index) — without it a candidate whose higher index hides a stale last term
    /// could be elected over a more up-to-date voter, a Leader-Completeness violation.
    /// <para><b>Wire compatibility:</b> a peer predating this field sends <c>0</c>. Voters treat a
    /// non-positive last-log term as "unknown" and fall back to index-only comparison, so a
    /// mixed-version cluster degrades to index-only ordering rather than mis-ordering.</para>
    /// </summary>
    public long LastLogTerm { get; set; }

    public HLCTimestamp Time { get; set; }

    public string Endpoint { get; set; }

    /// <summary>
    /// When true this RequestVotes is a side-effect-free pre-election probe (Raft §9.6);
    /// the peer does not persist term/vote state.
    /// </summary>
    public bool PreVote { get; set; }

    /// <summary>
    /// When true this is a leaderless notice: a pre-vote probe from a follower that has no leader and
    /// cannot campaign (a storage quarantine, a stalled disk, or a candidacy the application withheld).
    /// The sender asks for no grant and opens no round. The message only states that the sender is a
    /// follower at <see cref="Term"/> - 1, so that a quiesced leader wakes and a follower that still
    /// expects the sender to lead treats that leadership as vacated. Always sent with
    /// <see cref="PreVote"/> set.
    /// <para><b>Wire compatibility:</b> a peer that predates this field reads the message as an
    /// ordinary pre-vote probe and can answer with a pre-grant. The sender has no open round and
    /// drops that grant, so a mixed-version cluster stays correct.</para>
    /// </summary>
    public bool NoticeOnly { get; set; }

    public RequestVotesRequest(int partition, long term, long maxLogId, long lastLogTerm, HLCTimestamp time, string endpoint, bool preVote = false, bool noticeOnly = false)
    {
        Partition = partition;
        Term = term;
        Time = time;
        MaxLogId = maxLogId;
        LastLogTerm = lastLogTerm;
        Endpoint = endpoint;
        PreVote = preVote;
        NoticeOnly = noticeOnly;
    }
}
