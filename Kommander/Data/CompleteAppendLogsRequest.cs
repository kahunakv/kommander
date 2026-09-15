
using Kommander.Time;

namespace Kommander.Data;

public sealed class CompleteAppendLogsRequest
{
    public int Partition { get; set; }

    public long Term { get; set; }
    
    public HLCTimestamp Time { get; set; }

    public string Endpoint { get; set; }
    
    public RaftOperationStatus Status { get; set; }
    
    public long CommitIndex { get; set; }

    /// <summary>
    /// The follower's DURABLE contiguous commit frontier: the highest id that is both resolved and
    /// durably present on its disk with no hole below it. Unlike <see cref="CommitIndex"/>, which on
    /// a Success ack is the protocol frontier and advances when an append is merely queued for the
    /// storage engine, this value moves only when the engine has answered. It is the only evidence
    /// a leader may hold WAL retention on: a follower whose disk stalls keeps advertising a rising
    /// protocol frontier for every entry it has queued, and a leader that compacted on that report
    /// removed exactly the range the follower's backfill later needed (CamusDB slow-disk run sd8:
    /// a 205,000-entry gap opened in 16 s and the follower died on the snapshot). -1 when the sender
    /// does not track durability (a pre-report release); 0 carries no positional evidence.
    /// </summary>
    public long DurableIndex { get; set; } = -1;

    /// <summary>
    /// Age, in milliseconds, of the oldest WAL write the follower has handed to its storage engine
    /// that the engine has not answered; 0 when nothing is pending. A value at or above the leader's
    /// <see cref="RaftConfiguration.WalStallWarnThreshold"/> tells the leader the peer's disk is
    /// stalled: entry-carrying backfill and snapshot transfers to it cannot land until the disk
    /// answers, and buffering them is what killed a follower with under 1 GiB of headroom.
    /// </summary>
    public long WalStallMs { get; set; }

    public CompleteAppendLogsRequest(int partition, long term, HLCTimestamp time, string endpoint, RaftOperationStatus status, long commitIndex, long durableIndex = -1, long walStallMs = 0)
    {
        Partition = partition;
        Term = term;
        Time = time;
        Endpoint = endpoint;
        Status = status;
        CommitIndex = commitIndex;
        DurableIndex = durableIndex;
        WalStallMs = walStallMs;
    }
}