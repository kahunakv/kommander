
using Kommander.Time;

namespace Kommander.Data;

public sealed class AppendLogsRequest
{
    public int Partition { get; set; }

    public long Term { get; set; }

    public HLCTimestamp Time { get; set; }

    public string Endpoint { get; set; }

    public List<RaftLog>? Logs { get; set; }

    /// <summary>
    /// Log index of the entry immediately preceding the first entry in <see cref="Logs"/>.
    /// Zero when the batch starts from the beginning of the log.
    /// Together with <see cref="PrevLogTerm"/> this enforces the Log Matching Property:
    /// the follower must hold an entry at this index with this term before accepting the batch.
    /// </summary>
    public long PrevLogIndex { get; set; }

    /// <summary>
    /// Term of the entry at <see cref="PrevLogIndex"/>.
    /// Zero when <see cref="PrevLogIndex"/> is zero (no preceding entry).
    /// </summary>
    public long PrevLogTerm { get; set; }

    public AppendLogsRequest(int partition, long term, HLCTimestamp time, string endpoint, List<RaftLog>? logs = null, long prevLogIndex = 0, long prevLogTerm = 0)
    {
        Partition = partition;
        Term = term;
        Time = time;
        Endpoint = endpoint;
        Logs = logs;
        PrevLogIndex = prevLogIndex;
        PrevLogTerm = prevLogTerm;
    }
}
