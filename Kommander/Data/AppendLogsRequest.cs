
using System.Text.Json.Serialization;
using Kommander.Communication.Grpc;
using Kommander.Time;

namespace Kommander.Data;

public sealed class AppendLogsRequest
{
    public int Partition { get; set; }

    /// <summary>
    /// Stopwatch stamp taken when the transport dispatcher queued this message, or 0 when the round
    /// stages (<see cref="Kommander.Diagnostics.RoundStageInstrumentation"/>) are off. Local only:
    /// internal, so neither the REST JSON nor the gRPC mapping carries it.
    /// </summary>
    internal long DispatchStageTicks { get; set; }

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

    /// <summary>
    /// When <see langword="true"/>, this is the leader's final AppendLogs before suppressing
    /// per-partition heartbeats.  Receiving followers switch to SWIM-based election gating
    /// instead of the heartbeat timer.  Only meaningful when
    /// <see cref="RaftConfiguration.EnableQuiescence"/> is on.
    /// </summary>
    public bool Quiesce { get; set; }

    /// <summary>
    /// The leader's live-replica retention floor: the lowest log index a replica of this partition
    /// still needs from the log — the slowest peer's durable position + 1, or the leader's own when
    /// its disk is the one behind. A follower holds its WAL compaction there (within
    /// <see cref="RetentionBudget"/>), exactly as the leader does, so that whichever node leads next
    /// can still serve that replica by backfill.
    /// <para><see cref="long.MaxValue"/> says no replica constrains retention. Zero says nothing:
    /// a leader that predates the field, or one that has not computed a floor in its term yet. The
    /// follower then keeps whatever floor it last received until that goes stale.</para>
    /// <para>Without it only the leader held the log for a lagging replica, and the hold was lost
    /// at every leader change: a successor that had been a follower had compacted to its
    /// checkpoint, and a replica 100,000 entries behind — served by backfill a second earlier —
    /// needed a whole-partition snapshot (CamusDB fault soak rl5).</para>
    /// </summary>
    public long RetentionFloor { get; set; }

    /// <summary>
    /// The leader's rate-scaled retention budget in entries, sent with <see cref="RetentionFloor"/>:
    /// how far below its checkpoint a node keeps entries for that floor. Zero leaves the receiver's
    /// configured <see cref="RaftConfiguration.CompactionLiveReplicaLagBudget"/> to apply alone.
    /// </summary>
    public long RetentionBudget { get; set; }

    /// <summary>
    /// Shared gRPC log payload for one batch fanned out to multiple followers. Populated by the
    /// leader before fan-out; ignored by REST and not serialized on the wire.
    /// </summary>
    [JsonIgnore]
    public AppendLogsGrpcLogCache? GrpcLogCache { get; set; }

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
