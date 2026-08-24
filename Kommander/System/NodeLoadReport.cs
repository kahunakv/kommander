
using Kommander.Time;

namespace Kommander.System;

/// <summary>
/// Advisory, per-node snapshot of leadership placement and load, intended to be gossiped
/// to the system-partition (P0) leader so the balancer can compute a transfer plan.
///
/// <para><b>Never committed to the Raft log.</b> This type is high-churn and purely
/// advisory. A stale or dropped report only delays a balancing decision — it cannot
/// violate Raft safety, because every actual leadership transfer is validated and executed
/// by <c>TransferLeadershipAsync</c> at the time of the move.</para>
///
/// <para>The receiver keeps the highest <see cref="ReportVersion"/> per
/// <see cref="Endpoint"/> and expires entries older than a configured TTL. A node that
/// goes silent is simply excluded from the next planning pass; its phantom leaderships
/// are not carried forward.</para>
/// </summary>
public sealed class NodeLoadReport
{
    /// <summary>
    /// Network endpoint of the node that produced this report
    /// (e.g. <c>"host:port"</c>). Used as the keying identity by the P0 receiver.
    /// </summary>
    public string Endpoint { get; set; } = "";

    /// <summary>
    /// Monotonically increasing counter bumped on every emission by this node.
    /// The receiver retains only the entry with the highest version per endpoint,
    /// discarding older reports that arrive out of order.
    /// </summary>
    public long ReportVersion { get; set; }

    /// <summary>HLC timestamp at which this report was built.</summary>
    public HLCTimestamp Time { get; set; }

    /// <summary>
    /// The sender's locality hint (<see cref="RaftConfiguration.Zone"/>), or null when it has
    /// none configured. Carried so every node — the P0 placement planner in particular — learns
    /// remote nodes' zones without a committed roster change; without this only the local node's
    /// zone was known and zone-aware replica spread was inert on multi-process clusters.
    /// The report travels as JSON inside the gossip envelope, so absent/older senders simply
    /// deserialize as null (no wire change). Unlike the load figures, a zone is topology and
    /// effectively immutable for a node's lifetime, so consumers may use it from a report that
    /// has aged past the load-freshness TTL.
    /// </summary>
    public string? Zone { get; set; }

    /// <summary>
    /// Node-wide EWMA of the WAL enqueue-to-durable commit wait in milliseconds at the time this
    /// report was built. Unlike <see cref="PartitionLoad.CommitWaitMs"/>, which exists only for
    /// partitions this node leads, this figure covers every partition the node writes for —
    /// follower appends included — so a node that leads nothing still advertises its disk health.
    /// That is what makes it usable for judging a node that is a candidate to *receive* leadership.
    /// <para>Meaningless on its own: <c>0</c> is the wire default an older peer produces, and also
    /// what a node that has written nothing reports. Always read it together with
    /// <see cref="NodeCommitWaitSamples"/> and <see cref="NodeCommitWaitAgeMs"/>.</para>
    /// </summary>
    public double NodeCommitWaitMs { get; set; }

    /// <summary>
    /// Number of WAL group batches that fed <see cref="NodeCommitWaitMs"/>. <c>0</c> means
    /// <b>unknown</b> — either the node has written nothing, or the report came from a peer too old
    /// to carry the field. A consumer must never read an unknown node as a healthy one.
    /// </summary>
    public long NodeCommitWaitSamples { get; set; }

    /// <summary>
    /// Milliseconds elapsed between the last WAL batch observation and the moment this report was
    /// built. The commit-wait EWMA decays per sample rather than per second, so a node that goes
    /// quiet keeps reporting its last figure; this field is the only way a consumer can tell that
    /// <see cref="NodeCommitWaitMs"/> describes the past.
    /// </summary>
    public long NodeCommitWaitAgeMs { get; set; }

    /// <summary>
    /// Load snapshots for every partition this node currently leads.
    /// Partitions for which another node is leader are absent — the receiver
    /// must not infer "no leadership" from a missing entry; it must consult the
    /// corresponding node's latest report instead.
    /// </summary>
    public List<PartitionLoad> Leaderships { get; set; } = [];
}
