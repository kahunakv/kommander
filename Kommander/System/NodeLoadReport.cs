
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
    /// Load snapshots for every partition this node currently leads.
    /// Partitions for which another node is leader are absent — the receiver
    /// must not infer "no leadership" from a missing entry; it must consult the
    /// corresponding node's latest report instead.
    /// </summary>
    public List<PartitionLoad> Leaderships { get; set; } = [];
}
