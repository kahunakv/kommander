namespace Kommander.System;

/// <summary>
/// One node's WAL disk-health figures, reduced from its newest fresh <see cref="NodeLoadReport"/>.
///
/// <para>Distinct from <see cref="PartitionLoad"/> on purpose: these are properties of the node's
/// device and fsync path, not of any one partition, and they are reported by followers too. That is
/// what lets the balancer judge a node it is about to *give* leadership to, rather than only a node
/// that already leads.</para>
///
/// <para><see cref="Samples"/> is what separates <i>unknown</i> from <i>healthy</i>. A zero
/// <see cref="CommitWaitMs"/> is produced both by a fast node and by a node that has written
/// nothing, and it is also the wire default a peer too old to carry the fields produces. Reading a
/// zero as "fast" would make a freshly restarted node the most attractive transfer target in the
/// cluster.</para>
/// </summary>
/// <param name="CommitWaitMs">Node-wide EWMA enqueue-to-durable WAL wait, in milliseconds.</param>
/// <param name="Samples">WAL group batches behind the estimate. <c>0</c> means unknown.</param>
/// <param name="AgeMs">
/// Milliseconds since the observation, including gossip transit — the sender's own measured age
/// plus the age of the report that carried it. The commit-wait EWMA decays per sample rather than
/// per second, so without this a node that went quiet would keep being judged on a figure that
/// stopped being true.
/// </param>
public readonly record struct NodeHealthSample(double CommitWaitMs, long Samples, long AgeMs);
