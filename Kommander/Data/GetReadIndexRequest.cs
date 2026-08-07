namespace Kommander.Data;

/// <summary>
/// Sent by a non-leader to the partition leader asking for a quorum-confirmed read index
/// (Raft dissertation §6.4, follower read). The leader runs — or coalesces into — its
/// read-index confirmation machinery and answers with the commit frontier captured by a
/// same-term quorum ack round, which the sender then waits for locally
/// (<c>IRaft.ConfirmLocalApplicationAsync</c>).
/// </summary>
public sealed record GetReadIndexRequest(int PartitionId);
