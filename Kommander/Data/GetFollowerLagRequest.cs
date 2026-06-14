
namespace Kommander.Data;

/// <summary>
/// Asks the remote partition leader for the last committed log index it has seen from
/// <see cref="FollowerEndpoint"/> on <see cref="PartitionId"/>.  Used by the learner
/// promotion gate to evaluate lag on partitions not locally led.
/// </summary>
public sealed record GetFollowerLagRequest(int PartitionId, string FollowerEndpoint);
