
using Kommander.System.Protos;
using Kommander.Data;

namespace Kommander.System;

public sealed class RaftSystemRequest
{
    public RaftSystemRequestType Type { get; }

    /// <summary>Partition id; used by SplitPartition, CreatePartition, and RemovePartition.</summary>
    public int PartitionId { get; }

    public string? LeaderNode { get; }

    public byte[]? LogData { get; }

    /// <summary>Routing mode for CreatePartition requests.</summary>
    public RaftRoutingMode RoutingMode { get; }

    /// <summary>Inclusive hash-range start for HashRange CreatePartition requests; null for Unrouted.</summary>
    public int? HashRangeStart { get; }

    /// <summary>Inclusive hash-range end for HashRange CreatePartition requests; null for Unrouted.</summary>
    public int? HashRangeEnd { get; }

    /// <summary>
    /// Completion source for CreatePartition, RemovePartition, and SplitPartition requests.
    /// The coordinator sets the result when processing finishes; callers await it.
    /// Null for fire-and-forget requests.
    /// </summary>
    public TaskCompletionSource<(RaftOperationStatus Status, long Generation)>? Completion { get; init; }

    /// <summary>
    /// Merge plan for <see cref="RaftSystemRequestType.MergePartition"/> requests.
    /// </summary>
    public RaftMergePlan? MergePlan { get; init; }

    /// <summary>
    /// Split plan for <see cref="RaftSystemRequestType.SplitPartition"/> requests.
    /// When null the coordinator applies legacy midpoint-bisect defaults.
    /// </summary>
    public RaftSplitPlan? SplitPlan { get; init; }

    /// <summary>Node endpoint (host:port) for AddMember / PromoteMember / RemoveMember requests.</summary>
    public string? MemberEndpoint { get; init; }

    /// <summary>Node id for AddMember requests.</summary>
    public int MemberNodeId { get; init; }

    /// <summary>
    /// The <see cref="ClusterMembership.MembershipVersion"/> the caller read before building
    /// this request. The coordinator rejects the change with
    /// <see cref="RaftOperationStatus.StaleMembership"/> if the committed version has moved.
    /// </summary>
    public long ExpectedMembershipVersion { get; init; }

    /// <summary>
    /// Gossiped membership roster for <see cref="RaftSystemRequestType.ApplyGossipRoster"/> requests.
    /// Applied to the local cache only when its version exceeds the locally committed version;
    /// never written to the Raft log.
    /// </summary>
    public ClusterMembership? GossipedRoster { get; init; }

    /// <summary>
    /// Advisory load report for <see cref="RaftSystemRequestType.ApplyGossipLoadReport"/> requests.
    /// Stored in the coordinator's in-memory per-endpoint map; never written to the Raft log.
    /// </summary>
    public NodeLoadReport? GossipedLoadReport { get; init; }

    public RaftSystemRequest(RaftSystemRequestType type)
    {
        Type = type;
    }

    public RaftSystemRequest(RaftSystemRequestType type, byte[] logData)
    {
        Type = type;
        LogData = logData;
    }

    public RaftSystemRequest(RaftSystemRequestType type, string leaderNode)
    {
        Type = type;
        LeaderNode = leaderNode;
    }

    public RaftSystemRequest(RaftSystemRequestType type, int partitionId)
    {
        Type = type;
        PartitionId = partitionId;
    }

    /// <summary>
    /// Constructor for <see cref="RaftSystemRequestType.SplitPartition"/> requests
    /// that carry an explicit <see cref="RaftSplitPlan"/>.
    /// </summary>
    public RaftSystemRequest(
        int sourcePartitionId,
        RaftSplitPlan plan,
        TaskCompletionSource<(RaftOperationStatus Status, long Generation)>? completion = null)
    {
        Type = RaftSystemRequestType.SplitPartition;
        PartitionId = sourcePartitionId;
        SplitPlan = plan;
        Completion = completion;
    }

    /// <summary>
    /// Constructor for <see cref="RaftSystemRequestType.MergePartition"/> requests.
    /// </summary>
    public RaftSystemRequest(
        RaftMergePlan plan,
        TaskCompletionSource<(RaftOperationStatus Status, long Generation)>? completion = null)
    {
        Type = RaftSystemRequestType.MergePartition;
        MergePlan = plan;
        Completion = completion;
    }

    /// <summary>
    /// Constructor for <see cref="RaftSystemRequestType.ApplyGossipRoster"/> requests.
    /// </summary>
    public RaftSystemRequest(ClusterMembership gossipedRoster)
    {
        Type = RaftSystemRequestType.ApplyGossipRoster;
        GossipedRoster = gossipedRoster;
    }

    /// <summary>
    /// Constructor for <see cref="RaftSystemRequestType.ApplyGossipLoadReport"/> requests.
    /// </summary>
    public RaftSystemRequest(NodeLoadReport gossipedLoadReport)
    {
        Type = RaftSystemRequestType.ApplyGossipLoadReport;
        GossipedLoadReport = gossipedLoadReport;
    }

    /// <summary>
    /// Constructor for <see cref="RaftSystemRequestType.AddMember"/>,
    /// <see cref="RaftSystemRequestType.PromoteMember"/>, and
    /// <see cref="RaftSystemRequestType.RemoveMember"/> requests.
    /// <paramref name="expectedMembershipVersion"/> must match the current committed
    /// <see cref="ClusterMembership.MembershipVersion"/> or the coordinator rejects the change.
    /// </summary>
    public RaftSystemRequest(
        RaftSystemRequestType type,
        string endpoint,
        int nodeId,
        long expectedMembershipVersion,
        TaskCompletionSource<(RaftOperationStatus Status, long Generation)>? completion = null)
    {
        Type = type;
        MemberEndpoint = endpoint;
        MemberNodeId = nodeId;
        ExpectedMembershipVersion = expectedMembershipVersion;
        Completion = completion;
    }

    /// <summary>
    /// Constructor for <see cref="RaftSystemRequestType.CreatePartition"/> requests.
    /// Pass <paramref name="hashRangeStart"/> and <paramref name="hashRangeEnd"/> for
    /// <see cref="RaftRoutingMode.HashRange"/> partitions; leave both null for
    /// <see cref="RaftRoutingMode.Unrouted"/>.
    /// </summary>
    public RaftSystemRequest(
        int partitionId,
        RaftRoutingMode routingMode,
        int? hashRangeStart,
        int? hashRangeEnd,
        TaskCompletionSource<(RaftOperationStatus Status, long Generation)>? completion = null)
    {
        Type = RaftSystemRequestType.CreatePartition;
        PartitionId = partitionId;
        RoutingMode = routingMode;
        HashRangeStart = hashRangeStart;
        HashRangeEnd = hashRangeEnd;
        Completion = completion;
    }
}