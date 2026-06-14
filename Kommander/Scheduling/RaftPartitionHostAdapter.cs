
using Kommander.Data;
using Kommander.System;
using Kommander.Time;

namespace Kommander;

/// <summary>
/// Bridges <see cref="RaftManager"/> and <see cref="RaftPartition"/> to
/// <see cref="Scheduling.IRaftPartitionHost"/> for use by <see cref="RaftPartitionStateMachine"/>.
/// </summary>
internal sealed class RaftPartitionHostAdapter : Scheduling.IRaftPartitionHost
{
    private readonly RaftManager manager;
    private readonly RaftPartition partition;

    public RaftPartitionHostAdapter(RaftManager manager, RaftPartition partition)
    {
        this.manager = manager;
        this.partition = partition;
    }

    public int PartitionId => partition.PartitionId;

    public string Leader
    {
        get => partition.Leader;
        set => partition.Leader = value;
    }

    public string LocalEndpoint => manager.LocalEndpoint;

    public int LocalNodeId => manager.LocalNodeId;

    public ClusterMemberRole LocalRole => manager.LocalRole;

    public bool IsVoter(string endpoint)
    {
        ClusterMembership roster = manager.SystemCoordinator.GetMembership();
        if (roster.MembershipVersion == 0)
            return true; // pre-seed: treat all known peers as voters (backward compat)
        return roster.Members.Any(m => m.Endpoint == endpoint && m.Role == ClusterMemberRole.Voter);
    }

    public RaftConfiguration Configuration => manager.Configuration;

    public HybridLogicalClock HybridLogicalClock => manager.HybridLogicalClock;

    public IReadOnlyList<RaftNode> Nodes => manager.Nodes;

    public HLCTimestamp GetLastNodeActivity(string endpoint, int partitionId) => manager.GetLastNodeActivity(endpoint, partitionId);

    public HLCTimestamp GetLastNodeHearthbeat(string endpoint, int partitionId) => manager.GetLastNodeHearthbeat(endpoint, partitionId);

    public void UpdateLastHeartbeat(string endpoint, int partitionId, HLCTimestamp timestamp) => manager.UpdateLastHeartbeat(endpoint, partitionId, timestamp);

    public void UpdateLastNodeActivity(string endpoint, int partitionId, HLCTimestamp timestamp) => manager.UpdateLastNodeActivity(endpoint, partitionId, timestamp);

    public void EnqueueResponse(string endpoint, RaftResponderRequest request) => manager.EnqueueResponse(endpoint, request);

    public Task InvokeLeaderChanged(int partitionId, string leader) => manager.InvokeLeaderChanged(partitionId, leader);

    public Task<bool> InvokeReplicationReceived(int partitionId, RaftLog log) => manager.InvokeReplicationReceived(partitionId, log);

    public Task<bool> InvokeSystemReplicationReceived(int partitionId, RaftLog log) => manager.InvokeSystemReplicationReceived(partitionId, log);

    public void InvokeReplicationError(int partitionId, RaftLog log) => manager.InvokeReplicationError(partitionId, log);

    public IRaftStateMachineTransfer? StateMachineTransfer => manager.StateMachineTransfer;

    public Task<SnapshotResponse> SendInstallSnapshotAsync(RaftNode node, SnapshotRequest request, CancellationToken ct) =>
        manager.Communication.SendInstallSnapshot(manager, node, request, ct);
}
