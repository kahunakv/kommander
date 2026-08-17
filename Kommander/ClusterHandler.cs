
using Kommander.Discovery;
using Kommander.System;

namespace Kommander;

/// <summary>
/// Manages the operations of joining and leaving a cluster.
/// Keeps state about the current cluster and node status.
/// </summary>
public sealed class ClusterHandler
{
    public bool Joined { get; private set; }
    
    private readonly RaftManager manager;
    
    private readonly IDiscovery discovery;

    public ClusterHandler(RaftManager manager, IDiscovery discovery)
    {
        this.manager = manager;
        this.discovery = discovery;
    }

    /// <summary>
    /// Joins a cluster by registering the current node with the discovery service
    /// and marking the node as part of a cluster.
    /// </summary>
    /// <param name="configuration">The configuration details required to join the cluster.</param>
    /// <returns>A task representing the asynchronous join operation.</returns>
    public async Task JoinCluster(RaftConfiguration configuration)
    {
        await discovery.Register(configuration).ConfigureAwait(false);
        Joined = true;
    }

    /// <summary>
    /// Marks this node as having joined the cluster without registering with discovery.
    /// Used by the seed-based <c>JoinCluster(seeds)</c> path where the roster entry,
    /// not discovery registration, is the authoritative join signal.
    /// </summary>
    internal void MarkJoined() => Joined = true;

    /// <summary>
    /// Leaves the cluster by unregistering the current node from the discovery service
    /// and marking the node as no longer part of the cluster.
    /// </summary>
    /// <param name="configuration">The configuration details required to leave the cluster.</param>
    /// <returns>A task representing the asynchronous leave operation.</returns>
    public async Task LeaveCluster(RaftConfiguration configuration)
    {
        await discovery.Register(configuration).ConfigureAwait(false);
        Joined = false;
    }

    /// <summary>
    /// Updates <see cref="RaftManager.Nodes"/> from the committed cluster roster.
    /// Once the P0 leader has seeded the roster (MembershipVersion &gt; 0) every call
    /// here derives the peer set from committed voters, excluding self, so quorum math
    /// is always based on the same source of truth as the membership record.
    /// <para>
    /// Before the initial seed (pre-join transient or before the first P0 election),
    /// the roster is empty and this method falls back to <see cref="IDiscovery.GetNodes"/>
    /// exactly once, preserving today's static-discovery bootstrap behavior.
    /// </para>
    /// </summary>
    public Task UpdateNodes()
    {
        ClusterMembership roster = manager.SystemCoordinator.GetMembership();

        if (roster.MembershipVersion > 0)
        {
            // Roster is committed — include Voters, Learners, and Leaving members (excluding
            // self) so that the leader sends heartbeats and replication traffic to joining and
            // departing nodes alike. Learners participate in log replication but not in quorum
            // calculation.
            //
            // Leaving looks like a correctness downgrade here but is the opposite: a draining
            // node must stay in every peer list or the decommission wedges. The instant Leaving
            // commits, the placement pass adds learners to evacuate the leaver's ranges — and
            // those learners catch up FROM the leaver (or from a leader that still replicates to
            // it). Dropping it from Nodes severs heartbeats and replication both ways, the
            // evacuating learner never reaches the promotion lag, and the drain stalls forever.
            // Quorum safety is unaffected: roster-level quorums count Voter roles only, and
            // per-range quorums resolve against the committed replica set, which still names the
            // leaver until its replica is explicitly dropped.
            manager.Nodes = roster.Members
                .Where(m => (m.Role == ClusterMemberRole.Voter || m.Role == ClusterMemberRole.Learner || m.Role == ClusterMemberRole.Leaving)
                         && m.Endpoint != manager.LocalEndpoint)
                .Select(m => new RaftNode(m.Endpoint))
                .ToList();
        }
        else
        {
            // Pre-seed transient: no roster committed yet — fall back to discovery.
            manager.Nodes = discovery.GetNodes();
        }

        return Task.CompletedTask;
    }

    /// <summary>
    /// Determines if a specified endpoint matches any node in the cluster.
    /// </summary>
    /// <param name="endpoint">The endpoint to check for membership in the cluster.</param>
    /// <returns>True if the endpoint corresponds to a node in the cluster; otherwise, false.</returns>
    public bool IsNode(string endpoint)
    {
        foreach (RaftNode node in manager.Nodes)
        {
            if (node.Endpoint == endpoint)
                return true;
        }

        return false;
    }
}