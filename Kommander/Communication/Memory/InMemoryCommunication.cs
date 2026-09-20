
using System.Collections.Immutable;
using Kommander.Data;
using Kommander.Gossip;
using Microsoft.Extensions.Logging;

namespace Kommander.Communication.Memory;

/// <summary>
/// Allows to communicate with other nodes in the cluster via in-memory messages
/// This allows to simulate the communication between nodes without the need of a network
/// </summary>
public class InMemoryCommunication : ICommunication
{
    private static readonly Task<HandshakeResponse> handshakeResponse = Task.FromResult(new HandshakeResponse());
    
    private static readonly Task<RequestVotesResponse> requestVoteResponse = Task.FromResult(new RequestVotesResponse());

    private static readonly Task<VoteResponse> voteResponse = Task.FromResult(new VoteResponse());
    
    private static readonly Task<AppendLogsResponse> appendLogsResponse = Task.FromResult(new AppendLogsResponse());
    
    private static readonly Task<CompleteAppendLogsResponse> completeAppendLogsResponse = Task.FromResult(new CompleteAppendLogsResponse());
    
    /// <summary>
    /// Routing table. Volatile, and defensively copied in <see cref="SetNodes"/>: partition-executor
    /// threads read it concurrently, so a caller re-registering nodes mid-run (e.g. adding a joiner to
    /// an already-running cluster) must atomically publish a new dictionary — mutating a shared
    /// instance during a resize silently drops or corrupts RPC delivery between existing members,
    /// which manifests as spurious leadership churn.
    /// </summary>
    private volatile Dictionary<string, IRaft> nodes = new();

    /// <summary>
    /// The current delivery filters: the endpoints that <see cref="PartitionNode"/> isolated, and the
    /// directed links that <see cref="BlockLink"/> cut. Empty in normal operation.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Partition-executor threads read the filters on every message while a test or a simulation
    /// changes them. The state is one immutable object: a writer builds a new one and publishes it
    /// with a compare-and-swap, and a reader takes one volatile read. No reader waits or takes a
    /// lock, so the same code also works in the thread-free (browser) build, where nothing may block.
    /// </para>
    /// <para>
    /// When no filter is set the field holds <see cref="DeliveryFilters.None"/>, and the check on
    /// each message is one reference compare.
    /// </para>
    /// </remarks>
    private DeliveryFilters filters = DeliveryFilters.None;

    public void SetNodes(Dictionary<string, IRaft> nodes)
    {
        this.nodes = new(nodes);
    }

    /// <summary>
    /// Drops all traffic to and from <paramref name="endpoint"/> until <see cref="HealPartition"/>
    /// is called, simulating a transport pause while the node itself keeps ticking.
    /// </summary>
    /// <remarks>
    /// This isolates the node from every peer. To cut only some links, use <see cref="BlockLink"/>.
    /// The two filters are independent: a message is dropped when either one drops it, and
    /// <see cref="HealPartition"/> does not remove link blocks.
    /// </remarks>
    public void PartitionNode(string endpoint)
    {
        UpdateFilters(endpoint, static (current, e) => current with { Endpoints = current.Endpoints.Add(e) });
    }

    /// <summary>
    /// Restores traffic to and from <paramref name="endpoint"/> after a <see cref="PartitionNode"/> call.
    /// Link blocks set with <see cref="BlockLink"/> stay in place.
    /// </summary>
    public void HealPartition(string endpoint)
    {
        UpdateFilters(endpoint, static (current, e) => current with { Endpoints = current.Endpoints.Remove(e) });
    }

    /// <summary>
    /// Drops every message that <paramref name="from"/> sends to <paramref name="to"/>, until
    /// <see cref="UnblockLink"/> or <see cref="HealAll"/>. Messages from <paramref name="to"/> to
    /// <paramref name="from"/> still arrive. Use <see cref="BlockLinkBothWays"/> to cut the link
    /// in both directions.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A dropped message behaves like a message lost on the network: the sender gets the same
    /// default (failed or empty) response as for an isolated node, and nothing is logged.
    /// </para>
    /// <para>
    /// Most Raft messages are one-way: a vote, an append or an acknowledgement travels as its own
    /// message, and the answer is a separate message in the other direction. A block drops only the
    /// messages in the blocked direction. Some calls return their answer in the same call
    /// (handshake, join, leave, set member role, gossip, ping, ping-req, read index, follower lag,
    /// install snapshot and forwarded writes). For these, the request needs the
    /// <paramref name="from"/> → <paramref name="to"/> direction and the answer needs the other
    /// direction. If only the answer direction is blocked, the target still processes the request,
    /// but the caller gets the failure response. This is the same as a real lost reply: for
    /// example, a snapshot can be installed while the leader thinks the transfer failed.
    /// </para>
    /// <para>
    /// Known liveness behavior of one-way blocks. A block applies to every partition, not only one.
    /// A follower that receives from the leader but cannot send to it (<c>BlockLink(follower, leader)</c>)
    /// stays a follower: its acknowledgements are lost, so the leader counts it as lagging and commits
    /// with the other voters. A follower that cannot receive from the leader but can send to it
    /// (<c>BlockLink(leader, follower)</c>) hears no heartbeats, so it starts a pre-vote round on every
    /// leader check (in <c>TestInMemoryLinkBlocks</c>, about 84 rounds in 3 seconds). The other voters
    /// still hear the leader and refuse the pre-vote, so no term goes up and the leader stays. The cost
    /// is that the follower gets no new entries until the block is removed, and its pre-vote messages
    /// continue for the whole block. If the leader is cut one way from a majority of voters, those
    /// voters elect a new leader. Unless <c>EnableCheckQuorum</c> is on, the old leader thinks that it
    /// is still the leader until it learns the new term, but it cannot commit.
    /// </para>
    /// </remarks>
    public void BlockLink(string from, string to)
    {
        UpdateFilters((from, to), static (current, link) => current with { Links = current.Links.Add(link) });
    }

    /// <summary>
    /// Restores the <paramref name="from"/> → <paramref name="to"/> direction after a
    /// <see cref="BlockLink"/> call. The other direction and node isolation are not changed.
    /// </summary>
    public void UnblockLink(string from, string to)
    {
        UpdateFilters((from, to), static (current, link) => current with { Links = current.Links.Remove(link) });
    }

    /// <summary>
    /// Cuts the link between <paramref name="a"/> and <paramref name="b"/> in both directions: a
    /// network partition between exactly these two nodes. Their links to other nodes still work.
    /// </summary>
    public void BlockLinkBothWays(string a, string b)
    {
        UpdateFilters((a, b), static (current, link) => current with
        {
            Links = current.Links.Add(link).Add((link.Item2, link.Item1))
        });
    }

    /// <summary>
    /// Restores both directions of the link between <paramref name="a"/> and <paramref name="b"/>.
    /// </summary>
    public void UnblockLinkBothWays(string a, string b)
    {
        UpdateFilters((a, b), static (current, link) => current with
        {
            Links = current.Links.Remove(link).Remove((link.Item2, link.Item1))
        });
    }

    /// <summary>
    /// Removes every delivery filter: all node isolations from <see cref="PartitionNode"/> and all
    /// link blocks from <see cref="BlockLink"/>.
    /// </summary>
    public void HealAll()
    {
        Interlocked.Exchange(ref filters, DeliveryFilters.None);
    }

    /// <summary>
    /// Returns true when a message from <paramref name="from"/> to <paramref name="to"/> is dropped
    /// now, because either endpoint is isolated or the directed link is blocked.
    /// </summary>
    public bool IsDeliveryBlocked(string from, string to)
    {
        return IsPartitioned(from, to);
    }

    /// <summary>
    /// Applies <paramref name="change"/> to the current filters and publishes the result. The
    /// compare-and-swap loop keeps two concurrent writers from losing each other's change.
    /// </summary>
    private void UpdateFilters<TArg>(TArg arg, Func<DeliveryFilters, TArg, DeliveryFilters> change)
    {
        while (true)
        {
            DeliveryFilters current = Volatile.Read(ref filters);
            DeliveryFilters next = change(current, arg);

            if (next.Endpoints.IsEmpty && next.Links.IsEmpty)
                next = DeliveryFilters.None;

            if (ReferenceEquals(Interlocked.CompareExchange(ref filters, next, current), current))
                return;
        }
    }

    private bool IsPartitioned(string source, string target)
    {
        DeliveryFilters current = Volatile.Read(ref filters);

        if (ReferenceEquals(current, DeliveryFilters.None))
            return false;

        return current.Endpoints.Contains(source)
            || current.Endpoints.Contains(target)
            || current.Links.Contains((source, target));
    }

    /// <summary>
    /// For a call that returns its answer in the same call: true when the request or the answer is
    /// dropped. The caller checks the request direction before the call and the answer direction
    /// after it, so a blocked answer still lets the target process the request.
    /// </summary>
    private bool IsReplyBlocked(string source, string target)
    {
        return IsPartitioned(target, source);
    }

    /// <summary>
    /// Immutable snapshot of the delivery filters. <see cref="None"/> is the one instance that
    /// means "no filter", so the hot path can test it by reference.
    /// </summary>
    private sealed record DeliveryFilters(ImmutableHashSet<string> Endpoints, ImmutableHashSet<(string From, string To)> Links)
    {
        public static readonly DeliveryFilters None = new(
            ImmutableHashSet<string>.Empty,
            ImmutableHashSet<(string From, string To)>.Empty);
    }

    public Task<HandshakeResponse> Handshake(RaftManager manager, RaftNode node, HandshakeRequest request)
    {
        if (IsPartitioned(manager.LocalEndpoint, node.Endpoint))
            return handshakeResponse;

        if (manager.ClusterHandler.IsNode(node.Endpoint))
        {
            if (nodes.TryGetValue(node.Endpoint, out IRaft? targetNode))
            {
                targetNode.Handshake(request);
                if (targetNode is RaftManager targetManager && !IsReplyBlocked(manager.LocalEndpoint, node.Endpoint))
                    return Task.FromResult(targetManager.GetHandshakeResponse(request.Partition));
            }
            else
                Console.WriteLine("{0} Handshake Unknown node: {1} [1]", manager.LocalEndpoint, node.Endpoint);
        }
        else
            Console.WriteLine("{0} Handshake Unknown node: {1} [2]", manager.LocalEndpoint, node.Endpoint);
        
        return handshakeResponse;
    }
    
    public Task<RequestVotesResponse> RequestVotes(RaftManager manager, RaftNode node, RequestVotesRequest request)
    {
        if (IsPartitioned(manager.LocalEndpoint, node.Endpoint))
            return requestVoteResponse;

        if (manager.ClusterHandler.IsNode(node.Endpoint) && nodes.TryGetValue(node.Endpoint, out IRaft? targetNode))
            targetNode.RequestVote(request);
        else
            Console.WriteLine("RequestVotes Unknown node: " + node.Endpoint);
        
        return requestVoteResponse;
    }

    public Task<VoteResponse> Vote(RaftManager manager, RaftNode node, VoteRequest request)
    {
        if (IsPartitioned(manager.LocalEndpoint, node.Endpoint))
            return voteResponse;

        if (manager.ClusterHandler.IsNode(node.Endpoint) && nodes.TryGetValue(node.Endpoint, out IRaft? targetNode))
            targetNode.Vote(request);
        else
            Console.WriteLine("Vote Unknown node: " + node.Endpoint);
        
        return voteResponse;
    }

    public Task<AppendLogsResponse> AppendLogs(RaftManager manager, RaftNode node, AppendLogsRequest request)
    {
        if (IsPartitioned(manager.LocalEndpoint, node.Endpoint))
            return appendLogsResponse;

        if (manager.ClusterHandler.IsNode(node.Endpoint) && nodes.TryGetValue(node.Endpoint, out IRaft? targetNode))
            targetNode.AppendLogs(request);
        else
            Console.WriteLine("AppendLogs Unknown node: " + node.Endpoint);
        
        return appendLogsResponse;
    }
    
    public Task<CompleteAppendLogsResponse> CompleteAppendLogs(RaftManager manager, RaftNode node, CompleteAppendLogsRequest request)
    {
        if (IsPartitioned(manager.LocalEndpoint, node.Endpoint))
            return completeAppendLogsResponse;

        if (manager.ClusterHandler.IsNode(node.Endpoint) && nodes.TryGetValue(node.Endpoint, out IRaft? targetNode))
            targetNode.CompleteAppendLogs(request);
        else
            Console.WriteLine("CompleteAppendLogs Unknown node: " + node.Endpoint);
        
        return completeAppendLogsResponse;
    }

    public async Task<LeaveResponse> SendLeave(RaftManager manager, RaftNode node, LeaveRequest request, CancellationToken cancellationToken = default)
    {
        if (IsPartitioned(manager.LocalEndpoint, node.Endpoint))
            return new LeaveResponse(false);

        if (nodes.TryGetValue(node.Endpoint, out IRaft? targetNode) && targetNode is RaftManager targetManager)
        {
            LeaveResponse response = await targetManager.ReceiveLeave(request, cancellationToken).ConfigureAwait(false);
            return IsReplyBlocked(manager.LocalEndpoint, node.Endpoint) ? new LeaveResponse(false) : response;
        }

        Console.WriteLine("SendLeave Unknown node: " + node.Endpoint);
        return new LeaveResponse(false);
    }

    public async Task<SetMemberRoleResponse> SendSetMemberRole(RaftManager manager, RaftNode node, SetMemberRoleRequest request, CancellationToken cancellationToken = default)
    {
        if (IsPartitioned(manager.LocalEndpoint, node.Endpoint))
            return new SetMemberRoleResponse(false, Status: RaftOperationStatus.Errored);

        if (nodes.TryGetValue(node.Endpoint, out IRaft? targetNode) && targetNode is RaftManager targetManager)
        {
            SetMemberRoleResponse response = await targetManager.ReceiveSetMemberRole(request, cancellationToken).ConfigureAwait(false);
            return IsReplyBlocked(manager.LocalEndpoint, node.Endpoint)
                ? new SetMemberRoleResponse(false, Status: RaftOperationStatus.Errored)
                : response;
        }

        Console.WriteLine("SendSetMemberRole Unknown node: " + node.Endpoint);
        return new SetMemberRoleResponse(false, Status: RaftOperationStatus.Errored);
    }

    public Task<GossipAck> SendGossip(RaftManager manager, RaftNode node, GossipMessage digest, CancellationToken cancellationToken = default)
    {
        if (IsPartitioned(manager.LocalEndpoint, node.Endpoint))
            return Task.FromResult(new GossipAck(0, null));

        if (nodes.TryGetValue(node.Endpoint, out IRaft? targetNode) && targetNode is RaftManager targetManager)
        {
            GossipAck ack = targetManager.ReceiveGossip(digest);
            return Task.FromResult(IsReplyBlocked(manager.LocalEndpoint, node.Endpoint) ? new GossipAck(0, null) : ack);
        }

        return Task.FromResult(new GossipAck(0, null));
    }

    public Task<Gossip.PingResponse> SendPing(RaftManager manager, RaftNode node, Gossip.PingRequest request, CancellationToken cancellationToken = default)
    {
        if (IsPartitioned(manager.LocalEndpoint, node.Endpoint))
            return Task.FromResult(new Gossip.PingResponse(false, 0));

        if (nodes.TryGetValue(node.Endpoint, out IRaft? targetNode) && targetNode is RaftManager targetManager)
        {
            Gossip.PingResponse response = targetManager.ReceivePing(request);
            return Task.FromResult(IsReplyBlocked(manager.LocalEndpoint, node.Endpoint) ? new Gossip.PingResponse(false, 0) : response);
        }

        return Task.FromResult(new Gossip.PingResponse(false, 0));
    }

    public Task<Gossip.PingReqResponse> SendPingReq(RaftManager manager, RaftNode node, Gossip.PingReqRequest request, CancellationToken cancellationToken = default)
    {
        if (IsPartitioned(manager.LocalEndpoint, node.Endpoint))
            return Task.FromResult(new Gossip.PingReqResponse(false));

        if (nodes.TryGetValue(node.Endpoint, out IRaft? targetNode) && targetNode is RaftManager targetManager)
            return SendPingReqCore(manager, node, targetManager, request, cancellationToken);

        return Task.FromResult(new Gossip.PingReqResponse(false));
    }

    private async Task<Gossip.PingReqResponse> SendPingReqCore(
        RaftManager manager, RaftNode node, RaftManager targetManager, Gossip.PingReqRequest request, CancellationToken cancellationToken)
    {
        Gossip.PingReqResponse response = await targetManager.ReceivePingReq(request, cancellationToken).ConfigureAwait(false);
        return IsReplyBlocked(manager.LocalEndpoint, node.Endpoint) ? new Gossip.PingReqResponse(false) : response;
    }

    /// <summary>
    /// In-process follower read-index fetch: routes the request to the target node's
    /// <see cref="RaftManager.ReceiveGetReadIndex"/>, whose read-index round supplies the
    /// leadership proof. A partitioned or unknown target returns a failed response — the
    /// caller must fail closed exactly as it would on a network error.
    /// </summary>
    public async Task<GetReadIndexResponse> GetReadIndex(RaftManager manager, RaftNode node, GetReadIndexRequest request, CancellationToken cancellationToken = default)
    {
        if (IsPartitioned(manager.LocalEndpoint, node.Endpoint))
            return new GetReadIndexResponse(false);

        if (nodes.TryGetValue(node.Endpoint, out IRaft? targetNode) && targetNode is RaftManager targetManager)
        {
            GetReadIndexResponse response = await targetManager.ReceiveGetReadIndex(request, cancellationToken).ConfigureAwait(false);
            return IsReplyBlocked(manager.LocalEndpoint, node.Endpoint) ? new GetReadIndexResponse(false) : response;
        }

        return new GetReadIndexResponse(false);
    }

    public async Task<long?> GetRemoteFollowerLag(RaftManager manager, RaftNode node, int partitionId, string followerEndpoint)
    {
        if (IsPartitioned(manager.LocalEndpoint, node.Endpoint))
            return null;

        if (nodes.TryGetValue(node.Endpoint, out IRaft? targetNode))
        {
            long? lag = await targetNode.GetFollowerLagAsync(partitionId, followerEndpoint).ConfigureAwait(false);
            return IsReplyBlocked(manager.LocalEndpoint, node.Endpoint) ? null : lag;
        }

        return null;
    }

    public async Task<JoinResponse> SendJoin(RaftManager manager, RaftNode node, JoinRequest request)
    {
        if (IsPartitioned(manager.LocalEndpoint, node.Endpoint))
            return new JoinResponse(false);

        if (nodes.TryGetValue(node.Endpoint, out IRaft? targetNode) && targetNode is RaftManager targetManager)
        {
            JoinResponse response = await targetManager.ReceiveJoin(request).ConfigureAwait(false);
            return IsReplyBlocked(manager.LocalEndpoint, node.Endpoint) ? new JoinResponse(false) : response;
        }

        Console.WriteLine("SendJoin Unknown node: " + node.Endpoint);
        return new JoinResponse(false);
    }

    public async Task<SnapshotResponse> SendInstallSnapshot(RaftManager manager, RaftNode node, SnapshotRequest request, CancellationToken cancellationToken = default)
    {
        if (IsPartitioned(manager.LocalEndpoint, node.Endpoint))
            return new SnapshotResponse(false);

        if (nodes.TryGetValue(node.Endpoint, out IRaft? targetNode) && targetNode is RaftManager targetManager)
        {
            SnapshotResponse response = await targetManager.ReceiveInstallSnapshot(request, cancellationToken).ConfigureAwait(false);
            return IsReplyBlocked(manager.LocalEndpoint, node.Endpoint) ? new SnapshotResponse(false) : response;
        }

        return new SnapshotResponse(false);
    }

    /// <summary>
    /// Routes the terminal join-blocked reason directly to the target node's in-process
    /// <see cref="RaftManager.SetJoinTerminalReason"/> so the joiner's <c>JoinCluster(seeds)</c>
    /// loop can throw <see cref="System.InvalidOperationException"/> immediately.
    /// </summary>
    public Task NotifyJoinBlocked(RaftManager manager, string targetEndpoint, string reason, CancellationToken cancellationToken = default)
    {
        if (!IsPartitioned(manager.LocalEndpoint, targetEndpoint)
            && nodes.TryGetValue(targetEndpoint, out IRaft? targetNode)
            && targetNode is RaftManager targetManager)
        {
            targetManager.SetJoinTerminalReason(targetEndpoint, reason);
        }

        return Task.CompletedTask;
    }

    /// <summary>
    /// In-process forwarding: runs the proposal through the target node's own
    /// <c>ReplicateLogs</c> path so leader checks and the generation fence apply there.
    /// Returns null when the target is unknown or transport-partitioned, which the caller
    /// treats the same as an unreachable replica (try the next one).
    /// </summary>
    public async Task<RaftReplicationResult?> ForwardReplicateLogs(
        RaftManager manager, RaftNode node, int partitionId, string type,
        IReadOnlyList<byte[]> logs, bool autoCommit, long expectedGeneration, long expectedTerm,
        CancellationToken cancellationToken = default)
    {
        if (IsPartitioned(manager.LocalEndpoint, node.Endpoint) || !nodes.TryGetValue(node.Endpoint, out IRaft? targetNode))
            return null;

        RaftReplicationResult result = await targetNode.ReplicateLogs(
            partitionId, type, logs, autoCommit, expectedGeneration, expectedTerm, cancellationToken
        ).ConfigureAwait(false);

        // A lost reply after the target accepted the write: the caller sees an unreachable replica,
        // as on a real network. The write may still commit, so a retry can duplicate it.
        return IsReplyBlocked(manager.LocalEndpoint, node.Endpoint) ? null : result;
    }

    public async Task<BatchRequestsResponse> BatchRequests(RaftManager manager, RaftNode node, BatchRequestsRequest request)
    {
        if (IsPartitioned(manager.LocalEndpoint, node.Endpoint))
            return new();

        if (manager.ClusterHandler.IsNode(node.Endpoint))
        {
            if (nodes.TryGetValue(node.Endpoint, out IRaft? targetNode))
            {
                if (request.Requests is not null)
                {
                    foreach (BatchRequestsRequestItem item in request.Requests)
                    {
                        // Guard each item independently: a throw on one item (e.g. a vote for a
                        // data partition the target has not created yet) must never abort the
                        // remaining items in the batch, which may target the system partition or
                        // an already-live partition. Matches the per-item resilience in the gRPC
                        // batch handler (RaftService.BatchRequests).
                        try
                        {
                            switch (item.Type)
                            {
                                case BatchRequestsRequestType.Handshake:
                                    await targetNode.Handshake(item.Handshake!);
                                    break;

                                case BatchRequestsRequestType.Vote:
                                    targetNode.Vote(item.Vote!);
                                    break;

                                case BatchRequestsRequestType.RequestVote:
                                    targetNode.RequestVote(item.RequestVotes!);
                                    break;

                                case BatchRequestsRequestType.StepDownNotice:
                                    if (targetNode is RaftManager targetManager)
                                        targetManager.StepDownNotice(item.StepDownNotice!);
                                    break;

                                case BatchRequestsRequestType.TransferLeadership:
                                    if (targetNode is RaftManager transferManager)
                                        transferManager.TransferLeadership(item.TransferLeadership!);
                                    break;

                                case BatchRequestsRequestType.TransferLeadershipSuggestion:
                                    if (targetNode is RaftManager suggestionManager)
                                        suggestionManager.ReceiveTransferLeadershipSuggestion(item.TransferLeadershipSuggestion!);
                                    break;

                                case BatchRequestsRequestType.AppendLogs:
                                    targetNode.AppendLogs(item.AppendLogs!);
                                    break;

                                case BatchRequestsRequestType.CompleteAppendLogs:
                                    targetNode.CompleteAppendLogs(item.CompleteAppendLogs!);
                                    break;

                                default:
                                    throw new ArgumentOutOfRangeException();
                            }
                        }
                        catch (Exception ex)
                        {
                            manager.Logger.LogError("BatchRequests: {Type} {Message}\n{StackTrace}", ex.GetType().Name, ex.Message, ex.StackTrace);
                        }
                    }
                }
            }
            else
            {
                Console.WriteLine("CompleteAppendLogsBatch Unknown node: {0} [2]", node.Endpoint);
            }
        }
        else
            Console.WriteLine("CompleteAppendLogsBatch Unknown node: {0} [1]", node.Endpoint);

        return new();
    }
}
