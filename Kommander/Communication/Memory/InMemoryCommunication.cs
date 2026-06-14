
using Kommander.Data;
using Kommander.Gossip;

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
    
    private Dictionary<string, IRaft> nodes = new();

    /// <summary>
    /// Endpoints currently "transport-paused". A message is dropped if either its sender or its
    /// target is in this set, which simulates a full (both-directions) network partition without
    /// stopping the node's timers — the node keeps running and campaigning in isolation. Used by
    /// tests/simulations to reproduce disruptive-rejoin scenarios; empty in normal operation.
    /// </summary>
    private readonly HashSet<string> partitionedEndpoints = [];

    public void SetNodes(Dictionary<string, IRaft> nodes)
    {
        this.nodes = nodes;
    }

    /// <summary>
    /// Drops all traffic to and from <paramref name="endpoint"/> until <see cref="HealPartition"/>
    /// is called, simulating a transport pause while the node itself keeps ticking.
    /// </summary>
    public void PartitionNode(string endpoint)
    {
        lock (partitionedEndpoints)
            partitionedEndpoints.Add(endpoint);
    }

    /// <summary>
    /// Restores traffic to and from <paramref name="endpoint"/> after a <see cref="PartitionNode"/> call.
    /// </summary>
    public void HealPartition(string endpoint)
    {
        lock (partitionedEndpoints)
            partitionedEndpoints.Remove(endpoint);
    }

    private bool IsPartitioned(string source, string target)
    {
        lock (partitionedEndpoints)
            return partitionedEndpoints.Count > 0 &&
                (partitionedEndpoints.Contains(source) || partitionedEndpoints.Contains(target));
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
                if (targetNode is RaftManager targetManager)
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
            return await targetManager.ReceiveLeave(request, cancellationToken).ConfigureAwait(false);

        Console.WriteLine("SendLeave Unknown node: " + node.Endpoint);
        return new LeaveResponse(false);
    }

    public Task<GossipAck> SendGossip(RaftManager manager, RaftNode node, GossipMessage digest, CancellationToken cancellationToken = default)
    {
        if (IsPartitioned(manager.LocalEndpoint, node.Endpoint))
            return Task.FromResult(new GossipAck(0, null));

        if (nodes.TryGetValue(node.Endpoint, out IRaft? targetNode) && targetNode is RaftManager targetManager)
            return Task.FromResult(targetManager.ReceiveGossip(digest));

        return Task.FromResult(new GossipAck(0, null));
    }

    public Task<Gossip.PingResponse> SendPing(RaftManager manager, RaftNode node, Gossip.PingRequest request, CancellationToken cancellationToken = default)
    {
        if (IsPartitioned(manager.LocalEndpoint, node.Endpoint))
            return Task.FromResult(new Gossip.PingResponse(false, 0));

        if (nodes.TryGetValue(node.Endpoint, out IRaft? targetNode) && targetNode is RaftManager targetManager)
            return Task.FromResult(targetManager.ReceivePing(request));

        return Task.FromResult(new Gossip.PingResponse(false, 0));
    }

    public Task<Gossip.PingReqResponse> SendPingReq(RaftManager manager, RaftNode node, Gossip.PingReqRequest request, CancellationToken cancellationToken = default)
    {
        if (IsPartitioned(manager.LocalEndpoint, node.Endpoint))
            return Task.FromResult(new Gossip.PingReqResponse(false));

        if (nodes.TryGetValue(node.Endpoint, out IRaft? targetNode) && targetNode is RaftManager targetManager)
            return targetManager.ReceivePingReq(request, cancellationToken);

        return Task.FromResult(new Gossip.PingReqResponse(false));
    }

    public async Task<long?> GetRemoteFollowerLag(RaftManager manager, RaftNode node, int partitionId, string followerEndpoint)
    {
        if (IsPartitioned(manager.LocalEndpoint, node.Endpoint))
            return null;

        if (nodes.TryGetValue(node.Endpoint, out IRaft? targetNode))
            return await targetNode.GetFollowerLagAsync(partitionId, followerEndpoint).ConfigureAwait(false);

        return null;
    }

    public async Task<JoinResponse> SendJoin(RaftManager manager, RaftNode node, JoinRequest request)
    {
        if (IsPartitioned(manager.LocalEndpoint, node.Endpoint))
            return new JoinResponse(false);

        if (nodes.TryGetValue(node.Endpoint, out IRaft? targetNode) && targetNode is RaftManager targetManager)
            return await targetManager.ReceiveJoin(request).ConfigureAwait(false);

        Console.WriteLine("SendJoin Unknown node: " + node.Endpoint);
        return new JoinResponse(false);
    }

    public async Task<SnapshotResponse> SendInstallSnapshot(RaftManager manager, RaftNode node, SnapshotRequest request, CancellationToken cancellationToken = default)
    {
        if (IsPartitioned(manager.LocalEndpoint, node.Endpoint))
            return new SnapshotResponse(false);

        if (nodes.TryGetValue(node.Endpoint, out IRaft? targetNode) && targetNode is RaftManager targetManager)
            return await targetManager.ReceiveInstallSnapshot(request, cancellationToken).ConfigureAwait(false);

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
