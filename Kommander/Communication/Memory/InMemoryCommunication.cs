
using Kommander.Data;

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
    
    public void SetNodes(Dictionary<string, IRaft> nodes)
    {
        this.nodes = nodes;
    }

    public Task<HandshakeResponse> Handshake(RaftManager manager, RaftNode node, HandshakeRequest request)
    {
        if (manager.ClusterHandler.IsNode(node.Endpoint))
        {
            if (nodes.TryGetValue(node.Endpoint, out IRaft? targetNode))
                targetNode.Handshake(request);
            else
                Console.WriteLine("{0} Handshake Unknown node: {1} [1]", manager.LocalEndpoint, node.Endpoint);
        }
        else
            Console.WriteLine("{0} Handshake Unknown node: {1} [2]", manager.LocalEndpoint, node.Endpoint);
        
        return handshakeResponse;
    }
    
    public Task<RequestVotesResponse> RequestVotes(RaftManager manager, RaftNode node, RequestVotesRequest request)
    {
        if (manager.ClusterHandler.IsNode(node.Endpoint) && nodes.TryGetValue(node.Endpoint, out IRaft? targetNode))
            targetNode.RequestVote(request);
        else
            Console.WriteLine("RequestVotes Unknown node: " + node.Endpoint);
        
        return requestVoteResponse;
    }

    public Task<VoteResponse> Vote(RaftManager manager, RaftNode node, VoteRequest request)
    {
        if (manager.ClusterHandler.IsNode(node.Endpoint) && nodes.TryGetValue(node.Endpoint, out IRaft? targetNode))
            targetNode.Vote(request);
        else
            Console.WriteLine("Vote Unknown node: " + node.Endpoint);
        
        return voteResponse;
    }

    public Task<AppendLogsResponse> AppendLogs(RaftManager manager, RaftNode node, AppendLogsRequest request)
    {
        if (manager.ClusterHandler.IsNode(node.Endpoint) && nodes.TryGetValue(node.Endpoint, out IRaft? targetNode))
            targetNode.AppendLogs(request);
        else
            Console.WriteLine("AppendLogs Unknown node: " + node.Endpoint);
        
        return appendLogsResponse;
    }
    
    public Task<CompleteAppendLogsResponse> CompleteAppendLogs(RaftManager manager, RaftNode node, CompleteAppendLogsRequest request)
    {
        if (manager.ClusterHandler.IsNode(node.Endpoint) && nodes.TryGetValue(node.Endpoint, out IRaft? targetNode))
            targetNode.CompleteAppendLogs(request);
        else
            Console.WriteLine("CompleteAppendLogs Unknown node: " + node.Endpoint);
        
        return completeAppendLogsResponse;
    }

    public async Task<BatchRequestsResponse> BatchRequests(RaftManager manager, RaftNode node, BatchRequestsRequest request)
    {
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