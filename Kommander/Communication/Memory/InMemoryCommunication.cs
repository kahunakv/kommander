
using Kommander.Data;

namespace Kommander.Communication.Memory;

/// <summary>
/// Allows to communicate with other nodes in the cluster via in-memory messages
/// This allows to simulate the communication between nodes without the need of a network
/// </summary>
public class InMemoryCommunication : ICommunication
{
    private readonly Task<HandshakeResponse> handshakeResponse = Task.FromResult(new HandshakeResponse());
    
    private readonly Task<RequestVotesResponse> requestVoteResponse = Task.FromResult(new RequestVotesResponse());

    private readonly Task<VoteResponse> voteResponse = Task.FromResult(new VoteResponse());
    
    private readonly Task<AppendLogsResponse> appendLogsResponse = Task.FromResult(new AppendLogsResponse());
    
    private readonly Task<CompleteAppendLogsResponse> completeAppendLogsResponse = Task.FromResult(new CompleteAppendLogsResponse());
    
    private readonly Task<AppendLogsBatchResponse> appendLogsBatchResponse = Task.FromResult(new AppendLogsBatchResponse());
    
    private readonly Task<CompleteAppendLogsBatchResponse> completeAppendLogsBatchResponse = Task.FromResult(new CompleteAppendLogsBatchResponse());
    
    private Dictionary<string, IRaft> nodes = new();
    
    public void SetNodes(Dictionary<string, IRaft> nodes)
    {
        this.nodes = nodes;
    }

    public Task<HandshakeResponse> Handshake(RaftManager manager, RaftNode node, HandshakeRequest request)
    {
        if (manager.ClusterHandler.IsNode(node.Endpoint) && nodes.TryGetValue(node.Endpoint, out IRaft? targetNode))
            targetNode.Handshake(request);
        else
            Console.WriteLine("Handshake Unknown node: " + node.Endpoint);
        
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

    public Task<AppendLogsBatchResponse> AppendLogsBatch(RaftManager manager, RaftNode node, AppendLogsBatchRequest request)
    {
        if (manager.ClusterHandler.IsNode(node.Endpoint))
        {
            if (nodes.TryGetValue(node.Endpoint, out IRaft? targetNode))
            {
                if (request.AppendLogs is not null)
                {
                    foreach (AppendLogsRequest appendLogsRequest in request.AppendLogs)
                        targetNode.AppendLogs(appendLogsRequest);
                }
            }
            else
            {
                Console.WriteLine("AppendLogsBatch Unknown node: " + node.Endpoint + " [2]");
            }
        }
        else
            Console.WriteLine("AppendLogsBatch Unknown node: " + node.Endpoint + " [1]");
        
        return appendLogsBatchResponse;
    }
    
    public Task<CompleteAppendLogsResponse> CompleteAppendLogs(RaftManager manager, RaftNode node, CompleteAppendLogsRequest request)
    {
        if (manager.ClusterHandler.IsNode(node.Endpoint) && nodes.TryGetValue(node.Endpoint, out IRaft? targetNode))
            targetNode.CompleteAppendLogs(request);
        else
            Console.WriteLine("CompleteAppendLogs Unknown node: " + node.Endpoint);
        
        return completeAppendLogsResponse;
    }
    
    public Task<CompleteAppendLogsBatchResponse> CompleteAppendLogsBatch(RaftManager manager, RaftNode node, CompleteAppendLogsBatchRequest request)
    {
        if (manager.ClusterHandler.IsNode(node.Endpoint))
        {
            if (nodes.TryGetValue(node.Endpoint, out IRaft? targetNode))
            {
                if (request.CompleteLogs is not null)
                {
                    foreach (CompleteAppendLogsRequest appendLogsRequest in request.CompleteLogs)
                        targetNode.CompleteAppendLogs(appendLogsRequest);
                }
            }
            else
            {
                Console.WriteLine("CompleteAppendLogsBatch Unknown node: " + node.Endpoint + " [2]");
            }
        }
        else
            Console.WriteLine("CompleteAppendLogsBatch Unknown node: " + node.Endpoint + " [1]");
        
        return completeAppendLogsBatchResponse;
    }
}