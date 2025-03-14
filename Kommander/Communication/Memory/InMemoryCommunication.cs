using Kommander.Data;

namespace Kommander.Communication.Memory;

/// <summary>
/// Allows to communicate with other nodes in the cluster via in-memory messages
/// This allows to simulate the communication between nodes without the need of a network
/// </summary>
public class InMemoryCommunication : ICommunication
{
    private readonly Task<RequestVotesResponse> requestVoteResponse = Task.FromResult(new RequestVotesResponse());

    private readonly Task<VoteResponse> voteResponse = Task.FromResult(new VoteResponse());
    
    private readonly Task<AppendLogsResponse> appendLogsResponse = Task.FromResult(new AppendLogsResponse());
    
    private readonly Task<CompleteAppendLogsResponse> completeAppendLogsResponse = Task.FromResult(new CompleteAppendLogsResponse());
    
    private Dictionary<string, IRaft> nodes = new();
    
    public void SetNodes(Dictionary<string, IRaft> nodes)
    {
        this.nodes = nodes;
    }
    
    public Task<RequestVotesResponse> RequestVotes(RaftManager manager, RaftPartition partition, RaftNode node, RequestVotesRequest request)
    {
        if (manager.ClusterHandler.IsNode(node.Endpoint) && nodes.TryGetValue(node.Endpoint, out IRaft? targetNode))
            targetNode.RequestVote(request);
        else
            Console.WriteLine("Unknown node: " + node.Endpoint);
        
        return requestVoteResponse;
    }

    public Task<VoteResponse> Vote(RaftManager manager, RaftPartition partition, RaftNode node, VoteRequest request)
    {
        if (manager.ClusterHandler.IsNode(node.Endpoint) && nodes.TryGetValue(node.Endpoint, out IRaft? targetNode))
            targetNode.Vote(request);
        else
            Console.WriteLine("Unknown node: " + node.Endpoint);
        
        return voteResponse;
    }

    public Task<AppendLogsResponse> AppendLogs(RaftManager manager, RaftPartition partition, RaftNode node, AppendLogsRequest request)
    {
        if (manager.ClusterHandler.IsNode(node.Endpoint) && nodes.TryGetValue(node.Endpoint, out IRaft? targetNode))
            targetNode.AppendLogs(request);
        else
            Console.WriteLine("Unknown node: " + node.Endpoint);
        
        return appendLogsResponse;
    }
    
    public Task<CompleteAppendLogsResponse> CompleteAppendLogs(RaftManager manager, RaftPartition partition, RaftNode node, CompleteAppendLogsRequest request)
    {
        if (manager.ClusterHandler.IsNode(node.Endpoint) && nodes.TryGetValue(node.Endpoint, out IRaft? targetNode))
            targetNode.CompleteAppendLogs(request);
        else
            Console.WriteLine("Unknown node: " + node.Endpoint);
        
        return completeAppendLogsResponse;
    }
}