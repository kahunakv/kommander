using Kommander.Data;

namespace Kommander.Communication;

public class InMemoryCommunication : ICommunication
{
    private Dictionary<string, RaftManager> nodes = new();
    
    public void SetNodes(Dictionary<string, RaftManager> nodes)
    {
        this.nodes = nodes;
    }
    
    public Task<RequestVotesResponse> RequestVotes(RaftManager manager, RaftPartition partition, RaftNode node, RequestVotesRequest request)
    {
        if (nodes.TryGetValue(node.Endpoint, out RaftManager? targetNode))
            targetNode.RequestVote(request);
        
        return Task.FromResult(new RequestVotesResponse());
    }

    public Task<VoteResponse> Vote(RaftManager manager, RaftPartition partition, RaftNode node, VoteRequest request)
    {
        if (nodes.TryGetValue(node.Endpoint, out RaftManager? targetNode))
            targetNode.Vote(request);
        
        return Task.FromResult(new VoteResponse());
    }

    public Task<AppendLogsResponse> AppendLogToNode(RaftManager manager, RaftPartition partition, RaftNode node, AppendLogsRequest request)
    {
        if (nodes.TryGetValue(node.Endpoint, out RaftManager? targetNode))
            targetNode.AppendLogs(request);
        
        return Task.FromResult(new AppendLogsResponse());
    }
}