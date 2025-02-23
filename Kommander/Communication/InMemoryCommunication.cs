using Kommander.Data;

namespace Kommander.Communication;

public class InMemoryCommunication : ICommunication
{
    private Dictionary<string, IRaft> nodes = new();
    
    public void SetNodes(Dictionary<string, IRaft> nodes)
    {
        this.nodes = nodes;
    }
    
    public Task<RequestVotesResponse> RequestVotes(RaftManager manager, RaftPartition partition, RaftNode node, RequestVotesRequest request)
    {
        if (nodes.TryGetValue(node.Endpoint, out IRaft? targetNode))
            targetNode.RequestVote(request);
        
        return Task.FromResult(new RequestVotesResponse());
    }

    public Task<VoteResponse> Vote(RaftManager manager, RaftPartition partition, RaftNode node, VoteRequest request)
    {
        if (nodes.TryGetValue(node.Endpoint, out IRaft? targetNode))
            targetNode.Vote(request);
        
        return Task.FromResult(new VoteResponse());
    }

    public async Task<AppendLogsResponse> AppendLogToNode(RaftManager manager, RaftPartition partition, RaftNode node, AppendLogsRequest request)
    {
        long commitedIndex = -1;
        
        if (nodes.TryGetValue(node.Endpoint, out IRaft? targetNode))
            commitedIndex = await targetNode.AppendLogs(request);
        
        return new(commitedIndex);
    }
}