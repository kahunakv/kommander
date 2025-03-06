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
    
    private Dictionary<string, IRaft> nodes = new();
    
    public void SetNodes(Dictionary<string, IRaft> nodes)
    {
        this.nodes = nodes;
    }
    
    public Task<RequestVotesResponse> RequestVotes(RaftManager manager, RaftPartition partition, RaftNode node, RequestVotesRequest request)
    {
        if (nodes.TryGetValue(node.Endpoint, out IRaft? targetNode))
            _ = targetNode.RequestVote(request);
        
        return requestVoteResponse;
    }

    public Task<VoteResponse> Vote(RaftManager manager, RaftPartition partition, RaftNode node, VoteRequest request)
    {
        if (nodes.TryGetValue(node.Endpoint, out IRaft? targetNode))
            _ = targetNode.Vote(request);
        
        return voteResponse;
    }

    public async Task<AppendLogsResponse> AppendLogToNode(RaftManager manager, RaftPartition partition, RaftNode node, AppendLogsRequest request)
    {
        long commitedIndex = -1;
        RaftOperationStatus status = RaftOperationStatus.Success;
        
        if (nodes.TryGetValue(node.Endpoint, out IRaft? targetNode))
            (status, commitedIndex) = await targetNode.AppendLogs(request);
        
        return new(status, commitedIndex);
    }
}