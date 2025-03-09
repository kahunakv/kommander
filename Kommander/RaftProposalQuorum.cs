namespace Kommander.Data;

public sealed class RaftProposalQuorum
{
    private readonly Dictionary<string, bool> nodes = [];

    public List<string> Nodes => nodes.Keys.ToList();

    public List<RaftLog> Logs { get; }
    
    public long LastLogIndex => Logs.Last().Id;

    public RaftProposalQuorum(List<RaftLog> logs)
    {
        Logs = logs;
    }
    
    public void AddExpectedCompletion(string nodeId)
    {
        nodes.Add(nodeId, false);
    }
    
    public void MarkCompleted(string nodeId)
    {
        nodes[nodeId] = true;
    }
    
    public bool HasQuorum()
    {
        int quorum = Math.Max(2, (int)Math.Floor((nodes.Count + 1) / 2f));
        return nodes.Values.Count(x => x) >= quorum;
    }
}