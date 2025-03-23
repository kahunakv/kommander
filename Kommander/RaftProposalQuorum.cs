
namespace Kommander.Data;

public sealed class RaftProposalQuorum
{
    private readonly Dictionary<string, bool> nodes = [];

    private bool completed;

    public List<string> Nodes => nodes.Keys.ToList();

    public List<RaftLog> Logs { get; }
    
    public long LastLogIndex => Logs.Last().Id;

    public bool AutoCommit { get; }

    public RaftProposalQuorum(List<RaftLog> logs, bool autoCommit)
    {
        Logs = logs;
        AutoCommit = autoCommit;
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
        if (completed)
            return true;
        
        int quorum = Math.Max(2, (int)Math.Floor((nodes.Count + 1) / 2f));
        if (nodes.Values.Count(x => x) >= quorum)
        {
            completed = true;
            return true;
        }

        return false;
    }
}