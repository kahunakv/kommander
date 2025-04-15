
using Kommander.Time;

namespace Kommander.Data;

public sealed class RaftProposalQuorum
{
    private readonly Dictionary<string, bool> nodes = [];

    private bool completed;
    
    public RaftProposalState State { get; private set; }

    public List<string> Nodes => nodes.Keys.ToList();

    public List<RaftLog> Logs { get; }
    
    public long LastLogIndex => Logs.Last().Id;

    public bool AutoCommit { get; }
    
    public HLCTimestamp StartTimestamp { get; }

    public RaftProposalQuorum(List<RaftLog> logs, bool autoCommit, HLCTimestamp startTimestamp)
    {
        State = RaftProposalState.Incomplete;
        
        Logs = logs;
        AutoCommit = autoCommit;
        StartTimestamp = startTimestamp;
    }
    
    public void AddExpectedNodeCompletion(string nodeId)
    {
        nodes.Add(nodeId, false);
    }
    
    public void MarkNodeCompleted(string nodeId)
    {
        nodes[nodeId] = true;
    }

    public void SetState(RaftProposalState state)
    {
        State = state;
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