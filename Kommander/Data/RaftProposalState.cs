namespace Kommander.Data;

public enum RaftProposalState
{
    Incomplete,
    Completed,
    Committed,
    RolledBack
}