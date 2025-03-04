
namespace Kommander.Data;

public enum RaftLogType
{
    Proposed = 0,
    Commited = 1,
    ProposedCheckpoint = 2,
    CommitedCheckpoint = 3,
}
