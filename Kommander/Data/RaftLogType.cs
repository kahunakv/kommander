
namespace Kommander.Data;

public enum RaftLogType
{
    Proposed = 0,
    Committed = 1,
    ProposedCheckpoint = 2,
    CommittedCheckpoint = 3,
}
