
namespace Kommander.Data;

public enum RaftWALActionType
{
    Propose = 0,
    Commit = 1,
    Rollback = 2,
    Recover = 3,
    ProposeOrCommit = 4,
    GetMaxLog = 5,
    GetCurrentTerm = 6,
    GetRange = 7
}
