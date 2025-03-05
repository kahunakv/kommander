
namespace Kommander.Data;

public enum RaftWALActionType
{
    Propose = 0,
    Commit = 1,
    Recover = 2,
    ProposeOrCommit = 3,
    GetMaxLog = 4,
    GetCurrentTerm = 5,
    GetRange = 6
}
