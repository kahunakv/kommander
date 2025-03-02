
namespace Kommander.Data;

public enum RaftOperationStatus
{
    Success,
    Errored,
    NodeIsNotLeader,
    LeaderInOldTerm,
    LeaderInOutdatedTerm
}