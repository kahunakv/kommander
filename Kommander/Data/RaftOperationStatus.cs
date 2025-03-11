
namespace Kommander.Data;

public enum RaftOperationStatus
{
    Success = 0,
    Errored = 1,
    NodeIsNotLeader = 2,
    LeaderInOldTerm = 3,
    LeaderAlreadyElected = 4,
    LogsFromAnotherLeader = 5,
    ActiveProposal = 6,
}