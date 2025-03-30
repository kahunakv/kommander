
namespace Kommander.System;

public enum RaftSystemRequestType
{
    LeaderChanged,
    RestoreCompleted,
    ConfigRestored,
    ConfigReplicated,
}