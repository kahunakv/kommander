
namespace Lux;

public enum RaftWALActionType
{
    Ping = 0,
    Append = 1,
    AppendCheckpoint = 2,
    Recover = 3,
    Update = 4
}
