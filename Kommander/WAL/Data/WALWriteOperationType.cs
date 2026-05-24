namespace Kommander.WAL.Data;

public enum WALWriteOperationType
{
    LeaderPropose,
    LeaderCommit,
    LeaderRollback,
    FollowerAppend,
    Compaction
}
