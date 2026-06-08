
namespace Kommander.Data;

public sealed class RaftPartitionLifecycleResult
{
    public bool Success { get; init; }
    public RaftOperationStatus Status { get; init; }
    public long Generation { get; init; }
}
