
namespace Kommander;

/// <summary>
/// Thrown when an operation requires the local node's partition set but the node has not yet
/// completed cluster initialization (the system coordinator has not applied the partition map,
/// so user partitions are not constructed and <see cref="RaftManager.IsInitialized"/> is false).
/// This is a transient, retryable condition — typical right after a node restart, while it is
/// (re)joining the cluster. Callers should retry the operation, ideally against another node,
/// rather than treating it as an error.
/// </summary>
public sealed class RaftNodeNotReadyException : RaftException
{
    public RaftNodeNotReadyException(string message) : base(message)
    {

    }
}
