
namespace Kommander;

/// <summary>
/// Exception thrown by the Raft implementation.
/// </summary>
public class RaftException : Exception
{
    public RaftException(string message) : base(message)
    {

    }

    /// <summary>
    /// Wraps an underlying cause (e.g. a faulted WAL restore) so callers see the
    /// real failure rather than a generic Raft error.
    /// </summary>
    public RaftException(string message, Exception innerException) : base(message, innerException)
    {

    }
}