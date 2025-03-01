
namespace Kommander;

/// <summary>
/// Exception thrown by the Raft implementation.
/// </summary>
public sealed class RaftException : Exception
{
    public RaftException(string message) : base(message)
    {

    }
}