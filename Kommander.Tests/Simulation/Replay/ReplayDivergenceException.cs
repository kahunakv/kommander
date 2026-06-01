namespace Kommander.Tests.Simulation.Replay;

/// <summary>
/// Raised when a replay run diverges from the recorded enabled-event set or event choices.
/// </summary>
public sealed class ReplayDivergenceException : Exception
{
    public ReplayDivergenceException(string message) : base(message)
    {
    }

    public ReplayDivergenceException(string message, Exception innerException) : base(message, innerException)
    {
    }
}
