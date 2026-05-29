
using Kommander.Data;

namespace Kommander.Scheduling;

/// <summary>
/// Correlates asynchronous state-machine completions back to callers without
/// coupling the state machine to Nixie actor message types.
/// </summary>
public interface IRaftOperationReplySink
{
    /// <summary>
    /// Completes the caller identified by <paramref name="correlationId"/>.
    /// </summary>
    void TryComplete(ulong correlationId, RaftResponse response);
}
