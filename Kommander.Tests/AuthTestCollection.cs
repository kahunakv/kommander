namespace Kommander.Tests;

/// <summary>
/// Serializes tests that share the static <c>RaftTransportAuthenticator.ReplayCache</c>.
/// Parallel execution across these classes causes races: one class's
/// <c>ResetReplayCacheForTesting()</c> call can clear a nonce that another class's
/// in-flight test is relying on for replay detection.
/// </summary>
[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class AuthTestCollection
{
    public const string Name = "Auth";
}
