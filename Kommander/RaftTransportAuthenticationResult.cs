namespace Kommander;

/// <summary>
/// Result of validating a signed transport request.
/// </summary>
public sealed record RaftTransportAuthenticationResult
{
    /// <summary>
    /// Validation status.
    /// </summary>
    public required RaftTransportAuthenticationStatus Status { get; init; }

    /// <summary>
    /// Indicates whether the request authenticated successfully.
    /// </summary>
    public bool IsAuthenticated => Status == RaftTransportAuthenticationStatus.Success
        || Status == RaftTransportAuthenticationStatus.Disabled;
}
