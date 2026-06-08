namespace Kommander;

/// <summary>
/// Validation outcome for a signed transport request.
/// </summary>
public enum RaftTransportAuthenticationStatus
{
    Success = 0,
    Disabled = 1,
    TlsRequired = 2,
    MissingFields = 3,
    MalformedFields = 4,
    InvalidSignature = 5,
    TimestampSkewExceeded = 6,
    ReplayDetected = 7
}
