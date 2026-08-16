namespace Kommander;

/// <summary>
/// Validation outcome for a signed transport request.
/// </summary>
/// <remarks>
/// <see cref="RaftTransportAuthenticationResult.IsAuthenticated"/> is an allow-list over this
/// enum (<c>Success</c> or <c>Disabled</c>), so any value added here is treated as a rejection
/// unless it is explicitly added to that list. New failure modes therefore fail closed by
/// default, which is the property the MutualTls path depends on.
/// </remarks>
public enum RaftTransportAuthenticationStatus
{
    Success = 0,
    Disabled = 1,
    TlsRequired = 2,
    MissingFields = 3,
    MalformedFields = 4,
    InvalidSignature = 5,
    TimestampSkewExceeded = 6,
    ReplayDetected = 7,

    /// <summary>
    /// MutualTls mode was configured but the caller presented no client certificate. Also
    /// returned by the HMAC-shaped validation overload in MutualTls mode, so a transport that
    /// never routes to the certificate path is rejected rather than silently authenticated.
    /// </summary>
    CertificateRequired = 8,

    /// <summary>
    /// The client certificate did not match the configured trusted thumbprint allow-list.
    /// </summary>
    CertificateUntrusted = 9,

    /// <summary>
    /// The client certificate is outside its validity window (expired or not yet valid).
    /// </summary>
    CertificateExpired = 10,

    /// <summary>
    /// The request body exceeded the pre-authentication size ceiling
    /// (<c>RaftConfiguration.MaxPreAuthRequestBodyBytes</c>) and was refused before its signature
    /// could be verified.
    /// </summary>
    /// <remarks>
    /// Distinct from the other rejections because it is not an authentication verdict: the caller may
    /// well hold a valid credential. It maps to 413, not 401, so an operator sees a size problem
    /// rather than hunting a phantom credential mismatch.
    /// </remarks>
    RequestBodyTooLarge = 11
}
