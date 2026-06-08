namespace Kommander;

/// <summary>
/// Transport security settings for REST and gRPC Raft communication.
/// </summary>
public sealed class RaftTransportSecurityOptions
{
    /// <summary>
    /// Node authentication mode used by network transports.
    /// </summary>
    public RaftNodeAuthenticationMode NodeAuthenticationMode { get; set; } =
        RaftNodeAuthenticationMode.Disabled;

    /// <summary>
    /// Shared secret used to derive per-request authentication values.
    /// </summary>
    public string? SharedSecret { get; set; }

    /// <summary>
    /// Header or metadata name that carries the request signature.
    /// </summary>
    public string HeaderName { get; set; } = "X-Kommander-Cluster-Auth";

    /// <summary>
    /// Reject non-TLS network transport requests when enabled.
    /// </summary>
    public bool RequireTls { get; set; } = true;

    /// <summary>
    /// Allow development-only certificate validation bypasses when enabled.
    /// </summary>
    public bool AllowInsecureCertificateValidation { get; set; }

    /// <summary>
    /// Maximum allowed clock skew when validating signed requests.
    /// </summary>
    public TimeSpan AllowedClockSkew { get; set; } = TimeSpan.FromSeconds(60);

    /// <summary>
    /// Optional allow-list of trusted server certificate thumbprints.
    /// </summary>
    public IReadOnlyCollection<string> TrustedServerCertificateThumbprints { get; set; } = [];

    /// <summary>
    /// Optional allow-list of trusted client certificate thumbprints.
    /// </summary>
    public IReadOnlyCollection<string> TrustedClientCertificateThumbprints { get; set; } = [];
}
