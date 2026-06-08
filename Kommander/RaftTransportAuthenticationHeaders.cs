namespace Kommander;

/// <summary>
/// Signed transport authentication fields attached to REST or gRPC requests.
/// </summary>
public sealed record RaftTransportAuthenticationHeaders
{
    /// <summary>
    /// Header name carrying the derived HMAC signature.
    /// </summary>
    public const string DefaultSignatureHeaderName = "X-Kommander-Cluster-Auth";

    /// <summary>
    /// Header name carrying the sending node identity.
    /// </summary>
    public const string SenderNodeHeaderName = "X-Kommander-Cluster-Node";

    /// <summary>
    /// Header name carrying the request timestamp in Unix milliseconds.
    /// </summary>
    public const string TimestampHeaderName = "X-Kommander-Cluster-Timestamp";

    /// <summary>
    /// Header name carrying the per-request nonce.
    /// </summary>
    public const string NonceHeaderName = "X-Kommander-Cluster-Nonce";

    /// <summary>
    /// Configured signature header name.
    /// </summary>
    public string SignatureHeaderName { get; init; } = DefaultSignatureHeaderName;

    /// <summary>
    /// Derived HMAC request signature encoded as base64url.
    /// </summary>
    public string Signature { get; init; } = string.Empty;

    /// <summary>
    /// Sending node identity.
    /// </summary>
    public string SenderNode { get; init; } = string.Empty;

    /// <summary>
    /// Request timestamp in Unix milliseconds.
    /// </summary>
    public long TimestampUnixMilliseconds { get; init; }

    /// <summary>
    /// Per-request nonce encoded as base64url.
    /// </summary>
    public string Nonce { get; init; } = string.Empty;
}
