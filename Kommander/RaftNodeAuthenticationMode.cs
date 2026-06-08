namespace Kommander;

/// <summary>
/// Authentication mode for node-to-node network transports.
/// </summary>
public enum RaftNodeAuthenticationMode
{
    /// <summary>
    /// Do not require node authentication.
    /// </summary>
    Disabled = 0,

    /// <summary>
    /// Authenticate nodes with a shared cluster secret.
    /// </summary>
    SharedSecret = 1,

    /// <summary>
    /// Authenticate nodes with mutual TLS client certificates.
    /// </summary>
    MutualTls = 2
}
