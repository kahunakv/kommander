using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;

namespace Kommander;

/// <summary>
/// Transport security settings for REST and gRPC Raft communication.
/// </summary>
public sealed class RaftTransportSecurityOptions
{
    private readonly object clientCertificateLock = new();

    private volatile X509Certificate2? loadedClientCertificate;

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

    /// <summary>
    /// Path to the PKCS#12 (.pfx) certificate this node presents as a TLS client in
    /// <see cref="RaftNodeAuthenticationMode.MutualTls"/> mode. Ignored when
    /// <see cref="ClientCertificate"/> is set.
    /// </summary>
    public string? ClientCertificatePath { get; set; }

    /// <summary>
    /// Password for <see cref="ClientCertificatePath"/>. Empty for a password-less archive.
    /// </summary>
    public string? ClientCertificatePassword { get; set; }

    /// <summary>
    /// Pre-loaded client certificate, taking precedence over <see cref="ClientCertificatePath"/>.
    /// <para>
    /// This exists so tests and embedded hosts can supply an in-memory certificate without writing
    /// a PKCS#12 file to disk; it is deliberately not bound from configuration files or the CLI.
    /// </para>
    /// </summary>
    public X509Certificate2? ClientCertificate { get; set; }

    /// <summary>
    /// Returns the client certificate to present in MutualTls mode, loading it from
    /// <see cref="ClientCertificatePath"/> on first use and caching the instance. Returns null when
    /// neither <see cref="ClientCertificate"/> nor <see cref="ClientCertificatePath"/> is configured.
    /// </summary>
    /// <remarks>
    /// The returned instance is cached and shared: both the gRPC channel pool's
    /// <see cref="System.Net.Security.SslClientAuthenticationOptions"/> and the REST handler retain it
    /// for the process lifetime. Loading a PKCS#12 archive opens a key container, so re-loading per
    /// call would leak key handles rather than merely cost time.
    /// </remarks>
    /// <exception cref="RaftException">
    /// The file is missing, is not a readable PKCS#12 archive, the password does not match, or the
    /// certificate carries no private key. Failing here — at configuration time — is deliberate: a
    /// certificate problem discovered at the first replication attempt looks like a network fault.
    /// </exception>
    public X509Certificate2? GetClientCertificate()
    {
        if (ClientCertificate is not null)
            return ClientCertificate;

        if (string.IsNullOrWhiteSpace(ClientCertificatePath))
            return null;

        X509Certificate2? cached = loadedClientCertificate;
        if (cached is not null)
            return cached;

        lock (clientCertificateLock)
            return loadedClientCertificate ??= LoadClientCertificateFromDisk(
                ClientCertificatePath,
                ClientCertificatePassword);
    }

    private static X509Certificate2 LoadClientCertificateFromDisk(string path, string? password)
    {
        if (!File.Exists(path))
            throw new RaftException($"Client certificate not found at '{path}'.");

        X509Certificate2 certificate;

        try
        {
#if NET9_0_OR_GREATER
            // The X509Certificate2 file constructor is obsolete (SYSLIB0057) from .NET 9 on; the
            // loader is also stricter about what it will accept as PKCS#12, which is what we want for
            // a file an operator supplies.
            certificate = X509CertificateLoader.LoadPkcs12FromFile(
                path,
                password,
                ClientCertificateStorageFlags);
#else
            certificate = new X509Certificate2(path, password ?? string.Empty, ClientCertificateStorageFlags);
#endif
        }
        catch (CryptographicException ex)
        {
            // The platform message for a wrong password ("The specified network password is not
            // correct") never mentions certificates or the file, so surfacing it unwrapped leaves an
            // operator with nothing to act on.
            throw new RaftException(
                $"Could not load the client certificate at '{path}'. Verify it is a valid PKCS#12 " +
                "archive and that ClientCertificatePassword matches.",
                ex);
        }

        if (!certificate.HasPrivateKey)
        {
            certificate.Dispose();

            throw new RaftException(
                $"The client certificate at '{path}' has no private key, so it cannot complete a TLS " +
                "client handshake. Export the certificate with its private key included.");
        }

        return certificate;
    }

    /// <summary>
    /// Key storage flags used when loading the client certificate.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <c>EphemeralKeySet</c> keeps the private key out of the on-disk key store, which is what we want
    /// for a cluster credential — but it is usable on Linux only, so it is opt-in per platform rather
    /// than the default:
    /// </para>
    /// <list type="bullet">
    /// <item>macOS rejects the flag outright with <c>PlatformNotSupportedException</c> at load time.</item>
    /// <item>Windows accepts it, but SChannel cannot use an ephemeral key for TLS client
    /// authentication, so the certificate would instead fail later during the handshake — a much
    /// harder failure to attribute.</item>
    /// </list>
    /// <para>
    /// Both therefore fall back to the default (user) key store. Linux is the deployment target where
    /// this matters most, and it is the one that supports it.
    /// </para>
    /// </remarks>
    private static X509KeyStorageFlags ClientCertificateStorageFlags =>
        OperatingSystem.IsLinux()
            ? X509KeyStorageFlags.EphemeralKeySet | X509KeyStorageFlags.Exportable
            : X509KeyStorageFlags.Exportable;
}
