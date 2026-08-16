using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;

namespace Kommander;

/// <summary>
/// Validates a peer client certificate for <see cref="RaftNodeAuthenticationMode.MutualTls"/>.
/// </summary>
/// <remarks>
/// <para>
/// Shared by the gRPC (<c>RaftService.ValidateAuth</c>) and REST
/// (<c>RestCommunicationExtensions.AuthenticateRequestAsync</c>) server paths so the two transports
/// cannot drift on what "trusted" means. A drift there would be invisible to any test that exercises
/// only one transport, while leaving the other transport permanently more permissive.
/// </para>
/// <para>
/// Each rejection maps to its own <see cref="RaftTransportAuthenticationStatus"/> rather than a single
/// boolean, because the three failure modes need different operator responses: a missing certificate
/// is a listener misconfiguration, an expired one is a rotation that was missed, and an untrusted one
/// is a thumbprint allow-list that was not updated.
/// </para>
/// </remarks>
internal static class RaftClientCertificateValidator
{
    /// <summary>
    /// Validates a peer certificate against the configured trust policy.
    /// </summary>
    internal static RaftTransportAuthenticationStatus Validate(
        X509Certificate2? clientCertificate,
        RaftTransportSecurityOptions options,
        TimeProvider timeProvider)
    {
        if (clientCertificate is null)
            return RaftTransportAuthenticationStatus.CertificateRequired;

        // X509Certificate2.NotBefore/NotAfter return DateTimes with Kind=Local. Comparing them against
        // a UTC "now" without conversion silently shifts the validity window by the host's offset —
        // a bug that is invisible on a UTC CI container and appears only in other time zones.
        DateTime nowUtc = timeProvider.GetUtcNow().UtcDateTime;

        if (nowUtc < clientCertificate.NotBefore.ToUniversalTime()
            || nowUtc > clientCertificate.NotAfter.ToUniversalTime())
        {
            return RaftTransportAuthenticationStatus.CertificateExpired;
        }

        IReadOnlyCollection<string> trusted = options.TrustedClientCertificateThumbprints;

        // An empty allow-list means "accept anything that completed the handshake" — it delegates the
        // trust decision entirely to whatever certificate validation the host configured on its TLS
        // listener, and performs no chain building of its own.
        //
        // That delegation is only safe if the host actually validates. Kommander.Server does not: it
        // sets ClientCertificateValidation to accept everything on purpose, so that self-signed
        // per-node certificates survive the handshake and reach this allow-list. Reaching here with
        // an empty list under that listener would trust every certificate in existence, which is why
        // the server refuses to start in that configuration (KommanderServerBindingPolicy
        // .ValidateMutualTlsTrustAnchors) rather than relying on this method to fail closed.
        //
        // The permissive branch is kept for embedded hosts that do enforce chain validation in their
        // own Kestrel setup and use mTLS for transport identity only. If you host Kommander yourself,
        // configure an allow-list or ensure your listener validates.
        if (trusted.Count == 0)
            return RaftTransportAuthenticationStatus.Success;

        return IsThumbprintTrusted(clientCertificate, trusted)
            ? RaftTransportAuthenticationStatus.Success
            : RaftTransportAuthenticationStatus.CertificateUntrusted;
    }

    /// <summary>
    /// Validates a peer <b>server</b> certificate for an outgoing connection: thumbprint pinning plus
    /// the validity window.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The client half of the pinning model, shared by the gRPC channel pool and the REST handler so
    /// the two cannot drift — the same reason <see cref="Validate"/> is shared by the two server
    /// paths.
    /// </para>
    /// <para>
    /// The validity window is checked here because pinning alone does not imply it: both client paths
    /// previously compared thumbprints only, so an <em>expired</em> server certificate was accepted as
    /// long as its thumbprint matched, while the server side rejected an expired client certificate.
    /// That asymmetry was not deliberate.
    /// </para>
    /// <para>
    /// Still no chain building and no revocation check — that is inherent to a pinned-leaf model, and
    /// a compromised pinned certificate stays trusted until it is removed from every node's allow-list
    /// and the processes restart.
    /// </para>
    /// </remarks>
    /// <param name="certificate">Server certificate presented by the peer.</param>
    /// <param name="trustedThumbprints">Allow-list; an empty list trusts nothing on this path.</param>
    /// <param name="timeProvider">Clock, injectable so expiry is testable without waiting.</param>
    internal static bool IsServerCertificateTrusted(
        X509Certificate? certificate,
        IReadOnlyCollection<string> trustedThumbprints,
        TimeProvider timeProvider)
    {
        // Unlike the server path, an empty allow-list here trusts nothing: this callback is only
        // installed when the operator configured thumbprints, so reaching it with none configured
        // means the pinning policy was lost somewhere and the safe answer is to refuse.
        if (certificate is null || trustedThumbprints.Count == 0)
            return false;

        if (!IsThumbprintTrusted(certificate, trustedThumbprints))
            return false;

        return IsWithinValidityWindow(certificate, timeProvider);
    }

    /// <summary>
    /// Returns true when <paramref name="now"/> falls inside the certificate's validity window.
    /// </summary>
    /// <remarks>
    /// The callback signatures differ between transports — <c>HttpClientHandler</c> hands over an
    /// <see cref="X509Certificate2"/>, <c>SslClientAuthenticationOptions</c> the base
    /// <see cref="X509Certificate"/> — so the base type is re-materialized when needed rather than
    /// refusing what cannot be inspected, which would reject every connection if a platform ever
    /// passed the base type.
    /// </remarks>
    private static bool IsWithinValidityWindow(X509Certificate certificate, TimeProvider timeProvider)
    {
        X509Certificate2? owned = null;

        try
        {
            X509Certificate2 typed = certificate as X509Certificate2 ?? (owned = LoadFromRawData(certificate));

            // Same UTC conversion as the server path: NotBefore/NotAfter come back with Kind=Local,
            // and comparing them against a UTC now without converting shifts the window by the host's
            // offset — invisible on a UTC CI container, wrong everywhere else.
            DateTime nowUtc = timeProvider.GetUtcNow().UtcDateTime;

            return nowUtc >= typed.NotBefore.ToUniversalTime()
                && nowUtc <= typed.NotAfter.ToUniversalTime();
        }
        catch (CryptographicException)
        {
            // Unparseable certificate: refuse rather than treat "cannot tell" as valid.
            return false;
        }
        finally
        {
            owned?.Dispose();
        }
    }

    private static X509Certificate2 LoadFromRawData(X509Certificate certificate)
    {
#if NET9_0_OR_GREATER
        return X509CertificateLoader.LoadCertificate(certificate.GetRawCertData());
#else
        return new X509Certificate2(certificate.GetRawCertData());
#endif
    }

    /// <summary>
    /// Returns true when the certificate's SHA-256 thumbprint appears in the allow-list.
    /// </summary>
    internal static bool IsThumbprintTrusted(
        X509Certificate certificate,
        IReadOnlyCollection<string> trustedThumbprints)
    {
        string thumbprint = ComputeThumbprint(certificate);

        foreach (string candidate in trustedThumbprints)
        {
            // Normalize per comparison rather than trusting the caller: the allow-list reaching this
            // method has usually been normalized by GetEffectiveTransportSecurity, but the options
            // object is public and settable, so a directly-constructed instance can still carry raw
            // colon-separated values.
            if (string.Equals(Normalize(candidate), thumbprint, StringComparison.OrdinalIgnoreCase))
                return true;
        }

        return false;
    }

    /// <summary>
    /// Computes the canonical thumbprint for a certificate: uppercase hex of the SHA-256 hash over the
    /// DER-encoded certificate, with no separators.
    /// </summary>
    /// <remarks>
    /// This is deliberately <b>not</b> <see cref="X509Certificate2.Thumbprint"/>, which is SHA-1 and
    /// would never match a value produced here. It matches the server-side pinning already performed
    /// in <c>SharedChannels.BuildSslOptions</c>, so one documented encoding covers both directions.
    /// </remarks>
    internal static string ComputeThumbprint(X509Certificate certificate) =>
        Convert.ToHexString(SHA256.HashData(certificate.GetRawCertData()));

    /// <summary>
    /// Normalizes a configured thumbprint for comparison by stripping separators and whitespace.
    /// </summary>
    /// <remarks>
    /// Operators paste thumbprints from <c>openssl x509 -fingerprint -sha256</c>, which emits
    /// colon-separated hex. Without this the configured value never matches and the failure looks like
    /// an untrusted peer rather than a formatting mismatch.
    /// </remarks>
    internal static string Normalize(string? thumbprint)
    {
        if (string.IsNullOrWhiteSpace(thumbprint))
            return string.Empty;

        Span<char> buffer = thumbprint.Length <= 256
            ? stackalloc char[thumbprint.Length]
            : new char[thumbprint.Length];

        int length = 0;

        foreach (char c in thumbprint)
        {
            if (c is ':' or '-' or ' ' or '\t' or '\r' or '\n')
                continue;

            buffer[length++] = char.ToUpperInvariant(c);
        }

        return new string(buffer[..length]);
    }

    /// <summary>
    /// Normalizes an allow-list once, dropping entries that are empty after normalization.
    /// </summary>
    internal static IReadOnlyCollection<string> NormalizeAll(IReadOnlyCollection<string>? thumbprints)
    {
        if (thumbprints is null || thumbprints.Count == 0)
            return [];

        List<string> normalized = new(thumbprints.Count);

        foreach (string thumbprint in thumbprints)
        {
            string value = Normalize(thumbprint);
            if (value.Length > 0)
                normalized.Add(value);
        }

        return normalized;
    }
}
