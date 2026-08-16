
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Kommander;

namespace Kommander.Tests;

/// <summary>
/// Covers the client half of the certificate-pinning model:
/// <see cref="RaftClientCertificateValidator.IsServerCertificateTrusted"/>, used by both the gRPC
/// channel pool and the REST handler to validate a peer's <b>server</b> certificate.
/// </summary>
/// <remarks>
/// The expiry cases are the point. Both client paths previously compared thumbprints only, so an
/// expired server certificate was accepted as long as its thumbprint matched — while the server side
/// rejected an expired client certificate. Pinning does not imply a validity check; it has to be done
/// explicitly, and it has to be done on both sides or the asymmetry comes back.
/// </remarks>
public sealed class TestServerCertificateValidation
{
    private static readonly DateTimeOffset Now = new(2026, 8, 15, 12, 0, 0, TimeSpan.Zero);

    [Fact]
    public void ValidCertificate_WithMatchingThumbprint_IsTrusted()
    {
        using X509Certificate2 certificate = CreateCertificate("node-a");

        Assert.True(RaftClientCertificateValidator.IsServerCertificateTrusted(
            certificate,
            [Thumbprint(certificate)],
            Clock()));
    }

    /// <summary>
    /// The M2 fix: a pinned certificate that has expired must be refused, not accepted on the
    /// strength of its thumbprint.
    /// </summary>
    [Fact]
    public void ExpiredCertificate_WithMatchingThumbprint_IsRejected()
    {
        using X509Certificate2 certificate = CreateCertificate(
            "node-a",
            notBefore: Now.AddDays(-10),
            notAfter: Now.AddDays(-1));

        Assert.False(RaftClientCertificateValidator.IsServerCertificateTrusted(
            certificate,
            [Thumbprint(certificate)],
            Clock()));
    }

    /// <summary>
    /// The other end of the window: a certificate issued for the future is equally untrusted, which
    /// is what catches a badly skewed clock rather than silently accepting it.
    /// </summary>
    [Fact]
    public void NotYetValidCertificate_WithMatchingThumbprint_IsRejected()
    {
        using X509Certificate2 certificate = CreateCertificate(
            "node-a",
            notBefore: Now.AddDays(1),
            notAfter: Now.AddDays(30));

        Assert.False(RaftClientCertificateValidator.IsServerCertificateTrusted(
            certificate,
            [Thumbprint(certificate)],
            Clock()));
    }

    [Fact]
    public void ValidCertificate_WithNonMatchingThumbprint_IsRejected()
    {
        using X509Certificate2 presented = CreateCertificate("node-a");
        using X509Certificate2 trusted = CreateCertificate("node-b");

        Assert.False(RaftClientCertificateValidator.IsServerCertificateTrusted(
            presented,
            [Thumbprint(trusted)],
            Clock()));
    }

    [Fact]
    public void NullCertificate_IsRejected()
    {
        using X509Certificate2 trusted = CreateCertificate("node-a");

        Assert.False(RaftClientCertificateValidator.IsServerCertificateTrusted(
            null,
            [Thumbprint(trusted)],
            Clock()));
    }

    /// <summary>
    /// An empty allow-list trusts nothing on the client path — the opposite of the server path, where
    /// it delegates to the host's own TLS validation. The callback is only installed when thumbprints
    /// were configured, so reaching it with none means the policy was lost and refusing is correct.
    /// </summary>
    [Fact]
    public void EmptyAllowList_TrustsNothing()
    {
        using X509Certificate2 certificate = CreateCertificate("node-a");

        Assert.False(RaftClientCertificateValidator.IsServerCertificateTrusted(
            certificate,
            [],
            Clock()));
    }

    /// <summary>
    /// Thumbprints pasted from <c>openssl x509 -fingerprint -sha256</c> are colon-separated, and the
    /// client path normalizes them the same way the server path does.
    /// </summary>
    [Fact]
    public void OperatorPastedThumbprintFormat_IsAccepted()
    {
        using X509Certificate2 certificate = CreateCertificate("node-a");

        string colonSeparated = string.Join(':', Chunk(Thumbprint(certificate), 2)).ToLowerInvariant();

        Assert.True(RaftClientCertificateValidator.IsServerCertificateTrusted(
            certificate,
            [colonSeparated],
            Clock()));
    }

    /// <summary>
    /// The callback signatures differ between transports — <c>HttpClientHandler</c> supplies an
    /// <see cref="X509Certificate2"/>, <c>SslClientAuthenticationOptions</c> the base
    /// <see cref="X509Certificate"/> — so the base type must be handled without falling back to
    /// "cannot inspect, therefore reject", which would break every TLS connection.
    /// </summary>
    [Fact]
    public void BaseCertificateType_IsInspectedNotRefused()
    {
        using X509Certificate2 certificate = CreateCertificate("node-a");

        // A base X509Certificate carrying the same DER bytes, as SslStream may hand over.
        using X509Certificate baseTyped = new(certificate.RawData);

        Assert.True(RaftClientCertificateValidator.IsServerCertificateTrusted(
            baseTyped,
            [Thumbprint(certificate)],
            Clock()));
    }

    /// <summary>
    /// …and the validity window is still enforced when only the base type is available, so the
    /// re-materialization path cannot become a way to skip the expiry check.
    /// </summary>
    [Fact]
    public void BaseCertificateType_StillHonoursValidityWindow()
    {
        using X509Certificate2 certificate = CreateCertificate(
            "node-a",
            notBefore: Now.AddDays(-10),
            notAfter: Now.AddDays(-1));

        using X509Certificate baseTyped = new(certificate.RawData);

        Assert.False(RaftClientCertificateValidator.IsServerCertificateTrusted(
            baseTyped,
            [Thumbprint(certificate)],
            Clock()));
    }

    private static TimeProvider Clock() => new FixedTimeProvider(Now);

    private static string Thumbprint(X509Certificate2 certificate) =>
        Convert.ToHexString(SHA256.HashData(certificate.RawData));

    private static IEnumerable<string> Chunk(string value, int size)
    {
        for (int i = 0; i < value.Length; i += size)
            yield return value.Substring(i, Math.Min(size, value.Length - i));
    }

    private static X509Certificate2 CreateCertificate(
        string commonName,
        DateTimeOffset? notBefore = null,
        DateTimeOffset? notAfter = null)
    {
        using ECDsa key = ECDsa.Create(ECCurve.NamedCurves.nistP256);

        CertificateRequest request = new($"CN={commonName}", key, HashAlgorithmName.SHA256);

        return request.CreateSelfSigned(
            notBefore ?? Now.AddDays(-1),
            notAfter ?? Now.AddDays(365));
    }

    private sealed class FixedTimeProvider(DateTimeOffset now) : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() => now;
    }
}
