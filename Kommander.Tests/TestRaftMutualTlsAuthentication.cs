using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;

namespace Kommander.Tests;

/// <summary>
/// Covers the MutualTls transport authentication path: the fail-closed contract on the HMAC-shaped
/// <see cref="RaftTransportAuthenticator.Validate"/> overload, the certificate trust policy, and the
/// configuration surface that feeds them.
/// </summary>
[Collection(AuthTestCollection.Name)]
public sealed class TestRaftMutualTlsAuthentication
{
    private static readonly DateTimeOffset Now = new(2026, 3, 9, 16, 0, 0, TimeSpan.Zero);

    [Fact]
    public void Sign_ReturnsEmptyHeaders_ForMutualTls()
    {
        RaftTransportAuthenticator authenticator = CreateAuthenticator();

        RaftTransportAuthenticationHeaders headers = authenticator.Sign(
            method: "POST",
            pathOrGrpcMethod: "/v1/raft/append-logs",
            senderNode: "node-a:5000",
            bodyBytes: "payload"u8.ToArray());

        Assert.Empty(headers.Signature);
        Assert.Empty(headers.SenderNode);
        Assert.Empty(headers.Nonce);
        Assert.Equal(0, headers.TimestampUnixMilliseconds);
    }

    /// <summary>
    /// Both transports gate signing on SharedSecret before ever calling Sign, so MutualTls attaches
    /// no auth headers in practice. This pins that: a regression that removed the gate would start
    /// emitting empty-valued headers rather than failing visibly.
    /// </summary>
    [Fact]
    public void BuildAuthenticationHeaders_EmitsNothing_ForMutualTls()
    {
        using X509Certificate2 certificate = CreateCertificate("node-a");

        RaftConfiguration configuration = new()
        {
            TransportSecurity = new RaftTransportSecurityOptions
            {
                NodeAuthenticationMode = RaftNodeAuthenticationMode.MutualTls,
                ClientCertificate = certificate
            }
        };

        IReadOnlyDictionary<string, string> headers =
            global::Kommander.Communication.Rest.RestCommunication.BuildAuthenticationHeaders(
                configuration,
                senderNode: "node-a:5000",
                method: "POST",
                path: "/v1/raft/append-logs",
                payload: "{}"u8);

        Assert.Empty(headers);
    }

    /// <summary>
    /// The fail-closed guarantee. A correctly signed HMAC request must still be rejected in MutualTls
    /// mode, because reaching this overload means the transport never consulted a certificate. If this
    /// returned Success, any call site that forgot the certificate check would silently authenticate
    /// unauthenticated traffic.
    /// </summary>
    [Fact]
    public void Validate_RejectsValidSignature_InMutualTlsMode()
    {
        RaftTransportAuthenticator.ResetReplayCacheForTesting();

        // Sign with a shared-secret authenticator so the fields are genuinely well-formed and valid —
        // the rejection must come from the mode, not from a malformed signature.
        TestTimeProvider timeProvider = new(Now);

        RaftTransportAuthenticator signer = new(
            new RaftTransportSecurityOptions
            {
                NodeAuthenticationMode = RaftNodeAuthenticationMode.SharedSecret,
                SharedSecret = "top-secret-cluster-key"
            },
            timeProvider);

        RaftTransportAuthenticationHeaders headers = signer.Sign(
            method: "POST",
            pathOrGrpcMethod: "/v1/raft/append-logs",
            senderNode: "node-a:5000",
            bodyBytes: "payload"u8.ToArray(),
            timestampUnixMilliseconds: timeProvider.GetUtcNow().ToUnixTimeMilliseconds());

        RaftTransportAuthenticator mutualTls = CreateAuthenticator(timeProvider);

        RaftTransportAuthenticationResult result = mutualTls.Validate(
            method: "POST",
            pathOrGrpcMethod: "/v1/raft/append-logs",
            bodyBytes: "payload"u8.ToArray(),
            signature: headers.Signature,
            senderNode: headers.SenderNode,
            timestampUnixMilliseconds: headers.TimestampUnixMilliseconds.ToString(),
            nonce: headers.Nonce,
            isSecureTransport: true);

        Assert.Equal(RaftTransportAuthenticationStatus.CertificateRequired, result.Status);
        Assert.False(result.IsAuthenticated);
    }

    [Fact]
    public void ValidatePeerCertificate_RejectsNullCertificate()
    {
        RaftTransportAuthenticator authenticator = CreateAuthenticator();

        RaftTransportAuthenticationResult result =
            authenticator.ValidatePeerCertificate(null, isSecureTransport: true);

        Assert.Equal(RaftTransportAuthenticationStatus.CertificateRequired, result.Status);
        Assert.False(result.IsAuthenticated);
    }

    [Fact]
    public void ValidatePeerCertificate_AcceptsCertificate_WhenNoAllowListConfigured()
    {
        using X509Certificate2 certificate = CreateCertificate("node-a");
        RaftTransportAuthenticator authenticator = CreateAuthenticator();

        RaftTransportAuthenticationResult result =
            authenticator.ValidatePeerCertificate(certificate, isSecureTransport: true);

        Assert.Equal(RaftTransportAuthenticationStatus.Success, result.Status);
        Assert.True(result.IsAuthenticated);
    }

    [Fact]
    public void ValidatePeerCertificate_AcceptsMatchingThumbprint()
    {
        using X509Certificate2 certificate = CreateCertificate("node-a");

        RaftTransportAuthenticator authenticator = CreateAuthenticator(
            trustedThumbprints: [Thumbprint(certificate)]);

        RaftTransportAuthenticationResult result =
            authenticator.ValidatePeerCertificate(certificate, isSecureTransport: true);

        Assert.Equal(RaftTransportAuthenticationStatus.Success, result.Status);
    }

    [Fact]
    public void ValidatePeerCertificate_RejectsNonMatchingThumbprint()
    {
        using X509Certificate2 presented = CreateCertificate("node-a");
        using X509Certificate2 trusted = CreateCertificate("node-b");

        RaftTransportAuthenticator authenticator = CreateAuthenticator(
            trustedThumbprints: [Thumbprint(trusted)]);

        RaftTransportAuthenticationResult result =
            authenticator.ValidatePeerCertificate(presented, isSecureTransport: true);

        Assert.Equal(RaftTransportAuthenticationStatus.CertificateUntrusted, result.Status);
        Assert.False(result.IsAuthenticated);
    }

    /// <summary>
    /// Operators paste thumbprints from <c>openssl x509 -fingerprint -sha256</c>, which emits
    /// colon-separated hex. Without normalization the configured value never matches and the failure
    /// presents as an untrusted peer rather than a formatting mismatch.
    /// </summary>
    [Theory]
    [InlineData(true, false)]   // colon-separated, as openssl prints it
    [InlineData(false, true)]   // lower-case
    [InlineData(true, true)]    // both
    public void ValidatePeerCertificate_AcceptsThumbprint_InOperatorPastedFormats(
        bool colonSeparated,
        bool lowerCase)
    {
        using X509Certificate2 certificate = CreateCertificate("node-a");

        string configured = Thumbprint(certificate);

        if (colonSeparated)
            configured = string.Join(':', Chunk(configured, 2));

        if (lowerCase)
            configured = configured.ToLowerInvariant();

        RaftTransportAuthenticator authenticator = CreateAuthenticator(trustedThumbprints: [configured]);

        RaftTransportAuthenticationResult result =
            authenticator.ValidatePeerCertificate(certificate, isSecureTransport: true);

        Assert.Equal(RaftTransportAuthenticationStatus.Success, result.Status);
    }

    [Fact]
    public void ValidatePeerCertificate_RejectsExpiredCertificate()
    {
        using X509Certificate2 certificate = CreateCertificate(
            "node-a",
            notBefore: Now.AddDays(-10),
            notAfter: Now.AddDays(-1));

        RaftTransportAuthenticator authenticator = CreateAuthenticator();

        RaftTransportAuthenticationResult result =
            authenticator.ValidatePeerCertificate(certificate, isSecureTransport: true);

        Assert.Equal(RaftTransportAuthenticationStatus.CertificateExpired, result.Status);
    }

    [Fact]
    public void ValidatePeerCertificate_RejectsNotYetValidCertificate()
    {
        using X509Certificate2 certificate = CreateCertificate(
            "node-a",
            notBefore: Now.AddDays(1),
            notAfter: Now.AddDays(10));

        RaftTransportAuthenticator authenticator = CreateAuthenticator();

        RaftTransportAuthenticationResult result =
            authenticator.ValidatePeerCertificate(certificate, isSecureTransport: true);

        Assert.Equal(RaftTransportAuthenticationStatus.CertificateExpired, result.Status);
    }

    /// <summary>
    /// Guards the UTC conversion. NotBefore/NotAfter come back as local-kind DateTimes, so a window
    /// that is only a few hours wide is accepted or rejected differently depending on the host offset
    /// if the comparison forgets to convert — and a UTC CI container would never show it.
    /// </summary>
    [Fact]
    public void ValidatePeerCertificate_ComparesValidityWindowInUtc()
    {
        // A window that closed two hours ago in UTC. Read as local time on any host east of UTC, the
        // same instants would still appear open.
        using X509Certificate2 certificate = CreateCertificate(
            "node-a",
            notBefore: Now.AddHours(-14),
            notAfter: Now.AddHours(-2));

        RaftTransportAuthenticator authenticator = CreateAuthenticator();

        RaftTransportAuthenticationResult result =
            authenticator.ValidatePeerCertificate(certificate, isSecureTransport: true);

        Assert.Equal(RaftTransportAuthenticationStatus.CertificateExpired, result.Status);
    }

    [Fact]
    public void ValidatePeerCertificate_RequiresTls_WhenRequireTlsIsEnabled()
    {
        using X509Certificate2 certificate = CreateCertificate("node-a");
        RaftTransportAuthenticator authenticator = CreateAuthenticator();

        RaftTransportAuthenticationResult result =
            authenticator.ValidatePeerCertificate(certificate, isSecureTransport: false);

        Assert.Equal(RaftTransportAuthenticationStatus.TlsRequired, result.Status);
    }

    [Fact]
    public void ValidatePeerCertificate_RejectsSharedSecretMode()
    {
        using X509Certificate2 certificate = CreateCertificate("node-a");

        RaftTransportAuthenticator authenticator = new(
            new RaftTransportSecurityOptions
            {
                NodeAuthenticationMode = RaftNodeAuthenticationMode.SharedSecret,
                SharedSecret = "top-secret-cluster-key"
            },
            new TestTimeProvider(Now));

        RaftTransportAuthenticationResult result =
            authenticator.ValidatePeerCertificate(certificate, isSecureTransport: true);

        Assert.False(result.IsAuthenticated);
    }

    [Fact]
    public void ValidatePeerCertificate_ReportsDisabled_WhenAuthenticationIsOff()
    {
        RaftTransportAuthenticator authenticator = new(
            new RaftTransportSecurityOptions(),
            new TestTimeProvider(Now));

        RaftTransportAuthenticationResult result =
            authenticator.ValidatePeerCertificate(null, isSecureTransport: false);

        Assert.Equal(RaftTransportAuthenticationStatus.Disabled, result.Status);
        Assert.True(result.IsAuthenticated);
    }

    [Fact]
    public void GetClientCertificate_ReturnsSameInstanceAcrossCalls()
    {
        using X509Certificate2 certificate = CreateCertificate("node-a");
        string path = WriteTemporaryPfx(certificate, password: "pfx-password");

        try
        {
            RaftTransportSecurityOptions options = new()
            {
                NodeAuthenticationMode = RaftNodeAuthenticationMode.MutualTls,
                ClientCertificatePath = path,
                ClientCertificatePassword = "pfx-password"
            };

            X509Certificate2? first = options.GetClientCertificate();
            X509Certificate2? second = options.GetClientCertificate();

            Assert.NotNull(first);
            Assert.Same(first, second);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public void GetClientCertificate_ThrowsRaftException_OnWrongPassword()
    {
        using X509Certificate2 certificate = CreateCertificate("node-a");
        string path = WriteTemporaryPfx(certificate, password: "pfx-password");

        try
        {
            RaftTransportSecurityOptions options = new()
            {
                NodeAuthenticationMode = RaftNodeAuthenticationMode.MutualTls,
                ClientCertificatePath = path,
                ClientCertificatePassword = "wrong-password"
            };

            RaftException exception = Assert.Throws<RaftException>(() => options.GetClientCertificate());

            // The path must be in the message: the platform's own wording for a bad PKCS#12 password
            // mentions neither certificates nor the file, leaving an operator with nothing to act on.
            Assert.Contains(path, exception.Message);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public void GetClientCertificate_ThrowsRaftException_WhenFileIsMissing()
    {
        RaftTransportSecurityOptions options = new()
        {
            NodeAuthenticationMode = RaftNodeAuthenticationMode.MutualTls,
            ClientCertificatePath = Path.Combine(Path.GetTempPath(), $"missing-{Guid.NewGuid():N}.pfx")
        };

        Assert.Throws<RaftException>(() => options.GetClientCertificate());
    }

    [Fact]
    public void GetClientCertificate_PrefersExplicitInstance_OverPath()
    {
        using X509Certificate2 certificate = CreateCertificate("node-a");

        RaftTransportSecurityOptions options = new()
        {
            NodeAuthenticationMode = RaftNodeAuthenticationMode.MutualTls,
            ClientCertificate = certificate,
            ClientCertificatePath = Path.Combine(Path.GetTempPath(), $"missing-{Guid.NewGuid():N}.pfx")
        };

        // The missing path must never be consulted, so this both asserts precedence and proves the
        // in-memory seam works without touching disk.
        Assert.Same(certificate, options.GetClientCertificate());
    }

    [Fact]
    public void GetClientCertificate_ReturnsNull_WhenNothingConfigured()
    {
        RaftTransportSecurityOptions options = new()
        {
            NodeAuthenticationMode = RaftNodeAuthenticationMode.MutualTls
        };

        Assert.Null(options.GetClientCertificate());
    }

    [Fact]
    public void GetEffectiveTransportSecurity_DoesNotFoldLegacyBearerToken_InMutualTlsMode()
    {
        using X509Certificate2 certificate = CreateCertificate("node-a");

        RaftConfiguration configuration = new()
        {
            TransportSecurity = new RaftTransportSecurityOptions
            {
                NodeAuthenticationMode = RaftNodeAuthenticationMode.MutualTls,
                ClientCertificate = certificate
            }
        };

#pragma warning disable CS0618
        configuration.HttpAuthBearerToken = "legacy-secret";
#pragma warning restore CS0618

        RaftTransportSecurityOptions effective = configuration.GetEffectiveTransportSecurity();

        Assert.Null(effective.SharedSecret);
    }

    [Fact]
    public void GetEffectiveTransportSecurity_NormalizesThumbprintAllowLists()
    {
        RaftConfiguration configuration = new()
        {
            TransportSecurity = new RaftTransportSecurityOptions
            {
                TrustedClientCertificateThumbprints = ["ab:cd:ef"],
                TrustedServerCertificateThumbprints = [" 12:34 "]
            }
        };

        RaftTransportSecurityOptions effective = configuration.GetEffectiveTransportSecurity();

        Assert.Equal(["ABCDEF"], effective.TrustedClientCertificateThumbprints);
        Assert.Equal(["1234"], effective.TrustedServerCertificateThumbprints);
    }

    [Fact]
    public void GetEffectiveTransportSecurity_SharesOneLoadedCertificateAcrossCopies()
    {
        using X509Certificate2 certificate = CreateCertificate("node-a");
        string path = WriteTemporaryPfx(certificate, password: "pfx-password");

        try
        {
            RaftConfiguration configuration = new()
            {
                TransportSecurity = new RaftTransportSecurityOptions
                {
                    NodeAuthenticationMode = RaftNodeAuthenticationMode.MutualTls,
                    ClientCertificatePath = path,
                    ClientCertificatePassword = "pfx-password"
                }
            };

            // Each call allocates a fresh options object; they must still resolve to one loaded
            // certificate, or every derived copy would open its own key container.
            X509Certificate2? first = configuration.GetEffectiveTransportSecurity().GetClientCertificate();
            X509Certificate2? second = configuration.GetEffectiveTransportSecurity().GetClientCertificate();

            Assert.NotNull(first);
            Assert.Same(first, second);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public void Validate_ThrowsWhenMutualTlsHasNoClientCertificate()
    {
        RaftConfiguration configuration = new()
        {
            TransportSecurity = new RaftTransportSecurityOptions
            {
                NodeAuthenticationMode = RaftNodeAuthenticationMode.MutualTls
            }
        };

        Assert.Throws<RaftException>(configuration.Validate);
    }

    [Fact]
    public void Validate_ThrowsWhenMutualTlsIsCombinedWithInsecureCertificateValidation()
    {
        using X509Certificate2 certificate = CreateCertificate("node-a");

        RaftConfiguration configuration = new()
        {
            TransportSecurity = new RaftTransportSecurityOptions
            {
                NodeAuthenticationMode = RaftNodeAuthenticationMode.MutualTls,
                ClientCertificate = certificate,
                AllowInsecureCertificateValidation = true
            }
        };

        Assert.Throws<RaftException>(configuration.Validate);
    }

    [Fact]
    public void Validate_AcceptsWellFormedMutualTlsConfiguration()
    {
        using X509Certificate2 certificate = CreateCertificate("node-a");

        RaftConfiguration configuration = new()
        {
            TransportSecurity = new RaftTransportSecurityOptions
            {
                NodeAuthenticationMode = RaftNodeAuthenticationMode.MutualTls,
                ClientCertificate = certificate
            }
        };

        configuration.Validate();
    }

    private static RaftTransportAuthenticator CreateAuthenticator(
        TimeProvider? timeProvider = null,
        IReadOnlyCollection<string>? trustedThumbprints = null)
    {
        return new RaftTransportAuthenticator(
            new RaftTransportSecurityOptions
            {
                NodeAuthenticationMode = RaftNodeAuthenticationMode.MutualTls,
                TrustedClientCertificateThumbprints = trustedThumbprints ?? []
            },
            timeProvider ?? new TestTimeProvider(Now));
    }

    /// <summary>
    /// Canonical thumbprint encoding: uppercase hex of SHA-256 over the DER bytes. Deliberately not
    /// <see cref="X509Certificate2.Thumbprint"/>, which is SHA-1.
    /// </summary>
    private static string Thumbprint(X509Certificate2 certificate) =>
        Convert.ToHexString(SHA256.HashData(certificate.RawData));

    private static IEnumerable<string> Chunk(string value, int size)
    {
        for (int i = 0; i < value.Length; i += size)
            yield return value.Substring(i, Math.Min(size, value.Length - i));
    }

    /// <summary>
    /// Creates a self-signed test certificate.
    /// </summary>
    /// <remarks>
    /// Uses ECDSA P-256 rather than RSA-2048 deliberately. This class generates a key per test, and
    /// RSA-2048 keygen costs on the order of 100 ms of pure CPU each — enough contention, running
    /// alongside the timing-sensitive in-process cluster tests, to push
    /// <c>GracefulLeave_LastVoter_ExitsFastWithoutSpinning</c> past its 10 s condition timeout.
    /// P-256 keygen is orders of magnitude cheaper and exercises the same certificate code paths.
    /// </remarks>
    private static X509Certificate2 CreateCertificate(
        string commonName,
        DateTimeOffset? notBefore = null,
        DateTimeOffset? notAfter = null)
    {
        using ECDsa key = ECDsa.Create(ECCurve.NamedCurves.nistP256);

        CertificateRequest request = new(
            $"CN={commonName}",
            key,
            HashAlgorithmName.SHA256);

        return request.CreateSelfSigned(
            notBefore ?? Now.AddDays(-1),
            notAfter ?? Now.AddDays(365));
    }

    private static string WriteTemporaryPfx(X509Certificate2 certificate, string password)
    {
        string path = Path.Combine(Path.GetTempPath(), $"kommander-mtls-{Guid.NewGuid():N}.pfx");
        File.WriteAllBytes(path, certificate.Export(X509ContentType.Pkcs12, password));
        return path;
    }

    private sealed class TestTimeProvider(DateTimeOffset utcNow) : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() => utcNow;
    }
}
