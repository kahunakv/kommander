namespace Kommander.Tests;

public sealed class TestRaftTransportSecurityOptions
{
    [Fact]
    public void RaftConfiguration_InitializesTransportSecurityDefaults()
    {
        RaftConfiguration configuration = new();

        Assert.NotNull(configuration.TransportSecurity);
        Assert.Equal(
            RaftNodeAuthenticationMode.Disabled,
            configuration.TransportSecurity.NodeAuthenticationMode);
        Assert.Null(configuration.TransportSecurity.SharedSecret);
        Assert.Equal("X-Kommander-Cluster-Auth", configuration.TransportSecurity.HeaderName);
        Assert.True(configuration.TransportSecurity.RequireTls);
        Assert.False(configuration.TransportSecurity.AllowInsecureCertificateValidation);
        Assert.Equal(TimeSpan.FromSeconds(60), configuration.TransportSecurity.AllowedClockSkew);
        Assert.Empty(configuration.TransportSecurity.TrustedServerCertificateThumbprints);
        Assert.Empty(configuration.TransportSecurity.TrustedClientCertificateThumbprints);
    }

    [Fact]
    public void GetEffectiveTransportSecurity_UsesLegacyBearerToken_WhenSharedSecretIsEmpty()
    {
        RaftConfiguration configuration = new()
        {
#pragma warning disable CS0618
            HttpAuthBearerToken = "legacy-secret"
#pragma warning restore CS0618
        };

        RaftTransportSecurityOptions effective = configuration.GetEffectiveTransportSecurity();

        Assert.Equal("legacy-secret", effective.SharedSecret);
    }

    [Fact]
    public void GetEffectiveTransportSecurity_PrefersNewSharedSecret_OverLegacyBearerToken()
    {
        RaftConfiguration configuration = new()
        {
            TransportSecurity = new RaftTransportSecurityOptions
            {
                SharedSecret = "new-secret"
            }
        };

#pragma warning disable CS0618
        configuration.HttpAuthBearerToken = "legacy-secret";
#pragma warning restore CS0618

        RaftTransportSecurityOptions effective = configuration.GetEffectiveTransportSecurity();

        Assert.Equal("new-secret", effective.SharedSecret);
    }

    [Fact]
    public void GetEffectiveTransportSecurity_NormalizesEmptySecretsToNull()
    {
        RaftConfiguration configuration = new()
        {
            TransportSecurity = new RaftTransportSecurityOptions
            {
                SharedSecret = " "
            }
        };

#pragma warning disable CS0618
        configuration.HttpAuthBearerToken = "";
#pragma warning restore CS0618

        RaftTransportSecurityOptions effective = configuration.GetEffectiveTransportSecurity();

        Assert.Null(effective.SharedSecret);
    }
}
