
using System.Net;
using Kommander;
using Kommander.Server;

namespace Kommander.Tests;

/// <summary>
/// Covers the fail-closed startup rules in <see cref="KommanderServerBindingPolicy"/>: an
/// unauthenticated node must not bind the network, mTLS must not run without a trust anchor, and a
/// cleartext listener must not appear beside TLS unless it was asked for.
/// </summary>
/// <remarks>
/// These are regression tests for a reviewed build that shipped all three as defaults. Each
/// assertion is the *refusal*, not the happy path — a check that silently stops firing is the
/// failure mode that matters here, and only the negative cases detect it.
/// </remarks>
public sealed class TestKommanderServerBindingPolicy
{
    // ---- ResolveBindAddress -------------------------------------------------------------------

    [Theory]
    [InlineData("*")]
    [InlineData("+")]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData(null)]
    [InlineData("0.0.0.0")]
    public void ResolveBindAddress_MapsWildcardFormsToAny(string? host)
    {
        Assert.Equal(IPAddress.Any, KommanderServerBindingPolicy.ResolveBindAddress(host));
    }

    [Theory]
    [InlineData("localhost")]
    [InlineData("LOCALHOST")]
    [InlineData("127.0.0.1")]
    [InlineData("127.0.0.5")]
    [InlineData("::1")]
    public void ResolveBindAddress_MapsLoopbackFormsToLoopback(string host)
    {
        IPAddress resolved = KommanderServerBindingPolicy.ResolveBindAddress(host);

        Assert.True(IPAddress.IsLoopback(resolved), $"expected a loopback address, got {resolved}");
    }

    [Fact]
    public void ResolveBindAddress_KeepsSpecificInterfaceAddress()
    {
        Assert.Equal(
            IPAddress.Parse("172.20.0.2"),
            KommanderServerBindingPolicy.ResolveBindAddress("172.20.0.2"));
    }

    // ---- C1: unauthenticated network exposure -------------------------------------------------

    /// <summary>
    /// The finding itself: stock flags (auth Disabled) plus a wildcard bind exposed every Raft
    /// endpoint — append, snapshot install, vote, gossip — to any host that could reach the port.
    /// </summary>
    [Fact]
    public void ValidateAuthenticationExposure_Throws_WhenDisabledAuthOnWildcardBind()
    {
        RaftException exception = Assert.Throws<RaftException>(() =>
            KommanderServerBindingPolicy.ValidateAuthenticationExposure(
                RaftNodeAuthenticationMode.Disabled,
                IPAddress.Any,
                allowUnauthenticatedCluster: false));

        // The message has to name the way out, or the operator's only move is to delete the check.
        Assert.Contains("--node-auth-mode", exception.Message);
        Assert.Contains("--allow-unauthenticated-cluster", exception.Message);
    }

    [Fact]
    public void ValidateAuthenticationExposure_Throws_WhenDisabledAuthOnSpecificNetworkAddress()
    {
        Assert.Throws<RaftException>(() =>
            KommanderServerBindingPolicy.ValidateAuthenticationExposure(
                RaftNodeAuthenticationMode.Disabled,
                IPAddress.Parse("172.20.0.2"),
                allowUnauthenticatedCluster: false));
    }

    [Theory]
    [InlineData("127.0.0.1")]
    [InlineData("::1")]
    public void ValidateAuthenticationExposure_Allows_DisabledAuthOnLoopback(string address)
    {
        KommanderServerBindingPolicy.ValidateAuthenticationExposure(
            RaftNodeAuthenticationMode.Disabled,
            IPAddress.Parse(address),
            allowUnauthenticatedCluster: false);
    }

    [Fact]
    public void ValidateAuthenticationExposure_Allows_WhenOperatorOptsIn()
    {
        KommanderServerBindingPolicy.ValidateAuthenticationExposure(
            RaftNodeAuthenticationMode.Disabled,
            IPAddress.Any,
            allowUnauthenticatedCluster: true);
    }

    [Theory]
    [InlineData(RaftNodeAuthenticationMode.SharedSecret)]
    [InlineData(RaftNodeAuthenticationMode.MutualTls)]
    public void ValidateAuthenticationExposure_Allows_AuthenticatedModesOnAnyBind(
        RaftNodeAuthenticationMode mode)
    {
        KommanderServerBindingPolicy.ValidateAuthenticationExposure(
            mode,
            IPAddress.Any,
            allowUnauthenticatedCluster: false);
    }

    [Fact]
    public void IsUnauthenticatedNetworkExposure_IsTrue_OnlyForDisabledAuthOffLoopback()
    {
        Assert.True(KommanderServerBindingPolicy.IsUnauthenticatedNetworkExposure(
            RaftNodeAuthenticationMode.Disabled, IPAddress.Any));

        Assert.False(KommanderServerBindingPolicy.IsUnauthenticatedNetworkExposure(
            RaftNodeAuthenticationMode.Disabled, IPAddress.Loopback));

        Assert.False(KommanderServerBindingPolicy.IsUnauthenticatedNetworkExposure(
            RaftNodeAuthenticationMode.SharedSecret, IPAddress.Any));
    }

    // ---- H3: mTLS trust anchors ---------------------------------------------------------------

    /// <summary>
    /// With no allow-list, the TLS layer accepts every certificate (by design, so self-signed
    /// per-node certificates reach the application) and the application accepts everything too —
    /// so any self-generated certificate becomes a trusted cluster node.
    /// </summary>
    [Fact]
    public void ValidateMutualTlsTrustAnchors_Throws_WhenAllowListIsEmpty()
    {
        RaftException exception = Assert.Throws<RaftException>(() =>
            KommanderServerBindingPolicy.ValidateMutualTlsTrustAnchors(
                RaftNodeAuthenticationMode.MutualTls,
                []));

        Assert.Contains("--trusted-client-cert-thumbprint", exception.Message);
    }

    [Fact]
    public void ValidateMutualTlsTrustAnchors_Throws_WhenAllowListIsNull()
    {
        Assert.Throws<RaftException>(() =>
            KommanderServerBindingPolicy.ValidateMutualTlsTrustAnchors(
                RaftNodeAuthenticationMode.MutualTls,
                null));
    }

    /// <summary>
    /// Entries that normalize away to nothing must not satisfy the check. Counting raw entries would
    /// let an effectively empty allow-list through the guard that exists to forbid exactly that.
    /// </summary>
    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData(":")]
    [InlineData(":::")]
    [InlineData("- -")]
    public void ValidateMutualTlsTrustAnchors_Throws_ForEntriesThatNormalizeToNothing(string entry)
    {
        Assert.Throws<RaftException>(() =>
            KommanderServerBindingPolicy.ValidateMutualTlsTrustAnchors(
                RaftNodeAuthenticationMode.MutualTls,
                [entry]));
    }

    [Theory]
    [InlineData("AABBCC")]
    [InlineData("aa:bb:cc")]
    public void ValidateMutualTlsTrustAnchors_Allows_UsableThumbprint(string entry)
    {
        KommanderServerBindingPolicy.ValidateMutualTlsTrustAnchors(
            RaftNodeAuthenticationMode.MutualTls,
            [entry]);
    }

    [Theory]
    [InlineData(RaftNodeAuthenticationMode.Disabled)]
    [InlineData(RaftNodeAuthenticationMode.SharedSecret)]
    public void ValidateMutualTlsTrustAnchors_Ignores_NonMutualTlsModes(RaftNodeAuthenticationMode mode)
    {
        KommanderServerBindingPolicy.ValidateMutualTlsTrustAnchors(mode, []);
    }

    // ---- H4: cleartext listener ---------------------------------------------------------------

    /// <summary>
    /// A plaintext port has no handshake and therefore no peer certificate, so pairing one with mTLS
    /// is an unauthenticated door into a node that is otherwise enforcing certificates. There is no
    /// opt-in for this combination.
    /// </summary>
    [Fact]
    public void ValidatePlaintextListener_Throws_WhenCombinedWithMutualTls()
    {
        RaftException exception = Assert.Throws<RaftException>(() =>
            KommanderServerBindingPolicy.ValidatePlaintextListener(
                RaftNodeAuthenticationMode.MutualTls,
                allowPlaintextListener: true));

        Assert.Contains("--allow-plaintext-listener", exception.Message);
    }

    [Theory]
    [InlineData(RaftNodeAuthenticationMode.Disabled)]
    [InlineData(RaftNodeAuthenticationMode.SharedSecret)]
    public void ValidatePlaintextListener_Allows_OptInInOtherModes(RaftNodeAuthenticationMode mode)
    {
        KommanderServerBindingPolicy.ValidatePlaintextListener(mode, allowPlaintextListener: true);
    }

    [Fact]
    public void ValidatePlaintextListener_Allows_MutualTlsWithoutTheOptIn()
    {
        KommanderServerBindingPolicy.ValidatePlaintextListener(
            RaftNodeAuthenticationMode.MutualTls,
            allowPlaintextListener: false);
    }

    /// <summary>
    /// The core of H4: TLS configured must not imply a cleartext port as well.
    /// </summary>
    [Fact]
    public void ShouldBindPlaintextListener_IsFalse_WhenTlsConfiguredWithoutOptIn()
    {
        Assert.False(KommanderServerBindingPolicy.ShouldBindPlaintextListener(
            tlsConfigured: true,
            allowPlaintextListener: false));
    }

    [Fact]
    public void ShouldBindPlaintextListener_IsTrue_WhenTlsConfiguredAndOptedIn()
    {
        Assert.True(KommanderServerBindingPolicy.ShouldBindPlaintextListener(
            tlsConfigured: true,
            allowPlaintextListener: true));
    }

    /// <summary>
    /// Without a certificate the cleartext listener is the node's only transport, so it stays bound —
    /// otherwise the fix would leave a server with no way to serve at all.
    /// </summary>
    [Fact]
    public void ShouldBindPlaintextListener_IsTrue_WhenNoTlsConfigured()
    {
        Assert.True(KommanderServerBindingPolicy.ShouldBindPlaintextListener(
            tlsConfigured: false,
            allowPlaintextListener: false));
    }

    // ---- Port parsing -------------------------------------------------------------------------

    [Fact]
    public void ParsePorts_FallsBackToDefault_WhenNoneSupplied()
    {
        Assert.Equal([8004], KommanderServerBindingPolicy.ParsePorts(null, 8004, "--http-ports"));
        Assert.Equal([8004], KommanderServerBindingPolicy.ParsePorts([], 8004, "--http-ports"));
    }

    [Fact]
    public void ParsePorts_KeepsSuppliedOrder()
    {
        Assert.Equal(
            [8004, 8081],
            KommanderServerBindingPolicy.ParsePorts(["8004", "8081"], 8004, "--http-ports"));
    }

    [Theory]
    [InlineData("not-a-port")]
    [InlineData("70000")]
    [InlineData("-1")]
    [InlineData("")]
    public void ParsePorts_Throws_NamingTheOptionAndValue(string value)
    {
        RaftException exception = Assert.Throws<RaftException>(() =>
            KommanderServerBindingPolicy.ParsePorts([value], 8004, "--http-ports"));

        Assert.Contains("--http-ports", exception.Message);
    }
}
