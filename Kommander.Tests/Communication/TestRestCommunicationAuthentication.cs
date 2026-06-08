using Kommander.Communication.Rest;

namespace Kommander.Tests.Communication;

public sealed class TestRestCommunicationAuthentication
{
    [Fact]
    public void BuildAuthenticationHeaders_ReturnsEmpty_WhenAuthModeIsDisabled()
    {
        RaftConfiguration configuration = new();

        IReadOnlyDictionary<string, string> headers = RestCommunication.BuildAuthenticationHeaders(
            configuration,
            senderNode: "node-a:5000",
            method: "POST",
            path: "/v1/raft/append-logs",
            payload: "{\"ok\":true}");

        Assert.Empty(headers);
    }

    [Fact]
    public void BuildAuthenticationHeaders_UsesSharedSecretSigning_WhenEnabled()
    {
        RaftConfiguration configuration = new()
        {
            TransportSecurity = new RaftTransportSecurityOptions
            {
                NodeAuthenticationMode = RaftNodeAuthenticationMode.SharedSecret,
                SharedSecret = "cluster-secret"
            }
        };

        IReadOnlyDictionary<string, string> headers = RestCommunication.BuildAuthenticationHeaders(
            configuration,
            senderNode: "node-a:5000",
            method: "POST",
            path: "/v1/raft/append-logs",
            payload: "{\"ok\":true}");

        Assert.Equal(4, headers.Count);
        Assert.Contains(configuration.TransportSecurity.HeaderName, headers.Keys);
        Assert.Equal("node-a:5000", headers[RaftTransportAuthenticationHeaders.SenderNodeHeaderName]);
        Assert.Contains(RaftTransportAuthenticationHeaders.TimestampHeaderName, headers.Keys);
        Assert.Contains(RaftTransportAuthenticationHeaders.NonceHeaderName, headers.Keys);
    }

    [Fact]
    public void BuildAuthenticationHeaders_UsesLegacyBearerSecret_WhenSharedSecretModeEnabledWithoutNewSecret()
    {
        RaftConfiguration configuration = new()
        {
            TransportSecurity = new RaftTransportSecurityOptions
            {
                NodeAuthenticationMode = RaftNodeAuthenticationMode.SharedSecret
            }
        };

#pragma warning disable CS0618
        configuration.HttpAuthBearerToken = "legacy-secret";
#pragma warning restore CS0618

        IReadOnlyDictionary<string, string> actual = RestCommunication.BuildAuthenticationHeaders(
            configuration,
            senderNode: "node-a:5000",
            method: "POST",
            path: "/v1/raft/append-logs",
            payload: "{\"ok\":true}");

        RaftTransportAuthenticator authenticator = new(configuration.GetEffectiveTransportSecurity());
        RaftTransportAuthenticationHeaders expected = authenticator.Sign(
            "POST",
            "/v1/raft/append-logs",
            "node-a:5000",
            global::System.Text.Encoding.UTF8.GetBytes("{\"ok\":true}"),
            long.Parse(actual[RaftTransportAuthenticationHeaders.TimestampHeaderName]),
            actual[RaftTransportAuthenticationHeaders.NonceHeaderName]);

        Assert.Equal(expected.Signature, actual[expected.SignatureHeaderName]);
    }
}
