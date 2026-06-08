using System.Text;
using Kommander.Communication.Rest;
using Kommander.Time;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.Communication;

public sealed class TestRestRouteAuthentication
{
    [Fact]
    public async Task AuthenticateRequestAsync_AllowsValidSignedRequest()
    {
        RaftConfiguration configuration = CreateSharedSecretConfiguration();
        DefaultHttpContext context = CreateContext(
            method: "POST",
            path: "/v1/raft/append-logs",
            body: "{\"ok\":true}",
            isHttps: true);

        foreach ((string key, string value) in RestCommunication.BuildAuthenticationHeaders(
                     configuration,
                     "node-a:5000",
                     "POST",
                     "/v1/raft/append-logs",
                     "{\"ok\":true}"))
        {
            context.Request.Headers[key] = value;
        }

        RaftTransportAuthenticationResult result =
            await RestCommunicationExtensions.AuthenticateRequestAsync(context, configuration);

        Assert.Equal(RaftTransportAuthenticationStatus.Success, result.Status);
    }

    [Fact]
    public async Task AuthenticateRequestAsync_RejectsMissingHeaders()
    {
        DefaultHttpContext context = CreateContext(
            method: "POST",
            path: "/v1/raft/append-logs",
            body: "{\"ok\":true}",
            isHttps: true);

        RaftTransportAuthenticationResult result =
            await RestCommunicationExtensions.AuthenticateRequestAsync(context, CreateSharedSecretConfiguration());

        Assert.Equal(RaftTransportAuthenticationStatus.MissingFields, result.Status);
    }

    [Fact]
    public async Task AuthenticateRequestAsync_RejectsInvalidSignature()
    {
        RaftConfiguration configuration = CreateSharedSecretConfiguration();
        DefaultHttpContext context = CreateContext(
            method: "POST",
            path: "/v1/raft/append-logs",
            body: "{\"ok\":true}",
            isHttps: true);

        foreach ((string key, string value) in RestCommunication.BuildAuthenticationHeaders(
                     configuration,
                     "node-a:5000",
                     "POST",
                     "/v1/raft/append-logs",
                     "{\"ok\":true}"))
        {
            context.Request.Headers[key] = value;
        }

        context.Request.Headers[configuration.TransportSecurity.HeaderName] = "invalid-signature";

        RaftTransportAuthenticationResult result =
            await RestCommunicationExtensions.AuthenticateRequestAsync(context, configuration);

        Assert.Equal(RaftTransportAuthenticationStatus.MalformedFields, result.Status);
    }

    [Fact]
    public async Task AuthenticateRequestAsync_RejectsReplayedNonce()
    {
        RaftConfiguration configuration = CreateSharedSecretConfiguration();
        DefaultHttpContext firstContext = CreateContext(
            method: "POST",
            path: "/v1/raft/append-logs",
            body: "{\"ok\":true}",
            isHttps: true);

        IReadOnlyDictionary<string, string> headers = RestCommunication.BuildAuthenticationHeaders(
            configuration,
            "node-a:5000",
            "POST",
            "/v1/raft/append-logs",
            "{\"ok\":true}");

        foreach ((string key, string value) in headers)
            firstContext.Request.Headers[key] = value;

        RaftTransportAuthenticationResult first =
            await RestCommunicationExtensions.AuthenticateRequestAsync(firstContext, configuration);

        DefaultHttpContext replayContext = CreateContext(
            method: "POST",
            path: "/v1/raft/append-logs",
            body: "{\"ok\":true}",
            isHttps: true);

        foreach ((string key, string value) in headers)
            replayContext.Request.Headers[key] = value;

        RaftTransportAuthenticationResult replay =
            await RestCommunicationExtensions.AuthenticateRequestAsync(replayContext, configuration);

        Assert.Equal(RaftTransportAuthenticationStatus.Success, first.Status);
        Assert.Equal(RaftTransportAuthenticationStatus.ReplayDetected, replay.Status);
    }

    [Fact]
    public async Task AuthenticateRequestAsync_RejectsStaleTimestamp()
    {
        RaftConfiguration configuration = CreateSharedSecretConfiguration();
        DefaultHttpContext context = CreateContext(
            method: "POST",
            path: "/v1/raft/append-logs",
            body: "{\"ok\":true}",
            isHttps: true);

        RaftTransportAuthenticator authenticator = new(configuration.GetEffectiveTransportSecurity());
        RaftTransportAuthenticationHeaders headers = authenticator.Sign(
            "POST",
            "/v1/raft/append-logs",
            "node-a:5000",
            Encoding.UTF8.GetBytes("{\"ok\":true}"),
            DateTimeOffset.UtcNow.AddMinutes(-5).ToUnixTimeMilliseconds(),
            "AAECAwQFBgcICQoLDA0ODw");

        context.Request.Headers[headers.SignatureHeaderName] = headers.Signature;
        context.Request.Headers[RaftTransportAuthenticationHeaders.SenderNodeHeaderName] = headers.SenderNode;
        context.Request.Headers[RaftTransportAuthenticationHeaders.TimestampHeaderName] =
            headers.TimestampUnixMilliseconds.ToString();
        context.Request.Headers[RaftTransportAuthenticationHeaders.NonceHeaderName] = headers.Nonce;

        RaftTransportAuthenticationResult result =
            await RestCommunicationExtensions.AuthenticateRequestAsync(context, configuration);

        Assert.Equal(RaftTransportAuthenticationStatus.TimestampSkewExceeded, result.Status);
    }

    [Fact]
    public async Task AuthenticateRequestAsync_AllowsDisabledMode()
    {
        DefaultHttpContext context = CreateContext(
            method: "GET",
            path: "/v1/raft/get-leader/1",
            body: string.Empty,
            isHttps: false);

        RaftTransportAuthenticationResult result =
            await RestCommunicationExtensions.AuthenticateRequestAsync(context, new RaftConfiguration());

        Assert.Equal(RaftTransportAuthenticationStatus.Disabled, result.Status);
        Assert.True(result.IsAuthenticated);
    }

    [Fact]
    public async Task AuthorizeRequestAsync_Sets401_WhenAuthFails()
    {
        DefaultHttpContext context = CreateContext(
            method: "POST",
            path: "/v1/raft/append-logs",
            body: "{\"ok\":true}",
            isHttps: true);

        RaftManager manager = CreateManager(CreateSharedSecretConfiguration());

        bool allowed = await RestCommunicationExtensions.AuthorizeRequestAsync(context, manager);

        Assert.False(allowed);
        Assert.Equal(StatusCodes.Status401Unauthorized, context.Response.StatusCode);
    }

    [Fact]
    public async Task AuthorizeRequestAsync_DoesNotSet403_ForAuthenticatedOrDisabledRequests()
    {
        DefaultHttpContext disabledContext = CreateContext(
            method: "GET",
            path: "/v1/raft/get-leader/1",
            body: string.Empty,
            isHttps: false);

        RaftManager disabledManager = CreateManager(new RaftConfiguration());
        bool disabledAllowed =
            await RestCommunicationExtensions.AuthorizeRequestAsync(disabledContext, disabledManager);

        DefaultHttpContext authenticatedContext = CreateContext(
            method: "POST",
            path: "/v1/raft/append-logs",
            body: "{\"ok\":true}",
            isHttps: true);

        RaftConfiguration sharedSecretConfiguration = CreateSharedSecretConfiguration();
        foreach ((string key, string value) in RestCommunication.BuildAuthenticationHeaders(
                     sharedSecretConfiguration,
                     "node-a:5000",
                     "POST",
                     "/v1/raft/append-logs",
                     "{\"ok\":true}"))
        {
            authenticatedContext.Request.Headers[key] = value;
        }

        RaftManager authenticatedManager = CreateManager(sharedSecretConfiguration);
        bool authenticatedAllowed =
            await RestCommunicationExtensions.AuthorizeRequestAsync(authenticatedContext, authenticatedManager);

        Assert.True(disabledAllowed);
        Assert.True(authenticatedAllowed);
        Assert.NotEqual(StatusCodes.Status403Forbidden, disabledContext.Response.StatusCode);
        Assert.NotEqual(StatusCodes.Status403Forbidden, authenticatedContext.Response.StatusCode);
    }

    private static DefaultHttpContext CreateContext(string method, string path, string body, bool isHttps)
    {
        DefaultHttpContext context = new();
        context.Request.Method = method;
        context.Request.Path = path;
        context.Request.Scheme = isHttps ? "https" : "http";
        context.Request.Body = new MemoryStream(Encoding.UTF8.GetBytes(body));
        context.Request.ContentLength = context.Request.Body.Length;
        return context;
    }

    private static RaftConfiguration CreateSharedSecretConfiguration()
    {
        return new RaftConfiguration
        {
            TransportSecurity = new RaftTransportSecurityOptions
            {
                NodeAuthenticationMode = RaftNodeAuthenticationMode.SharedSecret,
                SharedSecret = "cluster-secret"
            }
        };
    }

    private static RaftManager CreateManager(RaftConfiguration configuration)
    {
        configuration.Host ??= "node-a";
        configuration.Port = configuration.Port == 0 ? 5000 : configuration.Port;

        return new RaftManager(
            configuration,
            new Kommander.Discovery.StaticDiscovery([new RaftNode("node-a:5000"), new RaftNode("node-b:5001")]),
            new Kommander.WAL.InMemoryWAL(NullLogger<IRaft>.Instance),
            new Kommander.Communication.Memory.InMemoryCommunication(),
            new HybridLogicalClock(),
            NullLogger<IRaft>.Instance);
    }
}
