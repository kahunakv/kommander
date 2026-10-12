using System.Globalization;
using Google.Protobuf;
using Grpc.Core;
using Kommander;
using Kommander.Communication.Grpc;
using Kommander.Communication.Memory;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.Communication;

/// <summary>
/// The server-side authentication gate of <see cref="RaftService"/> for the rejection branches
/// the regular transport tests do not reach: a replayed nonce, an undecodable timestamp, a stale
/// timestamp, and the mutual-TLS door when no certificate can be observed. Each must surface as
/// an <see cref="StatusCode.Unauthenticated"/> failure naming the reason, never as an unhandled
/// fault, and a disabled-mode service must let the call through with no credentials at all.
/// </summary>
/// <remarks>
/// Shares the static replay cache with the other authentication tests, so this class joins the
/// serializing collection and resets the cache before exercising replay detection.
/// </remarks>
[Collection(AuthTestCollection.Name)]
public sealed class TestRaftServiceAuthGates
{
    private const string SharedSecret = "auth-gate-secret";
    private const string GrpcMethod = "/Rafter/Vote";

    [Fact]
    public void DisabledMode_RequestWithoutCredentials_Passes()
    {
        using RaftManager manager = BuildManager(mode: RaftNodeAuthenticationMode.Disabled, secret: null);
        RaftService service = new(manager, NullLogger<IRaft>.Instance);

        FakeServerCallContext context = new(GrpcMethod, headers: [], attachHttpContext: true, isHttps: false);

        // No exception: with authentication disabled there is no gate to fail.
        _ = service.Vote(BuildVoteRequest(), context);
    }

    [Fact]
    public void ReplayedNonce_IsRejectedWithReplayDetected()
    {
        RaftTransportAuthenticator.ResetReplayCacheForTesting();

        using RaftManager manager = BuildManager(mode: RaftNodeAuthenticationMode.SharedSecret, secret: SharedSecret);
        RaftService service = new(manager, NullLogger<IRaft>.Instance);
        GrpcVoteRequest request = BuildVoteRequest();

        Metadata headers = SignHeaders(manager, request);

        // First use of the credential is accepted (no throw)…
        _ = service.Vote(request, new FakeServerCallContext(GrpcMethod, headers, attachHttpContext: true, isHttps: true));

        // …and its replay is rejected by the nonce cache.
        RpcException replay = AssertThrowsRpcException(
            () => _ = service.Vote(request, new FakeServerCallContext(GrpcMethod, headers, attachHttpContext: true, isHttps: true)));

        Assert.Equal(StatusCode.Unauthenticated, replay.StatusCode);
        Assert.Equal(RaftTransportAuthenticationStatus.ReplayDetected.ToString(), replay.Status.Detail);
    }

    [Fact]
    public void UndecodableTimestamp_IsRejectedWithMalformedFields()
    {
        RaftTransportAuthenticator.ResetReplayCacheForTesting();

        using RaftManager manager = BuildManager(mode: RaftNodeAuthenticationMode.SharedSecret, secret: SharedSecret);
        RaftService service = new(manager, NullLogger<IRaft>.Instance);
        GrpcVoteRequest request = BuildVoteRequest();

        Metadata headers = SignHeaders(manager, request);
        ReplaceHeader(
            headers,
            RaftTransportAuthenticationHeaders.TimestampHeaderName,
            "not-a-unix-timestamp");

        RpcException exception = AssertThrowsRpcException(
            () => _ = service.Vote(request, new FakeServerCallContext(GrpcMethod, headers, attachHttpContext: true, isHttps: true)));

        Assert.Equal(StatusCode.Unauthenticated, exception.StatusCode);
        Assert.Equal(RaftTransportAuthenticationStatus.MalformedFields.ToString(), exception.Status.Detail);
    }

    [Fact]
    public void StaleTimestamp_IsRejectedWithTimestampSkewExceeded()
    {
        RaftTransportAuthenticator.ResetReplayCacheForTesting();

        using RaftManager manager = BuildManager(mode: RaftNodeAuthenticationMode.SharedSecret, secret: SharedSecret);
        RaftService service = new(manager, NullLogger<IRaft>.Instance);
        GrpcVoteRequest request = BuildVoteRequest();

        // Signed with a secret the receiver shares, but stamped far outside the allowed skew.
        RaftTransportAuthenticator authenticator = manager.Configuration.GetTransportAuthenticator();
        RaftTransportAuthenticationHeaders signed = authenticator.Sign(
            "POST",
            GrpcMethod,
            "node1:8001",
            request.ToByteArray(),
            DateTimeOffset.UtcNow.AddMinutes(-30).ToUnixTimeMilliseconds(),
            "AAECAwQFBgcICQoLDA0ODw");

        Metadata headers =
        [
            new(signed.SignatureHeaderName, signed.Signature),
            new(RaftTransportAuthenticationHeaders.SenderNodeHeaderName, signed.SenderNode),
            new(RaftTransportAuthenticationHeaders.TimestampHeaderName,
                signed.TimestampUnixMilliseconds.ToString(CultureInfo.InvariantCulture)),
            new(RaftTransportAuthenticationHeaders.NonceHeaderName, signed.Nonce)
        ];

        RpcException exception = AssertThrowsRpcException(
            () => _ = service.Vote(request, new FakeServerCallContext(GrpcMethod, headers, attachHttpContext: true, isHttps: true)));

        Assert.Equal(StatusCode.Unauthenticated, exception.StatusCode);
        Assert.Equal(RaftTransportAuthenticationStatus.TimestampSkewExceeded.ToString(), exception.Status.Detail);
    }

    /// <summary>
    /// In mutual-TLS mode there is nothing a signature could do: with no ASP.NET Core context
    /// behind the call there is no TLS evidence at all, and the gate must fail closed with the
    /// TLS-required verdict rather than fall back to header validation.
    /// </summary>
    [Fact]
    public void MutualTls_WithoutHttpContext_IsRejectedWithTlsRequired()
    {
        using RaftManager manager = BuildManager(mode: RaftNodeAuthenticationMode.MutualTls, secret: null);
        RaftService service = new(manager, NullLogger<IRaft>.Instance);

        RpcException exception = AssertThrowsRpcException(
            () => _ = service.Vote(BuildVoteRequest(), new FakeServerCallContext(GrpcMethod, headers: [], attachHttpContext: false, isHttps: false)));

        Assert.Equal(StatusCode.Unauthenticated, exception.StatusCode);
        Assert.Equal(RaftTransportAuthenticationStatus.TlsRequired.ToString(), exception.Status.Detail);
    }

    /// <summary>
    /// Even over TLS, a caller that presented no client certificate cannot be authenticated in
    /// mutual-TLS mode — the mode has no fallback credential.
    /// </summary>
    [Fact]
    public void MutualTls_OverTlsWithoutClientCertificate_IsRejectedWithCertificateRequired()
    {
        using RaftManager manager = BuildManager(mode: RaftNodeAuthenticationMode.MutualTls, secret: null);
        RaftService service = new(manager, NullLogger<IRaft>.Instance);

        DefaultHttpContext httpContext = new();
        httpContext.Request.Scheme = "https";

        RpcException exception = AssertThrowsRpcException(
            () => _ = service.Vote(BuildVoteRequest(), new FakeServerCallContext(GrpcMethod, headers: [], attachHttpContext: true, isHttps: true, httpContext: httpContext)));

        Assert.Equal(StatusCode.Unauthenticated, exception.StatusCode);
        Assert.Equal(RaftTransportAuthenticationStatus.CertificateRequired.ToString(), exception.Status.Detail);
    }

    private static RpcException AssertThrowsRpcException(Action action) =>
        Assert.Throws<RpcException>(action);

    private static GrpcVoteRequest BuildVoteRequest() => new()
    {
        Partition = 0,
        Term = 1,
        Endpoint = "node2:8005"
    };

    private static Metadata SignHeaders(RaftManager manager, GrpcVoteRequest request)
    {
        RaftTransportAuthenticator authenticator = manager.Configuration.GetTransportAuthenticator();

        Span<byte> bodyHash = stackalloc byte[RaftTransportAuthenticator.BodyHashSizeInBytes];
        GrpcMessageBodyHash.Compute(request, bodyHash);

        RaftTransportAuthenticationHeaders signed = authenticator.SignWithBodyHash(
            "POST", GrpcMethod, "node1:8001", bodyHash);

        return
        [
            new(signed.SignatureHeaderName, signed.Signature),
            new(RaftTransportAuthenticationHeaders.SenderNodeHeaderName, signed.SenderNode),
            new(RaftTransportAuthenticationHeaders.TimestampHeaderName,
                signed.TimestampUnixMilliseconds.ToString(CultureInfo.InvariantCulture)),
            new(RaftTransportAuthenticationHeaders.NonceHeaderName, signed.Nonce)
        ];
    }

    private static void ReplaceHeader(Metadata headers, string name, string value)
    {
        for (int i = headers.Count - 1; i >= 0; i--)
        {
            if (headers[i].Key.Equals(name, StringComparison.OrdinalIgnoreCase))
                headers.RemoveAt(i);
        }

        headers.Add(name, value);
    }

    private static RaftManager BuildManager(RaftNodeAuthenticationMode mode, string? secret)
    {
        RaftTransportSecurityOptions security = new() { NodeAuthenticationMode = mode };

        if (secret is not null)
            security.SharedSecret = secret;

        if (mode == RaftNodeAuthenticationMode.MutualTls)
        {
            // The configuration validator refuses mutual TLS without a client certificate. The
            // certificate is configured but never presented by these fake call contexts, which is
            // exactly the rejection under test.
            security.ClientCertificate = CreateClientCertificate();
        }

        RaftConfiguration configuration = new()
        {
            NodeName = "node1",
            NodeId = 1,
            Host = "localhost",
            Port = 8001,
            InitialPartitions = 1,
            HeartbeatInterval = TimeSpan.FromMilliseconds(50),
            RecentHeartbeat = TimeSpan.FromMilliseconds(25),
            VotingTimeout = TimeSpan.FromMilliseconds(250),
            CheckLeaderInterval = TimeSpan.FromMilliseconds(25),
            UpdateNodesInterval = TimeSpan.FromMilliseconds(100),
            TimerInitialDelay = TimeSpan.FromMilliseconds(25),
            StartElectionTimeout = 100,
            EndElectionTimeout = 250,
            EnableQuiescence = false,
            TransportSecurity = security
        };

        return new RaftManager(
            configuration,
            new StaticDiscovery([]),
            new InMemoryWAL(NullLogger<IRaft>.Instance),
            new InMemoryCommunication(),
            new HybridLogicalClock(),
            NullLogger<IRaft>.Instance);
    }

    private static global::System.Security.Cryptography.X509Certificates.X509Certificate2 CreateClientCertificate()
    {
        using global::System.Security.Cryptography.RSA key = global::System.Security.Cryptography.RSA.Create(2048);

        global::System.Security.Cryptography.X509Certificates.CertificateRequest request =
            new("CN=node1", key, global::System.Security.Cryptography.HashAlgorithmName.SHA256,
                global::System.Security.Cryptography.RSASignaturePadding.Pkcs1);

        return request.CreateSelfSigned(
            DateTimeOffset.UtcNow.AddMinutes(-5),
            DateTimeOffset.UtcNow.AddHours(1));
    }

    /// <summary>
    /// Minimal <see cref="ServerCallContext"/> exposing the members <c>ValidateAuth</c> reads:
    /// the method, the request headers, and (optionally) the ASP.NET Core context that carries
    /// the TLS state. The context travels in <c>UserState</c>, which is where the
    /// <c>GetHttpContext()</c> extension looks; when none is attached the gate must fail closed.
    /// </summary>
    private sealed class FakeServerCallContext(
        string method,
        Metadata headers,
        bool attachHttpContext,
        bool isHttps,
        HttpContext? httpContext = null) : ServerCallContext
    {
        // The key gRPC's GetHttpContext() extension reads out of UserState.
        private const string HttpContextUserStateKey = "__HttpContext";

        private readonly IDictionary<object, object> _userState = CreateUserState(attachHttpContext, isHttps, httpContext);

        protected override string MethodCore => method;
        protected override string HostCore => "node2:8005";
        protected override string PeerCore => "ipv4:127.0.0.1:44100";
        protected override DateTime DeadlineCore => DateTime.MaxValue;
        protected override Metadata RequestHeadersCore => headers;
        protected override CancellationToken CancellationTokenCore => CancellationToken.None;
        protected override Metadata ResponseTrailersCore { get; } = [];
        protected override Status StatusCore { get; set; }
        protected override WriteOptions? WriteOptionsCore { get; set; }
        protected override AuthContext AuthContextCore => new(null, new Dictionary<string, List<AuthProperty>>());
        protected override IDictionary<object, object> UserStateCore => _userState;

        protected override ContextPropagationToken CreatePropagationTokenCore(ContextPropagationOptions? options) =>
            throw new NotImplementedException();

        protected override Task WriteResponseHeadersAsyncCore(Metadata responseHeaders) =>
            throw new NotImplementedException();

        private static IDictionary<object, object> CreateUserState(
            bool attachHttpContext, bool isHttps, HttpContext? httpContext)
        {
            Dictionary<object, object> userState = [];

            HttpContext? resolved = httpContext;

            if (resolved is null && attachHttpContext)
            {
                resolved = new DefaultHttpContext();
                resolved.Request.Scheme = isHttps ? "https" : "http";
            }

            if (resolved is not null)
                userState[HttpContextUserStateKey] = resolved;

            return userState;
        }
    }
}
