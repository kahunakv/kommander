
using System.Globalization;
using Grpc.Core;
using Kommander;
using Kommander.Communication.Grpc;
using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.Communication;

/// <summary>
/// Covers how <c>RaftService.ValidateAuth</c> decides whether a gRPC call arrived over TLS, which
/// gates <see cref="RaftTransportSecurityOptions.RequireTls"/> in shared-secret mode.
/// </summary>
/// <remarks>
/// <para>
/// Regression tests for a heuristic that inferred the transport from <c>context.Host</c> and
/// <c>context.Peer</c> and was wrong in both directions. <c>Host</c> is the <c>:authority</c>
/// (<c>host:port</c>) and is never scheme-prefixed, so:
/// </para>
/// <list type="bullet">
/// <item>an <c>ipv4:</c> peer was judged insecure and rejected with <c>TlsRequired</c> even over
/// TLS — a live availability break, since <c>RequireTls</c> defaults to true;</item>
/// <item>an <c>ipv6:</c> peer, or a null authority, was judged secure over cleartext and bypassed
/// the gate entirely.</item>
/// </list>
/// <para>
/// Both legs are asserted below, with the peer/authority values that used to decide the outcome,
/// so a regression to any transport-inference heuristic fails here.
/// </para>
/// </remarks>
public sealed class TestRaftServiceTransportSecurity
{
    private const string SharedSecret = "test-cluster-secret";
    private const string GrpcMethod = "/kommander.Rafter/Vote";

    /// <summary>
    /// The availability leg: an IPv4 peer over TLS must authenticate. The old expression returned
    /// false for exactly this shape, so every IPv4 gRPC caller was rejected with TlsRequired.
    /// </summary>
    [Fact]
    public void SignedRequestOverTls_Authenticates_FromIpv4Peer()
    {
        using RaftManager manager = BuildManager();
        RaftService service = new(manager, NullLogger<IRaft>.Instance);
        GrpcVoteRequest request = BuildVoteRequest();

        FakeServerCallContext context = CreateContext(
            manager,
            request,
            isHttps: true,
            host: "node2:8005",
            peer: "ipv4:172.20.0.3:44100");

        // No exception: the TLS gate passed and the signature verified.
        service.Vote(request, context);
    }

    /// <summary>
    /// The H1 regression test: a signature must not survive a rewritten payload.
    /// </summary>
    /// <remarks>
    /// The signature is computed over one request and the call is then made with a different one —
    /// exactly what an on-path attacker does when it rewrites a protobuf body in flight. Before the
    /// body was bound into the signature this was accepted, because the signed string covered only
    /// the method, sender, timestamp and nonce; the payload — vote terms here, log entries and
    /// snapshot bytes elsewhere — was free to change.
    /// </remarks>
    [Fact]
    public void TamperedBodyOverTls_IsRejected_WithInvalidSignature()
    {
        using RaftManager manager = BuildManager();
        RaftService service = new(manager, NullLogger<IRaft>.Instance);

        GrpcVoteRequest signedRequest = BuildVoteRequest();

        FakeServerCallContext context = CreateContext(
            manager,
            signedRequest,
            isHttps: true,
            host: "node2:8005",
            peer: "ipv4:172.20.0.3:44100");

        // Same method, same headers, same valid signature — one field changed.
        GrpcVoteRequest tamperedRequest = BuildVoteRequest();
        tamperedRequest.Term = signedRequest.Term + 1;

        RpcException exception = Assert.Throws<RpcException>(() =>
            InvokeVote(service, context, tamperedRequest));

        Assert.Equal(StatusCode.Unauthenticated, exception.StatusCode);
        Assert.Equal(
            RaftTransportAuthenticationStatus.InvalidSignature.ToString(),
            exception.Status.Detail);
    }

    /// <summary>
    /// Tampering with any field is caught, not just the one the test above happens to pick.
    /// </summary>
    [Fact]
    public void TamperedEndpointOverTls_IsRejected_WithInvalidSignature()
    {
        using RaftManager manager = BuildManager();
        RaftService service = new(manager, NullLogger<IRaft>.Instance);

        GrpcVoteRequest signedRequest = BuildVoteRequest();

        FakeServerCallContext context = CreateContext(
            manager,
            signedRequest,
            isHttps: true,
            host: "node2:8005",
            peer: "ipv4:172.20.0.3:44100");

        GrpcVoteRequest tamperedRequest = BuildVoteRequest();
        tamperedRequest.Endpoint = "attacker:9999";

        RpcException exception = Assert.Throws<RpcException>(() =>
            InvokeVote(service, context, tamperedRequest));

        Assert.Equal(
            RaftTransportAuthenticationStatus.InvalidSignature.ToString(),
            exception.Status.Detail);
    }

    /// <summary>
    /// An equal-but-distinct message instance still verifies: the signature binds the encoded bytes,
    /// not object identity. Without this, body binding would appear to work while actually rejecting
    /// every real request, since the peer always verifies a freshly deserialized instance.
    /// </summary>
    [Fact]
    public void EquivalentBodyInstanceOverTls_Authenticates()
    {
        using RaftManager manager = BuildManager();
        RaftService service = new(manager, NullLogger<IRaft>.Instance);

        FakeServerCallContext context = CreateContext(
            manager,
            BuildVoteRequest(),
            isHttps: true,
            host: "node2:8005",
            peer: "ipv4:172.20.0.3:44100");

        service.Vote(BuildVoteRequest(), context);
    }

    /// <summary>
    /// The security leg: cleartext must be rejected even for the peer/authority shapes the old
    /// heuristic reported as secure.
    /// </summary>
    [Theory]
    [InlineData("node2:8004", "ipv6:[::1]:44100")]   // old heuristic: secure (peer is not ipv4:)
    [InlineData(null, "ipv4:172.20.0.3:44100")]      // old heuristic: secure (null authority)
    [InlineData("node2:8004", "ipv4:172.20.0.3:44100")]
    public void SignedRequestOverCleartext_IsRejected_WithTlsRequired(string? host, string peer)
    {
        using RaftManager manager = BuildManager();
        RaftService service = new(manager, NullLogger<IRaft>.Instance);
        GrpcVoteRequest request = BuildVoteRequest();

        FakeServerCallContext context = CreateContext(manager, request, isHttps: false, host: host, peer: peer);

        RpcException exception = Assert.Throws<RpcException>(() => InvokeVote(service, context, request));

        Assert.Equal(StatusCode.Unauthenticated, exception.StatusCode);
        Assert.Equal(
            RaftTransportAuthenticationStatus.TlsRequired.ToString(),
            exception.Status.Detail);
    }

    /// <summary>
    /// No <see cref="HttpContext"/> means no evidence of TLS, so the null case must fail closed
    /// rather than default to "secure" the way the old expression did.
    /// </summary>
    [Fact]
    public void RequestWithoutHttpContext_IsRejected_WithTlsRequired()
    {
        using RaftManager manager = BuildManager();
        RaftService service = new(manager, NullLogger<IRaft>.Instance);
        GrpcVoteRequest request = BuildVoteRequest();

        FakeServerCallContext context = CreateContext(
            manager,
            request,
            isHttps: false,
            host: "node2:8005",
            peer: "ipv4:172.20.0.3:44100",
            attachHttpContext: false);

        RpcException exception = Assert.Throws<RpcException>(() => InvokeVote(service, context, request));

        Assert.Equal(
            RaftTransportAuthenticationStatus.TlsRequired.ToString(),
            exception.Status.Detail);
    }

    /// <summary>
    /// Passing the TLS gate is not the same as being authenticated: an unsigned request over TLS
    /// must still be rejected, which is what distinguishes "the gate opened" from "the check is gone".
    /// </summary>
    [Fact]
    public void UnsignedRequestOverTls_IsRejected_WithMissingFields()
    {
        using RaftManager manager = BuildManager();
        RaftService service = new(manager, NullLogger<IRaft>.Instance);
        GrpcVoteRequest request = BuildVoteRequest();

        FakeServerCallContext context = CreateContext(
            manager,
            request,
            isHttps: true,
            host: "node2:8005",
            peer: "ipv4:172.20.0.3:44100",
            signRequest: false);

        RpcException exception = Assert.Throws<RpcException>(() => InvokeVote(service, context, request));

        Assert.Equal(
            RaftTransportAuthenticationStatus.MissingFields.ToString(),
            exception.Status.Detail);
    }

    /// <summary>
    /// A tampered signature over TLS is rejected, confirming the signature itself is still verified
    /// on the path these tests exercise.
    /// </summary>
    [Fact]
    public void TamperedSignatureOverTls_IsRejected_WithInvalidSignature()
    {
        using RaftManager manager = BuildManager();
        RaftService service = new(manager, NullLogger<IRaft>.Instance);
        GrpcVoteRequest request = BuildVoteRequest();

        FakeServerCallContext context = CreateContext(
            manager,
            request,
            isHttps: true,
            host: "node2:8005",
            peer: "ipv4:172.20.0.3:44100",
            corruptSignature: true);

        RpcException exception = Assert.Throws<RpcException>(() => InvokeVote(service, context, request));

        Assert.Equal(
            RaftTransportAuthenticationStatus.InvalidSignature.ToString(),
            exception.Status.Detail);
    }

    /// <summary>
    /// Invokes <c>Vote</c> and discards the returned task.
    /// </summary>
    /// <remarks>
    /// <c>ValidateAuth</c> runs before <c>Vote</c> returns, so a rejection surfaces synchronously
    /// rather than as a faulted task. This void wrapper says so at the call site and keeps the
    /// assertions on <c>Assert.Throws</c>, which is what actually matches the behavior.
    /// </remarks>
    private static void InvokeVote(
        RaftService service,
        FakeServerCallContext context,
        GrpcVoteRequest request) =>
        _ = service.Vote(request, context);

    private static GrpcVoteRequest BuildVoteRequest() => new()
    {
        Partition = 0,
        Term = 1,
        Endpoint = "node2:8005"
    };

    /// <summary>
    /// Builds a manager in shared-secret mode. <c>RequireTls</c> is left at its default (true), which
    /// is the configuration the finding is about.
    /// </summary>
    private static RaftManager BuildManager()
    {
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
            TransportSecurity = new RaftTransportSecurityOptions
            {
                NodeAuthenticationMode = RaftNodeAuthenticationMode.SharedSecret,
                SharedSecret = SharedSecret
            }
        };

        return new RaftManager(
            configuration,
            new StaticDiscovery([]),
            new InMemoryWAL(NullLogger<IRaft>.Instance),
            new InMemoryCommunication(),
            new HybridLogicalClock(),
            NullLogger<IRaft>.Instance);
    }

    /// <summary>
    /// Builds a call context whose signature is computed over <paramref name="request"/>, the way a
    /// real peer signs the message it is about to send.
    /// </summary>
    /// <remarks>
    /// The request is signed here and passed to the service separately, so a test can deliberately
    /// send something other than what was signed — which is how the body-tampering cases work.
    /// </remarks>
    private static FakeServerCallContext CreateContext(
        RaftManager manager,
        GrpcVoteRequest request,
        bool isHttps,
        string? host,
        string peer,
        bool attachHttpContext = true,
        bool signRequest = true,
        bool corruptSignature = false)
    {
        Metadata headers = [];

        if (signRequest)
        {
            RaftTransportAuthenticator authenticator = manager.Configuration.GetTransportAuthenticator();

            // Signed through the same helper the production client uses, so a change to how the body
            // is digested breaks both sides together rather than leaving the test asserting a stale
            // encoding.
            Span<byte> bodyHash = stackalloc byte[RaftTransportAuthenticator.BodyHashSizeInBytes];
            GrpcMessageBodyHash.Compute(request, bodyHash);

            RaftTransportAuthenticationHeaders signed = authenticator.SignWithBodyHash(
                "POST",
                GrpcMethod,
                senderNode: "node1",
                bodyHash);

            string signature = corruptSignature
                ? new string([.. signed.Signature.Reverse()])
                : signed.Signature;

            headers.Add(signed.SignatureHeaderName, signature);
            headers.Add(RaftTransportAuthenticationHeaders.SenderNodeHeaderName, signed.SenderNode);
            headers.Add(
                RaftTransportAuthenticationHeaders.TimestampHeaderName,
                signed.TimestampUnixMilliseconds.ToString(CultureInfo.InvariantCulture));
            headers.Add(RaftTransportAuthenticationHeaders.NonceHeaderName, signed.Nonce);
        }

        FakeServerCallContext context = new(GrpcMethod, host, peer, headers);

        if (attachHttpContext)
        {
            DefaultHttpContext httpContext = new();
            httpContext.Request.Scheme = isHttps ? "https" : "http";

            // The key gRPC's GetHttpContext() extension reads out of UserState.
            context.UserState["__HttpContext"] = httpContext;
        }

        return context;
    }

    /// <summary>
    /// Minimal <see cref="ServerCallContext"/> exposing the four members <c>ValidateAuth</c> reads:
    /// method, authority, peer, and request headers — plus the <c>UserState</c> bag that carries the
    /// <see cref="HttpContext"/>.
    /// </summary>
    /// <remarks>
    /// Host and peer are settable, and deliberately still populated, because they are what the
    /// removed heuristic keyed on: a regression that reintroduces it would read these and produce
    /// the wrong answer, which is what the assertions here detect.
    /// </remarks>
    private sealed class FakeServerCallContext(
        string method,
        string? host,
        string peer,
        Metadata requestHeaders) : ServerCallContext
    {
        protected override string MethodCore => method;
        protected override string HostCore => host!;
        protected override string PeerCore => peer;
        protected override DateTime DeadlineCore => DateTime.MaxValue;
        protected override Metadata RequestHeadersCore => requestHeaders;
        protected override CancellationToken CancellationTokenCore => CancellationToken.None;
        protected override Metadata ResponseTrailersCore { get; } = [];
        protected override Status StatusCore { get; set; }
        protected override WriteOptions? WriteOptionsCore { get; set; }
        protected override AuthContext AuthContextCore => new(null, new Dictionary<string, List<AuthProperty>>());
        protected override IDictionary<object, object> UserStateCore { get; } = new Dictionary<object, object>();

        protected override ContextPropagationToken CreatePropagationTokenCore(ContextPropagationOptions? options) =>
            throw new NotImplementedException();

        protected override Task WriteResponseHeadersAsyncCore(Metadata responseHeaders) =>
            throw new NotImplementedException();
    }
}
