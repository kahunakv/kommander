using System.Net;
using System.Security.Cryptography;
using System.Text;
using Kommander;
using Kommander.Communication.Rest;
using Kommander.Discovery;
using Kommander.Gossip;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.Communication;

/// <summary>
/// Drives the REST authentication middleware over a real HTTP conversation: every rejection
/// branch of <c>AuthenticateRequestAsync</c> must answer 401 (before the handler runs), and a
/// correctly signed request from a peer that shares the secret must be let through.
/// </summary>
/// <remarks>
/// The unit tests in <c>TestRestRouteAuthentication</c> assert the returned status for each
/// branch; these tests pin the wire-level consequences — the status code an actual client sees,
/// and that the transport's own failure mapping (not an unhandled 500) is what surfaces.
/// </remarks>
[Collection(ClusterIntegrationCollection.Name)]
public sealed class TestRestSignatureMiddleware
{
    private const string ServerSecret = "rest-middleware-secret";
    private const string ServerEndpoint = "localhost:9800";
    private const int ServerPort = 9800;
    private const string PingPath = "/v1/raft/ping";

    private static readonly byte[] PingBody = Encoding.UTF8.GetBytes("""{"senderEndpoint":"node-x:1"}""");

    [Fact]
    public async Task SameSecretClientCall_IsAccepted()
    {
        await using SharedSecretRestNode server = await SharedSecretRestNode.StartAsync(ServerPort, ServerSecret);
        using RaftManager caller = CreateCallerManager(ServerSecret);

        RestCommunication rest = new();

        PingResponse response = await rest.SendPing(
            caller, new RaftNode(ServerEndpoint), new PingRequest("node-x:1"), TestContext.Current.CancellationToken);

        Assert.True(response.Alive, "a request signed with the cluster secret must reach the handler");
    }

    [Fact]
    public async Task DifferentSecretClientCall_MapsToUnreachable()
    {
        await using SharedSecretRestNode server = await SharedSecretRestNode.StartAsync(ServerPort, ServerSecret);
        using RaftManager caller = CreateCallerManager("an-attacker-secret");

        RestCommunication rest = new();

        PingResponse response = await rest.SendPing(
            caller, new RaftNode(ServerEndpoint), new PingRequest("node-x:1"), TestContext.Current.CancellationToken);

        Assert.False(response.Alive, "a signature made with a different secret must not be honored");
        Assert.Equal(0, response.Incarnation);
    }

    [Fact]
    public async Task FreshlySignedRequest_IsAccepted_WithStatus200()
    {
        await using SharedSecretRestNode server = await SharedSecretRestNode.StartAsync(ServerPort, ServerSecret);

        using HttpResponseMessage response = await SendSignedRequestAsync(
            ServerSecret, timestampUnixMilliseconds: null);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    [Fact]
    public async Task RequestWithoutSignatureHeaders_IsRefusedWith401()
    {
        await using SharedSecretRestNode server = await SharedSecretRestNode.StartAsync(ServerPort, ServerSecret);

        using HttpClient client = new();
        using StringContent content = new(Encoding.UTF8.GetString(PingBody), Encoding.UTF8, "application/json");

        using HttpResponseMessage response = await client.PostAsync(
            $"http://{ServerEndpoint}{PingPath}", content, TestContext.Current.CancellationToken);

        Assert.Equal(HttpStatusCode.Unauthorized, response.StatusCode);
    }

    [Fact]
    public async Task GarbageSignature_IsRefusedWith401()
    {
        await using SharedSecretRestNode server = await SharedSecretRestNode.StartAsync(ServerPort, ServerSecret);

        // All four headers present and well-named, but the signature is not a decodable value.
        using HttpResponseMessage response = await SendSignedRequestAsync(
            ServerSecret, signatureOverride: "this-is-not-a-signature");

        Assert.Equal(HttpStatusCode.Unauthorized, response.StatusCode);
    }

    [Fact]
    public async Task WrongSecretSignature_IsRefusedWith401()
    {
        await using SharedSecretRestNode server = await SharedSecretRestNode.StartAsync(ServerPort, ServerSecret);

        // Structurally valid headers signed with a different secret than the server holds.
        using HttpResponseMessage response = await SendSignedRequestAsync(
            "not-the-cluster-secret", timestampUnixMilliseconds: null);

        Assert.Equal(HttpStatusCode.Unauthorized, response.StatusCode);
    }

    [Fact]
    public async Task ExpiredTimestampSignature_IsRefusedWith401()
    {
        await using SharedSecretRestNode server = await SharedSecretRestNode.StartAsync(ServerPort, ServerSecret);

        long stale = DateTimeOffset.UtcNow.AddMinutes(-10).ToUnixTimeMilliseconds();

        using HttpResponseMessage response = await SendSignedRequestAsync(
            ServerSecret, timestampUnixMilliseconds: stale);

        Assert.Equal(HttpStatusCode.Unauthorized, response.StatusCode);
    }

    /// <summary>
    /// Sends one signed ping with full control over the credential: the secret that produced the
    /// signature, an optional forced (stale) timestamp, and an optional raw signature override.
    /// The nonce is fixed so a signature recomputed for a forced timestamp is reproducible.
    /// </summary>
    private static async Task<HttpResponseMessage> SendSignedRequestAsync(
        string signingSecret, long? timestampUnixMilliseconds = null, string? signatureOverride = null)
    {
        RaftConfiguration signingConfiguration = new()
        {
            TransportSecurity = new RaftTransportSecurityOptions
            {
                NodeAuthenticationMode = RaftNodeAuthenticationMode.SharedSecret,
                SharedSecret = signingSecret,
                RequireTls = false
            }
        };

        RaftTransportAuthenticator authenticator = signingConfiguration.GetTransportAuthenticator();

        RaftTransportAuthenticationHeaders signed = timestampUnixMilliseconds is { } timestamp
            ? authenticator.Sign("POST", PingPath, "node-x:1", PingBody, timestamp, FixedNonce)
            : authenticator.SignWithBodyHash("POST", PingPath, "node-x:1", SHA256.HashData(PingBody));

        HttpRequestMessage request = new(HttpMethod.Post, $"http://{ServerEndpoint}{PingPath}")
        {
            Content = new ByteArrayContent(PingBody)
        };
        request.Content.Headers.ContentType = new("application/json");
        request.Headers.Add(signed.SignatureHeaderName, signatureOverride ?? signed.Signature);
        request.Headers.Add(RaftTransportAuthenticationHeaders.SenderNodeHeaderName, signed.SenderNode);
        request.Headers.Add(
            RaftTransportAuthenticationHeaders.TimestampHeaderName,
            signed.TimestampUnixMilliseconds.ToString());
        request.Headers.Add(RaftTransportAuthenticationHeaders.NonceHeaderName, signed.Nonce);

        using HttpClient client = new();
        return await client.SendAsync(request, TestContext.Current.CancellationToken);
    }

    private const string FixedNonce = "AAECAwQFBgcICQoLDA0ODw";

    private static RaftManager CreateCallerManager(string secret)
    {
        RaftConfiguration configuration = new()
        {
            NodeName = "node-x",
            NodeId = 1,
            Host = "localhost",
            Port = 9801,
            InitialPartitions = 1,
            HttpScheme = "http://",
            HttpVersion = "1.1",
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
                SharedSecret = secret,
                RequireTls = false
            }
        };

        return new RaftManager(
            configuration,
            new StaticDiscovery([]),
            new InMemoryWAL(NullLogger<IRaft>.Instance),
            new RestCommunication(),
            new HybridLogicalClock(),
            NullLogger<IRaft>.Instance);
    }

    /// <summary>
    /// A one-node Kestrel host whose Raft routes demand a shared-secret signature, mirroring
    /// <see cref="RestTestNode"/> with a security configuration the default factory cannot express.
    /// </summary>
    private sealed class SharedSecretRestNode : IAsyncDisposable
    {
        private readonly WebApplication _app;
        private readonly RaftManager _manager;

        private SharedSecretRestNode(WebApplication app, RaftManager manager)
        {
            _app = app;
            _manager = manager;
        }

        public static async Task<SharedSecretRestNode> StartAsync(int port, string secret)
        {
            RaftConfiguration configuration = new()
            {
                NodeId = port,
                Host = "localhost",
                Port = port,
                InitialPartitions = 1,
                HttpScheme = "http://",
                HttpVersion = "1.1",
                HeartbeatInterval = TimeSpan.FromMilliseconds(150),
                VotingTimeout = TimeSpan.FromMilliseconds(500),
                StartElectionTimeout = 600,
                EnableQuiescence = false,
                EndElectionTimeout = 900,
                TimerInitialDelay = TimeSpan.FromMilliseconds(500),
                PingInterval = TimeSpan.Zero,
                TransportSecurity = new RaftTransportSecurityOptions
                {
                    NodeAuthenticationMode = RaftNodeAuthenticationMode.SharedSecret,
                    SharedSecret = secret,
                    RequireTls = false
                }
            };

            ILogger<IRaft> logger = NullLogger<IRaft>.Instance;

            RaftManager manager = new(
                configuration,
                new StaticDiscovery([]),
                new InMemoryWAL(logger),
                new RestCommunication(),
                new HybridLogicalClock(),
                logger);

            WebApplicationBuilder builder = WebApplication.CreateBuilder();
            builder.Logging.ClearProviders();
            builder.Services.AddSingleton<IRaft>(manager);
            builder.Services.AddSingleton(logger);

            builder.WebHost.ConfigureKestrel(kestrel =>
            {
                kestrel.Listen(IPAddress.Loopback, port, o => o.Protocols = HttpProtocols.Http1);
            });

            WebApplication app = builder.Build();
            app.MapRestRaftRoutes();

            await app.StartAsync(TestContext.Current.CancellationToken);

            return new SharedSecretRestNode(app, manager);
        }

        public async ValueTask DisposeAsync()
        {
            _manager.Dispose();
            using CancellationTokenSource cts = new(TimeSpan.FromSeconds(2));
            try { await _app.StopAsync(cts.Token).ConfigureAwait(false); } catch { /* best-effort */ }
            await _app.DisposeAsync().ConfigureAwait(false);
        }
    }
}
