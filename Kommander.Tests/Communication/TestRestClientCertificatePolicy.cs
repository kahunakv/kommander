using System.Net;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Kommander;
using Kommander.Communication.Rest;
using Kommander.Discovery;
using Kommander.Gossip;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.AspNetCore.Server.Kestrel.Https;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.Communication;

/// <summary>
/// Drives the REST client's certificate policy over a real TLS conversation with a self-signed
/// server certificate: the insecure bypass, thumbprint pinning, and the default (fail-closed)
/// validation each decide whether the call completes.
/// </summary>
/// <remarks>
/// The policy is installed per manager (not process-wide), so each test builds its own caller
/// with one policy setting against the same freshly minted untrusted server certificate. A
/// machine's trust store must never make the self-signed certificate valid, which is what makes
/// the reject legs meaningful.
/// </remarks>
[Collection(ClusterIntegrationCollection.Name)]
public sealed class TestRestClientCertificatePolicy
{
    private const int ServerPort = 9820;
    private static string ServerEndpoint => $"localhost:{ServerPort}";

    [Fact]
    public async Task InsecureValidationBypass_AcceptsSelfSignedServerCertificate()
    {
        await using HttpsRestServer server = await HttpsRestServer.StartAsync();
        using RaftManager caller = CreateCaller(_ => { }, allowInsecure: true);

        RestCommunication rest = new();

        PingResponse response = await rest.SendPing(
            caller, new RaftNode(ServerEndpoint), new PingRequest(ServerEndpoint), TestContext.Current.CancellationToken);

        Assert.True(response.Alive, "the bypass exists exactly to make untrusted dev certificates usable");
    }

    [Fact]
    public async Task DefaultValidation_RejectsSelfSignedServerCertificate()
    {
        await using HttpsRestServer server = await HttpsRestServer.StartAsync();
        using RaftManager caller = CreateCaller(_ => { }, allowInsecure: false);

        RestCommunication rest = new();

        PingResponse response = await rest.SendPing(
            caller, new RaftNode(ServerEndpoint), new PingRequest(ServerEndpoint), TestContext.Current.CancellationToken);

        Assert.False(response.Alive, "an untrusted certificate must fail the call, not bypass validation");
        Assert.Equal(0, response.Incarnation);
    }

    [Fact]
    public async Task PinnedThumbprint_AcceptsSelfSignedServerCertificate()
    {
        await using HttpsRestServer server = await HttpsRestServer.StartAsync();
        using RaftManager caller = CreateCaller(
            security => security.TrustedServerCertificateThumbprints = [Sha256Thumbprint(server.Certificate)],
            allowInsecure: false);

        RestCommunication rest = new();

        PingResponse response = await rest.SendPing(
            caller, new RaftNode(ServerEndpoint), new PingRequest(ServerEndpoint), TestContext.Current.CancellationToken);

        Assert.True(response.Alive, "the server presented exactly the pinned certificate");
    }

    [Fact]
    public async Task PinnedWrongThumbprint_RejectsSelfSignedServerCertificate()
    {
        await using HttpsRestServer server = await HttpsRestServer.StartAsync();

        using RSA otherKey = RSA.Create(2048);
        CertificateRequest otherRequest = new("CN=other", otherKey, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        using X509Certificate2 otherCertificate = otherRequest.CreateSelfSigned(
            DateTimeOffset.UtcNow.AddMinutes(-5), DateTimeOffset.UtcNow.AddHours(1));

        using RaftManager caller = CreateCaller(
            security => security.TrustedServerCertificateThumbprints = [Sha256Thumbprint(otherCertificate)],
            allowInsecure: false);

        RestCommunication rest = new();

        PingResponse response = await rest.SendPing(
            caller, new RaftNode(ServerEndpoint), new PingRequest(ServerEndpoint), TestContext.Current.CancellationToken);

        Assert.False(response.Alive, "a certificate whose thumbprint is not pinned must be refused");
    }

    /// <summary>The pin list stores SHA-256 hex thumbprints, not the SHA-1 <c>Thumbprint</c> property.</summary>
    private static string Sha256Thumbprint(X509Certificate2 certificate) =>
        Convert.ToHexString(certificate.GetCertHash(HashAlgorithmName.SHA256));

    private static RaftManager CreateCaller(Action<RaftTransportSecurityOptions> configureSecurity, bool allowInsecure)
    {
        RaftTransportSecurityOptions security = new() { AllowInsecureCertificateValidation = allowInsecure };
        configureSecurity(security);

        RaftConfiguration configuration = new()
        {
            NodeName = "node-x",
            NodeId = 1,
            Host = "localhost",
            Port = 9821,
            InitialPartitions = 1,
            HttpScheme = "https://",
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
            TransportSecurity = security
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
    /// A one-node HTTPS Kestrel host presenting a freshly minted self-signed certificate for
    /// localhost, with the standard Kommander REST routes mapped.
    /// </summary>
    private sealed class HttpsRestServer : IAsyncDisposable
    {
        private readonly WebApplication _app;
        private readonly RaftManager _manager;

        public X509Certificate2 Certificate { get; }

        private HttpsRestServer(WebApplication app, RaftManager manager, X509Certificate2 certificate)
        {
            _app = app;
            _manager = manager;
            Certificate = certificate;
        }

        public static async Task<HttpsRestServer> StartAsync()
        {
            using RSA key = RSA.Create(2048);
            CertificateRequest request = new("CN=localhost", key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);

            SubjectAlternativeNameBuilder subjectAlternateNames = new();
            subjectAlternateNames.AddDnsName("localhost");
            subjectAlternateNames.AddIpAddress(IPAddress.Loopback);
            request.CertificateExtensions.Add(subjectAlternateNames.Build());

            X509Certificate2 certificate = request.CreateSelfSigned(
                DateTimeOffset.UtcNow.AddMinutes(-5),
                DateTimeOffset.UtcNow.AddHours(2));

            ILogger<IRaft> logger = NullLogger<IRaft>.Instance;

            RaftConfiguration configuration = new()
            {
                NodeId = ServerPort,
                Host = "localhost",
                Port = ServerPort,
                InitialPartitions = 1,
                HttpScheme = "https://",
                HttpVersion = "1.1",
                HeartbeatInterval = TimeSpan.FromMilliseconds(150),
                VotingTimeout = TimeSpan.FromMilliseconds(500),
                StartElectionTimeout = 600,
                EnableQuiescence = false,
                EndElectionTimeout = 900,
                TimerInitialDelay = TimeSpan.FromMilliseconds(500),
                PingInterval = TimeSpan.Zero
            };

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
                kestrel.Listen(
                    IPAddress.Loopback,
                    ServerPort,
                    o =>
                    {
                        o.Protocols = HttpProtocols.Http1;
                        o.UseHttps(certificate);
                    });
            });

            WebApplication app = builder.Build();
            app.MapRestRaftRoutes();

            await app.StartAsync(TestContext.Current.CancellationToken);

            return new HttpsRestServer(app, manager, certificate);
        }

        public async ValueTask DisposeAsync()
        {
            _manager.Dispose();
            using CancellationTokenSource cts = new(TimeSpan.FromSeconds(2));
            try { await _app.StopAsync(cts.Token).ConfigureAwait(false); } catch { /* best-effort */ }
            await _app.DisposeAsync().ConfigureAwait(false);
            Certificate.Dispose();
        }
    }
}
