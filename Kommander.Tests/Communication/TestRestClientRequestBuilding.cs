using Flurl.Http;
using Kommander;
using Kommander.Communication.Rest;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.Communication;

/// <summary>
/// Verifies what <see cref="RestCommunication.CreateRaftRequest"/> puts on the wire envelope:
/// the signed headers must bind the exact payload bytes (empty payloads included), the legacy
/// bearer token must appear only in the modes that still read it, and the request must carry the
/// configured timeout and media type.
/// </summary>
public sealed class TestRestClientRequestBuilding
{
    private const string Path = "/v1/raft/append-logs";

    [Fact]
    public void SharedSecretMode_SignsTheExactPayloadBytes()
    {
        byte[] payload = """{"partition":0}"""u8.ToArray();
        using RaftManager manager = CreateManager(sharedSecret: "cluster-secret");

        IFlurlRequest request = RestCommunication.CreateRaftRequest(
            manager, new RaftNode("node-b:5001"), Path, payload);

        Assert.True(VerifiesWithTheSignedInputs(manager, request, payload), "signature must bind the exact payload bytes with the headers it travels with");
    }

    /// <summary>
    /// An empty payload (a body-less or default-valued request) is still a body on the wire and
    /// must be signed as such — an unpinned edge that would otherwise let two different payloads
    /// share one signature.
    /// </summary>
    [Fact]
    public void SharedSecretMode_SignsEmptyPayloadBytes()
    {
        using RaftManager manager = CreateManager(sharedSecret: "cluster-secret");

        IFlurlRequest request = RestCommunication.CreateRaftRequest(
            manager, new RaftNode("node-b:5001"), Path, []);

        Assert.True(VerifiesWithTheSignedInputs(manager, request, []), "an empty payload is still a signed body");
    }

    [Fact]
    public void DisabledMode_AddsNoSignatureHeaders()
    {
        using RaftManager manager = CreateManager(sharedSecret: null);

        IFlurlRequest request = RestCommunication.CreateRaftRequest(
            manager, new RaftNode("node-b:5001"), Path, "{}"u8);

        Assert.Null(GetHeader(request, manager.Configuration.TransportSecurity.HeaderName));
        Assert.Null(GetHeader(request, RaftTransportAuthenticationHeaders.SenderNodeHeaderName));
        Assert.Null(GetHeader(request, RaftTransportAuthenticationHeaders.TimestampHeaderName));
        Assert.Null(GetHeader(request, RaftTransportAuthenticationHeaders.NonceHeaderName));
    }

    /// <summary>
    /// The legacy bearer credential still travels in shared-secret (and disabled) modes, but must
    /// be suppressed under mutual TLS where the certificate authenticates the node — a leftover
    /// header there is a credential leak for no benefit.
    /// </summary>
    [Fact]
    public void BearerToken_IsSentInSharedSecretMode_AndSuppressedUnderMutualTls()
    {
        using RaftManager sharedSecret = CreateManager(sharedSecret: "cluster-secret", bearerToken: "legacy-token");
        using RaftManager mutualTls = CreateManager(sharedSecret: null, bearerToken: "legacy-token", mutualTls: true);

        IFlurlRequest fromSharedSecret = RestCommunication.CreateRaftRequest(
            sharedSecret, new RaftNode("node-b:5001"), Path, "{}"u8);
        IFlurlRequest fromMutualTls = RestCommunication.CreateRaftRequest(
            mutualTls, new RaftNode("node-b:5001"), Path, "{}"u8);

        Assert.Equal("Bearer legacy-token", GetHeader(fromSharedSecret, "Authorization"));
        Assert.Null(GetHeader(fromMutualTls, "Authorization"));
    }

    [Fact]
    public void RequestCarriesConfiguredTimeoutAndMediaType()
    {
        using RaftManager manager = CreateManager(sharedSecret: null, httpTimeoutSeconds: 7);

        IFlurlRequest request = RestCommunication.CreateRaftRequest(
            manager, new RaftNode("node-b:5001"), Path, "{}"u8);

        Assert.Equal(TimeSpan.FromSeconds(7), request.Settings.Timeout);
        Assert.Equal("application/json", GetHeader(request, "Accept"));
        Assert.Equal("application/json", GetHeader(request, "Content-Type"));
    }

    /// <summary>
    /// Independent recomputation, as a verifying peer would do: takes the nonce, timestamp and
    /// sender the request actually carries and re-signs the payload with them. A fresh Sign()
    /// here would mint its own nonce and could never match, which is the point of the check.
    /// </summary>
    private static bool VerifiesWithTheSignedInputs(RaftManager manager, IFlurlRequest request, byte[] payload)
    {
        RaftTransportAuthenticator authenticator = manager.Configuration.GetTransportAuthenticator();
        RaftTransportAuthenticationHeaders shaped = authenticator.SignWithBodyHash(
            "POST", Path, manager.GetLocalEndpoint(), global::System.Security.Cryptography.SHA256.HashData([]), 0, "shaped-probe");

        string? signature = GetHeader(request, shaped.SignatureHeaderName);
        string? nonce = GetHeader(request, RaftTransportAuthenticationHeaders.NonceHeaderName);
        string? timestamp = GetHeader(request, RaftTransportAuthenticationHeaders.TimestampHeaderName);
        string? sender = GetHeader(request, RaftTransportAuthenticationHeaders.SenderNodeHeaderName);

        Assert.NotNull(signature);
        Assert.NotNull(nonce);
        Assert.NotNull(timestamp);
        Assert.NotNull(sender);

        RaftTransportAuthenticationHeaders recomputed = authenticator.SignWithBodyHash(
            "POST",
            Path,
            sender!,
            global::System.Security.Cryptography.SHA256.HashData(payload),
            long.Parse(timestamp!, global::System.Globalization.CultureInfo.InvariantCulture),
            nonce!);

        return recomputed.Signature == signature;
    }

    private static string? GetHeader(IFlurlRequest request, string name)
    {
        foreach (var header in request.Headers)
        {
            if (string.Equals(header.Name, name, StringComparison.OrdinalIgnoreCase))
                return header.Value;
        }

        return null;
    }

    private static RaftManager CreateManager(
        string? sharedSecret,
        string? bearerToken = null,
        bool mutualTls = false,
        int httpTimeoutSeconds = 5)
    {
        RaftTransportSecurityOptions security = new();

        if (sharedSecret is not null)
        {
            security.NodeAuthenticationMode = RaftNodeAuthenticationMode.SharedSecret;
            security.SharedSecret = sharedSecret;
        }

        if (mutualTls)
        {
            // The configuration validator refuses mutual TLS without a client certificate, even
            // though this manager only ever builds requests and never connects.
            security.NodeAuthenticationMode = RaftNodeAuthenticationMode.MutualTls;
            security.ClientCertificate = CreateClientCertificate();
        }

        RaftConfiguration configuration = new()
        {
            NodeName = "node-a",
            NodeId = 5000,
            Host = "localhost",
            Port = 5000,
            InitialPartitions = 1,
            HttpScheme = "https://",
            HttpVersion = "1.1",
            HttpTimeout = httpTimeoutSeconds,
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

#pragma warning disable CS0618
        configuration.HttpAuthBearerToken = bearerToken;
#pragma warning restore CS0618

        return new RaftManager(
            configuration,
            new StaticDiscovery([]),
            new InMemoryWAL(NullLogger<IRaft>.Instance),
            new RestCommunication(),
            new HybridLogicalClock(),
            NullLogger<IRaft>.Instance);
    }

    private static global::System.Security.Cryptography.X509Certificates.X509Certificate2 CreateClientCertificate()
    {
        using global::System.Security.Cryptography.RSA key = global::System.Security.Cryptography.RSA.Create(2048);

        global::System.Security.Cryptography.X509Certificates.CertificateRequest request =
            new("CN=node-a", key, global::System.Security.Cryptography.HashAlgorithmName.SHA256,
                global::System.Security.Cryptography.RSASignaturePadding.Pkcs1);

        return request.CreateSelfSigned(
            DateTimeOffset.UtcNow.AddMinutes(-5),
            DateTimeOffset.UtcNow.AddHours(1));
    }
}
