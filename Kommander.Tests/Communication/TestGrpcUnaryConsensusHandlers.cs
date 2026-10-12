using Google.Protobuf;
using Grpc.Core;
using Kommander;
using Kommander.Communication;
using Kommander.Communication.Grpc;
using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.Gossip;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.Communication;

/// <summary>
/// The unary consensus RPC surface of <see cref="RaftService"/> — the methods the production
/// client no longer drives (its traffic rides the duplex BatchRequests stream), left answering
/// for interop callers: a RequestVotes carrying a higher term must be adopted by the receiving
/// partition even though the empty-log candidate is then denied on freshness, an AppendLogs or
/// CompleteAppendLogs for a partition the node does not host must be dropped without faulting
/// the call or disturbing the hosted partitions (the payload, including a sliced log body, is
/// still mapped first), and the mutual-TLS door must let a presented, trusted certificate
/// through.
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
public class TestGrpcUnaryConsensusHandlers
{
    private const int NodePort = 9866;
    private const string NodeEndpoint = "localhost:9866";

    static TestGrpcUnaryConsensusHandlers()
    {
        AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);
    }

    /// <summary>
    /// A unary RequestVotes from a roster member whose log is empty is denied on §5.4.1
    /// freshness — but the higher term it carries is adopted FIRST, unconditionally: without the
    /// adoption a node keeps answering its deposed leader at the old term. The partition's term
    /// must advance to the requested term even though no vote was granted back.
    /// </summary>
    [Fact]
    public async Task RequestVotes_HigherTermIsAdopted_EvenWhenTheVoteIsDeniedOnFreshness()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        using RaftManager manager = await BuildJoinedManager(NodePort);

        try
        {
            RaftService service = new(manager, NullLogger<IRaft>.Instance);

            RaftPartitionView before = await AwaitViewAsync(manager, partitionId: 1, ct);
            long requestedTerm = before.Term + 4;

            GrpcRequestVotesRequest request = new()
            {
                Partition = 1,
                Term = requestedTerm,
                MaxLogId = 0,
                LastLogTerm = 0,
                Endpoint = NodeEndpoint,
                PreVote = false,
            };

            _ = await service.RequestVotes(request, NewContext("/Rafter/RequestVotes"));

            RaftPartitionView after = await AwaitTermAtLeastAsync(manager, partitionId: 1, requestedTerm, ct);
            Assert.True(
                after.Term >= requestedTerm,
                $"the partition term must adopt the requested term {requestedTerm}; it reads {after.Term}");
        }
        finally
        {
            await manager.LeaveCluster(true, CancellationToken.None);
        }
    }

    /// <summary>
    /// An AppendLogs naming a partition this node does not host is dropped per-item, not
    /// faulted: the RPC answers normally and the hosted partitions are untouched. The log
    /// bodies are still mapped before the drop — including a body backed by a slice of a larger
    /// buffer, which must be materialized rather than aliased.
    /// </summary>
    [Fact]
    public async Task AppendLogs_ForUnhostedPartition_IsDroppedWithoutFaultingTheCall()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        using RaftManager manager = await BuildJoinedManager(NodePort);

        try
        {
            RaftService service = new(manager, NullLogger<IRaft>.Instance);
            RaftPartitionView before = await AwaitViewAsync(manager, partitionId: 1, ct);

            GrpcAppendLogsRequest request = new()
            {
                Partition = 4242,
                Term = 1,
                Endpoint = NodeEndpoint,
                PrevLogIndex = 0,
                PrevLogTerm = 0,
            };

            byte[] backing = new byte[32];
            GrpcRaftLog sliced = new()
            {
                Id = 1,
                Term = 1,
                LogType = "test",
                Data = UnsafeByteOperations.UnsafeWrap(backing.AsMemory(3, 7)),
            };
            request.Logs.Add(sliced);

            _ = await service.AppendLogs(request, NewContext("/Rafter/AppendLogs"));

            RaftPartitionView after = await AwaitViewAsync(manager, partitionId: 1, ct);
            Assert.Equal(before.Term, after.Term);
            Assert.Equal(before.CommitIndex, after.CommitIndex);
        }
        finally
        {
            await manager.LeaveCluster(true, CancellationToken.None);
        }
    }

    /// <summary>
    /// The containment contract for completion acks: an ack naming an unhosted partition is
    /// dropped, the RPC completes, and no hosted partition's term moves.
    /// </summary>
    [Fact]
    public async Task CompleteAppendLogs_ForUnhostedPartition_IsDroppedWithoutFaultingTheCall()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        using RaftManager manager = await BuildJoinedManager(NodePort);

        try
        {
            RaftService service = new(manager, NullLogger<IRaft>.Instance);
            RaftPartitionView before = await AwaitViewAsync(manager, partitionId: 1, ct);

            GrpcCompleteAppendLogsRequest request = new()
            {
                Partition = 4242,
                Term = 1,
                Endpoint = NodeEndpoint,
                Status = GrpcRaftOperationStatus.Success,
                CommitIndex = 99,
                DurableIndex = 99,
            };

            _ = await service.CompleteAppendLogs(request, NewContext("/Rafter/CompleteAppendLogs"));

            RaftPartitionView after = await AwaitViewAsync(manager, partitionId: 1, ct);
            Assert.Equal(before.Term, after.Term);
        }
        finally
        {
            await manager.LeaveCluster(true, CancellationToken.None);
        }
    }

    /// <summary>
    /// The mutual-TLS door with a certificate the validator accepts (an in-validity certificate
    /// and an empty allow-list delegating trust to the host's own handshake validation): the
    /// call proceeds past authentication instead of being rejected or faulted.
    /// </summary>
    [Fact]
    public async Task MutualTls_PresentedTrustedCertificate_LetsTheCallThrough()
    {
        using global::System.Security.Cryptography.X509Certificates.X509Certificate2 certificate = CreateCertificate();

        RaftTransportSecurityOptions security = new()
        {
            NodeAuthenticationMode = RaftNodeAuthenticationMode.MutualTls,
            ClientCertificate = certificate,
        };

        using RaftManager manager = BuildUnjoinedManager(NodePort, security);
        RaftService service = new(manager, NullLogger<IRaft>.Instance);

        DefaultHttpContext httpContext = new();
        httpContext.Request.Scheme = "https";
        httpContext.Connection.ClientCertificate = certificate;

        GrpcVoteRequest request = new() { Partition = 0, Term = 1, Endpoint = NodeEndpoint };

        // No RpcException: the certificate satisfied the gate and the vote itself is a drop
        // (the manager hosts nothing before join).
        _ = await service.Vote(request, NewContext("/Rafter/Vote", httpContext: httpContext));
    }

    /// <summary>
    /// A gossip push carrying only a piggybacked load report (no roster) is answered with the
    /// receiver's own membership version over the wire — the report JSON must survive the
    /// handler's deserialization without faulting the RPC.
    /// Ports 9860-9862.
    /// </summary>
    [Fact]
    public async Task Gossip_WithPiggybackedLoadReport_AnswersWithTheLocalVersion()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        using ILoggerFactory logFactory = LoggerFactory.Create(b => b.SetMinimumLevel(LogLevel.Warning));

        await using GrpcClusterHarness harness = await GrpcClusterHarness.CreateAsync([9860, 9861, 9862], logFactory, partitions: 1, ct: ct);

        GrpcTestNode sender = harness.Nodes[0];
        GrpcTestNode receiver = harness.Nodes[1];

        GrpcCommunication grpc = new();
        GossipMessage digest = new(sender.Endpoint, 0, Roster: null)
        {
            LoadReport = new NodeLoadReport
            {
                Endpoint = sender.Endpoint,
                ReportVersion = 1,
                Incarnation = 1,
            },
        };

        GossipAck ack = await grpc.SendGossip(sender.Manager, new RaftNode(receiver.Endpoint), digest, ct);

        Assert.True(ack.MembershipVersion > 0, "the receiver must answer with its committed roster version");
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private static FakeServerCallContext NewContext(string method, HttpContext? httpContext = null) =>
        new(method, headers: [], attachHttpContext: true, isHttps: false, httpContext);

    private static async Task<RaftPartitionView> AwaitViewAsync(RaftManager manager, int partitionId, CancellationToken ct)
    {
        using CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(TimeSpan.FromSeconds(15));

        while (!cts.Token.IsCancellationRequested)
        {
            RaftPartitionView? view = await manager.GetPartitionViewAsync(partitionId, cts.Token).ConfigureAwait(false);
            if (view is not null && view.Term > 0)
                return view;

            await Task.Delay(100, cts.Token).ConfigureAwait(false);
        }

        throw new TimeoutException($"partition {partitionId} never reported a positive term");
    }

    private static async Task<RaftPartitionView> AwaitTermAtLeastAsync(RaftManager manager, int partitionId, long minimumTerm, CancellationToken ct)
    {
        using CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(TimeSpan.FromSeconds(15));

        while (!cts.Token.IsCancellationRequested)
        {
            RaftPartitionView? view = await manager.GetPartitionViewAsync(partitionId, cts.Token).ConfigureAwait(false);
            if (view is not null && view.Term >= minimumTerm)
                return view;

            await Task.Delay(100, cts.Token).ConfigureAwait(false);
        }

        throw new TimeoutException($"partition {partitionId} never reached term {minimumTerm}");
    }

    private static async Task<RaftManager> BuildJoinedManager(int port)
    {
        RaftManager manager = BuildUnjoinedManager(port);

        using CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(15));
        await manager.JoinCluster(cts.Token).ConfigureAwait(false);

        return manager;
    }

    private static RaftManager BuildUnjoinedManager(int port, RaftTransportSecurityOptions? transport = null, ICommunication? communication = null)
    {
        RaftConfiguration configuration = new()
        {
            NodeName = $"unary-node-{port}",
            NodeId = port,
            Host = "localhost",
            Port = port,
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
            EnableCheckQuorum = false,
            TransportSecurity = transport ?? new RaftTransportSecurityOptions(),
        };

        return new RaftManager(
            configuration,
            new StaticDiscovery([]),
            new InMemoryWAL(NullLogger<IRaft>.Instance),
            communication ?? new InMemoryCommunication(),
            new HybridLogicalClock(),
            NullLogger<IRaft>.Instance);
    }

    private static global::System.Security.Cryptography.X509Certificates.X509Certificate2 CreateCertificate()
    {
        using global::System.Security.Cryptography.RSA key = global::System.Security.Cryptography.RSA.Create(2048);

        global::System.Security.Cryptography.X509Certificates.CertificateRequest request =
            new("CN=unary-node", key, global::System.Security.Cryptography.HashAlgorithmName.SHA256,
                global::System.Security.Cryptography.RSASignaturePadding.Pkcs1);

        return request.CreateSelfSigned(
            DateTimeOffset.UtcNow.AddMinutes(-5),
            DateTimeOffset.UtcNow.AddHours(1));
    }

    /// <summary>
    /// Minimal <see cref="ServerCallContext"/> exposing the members <c>ValidateAuth</c> reads:
    /// the method, the request headers, and (optionally) the ASP.NET Core context that carries
    /// the TLS state. Copied shape from the auth-gate tests: the context travels in
    /// <c>UserState</c>, which is where <c>GetHttpContext()</c> looks.
    /// </summary>
    private sealed class FakeServerCallContext(
        string method,
        Metadata headers,
        bool attachHttpContext,
        bool isHttps,
        HttpContext? httpContext = null) : ServerCallContext
    {
        private const string HttpContextUserStateKey = "__HttpContext";

        private readonly IDictionary<object, object> _userState = CreateUserState(attachHttpContext, isHttps, httpContext);

        protected override string MethodCore => method;
        protected override string HostCore => NodeEndpoint;
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
