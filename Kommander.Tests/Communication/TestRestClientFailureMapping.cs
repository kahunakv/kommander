using System.Net;
using Kommander;
using Kommander.Communication.Rest;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.Gossip;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using PingRequest = Kommander.Gossip.PingRequest;
using PingResponse = Kommander.Gossip.PingResponse;

namespace Kommander.Tests.Communication;

/// <summary>
/// Verifies how <see cref="RestCommunication"/> maps transport failures to protocol responses:
/// a dead peer, an HTTP 500, a non-JSON body and a timed-out call must each surface as that
/// RPC's documented failure value — never as an exception into the consensus machinery.
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
public sealed class TestRestClientFailureMapping
{
    private const int DeadPort = 9810;
    private const int MisbehavingPort = 9811;

    private static string DeadEndpoint => $"localhost:{DeadPort}";

    /// <summary>
    /// Every client RPC against an endpoint where nothing listens must return its failure shape:
    /// default/empty for the fire-and-forget consensus calls, explicit failure records for the
    /// membership calls, null for the lag query, and unreachable for the SWIM probes.
    /// </summary>
    [Fact]
    public async Task AllRpcsAgainstDeadEndpoint_ReturnDocumentedFailureValues()
    {
        using RaftManager caller = CreateCallerManager(httpTimeoutSeconds: 2);
        RaftNode dead = new(DeadEndpoint);
        RestCommunication rest = new();
        CancellationToken ct = TestContext.Current.CancellationToken;

        HandshakeResponse handshake = await rest.Handshake(caller, dead, new HandshakeRequest(1, 0, 0, DeadEndpoint));
        Assert.Equal(0, handshake.NodeId);
        Assert.Equal(0, handshake.MaxLogId);
        Assert.Equal(string.Empty, handshake.Endpoint);

        RequestVotesResponse requestVotes = await rest.RequestVotes(caller, dead, new RequestVotesRequest(0, 1, 0, 0, HLCTimestamp.Zero, DeadEndpoint, false));
        Assert.NotNull(requestVotes);

        VoteResponse vote = await rest.Vote(caller, dead, new VoteRequest(0, 1, 0, 0, HLCTimestamp.Zero, DeadEndpoint, false));
        Assert.NotNull(vote);

        AppendLogsResponse appendLogs = await rest.AppendLogs(caller, dead, new AppendLogsRequest(0, 1, HLCTimestamp.Zero, DeadEndpoint));
        Assert.NotNull(appendLogs);

        CompleteAppendLogsResponse completeAppendLogs = await rest.CompleteAppendLogs(
            caller, dead, new CompleteAppendLogsRequest(0, 1, HLCTimestamp.Zero, DeadEndpoint, RaftOperationStatus.Success, 0, 0, 0, 0, 0));
        Assert.NotNull(completeAppendLogs);

        BatchRequestsResponse batch = await rest.BatchRequests(caller, dead, new BatchRequestsRequest());
        Assert.NotNull(batch);

        LeaveResponse leave = await rest.SendLeave(caller, dead, new LeaveRequest(DeadEndpoint, 1), ct);
        Assert.False(leave.Success);
        Assert.Null(leave.LeaderHint);

        SetMemberRoleResponse setRole = await rest.SendSetMemberRole(
            caller, dead, new SetMemberRoleRequest(DeadEndpoint, 1, ClusterMemberRole.Leaving), ct);
        Assert.False(setRole.Success);
        Assert.Equal(RaftOperationStatus.Errored, setRole.Status);

        GossipAck gossip = await rest.SendGossip(caller, dead, new GossipMessage(DeadEndpoint, 1, null), ct);
        Assert.Equal(0, gossip.MembershipVersion);
        Assert.Null(gossip.Roster);

        PingResponse ping = await rest.SendPing(caller, dead, new PingRequest(DeadEndpoint), ct);
        Assert.False(ping.Alive);
        Assert.Equal(0, ping.Incarnation);

        Kommander.Gossip.PingReqResponse pingReq = await rest.SendPingReq(caller, dead, new Kommander.Gossip.PingReqRequest(DeadEndpoint, "other:1"), ct);
        Assert.False(pingReq.Reached);

        Assert.Null(await rest.GetRemoteFollowerLag(caller, dead, 0, "follower:1"));

        SnapshotResponse snapshot = await rest.SendInstallSnapshot(
            caller, dead, new SnapshotRequest { PartitionId = 0 }, ct);
        Assert.False(snapshot.Success);

        JoinResponse join = await rest.SendJoin(caller, dead, new JoinRequest(DeadEndpoint, 1));
        Assert.False(join.Success);
        Assert.Null(join.LeaderHint);
    }

    /// <summary>
    /// A peer that answers 500 must surface as the same failure values as an unreachable one —
    /// the callers cannot distinguish (or afford to throw on) either shape.
    /// </summary>
    [Fact]
    public async Task ServerError500_MapsToFailureValues()
    {
        await using MisbehavingServer server = await MisbehavingServer.StartAsync(
            MisbehavingPort,
            context =>
            {
                context.Response.StatusCode = StatusCodes.Status500InternalServerError;
                return Task.CompletedTask;
            },
            "/v1/raft/ping");

        using RaftManager caller = CreateCallerManager(httpTimeoutSeconds: 5);
        RestCommunication rest = new();
        RaftNode node = new($"localhost:{MisbehavingPort}");

        PingResponse response = await rest.SendPing(caller, node, new PingRequest(DeadEndpoint), TestContext.Current.CancellationToken);

        Assert.False(response.Alive);
        Assert.Equal(0, response.Incarnation);
    }

    /// <summary>
    /// A body that is not the expected JSON must fail the call rather than produce a half-built
    /// response object: the lag query returns null ("no record") exactly as for an unreachable
    /// peer.
    /// </summary>
    [Fact]
    public async Task NonJsonResponseBody_MapsToFailureValue()
    {
        await using MisbehavingServer server = await MisbehavingServer.StartAsync(
            MisbehavingPort,
            async context =>
            {
                context.Response.ContentType = "text/plain";
                await context.Response.WriteAsync("this is not json");
            },
            "/v1/raft/get-follower-lag");

        using RaftManager caller = CreateCallerManager(httpTimeoutSeconds: 5);
        RestCommunication rest = new();
        RaftNode node = new($"localhost:{MisbehavingPort}");

        long? lag = await rest.GetRemoteFollowerLag(caller, node, 0, "follower:1");

        Assert.Null(lag);
    }

    /// <summary>
    /// A call that exceeds the configured HTTP timeout must map to the failure value instead of
    /// hanging the caller until some outer watchdog fires.
    /// </summary>
    [Fact]
    public async Task SlowServerPastHttpTimeout_MapsToFailureValue()
    {
        await using MisbehavingServer server = await MisbehavingServer.StartAsync(
            MisbehavingPort,
            async context =>
            {
                await Task.Delay(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
                context.Response.ContentType = "application/json";
                await context.Response.WriteAsync("""{"alive":true,"incarnation":1}""");
            },
            "/v1/raft/ping");

        using RaftManager caller = CreateCallerManager(httpTimeoutSeconds: 1);
        RestCommunication rest = new();
        RaftNode node = new($"localhost:{MisbehavingPort}");

        PingResponse response = await rest.SendPing(caller, node, new PingRequest(DeadEndpoint), TestContext.Current.CancellationToken);

        Assert.False(response.Alive);
        Assert.Equal(0, response.Incarnation);
    }

    private static RaftManager CreateCallerManager(int httpTimeoutSeconds)
    {
        RaftConfiguration configuration = new()
        {
            NodeName = "node-x",
            NodeId = 1,
            Host = "localhost",
            Port = 9812,
            InitialPartitions = 1,
            HttpScheme = "http://",
            // Plain HTTP on purpose: these tests drive the client's failure mapping, not TLS.
            TransportSecurity = new() { RequireTls = false },
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
            EnableQuiescence = false
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
    /// A minimal HTTP host whose Kommander routes misbehave on purpose (wrong status, wrong
    /// content type, too slow), so the client's failure mapping can be driven without touching
    /// product server code.
    /// </summary>
    private sealed class MisbehavingServer : IAsyncDisposable
    {
        private readonly WebApplication _app;

        private MisbehavingServer(WebApplication app) => _app = app;

        public static async Task<MisbehavingServer> StartAsync(int port, RequestDelegate handler, string path)
        {
            WebApplicationBuilder builder = WebApplication.CreateBuilder();
            builder.Logging.ClearProviders();
            builder.WebHost.ConfigureKestrel(kestrel =>
            {
                kestrel.Listen(IPAddress.Loopback, port);
            });

            WebApplication app = builder.Build();
            app.MapPost(path, handler);

            await app.StartAsync(TestContext.Current.CancellationToken);

            return new MisbehavingServer(app);
        }

        public async ValueTask DisposeAsync()
        {
            using CancellationTokenSource cts = new(TimeSpan.FromSeconds(2));
            try { await _app.StopAsync(cts.Token).ConfigureAwait(false); } catch { /* best-effort */ }
            await _app.DisposeAsync().ConfigureAwait(false);
        }
    }
}
