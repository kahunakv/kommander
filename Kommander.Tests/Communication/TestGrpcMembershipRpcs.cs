using System.Net;
using System.Net.Sockets;
using Kommander;
using Kommander.Communication.Grpc;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.Gossip;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Grpc.Core;
using PingRequest = Kommander.Gossip.PingRequest;
using PingResponse = Kommander.Gossip.PingResponse;
using PingReqRequest = Kommander.Gossip.PingReqRequest;
using PingReqResponse = Kommander.Gossip.PingReqResponse;

namespace Kommander.Tests.Communication;

/// <summary>
/// Wire-level coverage for the gRPC membership and read-path RPCs that the regular election and
/// replication traffic never touches: join/leave/set-member-role redirect and idempotency
/// semantics, read-index confirmation, snapshot status queries, shared-secret authentication of
/// both the unary and the duplex-streaming paths, and the failure mapping of every unary call
/// against a dead peer.
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
public class TestGrpcMembershipRpcs
{
    static TestGrpcMembershipRpcs()
    {
        AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);
    }

    private static ILoggerFactory CreateLoggerFactory() =>
        LoggerFactory.Create(b => b.SetMinimumLevel(LogLevel.Warning));

    /// <summary>
    /// A join for an endpoint already committed in the roster is an idempotent success on the
    /// leader; the same request to a follower is refused with the leader as the hint.
    /// Ports 9830-9832.
    /// </summary>
    [Fact]
    public async Task Join_LeaderIsIdempotentForCommittedMember_FollowerRedirectsWithHint()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        using ILoggerFactory logFactory = CreateLoggerFactory();

        await using GrpcClusterHarness harness = await GrpcClusterHarness.CreateAsync(
            [9830, 9831, 9832], logFactory, partitions: 1, ct: ct);

        (GrpcTestNode leader, GrpcTestNode caller, GrpcTestNode otherFollower) = await IdentifyClusterRolesAsync(harness, ct);

        GrpcCommunication grpc = new();

        JoinResponse fromLeader = await grpc.SendJoin(
            caller.Manager, new RaftNode(leader.Endpoint), new JoinRequest(caller.Endpoint, caller.Port));

        Assert.True(fromLeader.Success, "joining an endpoint already in the roster must be an idempotent success");
        Assert.True(fromLeader.MembershipVersion > 0, "an idempotent join reports the roster version it matched");

        JoinResponse fromFollower = await grpc.SendJoin(
            caller.Manager, new RaftNode(otherFollower.Endpoint), new JoinRequest(caller.Endpoint, caller.Port));

        Assert.False(fromFollower.Success);
        Assert.Equal(leader.Endpoint, fromFollower.LeaderHint);
    }

    /// <summary>
    /// A leave request landing on a follower is redirected: no success, the leader's endpoint as
    /// the hint, and not terminal (the departing node is expected to retry).
    /// Ports 9833-9835.
    /// </summary>
    [Fact]
    public async Task Leave_FollowerRedirectsWithLeaderHint()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        using ILoggerFactory logFactory = CreateLoggerFactory();

        await using GrpcClusterHarness harness = await GrpcClusterHarness.CreateAsync(
            [9833, 9834, 9835], logFactory, partitions: 1, ct: ct);

        (GrpcTestNode leader, GrpcTestNode caller, GrpcTestNode otherFollower) = await IdentifyClusterRolesAsync(harness, ct);

        GrpcCommunication grpc = new();

        LeaveResponse response = await grpc.SendLeave(
            caller.Manager, new RaftNode(otherFollower.Endpoint), new LeaveRequest("ghost.nowhere:1", -1), ct);

        Assert.False(response.Success);
        Assert.Equal(leader.Endpoint, response.LeaderHint);
        Assert.False(response.Terminal, "a redirect is retryable, never terminal");
    }

    /// <summary>
    /// A role-transition request landing on a follower fails with the leader hint and the
    /// not-leader verdict, so a decommission drain retries at the node that owns the roster.
    /// Ports 9836-9838.
    /// </summary>
    [Fact]
    public async Task SetMemberRole_FollowerRedirectsWithLeaderHintAndNotLeaderStatus()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        using ILoggerFactory logFactory = CreateLoggerFactory();

        await using GrpcClusterHarness harness = await GrpcClusterHarness.CreateAsync(
            [9836, 9837, 9838], logFactory, partitions: 1, ct: ct);

        (GrpcTestNode leader, GrpcTestNode caller, GrpcTestNode otherFollower) = await IdentifyClusterRolesAsync(harness, ct);

        GrpcCommunication grpc = new();

        SetMemberRoleResponse response = await grpc.SendSetMemberRole(
            caller.Manager,
            new RaftNode(otherFollower.Endpoint),
            new SetMemberRoleRequest("ghost.nowhere:1", -1, ClusterMemberRole.Leaving),
            ct);

        Assert.False(response.Success);
        Assert.Equal(leader.Endpoint, response.LeaderHint);
        Assert.Equal(RaftOperationStatus.NodeIsNotLeader, response.Status);
    }

    /// <summary>
    /// The read-index RPC answered by the partition leader succeeds and reports a real commit
    /// frontier; the same RPC answered by a follower fails closed, because a node that cannot run
    /// the quorum round has no read index to give.
    /// Ports 9839-9841.
    /// </summary>
    [Fact]
    public async Task GetReadIndex_LeaderConfirms_FollowerFailsClosed()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        using ILoggerFactory logFactory = CreateLoggerFactory();

        await using GrpcClusterHarness harness = await GrpcClusterHarness.CreateAsync(
            [9839, 9840, 9841], logFactory, partitions: 1, ct: ct);

        (GrpcTestNode leader, GrpcTestNode caller, GrpcTestNode otherFollower) = await IdentifyClusterRolesAsync(harness, ct);

        GrpcCommunication grpc = new();
        RaftNode leaderNode = new(leader.Endpoint);

        // The confirmation round needs quorum heartbeats; on a freshly elected leader the first
        // attempts can still race the first heartbeat round, so retry briefly rather than flake.
        GetReadIndexResponse confirmed = new(false);
        DateTime deadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);
        while (DateTime.UtcNow < deadline && !confirmed.Success)
        {
            confirmed = await grpc.GetReadIndex(caller.Manager, leaderNode, new GetReadIndexRequest(0), ct);
            if (!confirmed.Success)
                await Task.Delay(200, ct);
        }

        Assert.True(confirmed.Success, "the partition leader must be able to confirm a read index");
        Assert.True(confirmed.ReadIndex >= 0, $"read index must be a real frontier, got {confirmed.ReadIndex}");

        GetReadIndexResponse fromFollower = await grpc.GetReadIndex(
            caller.Manager, new RaftNode(otherFollower.Endpoint), new GetReadIndexRequest(0), ct);

        Assert.False(fromFollower.Success, "a follower cannot answer a read-index confirmation");
        Assert.Equal(-1, fromFollower.ReadIndex);
    }

    /// <summary>
    /// A snapshot status query must be answered with the typed "no install" outcome when the
    /// partition never received one — the sender's polling loop distinguishes that from a
    /// transfer failure by the outcome code, so it must survive the protobuf round trip.
    /// Ports 9842-9844.
    /// </summary>
    [Fact]
    public async Task InstallSnapshot_StatusQueryWithoutInstall_AnswersNoInstall()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        using ILoggerFactory logFactory = CreateLoggerFactory();

        await using GrpcClusterHarness harness = await GrpcClusterHarness.CreateAsync(
            [9842, 9843, 9844], logFactory, partitions: 1, ct: ct);

        (_, GrpcTestNode caller, GrpcTestNode receiver) = await IdentifyClusterRolesAsync(harness, ct);

        GrpcCommunication grpc = new();

        SnapshotResponse response = await grpc.SendInstallSnapshot(
            caller.Manager,
            new RaftNode(receiver.Endpoint),
            new SnapshotRequest
            {
                StatusQuery = true,
                ChunkIndex = -1,
                PartitionId = 0
            },
            ct);

        Assert.Equal(SnapshotInstallOutcome.NoInstall, response.Outcome);
        Assert.False(response.Success, "NoInstall must read as a non-success for the chunk loop");
    }

    /// <summary>
    /// Every unary RPC against an endpoint where nothing listens must return its documented
    /// failure value rather than throw: membership calls fail explicitly, the lag query answers
    /// null, the SWIM probes answer unreachable, and the snapshot call reports rejection.
    /// </summary>
    [Fact]
    public async Task AllUnaryRpcsAgainstDeadEndpoint_ReturnDocumentedFailureValues()
    {
        int deadPort = ReserveRefusedPort();
        string deadEndpoint = $"localhost:{deadPort}";

        using RaftManager caller = CreateStandaloneCaller(deadPort);
        RaftNode dead = new(deadEndpoint);
        GrpcCommunication grpc = new();
        CancellationToken ct = TestContext.Current.CancellationToken;

        LeaveResponse leave = await grpc.SendLeave(caller, dead, new LeaveRequest(deadEndpoint, 1), ct);
        Assert.False(leave.Success);

        SetMemberRoleResponse setRole = await grpc.SendSetMemberRole(
            caller, dead, new SetMemberRoleRequest(deadEndpoint, 1, ClusterMemberRole.Leaving), ct);
        Assert.False(setRole.Success);
        Assert.Equal(RaftOperationStatus.Errored, setRole.Status);

        GossipAck gossip = await grpc.SendGossip(caller, dead, new GossipMessage(deadEndpoint, 1, null), ct);
        Assert.Equal(0, gossip.MembershipVersion);
        Assert.Null(gossip.Roster);

        PingResponse ping = await grpc.SendPing(caller, dead, new PingRequest(deadEndpoint), ct);
        Assert.False(ping.Alive);
        Assert.Equal(0, ping.Incarnation);

        PingReqResponse pingReq = await grpc.SendPingReq(caller, dead, new PingReqRequest(deadEndpoint, "other:1"), ct);
        Assert.False(pingReq.Reached);

        Assert.Null(await grpc.GetRemoteFollowerLag(caller, dead, 0, "follower:1"));

        GetReadIndexResponse readIndex = await grpc.GetReadIndex(caller, dead, new GetReadIndexRequest(0), ct);
        Assert.False(readIndex.Success);
        Assert.Equal(-1, readIndex.ReadIndex);

        SnapshotResponse snapshot = await grpc.SendInstallSnapshot(
            caller, dead, new SnapshotRequest { PartitionId = 0 }, ct);
        Assert.False(snapshot.Success);
        Assert.Equal(SnapshotInstallOutcome.Rejected, snapshot.Outcome);

        JoinResponse join = await grpc.SendJoin(caller, dead, new JoinRequest(deadEndpoint, 1));
        Assert.False(join.Success);
        Assert.Null(join.LeaderHint);
    }

    /// <summary>
    /// A shared-secret cluster over loopback: the unary calls carry a signature the peer accepts,
    /// the duplex BatchRequests stream (which authenticates once at establishment, with a fresh
    /// nonce per slot) carries the election traffic — a leader is elected and a direct probe is
    /// answered — while a caller holding a different secret is refused.
    /// Ports 9845-9847.
    /// </summary>
    [Fact]
    public async Task SharedSecretCluster_UnaryAndStreamingPathsAuthenticate()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        using ILoggerFactory logFactory = CreateLoggerFactory();

        const string secret = "grpc-shared-secret";

        int[] ports = [9845, 9846, 9847];
        List<RaftNode> peers = [.. ports.Select(p => new RaftNode($"localhost:{p}"))];

        List<GrpcTestNode> nodes = [];
        try
        {
            foreach (int port in ports)
            {
                GrpcTestNode node = GrpcTestNode.Create(
                    port,
                    peers,
                    logFactory,
                    partitions: 1,
                    configure: configuration =>
                    {
                        configuration.TransportSecurity = new RaftTransportSecurityOptions
                        {
                            NodeAuthenticationMode = RaftNodeAuthenticationMode.SharedSecret,
                            SharedSecret = secret,
                            RequireTls = false
                        };
                    });

                nodes.Add(node);
            }

            foreach (GrpcTestNode node in nodes)
                await node.StartAsync(ct);

            await Task.WhenAll(nodes.Select(n => n.Manager.JoinCluster(ct)));

            // Election traffic rides the duplex BatchRequests stream, which is authenticated at
            // establishment: no leader means the signed stream metadata was refused.
            GrpcTestNode? leader = null;
            using CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            cts.CancelAfter(TimeSpan.FromSeconds(15));
            while (leader is null && !cts.Token.IsCancellationRequested)
            {
                foreach (GrpcTestNode node in nodes)
                {
                    if (await node.Manager.AmILeaderQuick(0))
                    {
                        leader = node;
                        break;
                    }
                }

                if (leader is null)
                    await Task.Delay(200, cts.Token);
            }

            Assert.NotNull(leader);

            GrpcCommunication grpc = new();
            GrpcTestNode caller = nodes.First(n => n != leader);

            PingResponse accepted = await grpc.SendPing(
                caller.Manager, new RaftNode(leader.Endpoint), new PingRequest(caller.Endpoint), ct);
            Assert.True(accepted.Alive, "a unary call signed with the cluster secret must be accepted");

            using RaftManager outsider = CreateStandaloneCaller(9848, secret: "not-the-cluster-secret");

            PingResponse refused = await grpc.SendPing(
                outsider, new RaftNode(leader.Endpoint), new PingRequest(caller.Endpoint), ct);
            Assert.False(refused.Alive, "a unary call signed with a different secret must be refused");
            Assert.Equal(0, refused.Incarnation);
        }
        finally
        {
            foreach (GrpcTestNode node in nodes)
                await node.DisposeAsync();
        }
    }

    /// <summary>
    /// The per-chunk install-snapshot deadline is derived from the configuration's chunk ack
    /// timeout, so a wedged receiver fails the call in seconds instead of holding it open.
    /// </summary>
    [Fact]
    public void InstallSnapshotCallOptions_CarryDeadlineDerivedFromChunkAckTimeout()
    {
        RaftConfiguration configuration = new()
        {
            SnapshotChunkAckTimeout = TimeSpan.FromSeconds(23)
        };

        DateTime before = DateTime.UtcNow;
        CallOptions options = GrpcCommunication.BuildInstallSnapshotCallOptions(
            configuration, new Metadata(), CancellationToken.None);
        DateTime after = DateTime.UtcNow;

        DateTime earliest = before + TimeSpan.FromSeconds(23);
        DateTime latest = after + TimeSpan.FromSeconds(23);

        Assert.NotNull(options.Deadline);
        Assert.InRange(options.Deadline!.Value, earliest, latest);
    }

    private static async Task<(GrpcTestNode Leader, GrpcTestNode Caller, GrpcTestNode Other)> IdentifyClusterRolesAsync(
        GrpcClusterHarness harness, CancellationToken ct)
    {
        GrpcTestNode? leader = null;
        using CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(TimeSpan.FromSeconds(15));

        while (leader is null && !cts.Token.IsCancellationRequested)
        {
            foreach (GrpcTestNode node in harness.Nodes)
            {
                try
                {
                    if (await node.Manager.AmILeaderQuick(0))
                    {
                        leader = node;
                        break;
                    }
                }
                catch { /* node may not be ready yet */ }
            }

            if (leader is null)
                await Task.Delay(200, cts.Token).ConfigureAwait(false);
        }

        Assert.NotNull(leader);

        // Let the followers settle on the leader so redirect hints are populated.
        await Task.Delay(300, ct);

        List<GrpcTestNode> followers = [.. harness.Nodes.Where(n => n != leader)];
        Assert.True(followers.Count >= 2, "expected a leader plus two followers");
        return (leader, followers[0], followers[1]);
    }

    private static RaftManager CreateStandaloneCaller(int nodeId, string? secret = null)
    {
        RaftTransportSecurityOptions security = new();

        if (secret is not null)
        {
            security.NodeAuthenticationMode = RaftNodeAuthenticationMode.SharedSecret;
            security.SharedSecret = secret;
            security.RequireTls = false;
        }

        RaftConfiguration configuration = new()
        {
            NodeName = $"node-{nodeId}",
            NodeId = nodeId,
            Host = "localhost",
            Port = nodeId,
            InitialPartitions = 1,
            GrpcScheme = "http://",
            HeartbeatInterval = TimeSpan.FromMilliseconds(150),
            VotingTimeout = TimeSpan.FromMilliseconds(500),
            StartElectionTimeout = 600,
            EnableQuiescence = false,
            EndElectionTimeout = 900,
            TimerInitialDelay = TimeSpan.FromMilliseconds(500),
            PingInterval = TimeSpan.Zero,
            TransportSecurity = security
        };

        return new RaftManager(
            configuration,
            new StaticDiscovery([]),
            new InMemoryWAL(NullLogger<IRaft>.Instance),
            new GrpcCommunication(),
            new HybridLogicalClock(),
            NullLogger<IRaft>.Instance);
    }

    /// <summary>
    /// Reserves a loopback port and releases it again, so nothing is listening: connections to it
    /// are refused immediately, which is the failure shape the dead-peer tests rely on.
    /// </summary>
    private static int ReserveRefusedPort()
    {
        TcpListener listener = new(IPAddress.Loopback, 0);
        listener.Start();
        int port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }
}
