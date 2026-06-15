
using Kommander;
using Kommander.Data;
using Kommander.Gossip;
using Kommander.System;
using Kommander.Communication.Grpc;
using Microsoft.Extensions.Logging;
using PingRequest = Kommander.Gossip.PingRequest;
using PingResponse = Kommander.Gossip.PingResponse;
using PingReqRequest = Kommander.Gossip.PingReqRequest;
using PingReqResponse = Kommander.Gossip.PingReqResponse;

namespace Kommander.Tests.Communication;

/// <summary>
/// Integration tests that spin up real Kestrel gRPC servers to verify wire-level RPCs.
/// Shares the cluster-integration collection to prevent port collisions with other
/// timing-sensitive Raft tests.
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
public class TestGrpcTransport
{
    // Enable cleartext HTTP/2 once for the process; required for gRPC over loopback without TLS.
    static TestGrpcTransport()
    {
        AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);
    }

    private static ILoggerFactory CreateLoggerFactory() =>
        LoggerFactory.Create(b => b.SetMinimumLevel(LogLevel.Warning));

    /// <summary>
    /// A node leaving a 3-node cluster via the gRPC transport commits the removal before
    /// stopping; surviving nodes show a roster without the departed endpoint.
    /// Ports 8870-8872.
    /// </summary>
    [Fact]
    public async Task ThreeNodeCluster_LeaveViaGrpc_SurvivorsShowTwoVoterRoster()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        using ILoggerFactory logFactory = CreateLoggerFactory();

        await using GrpcClusterHarness harness = await GrpcClusterHarness.CreateAsync(
            [8870, 8871, 8872],
            logFactory,
            partitions: 1,
            ct: ct);

        await WaitForLeaderAsync(harness, partitionId: 0, TimeSpan.FromSeconds(15), ct);
        await Task.Delay(500, ct);

        // Pick any follower to leave.
        GrpcTestNode? leavingNode = null;
        foreach (GrpcTestNode n in harness.Nodes)
        {
            if (!await n.Manager.AmILeaderQuick(0))
            {
                leavingNode = n;
                break;
            }
        }
        Assert.NotNull(leavingNode);

        await leavingNode.Manager.LeaveCluster();

        // Give surviving nodes time to apply the removal entry.
        await Task.Delay(1500, ct);

        string leavingEndpoint = leavingNode.Endpoint;
        foreach (GrpcTestNode survivor in harness.Nodes.Where(n => n != leavingNode))
        {
            ClusterMembership membership = survivor.Manager.GetMembership();
            List<ClusterMember> voters = [.. membership.Members.Where(m => m.Role == ClusterMemberRole.Voter)];
            Assert.DoesNotContain(voters, v => v.Endpoint == leavingEndpoint);
        }
    }

    /// <summary>
    /// A follower node sends a <c>GetFollowerLag</c> gRPC call to the partition leader and
    /// receives a non-null committed index for another follower.  The call travels over the
    /// real loopback gRPC transport — it is not satisfied by the local in-memory accessor.
    /// Ports 8873-8875.
    /// </summary>
    [Fact]
    public async Task GetFollowerLag_WireCall_ReturnsNonNullCommittedIndex()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        using ILoggerFactory logFactory = CreateLoggerFactory();

        await using GrpcClusterHarness harness = await GrpcClusterHarness.CreateAsync(
            [8873, 8874, 8875],
            logFactory,
            partitions: 1,
            ct: ct);

        await WaitForLeaderAsync(harness, partitionId: 0, TimeSpan.FromSeconds(15), ct);
        await Task.Delay(300, ct);

        // Identify leader and two followers.
        GrpcTestNode? leader = null;
        foreach (GrpcTestNode n in harness.Nodes)
        {
            if (await n.Manager.AmILeaderQuick(0))
            {
                leader = n;
                break;
            }
        }
        Assert.NotNull(leader);

        List<GrpcTestNode> followers = [.. harness.Nodes.Where(n => n != leader)];
        Assert.True(followers.Count >= 2, "Expected at least 2 followers in a 3-node cluster");

        GrpcTestNode caller   = followers[0];  // sends the gRPC request
        GrpcTestNode target   = followers[1];  // whose lag is being queried

        // Call through the gRPC wire: caller → leader.GetFollowerLag(partition=0, target)
        // This exercises GrpcCommunication.GetRemoteFollowerLag → RaftService.GetFollowerLag.
        GrpcCommunication grpc = new();
        RaftNode leaderNode = new(leader.Endpoint);
        long? lag = await grpc.GetRemoteFollowerLag(caller.Manager, leaderNode, 0, target.Endpoint);

        Assert.NotNull(lag);
        Assert.True(lag >= 0, $"Expected committed index >= 0, got {lag}");
    }

    /// <summary>
    /// Sender at <c>currentVersion - 1</c> (strictly behind the receiver).
    /// Contract: receiver is strictly ahead → ACK carries the receiver's full roster and
    /// its membership version.  This is the push-pull exchange completing in one round trip.
    /// Ports 8876-8878.
    /// </summary>
    [Fact]
    public async Task GossipRpc_OlderSenderVersion_ResponderReturnsCurrentRoster()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        using ILoggerFactory logFactory = CreateLoggerFactory();

        await using GrpcClusterHarness harness = await GrpcClusterHarness.CreateAsync(
            [8876, 8877, 8878],
            logFactory,
            partitions: 1,
            ct: ct);

        await WaitForLeaderAsync(harness, partitionId: 0, TimeSpan.FromSeconds(15), ct);
        await Task.Delay(300, ct);

        GrpcTestNode caller   = harness.Nodes[0];
        GrpcTestNode receiver = harness.Nodes[1];

        // Read the receiver's version so we know what the ACK should echo back.
        ClusterMembership receiverMembership = receiver.Manager.GetMembership();
        long receiverVersion = receiverMembership.MembershipVersion;
        Assert.True(receiverVersion > 0, "Receiver must have a committed roster before testing gossip.");

        // Digest is one version behind the receiver — receiver is strictly ahead.
        GossipMessage staleDigest = new(caller.Endpoint, receiverVersion - 1, null);

        GrpcCommunication grpc = new();
        RaftNode receiverNode = new(receiver.Endpoint);

        GossipAck ack = await grpc.SendGossip(caller.Manager, receiverNode, staleDigest, ct);

        // Receiver is strictly ahead: ACK must include the current roster and its version.
        Assert.Equal(receiverVersion, ack.MembershipVersion);
        Assert.NotNull(ack.Roster);
        Assert.Equal(receiverVersion, ack.Roster.MembershipVersion);
    }

    /// <summary>
    /// Sender at <c>currentVersion</c> (same as receiver, not strictly behind).
    /// Contract: receiver is NOT strictly ahead → ACK carries the receiver's version but
    /// no roster (<see cref="GossipAck.Roster"/> is null).
    /// Ports 8879-8881.
    /// </summary>
    [Fact]
    public async Task GossipRpc_CurrentSenderVersion_EmptyAck()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        using ILoggerFactory logFactory = CreateLoggerFactory();

        await using GrpcClusterHarness harness = await GrpcClusterHarness.CreateAsync(
            [8879, 8880, 8881],
            logFactory,
            partitions: 1,
            ct: ct);

        await WaitForLeaderAsync(harness, partitionId: 0, TimeSpan.FromSeconds(15), ct);
        await Task.Delay(300, ct);

        GrpcTestNode caller   = harness.Nodes[0];
        GrpcTestNode receiver = harness.Nodes[1];

        ClusterMembership receiverMembership = receiver.Manager.GetMembership();
        long receiverVersion = receiverMembership.MembershipVersion;
        Assert.True(receiverVersion > 0, "Receiver must have a committed roster before testing gossip.");

        // Digest matches the receiver's version exactly — receiver is NOT strictly ahead.
        GossipMessage digest = new(caller.Endpoint, receiverVersion, receiverMembership);

        GrpcCommunication grpc = new();
        RaftNode receiverNode = new(receiver.Endpoint);

        GossipAck ack = await grpc.SendGossip(caller.Manager, receiverNode, digest, ct);

        // Receiver is not strictly ahead: ACK echoes the version but omits the roster.
        Assert.Equal(receiverVersion, ack.MembershipVersion);
        Assert.Null(ack.Roster);
    }

    /// <summary>
    /// A direct Ping RPC over gRPC returns <c>Alive=true</c> and a positive incarnation for a
    /// healthy node.  Validates <c>GrpcCommunication.SendPing</c> →
    /// <c>RaftService.Ping</c> → <c>RaftManager.ReceivePing</c>.
    /// Ports 8888-8890.
    /// </summary>
    [Fact]
    public async Task PingRpc_HealthyNode_ReturnsAliveTrue()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        using ILoggerFactory logFactory = CreateLoggerFactory();

        await using GrpcClusterHarness harness = await GrpcClusterHarness.CreateAsync(
            [8888, 8889, 8890],
            logFactory,
            partitions: 1,
            ct: ct);

        await WaitForLeaderAsync(harness, partitionId: 0, TimeSpan.FromSeconds(15), ct);

        GrpcTestNode caller   = harness.Nodes[0];
        GrpcTestNode receiver = harness.Nodes[1];

        GrpcCommunication grpc = new();
        RaftNode receiverNode = new(receiver.Endpoint);

        PingResponse resp = await grpc.SendPing(
            caller.Manager,
            receiverNode,
            new PingRequest(caller.Endpoint),
            ct);

        Assert.True(resp.Alive, "healthy node must respond Alive=true");
        Assert.True(resp.Incarnation >= 0, $"incarnation must be non-negative, got {resp.Incarnation}");
    }

    /// <summary>
    /// An indirect PingReq relay over gRPC: caller asks the relay node to probe the target;
    /// the relay confirms it can reach the target (<c>Reached=true</c>).
    /// Validates <c>GrpcCommunication.SendPingReq</c> → <c>RaftService.PingReq</c> →
    /// <c>RaftManager.ReceivePingReq</c> → outbound Ping to target.
    /// Ports 8891-8893.
    /// </summary>
    [Fact]
    public async Task PingReqRpc_HealthyTarget_ReturnsReachedTrue()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        using ILoggerFactory logFactory = CreateLoggerFactory();

        await using GrpcClusterHarness harness = await GrpcClusterHarness.CreateAsync(
            [8891, 8892, 8893],
            logFactory,
            partitions: 1,
            ct: ct);

        await WaitForLeaderAsync(harness, partitionId: 0, TimeSpan.FromSeconds(15), ct);

        GrpcTestNode caller  = harness.Nodes[0];
        GrpcTestNode relay   = harness.Nodes[1];
        GrpcTestNode target  = harness.Nodes[2];

        GrpcCommunication grpc = new();
        RaftNode relayNode = new(relay.Endpoint);

        PingReqResponse resp = await grpc.SendPingReq(
            caller.Manager,
            relayNode,
            new PingReqRequest(caller.Endpoint, target.Endpoint),
            ct);

        Assert.True(resp.Reached, "relay must be able to reach the healthy target");
    }

    private static async Task WaitForLeaderAsync(
        GrpcClusterHarness harness,
        int partitionId,
        TimeSpan timeout,
        CancellationToken ct)
    {
        using CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(timeout);

        while (!cts.Token.IsCancellationRequested)
        {
            foreach (GrpcTestNode n in harness.Nodes)
            {
                try
                {
                    if (await n.Manager.AmILeaderQuick(partitionId))
                        return;
                }
                catch { /* node may not be ready yet */ }
            }
            await Task.Delay(200, cts.Token).ConfigureAwait(false);
        }
        Assert.Fail($"No leader elected for partition {partitionId} within {timeout}");
    }
}
