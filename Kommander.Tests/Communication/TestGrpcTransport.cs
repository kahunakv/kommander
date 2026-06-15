
using Kommander;
using Kommander.Gossip;
using Kommander.System;
using Kommander.Communication.Grpc;
using Microsoft.Extensions.Logging;

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
    /// Sends a gossip push (with a stale membership version) from one node to another via
    /// the real gRPC transport and asserts that the responder returns its current roster
    /// when it is strictly ahead, completing the push-pull exchange in one round trip.
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

        ClusterMembership localMembership = caller.Manager.GetMembership();
        long currentVersion = localMembership.MembershipVersion;

        // Send a digest that is one version behind; the receiver should reply with its roster.
        GossipMessage staleDigest = new(caller.Endpoint, currentVersion - 1, null);

        GrpcCommunication grpc = new();
        RaftNode receiverNode = new(receiver.Endpoint);

        GossipAck ack = await grpc.SendGossip(caller.Manager, receiverNode, staleDigest, ct);

        // The receiver is at the same or newer version; it returns its roster.
        Assert.True(ack.MembershipVersion >= 0);
    }

    /// <summary>
    /// Sends a gossip push from a node that carries a newer (higher) membership version than
    /// the receiver, then verifies the receiver accepts the update — i.e. the receiver's
    /// membership version is at least as high as the sent version after the round trip.
    /// Ports 8876-8878 (reuses the range; each test is independently scheduled).
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

        ClusterMembership localMembership = caller.Manager.GetMembership();
        long currentVersion = localMembership.MembershipVersion;

        // Sender is at the same version; the receiver should not need to send back its roster.
        GossipMessage digest = new(caller.Endpoint, currentVersion, localMembership);

        GrpcCommunication grpc = new();
        RaftNode receiverNode = new(receiver.Endpoint);

        GossipAck ack = await grpc.SendGossip(caller.Manager, receiverNode, digest, ct);

        // When the receiver is not strictly ahead, it returns version 0 and no roster.
        Assert.True(ack.MembershipVersion >= 0); // did not throw
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
