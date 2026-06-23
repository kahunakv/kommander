
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

        await leavingNode.Manager.LeaveCluster(cancellationToken: ct);

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
    /// A permanently-stopped node (simulating a hard partition) transitions to
    /// Dead on the surviving nodes and is evicted from the committed roster by the P0 leader
    /// within <c>SuspicionTimeout + DeadMemberEvictionGrace</c>.
    ///
    /// This is the end-to-end SWIM pipeline over the real gRPC transport:
    /// timer-driven <c>PingAsync</c> → failed gRPC probe → Suspect → Dead → <c>RemoveMember</c>.
    /// Ports 8903-8905.
    /// </summary>
    [Fact]
    public async Task Swim_StoppedNode_IsEvictedFromRoster_Grpc()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        using ILoggerFactory logFactory = CreateLoggerFactory();

        // Fast SWIM: worst-case time from stop → eviction is
        //   1 probe interval + SuspicionTimeout + DeadMemberEvictionGrace
        //   = 200 ms + 1.5 s + 2 s ≈ 3.7 s.  Test timeout is 30 s.
        TimeSpan pingInterval            = TimeSpan.FromMilliseconds(200);
        TimeSpan suspicionTimeout        = TimeSpan.FromSeconds(1.5);
        TimeSpan deadMemberEvictionGrace = TimeSpan.FromSeconds(2);

        await using GrpcClusterHarness harness = await GrpcClusterHarness.CreateWithSwimAsync(
            [8903, 8904, 8905],
            logFactory,
            pingInterval,
            suspicionTimeout,
            deadMemberEvictionGrace,
            partitions: 1,
            ct: ct);

        await WaitForLeaderAsync(harness, partitionId: 0, TimeSpan.FromSeconds(15), ct);
        await Task.Delay(500, ct);   // let roster stabilize

        // Identify the node to stop (prefer a follower so leader election doesn't interfere).
        GrpcTestNode? toStop = null;
        foreach (GrpcTestNode n in harness.Nodes)
        {
            if (!await n.Manager.AmILeaderQuick(0))
            {
                toStop = n;
                break;
            }
        }
        Assert.NotNull(toStop);
        string stoppedEndpoint = toStop.Endpoint;

        long rosterBefore = harness.Nodes[0].Manager.GetMembership().MembershipVersion;

        // Stop the node: Kestrel stops accepting connections → gRPC probes fail.
        await toStop.DisposeAsync();

        // Wait for the two surviving nodes to evict the stopped node.
        // The timer-driven PingAsync fires every pingInterval; Suspect → Dead after
        // SuspicionTimeout; Dead → RemoveMember after DeadMemberEvictionGrace.
        GrpcTestNode survivor = harness.Nodes.First(n => n != toStop);
        await WaitForCondition(
            () =>
            {
                ClusterMembership m = survivor.Manager.GetMembership();
                return m.MembershipVersion > rosterBefore
                    && !m.Members.Any(mem => mem.Endpoint == stoppedEndpoint);
            },
            TimeSpan.FromSeconds(20),
            ct);

        ClusterMembership roster = survivor.Manager.GetMembership();
        Assert.DoesNotContain(roster.Members, m => m.Endpoint == stoppedEndpoint);
        Assert.Equal(2, roster.Members.Count);
    }

    /// <summary>
    /// Healthy nodes that respond to every probe are never evicted even after
    /// <c>SuspicionTimeout + DeadMemberEvictionGrace</c> has elapsed.
    ///
    /// Verifies that the gRPC <c>Ping</c> RPC correctly returns <c>Alive=true</c> for
    /// reachable peers, so the detector does not falsely evict any member.
    /// Ports 8906-8908.
    /// </summary>
    [Fact]
    public async Task Swim_HealthyNodes_AreNeverEvictedByDetector_Grpc()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        using ILoggerFactory logFactory = CreateLoggerFactory();

        TimeSpan pingInterval            = TimeSpan.FromMilliseconds(200);
        TimeSpan suspicionTimeout        = TimeSpan.FromSeconds(1);
        TimeSpan deadMemberEvictionGrace = TimeSpan.FromSeconds(1);

        await using GrpcClusterHarness harness = await GrpcClusterHarness.CreateWithSwimAsync(
            [8906, 8907, 8908],
            logFactory,
            pingInterval,
            suspicionTimeout,
            deadMemberEvictionGrace,
            partitions: 1,
            ct: ct);

        await WaitForLeaderAsync(harness, partitionId: 0, TimeSpan.FromSeconds(15), ct);
        await Task.Delay(500, ct);

        long rosterBefore = harness.Nodes[0].Manager.GetMembership().MembershipVersion;
        int membersBefore = harness.Nodes[0].Manager.GetMembership().Members.Count;

        // Let the SWIM timer run for longer than SuspicionTimeout + DeadMemberEvictionGrace.
        // All nodes are healthy and answering probes; none should be evicted.
        await Task.Delay(suspicionTimeout + deadMemberEvictionGrace + TimeSpan.FromSeconds(1), ct);

        foreach (GrpcTestNode n in harness.Nodes)
        {
            ClusterMembership roster = n.Manager.GetMembership();
            Assert.Equal(rosterBefore, roster.MembershipVersion);
            Assert.Equal(membersBefore, roster.Members.Count);
        }
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

    private static async Task WaitForCondition(Func<bool> condition, TimeSpan timeout, CancellationToken ct)
    {
        using CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(timeout);

        while (!cts.Token.IsCancellationRequested)
        {
            if (condition())
                return;
            await Task.Delay(200, cts.Token).ConfigureAwait(false);
        }
        Assert.Fail("Condition not met within the timeout.");
    }
}
