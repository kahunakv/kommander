
using Kommander;
using Kommander.Communication.Rest;
using Kommander.System;
using Kommander.Gossip;
using Microsoft.Extensions.Logging;
using PingRequest = Kommander.Gossip.PingRequest;
using PingResponse = Kommander.Gossip.PingResponse;
using PingReqRequest = Kommander.Gossip.PingReqRequest;
using PingReqResponse = Kommander.Gossip.PingReqResponse;

namespace Kommander.Tests.Communication;

/// <summary>
/// Integration tests that spin up real Kestrel HTTP/1.1 servers to verify wire-level REST RPCs.
/// Mirrors <see cref="TestGrpcTransport"/> for the REST transport.
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
public class TestRestTransport
{
    private static ILoggerFactory CreateLoggerFactory() =>
        LoggerFactory.Create(b => b.SetMinimumLevel(LogLevel.Warning));

    /// <summary>
    /// Sender at <c>currentVersion - 1</c> (strictly behind the receiver).
    /// The gossip route handler must deserialise the <see cref="Data.GossipRequest"/>, call
    /// <see cref="RaftManager.ReceiveGossip"/>, and return the receiver's roster in the
    /// <see cref="Data.GossipResponse"/> when strictly ahead.
    /// Ports 8882-8884.
    /// </summary>
    [Fact]
    public async Task GossipRpc_OlderSenderVersion_ResponderReturnsCurrentRoster_Rest()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        using ILoggerFactory logFactory = CreateLoggerFactory();

        await using RestClusterHarness harness = await RestClusterHarness.CreateAsync(
            [8882, 8883, 8884],
            logFactory,
            partitions: 1,
            ct: ct);

        await WaitForLeaderAsync(harness, partitionId: 0, TimeSpan.FromSeconds(15), ct);
        await Task.Delay(300, ct);

        RestTestNode caller   = harness.Nodes[0];
        RestTestNode receiver = harness.Nodes[1];

        ClusterMembership receiverMembership = receiver.Manager.GetMembership();
        long receiverVersion = receiverMembership.MembershipVersion;
        Assert.True(receiverVersion > 0, "Receiver must have a committed roster before testing gossip.");

        // Digest is one version behind the receiver — receiver is strictly ahead.
        GossipMessage staleDigest = new(caller.Endpoint, receiverVersion - 1, null);

        RestCommunication rest = new();
        RaftNode receiverNode = new(receiver.Endpoint);

        GossipAck ack = await rest.SendGossip(caller.Manager, receiverNode, staleDigest, ct);

        // Receiver is strictly ahead: ACK must include the current roster and its version.
        Assert.Equal(receiverVersion, ack.MembershipVersion);
        Assert.NotNull(ack.Roster);
        Assert.Equal(receiverVersion, ack.Roster.MembershipVersion);
    }

    /// <summary>
    /// Sender at <c>currentVersion</c> (same as receiver, not strictly behind).
    /// The ACK must carry the receiver's version but no roster.
    /// Ports 8885-8887.
    /// </summary>
    [Fact]
    public async Task GossipRpc_CurrentSenderVersion_EmptyAck_Rest()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        using ILoggerFactory logFactory = CreateLoggerFactory();

        await using RestClusterHarness harness = await RestClusterHarness.CreateAsync(
            [8885, 8886, 8887],
            logFactory,
            partitions: 1,
            ct: ct);

        await WaitForLeaderAsync(harness, partitionId: 0, TimeSpan.FromSeconds(15), ct);
        await Task.Delay(300, ct);

        RestTestNode caller   = harness.Nodes[0];
        RestTestNode receiver = harness.Nodes[1];

        ClusterMembership receiverMembership = receiver.Manager.GetMembership();
        long receiverVersion = receiverMembership.MembershipVersion;
        Assert.True(receiverVersion > 0, "Receiver must have a committed roster before testing gossip.");

        // Digest matches the receiver's version exactly — receiver is NOT strictly ahead.
        GossipMessage digest = new(caller.Endpoint, receiverVersion, receiverMembership);

        RestCommunication rest = new();
        RaftNode receiverNode = new(receiver.Endpoint);

        GossipAck ack = await rest.SendGossip(caller.Manager, receiverNode, digest, ct);

        // Receiver is not strictly ahead: ACK echoes the version but omits the roster.
        Assert.Equal(receiverVersion, ack.MembershipVersion);
        Assert.Null(ack.Roster);
    }

    /// <summary>
    /// A direct Ping REST RPC to a healthy node returns <c>Alive=true</c>.
    /// Validates <c>RestCommunication.SendPing</c> → <c>POST /v1/raft/ping</c> →
    /// <c>RaftManager.ReceivePing</c>.
    /// Ports 8894-8896.
    /// </summary>
    [Fact]
    public async Task PingRpc_HealthyNode_ReturnsAliveTrue_Rest()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        using ILoggerFactory logFactory = CreateLoggerFactory();

        await using RestClusterHarness harness = await RestClusterHarness.CreateAsync(
            [8894, 8895, 8896],
            logFactory,
            partitions: 1,
            ct: ct);

        await WaitForLeaderAsync(harness, partitionId: 0, TimeSpan.FromSeconds(15), ct);

        RestTestNode caller   = harness.Nodes[0];
        RestTestNode receiver = harness.Nodes[1];

        RestCommunication rest = new();
        RaftNode receiverNode = new(receiver.Endpoint);

        PingResponse resp = await rest.SendPing(
            caller.Manager,
            receiverNode,
            new PingRequest(caller.Endpoint),
            ct);

        Assert.True(resp.Alive, "healthy node must respond Alive=true");
        Assert.True(resp.Incarnation >= 0, $"incarnation must be non-negative, got {resp.Incarnation}");
    }

    /// <summary>
    /// An indirect PingReq relay over REST: caller asks the relay to probe the target;
    /// the relay confirms it can reach the target (<c>Reached=true</c>).
    /// Validates <c>RestCommunication.SendPingReq</c> → <c>POST /v1/raft/ping-req</c> →
    /// <c>RaftManager.ReceivePingReq</c>.
    /// Ports 8900-8902.
    /// </summary>
    [Fact]
    public async Task PingReqRpc_HealthyTarget_ReturnsReachedTrue_Rest()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        using ILoggerFactory logFactory = CreateLoggerFactory();

        await using RestClusterHarness harness = await RestClusterHarness.CreateAsync(
            [8900, 8901, 8902],
            logFactory,
            partitions: 1,
            ct: ct);

        await WaitForLeaderAsync(harness, partitionId: 0, TimeSpan.FromSeconds(15), ct);

        RestTestNode caller  = harness.Nodes[0];
        RestTestNode relay   = harness.Nodes[1];
        RestTestNode target  = harness.Nodes[2];

        RestCommunication rest = new();
        RaftNode relayNode = new(relay.Endpoint);

        PingReqResponse resp = await rest.SendPingReq(
            caller.Manager,
            relayNode,
            new PingReqRequest(caller.Endpoint, target.Endpoint),
            ct);

        Assert.True(resp.Reached, "relay must be able to reach the healthy target");
    }

    private static async Task WaitForLeaderAsync(
        RestClusterHarness harness,
        int partitionId,
        TimeSpan timeout,
        CancellationToken ct)
    {
        using CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(timeout);

        while (!cts.Token.IsCancellationRequested)
        {
            foreach (RestTestNode n in harness.Nodes)
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
