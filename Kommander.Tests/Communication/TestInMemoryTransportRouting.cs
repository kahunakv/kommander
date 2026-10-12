using Kommander;
using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.Gossip;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging.Abstractions;
using PingRequest = Kommander.Gossip.PingRequest;
using PingResponse = Kommander.Gossip.PingResponse;

namespace Kommander.Tests.Communication;

/// <summary>
/// The in-memory transport's routing edges beyond the delivery-filter tests: calls that carry
/// their answer in the same call must behave like a real lost reply when only the answer
/// direction is cut, and calls to a recipient that was never registered must fail the same way
/// an unreachable peer would.
/// </summary>
public sealed class TestInMemoryTransportRouting
{
    private const int NodeAPort = 8610;
    private const int NodeBPort = 8611;

    [Fact]
    public async Task Handshake_HealthyLink_ReturnsTargetIdentity()
    {
        await using InMemoryPair pair = await InMemoryPair.CreateAsync();

        HandshakeResponse response = await pair.Communication.Handshake(
            pair.A, new RaftNode(pair.BEndpoint), new HandshakeRequest(NodeAPort, 0, 0, pair.AEndpoint));

        Assert.Equal(NodeBPort, response.NodeId);
        Assert.Equal(pair.BEndpoint, response.Endpoint);
    }

    /// <summary>
    /// Cutting only the answer direction still lets the target process the handshake, but the
    /// caller must see the same shape as a lost reply: the default, empty identity — never a
    /// half-populated answer fabricated by the transport.
    /// </summary>
    [Fact]
    public async Task Handshake_ReplyDirectionCut_CallerSeesDefaultIdentity()
    {
        await using InMemoryPair pair = await InMemoryPair.CreateAsync();

        pair.Communication.BlockLink(pair.BEndpoint, pair.AEndpoint);

        HandshakeResponse response = await pair.Communication.Handshake(
            pair.A, new RaftNode(pair.BEndpoint), new HandshakeRequest(NodeAPort, 0, 0, pair.AEndpoint));

        Assert.Equal(0, response.NodeId);
        Assert.Equal(0, response.MaxLogId);
        Assert.Equal(string.Empty, response.Endpoint);
    }

    [Fact]
    public async Task SendPing_HealthyLink_ReportsAlive()
    {
        await using InMemoryPair pair = await InMemoryPair.CreateAsync();

        PingResponse response = await pair.Communication.SendPing(
            pair.A, new RaftNode(pair.BEndpoint), new PingRequest(pair.AEndpoint), TestContext.Current.CancellationToken);

        Assert.True(response.Alive);
        Assert.True(response.Incarnation >= 0);
    }

    /// <summary>
    /// A lost reply must look exactly like an unreachable node to the SWIM caller, so the probe
    /// escalates to indirect checks as it would on a real network.
    /// </summary>
    [Fact]
    public async Task SendPing_ReplyDirectionCut_ReportsUnreachable()
    {
        await using InMemoryPair pair = await InMemoryPair.CreateAsync();

        pair.Communication.BlockLink(pair.BEndpoint, pair.AEndpoint);

        PingResponse response = await pair.Communication.SendPing(
            pair.A, new RaftNode(pair.BEndpoint), new PingRequest(pair.AEndpoint), TestContext.Current.CancellationToken);

        Assert.False(response.Alive);
        Assert.Equal(0, response.Incarnation);
    }

    /// <summary>
    /// A recipient that was never registered behaves like a dead peer: membership calls fail,
    /// the lag query answers null, snapshot installs report rejection, and a forwarded write
    /// answers null so the caller tries the next replica.
    /// </summary>
    [Fact]
    public async Task CallsToUnregisteredRecipient_ReturnFailureShapes()
    {
        await using InMemoryPair pair = await InMemoryPair.CreateAsync();

        InMemoryCommunication communication = pair.Communication;
        RaftNode ghost = new("ghost.nowhere:1");
        CancellationToken ct = TestContext.Current.CancellationToken;

        LeaveResponse leave = await communication.SendLeave(pair.A, ghost, new LeaveRequest("ghost.nowhere:1", 1), ct);
        Assert.False(leave.Success);
        Assert.Null(leave.LeaderHint);

        SetMemberRoleResponse setRole = await communication.SendSetMemberRole(
            pair.A, ghost, new SetMemberRoleRequest("ghost.nowhere:1", 1, ClusterMemberRole.Leaving), ct);
        Assert.False(setRole.Success);
        Assert.Equal(RaftOperationStatus.Errored, setRole.Status);

        JoinResponse join = await communication.SendJoin(pair.A, ghost, new JoinRequest("ghost.nowhere:1", 1));
        Assert.False(join.Success);

        Assert.Null(await communication.GetRemoteFollowerLag(pair.A, ghost, 0, "follower:1"));

        SnapshotResponse snapshot = await communication.SendInstallSnapshot(
            pair.A, ghost, new SnapshotRequest { PartitionId = 0 }, ct);
        Assert.False(snapshot.Success);

        GetReadIndexResponse readIndex = await communication.GetReadIndex(
            pair.A, ghost, new GetReadIndexRequest(0), ct);
        Assert.False(readIndex.Success);

        RaftReplicationResult? forwarded = await communication.ForwardReplicateLogs(
            pair.A, ghost, 0, "write", [[]], autoCommit: true, 0, 0, ct);
        Assert.Null(forwarded);
    }

    /// <summary>
    /// A batch item whose payload is missing dereferences null on the dispatch path; the per-item
    /// guard must contain it so the sibling items are still delivered and the call returns
    /// instead of throwing the NullReferenceException at the caller.
    /// </summary>
    [Fact]
    public async Task BatchRequests_MissingPayloadItem_DoesNotAbortTheBatch()
    {
        await using InMemoryPair pair = await InMemoryPair.CreateAsync();

        BatchRequestsRequest batch = new()
        {
            Requests =
            [
                // Typed item with no payload: throws on dispatch, must be contained.
                new BatchRequestsRequestItem { Type = BatchRequestsRequestType.Handshake },

                // The surviving item: a well-formed handshake for the system partition.
                new BatchRequestsRequestItem
                {
                    Type = BatchRequestsRequestType.Handshake,
                    Handshake = new HandshakeRequest(NodeAPort, 0, 0, pair.AEndpoint)
                }
            ]
        };

        BatchRequestsResponse response = await pair.Communication.BatchRequests(
            pair.A, new RaftNode(pair.BEndpoint), batch);

        Assert.NotNull(response);
    }

    /// <summary>
    /// Two managers wired through one <see cref="InMemoryCommunication"/>, with discovery
    /// refreshed so membership checks recognize the peer. The managers never join a cluster;
    /// their timers are stretched past the test lifetime so no election traffic interferes.
    /// </summary>
    private sealed class InMemoryPair : IAsyncDisposable
    {
        private readonly RaftManager _a;
        private readonly RaftManager _b;

        private InMemoryPair(InMemoryCommunication communication, RaftManager a, RaftManager b)
        {
            Communication = communication;
            _a = a;
            _b = b;
        }

        public InMemoryCommunication Communication { get; }
        public RaftManager A => _a;
        public RaftManager B => _b;
        public string AEndpoint => _a.GetLocalEndpoint();
        public string BEndpoint => _b.GetLocalEndpoint();

        public static async Task<InMemoryPair> CreateAsync()
        {
            InMemoryCommunication communication = new();

            RaftManager a = MakeManager(NodeAPort, communication, $"localhost:{NodeBPort}");
            RaftManager b = MakeManager(NodeBPort, communication, $"localhost:{NodeAPort}");

            communication.SetNodes(new Dictionary<string, IRaft>
            {
                [a.GetLocalEndpoint()] = a,
                [b.GetLocalEndpoint()] = b
            });

            await Task.WhenAll(a.UpdateNodes(), b.UpdateNodes());

            // The transport routes to a peer only when the cluster roster knows it, so the pair
            // must join — UpdateNodes alone leaves the roster empty and every call would fall to
            // the unknown-recipient default.
            await Task.WhenAll(
                a.JoinCluster(TestContext.Current.CancellationToken),
                b.JoinCluster(TestContext.Current.CancellationToken));

            return new InMemoryPair(communication, a, b);
        }

        public async ValueTask DisposeAsync()
        {
            Communication.HealAll();
            _a.Dispose();
            _b.Dispose();
            await ValueTask.CompletedTask;
        }

        private static RaftManager MakeManager(int port, InMemoryCommunication communication, string peer)
        {
            RaftConfiguration configuration = new()
            {
                NodeName = $"node-{port}",
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
                StartElectionTimeout = 500,
                EndElectionTimeout = 1_000,
                EnableQuiescence = false
            };

            return new RaftManager(
                configuration,
                new StaticDiscovery([new RaftNode(peer)]),
                new InMemoryWAL(NullLogger<IRaft>.Instance),
                communication,
                new HybridLogicalClock(),
                NullLogger<IRaft>.Instance);
        }
    }
}
