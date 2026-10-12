using System.Net;
using Kommander;
using Kommander.Communication.Rest;
using Kommander.Data;
using Kommander.System;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kommander.Tests.Communication;

/// <summary>
/// Drives the REST routes that carry membership, lag reporting and snapshot status over a real
/// Kestrel cluster: each test issues the client RPC and asserts the answer the route produced,
/// rather than the in-manager shortcut.
/// </summary>
/// <remarks>
/// These routes sit behind <c>MapRestRaftRoutes</c> and answer through
/// <see cref="RaftManager"/> receive methods; a wiring mistake (wrong route, wrong body type,
/// a response the client cannot deserialize) only shows up when the full
/// serialize → route → handler → deserialize round trip runs, which is what the harness gives us.
/// </remarks>
[Collection(ClusterIntegrationCollection.Name)]
public class TestRestRouteSurface
{
    private static ILoggerFactory CreateLoggerFactory() =>
        LoggerFactory.Create(b => b.SetMinimumLevel(LogLevel.Warning));

    /// <summary>
    /// A follower-lag query for a peer the leader has actually replicated to returns the committed
    /// index it recorded; the answer must survive the JSON round trip with its value intact.
    /// Ports 9760-9762.
    /// </summary>
    [Fact]
    public async Task GetFollowerLag_KnownFollower_ReturnsCommittedIndex()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        using ILoggerFactory logFactory = CreateLoggerFactory();

        await using RestClusterHarness harness = await RestClusterHarness.CreateAsync(
            [9760, 9761, 9762], logFactory, partitions: 1, ct: ct);

        (RestTestNode leader, RestTestNode caller, RestTestNode watched) = await IdentifyClusterRolesAsync(harness, ct);

        RestCommunication rest = new();
        RaftNode leaderNode = new(leader.Endpoint);

        long? lag = await rest.GetRemoteFollowerLag(caller.Manager, leaderNode, 0, watched.Endpoint);

        Assert.NotNull(lag);
        Assert.True(lag >= 0, $"committed index must be non-negative, got {lag}");
    }

    /// <summary>
    /// A follower-lag query for an endpoint the leader has never heard of must come back as
    /// "no record" — null on the client — and not as a fabricated zero, which would make an
    /// unknown replica look caught-up.
    /// Ports 9763-9765.
    /// </summary>
    [Fact]
    public async Task GetFollowerLag_UnknownFollower_ReturnsNull()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        using ILoggerFactory logFactory = CreateLoggerFactory();

        await using RestClusterHarness harness = await RestClusterHarness.CreateAsync(
            [9763, 9764, 9765], logFactory, partitions: 1, ct: ct);

        (RestTestNode leader, RestTestNode caller, _) = await IdentifyClusterRolesAsync(harness, ct);

        RestCommunication rest = new();
        RaftNode leaderNode = new(leader.Endpoint);

        long? lag = await rest.GetRemoteFollowerLag(caller.Manager, leaderNode, 0, "ghost.nowhere:1");

        Assert.Null(lag);
    }

    /// <summary>
    /// A leave request that lands on a follower is not committed there; the follower answers with
    /// the leader hint so the departing node can retry against the node that owns the roster.
    /// Ports 9769-9771.
    /// </summary>
    [Fact]
    public async Task Leave_FollowerRedirectsWithLeaderHint()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        using ILoggerFactory logFactory = CreateLoggerFactory();

        await using RestClusterHarness harness = await RestClusterHarness.CreateAsync(
            [9769, 9770, 9771], logFactory, partitions: 1, ct: ct);

        (RestTestNode leader, RestTestNode caller, RestTestNode otherFollower) = await IdentifyClusterRolesAsync(harness, ct);

        RestCommunication rest = new();

        // An endpoint that is not part of the roster cannot mutate it even if the request reached
        // the leader, so this stays a pure routing assertion.
        LeaveResponse response = await rest.SendLeave(
            caller.Manager, new RaftNode(otherFollower.Endpoint), new LeaveRequest("ghost.nowhere:1", -1), ct);

        Assert.False(response.Success);
        Assert.Equal(leader.Endpoint, response.LeaderHint);
        Assert.False(response.Terminal, "a redirect is retryable, never terminal");
    }

    /// <summary>
    /// A role-transition request that lands on a follower must fail with the leader hint and the
    /// not-leader verdict, so a decommission drain retries at the right node instead of silently
    /// degrading into an immediate removal.
    /// Ports 9772-9774.
    /// </summary>
    [Fact]
    public async Task SetMemberRole_FollowerRedirectsWithLeaderHintAndNotLeaderStatus()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        using ILoggerFactory logFactory = CreateLoggerFactory();

        await using RestClusterHarness harness = await RestClusterHarness.CreateAsync(
            [9772, 9773, 9774], logFactory, partitions: 1, ct: ct);

        (RestTestNode leader, RestTestNode caller, RestTestNode otherFollower) = await IdentifyClusterRolesAsync(harness, ct);

        RestCommunication rest = new();

        SetMemberRoleResponse response = await rest.SendSetMemberRole(
            caller.Manager,
            new RaftNode(otherFollower.Endpoint),
            new SetMemberRoleRequest("ghost.nowhere:1", -1, ClusterMemberRole.Leaving),
            ct);

        Assert.False(response.Success);
        Assert.Equal(leader.Endpoint, response.LeaderHint);
        Assert.Equal(RaftOperationStatus.NodeIsNotLeader, response.Status);
    }

    /// <summary>
    /// A snapshot status query stages nothing and must be answered with the "no install" outcome
    /// on a partition that never received one — the sender-side polling loop distinguishes that
    /// from a transfer failure by the typed outcome, so the route must carry it unharmed.
    /// Ports 9775-9777.
    /// </summary>
    [Fact]
    public async Task InstallSnapshot_StatusQueryWithoutInstall_AnswersNoInstall()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        using ILoggerFactory logFactory = CreateLoggerFactory();

        await using RestClusterHarness harness = await RestClusterHarness.CreateAsync(
            [9775, 9776, 9777], logFactory, partitions: 1, ct: ct);

        (_, RestTestNode caller, RestTestNode receiver) = await IdentifyClusterRolesAsync(harness, ct);

        RestCommunication rest = new();

        SnapshotResponse response = await rest.SendInstallSnapshot(
            caller.Manager,
            new RaftNode(receiver.Endpoint),
            new SnapshotRequest
            {
                StatusQuery = true,
                ChunkIndex = -1,
                PartitionId = 0,
            },
            ct);

        Assert.Equal(SnapshotInstallOutcome.NoInstall, response.Outcome);
        Assert.False(response.Success, "NoInstall must read as a non-success for the chunk loop");
    }

    private static async Task<(RestTestNode Leader, RestTestNode Caller, RestTestNode Other)> IdentifyClusterRolesAsync(
        RestClusterHarness harness, CancellationToken ct)
    {
        RestTestNode? leader = null;
        using CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(TimeSpan.FromSeconds(15));

        while (leader is null && !cts.Token.IsCancellationRequested)
        {
            foreach (RestTestNode node in harness.Nodes)
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

        List<RestTestNode> followers = [.. harness.Nodes.Where(n => n != leader)];
        Assert.True(followers.Count >= 2, "expected a leader plus two followers");
        return (leader, followers[0], followers[1]);
    }

    private static async Task WaitForConditionAsync(Func<bool> condition, TimeSpan timeout, CancellationToken ct)
    {
        using CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(TestTimeouts.Scale(timeout));

        while (!cts.Token.IsCancellationRequested)
        {
            if (condition())
                return;

            await Task.Delay(100, cts.Token).ConfigureAwait(false);
        }

        Assert.Fail($"Condition was not met within {timeout}.");
    }
}
