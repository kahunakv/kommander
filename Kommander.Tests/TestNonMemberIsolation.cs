
using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests;

/// <summary>
/// Regression tests for leadership perturbation by endpoints that are reachable through the
/// transport but were never admitted to committed membership. A non-member endpoint must not be
/// able to be adopted as leader (AppendLogs / InstallSnapshot), clear the current leader and force
/// an election (StepDownNotice / TransferLeadership), or kill a member's process via the
/// NodeId-collision handshake exit. Origin: a downstream consumer registered a 4th node's endpoint
/// in the transport map before it joined membership and observed leader-stability churn.
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
public sealed class TestNonMemberIsolation
{
    private const int UserPartition = 1;

    private const string NonMemberEndpoint = "localhost:8299";

    private readonly ILogger<IRaft> logger = NullLoggerFactory.Instance.CreateLogger<IRaft>();

    private static async Task WaitForConditionAsync(Func<bool> condition, CancellationToken ct, int timeoutMs = 30_000)
    {
        long deadline = Environment.TickCount64 + timeoutMs;
        while (Environment.TickCount64 < deadline)
        {
            ct.ThrowIfCancellationRequested();
            if (condition()) return;
            await Task.Delay(50, ct).ConfigureAwait(false);
        }
        throw new TimeoutException($"Condition not met within {timeoutMs} ms.");
    }

    [Fact]
    public async Task NonMemberRpcs_DoNotPerturbLeadership()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        InMemoryCommunication communication = new();

        RaftManager node1 = BuildNode("node1", 1, 8291, ["localhost:8292", "localhost:8293"], communication);
        RaftManager node2 = BuildNode("node2", 2, 8292, ["localhost:8291", "localhost:8293"], communication);
        RaftManager node3 = BuildNode("node3", 3, 8293, ["localhost:8291", "localhost:8292"], communication);
        RaftManager[] members = [node1, node2, node3];

        communication.SetNodes(new Dictionary<string, IRaft>
        {
            ["localhost:8291"] = node1,
            ["localhost:8292"] = node2,
            ["localhost:8293"] = node3
        });

        try
        {
            await Task.WhenAll(node1.JoinCluster(ct), node2.JoinCluster(ct), node3.JoinCluster(ct));

            await WaitForConditionAsync(
                () => members.All(n => n.IsInitialized),
                ct);

            // The membership fence is only active once a roster has been seeded; before that,
            // IsMember treats every peer as a member (pre-seed backward compat).
            await WaitForConditionAsync(
                () => members.All(n => n.SystemCoordinator.GetMembership().MembershipVersion > 0),
                ct);

            string stableLeader = await node1.WaitForLeaderStableAsync(
                UserPartition,
                minStableFor: TimeSpan.FromMilliseconds(300),
                timeout: TimeSpan.FromSeconds(30),
                ct);

            long[] ticksBefore = members
                .Select(n => n.Partitions[UserPartition].LeaderChangedTicks)
                .ToArray();

            HLCTimestamp intruderTime = new HybridLogicalClock().SendOrLocalEvent(99);
            const long inflatedTerm = 1_000_000;

            foreach (RaftManager member in members)
            {
                // A fresh-term AppendLogs from a non-member must not be adopted as leader.
                member.AppendLogs(new AppendLogsRequest(UserPartition, inflatedTerm, intruderTime, NonMemberEndpoint));

                // A non-member step-down notice / leadership transfer must not clear the leader
                // and force an election.
                member.Partitions[UserPartition].StepDownNotice(
                    new StepDownNoticeRequest(UserPartition, inflatedTerm, intruderTime, NonMemberEndpoint));
                member.Partitions[UserPartition].TransferLeadership(
                    new TransferLeadershipRequest(UserPartition, inflatedTerm, intruderTime, NonMemberEndpoint, member.GetLocalEndpoint()));

                // A non-member handshake carrying the member's own NodeId must be ignored — before
                // the fence this called Environment.Exit(1) and killed the member's process.
                await member.Handshake(new HandshakeRequest(member.LocalNodeId, UserPartition, 999, NonMemberEndpoint));
            }

            foreach (RaftManager member in members)
                await member.Partitions[UserPartition].DrainAsync(ct);

            // Give any wrongly-triggered election time to manifest before asserting stability.
            await Task.Delay(500, ct);

            for (int i = 0; i < members.Length; i++)
            {
                RaftPartition partition = members[i].Partitions[UserPartition];

                Assert.Equal(stableLeader, partition.Leader);
                Assert.Equal(ticksBefore[i], partition.LeaderChangedTicks);
            }

            // The partition still satisfies a fresh stability window promptly.
            string leaderAfter = await node1.WaitForLeaderStableAsync(
                UserPartition,
                minStableFor: TimeSpan.FromMilliseconds(300),
                timeout: TimeSpan.FromSeconds(10),
                ct);

            Assert.Equal(stableLeader, leaderAfter);
        }
        finally
        {
            await node3.LeaveCluster(dispose: true, cancellationToken: CancellationToken.None);
            await node2.LeaveCluster(dispose: true, cancellationToken: CancellationToken.None);
            await node1.LeaveCluster(dispose: true, cancellationToken: CancellationToken.None);
        }
    }

    private RaftManager BuildNode(
        string name, int id, int port,
        string[] peers,
        InMemoryCommunication communication)
    {
        RaftConfiguration config = new()
        {
            NodeName = name,
            NodeId = id,
            Host = "localhost",
            Port = port,
            InitialPartitions = 1,
            HeartbeatInterval = TimeSpan.FromMilliseconds(50),
            RecentHeartbeat = TimeSpan.FromMilliseconds(25),
            VotingTimeout = TimeSpan.FromMilliseconds(500),
            CheckLeaderInterval = TimeSpan.FromMilliseconds(25),
            UpdateNodesInterval = TimeSpan.FromMilliseconds(200),
            TimerInitialDelay = TimeSpan.FromMilliseconds(25),
            StartElectionTimeout = 100,
            EnableQuiescence = false,
            EndElectionTimeout = 300,
        };

        return new RaftManager(
            config,
            new StaticDiscovery(peers.Select(p => new RaftNode(p)).ToList()),
            new InMemoryWAL(logger),
            communication,
            new HybridLogicalClock(),
            logger);
    }
}
