using System.Diagnostics.CodeAnalysis;
using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;

namespace Kommander.Tests;

/// <summary>
/// End-to-end coverage for the two fault-qualification hooks on a real three-node in-memory
/// cluster — the shape a consumer's own suite actually drives, through the public
/// <see cref="IRaft"/> surface rather than the state machine.
///
/// <para><see cref="IRaft.HoldCommittedProposalRepliesForTesting"/> separates two events that are
/// otherwise the same event in every in-process test: "the commit is durable on a quorum" and "the
/// coordinator learned it committed". The first two tests hold the leader's reply, prove the entry
/// is durable cluster-wide while its proposer knows nothing, and check both outcomes — a released
/// reply answers exactly as the unheld path does, and an abandoned one never reports a false
/// success even across a leader change.</para>
///
/// <para><see cref="IRaft.HoldConsumerAppliesForTesting"/> keeps committed entries pending in a
/// follower's log so a tail exists above a future snapshot boundary. The third test checks the
/// property that makes such a cell worth building: the held node keeps replicating and acking, but
/// advertises no applied progress it did not make.</para>
/// </summary>
[SuppressMessage("Performance", "CA1859:Use concrete types when possible for improved performance")]
[Collection(ClusterIntegrationCollection.Name)]
public class TestDurableButUnansweredCommit
{
    private const int UserPartition = 1;

    private readonly ILogger<IRaft> logger;

    public TestDurableButUnansweredCommit()
    {
        ILoggerFactory loggerFactory = LoggerFactory.Create(builder => builder.SetMinimumLevel(LogLevel.Warning));
        logger = loggerFactory.CreateLogger<IRaft>();
    }

    private static RaftConfiguration NodeConfig(string name, int id, int port) => new()
    {
        NodeName = name,
        NodeId = id,
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
        // Short enough to keep the dropped-reply case quick, long enough that the hold's own bound
        // (the same value) cannot fire before the test resolves the hold explicitly.
        ProposalTimeout = TimeSpan.FromSeconds(3),
    };

    private IRaft NewNode(InMemoryCommunication communication, string name, int id, int port, params string[] peers) =>
        new RaftManager(
            NodeConfig(name, id, port),
            new StaticDiscovery([.. peers.Select(p => new RaftNode(p))]),
            new InMemoryWAL(logger),
            communication,
            new HybridLogicalClock(),
            logger);

    private async Task<IRaft[]> AssembleThreeNodeCluster(InMemoryCommunication communication)
    {
        IRaft node1 = NewNode(communication, "node1", 1, 8001, "localhost:8002", "localhost:8003");
        IRaft node2 = NewNode(communication, "node2", 2, 8002, "localhost:8001", "localhost:8003");
        IRaft node3 = NewNode(communication, "node3", 3, 8003, "localhost:8001", "localhost:8002");

        communication.SetNodes(new()
        {
            { "localhost:8001", node1 },
            { "localhost:8002", node2 },
            { "localhost:8003", node3 },
        });

        await node1.UpdateNodes();
        await node2.UpdateNodes();
        await node3.UpdateNodes();

        await Task.WhenAll(
            node1.JoinCluster(TestContext.Current.CancellationToken),
            node2.JoinCluster(TestContext.Current.CancellationToken),
            node3.JoinCluster(TestContext.Current.CancellationToken));

        return [node1, node2, node3];
    }

    private static async Task<IRaft> WaitForLeaderAsync(IRaft[] nodes, int partitionId, IRaft? excluding = null)
    {
        for (int attempt = 0; attempt < 400; attempt++)
        {
            foreach (IRaft node in nodes)
            {
                if (excluding is not null && ReferenceEquals(node, excluding))
                    continue;

                if (await node.AmILeaderQuick(partitionId).ConfigureAwait(false))
                    return node;
            }

            await Task.Delay(25).ConfigureAwait(false);
        }

        throw new InvalidOperationException($"No leader elected for partition {partitionId}");
    }

    private static async Task WaitForCommitIndexAsync(IRaft node, int partitionId, long atLeast)
    {
        for (int attempt = 0; attempt < 400; attempt++)
        {
            if (node.GetCommitIndex(partitionId) >= atLeast)
                return;

            await Task.Delay(25).ConfigureAwait(false);
        }

        throw new InvalidOperationException(
            $"{node.GetLocalEndpoint()} never reached commit index {atLeast} on partition {partitionId} (last seen {node.GetCommitIndex(partitionId)})");
    }

    /// <summary>
    /// Holds the leader's reply for one committed entry, then changes leader underneath it.
    ///
    /// <list type="bullet">
    ///   <item>While the reply is held, the entry is durable on every node and the leader reports it
    ///         committed — and the proposer's call has not returned.</item>
    ///   <item>After heartbeats are suspended and another node wins the term, the entry is still
    ///         present on the new leader: it was durable before the change, so the term change
    ///         cannot lose it.</item>
    ///   <item>The original caller is answered with an honest non-success — never a false
    ///         success.</item>
    /// </list>
    /// </summary>
    [Fact]
    public async Task HeldReply_EntrySurvivesALeaderChange_AndTheCallerIsNeverToldItSucceeded()
    {
        InMemoryCommunication communication = new();
        IRaft[] nodes = await AssembleThreeNodeCluster(communication);

        try
        {
            IRaft leader = await WaitForLeaderAsync(nodes, UserPartition);

            TaskCompletionSource<HeldProposalReply> holdSignal = new(TaskCreationOptions.RunContinuationsAsynchronously);

            using IDisposable registration = leader.HoldCommittedProposalRepliesForTesting(
                UserPartition,
                held => holdSignal.TrySetResult(held));

            // The write blocks on the held reply, so it must not be awaited inline.
            Task<RaftReplicationResult> write = leader.ReplicateLogs(
                UserPartition,
                "Greeting",
                "Hello World"u8.ToArray(),
                cancellationToken: TestContext.Current.CancellationToken);

            HeldProposalReply reply = await holdSignal.Task.WaitAsync(TimeSpan.FromSeconds(15), TestContext.Current.CancellationToken);

            // Durable at quorum: the leader reports the entry committed and every node's frontier
            // covers it — while the proposer has learned nothing.
            Assert.False(write.IsCompleted, "the proposer must still be waiting while its reply is held");
            foreach (IRaft node in nodes)
                await WaitForCommitIndexAsync(node, UserPartition, reply.CommitIndex);

            // Force the term change the hook exists to make testable.
            Assert.Equal(RaftOperationStatus.Success, await leader.SuspendHeartbeatsAsync(UserPartition, TestContext.Current.CancellationToken));

            IRaft newLeader = await WaitForLeaderAsync(nodes, UserPartition, excluding: leader);

            // The entry was durable before the change, so the new leader holds it.
            await WaitForCommitIndexAsync(newLeader, UserPartition, reply.CommitIndex);

            // Abandon the reply (the killed-leader case). A step-down may already have failed the
            // waiter, in which case this is a no-op; either way the caller must not see a success.
            reply.Drop();

            RaftReplicationResult result = await write.WaitAsync(TimeSpan.FromSeconds(15), TestContext.Current.CancellationToken);

            Assert.False(result.Success);
            Assert.NotEqual(RaftOperationStatus.Success, result.Status);
        }
        finally
        {
            foreach (IRaft node in nodes)
                await node.LeaveCluster(true, CancellationToken.None);
        }
    }

    /// <summary>
    /// The follower half of the mid-install hook, on a real cluster: holding consumer applies keeps
    /// committed entries pending in the log while replication and acks carry on, and the node
    /// advertises no applied progress it did not make.
    ///
    /// <list type="bullet">
    ///   <item>Writes still commit — the held follower keeps acking, so quorum is unaffected.</item>
    ///   <item>Its consumer receives nothing, and <see cref="IRaft.ConfirmLocalApplicationAsync"/>
    ///         fails closed: a node that cannot prove its applied state covers the committed prefix
    ///         must not claim it does.</item>
    ///   <item>Resuming delivers everything that accumulated, in log id order and exactly once, and
    ///         the node can confirm again.</item>
    /// </list>
    /// </summary>
    [Fact]
    public async Task AppliesHeldOnAFollower_EntriesStayPending_AndResumeDeliversThemInOrderOnce()
    {
        InMemoryCommunication communication = new();
        IRaft[] nodes = await AssembleThreeNodeCluster(communication);

        try
        {
            IRaft leader = await WaitForLeaderAsync(nodes, UserPartition);
            IRaft follower = nodes.First(node => !ReferenceEquals(node, leader));

            List<long> deliveredToFollower = [];
            follower.OnReplicationReceived += (partitionId, log) =>
            {
                if (partitionId == UserPartition)
                {
                    lock (deliveredToFollower)
                        deliveredToFollower.Add(log.Id);
                }

                return Task.FromResult(true);
            };

            Assert.Equal(RaftOperationStatus.Success,
                await follower.HoldConsumerAppliesForTesting(UserPartition, TestContext.Current.CancellationToken));

            const int entries = 5;
            List<long> committed = [];
            for (int i = 0; i < entries; i++)
            {
                RaftReplicationResult result = await leader.ReplicateLogs(
                    UserPartition,
                    "Greeting",
                    "Hello World"u8.ToArray(),
                    cancellationToken: TestContext.Current.CancellationToken);

                Assert.True(result.Success, $"write {i} failed with {result.Status}");
                committed.Add(result.LogIndex);
            }

            // Replication and acks are untouched: the follower's own frontier covers the entries...
            await WaitForCommitIndexAsync(follower, UserPartition, committed[^1]);

            // ...while its consumer has seen none of them, and it refuses to claim otherwise.
            lock (deliveredToFollower)
                Assert.Empty(deliveredToFollower);

            Assert.False(
                await follower.ConfirmLocalApplicationAsync(UserPartition, TestContext.Current.CancellationToken),
                "a follower whose applies are held must not confirm local application");

            Assert.Equal(RaftOperationStatus.Success,
                await follower.ResumeConsumerAppliesForTesting(UserPartition, TestContext.Current.CancellationToken));

            // Everything that accumulated is delivered, in log id order and exactly once.
            long[] delivered;
            lock (deliveredToFollower)
                delivered = [.. deliveredToFollower];

            foreach (long id in committed)
                Assert.Equal(1, delivered.Count(d => d == id));

            long[] forCommitted = [.. delivered.Where(committed.Contains)];
            Assert.Equal([.. forCommitted.OrderBy(id => id)], forCommitted);

            // And the node can prove its applied state again.
            bool confirmed = false;
            for (int attempt = 0; attempt < 20 && !confirmed; attempt++)
                confirmed = await follower.ConfirmLocalApplicationAsync(UserPartition, TestContext.Current.CancellationToken);

            Assert.True(confirmed, "a resumed follower must be able to confirm local application again");
        }
        finally
        {
            foreach (IRaft node in nodes)
                await node.LeaveCluster(true, CancellationToken.None);
        }
    }

    /// <summary>
    /// Releasing a held reply returns exactly what the unheld path returns, so a test that only
    /// wants to observe the window does not change the outcome the consumer sees.
    /// </summary>
    [Fact]
    public async Task ReleasedReply_ReturnsTheSameSuccessTheUnheldPathReturns()
    {
        InMemoryCommunication communication = new();
        IRaft[] nodes = await AssembleThreeNodeCluster(communication);

        try
        {
            IRaft leader = await WaitForLeaderAsync(nodes, UserPartition);

            TaskCompletionSource<HeldProposalReply> holdSignal = new(TaskCreationOptions.RunContinuationsAsynchronously);

            using IDisposable registration = leader.HoldCommittedProposalRepliesForTesting(
                UserPartition,
                held => holdSignal.TrySetResult(held));

            Task<RaftReplicationResult> write = leader.ReplicateLogs(
                UserPartition,
                "Greeting",
                "Hello World"u8.ToArray(),
                cancellationToken: TestContext.Current.CancellationToken);

            HeldProposalReply reply = await holdSignal.Task.WaitAsync(TimeSpan.FromSeconds(15), TestContext.Current.CancellationToken);
            Assert.False(write.IsCompleted);

            reply.Release();

            RaftReplicationResult result = await write.WaitAsync(TimeSpan.FromSeconds(15), TestContext.Current.CancellationToken);

            Assert.True(result.Success);
            Assert.Equal(RaftOperationStatus.Success, result.Status);
            Assert.Equal(reply.CommitIndex, result.LogIndex);
        }
        finally
        {
            foreach (IRaft node in nodes)
                await node.LeaveCluster(true, CancellationToken.None);
        }
    }
}
