using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.Tests.Communication;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.Scheduling;

/// <summary>
/// <see cref="RaftConfiguration.FanOutBeforeLocalWrite"/>: a leader sends a proposal to its
/// followers while its own Proposed write is still queued, and the proposal still reaches quorum
/// only once that write is durable.
///
/// <para>The leader's WAL is held on a gate while one proposal is in flight. With the option on, the
/// followers receive the batch and acknowledge it while the gate is closed, and the caller is not
/// answered until the gate opens: a majority of follower acks alone must not complete a proposal
/// the leader has not written. With the option off, nothing reaches the followers until the gate
/// opens — the serial order the round used to pay for with two WAL writes in series.</para>
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
public class TestFanOutBeforeLocalWrite
{
    private const int PartitionId = 1;

    [Fact]
    public async Task FollowersGetTheBatchWhileTheLeaderWrites_ButTheQuorumWaitsForTheLeadersWrite()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using Cluster cluster = await Cluster.StartAsync(9321, fanOutBeforeLocalWrite: true, ct);
        byte[][] batch = [new byte[280], new byte[280], new byte[280]];

        long framesBefore = cluster.FramesToFollowers();
        long leaderMaxBefore = cluster.LeaderWal.GetMaxLog(PartitionId);

        // Fires when a proposal reaches quorum. The caller cannot see an early quorum itself: its
        // ReplicateLogs is answered only by the leader's propose completion.
        int quorums = 0;
        cluster.Leader.OnCommitAcksObserved += acks =>
        {
            if (acks.Count > 0 && acks[0].Partition == PartitionId)
                Interlocked.Increment(ref quorums);
        };

        cluster.LeaderWal.Stall();
        Task<RaftReplicationResult> proposal;
        try
        {
            proposal = cluster.Leader.ReplicateLogs(PartitionId, "test", batch, cancellationToken: ct);

            // Both followers get the batch and write it with the leader's write still held; their acks
            // follow the write, and the delay gives them time to arrive.
            await WaitUntil(() => cluster.FramesToFollowers() - framesBefore >= 2, ct);
            await WaitUntil(() => cluster.FollowerWals.All(w => w.GetMaxLog(PartitionId) >= leaderMaxBefore + batch.Length), ct);
            await Task.Delay(200, ct);

            Assert.True(cluster.LeaderWal.BlockedWrites > 0, "the leader's write was not held");

            Assert.True(Volatile.Read(ref quorums) == 0,
                "the proposal reached quorum while the leader's own Proposed write was still held: a majority of " +
                "follower acks must not complete it before the leader's copy is durable");
            Assert.False(proposal.IsCompleted);
        }
        finally
        {
            cluster.LeaderWal.Release();
        }

        RaftReplicationResult result = await proposal.WaitAsync(TimeSpan.FromSeconds(10), ct);
        Assert.True(result.Success, $"proposal failed after the leader's write was released: {result.Status}");
        Assert.True(Volatile.Read(ref quorums) >= 1);
    }

    [Fact]
    public async Task WithTheOptionOff_NothingIsSentUntilTheLeadersWriteIsDurable()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using Cluster cluster = await Cluster.StartAsync(9331, fanOutBeforeLocalWrite: false, ct);
        byte[][] batch = [new byte[280], new byte[280], new byte[280]];

        long framesBefore = cluster.FramesToFollowers();

        cluster.LeaderWal.Stall();
        Task<RaftReplicationResult> proposal;
        try
        {
            proposal = cluster.Leader.ReplicateLogs(PartitionId, "test", batch, cancellationToken: ct);

            await WaitUntil(() => cluster.LeaderWal.BlockedWrites > 0, ct);
            await Task.Delay(200, ct);

            Assert.Equal(framesBefore, cluster.FramesToFollowers());
            Assert.False(proposal.IsCompleted);
        }
        finally
        {
            cluster.LeaderWal.Release();
        }

        RaftReplicationResult result = await proposal.WaitAsync(TimeSpan.FromSeconds(10), ct);
        Assert.True(result.Success, $"proposal failed after the leader's write was released: {result.Status}");
    }

    private static async Task WaitUntil(Func<bool> condition, CancellationToken ct)
    {
        using CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(TestTimeouts.Scale(TimeSpan.FromSeconds(5)));

        while (!condition())
            await Task.Delay(5, cts.Token);
    }

    /// <summary>A three-voter in-memory cluster whose WALs can be held, with the leader of partition 1 found.</summary>
    private sealed class Cluster : IAsyncDisposable
    {
        private readonly RaftManager[] nodes;
        private readonly TestWalStallStepDown.GatedWal[] wals;

        public AppendCountingCommunication Counting { get; }
        public RaftManager Leader { get; private set; } = null!;
        public TestWalStallStepDown.GatedWal LeaderWal { get; private set; } = null!;
        private string[] followers = [];

        private Cluster(RaftManager[] nodes, TestWalStallStepDown.GatedWal[] wals, AppendCountingCommunication counting)
        {
            this.nodes = nodes;
            this.wals = wals;
            Counting = counting;
        }

        public long FramesToFollowers() => followers.Sum(f => Counting.FramesTo(PartitionId, f));

        public IEnumerable<TestWalStallStepDown.GatedWal> FollowerWals => wals.Where(w => !ReferenceEquals(w, LeaderWal));

        public static async Task<Cluster> StartAsync(int basePort, bool fanOutBeforeLocalWrite, CancellationToken ct)
        {
            ILogger<IRaft> logger = NullLoggerFactory.Instance.CreateLogger<IRaft>();
            string[] endpoints = [$"localhost:{basePort}", $"localhost:{basePort + 1}", $"localhost:{basePort + 2}"];

            AppendCountingCommunication counting = new(new InMemoryCommunication());
            TestWalStallStepDown.GatedWal[] wals = endpoints.Select(_ => new TestWalStallStepDown.GatedWal(new InMemoryWAL(logger))).ToArray();

            RaftManager[] nodes = new RaftManager[endpoints.Length];
            for (int i = 0; i < endpoints.Length; i++)
            {
                RaftConfiguration config = new()
                {
                    NodeId = i + 1,
                    Host = "localhost",
                    Port = basePort + i,
                    InitialPartitions = 1,
                    PingInterval = TimeSpan.Zero,
                    HeartbeatInterval = TimeSpan.FromMilliseconds(50),
                    RecentHeartbeat = TimeSpan.FromMilliseconds(25),
                    VotingTimeout = TimeSpan.FromMilliseconds(250),
                    CheckLeaderInterval = TimeSpan.FromMilliseconds(25),
                    UpdateNodesInterval = TimeSpan.FromMilliseconds(100),
                    TimerInitialDelay = TimeSpan.FromMilliseconds(25),
                    StartElectionTimeout = 100,
                    EnableQuiescence = false,
                    EndElectionTimeout = 250,
                    FanOutBeforeLocalWrite = fanOutBeforeLocalWrite,
                };

                nodes[i] = new RaftManager(
                    config,
                    new StaticDiscovery(endpoints.Where((_, j) => j != i).Select(e => new RaftNode(e)).ToList()),
                    wals[i],
                    counting,
                    new HybridLogicalClock(),
                    logger);
            }

            Cluster cluster = new(nodes, wals, counting);

            counting.SetNodes(endpoints.Select((e, i) => (e, (IRaft)nodes[i])).ToDictionary(x => x.e, x => x.Item2));
            await Task.WhenAll(nodes.Select(n => n.UpdateNodes()));
            await Task.WhenAll(nodes.Select(n => n.JoinCluster(ct)));

            for (int attempt = 0; attempt < 500 && cluster.Leader is null; attempt++)
            {
                for (int i = 0; i < nodes.Length; i++)
                {
                    if (await nodes[i].AmILeaderQuick(PartitionId))
                    {
                        cluster.Leader = nodes[i];
                        cluster.LeaderWal = wals[i];
                        cluster.followers = endpoints.Where((_, j) => j != i).ToArray();
                        break;
                    }
                }

                if (cluster.Leader is null)
                    await Task.Delay(10, ct);
            }

            Assert.NotNull(cluster.Leader);

            // Past the promotion barrier and first catch-up, so the next proposal is a plain one.
            for (int i = 0; i < 5; i++)
                Assert.True((await cluster.Leader.ReplicateLogs(PartitionId, "test", [new byte[16]], cancellationToken: ct)).Success);

            await Task.Delay(100, ct);
            return cluster;
        }

        public async ValueTask DisposeAsync()
        {
            foreach (TestWalStallStepDown.GatedWal wal in wals)
                wal.Release();

            foreach (RaftManager node in nodes)
                await node.LeaveCluster(true, CancellationToken.None);

            foreach (TestWalStallStepDown.GatedWal wal in wals)
                wal.Dispose();
        }
    }
}
