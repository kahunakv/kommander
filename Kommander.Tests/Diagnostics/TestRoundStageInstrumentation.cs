using System.Diagnostics.Metrics;
using Kommander.Communication.Memory;
using Kommander.Diagnostics;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;

namespace Kommander.Tests.Diagnostics;

/// <summary>
/// Pins the shape of the <c>raft.round.stage_ms</c> histogram (<see cref="RoundStageInstrumentation"/>)
/// on a three-node in-memory cluster.
///
/// <para>The assertions are structural, never on absolute timings: every successful call records
/// each leader-chain stage once; the follower stages appear; and, per call, the serial stages of
/// the leader chain are sub-intervals of the round in series, so their totals cannot exceed the
/// round total. That check is what proves the stages do not overlap or double-count. The leader's
/// own write (<c>leader.wal</c>, <c>leader.wal_completion</c>) runs beside the fan-out and the
/// replication (<see cref="RaftConfiguration.FanOutBeforeLocalWrite"/>, the default), and the
/// quorum waits for it, so it must fit inside those two instead.</para>
///
/// <para>Serialized in the cluster collection: the switch and the histogram are process-wide,
/// and a cluster from another test would add its own stages to the listener.</para>
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
public sealed class TestRoundStageInstrumentation
{
    private const int Calls = 40;

    private static readonly string[] LeaderChain =
    [
        "leader.queue",
        "leader.propose",
        "leader.wal",
        "leader.wal_completion",
        "leader.fanout",
        "leader.replication",
        "leader.resume",
    ];

    /// <summary>The leader-chain stages that run one after the other with the fan-out ahead of the local write.</summary>
    private static readonly string[] SerialChain =
    [
        "leader.queue",
        "leader.propose",
        "leader.fanout",
        "leader.replication",
        "leader.resume",
    ];

    private readonly ILogger<IRaft> logger;

    public TestRoundStageInstrumentation(ITestOutputHelper outputHelper)
    {
        ILoggerFactory lf = LoggerFactory.Create(b => b
            .AddXUnit(outputHelper)
            .SetMinimumLevel(LogLevel.Warning));
        logger = lf.CreateLogger<IRaft>();
    }

    [Fact]
    public async Task Enabled_RecordsEveryLeaderStageOncePerCall_AndTheChainFitsInsideTheRound()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        RaftManager[] nodes = await BuildThreeNodeCluster(ct);

        using StageRecorder recorder = new();

        try
        {
            RaftManager leader = await FindLeader(nodes, partitionId: 1, ct);

            // Warm the path so the first measured call is not a leadership-barrier edge case.
            Assert.True((await leader.ReplicateLogs(1, "stage", [new byte[32]], cancellationToken: ct)).Success);

            RoundStageInstrumentation.Enabled = true;
            recorder.Clear();

            byte[][] batch = [.. Enumerable.Range(0, 8).Select(_ => new byte[64])];
            for (int i = 0; i < Calls; i++)
            {
                RaftReplicationResult result = await leader.ReplicateLogs(1, "stage", batch, cancellationToken: ct);
                Assert.True(result.Success, $"call {i}: {result.Status}");
            }

            RoundStageInstrumentation.Enabled = false;

            // The follower side of the last call can still be in flight when the caller resumes
            // (only one follower is needed for the quorum). Give it a moment before reading.
            await Task.Delay(TestTimeouts.Scale(200), ct);

            Dictionary<string, (long Count, double SumMs)> stages = recorder.Snapshot();

            Assert.Empty(recorder.NegativeStages);

            Assert.Equal(Calls, stages["leader.round"].Count);
            foreach (string stage in LeaderChain)
            {
                Assert.True(stages.ContainsKey(stage), $"missing stage {stage}");
                Assert.Equal(Calls, stages[stage].Count);
            }

            Assert.True(stages["leader.ack"].Count == Calls, "one quorum-making ack per call");
            Assert.True(stages["leader.ack_queue"].Count >= Calls, "every ack waits in the executor queue");
            Assert.True(stages["leader.commit_wal"].Count >= Calls - 1, "the commit marker is written for each call");

            foreach (string stage in new[] { "follower.queue", "follower.append", "follower.wal", "follower.wal_completion", "follower.ack" })
                Assert.True(stages.TryGetValue(stage, out (long Count, double SumMs) s) && s.Count >= Calls, $"stage {stage}: {(stages.TryGetValue(stage, out s) ? s.Count : 0)} samples");

            Assert.True(stages["transport.dispatch"].Count >= Calls * 2, "at least one append per follower per call");

            // Per call, the serial stages are disjoint sub-intervals of the round in series, so their
            // total cannot exceed the round total. The leader's own write runs beside the fan-out and
            // the replication, and the quorum waits for it, so it fits inside those two. A small
            // tolerance covers the WAL stage, which is measured on the scheduler's tick source (the
            // stopwatch here) with its own rounding.
            double chainMs = SerialChain.Sum(s => stages[s].SumMs);
            double roundMs = stages["leader.round"].SumMs;
            Assert.True(chainMs <= roundMs * 1.01 + 0.5, $"leader chain {chainMs:0.000} ms exceeds the round {roundMs:0.000} ms");

            double localWriteMs = stages["leader.wal"].SumMs + stages["leader.wal_completion"].SumMs;
            double besideMs = stages["leader.fanout"].SumMs + stages["leader.replication"].SumMs;
            Assert.True(localWriteMs <= besideMs * 1.01 + 0.5, $"leader write {localWriteMs:0.000} ms exceeds the fan-out and replication {besideMs:0.000} ms it runs beside");
        }
        finally
        {
            RoundStageInstrumentation.Enabled = false;
            foreach (RaftManager node in nodes)
                node.Dispose();
        }
    }

    [Fact]
    public async Task Disabled_RecordsNothing_EvenWithAListener()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        RaftManager[] nodes = await BuildThreeNodeCluster(ct);

        using StageRecorder recorder = new();

        try
        {
            RoundStageInstrumentation.Enabled = false;
            Assert.False(RoundStageInstrumentation.IsActive);

            RaftManager leader = await FindLeader(nodes, partitionId: 1, ct);
            recorder.Clear();

            for (int i = 0; i < 10; i++)
                Assert.True((await leader.ReplicateLogs(1, "stage", [new byte[32]], cancellationToken: ct)).Success);

            Assert.Empty(recorder.Snapshot());
        }
        finally
        {
            foreach (RaftManager node in nodes)
                node.Dispose();
        }
    }

    /// <summary>Collects count and sum per stage from the histogram, for the whole process.</summary>
    private sealed class StageRecorder : IDisposable
    {
        private readonly MeterListener listener = new();
        private readonly object sync = new();
        private readonly Dictionary<string, (long Count, double SumMs)> stages = [];

        public List<string> NegativeStages { get; } = [];

        public StageRecorder()
        {
            listener.InstrumentPublished = (instrument, l) =>
            {
                if (instrument.Meter.Name == KommanderMetrics.MeterName && instrument.Name == RoundStageInstrumentation.HistogramName)
                    l.EnableMeasurementEvents(instrument);
            };

            listener.SetMeasurementEventCallback<double>((_, value, tags, _) =>
            {
                foreach (KeyValuePair<string, object?> tag in tags)
                {
                    if (tag.Key != RoundStageInstrumentation.StageTag || tag.Value is not string name)
                        continue;

                    // Never assert here: this runs on the Kommander thread that recorded the value.
                    lock (sync)
                    {
                        if (value < 0)
                            NegativeStages.Add(name);

                        (long count, double sum) = stages.GetValueOrDefault(name);
                        stages[name] = (count + 1, sum + value);
                    }
                }
            });

            listener.Start();
        }

        public void Clear()
        {
            lock (sync)
                stages.Clear();
        }

        public Dictionary<string, (long Count, double SumMs)> Snapshot()
        {
            lock (sync)
                return new(stages);
        }

        public void Dispose() => listener.Dispose();
    }

    private async Task<RaftManager[]> BuildThreeNodeCluster(CancellationToken ct)
    {
        InMemoryCommunication communication = new();

        RaftManager[] nodes =
        [
            MakeNode(communication, 8941, ["localhost:8942", "localhost:8943"]),
            MakeNode(communication, 8942, ["localhost:8941", "localhost:8943"]),
            MakeNode(communication, 8943, ["localhost:8941", "localhost:8942"]),
        ];

        communication.SetNodes(nodes.ToDictionary(n => n.GetLocalEndpoint(), n => (IRaft)n));

        foreach (RaftManager node in nodes)
            await node.UpdateNodes();

        await Task.WhenAll(nodes.Select(n => n.JoinCluster(ct)));
        return nodes;
    }

    private RaftManager MakeNode(InMemoryCommunication communication, int port, string[] peers)
    {
        RaftConfiguration config = new()
        {
            NodeName = $"node{port}",
            NodeId = port,
            Host = "localhost",
            Port = port,
            InitialPartitions = 1,
            HeartbeatInterval = TimeSpan.FromMilliseconds(50),
            RecentHeartbeat = TimeSpan.FromMilliseconds(25),
            VotingTimeout = TimeSpan.FromMilliseconds(250),
            CheckLeaderInterval = TimeSpan.FromMilliseconds(25),
            UpdateNodesInterval = TimeSpan.FromMilliseconds(50),
            TimerInitialDelay = TimeSpan.FromMilliseconds(25),
            StartElectionTimeout = 100,
            EnableQuiescence = false,
            EndElectionTimeout = 250,
        };

        return new RaftManager(
            config,
            new StaticDiscovery([.. peers.Select(e => new RaftNode(e))]),
            new InMemoryWAL(logger),
            communication,
            new HybridLogicalClock(),
            logger);
    }

    private static async Task<RaftManager> FindLeader(RaftManager[] nodes, int partitionId, CancellationToken ct)
    {
        ValueStopwatch sw = ValueStopwatch.StartNew();
        while (sw.GetElapsedMilliseconds() < TestTimeouts.Scale(15_000))
        {
            foreach (RaftManager node in nodes)
            {
                if (await node.AmILeaderQuick(partitionId))
                    return node;
            }

            await Task.Delay(25, ct);
        }

        throw new TimeoutException($"No leader for partition {partitionId}.");
    }
}
