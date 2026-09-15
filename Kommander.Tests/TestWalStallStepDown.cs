
using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Diagnostics;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;
using Kommander.Tests.Chaos;

namespace Kommander.Tests;

/// <summary>
/// The local durable-write stall watchdog (<see cref="RaftConfiguration.WalStallStepDownTimeout"/>): a leader
/// whose own WAL write stops being answered by its storage engine keeps heartbeating — its network liveness
/// is intact, so Raft's election timing never moves leadership — while every proposal it accepts waits on an
/// fsync that is not coming. The watchdog must shed such a leader within the bound, release the callers of
/// its stalled proposals at once, keep it from re-winning the term while the write is still pending, and
/// leave every replica with one agreed log once the write finally completes.
///
/// <para>The stall is injected with a WAL decorator whose <c>Write</c> blocks on a gate: exactly what a device
/// pause looks like from inside the process (the write neither fails nor returns).</para>
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
public sealed class TestWalStallStepDown
{
    private const int Partition = 1;

    private static readonly TimeSpan StepDownBound = TimeSpan.FromMilliseconds(400);

    /// <summary>An <see cref="IWAL"/> whose writes can be held open on a gate, over a real in-memory WAL.</summary>
    internal sealed class GatedWal : IWAL, IDisposable
    {
        private readonly IWAL inner;

        private readonly ManualResetEventSlim gate = new(initialState: true);

        private int blockedWrites;

        public GatedWal(IWAL inner) => this.inner = inner;

        /// <summary>Writes currently held on the gate.</summary>
        public int BlockedWrites => Volatile.Read(ref blockedWrites);

        /// <summary>Total writes that were held on the gate at some point.</summary>
        public int StalledWrites { get; private set; }

        public void Stall() => gate.Reset();

        public void Release() => gate.Set();

        public RaftOperationStatus Write(List<(int, List<RaftLog>)> logs) => Write(logs, sync: true);

        public RaftOperationStatus Write(List<(int, List<RaftLog>)> logs, bool sync)
        {
            if (!gate.IsSet)
            {
                StalledWrites++;
                Interlocked.Increment(ref blockedWrites);
                try
                {
                    gate.Wait();
                }
                finally
                {
                    Interlocked.Decrement(ref blockedWrites);
                }
            }

            return inner.Write(logs, sync);
        }

        public List<RaftLog> ReadLogs(int partitionId) => inner.ReadLogs(partitionId);

        public List<RaftLog> ReadLogsRange(int partitionId, long startLogIndex, int maxEntries = int.MaxValue) =>
            inner.ReadLogsRange(partitionId, startLogIndex, maxEntries);

        public List<RaftLog> ReadLogsRange(int partitionId, long startLogIndex, int maxEntries, long maxBytes) =>
            inner.ReadLogsRange(partitionId, startLogIndex, maxEntries, maxBytes);

        public long GetTermAt(int partitionId, long logIndex) => inner.GetTermAt(partitionId, logIndex);

        public long GetMaxLog(int partitionId) => inner.GetMaxLog(partitionId);

        public long GetCurrentTerm(int partitionId) => inner.GetCurrentTerm(partitionId);

        public long GetLastCheckpoint(int partitionId) => inner.GetLastCheckpoint(partitionId);

        public int CountPersistedLogs(int partitionId) => inner.CountPersistedLogs(partitionId);

        public int CountRemovableLogs(int partitionId) => inner.CountRemovableLogs(partitionId);

        public string? GetMetaData(string key) => inner.GetMetaData(key);

        // Hard state (term, vote) is metadata on the same engine: a real device pause holds it exactly
        // like a log row, so the gate covers it too — this is what made a stepped-down node unable to
        // vote for, or learn, its successor until its disk healed.
        public bool SetMetaData(string key, string value)
        {
            if (!gate.IsSet)
            {
                StalledWrites++;
                Interlocked.Increment(ref blockedWrites);
                try
                {
                    gate.Wait();
                }
                finally
                {
                    Interlocked.Decrement(ref blockedWrites);
                }
            }

            return inner.SetMetaData(key, value);
        }

        public (RaftOperationStatus Status, int Removed) CompactLogsOlderThan(int partitionId, long lastCheckpoint, int compactNumberEntries, int? maxTotalEntries = null) =>
            inner.CompactLogsOlderThan(partitionId, lastCheckpoint, compactNumberEntries, maxTotalEntries);

        public RaftOperationStatus DeletePartitionWAL(int partitionId) => inner.DeletePartitionWAL(partitionId);

        public RaftOperationStatus TruncateLogsAfter(int partitionId, long afterLogId) => inner.TruncateLogsAfter(partitionId, afterLogId);

        public (RaftOperationStatus Status, long MaxLogId) TruncateLogsAfterAndGetMax(int partitionId, long afterLogId) =>
            inner.TruncateLogsAfterAndGetMax(partitionId, afterLogId);

        public RaftOperationStatus TruncateProposedLogsAfter(int partitionId, long afterLogId) =>
            inner.TruncateProposedLogsAfter(partitionId, afterLogId);

        public (RaftOperationStatus Status, bool SuffixTruncated) InstallSnapshotBoundary(int partitionId, long snapshotIndex, long lastIncludedTerm, bool sync) =>
            inner.InstallSnapshotBoundary(partitionId, snapshotIndex, lastIncludedTerm, sync);

        public void Dispose()
        {
            gate.Set();
            gate.Dispose();
            (inner as IDisposable)?.Dispose();
        }
    }

    private static string Endpoint(int index) => $"localhost:{8200 + index}";

    private static RaftManager BuildNode(int index, InMemoryCommunication communication, GatedWal wal, ILogger<IRaft> logger, Action<RaftConfiguration>? configure = null)
    {
        List<RaftNode> peers = [];
        for (int i = 1; i <= 3; i++)
            if (i != index)
                peers.Add(new(Endpoint(i)));

        RaftConfiguration config = new()
        {
            NodeName = $"stall-node{index}",
            NodeId = index,
            Host = "localhost",
            Port = 8200 + index,
            InitialPartitions = Partition,
            HeartbeatInterval = TimeSpan.FromMilliseconds(50),
            RecentHeartbeat = TimeSpan.FromMilliseconds(25),
            VotingTimeout = TimeSpan.FromMilliseconds(250),
            CheckLeaderInterval = TimeSpan.FromMilliseconds(25),
            UpdateNodesInterval = TimeSpan.FromMilliseconds(100),
            TimerInitialDelay = TimeSpan.FromMilliseconds(25),
            StartElectionTimeout = 100,
            EndElectionTimeout = 250,
            EnableQuiescence = false,
            WalStallStepDownTimeout = StepDownBound,
            WalStallWarnThreshold = TimeSpan.FromMilliseconds(100),
        };

        configure?.Invoke(config);

        return new RaftManager(config, new StaticDiscovery(peers), wal, communication, new HybridLogicalClock(), logger);
    }

    private static async Task<(IRaft[] Nodes, Dictionary<IRaft, GatedWal> Wals)> AssembleAsync(ILogger<IRaft> logger, CancellationToken ct, Action<RaftConfiguration>? configure = null)
    {
        InMemoryCommunication communication = new();
        Dictionary<IRaft, GatedWal> wals = [];
        Dictionary<string, IRaft> network = [];
        IRaft[] nodes = new IRaft[3];

        for (int i = 1; i <= 3; i++)
        {
            GatedWal wal = new(new InMemoryWAL(logger));
            RaftManager node = BuildNode(i, communication, wal, logger, configure);
            nodes[i - 1] = node;
            wals[node] = wal;
            network[Endpoint(i)] = node;
        }

        communication.SetNodes(network);

        foreach (IRaft node in nodes)
            await node.UpdateNodes();

        await Task.WhenAll(nodes.Select(n => n.JoinCluster(ct)));

        await WaitForLeaderAsync(nodes, ct);

        return (nodes, wals);
    }

    private static async Task<IRaft> WaitForLeaderAsync(IRaft[] nodes, CancellationToken ct, IRaft? not = null, int timeoutMs = 15_000)
    {
        ValueStopwatch stopwatch = ValueStopwatch.StartNew();

        while (stopwatch.GetElapsedMilliseconds() < timeoutMs)
        {
            ct.ThrowIfCancellationRequested();

            foreach (IRaft node in nodes)
            {
                if (!ReferenceEquals(node, not) && await node.AmILeaderQuick(Partition).ConfigureAwait(false))
                    return node;
            }

            await Task.Delay(25, ct);
        }

        throw new TimeoutException($"No leader{(not is null ? "" : " other than the stalled node")} for partition {Partition} within {timeoutMs} ms.");
    }

    private static async Task WaitUntilAsync(Func<bool> predicate, int timeoutMs, string what, CancellationToken ct)
    {
        ValueStopwatch stopwatch = ValueStopwatch.StartNew();

        while (stopwatch.GetElapsedMilliseconds() < timeoutMs)
        {
            if (predicate())
                return;

            await Task.Delay(25, ct);
        }

        Assert.True(predicate(), what);
    }

    private static async Task LeaveAllAsync(IRaft[] nodes)
    {
        foreach (IRaft node in nodes)
        {
            try
            {
                await node.LeaveCluster(true);
            }
            catch
            {
                // best effort teardown
            }
        }
    }

    [Fact]
    public async Task LeaderWithStalledWal_StepsDownWithinTheBound_DoesNotRewinTheTerm_AndLogsConverge()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (IRaft[] nodes, Dictionary<IRaft, GatedWal> wals) = await AssembleAsync(new TempFileLogger<IRaft>(), ct);

        try
        {
            IRaft leader = await WaitForLeaderAsync(nodes, ct);
            GatedWal leaderWal = wals[leader];

            RaftReplicationResult before = await leader.ReplicateLogs(Partition, "Greeting", "before"u8.ToArray(), cancellationToken: ct);
            Assert.True(before.Success, $"baseline replication failed: {before.Status}");

            // The leader's disk stops answering. Its next proposal hands a write to the engine that never returns.
            leaderWal.Stall();
            ValueStopwatch failover = ValueStopwatch.StartNew();
            Task<RaftReplicationResult> stalledProposal = leader.ReplicateLogs(Partition, "Greeting", "during"u8.ToArray(), cancellationToken: ct);

            // Leadership moves to a replica whose disk answers, within the bound plus an election — an order of
            // magnitude under the 10 s the proposal's own timeout would have taken, and with heartbeats never
            // having stopped (the only reason an election could otherwise have started).
            IRaft successor = await WaitForLeaderAsync(nodes, ct, not: leader, timeoutMs: 5_000);
            double failoverMs = failover.GetElapsedMilliseconds();
            Assert.NotSame(leader, successor);
            Assert.True(leaderWal.StalledWrites > 0, "the stall must have caught a real write");

            // The stalled proposal's caller is released by the step-down, not by its own 10 s timeout.
            RaftReplicationResult stalledResult = await stalledProposal.WaitAsync(TimeSpan.FromSeconds(5), ct);
            Assert.False(stalledResult.Success, "a proposal whose leader stepped down mid-write must not report success");
            Assert.True(failover.GetElapsedMilliseconds() < 8_000, $"the caller waited {failover.GetElapsedMilliseconds():F0} ms; it must be released by the step-down");

            // The deposed node must LEARN its successor while its disk is still stalled: its vote for the
            // successor's term and its adoption of the new leader are persisted through the same stalled
            // engine, and neither may hold the partition executor — a node that cannot learn the leader
            // cannot re-route the proposals the step-down just released (CamusDB run sd4).
            string successorEndpoint = successor.GetLocalEndpoint();
            await WaitUntilAsync(
                () => leader.GetPartitionLeaderHint(Partition) == successorEndpoint,
                timeoutMs: 3_000,
                what: $"the stalled node must learn the successor while stalled; it believes the leader is '{leader.GetPartitionLeaderHint(Partition)}'",
                ct);
            Assert.True(leaderWal.BlockedWrites > 0, "the stalled node's engine must still be holding a write when it learned the successor");

            // While the write is still pending the deposed leader must not win the term back, however many
            // election timeouts pass: its log is the freshest, so only the candidacy gate keeps it out.
            for (int i = 0; i < 20; i++)
            {
                Assert.False(await leader.AmILeaderQuick(Partition), $"the stalled node regained leadership at check {i}");
                await Task.Delay(50, ct);
            }

            // The cluster serves writes on the successor meanwhile.
            RaftReplicationResult during = await successor.ReplicateLogs(Partition, "Greeting", "after-failover"u8.ToArray(), cancellationToken: ct);
            Assert.True(during.Success, $"replication on the successor failed: {during.Status}");

            // The disk heals: the stalled write completes late, in a term the cluster has left behind. It must be
            // discarded or truncated, never duplicated, and every replica must end with one agreed log.
            leaderWal.Release();

            RaftReplicationResult healed = await successor.ReplicateLogs(Partition, "Greeting", "healed"u8.ToArray(), cancellationToken: ct);
            Assert.True(healed.Success, $"replication after the heal failed: {healed.Status}");

            long expectedMax = successor.WalAdapter.GetMaxLog(Partition);
            await WaitUntilAsync(
                () => nodes.All(n => n.WalAdapter.GetMaxLog(Partition) == expectedMax),
                timeoutMs: 15_000,
                what: $"every replica must converge on max log {expectedMax}; got [{string.Join(",", nodes.Select(n => n.WalAdapter.GetMaxLog(Partition)))}]",
                ct);

            Dictionary<long, long> successorTerms = successor.WalAdapter.ReadLogs(Partition).ToDictionary(l => l.Id, l => l.Term);

            foreach (IRaft node in nodes)
            {
                List<RaftLog> logs = node.WalAdapter.ReadLogs(Partition);

                Assert.Equal(logs.Count, logs.Select(l => l.Id).Distinct().Count());

                foreach (RaftLog log in logs)
                {
                    if (successorTerms.TryGetValue(log.Id, out long term))
                        Assert.True(term == log.Term, $"entry {log.Id} carries term {log.Term} on a replica but {term} on the successor");
                }
            }

            Assert.Equal(0, leaderWal.BlockedWrites);
        }
        finally
        {
            // A leave replicates membership through the same (node-wide) WAL: open the gate first.
            foreach (GatedWal wal in wals.Values)
                wal.Release();

            await LeaveAllAsync(nodes);
            foreach (GatedWal wal in wals.Values)
                wal.Dispose();
        }
    }

    [Fact]
    public async Task WatchdogDisabled_StalledLeaderKeepsLeadership()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (IRaft[] nodes, Dictionary<IRaft, GatedWal> wals) = await AssembleAsync(
            new TempFileLogger<IRaft>(), ct,
            configure: config => config.WalStallStepDownTimeout = TimeSpan.Zero);

        try
        {
            IRaft leader = await WaitForLeaderAsync(nodes, ct);
            GatedWal leaderWal = wals[leader];

            leaderWal.Stall();
            Task<RaftReplicationResult> stalledProposal = leader.ReplicateLogs(Partition, "Greeting", "during"u8.ToArray(), cancellationToken: ct);

            // The pre-watchdog behaviour, kept reachable by configuration: heartbeats keep flowing, so the
            // stalled leader holds the partition for as long as the stall lasts.
            for (int i = 0; i < 40; i++)
            {
                Assert.True(await leader.AmILeaderQuick(Partition), $"with the watchdog disabled the stalled leader must keep leadership (check {i})");
                await Task.Delay(50, ct);
            }

            Assert.False(stalledProposal.IsCompleted, "nothing releases the caller while the leader keeps leadership and the write is pending");

            leaderWal.Release();

            RaftReplicationResult result = await stalledProposal.WaitAsync(TimeSpan.FromSeconds(10), ct);
            Assert.True(result.Success, $"the write completes once the disk answers: {result.Status}");
        }
        finally
        {
            foreach (GatedWal wal in wals.Values)
                wal.Release();

            await LeaveAllAsync(nodes);
            foreach (GatedWal wal in wals.Values)
                wal.Dispose();
        }
    }
}
