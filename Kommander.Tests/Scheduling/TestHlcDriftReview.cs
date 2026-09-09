using System.Collections.Concurrent;
using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.Gossip;
using Kommander.Scheduling;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL;
using Kommander.WAL.IO;
using Kommander.WAL.Data;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.Scheduling;

/// <summary>Exercises clock skew through production clock, partition, and recovery entry points.</summary>
[Collection(ClusterIntegrationCollection.Name)]
public class TestHlcDriftReview
{
    private const string VoterA = "follower-a:9001";
    private const string VoterB = "follower-b:9002";

    [Theory]
    [InlineData(0)]
    [InlineData(3_600_000)]
    public async Task LostProposal_RetriesAfterMonotonicHeartbeatInterval(int skew)
    {
        CapturingHost host = new();
        ProposeWal wal = new();
        RaftPartitionStateMachine sm = new(host, wal, new NoopSink(), NullLogger<IRaft>.Instance);
        sm.SetPostToExecutor(_ => { });
        sm.SetLeaderForTesting(1);
        host.Physical += skew;
        (_, HLCTimestamp ticket) = sm.ReplicateLogs([new RaftLog { LogType = "review", LogData = [1] }], true);
        host.Physical -= skew;
        await sm.CompleteWalOperationAsync(new(host.PartitionId, 1L, -1L, -1L, 1L,
            WALWriteOperationType.LeaderPropose, RaftOperationStatus.Success));
        Assert.Equal(2, host.Requests.Count(r => r.AppendLogsRequest?.Logs is { Count: > 0 }));
        host.Requests.Clear();
        host.Advance(2000);
        await sm.CheckPartitionLeadershipAsync();
        Assert.Contains(host.Requests, r => r.AppendLogsRequest is { Logs.Count: > 0 } append && append.Time == ticket);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(3_600_000)]
    public async Task FormerLeader_CanCampaignAfterNewLeaderFails(int skew)
    {
        CapturingHost host = new();
        RaftPartitionStateMachine sm = new(host, new ProposeWal(), new NoopSink(), NullLogger<IRaft>.Instance);
        sm.SetPostToExecutor(_ => { });
        await sm.CompleteRestoreAsync(await sm.StartRestoreAsync());
        sm.SetLeaderForTesting(1);
        host.Physical += skew;
        HLCTimestamp sent = host.HybridLogicalClock.SendOrLocalEvent(1);
        await sm.CompleteAppendLogsAsync(VoterA, sent, RaftOperationStatus.Success, 0, 1);
        Assert.NotEqual(HLCTimestamp.Zero, host.GetLastNodeActivity(VoterA, 1));
        host.Physical -= skew;
        await sm.AppendLogsAsync(VoterA, 2, sent, null);
        host.Requests.Clear();
        for (int i = 0; i < 5; i++)
        {
            host.Advance(10000);
            await sm.CheckPartitionLeadershipAsync();
        }
        Assert.Contains(host.Requests, r => r.Type == RaftResponderRequestType.RequestVotes);
    }

    [Fact]
    public void FuturePeerReport_DoesNotRemainFreshAfterAnHourWithoutUpdates()
    {
        const long physical = 1_800_000_000_000;
        NodeLoadReport report = new() { Endpoint = VoterA, ReportVersion = 1,
            Time = new(2, physical + 3_600_000, 0) };
        var view = GlobalLeadershipView.Build([report],
            [new ClusterMember { Endpoint = VoterA, Role = ClusterMemberRole.Voter }],
            new HashSet<string> { VoterA }, TimeSpan.FromSeconds(20), new(1, physical + 3_600_000, 0));
        Assert.Equal(0, view.FreshReportCount);
    }

    [Theory]
    [InlineData("memory")]
    [InlineData("sqlite")]
    [InlineData("rocksdb")]
    public async Task Restore_NewProposalOrdersAfterDurableFutureEntry(string backend)
    {
        string path = Path.Combine(Path.GetTempPath(), "kommander-hlc-review-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(path);
        IWAL CreateWal() => backend switch {
            "sqlite" => new SqliteWAL(path, "wal", NullLogger<IRaft>.Instance),
            "rocksdb" => new RocksDbWAL(path, "wal", NullLogger<IRaft>.Instance),
            _ => new InMemoryWAL(NullLogger<IRaft>.Instance)
        };
        IWAL wal = CreateWal();
        bool ownsWal = true;
        try
        {
            CapturingHost host = new() { Nodes = [] };
            HLCTimestamp durable = new(1, host.Physical + 3_600_000, 17);
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(1, [new RaftLog {
                Id = 1, Term = 1, Type = RaftLogType.Committed, Time = durable, LogType = "review", LogData = [1]
            }])]));
            if (backend != "memory")
            {
                wal.Dispose();
                wal = CreateWal();
            }
            using RaftManager manager = new(new RaftConfiguration { Host = "localhost", Port = 9000, InitialPartitions = 0 },
                new StaticDiscovery([]), wal, new InMemoryCommunication(), host.HybridLogicalClock, NullLogger<IRaft>.Instance);
            ownsWal = false;
            ((FairReadScheduler)manager.ReadScheduler).Start();
            ((FairWalScheduler)manager.WalScheduler).Start();
            using RaftPartition partition = new(manager, wal, 1, 0, 0, NullLogger<IRaft>.Instance);
            RaftWriteAhead writeAhead = new(manager, _ => { }, partition, wal);
            RaftPartitionStateMachine sm = new(host, new RaftWalFacadeAdapter(writeAhead), new NoopSink(), NullLogger<IRaft>.Instance);
            HLCTimestamp restored = default;
            manager.OnLogRestored += (_, log) => { restored = log.Time; return Task.FromResult(true); };
            await sm.CompleteRestoreAsync(await sm.StartRestoreAsync());
            Assert.Equal(durable, restored);
            sm.SetPostToExecutor(_ => { });
            await sm.ForceLeaderForTestingAsync(null);
            Assert.Equal(host.LocalEndpoint, host.Leader);
            (_, HLCTimestamp ticket) = sm.ReplicateLogs([new RaftLog { LogType = "review", LogData = [2] }], true);
            Assert.True(ticket > durable, $"New proposal {ticket} predates restored {durable}");
        }
        finally
        {
            if (ownsWal) wal.Dispose();
            Directory.Delete(path, true);
        }
    }

    [Fact]
    public async Task MixedSendReceive_WithRollbackAndCounterRollover_RemainsUnique()
    {
        long physical = 1_800_000_000_000;
        using HybridLogicalClock clock = new(() => Volatile.Read(ref physical));
        clock.ReceiveEvent(1, new HLClockMessage(physical + 3_600_000, uint.MaxValue));
        Volatile.Write(ref physical, physical - 3_600_000);
        ConcurrentBag<HLCTimestamp> stamps = [];
        await Task.WhenAll(Enumerable.Range(0, 8).Select(worker => Task.Run(() => {
            HLCTimestamp last = default;
            for (int i = 0; i < 20000; i++)
            {
                HLCTimestamp ts = worker % 2 == 0 ? clock.SendOrLocalEvent(1)
                    : clock.ReceiveEvent(1, new HLClockMessage(1_800_000_000_000, uint.MaxValue));
                Assert.True(ts > last);
                last = ts;
                stamps.Add(ts);
            }
        })));
        Assert.Equal(160000, stamps.Distinct().Count());
    }
    private sealed class ProposeWal : IRaftWalFacade
    {
        public List<RaftLog> Committed { get; } = [];
        private long _nextId = 1;

        public ValueTask<IReadOnlyList<RaftLog>> LoadRestoreLogsAsync() =>
            ValueTask.FromResult<IReadOnlyList<RaftLog>>([]);
        public ValueTask CompleteRestoreAsync(IReadOnlyList<RaftLog> logs) => ValueTask.CompletedTask;
        public ValueTask<long> GetMaxLogAsync() => ValueTask.FromResult(_nextId - 1);
        public ValueTask<long> TruncateLogsAfterAsync(long afterLogId) => ValueTask.FromResult(afterLogId);
        public ValueTask<long> GetCurrentTermAsync() => ValueTask.FromResult(1L);
        public ValueTask<List<RaftLog>> GetRangeAsync(long start, int max) =>
            ValueTask.FromResult(Committed.Where(l => l.Id >= start).Take(max).ToList());
        public ValueTask<long> GetAnyTermAtAsync(long logIndex) => ValueTask.FromResult(1L);
        public ValueTask<long> GetLastCheckpointAsync() => ValueTask.FromResult(0L);
        public long GetCommitIndex() => 0;

        public WALWriteOperation EnqueuePropose(long term, List<RaftLog> logs, HLCTimestamp ts, bool autoCommit)
        {
            foreach (RaftLog log in logs)
            {
                log.Id = _nextId++;
                log.Term = term;
            }
            long maxId = logs.Count > 0 ? logs.Max(l => l.Id) : 0;
            return new(null!, 1L, WALWriteOperationType.LeaderPropose, (1, logs), ts, autoCommit: autoCommit, term: term, logIndex: maxId);
        }

        public WALWriteOperation EnqueueCommit(List<RaftLog> logs)
        {
            foreach (RaftLog log in logs)
            {
                if (log.Type == RaftLogType.Proposed)
                    log.Type = RaftLogType.Committed;
            }
            Committed.AddRange(logs);
            long maxId = logs.Count > 0 ? logs.Max(l => l.Id) : 0;
            return new(null!, 2L, WALWriteOperationType.LeaderCommit, (1, logs), logIndex: maxId);
        }

        public WALWriteOperation EnqueueRollback(List<RaftLog> logs) =>
            new(_ => { }, 3L, WALWriteOperationType.LeaderRollback, (1, logs));
        public WALWriteOperation? EnqueueProposeOrCommit(List<RaftLog>? logs, HLCTimestamp t = default, string? ep = null, long term = -1) =>
            logs is null ? null : EnqueuePropose(term, logs, t, autoCommit: false);
        public void NotifyCommitted() { }
    }

    private sealed class CapturingHost : IRaftPartitionHost
    {
        public ConcurrentBag<RaftResponderRequest> Requests { get; } = [];

        public int PartitionId => 1;
        public string Leader { get; set; } = "";
        public string LocalEndpoint => "leader:9000";
        public int LocalNodeId => 1;
        public ClusterMemberRole LocalRole => ClusterMemberRole.Voter;
        public bool IsVoter(string endpoint) => true;

        public RaftConfiguration Configuration { get; } = new()
        {
            Host = "leader", Port = 9000, InitialPartitions = 1, BackfillThreshold = 10,
            HeartbeatInterval = TimeSpan.FromMilliseconds(100), RecentHeartbeat = TimeSpan.Zero, EnableQuiescence = false,
        };

        public long Physical = 1_800_000_000_000;
        public long Ticks = 1_000_000_000;
        public HybridLogicalClock HybridLogicalClock { get; }
        private readonly NodeActivityTracker activity;
        public CapturingHost()
        {
            HybridLogicalClock = new(() => Physical);
            activity = new(() => HybridLogicalClock.SendOrLocalEvent(1), LocalEndpoint);
        }
        public long GetMonotonicTimestamp() => Ticks;
        public void Advance(int milliseconds)
        {
            Physical += milliseconds;
            Ticks += (long)(milliseconds / 1000.0 * global::System.Diagnostics.Stopwatch.Frequency);
        }
        public IReadOnlyList<RaftNode> Nodes { get; set; } = [new(VoterA), new(VoterB)];
        public MemberLivenessState GetNodeLiveness(string endpoint) => MemberLivenessState.Alive;

        public HLCTimestamp GetLastNodeActivity(string e, int p) => activity.GetLastNodeActivity(e, p);
        public void UpdateLastNodeActivity(string e, int p, HLCTimestamp t) => activity.UpdateLastNodeActivity(e, p, t);
        public void EnqueueResponse(string e, RaftResponderRequest r) => Requests.Add(r);
        public Task InvokeLeaderChanged(int p, string l) => Task.CompletedTask;
        public Task<bool> InvokeReplicationReceived(int p, RaftLog l) => Task.FromResult(true);
        public Task<bool> InvokeSystemReplicationReceived(int p, RaftLog l) => Task.FromResult(true);
        public void InvokeReplicationError(int p, RaftLog l) { }

        public IRaftStateMachineTransfer? StateMachineTransfer => null;
        public IRaftSystemStateTransfer? SystemStateTransfer => null;

        public Task<SnapshotResponse> SendInstallSnapshotAsync(RaftNode node, SnapshotRequest request, CancellationToken ct) =>
            Task.FromResult(new SnapshotResponse(true));
    }

    private sealed class NoopSink : IRaftOperationReplySink
    {
        public void TryComplete(ulong correlationId, RaftResponse response) { }
    }
}
