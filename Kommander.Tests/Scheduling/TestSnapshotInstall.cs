
using Kommander;
using Kommander.Data;
using Kommander.Diagnostics;
using Kommander.Gossip;
using Kommander.Scheduling;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL.Data;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.Scheduling;

/// <summary>
/// Unit tests for the snapshot catch-up path in <see cref="RaftPartitionStateMachine"/>.
/// Uses a controlled WAL stub that simulates a compaction floor so the in-memory WAL's
/// "always returns -1" limitation does not prevent the path from being exercised.
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
public class TestSnapshotInstall
{
    // ── tests ─────────────────────────────────────────────────────────────────

    /// <summary>
    /// CompleteSnapshotInstalled should advance the internal lastCommitIndex for the
    /// follower endpoint and be idempotent for lower values.
    /// </summary>
    [Fact]
    public void CompleteSnapshotInstalled_AdvancesLastCommitIndex()
    {
        FakePartitionHost host = new(checkpointFloor: 100, transfer: null, comm: null);
        FloorWal wal = new(floor: 100);
        CapturingSink sink = new();
        RaftPartitionStateMachine sm = new(host, wal, sink, NullLogger<IRaft>.Instance);
        sm.SetPostToExecutor(_ => { });

        // Should not throw.
        sm.CompleteSnapshotInstalled("follower:9001", 100);
        sm.CompleteSnapshotInstalled("follower:9001", 50); // lower index — no-op

        Assert.True(true);
    }

    /// <summary>
    /// On a manager with no hosted partition (never joined), the terminal chunk cannot be routed to a
    /// partition executor, so ReceiveInstallSnapshot returns false. (Post-increment-B the receiver no
    /// longer imports directly; the install runs on the executor, which requires a live partition.)
    /// </summary>
    [Fact]
    public async Task ReceiveInstallSnapshot_NoHostedPartition_ReturnsFalse()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        using Kommander.WAL.InMemoryWAL wal = new(NullLogger<IRaft>.Instance);
        Kommander.Discovery.StaticDiscovery discovery = new([]);
        Kommander.Communication.Memory.InMemoryCommunication comm = new();
        RaftConfiguration cfg = new() { NodeId = 1, Host = "localhost", Port = 9999, InitialPartitions = 1 };
        RaftManager manager = new(cfg, discovery, wal, comm, new HybridLogicalClock(), NullLogger<IRaft>.Instance);

        try
        {
            SnapshotRequest req = new() { SessionId = "s1", PartitionId = 1, SnapshotIndex = 100, FollowerEndpoint = "x:1", IsLast = true, Data = ReadOnlyMemory<byte>.Empty };
            SnapshotResponse resp = await manager.ReceiveInstallSnapshot(req, ct);

            Assert.False(resp.Success);
        }
        finally
        {
            manager.Dispose();
        }
    }

    [Fact]
    public async Task Dispose_WithIncompleteSnapshotSession_ReleasesBufferedChunks()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        CapturingTransfer transfer = new();

        using Kommander.WAL.InMemoryWAL wal = new(NullLogger<IRaft>.Instance);
        Kommander.Discovery.StaticDiscovery discovery = new([]);
        Kommander.Communication.Memory.InMemoryCommunication comm = new();
        RaftConfiguration cfg = new() { NodeId = 1, Host = "localhost", Port = 9995, InitialPartitions = 1 };
        RaftManager manager = new(cfg, discovery, wal, comm, new HybridLogicalClock(), NullLogger<IRaft>.Instance);
        manager.RegisterStateMachineTransfer(transfer);

        SnapshotResponse r = await manager.ReceiveInstallSnapshot(new SnapshotRequest
        {
            SessionId = "session-to-dispose", PartitionId = 1, SnapshotIndex = 400,
            FollowerEndpoint = "x:1", ChunkIndex = 0, IsLast = false,
            Data = new byte[] { 0xCC, 0xDD },
        }, ct);

        Assert.True(r.Success);
        Assert.Equal(1, PendingSnapshotSessionCount(manager));

        manager.Dispose();

        Assert.Equal(0, PendingSnapshotSessionCount(manager));

        SnapshotResponse afterDispose = await manager.ReceiveInstallSnapshot(new SnapshotRequest
        {
            SessionId = "session-to-dispose", PartitionId = 1, SnapshotIndex = 400,
            FollowerEndpoint = "x:1", ChunkIndex = 1, IsLast = true,
            Data = new byte[] { 0xEE },
        }, ct);

        Assert.False(afterDispose.Success);
    }

    /// <summary>
    /// SetJoinTerminalReason/GetJoinTerminalReason round-trip: once set, the reason is
    /// retrievable and a second call to SetJoinTerminalReason for the same endpoint
    /// overwrites it (last-write-wins).
    /// </summary>
    [Fact]
    public void TerminalReason_RoundTrip()
    {
        using Kommander.WAL.InMemoryWAL wal = new(NullLogger<IRaft>.Instance);
        Kommander.Discovery.StaticDiscovery discovery = new([]);
        Kommander.Communication.Memory.InMemoryCommunication comm = new();
        RaftConfiguration cfg = new() { NodeId = 1, Host = "localhost", Port = 9990, InitialPartitions = 1 };
        RaftManager manager = new(cfg, discovery, wal, comm, new HybridLogicalClock(), NullLogger<IRaft>.Instance);

        try
        {
            Assert.Null(manager.GetJoinTerminalReason("localhost:9990"));

            manager.SetJoinTerminalReason("localhost:9990", "reason A");
            Assert.Equal("reason A", manager.GetJoinTerminalReason("localhost:9990"));

            // Last-write-wins.
            manager.SetJoinTerminalReason("localhost:9990", "reason B");
            Assert.Equal("reason B", manager.GetJoinTerminalReason("localhost:9990"));

            // Unrelated endpoint is unaffected.
            Assert.Null(manager.GetJoinTerminalReason("localhost:9991"));
        }
        finally
        {
            manager.Dispose();
        }
    }

    /// <summary>
    /// When the P0 coordinator sets a terminal reason for the local endpoint,
    /// JoinCluster(seeds) throws <see cref="InvalidOperationException"/> almost
    /// immediately — well inside the 60 s deadline — rather than timing out.
    ///
    /// The test pre-injects the terminal reason on n4 before calling JoinCluster(seeds).
    /// The reason is only checked in the Voter-promotion wait loop (Phase 3), so Phases
    /// 1 (admitted as Learner) and 2 (IsInitialized) must complete first, which requires
    /// a real running cluster.
    /// </summary>
    [Fact]
    public async Task JoinCluster_TerminalReason_ThrowsImmediately()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        Kommander.Communication.Memory.InMemoryCommunication comm = new();

        // Use a long stable window so n4 is never auto-promoted during the ~500 ms Phase 3
        // first-iteration window. The terminal-reason check fires before the window expires.
        static RaftManager BuildNode(string host, int port, int nodeId,
            string[] peers, Kommander.Communication.Memory.InMemoryCommunication comm,
            int initialPartitions = 1)
        {
            RaftConfiguration cfg = new()
            {
                NodeId = nodeId, Host = host, Port = port,
                InitialPartitions = initialPartitions,
                HeartbeatInterval = TimeSpan.FromMilliseconds(50),
                RecentHeartbeat = TimeSpan.FromMilliseconds(25),
                VotingTimeout = TimeSpan.FromMilliseconds(500),
                CheckLeaderInterval = TimeSpan.FromMilliseconds(25),
                UpdateNodesInterval = TimeSpan.FromMilliseconds(200),
                TimerInitialDelay = TimeSpan.FromMilliseconds(25),
                StartElectionTimeout = 100,
                EnableQuiescence = false,
                EndElectionTimeout = 300,
                BackfillThreshold = 0,
                MaxBackfillEntriesPerRound = 128,
                LearnerPromotionLag = 5,
                LearnerPromotionStableWindow = TimeSpan.FromSeconds(60),
            };
            return new RaftManager(cfg,
                new Kommander.Discovery.StaticDiscovery(peers.Select(e => new RaftNode(e)).ToList()),
                new Kommander.WAL.InMemoryWAL(NullLogger<IRaft>.Instance),
                comm,
                new HybridLogicalClock(),
                NullLogger<IRaft>.Instance);
        }

        RaftManager n1 = BuildNode("localhost", 8310, 1, ["localhost:8311", "localhost:8312"], comm);
        RaftManager n2 = BuildNode("localhost", 8311, 2, ["localhost:8310", "localhost:8312"], comm);
        RaftManager n3 = BuildNode("localhost", 8312, 3, ["localhost:8310", "localhost:8311"], comm);
        // n4 starts with no partitions and seeds pointing at the running cluster.
        RaftManager n4 = BuildNode("localhost", 8313, 4,
            ["localhost:8310", "localhost:8311", "localhost:8312"], comm,
            initialPartitions: 0);

        comm.SetNodes(new Dictionary<string, IRaft>
        {
            ["localhost:8310"] = n1,
            ["localhost:8311"] = n2,
            ["localhost:8312"] = n3,
            ["localhost:8313"] = n4,
        });

        try
        {
            // Phase 0: start the 3-node cluster and wait until initialized.
            await Task.WhenAll(n1.JoinCluster(ct), n2.JoinCluster(ct), n3.JoinCluster(ct));

            ValueStopwatch sw = ValueStopwatch.StartNew();
            while (!(n1.IsInitialized && n2.IsInitialized && n3.IsInitialized))
            {
                ct.ThrowIfCancellationRequested();
                if (sw.GetElapsedMilliseconds() > 15_000)
                    throw new TimeoutException("Cluster did not initialize within 15 s.");
                await Task.Delay(50, ct);
            }

            // Pre-inject the terminal reason on n4 before it joins. JoinCluster(seeds)
            // checks this only in the Voter-promotion loop (after Phases 1 and 2 succeed),
            // so it will throw InvalidOperationException the first time it polls.
            n4.SetJoinTerminalReason("localhost:8313",
                "test: no IRaftStateMachineTransfer registered; learner below WAL floor");

            InvalidOperationException ex = await Assert.ThrowsAsync<InvalidOperationException>(
                () => n4.JoinCluster(["localhost:8310"], ct));

            Assert.Contains("permanently blocked", ex.Message);
            Assert.Contains("no IRaftStateMachineTransfer", ex.Message);
        }
        finally
        {
            n1.Dispose();
            n2.Dispose();
            n3.Dispose();
            n4.Dispose();
        }
    }

    // ── stubs ──────────────────────────────────────────────────────────────────

    private static int PendingSnapshotSessionCount(RaftManager manager)
    {
        global::System.Reflection.FieldInfo field = typeof(RaftManager).GetField(
            "snapshotReceiver",
            global::System.Reflection.BindingFlags.NonPublic | global::System.Reflection.BindingFlags.Instance)!;

        SnapshotReceiver receiver = (SnapshotReceiver)field.GetValue(manager)!;

        return receiver.PendingSessionCount;
    }

    /// <summary>Captures all bytes passed to ImportRange for chunk-order assertions.</summary>
    private sealed class CapturingTransfer : IRaftStateMachineTransfer
    {
        public bool ImportCalled { get; private set; }
        public byte[] ReceivedBytes { get; private set; } = [];

        public Task<Stream> ExportRange(RaftSplitPlan plan, long upToIndex, CancellationToken ct) =>
            Task.FromResult<Stream>(new MemoryStream([0x11, 0x22]));

        public async Task ImportRange(int targetPartitionId, Stream snapshot, CancellationToken ct)
        {
            ImportCalled = true;
            using MemoryStream ms = new();
            await snapshot.CopyToAsync(ms, ct);
            ReceivedBytes = ms.ToArray();
        }
    }

    private sealed class CapturingComm
    {
        public List<SnapshotRequest> Requests { get; } = [];

        public Task<SnapshotResponse> SendInstallSnapshot(RaftManager _, RaftNode __, SnapshotRequest r, CancellationToken ___)
        {
            Requests.Add(r);
            return Task.FromResult(new SnapshotResponse(true));
        }
    }

    private sealed class FakePartitionHost : IRaftPartitionHost
    {
        private readonly long checkpointFloor;
        private readonly IRaftStateMachineTransfer? transfer;
        private readonly CapturingComm? comm;

        public FakePartitionHost(long checkpointFloor, IRaftStateMachineTransfer? transfer, CapturingComm? comm)
        {
            this.checkpointFloor = checkpointFloor;
            this.transfer = transfer;
            this.comm = comm;
        }

        public int PartitionId => 1;
        public string Leader { get => ""; set { } }
        public string LocalEndpoint => "leader:9000";
        public int LocalNodeId => 1;
        public ClusterMemberRole LocalRole => ClusterMemberRole.Voter;
        public bool IsVoter(string endpoint) => true;
        public RaftConfiguration Configuration { get; } = new() { NodeId = 1, Host = "leader", Port = 9000, InitialPartitions = 1 };
        public HybridLogicalClock HybridLogicalClock { get; } = new();
        public IReadOnlyList<RaftNode> Nodes => [];

        public HLCTimestamp GetLastNodeActivity(string e, int p) => HLCTimestamp.Zero;
        public void UpdateLastNodeActivity(string e, int p, HLCTimestamp t) { }
        public void EnqueueResponse(string e, RaftResponderRequest r) { }
        public Task InvokeLeaderChanged(int p, string l) => Task.CompletedTask;
        public Task<bool> InvokeReplicationReceived(int p, RaftLog l) => Task.FromResult(true);
        public Task<bool> InvokeSystemReplicationReceived(int p, RaftLog l) => Task.FromResult(true);
        public void InvokeReplicationError(int p, RaftLog l) { }

        public IRaftStateMachineTransfer? StateMachineTransfer => transfer;

        public IRaftSystemStateTransfer? SystemStateTransfer => null;

        public Task<SnapshotResponse> SendInstallSnapshotAsync(RaftNode node, SnapshotRequest request, CancellationToken ct) =>
            comm?.SendInstallSnapshot(null!, node, request, ct) ?? Task.FromResult(new SnapshotResponse(false));

        public MemberLivenessState GetNodeLiveness(string endpoint) => MemberLivenessState.Alive;
    }

    private sealed class FloorWal : IRaftWalFacade
    {
        private readonly long floor;
        public FloorWal(long floor) => this.floor = floor;

        public ValueTask<IReadOnlyList<RaftLog>> LoadRestoreLogsAsync() =>
            ValueTask.FromResult<IReadOnlyList<RaftLog>>([]);
        public ValueTask CompleteRestoreAsync(IReadOnlyList<RaftLog> logs) => ValueTask.CompletedTask;
        public ValueTask<long> GetMaxLogAsync() => ValueTask.FromResult(0L);
        public ValueTask<long> TruncateLogsAfterAsync(long afterLogId) => ValueTask.FromResult(afterLogId);
        public ValueTask<long> GetCurrentTermAsync() => ValueTask.FromResult(1L);
        public ValueTask<List<RaftLog>> GetRangeAsync(long startLogIndex, int maxEntries) =>
            ValueTask.FromResult(new List<RaftLog>());
        public ValueTask<long> GetAnyTermAtAsync(long logIndex) => ValueTask.FromResult(-1L);
        public ValueTask<long> GetLastCheckpointAsync() => ValueTask.FromResult(floor);
        public long GetCommitIndex() => 0;
        public WALWriteOperation EnqueuePropose(long term, List<RaftLog> logs, HLCTimestamp ts, bool autoCommit) =>
            MakeNoOp();
        public WALWriteOperation EnqueueCommit(List<RaftLog> logs) => MakeNoOp();
        public WALWriteOperation EnqueueRollback(List<RaftLog> logs) => MakeNoOp();
        public WALWriteOperation? EnqueueProposeOrCommit(List<RaftLog>? logs, HLCTimestamp timestamp = default, string? endpoint = null, long term = -1) =>
            logs is null ? null : MakeNoOp();
        public void NotifyCommitted() { }

        private static WALWriteOperation MakeNoOp() =>
            new(_ => { }, 0, WALWriteOperationType.LeaderPropose, (0, []));
    }

    private sealed class CapturingSink : IRaftOperationReplySink
    {
        public void TryComplete(ulong correlationId, RaftResponse response) { }
    }
}
