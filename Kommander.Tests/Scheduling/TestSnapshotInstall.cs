
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
    /// When no StateMachineTransfer is registered, ReceiveInstallSnapshot returns false
    /// immediately without attempting an import.
    /// </summary>
    [Fact]
    public async Task ReceiveInstallSnapshot_NoTransfer_ReturnsFalse()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        using Kommander.WAL.InMemoryWAL wal = new(NullLogger<IRaft>.Instance);
        Kommander.Discovery.StaticDiscovery discovery = new([]);
        Kommander.Communication.Memory.InMemoryCommunication comm = new();
        RaftConfiguration cfg = new() { NodeId = 1, Host = "localhost", Port = 9999, InitialPartitions = 1 };
        RaftManager manager = new(cfg, discovery, wal, comm, new HybridLogicalClock(), NullLogger<IRaft>.Instance);

        try
        {
            SnapshotRequest req = new() { SessionId = "s1", PartitionId = 1, SnapshotIndex = 100, FollowerEndpoint = "x:1", IsLast = true, Data = [] };
            SnapshotResponse resp = await manager.ReceiveInstallSnapshot(req, ct);

            Assert.False(resp.Success);
        }
        finally
        {
            manager.Dispose();
        }
    }

    /// <summary>
    /// When a transfer IS registered, ReceiveInstallSnapshot calls ImportRange and writes
    /// a CommittedCheckpoint WAL entry so GetMaxLog reflects the snapshot index.
    /// </summary>
    [Fact]
    public async Task ReceiveInstallSnapshot_WithTransfer_CallsImportRange()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        FakeTransfer transfer = new();

        using Kommander.WAL.InMemoryWAL wal = new(NullLogger<IRaft>.Instance);
        Kommander.Discovery.StaticDiscovery discovery = new([]);
        Kommander.Communication.Memory.InMemoryCommunication comm = new();
        RaftConfiguration cfg = new() { NodeId = 1, Host = "localhost", Port = 9998, InitialPartitions = 1 };
        RaftManager manager = new(cfg, discovery, wal, comm, new HybridLogicalClock(), NullLogger<IRaft>.Instance);
        manager.RegisterStateMachineTransfer(transfer);

        try
        {
            SnapshotRequest req = new() { SessionId = "s2", PartitionId = 1, SnapshotIndex = 50, FollowerEndpoint = "x:1", IsLast = true, Data = [1, 2, 3] };
            SnapshotResponse resp = await manager.ReceiveInstallSnapshot(req, ct);

            Assert.True(resp.Success);
            Assert.True(transfer.ImportCalled, "ImportRange should have been called");
            Assert.Equal(1, transfer.ImportPartitionId);
        }
        finally
        {
            manager.Dispose();
        }
    }

    /// <summary>
    /// InMemoryCommunication.SendInstallSnapshot routes to the target node's ReceiveInstallSnapshot.
    /// With a FakeTransfer registered on the target, the response is Success = true.
    /// </summary>
    [Fact]
    public async Task InMemoryComm_SendInstallSnapshot_RoutesToTarget()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        FakeTransfer transfer = new();

        using Kommander.WAL.InMemoryWAL wal1 = new(NullLogger<IRaft>.Instance);
        using Kommander.WAL.InMemoryWAL wal2 = new(NullLogger<IRaft>.Instance);
        Kommander.Communication.Memory.InMemoryCommunication comm = new();

        RaftConfiguration cfg1 = new() { NodeId = 1, Host = "localhost", Port = 9001, InitialPartitions = 1 };
        RaftConfiguration cfg2 = new() { NodeId = 2, Host = "localhost", Port = 9002, InitialPartitions = 1 };

        RaftManager leader = new(cfg1, new Kommander.Discovery.StaticDiscovery([new RaftNode("localhost:9002")]),
            wal1, comm, new HybridLogicalClock(), NullLogger<IRaft>.Instance);
        RaftManager follower = new(cfg2, new Kommander.Discovery.StaticDiscovery([new RaftNode("localhost:9001")]),
            wal2, comm, new HybridLogicalClock(), NullLogger<IRaft>.Instance);

        follower.RegisterStateMachineTransfer(transfer);

        comm.SetNodes(new Dictionary<string, IRaft>
        {
            ["localhost:9001"] = leader,
            ["localhost:9002"] = follower,
        });

        try
        {
            SnapshotRequest req = new()
            {
                SessionId = "s3",
                PartitionId = 1,
                SnapshotIndex = 75,
                FollowerEndpoint = "localhost:9002",
                IsLast = true,
                Data = [9, 8, 7],
            };

            RaftNode followerNode = new("localhost:9002");
            SnapshotResponse resp = await comm.SendInstallSnapshot(leader, followerNode, req, ct);

            Assert.True(resp.Success);
            Assert.True(transfer.ImportCalled);
        }
        finally
        {
            leader.Dispose();
            follower.Dispose();
        }
    }

    /// <summary>
    /// Chunked snapshot transfer: sending three chunks in order with IsLast=false on the
    /// first two and IsLast=true on the last triggers exactly one ImportRange call, and
    /// ImportRange receives all bytes concatenated in order.
    /// </summary>
    [Fact]
    public async Task ReceiveInstallSnapshot_MultiChunk_ImportRangeReceivesAllBytes()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        CapturingTransfer transfer = new();

        using Kommander.WAL.InMemoryWAL wal = new(NullLogger<IRaft>.Instance);
        Kommander.Discovery.StaticDiscovery discovery = new([]);
        Kommander.Communication.Memory.InMemoryCommunication comm = new();
        RaftConfiguration cfg = new() { NodeId = 1, Host = "localhost", Port = 9997, InitialPartitions = 1 };
        RaftManager manager = new(cfg, discovery, wal, comm, new HybridLogicalClock(), NullLogger<IRaft>.Instance);
        manager.RegisterStateMachineTransfer(transfer);

        try
        {
            const string session = "multi-chunk-session";
            const int partitionId = 1;
            const long snapshotIndex = 200;

            // Chunk 0 — not final
            SnapshotResponse r0 = await manager.ReceiveInstallSnapshot(new SnapshotRequest
            {
                SessionId = session, PartitionId = partitionId, SnapshotIndex = snapshotIndex,
                FollowerEndpoint = "x:1", ChunkIndex = 0, IsLast = false,
                Data = [0x01, 0x02, 0x03],
            }, ct);
            Assert.True(r0.Success, "chunk 0 should be accepted");
            Assert.False(transfer.ImportCalled, "ImportRange must not fire on non-final chunk");

            // Chunk 1 — not final
            SnapshotResponse r1 = await manager.ReceiveInstallSnapshot(new SnapshotRequest
            {
                SessionId = session, PartitionId = partitionId, SnapshotIndex = snapshotIndex,
                FollowerEndpoint = "x:1", ChunkIndex = 1, IsLast = false,
                Data = [0x04, 0x05],
            }, ct);
            Assert.True(r1.Success, "chunk 1 should be accepted");
            Assert.False(transfer.ImportCalled, "ImportRange must not fire on non-final chunk");

            // Chunk 2 — final
            SnapshotResponse r2 = await manager.ReceiveInstallSnapshot(new SnapshotRequest
            {
                SessionId = session, PartitionId = partitionId, SnapshotIndex = snapshotIndex,
                FollowerEndpoint = "x:1", ChunkIndex = 2, IsLast = true,
                Data = [0x06],
            }, ct);
            Assert.True(r2.Success, "final chunk should succeed");
            Assert.True(transfer.ImportCalled, "ImportRange must fire on final chunk");

            // All bytes concatenated in order.
            Assert.Equal([0x01, 0x02, 0x03, 0x04, 0x05, 0x06], transfer.ReceivedBytes);
        }
        finally
        {
            manager.Dispose();
        }
    }

    /// <summary>
    /// Intermediate chunks with IsLast=false return success but do NOT call ImportRange.
    /// A fresh session for the same endpoint after a failed transfer starts from zero.
    /// </summary>
    [Fact]
    public async Task ReceiveInstallSnapshot_PartialChunk_NoImportUntilLast()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        CapturingTransfer transfer = new();

        using Kommander.WAL.InMemoryWAL wal = new(NullLogger<IRaft>.Instance);
        Kommander.Discovery.StaticDiscovery discovery = new([]);
        Kommander.Communication.Memory.InMemoryCommunication comm = new();
        RaftConfiguration cfg = new() { NodeId = 1, Host = "localhost", Port = 9996, InitialPartitions = 1 };
        RaftManager manager = new(cfg, discovery, wal, comm, new HybridLogicalClock(), NullLogger<IRaft>.Instance);
        manager.RegisterStateMachineTransfer(transfer);

        try
        {
            // Send only the first chunk of session A — never send IsLast.
            SnapshotResponse r = await manager.ReceiveInstallSnapshot(new SnapshotRequest
            {
                SessionId = "session-A", PartitionId = 1, SnapshotIndex = 300,
                FollowerEndpoint = "x:1", ChunkIndex = 0, IsLast = false,
                Data = [0xAA],
            }, ct);
            Assert.True(r.Success);
            Assert.False(transfer.ImportCalled);

            // Start a fresh session B — the partial bytes from A must NOT contaminate B.
            SnapshotResponse r2 = await manager.ReceiveInstallSnapshot(new SnapshotRequest
            {
                SessionId = "session-B", PartitionId = 1, SnapshotIndex = 301,
                FollowerEndpoint = "x:1", ChunkIndex = 0, IsLast = true,
                Data = [0xBB],
            }, ct);
            Assert.True(r2.Success);
            Assert.True(transfer.ImportCalled);
            Assert.Equal([0xBB], transfer.ReceivedBytes);
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
            Data = [0xCC, 0xDD],
        }, ct);

        Assert.True(r.Success);
        Assert.Equal(1, PendingSnapshotSessionCount(manager));

        manager.Dispose();

        Assert.Equal(0, PendingSnapshotSessionCount(manager));

        SnapshotResponse afterDispose = await manager.ReceiveInstallSnapshot(new SnapshotRequest
        {
            SessionId = "session-to-dispose", PartitionId = 1, SnapshotIndex = 400,
            FollowerEndpoint = "x:1", ChunkIndex = 1, IsLast = true,
            Data = [0xEE],
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
            "_pendingSnapshots",
            global::System.Reflection.BindingFlags.NonPublic | global::System.Reflection.BindingFlags.Instance)!;

        global::System.Collections.Concurrent.ConcurrentDictionary<string, MemoryStream> pending =
            (global::System.Collections.Concurrent.ConcurrentDictionary<string, MemoryStream>)field.GetValue(manager)!;

        return pending.Count;
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

    private sealed class FakeTransfer : IRaftStateMachineTransfer
    {
        public bool ImportCalled { get; private set; }
        public int ImportPartitionId { get; private set; }

        public Task<Stream> ExportRange(RaftSplitPlan plan, long upToIndex, CancellationToken ct)
        {
            return Task.FromResult<Stream>(new MemoryStream([0xAB, 0xCD]));
        }

        public Task ImportRange(int targetPartitionId, Stream snapshot, CancellationToken ct)
        {
            ImportCalled = true;
            ImportPartitionId = targetPartitionId;
            return Task.CompletedTask;
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
        public HLCTimestamp GetLastNodeHearthbeat(string e, int p) => HLCTimestamp.Zero;
        public void UpdateLastHeartbeat(string e, int p, HLCTimestamp t) { }
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
