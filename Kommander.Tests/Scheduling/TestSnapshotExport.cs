
using Kommander;
using Kommander.Data;
using Kommander.Gossip;
using Kommander.Scheduling;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL.Data;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.Scheduling;

/// <summary>
/// Tests the leader-side snapshot export path:
/// - When the partition is P0 and a <see cref="IRaftSystemStateTransfer"/> is registered,
///   <c>TrySendSnapshotAsync</c> calls <see cref="IRaftSystemStateTransfer.ExportPartitionState"/>
///   and stamps every chunk with <see cref="SnapshotKind.SystemState"/>.
/// - When only a <see cref="IRaftStateMachineTransfer"/> is registered (or the partition is not
///   P0), the existing <see cref="IRaftStateMachineTransfer.ExportRange"/> path is used and
///   chunks carry <see cref="SnapshotKind.Range"/>.
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
public class TestSnapshotExport
{
    // ── helpers ───────────────────────────────────────────────────────────────

    /// <summary>
    /// Drives the state machine to Leader with the given follower node, seeds lastCommitIndexes
    /// for that follower via ReceivedVoteAsync, then fires CheckPartitionLeadershipAsync so the
    /// backfill path runs (GetRangeAsync returns [] → floor exceeded → snapshot triggered).
    /// Returns all SnapshotRequest chunks captured by <paramref name="comm"/>.
    /// </summary>
    private static async Task<List<SnapshotRequest>> DriveSnapshotAsync(
        RaftPartitionStateMachine sm,
        string followerEndpoint,
        CapturingComm comm)
    {
        // Start an election as Candidate, then grant quorum so the node becomes Leader.
        // ReceivedVoteAsync seeds lastCommitIndexes[followerEndpoint] = 0 and, once quorum
        // is reached, calls BecomeLeader() → SendHeartbeat(true), which triggers the snapshot.
        await sm.ForceLeaderForTestingAsync(replyCorrelationId: null);
        Assert.Equal(RaftNodeState.Candidate, sm.NodeState);

        // Grant the deciding vote. currentTerm was incremented to 1 by ForceLeader; voteTerm=1 matches.
        // ReceivedVoteAsync reaches quorum → BecomeLeader → SendHeartbeat(true) → TrySendSnapshotAsync.
        await sm.ReceivedVoteAsync(followerEndpoint, voteTerm: 1, remoteMaxLogId: 0);
        Assert.Equal(RaftNodeState.Leader, sm.NodeState);

        // TrySendSnapshotAsync is fire-and-forget; wait for the capturing comm to capture.
        List<SnapshotRequest> chunks = await comm.WaitForChunksAsync(TimeSpan.FromSeconds(5));
        return chunks;
    }

    // ── P0 with SystemStateTransfer → ExportPartitionState + Kind=SystemState ──

    [Fact]
    public async Task P0_WithSystemTransfer_CallsExportPartitionState()
    {
        CapturingSystemTransfer systemTransfer = new();
        CapturingComm comm = new();

        FakeSnapshotHost host = new(partitionId: 0, systemTransfer: systemTransfer, rangeTransfer: null, comm: comm);
        FloorWal wal = new(floor: 100, commitIndex: 100);
        RaftPartitionStateMachine sm = new(host, wal, new CapturingSink(), NullLogger<IRaft>.Instance);
        sm.SetPostToExecutor(_ => { });

        List<SnapshotRequest> chunks = await DriveSnapshotAsync(sm, "follower:9000", comm);

        Assert.NotEmpty(chunks);
        Assert.All(chunks, c => Assert.Equal(SnapshotKind.SystemState, c.Kind));
        Assert.True(systemTransfer.ExportCalled, "ExportPartitionState must be called");
        Assert.Equal(0, systemTransfer.ExportedPartitionId);
        Assert.Equal(100L, systemTransfer.ExportedUpToIndex);
    }

    [Fact]
    public async Task P0_WithSystemTransfer_ChunksCarrySystemStateKind()
    {
        // Multi-chunk: export returns 7 MB so three 3 MB chunks are sent.
        byte[] payload = new byte[7 * 1024 * 1024];
        CapturingSystemTransfer systemTransfer = new(payload);
        CapturingComm comm = new();

        FakeSnapshotHost host = new(partitionId: 0, systemTransfer: systemTransfer, rangeTransfer: null, comm: comm);
        FloorWal wal = new(floor: 50, commitIndex: 50);
        RaftPartitionStateMachine sm = new(host, wal, new CapturingSink(), NullLogger<IRaft>.Instance);
        sm.SetPostToExecutor(_ => { });

        List<SnapshotRequest> chunks = await DriveSnapshotAsync(sm, "follower:9000", comm);

        Assert.True(chunks.Count >= 2, "large payload must produce multiple chunks");
        Assert.All(chunks, c => Assert.Equal(SnapshotKind.SystemState, c.Kind));
        // Exactly one IsLast chunk.
        Assert.Single(chunks, c => c.IsLast);
    }

    // ── P0 with RangeTransfer only → ExportRange + Kind=Range ─────────────────

    [Fact]
    public async Task P0_WithRangeTransferOnly_CallsExportRange()
    {
        CapturingRangeTransfer rangeTransfer = new();
        CapturingComm comm = new();

        FakeSnapshotHost host = new(partitionId: 0, systemTransfer: null, rangeTransfer: rangeTransfer, comm: comm);
        FloorWal wal = new(floor: 100, commitIndex: 100);
        RaftPartitionStateMachine sm = new(host, wal, new CapturingSink(), NullLogger<IRaft>.Instance);
        sm.SetPostToExecutor(_ => { });

        List<SnapshotRequest> chunks = await DriveSnapshotAsync(sm, "follower:9000", comm);

        Assert.NotEmpty(chunks);
        Assert.All(chunks, c => Assert.Equal(SnapshotKind.Range, c.Kind));
        Assert.True(rangeTransfer.ExportCalled, "ExportRange must be called");
    }

    // ── Non-P0 partition → ExportRange path regardless of SystemStateTransfer ──

    [Fact]
    public async Task NonP0_WithRangeTransfer_CallsExportRange()
    {
        CapturingRangeTransfer rangeTransfer = new();
        CapturingSystemTransfer systemTransfer = new(); // also registered but must not be used
        CapturingComm comm = new();

        // partitionId = 1 (not P0)
        FakeSnapshotHost host = new(partitionId: 1, systemTransfer: systemTransfer, rangeTransfer: rangeTransfer, comm: comm);
        FloorWal wal = new(floor: 100, commitIndex: 100);
        RaftPartitionStateMachine sm = new(host, wal, new CapturingSink(), NullLogger<IRaft>.Instance);
        sm.SetPostToExecutor(_ => { });

        List<SnapshotRequest> chunks = await DriveSnapshotAsync(sm, "follower:9000", comm);

        Assert.NotEmpty(chunks);
        Assert.All(chunks, c => Assert.Equal(SnapshotKind.Range, c.Kind));
        Assert.True(rangeTransfer.ExportCalled, "ExportRange must be called for non-P0");
        Assert.False(systemTransfer.ExportCalled, "ExportPartitionState must NOT be called for non-P0");
    }

    // ── P0 with neither transfer → no snapshot triggered ──────────────────────

    [Fact]
    public async Task P0_WithNoTransfer_NoSnapshotSent()
    {
        CapturingComm comm = new();

        FakeSnapshotHost host = new(partitionId: 0, systemTransfer: null, rangeTransfer: null, comm: comm);
        FloorWal wal = new(floor: 100, commitIndex: 100);
        RaftPartitionStateMachine sm = new(host, wal, new CapturingSink(), NullLogger<IRaft>.Instance);
        sm.SetPostToExecutor(_ => { });

        await sm.ForceLeaderForTestingAsync(replyCorrelationId: null);
        await sm.ReceivedVoteAsync("follower:9000", voteTerm: 1, remoteMaxLogId: 0);

        // No transfer → no snapshot; wait a short time and verify nothing was sent.
        await Task.Delay(100);
        Assert.Empty(comm.Captured);
    }

    // ── inner stubs ───────────────────────────────────────────────────────────

    private sealed class CapturingSystemTransfer : IRaftSystemStateTransfer
    {
        private readonly byte[] _payload;
        public bool ExportCalled { get; private set; }
        public int ExportedPartitionId { get; private set; }
        public long ExportedUpToIndex { get; private set; }

        public CapturingSystemTransfer(byte[]? payload = null) =>
            _payload = payload ?? [0xAA, 0xBB, 0xCC];

        public Task<Stream> ExportPartitionState(int partitionId, long upToIndex, CancellationToken ct)
        {
            ExportCalled = true;
            ExportedPartitionId = partitionId;
            ExportedUpToIndex = upToIndex;
            return Task.FromResult<Stream>(new MemoryStream(_payload));
        }

        public Task ImportPartitionState(int partitionId, Stream snapshot, CancellationToken ct) =>
            Task.CompletedTask;
    }

    private sealed class CapturingRangeTransfer : IRaftStateMachineTransfer
    {
        public bool ExportCalled { get; private set; }

        public Task<Stream> ExportRange(RaftSplitPlan plan, long upToIndex, CancellationToken ct)
        {
            ExportCalled = true;
            return Task.FromResult<Stream>(new MemoryStream([0x11, 0x22, 0x33]));
        }

        public Task ImportRange(int targetPartitionId, Stream snapshot, CancellationToken ct) =>
            Task.CompletedTask;
    }

    /// <summary>
    /// Captures all SnapshotRequest chunks sent by TrySendSnapshotAsync and provides
    /// a task that completes when at least the final (IsLast) chunk arrives.
    /// </summary>
    private sealed class CapturingComm
    {
        private readonly TaskCompletionSource<bool> _lastChunkTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public List<SnapshotRequest> Captured { get; } = [];

        public Task<SnapshotResponse> SendInstallSnapshotAsync(RaftNode _, SnapshotRequest r, CancellationToken __)
        {
            lock (Captured) Captured.Add(r);
            if (r.IsLast) _lastChunkTcs.TrySetResult(true);
            return Task.FromResult(new SnapshotResponse(true));
        }

        public async Task<List<SnapshotRequest>> WaitForChunksAsync(TimeSpan timeout)
        {
            try { await _lastChunkTcs.Task.WaitAsync(timeout); }
            catch (TimeoutException) { /* test will fail on Assert.NotEmpty */ }
            lock (Captured) return [.. Captured];
        }
    }

    private sealed class FakeSnapshotHost : IRaftPartitionHost
    {
        private readonly IRaftSystemStateTransfer? _systemTransfer;
        private readonly IRaftStateMachineTransfer? _rangeTransfer;
        private readonly CapturingComm _comm;

        public FakeSnapshotHost(int partitionId, IRaftSystemStateTransfer? systemTransfer,
            IRaftStateMachineTransfer? rangeTransfer, CapturingComm comm)
        {
            PartitionId = partitionId;
            _systemTransfer = systemTransfer;
            _rangeTransfer = rangeTransfer;
            _comm = comm;
        }

        public int PartitionId { get; }
        public string Leader { get => ""; set { } }
        public string LocalEndpoint => "leader:9000";
        public int LocalNodeId => 1;
        public ClusterMemberRole LocalRole => ClusterMemberRole.Voter;
        public bool IsVoter(string endpoint) => true;

        public RaftConfiguration Configuration { get; } = new()
        {
            NodeId = 1, Host = "leader", Port = 9000, InitialPartitions = 1,
            HeartbeatInterval = TimeSpan.Zero,          // fires immediately
            BackfillThreshold = 0,                      // any lag triggers backfill
            MaxBackfillEntriesPerRound = 128,
        };

        public HybridLogicalClock HybridLogicalClock { get; } = new();
        public IReadOnlyList<RaftNode> Nodes => [new("follower:9000")];

        public HLCTimestamp GetLastNodeActivity(string e, int p) => HLCTimestamp.Zero;
        public HLCTimestamp GetLastNodeHearthbeat(string e, int p) => HLCTimestamp.Zero;
        public void UpdateLastHeartbeat(string e, int p, HLCTimestamp t) { }
        public void UpdateLastNodeActivity(string e, int p, HLCTimestamp t) { }
        public void EnqueueResponse(string e, RaftResponderRequest r) { }
        public Task InvokeLeaderChanged(int p, string l) => Task.CompletedTask;
        public Task<bool> InvokeReplicationReceived(int p, RaftLog l) => Task.FromResult(true);
        public Task<bool> InvokeSystemReplicationReceived(int p, RaftLog l) => Task.FromResult(true);
        public void InvokeReplicationError(int p, RaftLog l) { }

        public IRaftStateMachineTransfer? StateMachineTransfer => _rangeTransfer;
        public IRaftSystemStateTransfer? SystemStateTransfer => _systemTransfer;

        public Task<SnapshotResponse> SendInstallSnapshotAsync(RaftNode node, SnapshotRequest request, CancellationToken ct) =>
            _comm.SendInstallSnapshotAsync(node, request, ct);

        public MemberLivenessState GetNodeLiveness(string endpoint) => MemberLivenessState.Alive;
    }

    /// <summary>WAL stub that simulates a compaction floor by returning an empty log range.</summary>
    private sealed class FloorWal : IRaftWalFacade
    {
        private readonly long _floor;
        private readonly long _commitIndex;

        public FloorWal(long floor, long commitIndex)
        {
            _floor = floor;
            _commitIndex = commitIndex;
        }

        public ValueTask<IReadOnlyList<RaftLog>> LoadRestoreLogsAsync() =>
            ValueTask.FromResult<IReadOnlyList<RaftLog>>([]);
        public ValueTask CompleteRestoreAsync(IReadOnlyList<RaftLog> logs) => ValueTask.CompletedTask;
        public ValueTask<long> GetMaxLogAsync() => ValueTask.FromResult(_commitIndex);
        public ValueTask<long> TruncateLogsAfterAsync(long afterLogId) => ValueTask.FromResult(afterLogId);
        public ValueTask<long> GetCurrentTermAsync() => ValueTask.FromResult(1L);
        public ValueTask<List<RaftLog>> GetRangeAsync(long startLogIndex, int maxEntries) =>
            ValueTask.FromResult(new List<RaftLog>());
        public ValueTask<long> GetAnyTermAtAsync(long logIndex) => ValueTask.FromResult(-1L);
        public ValueTask<long> GetLastCheckpointAsync() => ValueTask.FromResult(_floor);
        public long GetCommitIndex() => _commitIndex;
        public WALWriteOperation EnqueuePropose(long term, List<RaftLog> logs, HLCTimestamp ts, bool ac) => MakeNoOp();
        public WALWriteOperation EnqueueCommit(List<RaftLog> logs) => MakeNoOp();
        public WALWriteOperation EnqueueRollback(List<RaftLog> logs) => MakeNoOp();
        public WALWriteOperation? EnqueueProposeOrCommit(List<RaftLog>? logs, HLCTimestamp ts = default, string? ep = null, long term = -1) =>
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
