
using System.Diagnostics;
using Kommander.Data;
using Kommander.Gossip;
using Kommander.Scheduling;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL.Data;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.Scheduling;

/// <summary>
/// A follower below the compaction floor whose snapshot cannot be produced or delivered used to be
/// retried at full rate every heartbeat forever — an unbounded error loop whose only evidence was
/// a log line per attempt — and a follower that was merely <em>saturated</em> could be shipped a
/// full snapshot because every no-batch cause collapsed into the same <c>false</c>. These tests pin
/// the fixes: per-follower failure backoff, the queryable
/// <see cref="RaftPartitionStateMachine.GetSnapshotStatuses"/> surface, the cause-split that keeps
/// saturation pauses from escalating to snapshots, and the previously-silent
/// "snapshot required but no transfer registered" condition being reported.
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
public class TestSnapshotFailureBackoff
{
    private const string Follower = "follower:9001";

    // ── failure backoff ───────────────────────────────────────────────────────

    [Fact]
    public async Task ExportFailure_RecordsStatusAndBacksOff_NoHotLoop()
    {
        // Zero heartbeat interval so the heartbeat drive fires on every tick; the backoff base
        // then floors at 100 ms, and the immediate re-drives below land inside that window.
        ThrowingTransfer transfer = new(failuresBeforeSuccess: int.MaxValue);
        Harness h = await Harness.BuildLeaderAsync(transfer, heartbeatInterval: TimeSpan.Zero);

        await h.AckSuccess();
        await h.Sm.CheckPartitionLeadershipAsync();
        await h.WaitUntilAsync(() => transfer.ExportCalls == 1, "first export attempt");
        await h.WaitUntilAsync(() => h.Sm.GetSnapshotStatuses().Count > 0, "failure recorded");

        // Status reflects the recorded episode while the pause is armed.
        RaftSnapshotStatus status = Assert.Single(h.Sm.GetSnapshotStatuses());
        Assert.Equal(Follower, status.FollowerEndpoint);
        Assert.Equal(1, status.FailedAttempts);
        Assert.False(status.Unproducible);
        Assert.False(status.InFlight);
        Assert.Contains("ExportPartitionState", status.LastError);
        Assert.True(status.RetryBackoffRemaining > TimeSpan.Zero, "a retry pause must be armed");
        Assert.NotNull(status.FirstFailureAt);
        Assert.NotNull(status.LastFailureAt);

        // Re-driving the heartbeat inside the backoff window must NOT re-attempt the export —
        // this was the unbounded once-per-heartbeat error loop. (No further heartbeat is driven
        // after the pause expires, so the count must hold through the trailing delay too.)
        await h.Sm.CheckPartitionLeadershipAsync();
        await h.Sm.CheckPartitionLeadershipAsync();
        Assert.Equal(1, transfer.ExportCalls);

        await Task.Delay(150, TestContext.Current.CancellationToken);
        Assert.Equal(1, transfer.ExportCalls);
    }

    [Fact]
    public async Task ExportFailsTwiceThenSucceeds_FollowerIsSeeded_AndStatusClears()
    {
        // Zero heartbeat interval → 100 ms backoff floor, so the retries complete quickly.
        ThrowingTransfer transfer = new(failuresBeforeSuccess: 2);
        Harness h = await Harness.BuildLeaderAsync(transfer, heartbeatInterval: TimeSpan.Zero);

        await h.AckSuccess();

        TimeSpan budget = TestTimeouts.Scale(TimeSpan.FromSeconds(10));
        long started = Stopwatch.GetTimestamp();
        while (!h.LastChunkCaptured)
        {
            if (Stopwatch.GetElapsedTime(started) > budget)
                Assert.Fail($"follower never seeded; export attempts={transfer.ExportCalls}");

            await h.Sm.CheckPartitionLeadershipAsync();
            await Task.Delay(25, TestContext.Current.CancellationToken);
        }

        // Two failures + the successful attempt; the pauses were honoured in between.
        Assert.Equal(3, transfer.ExportCalls);
        Assert.All(h.CapturedChunks, c => Assert.Equal(SnapshotKind.PartitionState, c.Kind));

        // Success clears the failure episode from the status surface.
        await h.WaitUntilAsync(() => h.Sm.GetSnapshotStatuses().Count == 0, "status cleared after success");
    }

    // ── cause split: saturation must not escalate to a snapshot ───────────────

    [Fact]
    public async Task SaturatedFollower_DoesNotReceiveSnapshot_UntilPauseExpires()
    {
        ThrowingTransfer transfer = new(failuresBeforeSuccess: 0); // always succeeds
        Harness h = await Harness.BuildLeaderAsync(transfer, heartbeatInterval: TimeSpan.Zero);
        h.Host.Configuration.FollowerSaturationBackoff = TimeSpan.FromSeconds(30);

        await h.AckSuccess();
        await h.Ack(RaftOperationStatus.FollowerWalSaturated);

        // Below the floor AND saturated: the old collapsed-false code fell through to the
        // snapshot path here. The saturation pause must win — no export, no chunks.
        await h.Sm.CheckPartitionLeadershipAsync();
        await h.Sm.CheckPartitionLeadershipAsync();
        await Task.Delay(150, TestContext.Current.CancellationToken);
        Assert.Equal(0, transfer.ExportCalls);
        Assert.Empty(h.CapturedChunks);

        // Once the saturation window elapses, the same follower's floor condition escalates
        // to a snapshot normally.
        h.Host.Configuration.FollowerSaturationBackoff = TimeSpan.Zero;
        await h.Ack(RaftOperationStatus.FollowerWalSaturated); // re-arms with the zero window
        await h.Sm.CheckPartitionLeadershipAsync();

        await h.WaitUntilAsync(() => h.LastChunkCaptured, "snapshot after the pause expired");
        Assert.Equal(1, transfer.ExportCalls);
    }

    // ── unproducible: below the floor with no transfer registered ─────────────

    [Fact]
    public async Task NoTransferRegistered_ReportsUnproducible_OncePerEpisode()
    {
        Harness h = await Harness.BuildLeaderAsync(transfer: null, heartbeatInterval: TimeSpan.Zero);

        await h.AckSuccess();
        await h.Sm.CheckPartitionLeadershipAsync();
        await Task.Delay(50, TestContext.Current.CancellationToken);
        await h.Sm.CheckPartitionLeadershipAsync();

        // Previously this condition was fully silent: no snapshot attempt, no log, no status —
        // the follower was permanently stranded. Now it is a single recorded episode.
        RaftSnapshotStatus status = Assert.Single(h.Sm.GetSnapshotStatuses());
        Assert.Equal(Follower, status.FollowerEndpoint);
        Assert.True(status.Unproducible);
        Assert.Equal(1, status.FailedAttempts); // the second heartbeat refreshed, not re-counted
        Assert.Contains("no snapshot transfer is registered", status.LastError);
        Assert.Empty(h.CapturedChunks);
    }

    // ── harness ───────────────────────────────────────────────────────────────

    private sealed class Harness
    {
        public required RaftPartitionStateMachine Sm { get; init; }
        public required FailingSnapshotHost Host { get; init; }

        public IReadOnlyList<SnapshotRequest> CapturedChunks => Host.CapturedChunks();
        public bool LastChunkCaptured => Host.CapturedChunks().Any(c => c.IsLast);

        public static async Task<Harness> BuildLeaderAsync(ThrowingTransfer? transfer, TimeSpan heartbeatInterval)
        {
            FailingSnapshotHost host = new(transfer, heartbeatInterval);
            FloorWal wal = new(floor: 50, commitIndex: 100);

            RaftPartitionStateMachine sm = new(host, wal, new NoopSink(), NullLogger<IRaft>.Instance);
            IReadOnlyList<RaftLog> logs = await sm.StartRestoreAsync();
            await sm.CompleteRestoreAsync(logs);
            sm.SetPostToExecutor(_ => { });
            sm.SetLeaderForTesting(term: 1);

            return new Harness { Sm = sm, Host = host };
        }

        public Task AckSuccess() => Ack(RaftOperationStatus.Success);

        public Task Ack(RaftOperationStatus status) =>
            Sm.CompleteAppendLogsAsync(Follower, Host.HybridLogicalClock.TrySendOrLocalEvent(1),
                status, committedIndex: 0).AsTask();

        public async Task WaitUntilAsync(Func<bool> condition, string what)
        {
            TimeSpan budget = TestTimeouts.Scale(TimeSpan.FromSeconds(5));
            long started = Stopwatch.GetTimestamp();
            while (!condition())
            {
                if (Stopwatch.GetElapsedTime(started) > budget)
                    Assert.Fail($"timed out waiting for: {what}");
                await Task.Delay(10, TestContext.Current.CancellationToken);
            }
        }
    }

    /// <summary>
    /// Whole-partition transfer that fails its first N exports (counted), then serves a small
    /// blob. N = 0 always succeeds; N = int.MaxValue always fails.
    /// </summary>
    private sealed class ThrowingTransfer(int failuresBeforeSuccess) : IRaftPartitionStateTransfer
    {
        private int exportCalls;

        public int ExportCalls => Volatile.Read(ref exportCalls);

        public Task<Stream> ExportPartitionState(int partitionId, long upToIndex, CancellationToken ct)
        {
            int attempt = Interlocked.Increment(ref exportCalls);
            if (attempt <= failuresBeforeSuccess)
                throw new InvalidOperationException($"ExportPartitionState failure injected (attempt {attempt})");

            return Task.FromResult<Stream>(new MemoryStream([0xAB, 0xCD]));
        }

        public Task ImportPartitionState(int partitionId, Stream snapshot, CancellationToken ct) =>
            Task.CompletedTask;
    }

    private sealed class FailingSnapshotHost : IRaftPartitionHost
    {
        private readonly ThrowingTransfer? transfer;
        private readonly List<SnapshotRequest> chunks = [];

        public FailingSnapshotHost(ThrowingTransfer? transfer, TimeSpan heartbeatInterval)
        {
            this.transfer = transfer;
            Configuration = new RaftConfiguration
            {
                NodeId = 1, Host = "leader", Port = 9000, InitialPartitions = 1,
                HeartbeatInterval = heartbeatInterval,
                BackfillThreshold = 0,
                MaxBackfillEntriesPerRound = 128,
            };
        }

        public int PartitionId => 1;
        public string Leader { get; set; } = "";
        public string LocalEndpoint => "leader:9000";
        public int LocalNodeId => 1;
        public ClusterMemberRole LocalRole => ClusterMemberRole.Voter;
        public bool IsVoter(string endpoint) => true;
        public RaftConfiguration Configuration { get; }
        public HybridLogicalClock HybridLogicalClock { get; } = new();
        public IReadOnlyList<RaftNode> Nodes => [new(Follower)];

        public HLCTimestamp GetLastNodeActivity(string e, int p) => HLCTimestamp.Zero;
        public HLCTimestamp GetLastNodeHearthbeat(string e, int p) => HLCTimestamp.Zero;
        public void UpdateLastHeartbeat(string e, int p, HLCTimestamp t) { }
        public void UpdateLastNodeActivity(string e, int p, HLCTimestamp t) { }
        public void EnqueueResponse(string e, RaftResponderRequest r) { }
        public Task InvokeLeaderChanged(int p, string l) => Task.CompletedTask;
        public Task<bool> InvokeReplicationReceived(int p, RaftLog l) => Task.FromResult(true);
        public Task<bool> InvokeSystemReplicationReceived(int p, RaftLog l) => Task.FromResult(true);
        public void InvokeReplicationError(int p, RaftLog l) { }
        public MemberLivenessState GetNodeLiveness(string endpoint) => MemberLivenessState.Alive;

        public IRaftStateMachineTransfer? StateMachineTransfer => null;
        public IRaftSystemStateTransfer? SystemStateTransfer => null;
        public IRaftPartitionStateTransfer? PartitionStateTransfer => transfer;

        public Task<SnapshotResponse> SendInstallSnapshotAsync(RaftNode node, SnapshotRequest request, CancellationToken ct)
        {
            // Copy: the sender's chunk Data is a view over a reused buffer.
            lock (chunks)
                chunks.Add(new SnapshotRequest
                {
                    SessionId = request.SessionId,
                    PartitionId = request.PartitionId,
                    SnapshotIndex = request.SnapshotIndex,
                    ChunkIndex = request.ChunkIndex,
                    IsLast = request.IsLast,
                    Kind = request.Kind,
                    Data = request.Data.ToArray(),
                });
            return Task.FromResult(new SnapshotResponse(true));
        }

        public IReadOnlyList<SnapshotRequest> CapturedChunks()
        {
            lock (chunks) return [.. chunks];
        }
    }

    /// <summary>WAL stub with a compaction floor: committed range reads come back empty.</summary>
    private sealed class FloorWal : IRaftWalFacade
    {
        private readonly long floor;
        private readonly long commitIndex;

        public FloorWal(long floor, long commitIndex)
        {
            this.floor = floor;
            this.commitIndex = commitIndex;
        }

        public ValueTask<IReadOnlyList<RaftLog>> LoadRestoreLogsAsync() =>
            ValueTask.FromResult<IReadOnlyList<RaftLog>>([]);
        public ValueTask CompleteRestoreAsync(IReadOnlyList<RaftLog> logs) => ValueTask.CompletedTask;
        public ValueTask<long> GetMaxLogAsync() => ValueTask.FromResult(commitIndex);
        public ValueTask<long> TruncateLogsAfterAsync(long afterLogId) => ValueTask.FromResult(afterLogId);
        public ValueTask<long> GetCurrentTermAsync() => ValueTask.FromResult(1L);
        public ValueTask<List<RaftLog>> GetRangeAsync(long startLogIndex, int maxEntries) =>
            ValueTask.FromResult(new List<RaftLog>());
        public ValueTask<long> GetAnyTermAtAsync(long logIndex) => ValueTask.FromResult(-1L);
        public ValueTask<long> GetLastCheckpointAsync() => ValueTask.FromResult(floor);
        public long GetCommitIndex() => commitIndex;
        public WALWriteOperation EnqueuePropose(long term, List<RaftLog> logs, HLCTimestamp ts, bool ac) => MakeNoOp();
        public WALWriteOperation EnqueueCommit(List<RaftLog> logs) => MakeNoOp();
        public WALWriteOperation EnqueueRollback(List<RaftLog> logs) => MakeNoOp();
        public WALWriteOperation? EnqueueProposeOrCommit(List<RaftLog>? logs, HLCTimestamp timestamp = default, string? endpoint = null, long term = -1) =>
            logs is null ? null : MakeNoOp();
        public void NotifyCommitted() { }

        private static WALWriteOperation MakeNoOp() =>
            new(_ => { }, 0, WALWriteOperationType.LeaderPropose, (0, []));
    }

    private sealed class NoopSink : IRaftOperationReplySink
    {
        public void TryComplete(ulong correlationId, RaftResponse response) { }
    }
}
