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
/// A hung snapshot-transfer step must never latch the rescue path shut.
///
/// <para>The second Caraxes soak (run D, Kommander 1.2.9) wedged with the refusal escalation in
/// place: every refusal escalated, but the single transfer each new leader started hung — the
/// export and every chunk send ran with <c>CancellationToken.None</c> and the install RPC carries
/// no deadline — so the <c>pendingSnapshotEndpoints</c> entry never released, <c>CanAttempt</c>
/// silently vetoed every later rescue for that follower, and the only line ever produced was the
/// transfer-start log at a level the consumer filtered out. These tests pin the fix: each awaited
/// step is bounded by <see cref="RaftConfiguration.SnapshotTransferStepTimeout"/>; a timeout is
/// recorded as a visible, queryable failure; the in-flight guard is released so the normal backoff
/// paces a retry; and a healed follower is then seeded end to end.</para>
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
public class TestSnapshotTransferStepTimeout
{
    private const string Follower = "follower:9001";

    /// <summary>
    /// An export that never completes must be abandoned at the step timeout, recorded as a failure
    /// (not left silently in flight), and retried after the backoff — the pre-fix code kept the
    /// first attempt in flight forever and never tried again.
    /// </summary>
    [Fact]
    public async Task HungExport_TimesOut_RecordsFailure_AndRetries()
    {
        HangingExportTransfer transfer = new();
        Harness h = await Harness.BuildLeaderAsync(transfer, hangInstall: false);

        await h.AckSuccess();
        await h.WaitUntilAsync(
            () => h.Sm.GetSnapshotStatuses().Any(s => !s.InFlight && s.FailedAttempts >= 1),
            "timeout failure recorded");

        RaftSnapshotStatus status = Assert.Single(h.Sm.GetSnapshotStatuses());
        Assert.Equal(Follower, status.FollowerEndpoint);
        Assert.False(status.InFlight);
        Assert.True(status.FailedAttempts >= 1);
        Assert.Contains("ExportPartitionState", status.LastError);
        Assert.Contains("SnapshotTransferStepTimeout", status.LastError);

        // The guard is released: once the backoff expires, the next refused ack starts a second
        // attempt. Pre-fix the first zombie held the in-flight guard forever and this never fired.
        TimeSpan budget = TestTimeouts.Scale(TimeSpan.FromSeconds(5));
        long started = Stopwatch.GetTimestamp();
        while (transfer.ExportCalls < 2)
        {
            if (Stopwatch.GetElapsedTime(started) > budget)
                Assert.Fail($"no retry after the step timeout; export attempts={transfer.ExportCalls}");
            await h.AckSuccess();
            await Task.Delay(25, TestContext.Current.CancellationToken);
        }
    }

    /// <summary>
    /// A hung install RPC (the deadline-less terminal-chunk wait against a stalled receiver) must
    /// surface as an in-flight status with an age while it runs, then time out, release the guard,
    /// and — once the receiver heals — the same follower must be seeded end to end.
    /// </summary>
    [Fact]
    public async Task HungInstallRpc_TimesOut_ReleasesGuard_AndSeedsAfterHeal()
    {
        Harness h = await Harness.BuildLeaderAsync(new InstantTransfer(), hangInstall: true);

        await h.AckSuccess();

        // While the send hangs, the condition is queryable: in flight, with an age.
        await h.WaitUntilAsync(() => h.Sm.GetSnapshotStatuses().Any(s => s.InFlight), "transfer in flight");
        RaftSnapshotStatus inFlight = Assert.Single(h.Sm.GetSnapshotStatuses());
        Assert.NotNull(inFlight.InFlightFor);

        // The step timeout abandons the attempt and records it.
        await h.WaitUntilAsync(
            () => h.Sm.GetSnapshotStatuses().Any(s => !s.InFlight && s.FailedAttempts >= 1),
            "timeout failure recorded");
        RaftSnapshotStatus failed = Assert.Single(h.Sm.GetSnapshotStatuses());
        Assert.Contains("install chunk 0", failed.LastError);

        // Heal the receiver: the next refused ack after the backoff must seed the follower fully.
        h.Host.HangInstall = false;
        TimeSpan budget = TestTimeouts.Scale(TimeSpan.FromSeconds(5));
        long started = Stopwatch.GetTimestamp();
        while (!h.LastChunkCaptured)
        {
            if (Stopwatch.GetElapsedTime(started) > budget)
                Assert.Fail("follower never seeded after the receiver healed");
            await h.AckSuccess();
            await Task.Delay(25, TestContext.Current.CancellationToken);
        }

        // Success clears the failure episode from the status surface.
        await h.WaitUntilAsync(() => h.Sm.GetSnapshotStatuses().Count == 0, "status cleared after success");
    }

    // ── harness ───────────────────────────────────────────────────────────────

    private sealed class Harness
    {
        public required RaftPartitionStateMachine Sm { get; init; }
        public required HangableHost Host { get; init; }

        public bool LastChunkCaptured => Host.CapturedChunks().Any(c => c.IsLast);

        public static async Task<Harness> BuildLeaderAsync(IRaftPartitionStateTransfer transfer, bool hangInstall)
        {
            HangableHost host = new(transfer) { HangInstall = hangInstall };
            FloorWal wal = new(floor: 50, commitIndex: 100);

            RaftPartitionStateMachine sm = new(host, wal, new NoopSink(), NullLogger<IRaft>.Instance);
            IReadOnlyList<RaftLog> logs = await sm.StartRestoreAsync();
            await sm.CompleteRestoreAsync(logs);
            sm.SetPostToExecutor(_ => { });
            sm.SetLeaderForTesting(term: 1);

            return new Harness { Sm = sm, Host = host };
        }

        public Task AckSuccess() =>
            Sm.CompleteAppendLogsAsync(Follower, Host.HybridLogicalClock.TrySendOrLocalEvent(1),
                RaftOperationStatus.Success, committedIndex: 0).AsTask();

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

    /// <summary>Export that never completes and ignores its cancellation token — the worst case.</summary>
    private sealed class HangingExportTransfer : IRaftPartitionStateTransfer
    {
        private int exportCalls;

        public int ExportCalls => Volatile.Read(ref exportCalls);

        public Task<Stream> ExportPartitionState(int partitionId, long upToIndex, CancellationToken ct)
        {
            Interlocked.Increment(ref exportCalls);
            return new TaskCompletionSource<Stream>().Task;
        }

        public Task ImportPartitionState(int partitionId, Stream snapshot, CancellationToken ct) =>
            Task.CompletedTask;
    }

    /// <summary>Serves a tiny snapshot immediately, so only the send path can hang.</summary>
    private sealed class InstantTransfer : IRaftPartitionStateTransfer
    {
        public Task<Stream> ExportPartitionState(int partitionId, long upToIndex, CancellationToken ct) =>
            Task.FromResult<Stream>(new MemoryStream([0xAB, 0xCD]));

        public Task ImportPartitionState(int partitionId, Stream snapshot, CancellationToken ct) =>
            Task.CompletedTask;
    }

    private sealed class HangableHost : IRaftPartitionHost
    {
        private readonly IRaftPartitionStateTransfer transfer;
        private readonly List<SnapshotRequest> chunks = [];

        /// <summary>While true, SendInstallSnapshotAsync never completes (a stalled receiver).</summary>
        public volatile bool HangInstall;

        public HangableHost(IRaftPartitionStateTransfer transfer)
        {
            this.transfer = transfer;
            Configuration = new RaftConfiguration
            {
                NodeId = 1, Host = "leader", Port = 9000, InitialPartitions = 1,
                HeartbeatInterval = TimeSpan.Zero, RecentHeartbeat = TimeSpan.Zero,
                BackfillThreshold = 0,
                MaxBackfillEntriesPerRound = 128,
                SnapshotTransferStepTimeout = TimeSpan.FromMilliseconds(250),
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
            if (HangInstall)
                return new TaskCompletionSource<SnapshotResponse>().Task;

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
