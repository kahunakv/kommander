
using System.Diagnostics;
using System.Security.Cryptography;
using Kommander.Data;
using Kommander.Gossip;
using Kommander.Scheduling;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL.Data;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.Scheduling;

/// <summary>
/// Pins the fixes for the non-converging snapshot rescue loop (Caraxes
/// <c>bank-optimistic-45m-p</c>): a follower that returns below the WAL compaction floor after
/// every successful install drove an unbounded whole-partition export every cooldown — 29 exports
/// in 15 minutes, every one reporting success, until the leader died of memory exhaustion. None of
/// the failure-path controls fire on a loop in which nothing fails, so three dedicated controls
/// exist and are tested here:
/// <list type="number">
///   <item>a convergence breaker bounds consecutive install→re-escalation cycles per follower and
///   surfaces the condition as <see cref="RaftSnapshotStatus.RescueNotConverging"/>;</item>
///   <item>a retry at the same snapshot index replays the cached export instead of re-running
///   <c>ExportPartitionState</c>;</item>
///   <item>the leader's heartbeat publishes a live-replica retention floor so compaction stops
///   re-creating the below-floor condition (the compaction side is tested in
///   <c>TestRaftWriteAheadCompaction</c>).</item>
/// </list>
/// The harness drives the loop deliberately: the mutable WAL floor is advanced past the follower
/// after each confirmed install, exactly what the leader's own compaction cadence did in the soak.
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
public class TestSnapshotRescueConvergence
{
    private const string Follower = "follower:9001";

    private const long InitialFloor = 300;
    private const long CommitIndex = 10_000;

    // ── convergence breaker ───────────────────────────────────────────────────

    [Fact]
    public async Task NonConvergingRescue_TripsBreakerAfterConfiguredCycles()
    {
        Harness h = await Harness.BuildLeaderAsync();

        // The follower last reported frontier 199 — below the floor. Every heartbeat from here
        // refuses the backfill and escalates to a snapshot.
        await h.AckSuccess(199);

        // Rescue loop: each confirmed install is followed by the floor advancing past the seeded
        // follower (the compaction cadence of the incident), so the next heartbeat escalates again.
        // With SnapshotRescueMaxConsecutiveCycles = 3 (default), installs 1..3 happen and the
        // escalation after the third install trips the breaker.
        await h.DriveRescueCycleAsync(expectedInstalls: 1);
        await h.DriveRescueCycleAsync(expectedInstalls: 2);
        await h.DriveRescueCycleAsync(expectedInstalls: 3);

        // The escalation that would start cycle 4 must be refused by the breaker — and stay
        // refused on later heartbeats.
        h.AdvanceFloorPastFollower();
        h.AdvanceMs(200);
        await h.Sm.CheckPartitionLeadershipAsync();
        h.AdvanceMs(200);
        await h.Sm.CheckPartitionLeadershipAsync();
        await Task.Delay(100, TestContext.Current.CancellationToken);

        Assert.Equal(3, h.Transfer.ExportCalls);
        Assert.Equal(3, h.Installs);

        RaftSnapshotStatus status = Assert.Single(h.Sm.GetSnapshotStatuses());
        Assert.Equal(Follower, status.FollowerEndpoint);
        Assert.True(status.RescueNotConverging,
            "a rescue loop in which every install succeeds and the follower still returns below " +
            "the floor must be surfaced as RescueNotConverging — nothing in it ever fails, so no " +
            "failure-path status will ever report it");
        Assert.Equal(3, status.ConsecutiveRescueCycles);
        Assert.Equal(0, status.FailedAttempts);
    }

    [Fact]
    public async Task TrippedBreaker_AllowsOnePacedProbe()
    {
        Harness h = await Harness.BuildLeaderAsync(
            configure: c => c.SnapshotRescueProbeInterval = TimeSpan.FromMinutes(2));

        await h.AckSuccess(199);
        await h.DriveRescueCycleAsync(expectedInstalls: 1);
        await h.DriveRescueCycleAsync(expectedInstalls: 2);
        await h.DriveRescueCycleAsync(expectedInstalls: 3);

        // Trip the breaker.
        h.AdvanceFloorPastFollower();
        h.AdvanceMs(200);
        await h.Sm.CheckPartitionLeadershipAsync();
        Assert.Equal(3, h.Transfer.ExportCalls);

        // Heartbeats every 30 s keep the episode alive (under the 60 s quiet window) but stay
        // inside the 2-minute probe interval: still blocked.
        for (int i = 0; i < 3; i++)
        {
            h.AdvanceMs(30_000);
            await h.Sm.CheckPartitionLeadershipAsync();
        }

        Assert.Equal(3, h.Transfer.ExportCalls);

        // The fourth step crosses the probe interval: exactly one attempt is admitted.
        h.AdvanceMs(30_000);
        await h.Sm.CheckPartitionLeadershipAsync();
        await h.WaitForInstallsAsync(4);

        Assert.Equal(4, h.Transfer.ExportCalls);

        // The probe did not converge either (the floor still sits above the follower), so the
        // breaker stays tripped and the very next escalation is blocked again.
        h.AdvanceFloorPastFollower();
        h.AdvanceMs(200);
        await h.Sm.CheckPartitionLeadershipAsync();
        await Task.Delay(100, TestContext.Current.CancellationToken);
        Assert.Equal(4, h.Transfer.ExportCalls);
        Assert.Contains(h.Sm.GetSnapshotStatuses(), s => s.RescueNotConverging);
    }

    [Fact]
    public async Task QuietEpisode_ResetsTheBreaker()
    {
        Harness h = await Harness.BuildLeaderAsync();

        await h.AckSuccess(199);
        await h.DriveRescueCycleAsync(expectedInstalls: 1);
        await h.DriveRescueCycleAsync(expectedInstalls: 2);
        await h.DriveRescueCycleAsync(expectedInstalls: 3);

        h.AdvanceFloorPastFollower();
        h.AdvanceMs(200);
        await h.Sm.CheckPartitionLeadershipAsync();
        Assert.Equal(3, h.Transfer.ExportCalls);

        // Refusals stop for longer than the quiet window (60 s): whatever held the follower back
        // is gone as far as the leader can tell, so the episode — including the tripped breaker —
        // starts fresh when a refusal next arrives.
        h.AdvanceMs(61_000);
        await h.Sm.CheckPartitionLeadershipAsync();
        await h.WaitForInstallsAsync(4);

        Assert.Equal(4, h.Transfer.ExportCalls);
        Assert.DoesNotContain(h.Sm.GetSnapshotStatuses(), s => s.RescueNotConverging);
    }

    [Fact]
    public async Task BreakerDisabled_KeepsEscalating()
    {
        Harness h = await Harness.BuildLeaderAsync(
            configure: c => c.SnapshotRescueMaxConsecutiveCycles = 0);

        await h.AckSuccess(199);

        for (int cycle = 1; cycle <= 5; cycle++)
            await h.DriveRescueCycleAsync(expectedInstalls: cycle);

        Assert.Equal(5, h.Transfer.ExportCalls);
        Assert.DoesNotContain(h.Sm.GetSnapshotStatuses(), s => s.RescueNotConverging);
    }

    // ── export retry cache ────────────────────────────────────────────────────

    [Fact]
    public async Task RetryAtSameIndex_ReusesTheExport()
    {
        Harness h = await Harness.BuildLeaderAsync(rejectTerminalChunks: 1);

        // First attempt: exports, and the follower rejects the terminal chunk — a failed transfer
        // with a produced export.
        await h.AckSuccess(199);
        await h.Sm.CheckPartitionLeadershipAsync();
        await h.WaitUntilAsync(
            () => h.Sm.GetSnapshotStatuses().Any(s => s.FailedAttempts >= 1 && !s.InFlight),
            "first attempt failed on the terminal chunk");
        Assert.Equal(1, h.Transfer.ExportCalls);

        // Retry after the backoff, at the same index: the cached export must be replayed —
        // re-running the export per retry is what turned a failing send into an allocation storm.
        h.AdvanceMs(200);
        await h.Sm.CheckPartitionLeadershipAsync();
        await h.WaitForInstallsAsync(1);

        Assert.Equal(1, h.Transfer.ExportCalls);

        // The replayed chunks carry the same bytes and the correct whole-snapshot digest.
        SnapshotRequest last = h.Host.CapturedChunks().Last(c => c.IsLast);
        Assert.Equal(h.Transfer.Blob, last.Data.ToArray());
        Assert.Equal(Convert.ToHexString(SHA256.HashData(h.Transfer.Blob)), last.SnapshotChecksum);
    }

    [Fact]
    public async Task RetryCacheDisabled_ReExportsPerAttempt()
    {
        Harness h = await Harness.BuildLeaderAsync(
            rejectTerminalChunks: 1,
            configure: c => c.SnapshotExportRetryCacheMaxBytes = 0);

        await h.AckSuccess(199);
        await h.Sm.CheckPartitionLeadershipAsync();
        await h.WaitUntilAsync(
            () => h.Sm.GetSnapshotStatuses().Any(s => s.FailedAttempts >= 1 && !s.InFlight),
            "first attempt failed on the terminal chunk");

        h.AdvanceMs(200);
        await h.Sm.CheckPartitionLeadershipAsync();
        await h.WaitForInstallsAsync(1);

        Assert.Equal(2, h.Transfer.ExportCalls);
    }

    // ── live-replica retention floor publisher ────────────────────────────────

    [Fact]
    public async Task Heartbeat_PublishesLiveReplicaRetentionFloor()
    {
        // No transfer registered: the escalation path records "unproducible" and never touches the
        // tracker, so the published floor stays derived from the ack alone.
        Harness h = await Harness.BuildLeaderAsync(withTransfer: false);

        // No positional evidence for the peer yet — nothing constrains retention.
        await h.Sm.CheckPartitionLeadershipAsync();
        Assert.Equal(long.MaxValue, h.Wal.PublishedReplicaFloor);

        // The follower reported frontier 199: the first index it still needs is 200.
        await h.AckSuccess(199);
        await h.Sm.CheckPartitionLeadershipAsync();
        Assert.Equal(200, h.Wal.PublishedReplicaFloor);

        // A peer that is no longer SWIM-Alive must not hold the floor.
        h.Host.Liveness = MemberLivenessState.Dead;
        await h.Sm.CheckPartitionLeadershipAsync();
        Assert.Equal(long.MaxValue, h.Wal.PublishedReplicaFloor);
    }

    [Fact]
    public async Task LagBudgetDisabled_PublishesNothing()
    {
        Harness h = await Harness.BuildLeaderAsync(
            withTransfer: false,
            configure: c => c.CompactionLiveReplicaLagBudget = 0);

        await h.AckSuccess(199);
        await h.Sm.CheckPartitionLeadershipAsync();
        Assert.Equal(0, h.Wal.PublishCalls);
    }

    // ── harness ───────────────────────────────────────────────────────────────

    private sealed class Harness
    {
        public required RaftPartitionStateMachine Sm { get; init; }
        public required RescueHost Host { get; init; }
        public required MutableFloorWal Wal { get; init; }
        public required CountingTransfer Transfer { get; init; }

        private int installs;

        public int Installs => Volatile.Read(ref installs);

        public static Task<Harness> BuildLeaderAsync(
            int rejectTerminalChunks = 0,
            bool withTransfer = true,
            Action<RaftConfiguration>? configure = null)
        {
            CountingTransfer? transfer = withTransfer ? new CountingTransfer() : null;
            RescueHost host = new(transfer, rejectTerminalChunks);
            configure?.Invoke(host.Configuration);
            MutableFloorWal wal = new(InitialFloor, CommitIndex);

            RaftPartitionStateMachine sm = new(host, wal, new NoopSink(), NullLogger<IRaft>.Instance);
            Harness h = new() { Sm = sm, Host = host, Wal = wal, Transfer = transfer ?? new CountingTransfer() };

            return FinishAsync(h, sm);
        }

        private static async Task<Harness> FinishAsync(Harness h, RaftPartitionStateMachine sm)
        {
            IReadOnlyList<RaftLog> logs = await sm.StartRestoreAsync();
            await sm.CompleteRestoreAsync(logs);

            // Mirror the executor's SnapshotInstalled dispatch (RaftPartitionExecutor): the
            // completion is what advances the leader's replication cursors to the boundary, which
            // is what lets the floor overtake the follower AGAIN and the loop continue.
            sm.SetPostToExecutor(r =>
            {
                if (r.Type != RaftRequestType.SnapshotInstalled)
                    return;

                sm.CompleteSnapshotInstalled(r.Endpoint ?? "", r.CommitIndex);
                Interlocked.Increment(ref h.installs);
            });

            sm.SetLeaderForTesting(term: 1);
            return h;
        }

        public void AdvanceMs(long ms) => Host.AdvanceMs(ms);

        /// <summary>The leader's compaction cadence, condensed: the floor jumps past everything the last install seeded.</summary>
        public void AdvanceFloorPastFollower() => Wal.Floor += 100;

        /// <summary>
        /// One full rescue cycle: exit the post-success pause, let the floor overtake the
        /// follower, drive one heartbeat (refusal → escalation → transfer), and wait for the
        /// confirmed install. The first cycle needs no floor advance — the follower starts below
        /// the initial floor.
        /// </summary>
        public async Task DriveRescueCycleAsync(int expectedInstalls)
        {
            if (expectedInstalls > 1)
                AdvanceFloorPastFollower();

            AdvanceMs(200);
            await Sm.CheckPartitionLeadershipAsync();
            await WaitForInstallsAsync(expectedInstalls);
        }

        public Task AckSuccess(long committedIndex) =>
            Sm.CompleteAppendLogsAsync(Follower, Host.HybridLogicalClock.TrySendOrLocalEvent(1),
                RaftOperationStatus.Success, committedIndex).AsTask();

        public Task WaitForInstallsAsync(int n) =>
            WaitUntilAsync(
                () => Installs >= n && Sm.GetSnapshotStatuses().All(s => !s.InFlight),
                $"install #{n} confirmed and the transfer task finished");

        public async Task WaitUntilAsync(Func<bool> condition, string what)
        {
            TimeSpan budget = TestTimeouts.Scale(TimeSpan.FromSeconds(10));
            long started = Stopwatch.GetTimestamp();
            while (!condition())
            {
                if (Stopwatch.GetElapsedTime(started) > budget)
                    Assert.Fail($"timed out waiting for: {what}");
                await Task.Delay(10, TestContext.Current.CancellationToken);
            }
        }
    }

    /// <summary>Whole-partition transfer that counts exports and serves a fixed blob.</summary>
    private sealed class CountingTransfer : IRaftPartitionStateTransfer
    {
        private int exportCalls;

        public byte[] Blob { get; } = [0xAB, 0xCD, 0xEF, 0x01, 0x23, 0x45];

        public int ExportCalls => Volatile.Read(ref exportCalls);

        public Task<Stream> ExportPartitionState(int partitionId, long upToIndex, CancellationToken ct)
        {
            Interlocked.Increment(ref exportCalls);
            return Task.FromResult<Stream>(new MemoryStream(Blob));
        }

        public Task ImportPartitionState(int partitionId, Stream snapshot, CancellationToken ct) =>
            Task.CompletedTask;
    }

    /// <summary>
    /// Host with a test-controlled monotonic clock (Stopwatch units), so the sender's pauses,
    /// backoffs, quiet windows, and probe intervals advance deterministically, and a configurable
    /// count of terminal-chunk rejections to force a retry at the same snapshot index.
    /// </summary>
    private sealed class RescueHost : IRaftPartitionHost
    {
        private readonly CountingTransfer? transfer;
        private readonly List<SnapshotRequest> chunks = [];
        private long nowTicks = 1000L * Stopwatch.Frequency;
        private int rejectTerminalChunksRemaining;

        public MemberLivenessState Liveness { get; set; } = MemberLivenessState.Alive;

        public RescueHost(CountingTransfer? transfer, int rejectTerminalChunks)
        {
            this.transfer = transfer;
            rejectTerminalChunksRemaining = rejectTerminalChunks;
            Configuration = new RaftConfiguration
            {
                NodeId = 1, Host = "leader", Port = 9000, InitialPartitions = 1,
                HeartbeatInterval = TimeSpan.Zero,
                BackfillThreshold = 0,
                MaxBackfillEntriesPerRound = 128,
            };
        }

        public void AdvanceMs(long ms) => Interlocked.Add(ref nowTicks, ms * Stopwatch.Frequency / 1000);

        public long GetMonotonicTimestamp() => Volatile.Read(ref nowTicks);

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
        public MemberLivenessState GetNodeLiveness(string endpoint) => Liveness;

        public IRaftStateMachineTransfer? StateMachineTransfer => null;
        public IRaftSystemStateTransfer? SystemStateTransfer => null;
        public IRaftPartitionStateTransfer? PartitionStateTransfer => transfer;

        public Task<SnapshotResponse> SendInstallSnapshotAsync(RaftNode node, SnapshotRequest request, CancellationToken ct)
        {
            // Copy: a streamed chunk's Data is a view over a reused buffer.
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
                    SnapshotChecksum = request.SnapshotChecksum,
                });

            if (request.IsLast && rejectTerminalChunksRemaining > 0)
            {
                rejectTerminalChunksRemaining--;
                return Task.FromResult(new SnapshotResponse(false));
            }

            return Task.FromResult(new SnapshotResponse(true));
        }

        public IReadOnlyList<SnapshotRequest> CapturedChunks()
        {
            lock (chunks) return [.. chunks];
        }
    }

    /// <summary>
    /// WAL stub with a mutable compaction floor: an anchored read at or below the floor comes back
    /// starting at floor + 1 — the non-contiguous shape the backfill path refuses — and the test
    /// advances the floor to re-create the below-floor condition after each install, exactly what
    /// the leader's own compaction cadence did in the incident. Also records the live-replica
    /// retention floor published by the heartbeat round.
    /// </summary>
    private sealed class MutableFloorWal : IRaftWalFacade
    {
        private readonly long commitIndex;

        public long Floor;
        public long PublishedReplicaFloor = long.MaxValue;
        public int PublishCalls;

        public MutableFloorWal(long floor, long commitIndex)
        {
            Floor = floor;
            this.commitIndex = commitIndex;
        }

        public ValueTask<IReadOnlyList<RaftLog>> LoadRestoreLogsAsync() =>
            ValueTask.FromResult<IReadOnlyList<RaftLog>>([]);
        public ValueTask CompleteRestoreAsync(IReadOnlyList<RaftLog> logs) => ValueTask.CompletedTask;
        public ValueTask<long> GetMaxLogAsync() => ValueTask.FromResult(commitIndex);
        public ValueTask<long> TruncateLogsAfterAsync(long afterLogId) => ValueTask.FromResult(afterLogId);
        public ValueTask<long> GetCurrentTermAsync() => ValueTask.FromResult(1L);

        public ValueTask<List<RaftLog>> GetRangeAsync(long startLogIndex, int maxEntries)
        {
            List<RaftLog> batch = [];
            long first = Math.Max(startLogIndex, Volatile.Read(ref Floor) + 1);
            for (long id = first; id < first + 3 && id <= commitIndex; id++)
                batch.Add(new() { Id = id, Term = 1, Type = RaftLogType.Committed, LogType = "test" });

            return ValueTask.FromResult(batch);
        }

        public ValueTask<long> GetAnyTermAtAsync(long logIndex) => ValueTask.FromResult(1L);
        public ValueTask<long> GetLastCheckpointAsync() => ValueTask.FromResult(Volatile.Read(ref Floor));
        public long GetCommitIndex() => commitIndex;
        public WALWriteOperation EnqueuePropose(long term, List<RaftLog> logs, HLCTimestamp ts, bool autoCommit) => MakeNoOp();
        public WALWriteOperation EnqueueCommit(List<RaftLog> logs) => MakeNoOp();
        public WALWriteOperation EnqueueRollback(List<RaftLog> logs) => MakeNoOp();
        public WALWriteOperation? EnqueueProposeOrCommit(List<RaftLog>? logs, HLCTimestamp timestamp = default, string? endpoint = null, long term = -1) =>
            logs is null ? null : MakeNoOp();
        public void NotifyCommitted() { }

        public void SetLiveReplicaRetentionFloor(long floor)
        {
            Volatile.Write(ref PublishedReplicaFloor, floor);
            Interlocked.Increment(ref PublishCalls);
        }

        private static WALWriteOperation MakeNoOp() =>
            new(_ => { }, 0, WALWriteOperationType.LeaderPropose, (0, []));
    }

    private sealed class NoopSink : IRaftOperationReplySink
    {
        public void TryComplete(ulong correlationId, RaftResponse response) { }
    }
}
