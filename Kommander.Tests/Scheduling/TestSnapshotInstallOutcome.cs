using System.Collections.Concurrent;
using System.Diagnostics;
using Kommander.Data;
using Kommander.Gossip;
using Kommander.Scheduling;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL.Data;
using Microsoft.Extensions.Logging;

namespace Kommander.Tests.Scheduling;

/// <summary>
/// Sender-side contract of the typed snapshot install reply (<see cref="SnapshotInstallOutcome"/>).
/// The receiver's terminal answer is the only statement about installation, so the sender:
///
/// <list type="bullet">
///   <item>logs "the follower is seeded" ONLY for <see cref="SnapshotInstallOutcome.Installed"/>;</item>
///   <item>logs a skip, and still advances its replication cursors, for
///         <see cref="SnapshotInstallOutcome.SkippedAlreadyCovered"/>;</item>
///   <item>records a failure (no cursor advance, no "seeded") when the terminal chunk comes back
///         as a merely staged chunk — a receiver that never ran the install.</item>
/// </list>
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
public class TestSnapshotInstallOutcome
{
    private const string Follower = "follower:9001";

    [Fact]
    public async Task TerminalChunkInstalled_LogsSeeded_AndAdvancesTheCursor()
    {
        Harness h = await Harness.BuildLeaderAsync(SnapshotInstallOutcome.Installed);

        await h.AckSuccess();
        await h.WaitUntilAsync(() => h.Posted.Any(r => r.Type == RaftRequestType.SnapshotInstalled), "cursor advance posted");

        Assert.Equal(1, h.Logger.Count(LogLevel.Warning, "the follower is seeded"));
        Assert.Equal(0, h.Logger.Count(LogLevel.Warning, "skipped by the receiver"));
        await h.WaitUntilAsync(() => h.Sm.GetSnapshotStatuses().Count == 0, "status cleared after success");
    }

    [Fact]
    public async Task TerminalChunkSkippedAlreadyCovered_LogsTheSkip_NotSeeded_AndAdvancesTheCursor()
    {
        Harness h = await Harness.BuildLeaderAsync(SnapshotInstallOutcome.SkippedAlreadyCovered);

        await h.AckSuccess();
        await h.WaitUntilAsync(() => h.Posted.Any(r => r.Type == RaftRequestType.SnapshotInstalled), "cursor advance posted");

        Assert.Equal(1, h.Logger.Count(LogLevel.Warning, "skipped by the receiver as already covered"));
        Assert.Equal(0, h.Logger.Count(LogLevel.Warning, "the follower is seeded"));
        await h.WaitUntilAsync(() => h.Sm.GetSnapshotStatuses().Count == 0, "status cleared after success");
    }

    [Fact]
    public async Task TerminalChunkAcknowledgedWithoutInstall_IsAFailure_NotASeededFollower()
    {
        Harness h = await Harness.BuildLeaderAsync(SnapshotInstallOutcome.ChunkAccepted);

        await h.AckSuccess();
        await h.WaitUntilAsync(
            () => h.Sm.GetSnapshotStatuses().Any(s => !s.InFlight && s.FailedAttempts >= 1),
            "failure recorded");

        RaftSnapshotStatus status = Assert.Single(h.Sm.GetSnapshotStatuses());
        Assert.Equal(Follower, status.FollowerEndpoint);
        Assert.Contains("without an install outcome", status.LastError);
        Assert.Equal(0, h.Logger.Count(LogLevel.Warning, "the follower is seeded"));
        Assert.DoesNotContain(h.Posted, r => r.Type == RaftRequestType.SnapshotInstalled);
    }

    [Fact]
    public void LegacyBoolReply_MapsToInstalledOrRejected()
    {
        Assert.Equal(SnapshotInstallOutcome.Installed, new SnapshotResponse(true).Outcome);
        Assert.Equal(SnapshotInstallOutcome.Rejected, new SnapshotResponse(false).Outcome);
        Assert.True(new SnapshotResponse(SnapshotInstallOutcome.ChunkAccepted).Success);
        Assert.True(new SnapshotResponse(SnapshotInstallOutcome.SkippedAlreadyCovered).Success);
        Assert.False(new SnapshotResponse(SnapshotInstallOutcome.Rejected).Success);
        // A wire peer that sends no outcome reads as Rejected, never as a silent success.
        Assert.Equal(SnapshotInstallOutcome.Rejected, new SnapshotResponse().Outcome);
    }

    // ── harness ───────────────────────────────────────────────────────────────

    private sealed class Harness
    {
        public required RaftPartitionStateMachine Sm { get; init; }
        public required OutcomeHost Host { get; init; }
        public required LevelCountingLogger Logger { get; init; }
        public ConcurrentBag<RaftRequest> Posted { get; } = [];

        public static async Task<Harness> BuildLeaderAsync(SnapshotInstallOutcome terminalOutcome)
        {
            OutcomeHost host = new(new InstantTransfer(), terminalOutcome);
            FloorWal wal = new(floor: 50, commitIndex: 100);
            LevelCountingLogger logger = new();

            RaftPartitionStateMachine sm = new(host, wal, new NoopSink(), logger);
            IReadOnlyList<RaftLog> logs = await sm.StartRestoreAsync();
            await sm.CompleteRestoreAsync(logs);
            Harness h = new() { Sm = sm, Host = host, Logger = logger };
            sm.SetPostToExecutor(req => h.Posted.Add(req));
            sm.SetLeaderForTesting(term: 1);
            return h;
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

    private sealed class InstantTransfer : IRaftPartitionStateTransfer
    {
        public Task<Stream> ExportPartitionState(int partitionId, long upToIndex, CancellationToken ct) =>
            Task.FromResult<Stream>(new MemoryStream([0xAB, 0xCD]));

        public Task ImportPartitionState(int partitionId, Stream snapshot, CancellationToken ct) =>
            Task.CompletedTask;
    }

    private sealed class OutcomeHost : IRaftPartitionHost
    {
        private readonly IRaftPartitionStateTransfer transfer;
        private readonly SnapshotInstallOutcome terminalOutcome;

        public OutcomeHost(IRaftPartitionStateTransfer transfer, SnapshotInstallOutcome terminalOutcome)
        {
            this.transfer = transfer;
            this.terminalOutcome = terminalOutcome;
            Configuration = new RaftConfiguration
            {
                NodeId = 1, Host = "leader", Port = 9000, InitialPartitions = 1,
                HeartbeatInterval = TimeSpan.Zero, RecentHeartbeat = TimeSpan.Zero,
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

        public Task<SnapshotResponse> SendInstallSnapshotAsync(RaftNode node, SnapshotRequest request, CancellationToken ct) =>
            Task.FromResult(new SnapshotResponse(request.IsLast ? terminalOutcome : SnapshotInstallOutcome.ChunkAccepted));
    }

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

    private sealed class LevelCountingLogger : ILogger<IRaft>
    {
        private readonly List<(LogLevel Level, string Message)> messages = [];
        private readonly object sync = new();

        public int Count(LogLevel level, string substring)
        {
            lock (sync)
                return messages.Count(m => m.Level == level && m.Message.Contains(substring));
        }

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => logLevel != LogLevel.None;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
                                Func<TState, Exception?, string> formatter)
        {
            lock (sync)
                messages.Add((logLevel, formatter(state, exception)));
        }
    }
}
