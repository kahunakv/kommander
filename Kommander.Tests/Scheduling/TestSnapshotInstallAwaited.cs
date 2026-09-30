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
/// The sender's half of "the install is a step of its own", against a real
/// <see cref="SnapshotReceiver"/>: a leader state machine whose host delivers every chunk and every
/// status question to the receiver, and an install callback the test holds open.
///
/// <para>The loop these pin shut (CamusDB fault soaks rl4b and rl5): the terminal chunk's call was
/// held open for the install and expired after <see cref="RaftConfiguration.SnapshotChunkAckTimeout"/>;
/// the sender recorded a rejected last chunk; the next refused backfill escalated at the leader's
/// newer checkpoint, exported the partition again and opened a new session on a follower that was
/// still importing the first snapshot. Six exports in four minutes ended with the follower out of
/// memory.</para>
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
public class TestSnapshotInstallAwaited
{
    private const string Follower = "follower:9001";
    private const string PreviousLeader = "previous-leader:9000";
    private const long Floor = 50;

    /// <summary>
    /// An install that outlasts the chunk-acknowledgement bound several times over is still one
    /// transfer: one export, one receive session, no failed attempt. While it runs the transfer is
    /// reported as waiting for the install.
    /// </summary>
    [Fact]
    public async Task InstallThatOutlastsTheChunkAckTimeout_IsOneTransfer_WithNoFailedAttempt()
    {
        Harness h = await Harness.BuildLeaderAsync(c => c.SnapshotChunkAckTimeout = TimeSpan.FromMilliseconds(100));
        h.Installer.Hold();

        await h.AckSuccess();
        await h.WaitUntilAsync(() => h.Statuses().Any(s => s.AwaitingInstall), "the transfer waits for the install");

        // Four chunk-acknowledgement bounds later the install is still running and nothing has failed.
        await Task.Delay(400, TestContext.Current.CancellationToken);

        RaftSnapshotStatus waiting = Assert.Single(h.Statuses());
        Assert.True(waiting.InFlight);
        Assert.True(waiting.AwaitingInstall);
        Assert.Equal(Floor, waiting.AwaitingInstallIndex);
        Assert.Equal(0, waiting.FailedAttempts);

        h.Installer.Release();
        await h.WaitUntilAsync(() => h.Statuses().Count == 0, "the transfer completes once the install does");

        Assert.Equal(1, h.Transfer.ExportCalls);
        Assert.Equal(1, h.Host.SessionsOpened);
        Assert.Equal(1, h.Installer.Started);
        Assert.Equal(Floor, h.FollowerCursor());
    }

    /// <summary>
    /// A retry against a running install waits for that install and does not export again. The first
    /// attempt is abandoned mid-wait (the install reports no progress for the step timeout); the
    /// leader's checkpoint then moves, so the next escalation carries a newer index — the shape that
    /// used to open a second session at that index. It must attach to the running install: no second
    /// export, no second session, and the follower's cursor lands on the index that was installed.
    /// </summary>
    [Fact]
    public async Task RetryAgainstARunningInstall_WaitsForIt_AndDoesNotExportAgain()
    {
        Harness h = await Harness.BuildLeaderAsync(c => c.SnapshotTransferStepTimeout = TimeSpan.FromMilliseconds(300));
        h.Installer.Hold();

        await h.AckSuccess();
        await h.WaitUntilAsync(
            () => h.Statuses().Any(s => !s.InFlight && s.FailedAttempts >= 1),
            "the first attempt gives up waiting");

        RaftSnapshotStatus abandoned = Assert.Single(h.Statuses());
        Assert.Contains("made no progress", abandoned.LastError);
        Assert.Contains("SnapshotTransferStepTimeout", abandoned.LastError);
        Assert.Equal(1, h.Transfer.ExportCalls);

        // The leader checkpoints again while the follower is still importing.
        h.Wal.Checkpoint = 80;

        // The backoff expires and refused acks escalate again, now at index 80.
        await h.AckUntilAsync(() => h.Statuses().Any(s => s.InFlight && s.AwaitingInstall), "the retry attaches to the running install");

        RaftSnapshotStatus attached = Assert.Single(h.Statuses());
        Assert.Equal(Floor, attached.AwaitingInstallIndex);
        Assert.Equal(1, h.Transfer.ExportCalls);
        Assert.Equal(1, h.Host.SessionsOpened);
        Assert.Equal(1, h.Installer.Started);

        h.Installer.Release();
        await h.WaitUntilAsync(() => h.Statuses().Count == 0, "the retry completes with the install's outcome");

        Assert.Equal(1, h.Transfer.ExportCalls);
        Assert.Equal(1, h.Host.SessionsOpened);
        Assert.Equal(1, h.Installer.Started);
        // The index that was installed, not the one the retry was started for.
        Assert.Equal(Floor, h.FollowerCursor());
    }

    /// <summary>
    /// The terminal chunk's answer is lost (the call expired, as <c>DeadlineExceeded</c> did) after
    /// the receiver had staged the chunk and started the install. The sender sees a bare rejection,
    /// and the install then finishes with no call open. The next attempt names that session in its
    /// first question and adopts the outcome: nothing is sent twice.
    /// </summary>
    [Fact]
    public async Task UnansweredTerminalChunk_IsAskedAbout_BeforeAnythingIsSentAgain()
    {
        Harness h = await Harness.BuildLeaderAsync();
        h.Host.LoseNextTerminalAnswer = true;

        await h.AckSuccess();
        await h.WaitUntilAsync(
            () => h.Statuses().Any(s => !s.InFlight && s.FailedAttempts >= 1),
            "the lost answer is recorded as a failed attempt");
        await h.WaitUntilAsync(() => h.Receiver.ActiveInstallCount == 0 && h.Installer.Started == 1, "the install finishes unobserved");

        await h.AckUntilAsync(() => h.Statuses().Count == 0, "the next attempt adopts the finished install");

        Assert.Equal(1, h.Transfer.ExportCalls);
        Assert.Equal(1, h.Host.SessionsOpened);
        Assert.Equal(1, h.Installer.Started);
        Assert.Equal(Floor, h.FollowerCursor());
    }

    /// <summary>
    /// A leader change mid-install. The new leader has no memory of the transfer; its first
    /// escalation finds the previous leader's install running on the follower, waits for it, and
    /// exports nothing. The follower's cursor advances to the index that install carried.
    /// </summary>
    [Fact]
    public async Task NewLeader_FindsThePreviousLeadersInstallRunning_WaitsForIt_AndExportsNothing()
    {
        Harness h = await Harness.BuildLeaderAsync();
        h.Installer.Hold();

        // The previous leader's transfer, at its own (older) checkpoint.
        byte[] payload = [1, 2, 3];
        SnapshotResponse staged = await h.Receiver.ReceiveInstallSnapshot(new SnapshotRequest
        {
            SessionId = "previous-leaders-session",
            PartitionId = 1,
            SnapshotIndex = 40,
            FollowerEndpoint = Follower,
            LeaderEndpoint = PreviousLeader,
            LeaderTerm = 1,
            LastIncludedTerm = 1,
            ChunkIndex = 0,
            IsLast = true,
            Data = payload,
            SnapshotChecksum = Convert.ToHexString(SHA256.HashData(payload)),
            InstallPolling = true,
        }, TestContext.Current.CancellationToken);
        Assert.Equal(SnapshotInstallOutcome.InstallPending, staged.Outcome);

        await h.AckSuccess();
        await h.WaitUntilAsync(() => h.Statuses().Any(s => s.AwaitingInstall), "the new leader waits for the running install");

        RaftSnapshotStatus waiting = Assert.Single(h.Statuses());
        Assert.Equal(40, waiting.AwaitingInstallIndex);
        Assert.Equal(0, h.Transfer.ExportCalls);
        Assert.Equal(0, h.Host.SessionsOpened);

        h.Installer.Release();
        await h.WaitUntilAsync(() => h.Statuses().Count == 0, "the wait ends with the install");

        Assert.Equal(0, h.Transfer.ExportCalls);
        Assert.Equal(0, h.Host.SessionsOpened);
        Assert.Equal(40, h.FollowerCursor());
    }

    /// <summary>
    /// The follower restarts while the leader waits: its receiver no longer knows the install. That
    /// is a recorded failure, not a wait that never ends, and the next attempt sends the snapshot.
    /// </summary>
    [Fact]
    public async Task FollowerThatNoLongerKnowsTheInstall_IsSentTheSnapshotAgain()
    {
        Harness h = await Harness.BuildLeaderAsync();
        h.Installer.Hold();

        await h.AckSuccess();
        await h.WaitUntilAsync(() => h.Statuses().Any(s => s.AwaitingInstall), "the transfer waits for the install");

        GatedInstaller afterRestart = new();
        h.Host.Receiver = Harness.NewReceiver(afterRestart);

        await h.WaitUntilAsync(
            () => h.Statuses().Any(s => !s.InFlight && s.FailedAttempts >= 1),
            "the lost install is recorded");
        Assert.Contains("no longer reports", Assert.Single(h.Statuses()).LastError);

        await h.AckUntilAsync(() => h.Statuses().Count == 0, "the snapshot is sent again and installs");

        Assert.Equal(2, h.Host.SessionsOpened);
        Assert.Equal(1, afterRestart.Started);

        h.Installer.Release();
    }

    /// <summary>
    /// An install the follower reports as failed ends the wait with a failure that says so, and the
    /// transfer is retried on the normal backoff.
    /// </summary>
    [Fact]
    public async Task InstallThatFailsOnTheFollower_IsRecorded_AndRetried()
    {
        Harness h = await Harness.BuildLeaderAsync();
        h.Installer.Hold();
        h.Installer.Outcome = SnapshotInstallOutcome.Rejected;

        await h.AckSuccess();
        await h.WaitUntilAsync(() => h.Statuses().Any(s => s.AwaitingInstall), "the transfer waits for the install");

        h.Installer.Release();
        await h.WaitUntilAsync(
            () => h.Statuses().Any(s => !s.InFlight && s.FailedAttempts >= 1),
            "the failed install is recorded");
        Assert.Contains("failed on the follower", Assert.Single(h.Statuses()).LastError);

        h.Installer.Outcome = SnapshotInstallOutcome.Installed;
        await h.AckUntilAsync(() => h.Statuses().Count == 0, "the retry installs");

        Assert.Equal(2, h.Installer.Started);
        Assert.Equal(Floor, h.FollowerCursor());
    }

    /// <summary>
    /// A receiver from before the pending outcome: it refuses the status question and holds the
    /// terminal chunk until its install is done. A sender that polls must still seed it.
    /// </summary>
    [Fact]
    public async Task ReceiverThatPredatesThePendingOutcome_IsStillSeeded()
    {
        Harness h = await Harness.BuildLeaderAsync();
        h.Host.ReceiverPredatesPolling = true;

        await h.AckSuccess();
        await h.WaitUntilAsync(() => h.Installer.Started == 1, "the install ran");
        await h.WaitUntilAsync(() => h.Statuses().Count == 0, "the transfer completed");

        Assert.Equal(1, h.Transfer.ExportCalls);
        Assert.Equal(Floor, h.FollowerCursor());
    }

    // ── harness ───────────────────────────────────────────────────────────────

    private sealed class Harness
    {
        public required RaftPartitionStateMachine Sm { get; init; }
        public required ReceiverHost Host { get; init; }
        public required CheckpointWal Wal { get; init; }
        public required CountingTransfer Transfer { get; init; }
        public required GatedInstaller Installer { get; init; }

        public SnapshotReceiver Receiver => Host.Receiver;

        /// <summary>
        /// Confirmed installs the background transfer posted for the executor. There is no executor
        /// here, and the state machine is single-threaded, so they are applied on the test's thread.
        /// </summary>
        private readonly global::System.Collections.Concurrent.ConcurrentQueue<RaftRequest> posted = new();

        private void ApplyPosted()
        {
            while (posted.TryDequeue(out RaftRequest? request))
            {
                if (request.Type == RaftRequestType.SnapshotInstalled)
                    Sm.CompleteSnapshotInstalled(request.Endpoint ?? "", request.CommitIndex);
            }
        }

        /// <summary>The follower's replication cursor on the leader, after every posted install was applied.</summary>
        public long FollowerCursor()
        {
            ApplyPosted();
            return Sm.GetFollowerCommittedIndex(Follower);
        }

        public IReadOnlyList<RaftSnapshotStatus> Statuses() => Sm.GetSnapshotStatuses();

        public static SnapshotReceiver NewReceiver(GatedInstaller installer) =>
            new(
                isDisposed: () => false,
                installOnExecutor: installer.Install,
                logger: NullLogger<IRaft>.Instance,
                localEndpoint: Follower,
                sessionTtlTicks: SnapshotReceiver.TicksForDuration(TimeSpan.FromSeconds(30)),
                maxPendingSessions: 8,
                maxPendingBytes: 1_000_000,
                getMonotonicTimestamp: Stopwatch.GetTimestamp);

        public static async Task<Harness> BuildLeaderAsync(Action<RaftConfiguration>? configure = null)
        {
            CountingTransfer transfer = new();
            GatedInstaller installer = new();
            ReceiverHost host = new(transfer, NewReceiver(installer));
            configure?.Invoke(host.Configuration);

            CheckpointWal wal = new(checkpoint: Floor, commitIndex: 100);

            RaftPartitionStateMachine sm = new(host, wal, new NoopSink(), NullLogger<IRaft>.Instance);
            IReadOnlyList<RaftLog> logs = await sm.StartRestoreAsync();
            await sm.CompleteRestoreAsync(logs);

            sm.SetLeaderForTesting(term: 1);

            Harness harness = new() { Sm = sm, Host = host, Wal = wal, Transfer = transfer, Installer = installer };
            sm.SetPostToExecutor(harness.posted.Enqueue);
            return harness;
        }

        /// <summary>A follower ack at frontier 0: below the floor, so its backfill is refused and escalates.</summary>
        public Task AckSuccess()
        {
            ApplyPosted();
            return Sm.CompleteAppendLogsAsync(Follower, Host.HybridLogicalClock.TrySendOrLocalEvent(1),
                RaftOperationStatus.Success, committedIndex: 0).AsTask();
        }

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

        /// <summary>
        /// Keeps the follower's refused acks coming until <paramref name="condition"/> holds, so the
        /// escalation fires as soon as the backoff allows. Stops acking the moment it does: a further
        /// ack could start a transfer the test did not ask for.
        /// </summary>
        public async Task AckUntilAsync(Func<bool> condition, string what)
        {
            TimeSpan budget = TestTimeouts.Scale(TimeSpan.FromSeconds(10));
            long started = Stopwatch.GetTimestamp();
            while (!condition())
            {
                if (Stopwatch.GetElapsedTime(started) > budget)
                    Assert.Fail($"timed out waiting for: {what}");
                await AckSuccess();
                await Task.Delay(20, TestContext.Current.CancellationToken);
            }
        }
    }

    /// <summary>
    /// Stands in for the partition executor's install path. While held, an install waits for
    /// <see cref="Release"/>; it then drains the staged snapshot and answers <see cref="Outcome"/>.
    /// </summary>
    private sealed class GatedInstaller
    {
        private volatile TaskCompletionSource? gate;
        private int started;

        public int Started => Volatile.Read(ref started);

        public volatile SnapshotInstallOutcome Outcome = SnapshotInstallOutcome.Installed;

        public void Hold() => gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        public void Release() => gate?.TrySetResult();

        public async Task<SnapshotResponse> Install(SnapshotInstallRequest request)
        {
            Interlocked.Increment(ref started);

            TaskCompletionSource? held = gate;
            if (held is not null)
                await held.Task.ConfigureAwait(false);

            await request.Snapshot.CopyToAsync(Stream.Null).ConfigureAwait(false);
            return new SnapshotResponse(Outcome);
        }
    }

    private sealed class CountingTransfer : IRaftPartitionStateTransfer
    {
        private int exportCalls;

        public int ExportCalls => Volatile.Read(ref exportCalls);

        public Task<Stream> ExportPartitionState(int partitionId, long upToIndex, CancellationToken ct)
        {
            Interlocked.Increment(ref exportCalls);
            return Task.FromResult<Stream>(new MemoryStream([0xAB, 0xCD, 0xEF]));
        }

        public Task ImportPartitionState(int partitionId, Stream snapshot, CancellationToken ct) =>
            Task.CompletedTask;
    }

    /// <summary>A leader-side host whose snapshot calls reach a real <see cref="SnapshotReceiver"/>.</summary>
    private sealed class ReceiverHost : IRaftPartitionHost
    {
        private readonly IRaftPartitionStateTransfer transfer;
        private int sessionsOpened;

        public ReceiverHost(IRaftPartitionStateTransfer transfer, SnapshotReceiver receiver)
        {
            this.transfer = transfer;
            Receiver = receiver;
            Configuration = new RaftConfiguration
            {
                NodeId = 1, Host = "leader", Port = 9000, InitialPartitions = 1,
                HeartbeatInterval = TimeSpan.Zero, RecentHeartbeat = TimeSpan.Zero,
                BackfillThreshold = 0,
                MaxBackfillEntriesPerRound = 128,
            };
        }

        /// <summary>The follower's receiver. Replaced to model a follower that restarted.</summary>
        public volatile SnapshotReceiver Receiver;

        /// <summary>Deliver the next terminal chunk, then answer as a call that expired would.</summary>
        public volatile bool LoseNextTerminalAnswer;

        /// <summary>Answer as a receiver from before the pending outcome and the status question.</summary>
        public volatile bool ReceiverPredatesPolling;

        /// <summary>Chunk-0 sends: one per receive session this leader tried to open.</summary>
        public int SessionsOpened => Volatile.Read(ref sessionsOpened);

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

        public async Task<SnapshotResponse> SendInstallSnapshotAsync(RaftNode node, SnapshotRequest request, CancellationToken ct)
        {
            if (request.ChunkIndex == 0)
                Interlocked.Increment(ref sessionsOpened);

            SnapshotRequest delivered = ReceiverPredatesPolling ? WithoutPolling(request) : request;

            SnapshotResponse answer = await Receiver.ReceiveInstallSnapshot(delivered, CancellationToken.None).ConfigureAwait(false);

            if (request.IsLast && LoseNextTerminalAnswer)
            {
                LoseNextTerminalAnswer = false;
                return new SnapshotResponse(false);
            }

            return ReceiverPredatesPolling ? new SnapshotResponse(answer.Outcome) : answer;
        }

        public Task<SnapshotResponse> QuerySnapshotInstallAsync(RaftNode node, SnapshotRequest query, CancellationToken ct) =>
            ReceiverPredatesPolling
                ? Task.FromResult(new SnapshotResponse(false))
                : Receiver.ReceiveInstallSnapshot(query, CancellationToken.None);

        /// <summary>The chunk as a receiver that does not know the polling field reads it.</summary>
        private static SnapshotRequest WithoutPolling(SnapshotRequest request) =>
            new()
            {
                SessionId = request.SessionId,
                PartitionId = request.PartitionId,
                SnapshotIndex = request.SnapshotIndex,
                FollowerEndpoint = request.FollowerEndpoint,
                LeaderEndpoint = request.LeaderEndpoint,
                LeaderTerm = request.LeaderTerm,
                LastIncludedTerm = request.LastIncludedTerm,
                ChunkIndex = request.ChunkIndex,
                IsLast = request.IsLast,
                Data = request.Data,
                Kind = request.Kind,
                SnapshotChecksum = request.SnapshotChecksum,
                Forced = request.Forced,
            };
    }

    /// <summary>
    /// WAL stub with a compaction floor at its checkpoint: every range read comes back empty, so a
    /// follower below the checkpoint is refused and escalates to a snapshot at it.
    /// </summary>
    private sealed class CheckpointWal : IRaftWalFacade
    {
        private readonly long commitIndex;
        private long checkpoint;

        public CheckpointWal(long checkpoint, long commitIndex)
        {
            this.checkpoint = checkpoint;
            this.commitIndex = commitIndex;
        }

        public long Checkpoint
        {
            get => Volatile.Read(ref checkpoint);
            set => Volatile.Write(ref checkpoint, value);
        }

        public ValueTask<IReadOnlyList<RaftLog>> LoadRestoreLogsAsync() =>
            ValueTask.FromResult<IReadOnlyList<RaftLog>>([]);
        public ValueTask CompleteRestoreAsync(IReadOnlyList<RaftLog> logs) => ValueTask.CompletedTask;
        public ValueTask<long> GetMaxLogAsync() => ValueTask.FromResult(commitIndex);
        public ValueTask<long> TruncateLogsAfterAsync(long afterLogId) => ValueTask.FromResult(afterLogId);
        public ValueTask<long> GetCurrentTermAsync() => ValueTask.FromResult(1L);
        public ValueTask<List<RaftLog>> GetRangeAsync(long startLogIndex, int maxEntries) =>
            ValueTask.FromResult(new List<RaftLog>());
        public ValueTask<long> GetAnyTermAtAsync(long logIndex) => ValueTask.FromResult(1L);
        public ValueTask<long> GetLastCheckpointAsync() => ValueTask.FromResult(Checkpoint);
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
