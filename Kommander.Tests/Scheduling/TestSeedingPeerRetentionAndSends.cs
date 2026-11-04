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
/// What a leader does for a follower it is seeding by snapshot, against a real
/// <see cref="SnapshotReceiver"/> whose install the test holds open: it sends the follower nothing
/// entry-carrying while the transfer is in flight, and it holds log retention from the index the
/// follower is seeded at — exempt from the rate-scaled budget — until the follower has caught up from
/// the installed boundary.
///
/// <para>The loop these pin shut (CamusDB fault soak, 2026-11-04): the leader streamed live entries at a
/// follower whose executor its install held (two million queued entries, the node out of memory), and
/// the install outlasted the 1,000,000-entry budget, so the first backfill after it was
/// non-contiguous and the leader installed again.</para>
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
public class TestSeedingPeerRetentionAndSends
{
    private const string Follower = "follower:9001";
    private const long Floor = 50;
    private const long Commit = 100;

    /// <summary>
    /// While the install runs the follower is heartbeated and nothing else: no live proposal, no
    /// backfill batch. The retention floor holds at the seeding index + 1 although the follower's last
    /// reported position was 0, and the published budget covers the hold although the rate budget is
    /// far smaller.
    /// </summary>
    [Fact]
    public async Task WhileTheInstallRuns_NoEntriesAreSent_AndRetentionHoldsFromTheSeedingIndex()
    {
        Harness h = await Harness.BuildLeaderAsync(c =>
        {
            c.CompactionLiveReplicaLagBudget = 10;
            c.CompactionLiveReplicaLagWindow = TimeSpan.Zero;
        });
        h.Installer.Hold();

        await h.AckSuccess();
        await h.WaitUntilAsync(() => h.Statuses().Any(s => s.AwaitingInstall), "the transfer waits for the install");

        int entriesBefore = h.Host.EntryBatchesSent;
        int heartbeatsBefore = h.Host.HeartbeatsSent;

        for (int round = 0; round < 5; round++)
        {
            await h.Sm.CheckPartitionLeadershipAsync();
            await h.ProposeAsync();
        }

        Assert.Equal(entriesBefore, h.Host.EntryBatchesSent);
        Assert.True(h.Host.HeartbeatsSent > heartbeatsBefore, "the follower must keep being heartbeated");

        // Seeded at the leader's checkpoint (50): the follower needs 51 first, whatever it last reported.
        Assert.Equal(Floor + 1, h.Wal.PublishedReplicaFloor);
        // The rate budget is 10 entries; the hold needs commit − need + 1 = 100 − 51 + 1 = 50.
        Assert.Equal(Commit - (Floor + 1) + 1, h.Wal.PublishedBudget);
        Assert.Equal(Commit - (Floor + 1) + 1, h.Host.LastAppendLogs!.RetentionBudget);

        h.Installer.Release();
        await h.WaitUntilAsync(() => h.Statuses().Count == 0, "the transfer completes once the install does");
    }

    /// <summary>
    /// After the install the follower catches up from the boundary over the log. Its need keeps the
    /// budget raised while its install is on record, and the budget falls back to the rate budget once
    /// it has caught up.
    /// </summary>
    [Fact]
    public async Task AfterTheInstall_TheHoldFollowsTheFollowersNeed_UntilItCatchesUp()
    {
        Harness h = await Harness.BuildLeaderAsync(c =>
        {
            c.CompactionLiveReplicaLagBudget = 10;
            c.CompactionLiveReplicaLagWindow = TimeSpan.Zero;
        });

        await h.AckSuccess();
        await h.WaitUntilAsync(() => h.Statuses().Count == 0 && h.FollowerCursor() == Floor, "the install completes");

        // Confirmed at 50: the follower needs 51 and the hold covers it.
        await h.Sm.CheckPartitionLeadershipAsync();
        Assert.Equal(Floor + 1, h.Wal.PublishedReplicaFloor);
        Assert.Equal(Commit - (Floor + 1) + 1, h.Wal.PublishedBudget);

        // Entries flow again now that no transfer is in flight.
        int entriesBefore = h.Host.EntryBatchesSent;
        await h.ProposeAsync();
        Assert.True(h.Host.EntryBatchesSent > entriesBefore, "entries resume once the install is confirmed");

        // Halfway up: the hold follows the need.
        await h.AckSuccess(committedIndex: 80, durableIndex: 80);
        await h.Sm.CheckPartitionLeadershipAsync();
        Assert.Equal(81, h.Wal.PublishedReplicaFloor);
        Assert.Equal(Commit - 81 + 1, h.Wal.PublishedBudget);

        // Caught up: nothing is held past the rate budget any more.
        await h.AckSuccess(committedIndex: Commit, durableIndex: Commit);
        await h.Sm.CheckPartitionLeadershipAsync();
        Assert.Equal(Commit + 1, h.Wal.PublishedReplicaFloor);
        Assert.Equal(10, h.Wal.PublishedBudget);
    }

    // ── harness ───────────────────────────────────────────────────────────────

    private sealed class Harness
    {
        public required RaftPartitionStateMachine Sm { get; init; }
        public required ReceiverHost Host { get; init; }
        public required FloorRecordingCheckpointWal Wal { get; init; }
        public required GatedInstaller Installer { get; init; }

        private readonly global::System.Collections.Concurrent.ConcurrentQueue<RaftRequest> posted = new();

        private long nextLogId = Commit + 1;

        private void ApplyPosted()
        {
            while (posted.TryDequeue(out RaftRequest? request))
            {
                if (request.Type == RaftRequestType.SnapshotInstalled)
                    Sm.CompleteSnapshotInstalled(request.Endpoint ?? "", request.CommitIndex);
            }
        }

        public long FollowerCursor()
        {
            ApplyPosted();
            return Sm.GetFollowerCommittedIndex(Follower);
        }

        public IReadOnlyList<RaftSnapshotStatus> Statuses() => Sm.GetSnapshotStatuses();

        public static async Task<Harness> BuildLeaderAsync(Action<RaftConfiguration>? configure = null)
        {
            GatedInstaller installer = new();
            SnapshotReceiver receiver = new(
                isDisposed: () => false,
                installOnExecutor: installer.Install,
                logger: NullLogger<IRaft>.Instance,
                localEndpoint: Follower,
                sessionTtlTicks: SnapshotReceiver.TicksForDuration(TimeSpan.FromSeconds(30)),
                maxPendingSessions: 8,
                maxPendingBytes: 1_000_000,
                getMonotonicTimestamp: Stopwatch.GetTimestamp);

            ReceiverHost host = new(new CountingTransfer(), receiver);
            configure?.Invoke(host.Configuration);

            FloorRecordingCheckpointWal wal = new(checkpoint: Floor, commitIndex: Commit);

            RaftPartitionStateMachine sm = new(host, wal, new NoopSink(), NullLogger<IRaft>.Instance);
            IReadOnlyList<RaftLog> logs = await sm.StartRestoreAsync();
            await sm.CompleteRestoreAsync(logs);

            sm.SetLeaderForTesting(term: 1);

            Harness harness = new() { Sm = sm, Host = host, Wal = wal, Installer = installer };
            sm.SetPostToExecutor(harness.posted.Enqueue);
            return harness;
        }

        /// <summary>A follower ack; at frontier 0 its backfill is refused (below the checkpoint) and escalates.</summary>
        public Task AckSuccess(long committedIndex = 0, long durableIndex = -1)
        {
            ApplyPosted();
            return Sm.CompleteAppendLogsAsync(Follower, Host.HybridLogicalClock.TrySendOrLocalEvent(1),
                RaftOperationStatus.Success, committedIndex, durableIndex: durableIndex).AsTask();
        }

        /// <summary>One live proposal through the leader's replication path, the way a client write goes out
        /// (the fan-out to the followers happens at propose time, before the local write completes).</summary>
        public Task ProposeAsync()
        {
            ApplyPosted();
            List<RaftLog> logs = [new() { Id = nextLogId++, Term = 1, Type = RaftLogType.Proposed, LogType = "test", LogData = [1] }];
            Sm.ReplicateLogs(logs, autoCommit: true, replyCorrelationId: null);
            return Task.CompletedTask;
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
    }

    /// <summary>Stands in for the partition executor's install path; while held, an install waits for <see cref="Release"/>.</summary>
    private sealed class GatedInstaller
    {
        private volatile TaskCompletionSource? gate;

        public void Hold() => gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        public void Release() => gate?.TrySetResult();

        public async Task<SnapshotResponse> Install(SnapshotInstallRequest request)
        {
            TaskCompletionSource? held = gate;
            if (held is not null)
                await held.Task.ConfigureAwait(false);

            await request.Snapshot.CopyToAsync(Stream.Null).ConfigureAwait(false);
            return new SnapshotResponse(SnapshotInstallOutcome.Installed);
        }
    }

    private sealed class CountingTransfer : IRaftPartitionStateTransfer
    {
        public Task<Stream> ExportPartitionState(int partitionId, long upToIndex, CancellationToken ct) =>
            Task.FromResult<Stream>(new MemoryStream([0xAB, 0xCD, 0xEF]));

        public Task ImportPartitionState(int partitionId, Stream snapshot, CancellationToken ct) =>
            Task.CompletedTask;
    }

    /// <summary>A leader-side host whose snapshot calls reach a real receiver and whose sends are counted by kind.</summary>
    private sealed class ReceiverHost : IRaftPartitionHost
    {
        private readonly IRaftPartitionStateTransfer transfer;
        private int entryBatchesSent;
        private int heartbeatsSent;

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
                EnableCheckQuorum = false,
            };
        }

        public SnapshotReceiver Receiver { get; }

        /// <summary>Entry-carrying AppendLogs handed to the transport for the follower.</summary>
        public int EntryBatchesSent => Volatile.Read(ref entryBatchesSent);

        /// <summary>Entry-less AppendLogs handed to the transport for the follower.</summary>
        public int HeartbeatsSent => Volatile.Read(ref heartbeatsSent);

        /// <summary>The most recent AppendLogs (heartbeat or batch) handed to the transport for the follower.</summary>
        public volatile AppendLogsRequest? LastAppendLogs;

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

        public void EnqueueResponse(string e, RaftResponderRequest r)
        {
            if (r.AppendLogsRequest is not { } append)
                return;

            LastAppendLogs = append;

            if (append.Logs is { Count: > 0 })
                Interlocked.Increment(ref entryBatchesSent);
            else
                Interlocked.Increment(ref heartbeatsSent);
        }

        public Task InvokeLeaderChanged(int p, string l) => Task.CompletedTask;
        public Task<bool> InvokeReplicationReceived(int p, RaftLog l) => Task.FromResult(true);
        public Task<bool> InvokeSystemReplicationReceived(int p, RaftLog l) => Task.FromResult(true);
        public void InvokeReplicationError(int p, RaftLog l) { }
        public MemberLivenessState GetNodeLiveness(string endpoint) => MemberLivenessState.Alive;

        public IRaftStateMachineTransfer? StateMachineTransfer => null;
        public IRaftSystemStateTransfer? SystemStateTransfer => null;
        public IRaftPartitionStateTransfer? PartitionStateTransfer => transfer;

        public Task<SnapshotResponse> SendInstallSnapshotAsync(RaftNode node, SnapshotRequest request, CancellationToken ct) =>
            Receiver.ReceiveInstallSnapshot(request, CancellationToken.None);

        public Task<SnapshotResponse> QuerySnapshotInstallAsync(RaftNode node, SnapshotRequest query, CancellationToken ct) =>
            Receiver.ReceiveInstallSnapshot(query, CancellationToken.None);
    }

    /// <summary>
    /// WAL stub with a compaction floor at its checkpoint — every range read comes back empty, so a
    /// follower below it is refused and escalates to a snapshot at the checkpoint — that records the
    /// live-replica retention floor and budget the heartbeat round publishes.
    /// </summary>
    private sealed class FloorRecordingCheckpointWal : IRaftWalFacade
    {
        private readonly long commitIndex;
        private readonly long checkpoint;

        public long PublishedReplicaFloor = long.MaxValue;
        public long PublishedBudget;

        public FloorRecordingCheckpointWal(long checkpoint, long commitIndex)
        {
            this.checkpoint = checkpoint;
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
        public ValueTask<long> GetAnyTermAtAsync(long logIndex) => ValueTask.FromResult(1L);
        public ValueTask<long> GetLastCheckpointAsync() => ValueTask.FromResult(checkpoint);
        public long GetCommitIndex() => commitIndex;
        public long GetDurableCommitFrontier() => commitIndex;
        public WALWriteOperation EnqueuePropose(long term, List<RaftLog> logs, HLCTimestamp ts, bool ac) => MakeNoOp();
        public WALWriteOperation EnqueueCommit(List<RaftLog> logs) => MakeNoOp();
        public WALWriteOperation EnqueueRollback(List<RaftLog> logs) => MakeNoOp();
        public WALWriteOperation? EnqueueProposeOrCommit(List<RaftLog>? logs, HLCTimestamp timestamp = default, string? endpoint = null, long term = -1) =>
            logs is null ? null : MakeNoOp();
        public void NotifyCommitted() { }

        public void SetLiveReplicaRetentionFloor(long floor) => SetLiveReplicaRetentionFloor(floor, 0);

        public void SetLiveReplicaRetentionFloor(long floor, long budget)
        {
            Volatile.Write(ref PublishedReplicaFloor, floor);
            Volatile.Write(ref PublishedBudget, budget);
        }

        private static WALWriteOperation MakeNoOp() =>
            new(_ => { }, 0, WALWriteOperationType.LeaderPropose, (0, []));
    }

    private sealed class NoopSink : IRaftOperationReplySink
    {
        public void TryComplete(ulong correlationId, RaftResponse response) { }
    }
}
