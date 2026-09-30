using System.Diagnostics;
using System.Security.Cryptography;
using Kommander;
using Kommander.Data;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.Scheduling;

/// <summary>
/// The receiver's half of "the install is a step of its own": the terminal chunk is answered when the
/// snapshot is staged, the install's outcome is asked for separately, and while an install of a
/// partition runs nothing else of that partition is staged.
///
/// <para>What it replaces: the terminal chunk's call was held open until the install had finished. An
/// install takes as long as the application's import, a chunk acknowledgement may take
/// <see cref="RaftConfiguration.SnapshotChunkAckTimeout"/>, and a large partition on a busy disk
/// missed that deadline every time. The sender read the expired call as a rejected last chunk and
/// exported the partition again at a newer index, and the receiver staged that second snapshot beside
/// the first while the first was still being imported (CamusDB fault soaks rl4b and rl5).</para>
///
/// <para>The receiver is built in isolation with an install callback the test holds open, so "an
/// install is running" is a state the test controls, not a race it hopes to hit.</para>
/// </summary>
public class TestSnapshotInstallAsItsOwnStep
{
    private const string Leader = "leader:1";
    private const string OtherLeader = "leader:2";

    // ── the terminal chunk and the question that follows it ─────────────────────

    [Fact]
    public async Task PollingSender_IsAnsweredPendingAtTheTerminalChunk_AndLearnsTheOutcomeByAsking()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        HeldInstaller installer = new();
        SnapshotReceiver r = NewReceiver(installer.Install);

        Assert.Equal(SnapshotInstallOutcome.ChunkAccepted,
            (await r.ReceiveInstallSnapshot(Chunk("s1", 0, isLast: false, [1, 2]), ct)).Outcome);

        SnapshotResponse terminal = await r.ReceiveInstallSnapshot(
            Chunk("s1", 1, isLast: true, [3], wholeSnapshot: [1, 2, 3]), ct);

        // Answered while the install is still held: the call does not span the install.
        Assert.Equal(SnapshotInstallOutcome.InstallPending, terminal.Outcome);
        Assert.False(terminal.Success);
        Assert.Equal("s1", terminal.InstallSessionId);
        Assert.Equal(100, terminal.InstallIndex);
        Assert.Equal(3, terminal.InstallLeaderTerm);
        Assert.Equal(Leader, terminal.InstallLeaderEndpoint);
        Assert.Equal(1, installer.Started);
        Assert.Equal(1, r.ActiveInstallCount);
        Assert.Equal(3, r.InstallingByteCount);
        Assert.Equal(0, r.PendingByteCount);

        Assert.Equal(SnapshotInstallOutcome.InstallPending,
            (await r.ReceiveInstallSnapshot(Query("s1"), ct)).Outcome);

        installer.Release(SnapshotInstallOutcome.Installed);
        await installer.Finished;

        SnapshotResponse outcome = await WaitForOutcomeAsync(r, "s1", ct);
        Assert.Equal(SnapshotInstallOutcome.Installed, outcome.Outcome);
        Assert.Equal("s1", outcome.InstallSessionId);
        Assert.Equal(100, outcome.InstallIndex);

        // The staged buffer and its accounting are released with the install.
        Assert.Equal(0, r.ActiveInstallCount);
        Assert.Equal(0, r.InstallingByteCount);
        Assert.Equal(0, r.InMemoryStagedByteCount);
        Assert.Equal([1, 2, 3], installer.ReceivedBytes);
    }

    /// <summary>
    /// A sender that does not set <see cref="SnapshotRequest.InstallPolling"/> predates the pending
    /// outcome: it would read it as neither a rejection nor an install. Its terminal chunk is held
    /// until the install completes, as before.
    /// </summary>
    [Fact]
    public async Task SenderThatDoesNotPoll_IsAnsweredWhenTheInstallCompletes()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        HeldInstaller installer = new();
        SnapshotReceiver r = NewReceiver(installer.Install);

        Task<SnapshotResponse> terminal = r.ReceiveInstallSnapshot(
            Chunk("s1", 0, isLast: true, [7], polling: false), ct);

        await installer.WaitUntilStartedAsync(ct);
        await Task.Delay(50, ct);
        Assert.False(terminal.IsCompleted, "a sender that does not poll must not be answered before the install completes");

        installer.Release(SnapshotInstallOutcome.Installed);

        SnapshotResponse answer = await terminal.WaitAsync(TestTimeouts.Scale(TimeSpan.FromSeconds(5)), ct);
        Assert.Equal(SnapshotInstallOutcome.Installed, answer.Outcome);
        Assert.True(answer.Success);
    }

    [Fact]
    public async Task InstallThatFinishesAtOnce_IsAnsweredWithItsOutcome_NotPending()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        SnapshotReceiver r = NewReceiver(_ => Task.FromResult(new SnapshotResponse(SnapshotInstallOutcome.SkippedAlreadyCovered)));

        SnapshotResponse terminal = await r.ReceiveInstallSnapshot(Chunk("s1", 0, isLast: true, [7]), ct);

        Assert.Equal(SnapshotInstallOutcome.SkippedAlreadyCovered, terminal.Outcome);
        Assert.Equal(0, r.ActiveInstallCount);
    }

    /// <summary>
    /// A failed install is a <see cref="SnapshotInstallOutcome.Rejected"/> answer that names the
    /// install. That is what lets a sender tell "the install failed" from a call that was never
    /// answered, which carries no install at all.
    /// </summary>
    [Fact]
    public async Task FailedInstall_IsReportedAsRejected_WithTheInstallNamed()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        HeldInstaller installer = new();
        SnapshotReceiver r = NewReceiver(installer.Install);

        await r.ReceiveInstallSnapshot(Chunk("s1", 0, isLast: true, [7]), ct);
        installer.Release(SnapshotInstallOutcome.Rejected);
        await installer.Finished;

        SnapshotResponse outcome = await WaitForOutcomeAsync(r, "s1", ct);
        Assert.Equal(SnapshotInstallOutcome.Rejected, outcome.Outcome);
        Assert.Equal(100, outcome.InstallIndex);
        Assert.Equal("s1", outcome.InstallSessionId);
        Assert.Equal(0, r.InstallingByteCount);
    }

    [Fact]
    public async Task InstallThatThrows_IsRecordedAsRejected_AndReleasesItsBuffer()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        SnapshotReceiver r = NewReceiver(_ => throw new InvalidOperationException("import blew up"));

        SnapshotResponse terminal = await r.ReceiveInstallSnapshot(Chunk("s1", 0, isLast: true, [7]), ct);

        Assert.Equal(SnapshotInstallOutcome.Rejected, terminal.Outcome);
        Assert.Equal(100, terminal.InstallIndex);
        Assert.Equal(0, r.ActiveInstallCount);
        Assert.Equal(0, r.TotalStagedByteCount);
    }

    // ── what a status query answers ─────────────────────────────────────────────

    [Fact]
    public async Task Query_WithNoInstallOnRecord_AnswersNoInstall_AndStagesNothing()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        HeldInstaller installer = new();
        SnapshotReceiver r = NewReceiver(installer.Install);

        SnapshotResponse answer = await r.ReceiveInstallSnapshot(Query(""), ct);

        Assert.Equal(SnapshotInstallOutcome.NoInstall, answer.Outcome);
        Assert.False(answer.Success);
        Assert.Equal(0, r.PendingSessionCount);
        Assert.Equal(0, installer.Started);
    }

    /// <summary>
    /// A running install is reported to whoever asks, whichever session it belongs to: it is what the
    /// asker has to wait for. A completed one is reported only to a query that names its session —
    /// otherwise the outcome of an old install would answer for a transfer that has not been sent,
    /// and a re-seed at the same index would never go out.
    /// </summary>
    [Fact]
    public async Task RunningInstall_IsReportedToAnyAsker_CompletedInstall_OnlyToItsOwnSession()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        HeldInstaller installer = new();
        SnapshotReceiver r = NewReceiver(installer.Install);

        await r.ReceiveInstallSnapshot(Chunk("s1", 0, isLast: true, [7]), ct);

        SnapshotResponse byAnyone = await r.ReceiveInstallSnapshot(Query("", leader: OtherLeader), ct);
        Assert.Equal(SnapshotInstallOutcome.InstallPending, byAnyone.Outcome);
        Assert.Equal("s1", byAnyone.InstallSessionId);
        Assert.Equal(Leader, byAnyone.InstallLeaderEndpoint);

        SnapshotResponse byAnotherSession = await r.ReceiveInstallSnapshot(Query("some-other-session"), ct);
        Assert.Equal(SnapshotInstallOutcome.InstallPending, byAnotherSession.Outcome);
        Assert.Equal("s1", byAnotherSession.InstallSessionId);

        installer.Release(SnapshotInstallOutcome.Installed);
        await installer.Finished;
        await WaitForOutcomeAsync(r, "s1", ct);

        Assert.Equal(SnapshotInstallOutcome.NoInstall, (await r.ReceiveInstallSnapshot(Query(""), ct)).Outcome);
        Assert.Equal(SnapshotInstallOutcome.NoInstall, (await r.ReceiveInstallSnapshot(Query("some-other-session"), ct)).Outcome);
        // Another leader that was told to wait for s1 learns how it ended by naming it.
        Assert.Equal(SnapshotInstallOutcome.Installed, (await r.ReceiveInstallSnapshot(Query("s1", leader: OtherLeader), ct)).Outcome);
        // A different partition has no install on record.
        Assert.Equal(SnapshotInstallOutcome.NoInstall, (await r.ReceiveInstallSnapshot(Query("s1", partitionId: 2), ct)).Outcome);
    }

    /// <summary>
    /// The sender bounds its wait by the install's progress, so the receiver has to report some: how
    /// far the importer has read into the staged snapshot.
    /// </summary>
    [Fact]
    public async Task Progress_FollowsTheImportersReads_AndStaysAtItsLastValueAfterTheInstall()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        byte[] payload = new byte[1000];
        TaskCompletionSource readHalf = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource finish = new(TaskCreationOptions.RunContinuationsAsynchronously);

        SnapshotReceiver r = NewReceiver(async request =>
        {
            byte[] half = new byte[400];
            await request.Snapshot.ReadExactlyAsync(half);
            readHalf.SetResult();
            await finish.Task;
            await request.Snapshot.CopyToAsync(Stream.Null);
            return new SnapshotResponse(SnapshotInstallOutcome.Installed);
        });

        await r.ReceiveInstallSnapshot(Chunk("s1", 0, isLast: true, payload), ct);
        await readHalf.Task.WaitAsync(TestTimeouts.Scale(TimeSpan.FromSeconds(5)), ct);

        Assert.Equal(400, (await r.ReceiveInstallSnapshot(Query("s1"), ct)).InstallProgress);

        finish.SetResult();
        SnapshotResponse outcome = await WaitForOutcomeAsync(r, "s1", ct);
        Assert.Equal(1000, outcome.InstallProgress);
    }

    // ── one install per partition ───────────────────────────────────────────────

    /// <summary>
    /// The memory bound. A second snapshot of the partition cannot be installed until the first has
    /// finished, so staging it buys nothing and costs a whole copy of the partition beside the running
    /// import. A sender that polls is told which install to wait for; one that does not is refused.
    /// </summary>
    [Fact]
    public async Task WhileAnInstallRuns_NoOtherSessionOfThePartitionIsStaged()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        HeldInstaller installer = new();
        SnapshotReceiver r = NewReceiver(installer.Install);

        await r.ReceiveInstallSnapshot(Chunk("s1", 0, isLast: true, [1, 2, 3]), ct);
        long stagedWhileInstalling = r.TotalStagedByteCount;
        Assert.Equal(3, stagedWhileInstalling);

        // The retry at a newer index, from the same leader.
        SnapshotResponse retry = await r.ReceiveInstallSnapshot(
            Chunk("s2", 0, isLast: false, new byte[500], snapshotIndex: 250), ct);
        Assert.Equal(SnapshotInstallOutcome.InstallPending, retry.Outcome);
        Assert.Equal("s1", retry.InstallSessionId);
        Assert.Equal(100, retry.InstallIndex);

        // Another leader, in a higher term.
        SnapshotResponse fromNewLeader = await r.ReceiveInstallSnapshot(
            Chunk("s3", 0, isLast: false, new byte[500], leader: OtherLeader, leaderTerm: 4, snapshotIndex: 250), ct);
        Assert.Equal(SnapshotInstallOutcome.InstallPending, fromNewLeader.Outcome);
        Assert.Equal(Leader, fromNewLeader.InstallLeaderEndpoint);

        // A sender that does not poll.
        SnapshotResponse legacy = await r.ReceiveInstallSnapshot(
            Chunk("s4", 0, isLast: false, new byte[500], snapshotIndex: 250, polling: false), ct);
        Assert.Equal(SnapshotInstallOutcome.Rejected, legacy.Outcome);
        Assert.Equal(0, legacy.InstallIndex);

        Assert.Equal(0, r.PendingSessionCount);
        Assert.Equal(stagedWhileInstalling, r.TotalStagedByteCount);
        Assert.Equal(1, installer.Started);

        // Another partition is not held up by this one's install.
        Assert.Equal(SnapshotInstallOutcome.ChunkAccepted,
            (await r.ReceiveInstallSnapshot(Chunk("p2", 0, isLast: false, [9], partitionId: 2), ct)).Outcome);

        // Once the install has ended the partition takes a new session again.
        installer.Release(SnapshotInstallOutcome.Installed);
        await installer.Finished;
        await WaitForOutcomeAsync(r, "s1", ct);

        Assert.Equal(SnapshotInstallOutcome.ChunkAccepted,
            (await r.ReceiveInstallSnapshot(Chunk("s5", 0, isLast: false, [1], snapshotIndex: 250), ct)).Outcome);
    }

    /// <summary>
    /// Sessions of the partition that were already open when an install starts are dropped then, not
    /// left to stage a full snapshot that would be refused at its terminal chunk.
    /// </summary>
    [Fact]
    public async Task InstallStart_DropsThePartitionsOtherPendingSessions_AndTheirSendersAreToldToWait()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        HeldInstaller installer = new();
        SnapshotReceiver r = NewReceiver(installer.Install);

        // Two leaders stage at once (a leader change mid-transfer); a third session is for another partition.
        await r.ReceiveInstallSnapshot(Chunk("a", 0, isLast: false, new byte[300]), ct);
        await r.ReceiveInstallSnapshot(Chunk("b", 0, isLast: false, new byte[200], leader: OtherLeader, leaderTerm: 3), ct);
        await r.ReceiveInstallSnapshot(Chunk("other", 0, isLast: false, new byte[50], partitionId: 2), ct);
        Assert.Equal(550, r.PendingByteCount);

        byte[] whole = new byte[201];
        await r.ReceiveInstallSnapshot(
            Chunk("b", 1, isLast: true, new byte[1], leader: OtherLeader, leaderTerm: 3, wholeSnapshot: whole), ct);

        // "a" is gone; only the other partition's session is still pending.
        Assert.Equal(1, r.PendingSessionCount);
        Assert.Equal(50, r.PendingByteCount);
        Assert.Equal(201, r.InstallingByteCount);

        SnapshotResponse next = await r.ReceiveInstallSnapshot(Chunk("a", 1, isLast: false, new byte[300]), ct);
        Assert.Equal(SnapshotInstallOutcome.InstallPending, next.Outcome);
        Assert.Equal("b", next.InstallSessionId);
        Assert.Equal(OtherLeader, next.InstallLeaderEndpoint);
        Assert.Equal(50, r.PendingByteCount);

        installer.Release(SnapshotInstallOutcome.Installed);
        await installer.Finished;
    }

    /// <summary>
    /// The terminal chunk sent twice — a transport that retried it, or a sender that never saw the
    /// first answer. It used to meet "no such session" and be refused; the snapshot is staged and its
    /// install is running, so the repeat is a question about that install.
    /// </summary>
    [Fact]
    public async Task RepeatedTerminalChunk_AsksAboutTheInstall_AndNeverInstallsTwice()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        HeldInstaller installer = new();
        SnapshotReceiver r = NewReceiver(installer.Install);

        SnapshotRequest terminal = Chunk("s1", 0, isLast: true, [1, 2, 3]);
        await r.ReceiveInstallSnapshot(terminal, ct);

        SnapshotResponse repeat = await r.ReceiveInstallSnapshot(terminal, ct);
        Assert.Equal(SnapshotInstallOutcome.InstallPending, repeat.Outcome);
        Assert.Equal("s1", repeat.InstallSessionId);

        // The repeat of a sender that does not poll waits for the install like its first chunk did.
        Task<SnapshotResponse> legacyRepeat = r.ReceiveInstallSnapshot(
            Chunk("s1", 0, isLast: true, [1, 2, 3], polling: false), ct);
        await Task.Delay(50, ct);
        Assert.False(legacyRepeat.IsCompleted);

        installer.Release(SnapshotInstallOutcome.Installed);

        Assert.Equal(SnapshotInstallOutcome.Installed,
            (await legacyRepeat.WaitAsync(TestTimeouts.Scale(TimeSpan.FromSeconds(5)), ct)).Outcome);
        Assert.Equal(SnapshotInstallOutcome.Installed, (await r.ReceiveInstallSnapshot(terminal, ct)).Outcome);

        Assert.Equal(1, installer.Started);
        Assert.Equal(0, r.TotalStagedByteCount);
    }

    [Fact]
    public async Task HeapHighWaterMark_IsSampledAroundAnInstall()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        HeldInstaller installer = new();
        SnapshotReceiver r = NewReceiver(installer.Install);

        Assert.Equal(0, r.InstallPeakHeapBytes);

        await r.ReceiveInstallSnapshot(Chunk("s1", 0, isLast: true, [7]), ct);
        Assert.True(r.InstallPeakHeapBytes > 0);

        long atStart = r.InstallPeakHeapBytes;
        await r.ReceiveInstallSnapshot(Query("s1"), ct);
        installer.Release(SnapshotInstallOutcome.Installed);
        await installer.Finished;

        // A high-water mark: it never falls.
        Assert.True(r.InstallPeakHeapBytes >= atStart);
    }

    // ── helpers ────────────────────────────────────────────────────────────────

    /// <summary>
    /// Stands in for the partition executor's install path and holds each install open until the test
    /// releases it with an outcome.
    /// </summary>
    private sealed class HeldInstaller
    {
        private readonly TaskCompletionSource<SnapshotInstallOutcome> release =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        private readonly TaskCompletionSource started = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource finished = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int startedCount;

        public int Started => Volatile.Read(ref startedCount);
        public byte[] ReceivedBytes { get; private set; } = [];

        /// <summary>Completes when the (first) install has returned its outcome to the receiver.</summary>
        public Task Finished => finished.Task;

        public void Release(SnapshotInstallOutcome outcome) => release.TrySetResult(outcome);

        public Task WaitUntilStartedAsync(CancellationToken ct) =>
            started.Task.WaitAsync(TestTimeouts.Scale(TimeSpan.FromSeconds(5)), ct);

        public async Task<SnapshotResponse> Install(SnapshotInstallRequest request)
        {
            Interlocked.Increment(ref startedCount);
            started.TrySetResult();

            SnapshotInstallOutcome outcome = await release.Task.ConfigureAwait(false);

            using MemoryStream copy = new();
            await request.Snapshot.CopyToAsync(copy).ConfigureAwait(false);
            ReceivedBytes = copy.ToArray();

            finished.TrySetResult();
            return new SnapshotResponse(outcome);
        }
    }

    private static SnapshotReceiver NewReceiver(Func<SnapshotInstallRequest, Task<SnapshotResponse>> installOnExecutor) =>
        new(
            isDisposed: () => false,
            installOnExecutor: installOnExecutor,
            logger: NullLogger<IRaft>.Instance,
            localEndpoint: "test:1",
            sessionTtlTicks: 1_000_000,
            maxPendingSessions: 8,
            maxPendingBytes: 1_000_000,
            getMonotonicTimestamp: () => 1000);

    /// <summary>
    /// The install's outcome lands on the record a moment after the install callback returns; ask
    /// until the receiver stops answering <see cref="SnapshotInstallOutcome.InstallPending"/>.
    /// </summary>
    private static async Task<SnapshotResponse> WaitForOutcomeAsync(SnapshotReceiver r, string session, CancellationToken ct)
    {
        TimeSpan budget = TestTimeouts.Scale(TimeSpan.FromSeconds(5));
        long started = Stopwatch.GetTimestamp();

        while (true)
        {
            SnapshotResponse answer = await r.ReceiveInstallSnapshot(Query(session), ct);
            if (answer.Outcome != SnapshotInstallOutcome.InstallPending)
                return answer;

            if (Stopwatch.GetElapsedTime(started) > budget)
                Assert.Fail($"the install of session {session} never reported an outcome");

            await Task.Delay(5, ct);
        }
    }

    private static SnapshotRequest Query(string session, string leader = Leader, int partitionId = 1) =>
        new()
        {
            StatusQuery = true,
            InstallPolling = true,
            SessionId = session,
            PartitionId = partitionId,
            SnapshotIndex = 100,
            FollowerEndpoint = "test:1",
            LeaderEndpoint = leader,
            LeaderTerm = 3,
            ChunkIndex = -1,
        };

    private static SnapshotRequest Chunk(
        string session, int chunkIndex, bool isLast, byte[] data,
        string leader = Leader, int partitionId = 1, long snapshotIndex = 100,
        long leaderTerm = 3, byte[]? wholeSnapshot = null, bool polling = true) =>
        new()
        {
            SessionId = session,
            PartitionId = partitionId,
            SnapshotIndex = snapshotIndex,
            FollowerEndpoint = "test:1",
            LeaderEndpoint = leader,
            LeaderTerm = leaderTerm,
            LastIncludedTerm = 2,
            ChunkIndex = chunkIndex,
            IsLast = isLast,
            Data = data,
            SnapshotChecksum = isLast ? Convert.ToHexString(SHA256.HashData(wholeSnapshot ?? data)) : "",
            InstallPolling = polling,
        };
}
