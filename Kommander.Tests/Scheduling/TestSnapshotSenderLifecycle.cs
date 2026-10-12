using System.Collections.Concurrent;
using System.Diagnostics;
using System.Security.Cryptography;

using Kommander.Data;
using Kommander.Gossip;
using Kommander.Scheduling;
using Kommander.System;
using Kommander.Time;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.Scheduling;

/// <summary>
/// Failure, guard and wait edges of <see cref="SnapshotSender"/> driven directly against fake
/// transports, in the shape the partition state machine drives it: the in-flight guard that keeps
/// one transfer per follower, the deferral of a peer whose disk is stalled, the failure backoff
/// ladder and its permanent (unproducible) tier, an export stream that fails or stalls mid-way,
/// the retry cache when a replay is refused mid-stream or answered with a running install, the
/// install wait's progress bound and its silent-follower bound, the wait that ends when this node
/// stops leading, and the completion that adopts a SkippedAlreadyCovered answer. Timing-dependent
/// waits use the wall clock with scaled budgets, like <see cref="TestSnapshotInstallAwaited"/>;
/// the backoff tests drive a frozen manual clock so the pauses are exact.
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
public class TestSnapshotSenderLifecycle
{
    private const string Follower = "follower:9001";

    private static readonly RaftNode FollowerNode = new(Follower);

    // ── guards ─────────────────────────────────────────────────────────────────

    /// <summary>
    /// While one transfer to a follower is in flight the status surface reports it (with its
    /// trigger and elapsed time), a second escalation for the same follower starts nothing, and
    /// <see cref="SnapshotSender.CanAttempt"/> says no — the guard is what keeps a refused ack's
    /// fast path from stacking transfers. Once the transfer ends the follower is eligible again
    /// and the confirmed install reached the executor as a SnapshotInstalled request.
    /// </summary>
    [Fact]
    public async Task TransferInFlight_BlocksASecondTransferToTheSameFollower()
    {
        Harness h = Harness.Build(transfer: new BlobTransfer([0xAB, 0xCD]));

        // A healthy partition with no history reports nothing.
        Assert.Empty(h.Sender.GetStatuses());

        TaskCompletionSource<SnapshotResponse> firstQueryGate = new(TaskCreationOptions.RunContinuationsAsynchronously);
        h.Host.QueryAnswer = (_, query, _) =>
            query.SessionId.Length == 0 ? firstQueryGate.Task : Task.FromResult(InstalledAnswer(query.SessionId, 50));

        h.Sender.TrySend(FollowerNode, 50, leaderTerm: 1, lastIncludedTerm: 1,
            SnapshotTransferTrigger.NonContiguousBackfill, "anchor 50 was compacted away");
        await h.WaitUntilAsync(() => h.Host.QueryCount == 1, "the transfer's first install question");

        RaftSnapshotStatus active = Assert.Single(h.Sender.GetStatuses());
        Assert.True(active.InFlight);
        Assert.Equal(0, active.FailedAttempts);
        Assert.Null(active.LastError);
        Assert.Equal(SnapshotTransferTrigger.NonContiguousBackfill, active.Trigger);
        Assert.NotNull(active.InFlightFor);
        Assert.True(active.InFlightFor >= TimeSpan.Zero);
        Assert.False(active.AwaitingInstall);
        Assert.False(h.Sender.CanAttempt(Follower));

        // The second escalation is vetoed by the in-flight guard before any work starts.
        h.Sender.TrySend(FollowerNode, 50, leaderTerm: 1, lastIncludedTerm: 1,
            SnapshotTransferTrigger.NonContiguousBackfill, "second escalation");
        Assert.Equal(1, h.Host.QueryCount);

        firstQueryGate.SetResult(new SnapshotResponse(SnapshotInstallOutcome.NoInstall));
        await h.WaitUntilAsync(() => h.Installs.Count == 1, "the transfer completes");

        (string endpoint, long index) = Assert.Single(h.Installs.ToList());
        Assert.Equal(Follower, endpoint);
        Assert.Equal(50, index);

        RaftRequest posted = Assert.Single(h.Posted.ToList());
        Assert.Equal(RaftRequestType.SnapshotInstalled, posted.Type);
        Assert.Equal(Follower, posted.Endpoint);
        Assert.Equal(50, posted.CommitIndex);

        Assert.Empty(h.Sender.GetStatuses());

        // The follower is eligible again once the post-success pause (which keeps the ack fast
        // path from re-escalating while the follower's frontier catches up) has passed.
        await h.WaitUntilAsync(() => h.Sender.CanAttempt(Follower), "the post-success pause passes");
    }

    /// <summary>
    /// A peer that reports a durable-write stall is deferred before anything starts: not even the
    /// opening install question is sent, because nothing shipped could be installed until its
    /// disk answers.
    /// </summary>
    [Fact]
    public async Task TransferForAStalledPeer_StartsNotEvenAQuestion()
    {
        BlobTransfer transfer = new([1]);
        Harness h = Harness.Build(transfer: transfer, defer: _ => true);

        h.Sender.TrySend(FollowerNode, 50, leaderTerm: 1, lastIncludedTerm: 1,
            SnapshotTransferTrigger.NonContiguousBackfill, "deferred");
        await Task.Delay(100, TestContext.Current.CancellationToken);

        Assert.Equal(0, h.Host.QueryCount);
        Assert.Equal(0, transfer.ExportCalls);
        Assert.Empty(h.Sender.GetStatuses());
    }

    // ── failure backoff ────────────────────────────────────────────────────────

    /// <summary>
    /// Each failed attempt doubles the retry pause from one heartbeat interval (floored at
    /// 100 ms), the status surface carries the episode while the pause is armed, and a follower
    /// quiet for two full cap windows starts a fresh episode at one attempt — the follower
    /// progressed by other means, so the ladder must not resume at the old rung.
    /// </summary>
    [Fact]
    public async Task FailedTransfer_BackoffDoublesPerAttempt_AndANewEpisodeStartsFresh()
    {
        ClockBox clock = new(Stopwatch.GetTimestamp());
        FailingTransfer transfer = new(new InvalidOperationException("disk full"));
        Harness h = Harness.Build(transfer: transfer, clock: clock.Read,
            configure: c => c.HeartbeatInterval = TimeSpan.FromMilliseconds(200));

        void Escalate() => h.Sender.TrySend(FollowerNode, 50, leaderTerm: 1, lastIncludedTerm: 1,
            SnapshotTransferTrigger.NonContiguousBackfill, "attempt");

        Escalate();
        await h.WaitUntilAsync(() => Statuses(h, s => s.FailedAttempts == 1), "first failure recorded");

        RaftSnapshotStatus first = Assert.Single(h.Sender.GetStatuses());
        Assert.Equal(1, first.FailedAttempts);
        Assert.False(first.Unproducible);
        Assert.False(first.InFlight);
        Assert.Contains("ExportPartitionState failed", first.LastError);
        Assert.NotNull(first.FirstFailureAt);
        Assert.NotNull(first.LastFailureAt);
        Assert.InRange(first.RetryBackoffRemaining.TotalMilliseconds, 150, 200);
        Assert.False(h.Sender.CanAttempt(Follower));

        // Inside the first pause a new escalation changes nothing.
        clock.AdvanceMs(100);
        Escalate();
        Assert.Equal(1, transfer.ExportCalls);

        clock.AdvanceMs(120);
        Assert.True(h.Sender.CanAttempt(Follower));
        Escalate();
        await h.WaitUntilAsync(() => Statuses(h, s => s.FailedAttempts == 2), "second failure recorded");

        RaftSnapshotStatus second = Assert.Single(h.Sender.GetStatuses());
        Assert.InRange(second.RetryBackoffRemaining.TotalMilliseconds, 350, 400);

        clock.AdvanceMs(100);
        Assert.False(h.Sender.CanAttempt(Follower));
        clock.AdvanceMs(320);
        Assert.True(h.Sender.CanAttempt(Follower));

        Escalate();
        await h.WaitUntilAsync(() => Statuses(h, s => s.FailedAttempts == 3), "third failure recorded");
        Assert.InRange(
            Assert.Single(h.Sender.GetStatuses()).RetryBackoffRemaining.TotalMilliseconds, 750, 800);

        // Two cap windows (2 x 30 s) of silence end the episode.
        clock.AdvanceMs(61_000);
        Assert.True(h.Sender.CanAttempt(Follower));
        Escalate();
        await h.WaitUntilAsync(() => Statuses(h, s => transfer.ExportCalls == 4 && s.FailedAttempts == 1),
            "a fresh episode counts its first attempt");
    }

    /// <summary>
    /// An application that rejects the export as unsupported is a permanent cause: the follower is
    /// reported unproducible and the pause jumps straight to the 30-second cap, so a condition
    /// only an operator can fix costs one attempt per cap window instead of one per heartbeat.
    /// </summary>
    [Fact]
    public async Task ExportRejectedAsUnsupported_MarksTheFollowerUnproducible_AtTheMaximumPause()
    {
        ClockBox clock = new(Stopwatch.GetTimestamp());
        FailingTransfer transfer = new(new NotSupportedException("whole-partition export unsupported"));
        Harness h = Harness.Build(transfer: transfer, clock: clock.Read);

        h.Sender.TrySend(FollowerNode, 50, leaderTerm: 1, lastIncludedTerm: 1,
            SnapshotTransferTrigger.NonContiguousBackfill, "attempt");
        await h.WaitUntilAsync(() => Statuses(h, s => s.FailedAttempts == 1), "failure recorded");

        RaftSnapshotStatus status = Assert.Single(h.Sender.GetStatuses());
        Assert.True(status.Unproducible);
        Assert.Contains("ExportPartitionState failed", status.LastError);
        Assert.InRange(status.RetryBackoffRemaining.TotalMilliseconds, 29_000, 30_000);

        clock.AdvanceMs(29_000);
        Assert.False(h.Sender.CanAttempt(Follower));
        clock.AdvanceMs(1_100);
        Assert.True(h.Sender.CanAttempt(Follower));
        Assert.Equal(1, transfer.ExportCalls);
    }

    /// <summary>
    /// The legacy split-transfer fallback fails through the same recording: an export the
    /// application rejects as unsupported is unproducible, any other failure is paced as
    /// transient.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task LegacyRangeExportFailure_IsRecordedWithItsCause(bool notSupported)
    {
        FailingRangeTransfer transfer = new(notSupported);
        Harness h = Harness.Build(rangeTransfer: transfer);

        h.Sender.TrySend(FollowerNode, 50, leaderTerm: 1, lastIncludedTerm: 1,
            SnapshotTransferTrigger.NonContiguousBackfill, "attempt");
        await h.WaitUntilAsync(() => Statuses(h, s => s.FailedAttempts == 1), "failure recorded");

        RaftSnapshotStatus status = Assert.Single(h.Sender.GetStatuses());
        Assert.Contains("ExportRange failed", status.LastError);
        Assert.Equal(notSupported, status.Unproducible);
        Assert.True(status.RetryBackoffRemaining > TimeSpan.Zero);
    }

    // ── the export stream ──────────────────────────────────────────────────────

    /// <summary>
    /// A stream that fails part-way through the export fails the transfer as a whole (no install
    /// is posted, the in-flight guard is released) and poisons nothing: no retry cache was
    /// published from the partial read, so the retry re-exports and installs the real bytes.
    /// </summary>
    [Fact]
    public async Task ExportStreamFailingMidRead_FailsTheTransfer_AndPoisonsNoRetryCache()
    {
        byte[] payload = [1, 2, 3, 4, 5, 6, 7];
        ScriptedTransfer transfer = new(attempt => attempt == 1
            ? new FailsAfterReadStream([1, 2, 3], new IOException("the export stream died mid-read"))
            : new MemoryStream(payload));
        Harness h = Harness.Build(transfer: transfer);

        h.Sender.TrySend(FollowerNode, 50, leaderTerm: 1, lastIncludedTerm: 1,
            SnapshotTransferTrigger.NonContiguousBackfill, "attempt");
        await h.WaitUntilAsync(() => Statuses(h, s => s.FailedAttempts == 1 && !s.InFlight), "the failure surfaced");
        Assert.Contains("unhandled snapshot transfer error", Assert.Single(h.Sender.GetStatuses()).LastError);
        Assert.Empty(h.Installs);

        // Nothing was cached from the failed attempt: the retry re-exports…
        await h.WaitUntilAsync(() => h.Sender.CanAttempt(Follower), "the backoff window passes");
        h.Sender.TrySend(FollowerNode, 50, leaderTerm: 1, lastIncludedTerm: 1,
            SnapshotTransferTrigger.NonContiguousBackfill, "retry");
        await h.WaitUntilAsync(() => h.Installs.Count == 1, "the retry installs");

        Assert.Equal(2, transfer.ExportCalls);
        Assert.Empty(h.Sender.GetStatuses());

        // …and the bytes that reached the follower are the export's, digest and all.
        SnapshotRequest terminal = h.Host.Chunks().Last(c => c.IsLast);
        Assert.Equal(payload, terminal.Data.ToArray());
        Assert.Equal(Checksum(payload), terminal.SnapshotChecksum);
    }

    /// <summary>
    /// A stream read that stops moving is abandoned at the step timeout — the watchdog cancels
    /// the read so a token-honouring callee stops too — the failure is recorded as a step
    /// timeout, and the in-flight guard is released so the follower is not silently vetoed
    /// forever.
    /// </summary>
    [Fact]
    public async Task StreamReadThatNeverCompletes_TimesOutTheStep_CancelsTheRead_AndReleasesTheGuard()
    {
        StuckReadStream stuck = new();
        ScriptedTransfer transfer = new(attempt => attempt == 1 ? stuck : new MemoryStream([9, 9, 9]));
        Harness h = Harness.Build(transfer: transfer, configure: c =>
        {
            c.SnapshotTransferStepTimeout = TestTimeouts.Scale(TimeSpan.FromMilliseconds(600));
            c.SnapshotExportRetryCacheMaxBytes = 0;
        });

        h.Sender.TrySend(FollowerNode, 50, leaderTerm: 1, lastIncludedTerm: 1,
            SnapshotTransferTrigger.NonContiguousBackfill, "attempt");
        await h.WaitUntilAsync(() => Statuses(h, s => s.FailedAttempts == 1), "the step timeout is recorded");
        Assert.Contains("snapshot stream read", Assert.Single(h.Sender.GetStatuses()).LastError);
        Assert.Empty(h.Installs);

        await h.WaitUntilAsync(() => stuck.CancelObserved, "the watchdog cancelled the abandoned read");

        // The guard and the backoff recover: a healthy attempt after the pause completes.
        await h.WaitUntilAsync(() => h.Sender.CanAttempt(Follower), "the backoff window passes");
        h.Sender.TrySend(FollowerNode, 50, leaderTerm: 1, lastIncludedTerm: 1,
            SnapshotTransferTrigger.NonContiguousBackfill, "retry");
        await h.WaitUntilAsync(() => h.Installs.Count == 1, "the retry installs");
        Assert.Empty(h.Sender.GetStatuses());
    }

    // ── the retry cache ────────────────────────────────────────────────────────

    /// <summary>
    /// A replay of the cached export that a follower refuses on a mid-stream chunk ends that
    /// attempt without a second export, and the refusal poisons nothing: the next retry replays
    /// the same cached chunks and the terminal chunk still carries the digest of the whole
    /// snapshot.
    /// </summary>
    [Fact]
    public async Task CachedReplay_RefusedOnAMidStreamChunk_LeavesTheCacheIntactForTheNextRetry()
    {
        byte[] blob = new byte[3 * 1024 * 1024 + 10];
        for (int i = 0; i < blob.Length; i++)
            blob[i] = (byte)(i * 31 & 0xFF);

        BlobTransfer transfer = new(blob);
        Harness h = Harness.Build(transfer: transfer);

        // The follower is silent about installs it does not know, so every attempt proceeds.
        h.Host.QueryAnswer = (_, query, _) => Task.FromResult(query.SessionId.Length == 0
            ? new SnapshotResponse(SnapshotInstallOutcome.NoInstall)
            : new SnapshotResponse(SnapshotInstallOutcome.Rejected));

        bool refuseTerminal = true;
        bool refuseMidStreamChunk = false;
        h.Host.SendAnswer = (_, chunk, _) =>
        {
            if (!chunk.IsLast)
            {
                if (refuseMidStreamChunk && chunk.ChunkIndex == 0)
                {
                    refuseMidStreamChunk = false;
                    return Task.FromResult(new SnapshotResponse(SnapshotInstallOutcome.Rejected));
                }

                return Task.FromResult(new SnapshotResponse(SnapshotInstallOutcome.ChunkAccepted));
            }

            if (refuseTerminal)
            {
                refuseTerminal = false;
                return Task.FromResult(new SnapshotResponse(false));
            }

            return Task.FromResult(InstalledAnswer(chunk.SessionId, chunk.SnapshotIndex));
        };

        // First attempt: exported, staged, and the terminal chunk refused.
        h.Sender.TrySend(FollowerNode, 50, leaderTerm: 1, lastIncludedTerm: 1,
            SnapshotTransferTrigger.NonContiguousBackfill, "attempt");
        await h.WaitUntilAsync(() => Statuses(h, s => s.FailedAttempts == 1 && !s.InFlight), "the refused terminal chunk");
        Assert.Equal(1, transfer.ExportCalls);

        // Second attempt (forced past the backoff): replays the cache and is refused on chunk 0
        // alone — the replay stopped before any terminal chunk of this session was sent.
        refuseMidStreamChunk = true;
        h.Sender.TrySend(FollowerNode, 50, leaderTerm: 1, lastIncludedTerm: 1,
            SnapshotTransferTrigger.NonContiguousBackfill, "retry", forced: true);
        await h.WaitUntilAsync(() => Statuses(h, s => s.FailedAttempts == 2 && !s.InFlight), "the refused mid-stream chunk");

        string firstSession = h.Host.Chunks()[0].SessionId;
        SnapshotRequest refusedReplayOnly = Assert.Single(h.Host.Chunks(), c => c.SessionId != firstSession);
        Assert.Equal(0, refusedReplayOnly.ChunkIndex);
        Assert.False(refusedReplayOnly.IsLast);
        Assert.Equal(1, transfer.ExportCalls);

        // Third attempt: the same cache still serves the full replay, bytes and digest intact.
        h.Sender.TrySend(FollowerNode, 50, leaderTerm: 1, lastIncludedTerm: 1,
            SnapshotTransferTrigger.NonContiguousBackfill, "retry", forced: true);
        await h.WaitUntilAsync(() => h.Installs.Count == 1, "the third attempt installs");

        Assert.Equal(1, transfer.ExportCalls);
        SnapshotRequest terminal = h.Host.Chunks().Last(c => c.IsLast);
        Assert.Equal(blob[^10..], terminal.Data.ToArray());
        Assert.Equal(Checksum(blob), terminal.SnapshotChecksum);
        Assert.Empty(h.Sender.GetStatuses());
    }

    /// <summary>
    /// A replay answered <see cref="SnapshotInstallOutcome.InstallPending"/> — another install of
    /// the partition is running on the follower — stops the replay, waits for that install, and
    /// adopts its outcome at its index: one export in total, and the cursor lands on the index
    /// the follower actually installed.
    /// </summary>
    [Fact]
    public async Task CachedReplay_AnsweredInstallPending_AdoptsTheRunningInstall()
    {
        BlobTransfer transfer = new([1, 2, 3, 4]);
        Harness h = Harness.Build(transfer: transfer);

        h.Host.QueryAnswer = (_, query, _) => Task.FromResult(query.SessionId switch
        {
            "" => new SnapshotResponse(SnapshotInstallOutcome.NoInstall),
            "other-leaders-install" => InstalledAnswer("other-leaders-install", 70),
            _ => new SnapshotResponse(SnapshotInstallOutcome.Rejected),
        });

        bool refuseTerminal = true;
        h.Host.SendAnswer = (_, chunk, _) =>
        {
            if (chunk.IsLast && refuseTerminal)
            {
                refuseTerminal = false;
                return Task.FromResult(new SnapshotResponse(false));
            }

            return Task.FromResult(chunk.IsLast
                ? new SnapshotResponse(SnapshotInstallOutcome.InstallPending)
                {
                    InstallSessionId = "other-leaders-install",
                    InstallIndex = 70,
                }
                : new SnapshotResponse(SnapshotInstallOutcome.ChunkAccepted));
        };

        h.Sender.TrySend(FollowerNode, 50, leaderTerm: 1, lastIncludedTerm: 1,
            SnapshotTransferTrigger.NonContiguousBackfill, "attempt");
        await h.WaitUntilAsync(() => Statuses(h, s => s.FailedAttempts == 1 && !s.InFlight), "the refused terminal chunk");

        h.Sender.TrySend(FollowerNode, 50, leaderTerm: 1, lastIncludedTerm: 1,
            SnapshotTransferTrigger.NonContiguousBackfill, "retry", forced: true);
        await h.WaitUntilAsync(() => h.Installs.Count == 1, "the running install is adopted");

        (string endpoint, long index) = Assert.Single(h.Installs.ToList());
        Assert.Equal(Follower, endpoint);
        Assert.Equal(70, index);
        Assert.Equal(70, Assert.Single(h.Posted.ToList()).CommitIndex);
        Assert.Equal(1, transfer.ExportCalls);
        Assert.Empty(h.Sender.GetStatuses());
    }

    // ── the install wait ───────────────────────────────────────────────────────

    /// <summary>
    /// Two transfers in a row that each find an install already running on the follower log one
    /// Warning for the first wait and demote the repeat to Debug inside the cooldown — a follower
    /// whose installs keep being replaced must not warn per cycle.
    /// </summary>
    [Fact]
    public async Task WaitingForARunningInstall_RepeatsWithinTheCooldown_DropToDebug()
    {
        BlobTransfer transfer = new([1]);
        Harness h = Harness.Build(transfer: transfer);

        int installsSeen = 0;
        h.Host.QueryAnswer = (_, query, _) => Task.FromResult(query.SessionId.Length == 0
            ? new SnapshotResponse(SnapshotInstallOutcome.InstallPending)
            {
                InstallSessionId = $"previous-install-{Interlocked.Increment(ref installsSeen)}",
                InstallIndex = 40,
            }
            : InstalledAnswer(query.SessionId, 40));

        h.Sender.TrySend(FollowerNode, 50, leaderTerm: 1, lastIncludedTerm: 1,
            SnapshotTransferTrigger.NonContiguousBackfill, "attempt");
        await h.WaitUntilAsync(() => h.Installs.Count == 1, "the first wait ends with the install");
        Assert.Equal(1, h.Logger.Count(LogLevel.Warning, "is waiting for an install already running"));

        await h.WaitUntilAsync(() => h.Sender.CanAttempt(Follower), "the post-success pause passes");
        h.Sender.TrySend(FollowerNode, 50, leaderTerm: 1, lastIncludedTerm: 1,
            SnapshotTransferTrigger.NonContiguousBackfill, "attempt");
        await h.WaitUntilAsync(() => h.Installs.Count == 2, "the second wait ends with the install");

        Assert.Equal(1, h.Logger.Count(LogLevel.Warning, "is waiting for an install already running"));
        Assert.True(h.Logger.Count(LogLevel.Debug, "is waiting for the install already running") >= 1,
            "the repeat inside the cooldown must be demoted to Debug");
        Assert.Equal(0, transfer.ExportCalls);
    }

    /// <summary>
    /// The install wait is bounded by progress, not by a fixed time: an install that keeps
    /// reporting fresh staged-bytes readings is waited for past the step timeout, shows as
    /// AwaitingInstall while it runs, and no failed attempt is ever recorded for it.
    /// </summary>
    [Fact]
    public async Task InstallThatKeepsReportingProgress_IsWaitedFor_PastTheStepTimeout()
    {
        const int pendingPolls = 12;
        int polls = 0;

        Harness h = Harness.Build(transfer: new BlobTransfer([1, 2, 3]), configure: c =>
        {
            c.SnapshotTransferStepTimeout = TestTimeouts.Scale(TimeSpan.FromMilliseconds(700));
            c.SnapshotChunkAckTimeout = TestTimeouts.Scale(TimeSpan.FromSeconds(1));
        });

        h.Host.QueryAnswer = (_, query, _) =>
        {
            if (query.SessionId.Length == 0)
                return Task.FromResult(new SnapshotResponse(SnapshotInstallOutcome.NoInstall));

            int poll = Interlocked.Increment(ref polls);
            return Task.FromResult(poll <= pendingPolls
                ? new SnapshotResponse(SnapshotInstallOutcome.InstallPending)
                {
                    InstallSessionId = query.SessionId,
                    InstallIndex = 50,
                    InstallProgress = 4096L * poll,
                }
                : InstalledAnswer(query.SessionId, 50));
        };

        h.Host.SendAnswer = (_, chunk, _) => Task.FromResult(chunk.IsLast
            ? new SnapshotResponse(SnapshotInstallOutcome.InstallPending)
            {
                InstallSessionId = chunk.SessionId,
                InstallIndex = chunk.SnapshotIndex,
            }
            : new SnapshotResponse(SnapshotInstallOutcome.ChunkAccepted));

        h.Sender.TrySend(FollowerNode, 50, leaderTerm: 1, lastIncludedTerm: 1,
            SnapshotTransferTrigger.NonContiguousBackfill, "attempt");
        await h.WaitUntilAsync(() => Statuses(h, s => s.AwaitingInstall), "the transfer waits for the install");

        RaftSnapshotStatus waiting = Assert.Single(h.Sender.GetStatuses());
        Assert.True(waiting.InFlight);
        Assert.Equal(50, waiting.AwaitingInstallIndex);
        Assert.Equal(0, waiting.FailedAttempts);

        // Thirteen polls at the sender's cadence run well past the step timeout; every poll
        // reports new progress, so only a wait bounded by total time would give up here.
        await h.WaitUntilAsync(() => h.Installs.Count == 1, "the install finishes and is adopted");

        Assert.Equal(50, Assert.Single(h.Installs.ToList()).Index);
        Assert.Empty(h.Sender.GetStatuses());
    }

    /// <summary>
    /// A follower that answers nothing about its install for as long as a chunk acknowledgement
    /// may take ends the wait with a recorded failure — the install is not forgotten: the next
    /// attempt asks about that session by name before it exports anything, and adopts the
    /// outcome without a second export when the follower finally reports it.
    /// </summary>
    [Fact]
    public async Task FollowerSilentAboutItsInstall_IsRecordedUnanswered_AndAskedFirstNextTime()
    {
        BlobTransfer transfer = new([1, 2, 3]);
        Harness h = Harness.Build(transfer: transfer, configure: c =>
            c.SnapshotChunkAckTimeout = TestTimeouts.Scale(TimeSpan.FromMilliseconds(400)));

        bool installFinished = false;
        h.Host.QueryAnswer = (_, query, _) => Task.FromResult(query.SessionId.Length == 0
            ? new SnapshotResponse(SnapshotInstallOutcome.NoInstall)
            : installFinished
                ? InstalledAnswer(query.SessionId, 50)
                : new SnapshotResponse(SnapshotInstallOutcome.Rejected));

        h.Host.SendAnswer = (_, chunk, _) => Task.FromResult(chunk.IsLast
            ? new SnapshotResponse(SnapshotInstallOutcome.InstallPending)
            {
                InstallSessionId = chunk.SessionId,
                InstallIndex = chunk.SnapshotIndex,
            }
            : new SnapshotResponse(SnapshotInstallOutcome.ChunkAccepted));

        h.Sender.TrySend(FollowerNode, 50, leaderTerm: 1, lastIncludedTerm: 1,
            SnapshotTransferTrigger.NonContiguousBackfill, "attempt");
        await h.WaitUntilAsync(() => Statuses(h, s => s.FailedAttempts == 1 && !s.InFlight), "the silence is recorded");
        Assert.Contains("has not answered", Assert.Single(h.Sender.GetStatuses()).LastError);

        string stagedSession = h.Host.Chunks().Single(c => c.IsLast).SessionId;

        await h.WaitUntilAsync(() => h.Sender.CanAttempt(Follower), "the backoff window passes");
        installFinished = true;
        h.Sender.TrySend(FollowerNode, 50, leaderTerm: 1, lastIncludedTerm: 1,
            SnapshotTransferTrigger.NonContiguousBackfill, "retry");

        // The retry's first question names the install it never heard the end of…
        await h.WaitUntilAsync(() => h.Host.Queries().Any(q => q.SessionId == stagedSession),
            "the remembered install is asked about first");
        await h.WaitUntilAsync(() => h.Installs.Count == 1, "the outcome is adopted");

        Assert.Equal(50, Assert.Single(h.Installs.ToList()).Index);
        Assert.Equal(1, transfer.ExportCalls);
        Assert.Empty(h.Sender.GetStatuses());
    }

    /// <summary>
    /// A leader that stops leading mid-wait gives up on the install's outcome without recording
    /// any failure (its successor asks the follower itself), exports nothing, and still remembers
    /// the install so a later leadership asks about it before sending anything.
    /// </summary>
    [Fact]
    public async Task LeaderThatStopsLeadingMidWait_EndsWithoutFailure_AndRemembersTheInstall()
    {
        BlobTransfer transfer = new([1, 2, 3]);
        Harness h = Harness.Build(transfer: transfer);

        bool foreignFinished = false;
        h.Host.QueryAnswer = (_, query, _) => Task.FromResult(query.SessionId.Length == 0
            ? new SnapshotResponse(SnapshotInstallOutcome.InstallPending)
            {
                InstallSessionId = "foreign-install",
                InstallIndex = 40,
            }
            : foreignFinished
                ? InstalledAnswer(query.SessionId, 40)
                : new SnapshotResponse(SnapshotInstallOutcome.InstallPending)
                {
                    InstallSessionId = query.SessionId,
                    InstallIndex = 40,
                    InstallProgress = 1024,
                });

        h.Sender.TrySend(FollowerNode, 50, leaderTerm: 1, lastIncludedTerm: 1,
            SnapshotTransferTrigger.NonContiguousBackfill, "attempt");
        await h.WaitUntilAsync(() => Statuses(h, s => s.AwaitingInstall), "the transfer waits for the install");

        h.NodeState = RaftNodeState.Follower;
        await h.WaitUntilAsync(() => h.Sender.GetStatuses().Count == 0, "the wait ends with the leadership");
        await h.WaitUntilAsync(() => h.Sender.CanAttempt(Follower), "the follower is eligible again");

        Assert.Empty(h.Installs);
        Assert.Equal(0, transfer.ExportCalls);

        h.NodeState = RaftNodeState.Leader;
        foreignFinished = true;
        h.Sender.TrySend(FollowerNode, 50, leaderTerm: 1, lastIncludedTerm: 1,
            SnapshotTransferTrigger.NonContiguousBackfill, "attempt");

        await h.WaitUntilAsync(() => h.Host.Queries().Any(q => q.SessionId == "foreign-install"),
            "the remembered install is asked about first");
        await h.WaitUntilAsync(() => h.Installs.Count == 1, "the remembered install's outcome is adopted");

        Assert.Equal(40, Assert.Single(h.Installs.ToList()).Index);
        Assert.Equal(0, transfer.ExportCalls);
        Assert.Empty(h.Sender.GetStatuses());
    }

    // ── completion ─────────────────────────────────────────────────────────────

    /// <summary>
    /// A terminal chunk answered <see cref="SnapshotInstallOutcome.SkippedAlreadyCovered"/> at an
    /// index of the follower's choosing completes the transfer at that index, clears the failure
    /// surface, and arms the post-success pause: an immediate second escalation starts nothing,
    /// while a forced one (the follower's own re-seed request) goes through and its chunks carry
    /// Forced.
    /// </summary>
    [Fact]
    public async Task SkippedAlreadyCovered_Answer_CompletesAtTheAnsweredIndex_AndArmsTheSuccessPause()
    {
        BlobTransfer transfer = new([1, 2, 3]);
        Harness h = Harness.Build(transfer: transfer,
            configure: c => c.HeartbeatInterval = TimeSpan.FromSeconds(30));

        h.Host.SendAnswer = (_, chunk, _) => Task.FromResult(chunk.IsLast
            ? new SnapshotResponse(SnapshotInstallOutcome.SkippedAlreadyCovered)
            {
                InstallSessionId = chunk.SessionId,
                InstallIndex = 77,
            }
            : new SnapshotResponse(SnapshotInstallOutcome.ChunkAccepted));

        h.Sender.TrySend(FollowerNode, 50, leaderTerm: 1, lastIncludedTerm: 1,
            SnapshotTransferTrigger.NonContiguousBackfill, "attempt");
        await h.WaitUntilAsync(() => h.Installs.Count == 1, "the skip completes the transfer");

        Assert.Equal(77, Assert.Single(h.Installs.ToList()).Index);
        RaftRequest posted = Assert.Single(h.Posted.ToList());
        Assert.Equal(RaftRequestType.SnapshotInstalled, posted.Type);
        Assert.Equal(Follower, posted.Endpoint);
        Assert.Equal(77, posted.CommitIndex);
        Assert.Empty(h.Sender.GetStatuses());
        Assert.False(h.Sender.CanAttempt(Follower));

        // The post-success pause holds the next escalation off…
        h.Sender.TrySend(FollowerNode, 50, leaderTerm: 1, lastIncludedTerm: 1,
            SnapshotTransferTrigger.NonContiguousBackfill, "second escalation");
        Assert.Equal(1, transfer.ExportCalls);
        Assert.Equal(1, h.Host.QueryCount);

        string voluntarySession = h.Host.Chunks()[0].SessionId;
        Assert.All(h.Host.Chunks().Where(c => c.SessionId == voluntarySession), c => Assert.False(c.Forced));
        Assert.All(h.Host.Chunks().Where(c => c.SessionId != voluntarySession), c => Assert.True(c.Forced));
    }

    // ── helpers ────────────────────────────────────────────────────────────────

    private static bool Statuses(Harness h, Func<RaftSnapshotStatus, bool> predicate) =>
        h.Sender.GetStatuses().Any(predicate);

    private static SnapshotResponse InstalledAnswer(string session, long index) =>
        new(SnapshotInstallOutcome.Installed) { InstallSessionId = session, InstallIndex = index };

    private static string Checksum(byte[] snapshot) => Convert.ToHexString(SHA256.HashData(snapshot));

    private sealed class ClockBox
    {
        public long Value;
        public ClockBox(long start) => Value = start;
        public long Read() => Value;
        public void AdvanceMs(long ms) => Value += ms * Stopwatch.Frequency / 1000;
    }

    /// <summary>Captures the (level, message) pairs the sender logs, for cooldown assertions.</summary>
    private sealed class RecordingLogger : ILogger<IRaft>
    {
        private readonly object gate = new();
        private readonly List<(LogLevel Level, string Message)> entries = [];

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            string message = formatter is not null ? formatter(state, exception) : state?.ToString() ?? "";
            lock (gate)
                entries.Add((logLevel, message));
        }

        public int Count(LogLevel level, string contains)
        {
            lock (gate)
                return entries.Count(e => e.Level == level && e.Message.Contains(contains, StringComparison.Ordinal));
        }
    }

    private sealed class Harness
    {
        public required SenderHost Host { get; init; }
        public required RecordingLogger Logger { get; init; }

        public SnapshotSender Sender { get; private set; } = null!;

        public ConcurrentQueue<RaftRequest> Posted { get; } = new();
        public ConcurrentQueue<(string Endpoint, long Index)> Installs { get; } = new();

        /// <summary>The node state the sender reads on every wait iteration; tests flip it.</summary>
        public RaftNodeState NodeState { get; set; } = RaftNodeState.Leader;

        public static Harness Build(
            IRaftPartitionStateTransfer? transfer = null,
            IRaftStateMachineTransfer? rangeTransfer = null,
            Action<RaftConfiguration>? configure = null,
            Func<RaftNode, SnapshotRequest, CancellationToken, Task<SnapshotResponse>>? send = null,
            Func<RaftNode, SnapshotRequest, CancellationToken, Task<SnapshotResponse>>? query = null,
            Func<long>? clock = null,
            Func<string, bool>? defer = null)
        {
            RecordingLogger logger = new();
            SenderHost host = new(transfer, rangeTransfer, configure) { ReadClock = clock };
            Harness harness = new() { Host = host, Logger = logger };

            // Mirrors the partition executor's SnapshotInstalled dispatch: the posted request is
            // what advances the leader's cursors, and the sender is only told once it lands.
            void Post(RaftRequest request)
            {
                harness.Posted.Enqueue(request);
                if (request.Type == RaftRequestType.SnapshotInstalled)
                    harness.Sender.CompleteSnapshotInstalled(request.Endpoint ?? "", request.CommitIndex);
            }

            harness.Sender = new SnapshotSender(
                host,
                logger,
                () => harness.NodeState,
                () => Post,
                (endpoint, index) => harness.Installs.Enqueue((endpoint, index)),
                defer);
            host.SendAnswer = send;
            host.QueryAnswer = query;
            return harness;
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

    /// <summary>
    /// A leader-side host whose snapshot calls go to per-test delegates over a log of every chunk
    /// and install question, with an optional manual monotonic clock for exact backoff pauses.
    /// </summary>
    private sealed class SenderHost : IRaftPartitionHost
    {
        private readonly IRaftPartitionStateTransfer? transfer;
        private readonly IRaftStateMachineTransfer? rangeTransfer;
        private readonly List<SnapshotRequest> chunkLog = [];
        private readonly List<SnapshotRequest> queryLog = [];

        public SenderHost(
            IRaftPartitionStateTransfer? transfer,
            IRaftStateMachineTransfer? rangeTransfer,
            Action<RaftConfiguration>? configure)
        {
            this.transfer = transfer;
            this.rangeTransfer = rangeTransfer;
            Configuration = new RaftConfiguration
            {
                NodeId = 1, Host = "leader", Port = 9000, InitialPartitions = 1,
                HeartbeatInterval = TimeSpan.Zero, RecentHeartbeat = TimeSpan.Zero,
            };
            configure?.Invoke(Configuration);
        }

        /// <summary>The monotonic tick source; null for the process clock.</summary>
        public Func<long>? ReadClock;

        /// <summary>Answer for one chunk send; null accepts staged chunks and installs the terminal one.</summary>
        public Func<RaftNode, SnapshotRequest, CancellationToken, Task<SnapshotResponse>>? SendAnswer { get; set; }

        /// <summary>Answer for one install question; null says NoInstall for any session.</summary>
        public Func<RaftNode, SnapshotRequest, CancellationToken, Task<SnapshotResponse>>? QueryAnswer { get; set; }

        public IReadOnlyList<SnapshotRequest> Chunks()
        {
            lock (chunkLog) return [.. chunkLog];
        }

        public IReadOnlyList<SnapshotRequest> Queries()
        {
            lock (queryLog) return [.. queryLog];
        }

        public int QueryCount => Queries().Count;

        public long GetMonotonicTimestamp() => ReadClock?.Invoke() ?? Stopwatch.GetTimestamp();

        public int PartitionId => 1;
        public string Leader { get; set; } = "";
        public string LocalEndpoint => "leader:9000";
        public int LocalNodeId => 1;
        public ClusterMemberRole LocalRole => ClusterMemberRole.Voter;
        public bool IsVoter(string endpoint) => true;
        public RaftConfiguration Configuration { get; }
        public HybridLogicalClock HybridLogicalClock { get; } = new();
        public IReadOnlyList<RaftNode> Nodes => [FollowerNode];

        public HLCTimestamp GetLastNodeActivity(string e, int p) => HLCTimestamp.Zero;
        public void UpdateLastNodeActivity(string e, int p, HLCTimestamp t) { }
        public void EnqueueResponse(string e, RaftResponderRequest r) { }
        public Task InvokeLeaderChanged(int p, string l) => Task.CompletedTask;
        public Task<bool> InvokeReplicationReceived(int p, RaftLog l) => Task.FromResult(true);
        public Task<bool> InvokeSystemReplicationReceived(int p, RaftLog l) => Task.FromResult(true);
        public void InvokeReplicationError(int p, RaftLog l) { }
        public MemberLivenessState GetNodeLiveness(string endpoint) => MemberLivenessState.Alive;

        public IRaftStateMachineTransfer? StateMachineTransfer => rangeTransfer;
        public IRaftSystemStateTransfer? SystemStateTransfer => null;
        public IRaftPartitionStateTransfer? PartitionStateTransfer => transfer;

        public Task<SnapshotResponse> SendInstallSnapshotAsync(RaftNode node, SnapshotRequest request, CancellationToken ct)
        {
            // Copy: a streamed chunk's Data is a view over a reused buffer.
            lock (chunkLog)
            {
                chunkLog.Add(new SnapshotRequest
                {
                    SessionId = request.SessionId,
                    PartitionId = request.PartitionId,
                    SnapshotIndex = request.SnapshotIndex,
                    ChunkIndex = request.ChunkIndex,
                    IsLast = request.IsLast,
                    Kind = request.Kind,
                    Data = request.Data.ToArray(),
                    SnapshotChecksum = request.SnapshotChecksum,
                    Forced = request.Forced,
                    InstallPolling = request.InstallPolling,
                });
            }

            return SendAnswer?.Invoke(node, request, ct) ?? DefaultSendAnswer(request);
        }

        public Task<SnapshotResponse> QuerySnapshotInstallAsync(RaftNode node, SnapshotRequest query, CancellationToken ct)
        {
            lock (queryLog)
            {
                queryLog.Add(new SnapshotRequest
                {
                    SessionId = query.SessionId,
                    StatusQuery = true,
                    PartitionId = query.PartitionId,
                    SnapshotIndex = query.SnapshotIndex,
                    ChunkIndex = query.ChunkIndex,
                });
            }

            return QueryAnswer?.Invoke(node, query, ct)
                ?? Task.FromResult(new SnapshotResponse(SnapshotInstallOutcome.NoInstall));
        }

        private static Task<SnapshotResponse> DefaultSendAnswer(SnapshotRequest chunk) =>
            Task.FromResult(chunk.IsLast
                ? InstalledAnswer(chunk.SessionId, chunk.SnapshotIndex)
                : new SnapshotResponse(SnapshotInstallOutcome.ChunkAccepted));
    }

    /// <summary>Whole-partition transfer that serves a fixed blob and counts exports.</summary>
    private sealed class BlobTransfer(byte[] blob) : IRaftPartitionStateTransfer
    {
        private int exportCalls;

        public int ExportCalls => Volatile.Read(ref exportCalls);

        public Task<Stream> ExportPartitionState(int partitionId, long upToIndex, CancellationToken ct)
        {
            Interlocked.Increment(ref exportCalls);
            return Task.FromResult<Stream>(new MemoryStream(blob));
        }

        public Task ImportPartitionState(int partitionId, Stream snapshot, CancellationToken ct) =>
            Task.CompletedTask;
    }

    /// <summary>Whole-partition transfer whose export always fails with the given exception.</summary>
    private sealed class FailingTransfer(Exception failure) : IRaftPartitionStateTransfer
    {
        private int exportCalls;

        public int ExportCalls => Volatile.Read(ref exportCalls);

        public Task<Stream> ExportPartitionState(int partitionId, long upToIndex, CancellationToken ct)
        {
            Interlocked.Increment(ref exportCalls);
            throw failure;
        }

        public Task ImportPartitionState(int partitionId, Stream snapshot, CancellationToken ct) =>
            Task.CompletedTask;
    }

    /// <summary>Split transfer whose export always fails, optionally as unsupported.</summary>
    private sealed class FailingRangeTransfer(bool notSupported) : IRaftStateMachineTransfer
    {
        public Task<Stream> ExportRange(RaftSplitPlan plan, long upToIndex, CancellationToken ct)
        {
            if (notSupported)
                throw new NotSupportedException("boundless plan: whole-partition export unsupported");

            throw new InvalidOperationException("ExportRange failed on the application side");
        }

        public Task ImportRange(int targetPartitionId, Stream snapshot, CancellationToken ct) =>
            Task.CompletedTask;
    }

    /// <summary>
    /// Transfer that serves one stream per export from a per-attempt factory — a failed attempt
    /// followed by a healthy retry.
    /// </summary>
    private sealed class ScriptedTransfer(Func<int, Stream> streamForAttempt) : IRaftPartitionStateTransfer
    {
        private int exportCalls;

        public int ExportCalls => Volatile.Read(ref exportCalls);

        public Task<Stream> ExportPartitionState(int partitionId, long upToIndex, CancellationToken ct) =>
            Task.FromResult(streamForAttempt(Interlocked.Increment(ref exportCalls)));

        public Task ImportPartitionState(int partitionId, Stream snapshot, CancellationToken ct) =>
            Task.CompletedTask;
    }

    /// <summary>A stream that serves one short read and then fails every later read.</summary>
    private sealed class FailsAfterReadStream(byte[] first, Exception failure) : Stream
    {
        private bool served;

        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();
        public override long Position { get; set; }

        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken)
        {
            if (served)
                throw failure;

            served = true;
            int count = Math.Min(first.Length, buffer.Length);
            first.AsSpan(0, count).CopyTo(buffer.Span);
            return ValueTask.FromResult(count);
        }

        public override void Flush() { }
        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }

    /// <summary>A stream whose reads never complete until the caller's token is cancelled.</summary>
    private sealed class StuckReadStream : Stream
    {
        private readonly TaskCompletionSource<int> stuck = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public volatile bool CancelObserved;

        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();
        public override long Position { get; set; }

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken)
        {
            try
            {
                return await stuck.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                CancelObserved = true;
                throw;
            }
        }

        public override void Flush() { }
        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }
}
