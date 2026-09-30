
using System.Buffers;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Security.Cryptography;
using Kommander.Data;
using Kommander.Diagnostics;
using Kommander.Logging;
using Kommander.Scheduling;
using Kommander.Support.Parallelization;
using Kommander.System;
using Microsoft.Extensions.Logging;

namespace Kommander;

/// <summary>
/// Owns the in-flight snapshot-send guard table (<c>pendingSnapshotEndpoints</c>) for
/// <see cref="RaftPartitionStateMachine"/> and encapsulates the full chunked-send loop.
/// <see cref="TrySend"/> is the entry point called on the executor thread; it fires
/// <see cref="TrySendSnapshotAsync"/> as a detached background <see cref="Task"/> and
/// guarantees at most one concurrent transfer per follower endpoint.
/// All background work runs off the executor thread — no executor locks are held during I/O.
///
/// <para><b>Failure pacing:</b> a follower below the compaction floor whose snapshot cannot be
/// produced or delivered used to be retried at full rate every heartbeat forever, with a log line
/// per attempt as the only evidence. Failures are now recorded per follower endpoint with an
/// exponential backoff (heartbeat-interval base, capped at <see cref="MaxPauseMs"/>), the
/// condition is queryable through <see cref="GetStatuses"/> (surfaced on
/// <see cref="IRaft.GetSnapshotStatuses"/>), and a permanent cause — no transfer registered, or
/// the application rejecting the export as unsupported — jumps straight to the maximum pause with
/// a single Warning instead of hot-looping. Backoff paces retries rather than stopping them, so a
/// late registration or a recovered application is picked up without operator action.</para>
///
/// <para><b>Convergence breaker:</b> the failure controls above only fire on FAILING transfers. A
/// rescue loop in which every install succeeds but the follower returns below the compaction
/// floor before the next heartbeat is invisible to all of them, and it re-runs an unbounded-cost
/// export on a fixed cadence forever — the Caraxes <c>bank-optimistic-45m-p</c> leader ran 29
/// such exports in 15 minutes and died of memory exhaustion. Consecutive install→re-escalation
/// cycles are therefore counted per follower (<see cref="RescueCycleState"/>); after
/// <see cref="RaftConfiguration.SnapshotRescueMaxConsecutiveCycles"/> of them the breaker trips:
/// escalations stop (except one paced probe per
/// <see cref="RaftConfiguration.SnapshotRescueProbeInterval"/>), the condition is surfaced as
/// <see cref="RaftSnapshotStatus.RescueNotConverging"/>, and one Warning is logged. A follower
/// that snapshots cannot rescue is an operator problem, not something to retry into an OOM.</para>
///
/// <para><b>Export retry reuse:</b> a retry at the same snapshot index re-sends the chunks of the
/// previously produced export (single-slot <see cref="CachedExport"/>) instead of re-running
/// <c>ExportPartitionState</c> — under memory pressure the old behaviour re-ran the most
/// allocation-hungry operation in the process on a 100–200 ms failure backoff. Exports above
/// <see cref="RaftConfiguration.SnapshotExportRetryCacheMaxBytes"/> stream chunk-by-chunk exactly
/// as before and are not cached.</para>
///
/// <para><b>The install is awaited, not timed.</b> A chunk acknowledgement is bounded by
/// <see cref="RaftConfiguration.SnapshotChunkAckTimeout"/>; the follower's install is not, because
/// it takes as long as the application's import does. The receiver therefore answers the terminal
/// chunk with <see cref="SnapshotInstallOutcome.InstallPending"/> and this sender polls for the
/// outcome (<see cref="AwaitInstallAsync"/>), bounded by the install's reported progress rather than
/// by a fixed time. When the terminal chunk's call was instead held open for the install, a large
/// partition on a busy disk missed the deadline every time: the attempt was recorded as a rejected
/// last chunk, and the retry exported the partition again at a newer index while the follower was
/// still importing the first one — six exports in four minutes, and a follower that ran out of
/// memory under the copies (CamusDB fault soak rl5).</para>
///
/// <para><b>A retry resumes.</b> Every attempt asks the follower, before exporting anything, whether
/// an install of the partition is already queued or running there — this leader's earlier attempt,
/// or a previous leader's. If one is, the attempt waits for it and exports nothing. The session whose
/// terminal chunk went unanswered is remembered per follower (<see cref="unresolvedInstalls"/>), so
/// an install that finished while no call was open is adopted instead of being sent again.</para>
/// </summary>
internal sealed class SnapshotSender
{
    /// <summary>Hard cap on the retry pause, and the fixed pause for permanent causes.</summary>
    private const long MaxPauseMs = 30_000;

    /// <summary>
    /// Sliding window inside which a repeated transfer-start or install-complete line for the same
    /// follower drops from Warning to Debug. The rescue must be visible at the default consumer log
    /// level (Warning — consumers commonly filter the Kommander category there, which is how two
    /// soak runs produced "zero snapshot mentions" while transfers were in fact attempted), but a
    /// fast install loop against a follower whose frontier keeps resetting must not warn per cycle.
    /// </summary>
    private const long RescueWarnCooldownMs = 10_000;

    /// <summary>
    /// Quiet window after which a follower's rescue-cycle episode is considered over: no
    /// escalation and no install for this long means the follower converged (or is gone), so the
    /// cycle count — and a tripped breaker — start fresh. Two full backoff caps, mirroring the
    /// failure-episode reset in <see cref="IsBackedOff"/>; the observed non-converging loop
    /// escalated every ~10–30 s, well inside this window.
    /// </summary>
    private const long RescueQuietWindowMs = 2 * MaxPauseMs;

    /// <summary>
    /// Retry-cache TTL: long enough to cover the full failure-backoff ladder (whose cap is
    /// <see cref="MaxPauseMs"/>), short enough that an abandoned rescue releases the cached
    /// snapshot bytes soon after.
    /// </summary>
    private const long ExportCacheTtlMs = 2 * MaxPauseMs;

    /// <summary>First pause before asking the follower what became of an install; doubles per poll.</summary>
    private static readonly TimeSpan InstallPollMinPause = TimeSpan.FromMilliseconds(10);

    /// <summary>
    /// Longest pause between two polls of a running install: what a finished install waits, at most,
    /// before its leader hears of it.
    /// </summary>
    private static readonly TimeSpan InstallPollMaxPause = TimeSpan.FromMilliseconds(250);

    /// <summary>
    /// A follower-side install a transfer is waiting for, or stopped waiting for without learning
    /// how it ended.
    /// </summary>
    private readonly record struct AwaitedInstall(string SessionId, long SnapshotIndex);

    /// <summary>
    /// Per follower, the install whose outcome this leader does not know: the session of a terminal
    /// chunk that was never answered, or an install a transfer stopped waiting for (a step timeout,
    /// unanswered polls, a lost leadership). The next attempt names it in its first question to the
    /// follower, so an install that has finished meanwhile is adopted and one that is still running
    /// is waited for — neither is sent again. Removed once the follower reports an outcome for it or
    /// no longer knows it.
    /// </summary>
    private readonly ConcurrentDictionary<string, AwaitedInstall> unresolvedInstalls = new();

    /// <summary>
    /// Per follower, the snapshot index of the install its in-flight transfer is currently waiting
    /// for. Diagnostic: surfaced as <see cref="RaftSnapshotStatus.AwaitingInstall"/>.
    /// </summary>
    private readonly ConcurrentDictionary<string, long> awaitingInstallIndexes = new();

    /// <summary>Last Warning-level "waiting for an install already running" line per endpoint — see <see cref="RescueWarnCooldownMs"/>.</summary>
    private readonly ConcurrentDictionary<string, long> lastWaitWarnTicks = new();

    /// <summary>
    /// In-flight guard, keyed by follower endpoint. The value is the transfer's start timestamp
    /// (monotonic ticks), surfaced as <see cref="RaftSnapshotStatus.InFlightFor"/> so a live query
    /// can tell a progressing transfer from a stuck one.
    /// </summary>
    private readonly ConcurrentDictionary<string, long> pendingSnapshotEndpoints = new();

    /// <summary>Last Warning-level transfer-start line per endpoint — see <see cref="RescueWarnCooldownMs"/>.</summary>
    private readonly ConcurrentDictionary<string, long> lastStartWarnTicks = new();

    /// <summary>Last Warning-level install-complete line per endpoint — see <see cref="RescueWarnCooldownMs"/>.</summary>
    private readonly ConcurrentDictionary<string, long> lastInstallWarnTicks = new();

    /// <summary>
    /// Per-follower failure episode. Mutated by background transfer tasks and read by
    /// <see cref="TrySend"/> (executor thread) and <see cref="GetStatuses"/> (arbitrary threads);
    /// the 64-bit tick fields use <see cref="Volatile"/> access so a torn read can never produce a
    /// bogus pause, and everything else is diagnostic where a benign race is acceptable.
    /// </summary>
    private sealed class FollowerSnapshotState
    {
        public int FailedAttempts;
        public long PausedUntilTicks;
        public long LastFailureTicks;
        public string? LastError;
        public bool Unproducible;
        public DateTimeOffset FirstFailureAt;
        public DateTimeOffset LastFailureAt;
    }

    private readonly ConcurrentDictionary<string, FollowerSnapshotState> failureStates = new();

    /// <summary>
    /// Per-follower convergence accounting for the SUCCEEDING rescue loop (see the class summary).
    /// A successful install arms <see cref="InstallPendingConvergence"/>; the next escalation for
    /// the same endpoint consumes it as one non-converging cycle. Touched from the executor thread
    /// (<see cref="TrySend"/>/<see cref="CanAttempt"/>) and from background transfer tasks (the
    /// install confirmation), so every access takes the per-entry lock — all critical sections are
    /// a few field reads, never I/O.
    /// </summary>
    private sealed class RescueCycleState
    {
        public int ConsecutiveCycles;
        public bool InstallPendingConvergence;
        public bool Tripped;
        public long LastActivityTicks;
        public long LastProbeTicks;
    }

    private readonly ConcurrentDictionary<string, RescueCycleState> rescueCycles = new();

    /// <summary>
    /// Per-follower pause armed after a SUCCESSFUL transfer. The refusal-path escalation fires per
    /// refused batch — on the ack fast-path that is per ack — and a follower keeps reporting a
    /// below-floor frontier until it finishes installing and its next ack reflects the seeded
    /// state. Without this pause, every ack in that window fired another full multi-chunk transfer
    /// back to back. One base pause restores the old heartbeat-interval pacing without touching
    /// <see cref="failureStates"/>, so a success still clears the failure-status surface.
    /// </summary>
    private readonly ConcurrentDictionary<string, long> successPauseUntilTicks = new();

    /// <summary>
    /// A fully drained snapshot export held for reuse by retries at the same index — single slot
    /// per partition, published with <see cref="Volatile"/> writes. Chunks are exact-size arrays
    /// in send order; the final chunk is shorter than the chunk size (possibly empty) and carries
    /// the checksum, mirroring the wire contract, so any follower transfer at the same index can
    /// replay them verbatim.
    /// </summary>
    private sealed record CachedExport(
        long SnapshotIndex,
        SnapshotKind Kind,
        IReadOnlyList<byte[]> Chunks,
        string Checksum,
        long CreatedTicks);

    private CachedExport? exportCache;

    // Followers whose in-flight transfer answers a re-seed request: their chunks carry Forced. Set
    // with the in-flight entry and cleared with it.
    private readonly ConcurrentDictionary<string, byte> forcedEndpoints = new();

    private readonly IRaftPartitionHost host;
    private readonly ILogger<IRaft> logger;
    private readonly Func<RaftNodeState> getNodeState;
    private readonly Func<Action<RaftRequest>?> getPostToExecutor;
    private readonly Action<string, long> onSnapshotInstalled;

    internal SnapshotSender(
        IRaftPartitionHost host,
        ILogger<IRaft> logger,
        Func<RaftNodeState> getNodeState,
        Func<Action<RaftRequest>?> getPostToExecutor,
        Action<string, long> onSnapshotInstalled,
        Func<string, bool>? deferTransferTo = null)
    {
        this.host = host;
        this.logger = logger;
        this.getNodeState = getNodeState;
        this.getPostToExecutor = getPostToExecutor;
        this.onSnapshotInstalled = onSnapshotInstalled;
        this.deferTransferTo = deferTransferTo ?? (static _ => false);
    }

    /// <summary>
    /// Whether a transfer to the endpoint must wait — true while the peer reports a durable-write
    /// stall (see <c>BackfillSender.EscalateRefusalToSnapshotAsync</c>, which also logs the
    /// deferral). Re-checked here so no present or future caller can start a transfer the peer
    /// cannot install.
    /// </summary>
    private readonly Func<string, bool> deferTransferTo;

    /// <summary>
    /// Called on the executor thread by the refused-backfill escalation in <c>BackfillSender</c>.
    /// Fires a background snapshot transfer to <paramref name="node"/> if the convergence breaker
    /// admits the attempt, the follower is not inside a failure backoff window or the post-success
    /// pause, and no transfer is already in progress for that endpoint (guarded by
    /// <c>pendingSnapshotEndpoints.TryAdd</c>). The entry is removed in the <c>finally</c> block of
    /// <see cref="TrySendSnapshotAsync"/> so a later refusal can retry on failure — paced by the
    /// recorded backoff rather than per refusal.
    /// </summary>
    internal void TrySend(RaftNode node, long snapshotIndex, long leaderTerm, long lastIncludedTerm) =>
        TrySend(node, snapshotIndex, leaderTerm, lastIncludedTerm, forced: false);

    /// <summary>
    /// <see cref="TrySend(RaftNode, long, long, long)"/> for a transfer the follower asked for
    /// (<see cref="Data.ReseedRequest"/>): the convergence breaker, the failure backoff and the
    /// post-success pause are bypassed, because they pace the leader's own rescue attempts and this
    /// transfer is the follower's explicit request. The in-flight guard and the durable-write stall
    /// deferral still apply — two transfers to one follower are never useful, and a stalled disk
    /// cannot install anything. The chunks carry <see cref="SnapshotRequest.Forced"/>.
    /// </summary>
    internal void TrySend(RaftNode node, long snapshotIndex, long leaderTerm, long lastIncludedTerm, bool forced)
    {
        if (deferTransferTo(node.Endpoint))
            return;

        if (!forced)
        {
            if (!RescueCycleAdmits(node.Endpoint))
                return;

            if (IsBackedOff(node.Endpoint) || IsInSuccessPause(node.Endpoint))
                return;
        }

        if (pendingSnapshotEndpoints.TryAdd(node.Endpoint, host.GetMonotonicTimestamp()))
        {
            if (forced)
                forcedEndpoints[node.Endpoint] = 0;
            else
                forcedEndpoints.TryRemove(node.Endpoint, out _);

            // A transfer start is always logged, and at Warning outside the cooldown: the only
            // caller is the refused-backfill escalation, so a start here means a peer sits below
            // the compaction floor — an abnormal condition whose rescue attempt must be visible at
            // the default consumer log level (see RescueWarnCooldownMs).
            if (TryOpenWarnWindow(lastStartWarnTicks, node.Endpoint))
                logger.LogWarnStartingSnapshotTransfer(
                    host.LocalEndpoint, host.PartitionId, getNodeState(), node.Endpoint, snapshotIndex);
            else if (logger.IsEnabled(LogLevel.Debug))
                logger.LogDebugStartingSnapshotTransfer(
                    host.LocalEndpoint, host.PartitionId, getNodeState(), node.Endpoint, snapshotIndex);

            FireAndForget.Observe(TrySendSnapshotAsync(node, snapshotIndex, leaderTerm, lastIncludedTerm), logger, "SnapshotSender.TrySend");
        }
    }

    /// <summary>
    /// The convergence-breaker gate for one escalation attempt (see the class summary). Counts a
    /// non-converging cycle when this attempt follows a successful install, trips the breaker at
    /// the configured cycle count, and — while tripped — admits only one paced probe per
    /// <see cref="RaftConfiguration.SnapshotRescueProbeInterval"/>. A quiet period of
    /// <see cref="RescueQuietWindowMs"/> without escalations resets the episode: the follower
    /// converged by other means, so the counter must not resume where it left off.
    /// </summary>
    private bool RescueCycleAdmits(string endpoint)
    {
        int maxCycles = host.Configuration.SnapshotRescueMaxConsecutiveCycles;
        if (maxCycles <= 0)
            return true;

        RescueCycleState state = rescueCycles.GetOrAdd(endpoint, static _ => new RescueCycleState());
        long now = host.GetMonotonicTimestamp();

        lock (state)
        {
            ResetRescueEpisodeIfQuietLocked(state, endpoint, now);

            state.LastActivityTicks = now;

            if (state.InstallPendingConvergence)
            {
                // The previous install "succeeded" and yet here is another below-floor escalation
                // for the same follower: that pair is one non-converging rescue cycle.
                state.InstallPendingConvergence = false;
                state.ConsecutiveCycles++;

                if (!state.Tripped && state.ConsecutiveCycles >= maxCycles)
                {
                    state.Tripped = true;
                    state.LastProbeTicks = now;
                    KommanderMetrics.RecordSnapshotRescueBreakerTripped(host.PartitionId);
                    logger.LogWarning(
                        "[{LocalEndpoint}/{PartitionId}/{State}] Snapshot rescue for {Endpoint} is not converging: {Cycles} consecutive successful installs were each followed by another below-floor refusal. " +
                        "Escalations are stopped (one probe per {ProbeInterval}); the condition is surfaced as RescueNotConverging on IRaft.GetSnapshotStatuses. " +
                        "A follower that snapshots cannot rescue needs operator attention — check whether the follower applies installed state, and whether WAL compaction outruns it (CompactionLiveReplicaLagBudget)",
                        host.LocalEndpoint, host.PartitionId, getNodeState(), endpoint,
                        state.ConsecutiveCycles, host.Configuration.SnapshotRescueProbeInterval);
                    return false;
                }
            }

            if (!state.Tripped)
                return true;

            long probeMs = (long)host.Configuration.SnapshotRescueProbeInterval.TotalMilliseconds;
            if (probeMs <= 0 || now - state.LastProbeTicks < MsToTicks(probeMs))
                return false;

            state.LastProbeTicks = now;
            return true;
        }
    }

    /// <summary>
    /// Records a confirmed install for the convergence accounting: if the next event for this
    /// follower is another below-floor escalation rather than silence, that pair counts as one
    /// non-converging rescue cycle in <see cref="RescueCycleAdmits"/>.
    /// </summary>
    private void RecordInstallForConvergenceTracking(string endpoint)
    {
        if (host.Configuration.SnapshotRescueMaxConsecutiveCycles <= 0)
            return;

        RescueCycleState state = rescueCycles.GetOrAdd(endpoint, static _ => new RescueCycleState());
        lock (state)
        {
            state.InstallPendingConvergence = true;
            state.LastActivityTicks = host.GetMonotonicTimestamp();
        }
    }

    /// <summary>
    /// Whether the tripped breaker is currently blocking attempts for <paramref name="endpoint"/>.
    /// Refreshes the episode's activity stamp while blocking, so a stream of refusals that never
    /// reaches <see cref="TrySend"/> still keeps the episode (and its status entry) alive — the
    /// quiet-window reset must fire only when the refusals themselves stop.
    /// </summary>
    private bool IsRescueBreakerBlocking(string endpoint)
    {
        if (host.Configuration.SnapshotRescueMaxConsecutiveCycles <= 0)
            return false;

        if (!rescueCycles.TryGetValue(endpoint, out RescueCycleState? state))
            return false;

        lock (state)
        {
            long now = host.GetMonotonicTimestamp();
            ResetRescueEpisodeIfQuietLocked(state, endpoint, now);

            if (!state.Tripped)
                return false;

            state.LastActivityTicks = now;

            long probeMs = (long)host.Configuration.SnapshotRescueProbeInterval.TotalMilliseconds;
            return probeMs <= 0 || now - state.LastProbeTicks < MsToTicks(probeMs);
        }
    }

    /// <summary>
    /// Resets a rescue-cycle episode whose last activity is older than
    /// <see cref="RescueQuietWindowMs"/>: refusals stopped, so the follower converged (or is
    /// gone) and the counter — including a tripped breaker — must start fresh when refusals
    /// resume. The caller holds the entry's lock.
    /// </summary>
    private void ResetRescueEpisodeIfQuietLocked(RescueCycleState state, string endpoint, long now)
    {
        if (state.LastActivityTicks == 0 || now - state.LastActivityTicks <= MsToTicks(RescueQuietWindowMs))
            return;

        if (state.Tripped)
            logger.LogWarning(
                "[{LocalEndpoint}/{PartitionId}/{State}] Snapshot rescue breaker for {Endpoint} reset after a quiet period — the follower converged by other means or its refusals stopped",
                host.LocalEndpoint, host.PartitionId, getNodeState(), endpoint);

        state.ConsecutiveCycles = 0;
        state.InstallPendingConvergence = false;
        state.Tripped = false;
    }

    /// <summary>
    /// Records that <paramref name="node"/> needs a snapshot but none can be produced because no
    /// suitable transfer is registered on this leader. Called from the heartbeat path, which used
    /// to skip the follower <em>silently</em> in this situation — the follower was permanently
    /// unable to catch up (backfill cannot help below the floor) and nothing surfaced anywhere.
    /// One Warning per episode; while the condition persists the episode is kept alive so
    /// <see cref="GetStatuses"/> keeps reporting it, and the recorded pause keeps re-checks cheap.
    /// </summary>
    internal void ReportUnproducible(RaftNode node)
    {
        if (failureStates.TryGetValue(node.Endpoint, out FollowerSnapshotState? existing) && existing.Unproducible)
        {
            // Same episode: keep it alive without another log line or counted attempt.
            long now = host.GetMonotonicTimestamp();
            Volatile.Write(ref existing.LastFailureTicks, now);
            Volatile.Write(ref existing.PausedUntilTicks, now + MsToTicks(MaxPauseMs));
            return;
        }

        RecordFailure(node.Endpoint, cause: "no_transfer",
            error: "follower requires a snapshot (below the WAL compaction floor) but no snapshot transfer is registered; " +
                   "register IRaftPartitionStateTransfer (or IRaftStateMachineTransfer) on this node",
            unproducible: true);
    }

    /// <summary>
    /// Cheap pre-check for the refusal-path escalation in <c>BackfillSender</c>: whether a snapshot
    /// attempt for <paramref name="endpoint"/> could proceed right now. False while a transfer is
    /// already in flight, the follower sits inside a failure/unproducible backoff window, or the
    /// convergence breaker is tripped for it (probes excepted). The caller uses it to skip the WAL
    /// checkpoint read on the per-ack hot path — <see cref="TrySend"/> re-checks every guard
    /// itself, so this is an optimization, never the correctness gate.
    /// </summary>
    internal bool CanAttempt(string endpoint) =>
        !IsRescueBreakerBlocking(endpoint)
        && !pendingSnapshotEndpoints.ContainsKey(endpoint)
        && !IsBackedOff(endpoint)
        && !IsInSuccessPause(endpoint);

    /// <summary>
    /// Whether <paramref name="endpoint"/> sits inside the post-success pause — see
    /// <see cref="successPauseUntilTicks"/>. Expired entries are removed on read so the map does
    /// not accumulate healed followers.
    /// </summary>
    private bool IsInSuccessPause(string endpoint)
    {
        if (!successPauseUntilTicks.TryGetValue(endpoint, out long until))
            return false;

        if (host.GetMonotonicTimestamp() < until)
            return true;

        successPauseUntilTicks.TryRemove(endpoint, out _);
        return false;
    }

    /// <summary>
    /// Point-in-time snapshot-transfer status for every follower with an in-flight transfer, a
    /// recorded failure episode, or an active rescue-cycle episode (a non-converging rescue never
    /// fails, so without the last source it was invisible here). Empty on a healthy partition.
    /// Safe to call from any thread.
    /// </summary>
    internal IReadOnlyList<RaftSnapshotStatus> GetStatuses()
    {
        if (failureStates.IsEmpty && pendingSnapshotEndpoints.IsEmpty && rescueCycles.IsEmpty)
            return [];

        List<RaftSnapshotStatus> statuses = [];
        HashSet<string> reported = [];
        long now = host.GetMonotonicTimestamp();

        foreach ((string endpoint, FollowerSnapshotState state) in failureStates)
        {
            long remainingTicks = Volatile.Read(ref state.PausedUntilTicks) - now;
            bool inFlight = pendingSnapshotEndpoints.TryGetValue(endpoint, out long startedTicks);
            bool awaiting = awaitingInstallIndexes.TryGetValue(endpoint, out long awaitedIndex);
            (bool notConverging, int cycles) = ReadRescueView(endpoint);
            statuses.Add(new RaftSnapshotStatus
            {
                FollowerEndpoint = endpoint,
                FailedAttempts = state.FailedAttempts,
                LastError = state.LastError,
                Unproducible = state.Unproducible,
                InFlight = inFlight,
                AwaitingInstall = awaiting,
                AwaitingInstallIndex = awaiting ? awaitedIndex : null,
                InFlightFor = inFlight
                    ? TimeSpan.FromSeconds((double)(now - startedTicks) / Stopwatch.Frequency)
                    : null,
                FirstFailureAt = state.FirstFailureAt,
                LastFailureAt = state.LastFailureAt,
                RetryBackoffRemaining = remainingTicks > 0
                    ? TimeSpan.FromSeconds((double)remainingTicks / Stopwatch.Frequency)
                    : TimeSpan.Zero,
                RescueNotConverging = notConverging,
                ConsecutiveRescueCycles = cycles,
            });
            reported.Add(endpoint);
        }

        foreach ((string endpoint, long startedTicks) in pendingSnapshotEndpoints)
        {
            if (!reported.Add(endpoint))
                continue;

            bool awaiting = awaitingInstallIndexes.TryGetValue(endpoint, out long awaitedIndex);
            (bool notConverging, int cycles) = ReadRescueView(endpoint);
            statuses.Add(new RaftSnapshotStatus
            {
                FollowerEndpoint = endpoint,
                InFlight = true,
                AwaitingInstall = awaiting,
                AwaitingInstallIndex = awaiting ? awaitedIndex : null,
                InFlightFor = TimeSpan.FromSeconds((double)(now - startedTicks) / Stopwatch.Frequency),
                RescueNotConverging = notConverging,
                ConsecutiveRescueCycles = cycles,
            });
        }

        foreach ((string endpoint, RescueCycleState state) in rescueCycles)
        {
            bool tripped;
            int cycles;
            long lastActivity;
            lock (state)
            {
                tripped = state.Tripped;
                cycles = state.ConsecutiveCycles;
                lastActivity = state.LastActivityTicks;
            }

            // A quiet episode is over: purge it lazily so a healthy partition reports an empty
            // list again. Racing an executor-thread re-arm at the exact quiet boundary at worst
            // restarts the episode's counters — the same thing the quiet reset does deliberately.
            if (now - lastActivity > MsToTicks(RescueQuietWindowMs))
            {
                rescueCycles.TryRemove(endpoint, out _);
                continue;
            }

            if (reported.Contains(endpoint) || (!tripped && cycles == 0))
                continue;

            statuses.Add(new RaftSnapshotStatus
            {
                FollowerEndpoint = endpoint,
                RescueNotConverging = tripped,
                ConsecutiveRescueCycles = cycles,
            });
        }

        return statuses;
    }

    private (bool NotConverging, int Cycles) ReadRescueView(string endpoint)
    {
        if (!rescueCycles.TryGetValue(endpoint, out RescueCycleState? state))
            return (false, 0);

        lock (state)
            return (state.Tripped, state.ConsecutiveCycles);
    }

    /// <summary>
    /// Advances the follower's tracked replication progress (commit frontier, matchIndex,
    /// nextIndex, log-start) after the background snapshot task confirmed successful installation.
    /// Always called on the executor thread via the <c>postToExecutor</c> callback, preserving the
    /// single-owner invariant.
    /// </summary>
    internal void CompleteSnapshotInstalled(string endpoint, long snapshotIndex) =>
        onSnapshotInstalled(endpoint, snapshotIndex);

    private bool IsBackedOff(string endpoint)
    {
        if (!failureStates.TryGetValue(endpoint, out FollowerSnapshotState? state))
            return false;

        long now = host.GetMonotonicTimestamp();
        if (now < Volatile.Read(ref state.PausedUntilTicks))
            return true;

        // A long-quiet episode is a new episode: the follower progressed by other means (or the
        // condition cleared) before falling below the floor again — start the backoff ladder
        // fresh instead of resuming at the old attempt count.
        if (now - Volatile.Read(ref state.LastFailureTicks) > 2 * MsToTicks(MaxPauseMs))
            failureStates.TryRemove(endpoint, out _);

        return false;
    }

    /// <summary>
    /// Records one failed attempt for <paramref name="endpoint"/> and arms its retry pause:
    /// exponential from the heartbeat interval for transient causes, straight to
    /// <see cref="MaxPauseMs"/> for permanent ones. The first failure of an episode (or a changed
    /// error) logs at Warning; identical repeats drop to Debug, so a permanent condition costs one
    /// log line rather than one per heartbeat.
    /// </summary>
    private void RecordFailure(string endpoint, string cause, string error, bool unproducible)
    {
        FollowerSnapshotState state = failureStates.GetOrAdd(
            endpoint, static _ => new FollowerSnapshotState { FirstFailureAt = DateTimeOffset.UtcNow });

        int attempts = Interlocked.Increment(ref state.FailedAttempts);
        bool changedError = !string.Equals(state.LastError, error, StringComparison.Ordinal);
        state.LastError = error;
        state.Unproducible = unproducible;
        state.LastFailureAt = DateTimeOffset.UtcNow;

        long now = host.GetMonotonicTimestamp();
        Volatile.Write(ref state.LastFailureTicks, now);

        long pauseMs = unproducible
            ? MaxPauseMs
            : Math.Min(MaxPauseMs, BasePauseMs() << Math.Min(attempts - 1, 20));
        Volatile.Write(ref state.PausedUntilTicks, now + MsToTicks(pauseMs));

        KommanderMetrics.RecordSnapshotTransferFailure(host.PartitionId, cause);

        if (attempts == 1 || changedError)
            logger.LogWarning(
                "[{LocalEndpoint}/{PartitionId}/{State}] Snapshot transfer to {Endpoint} failed ({Cause}, attempt {Attempts}, retry in {PauseMs} ms): {Error}",
                host.LocalEndpoint, host.PartitionId, getNodeState(), endpoint, cause, attempts, pauseMs, error);
        else if (logger.IsEnabled(LogLevel.Debug))
            logger.LogDebug(
                "[{LocalEndpoint}/{PartitionId}/{State}] Snapshot transfer to {Endpoint} failed again ({Cause}, attempt {Attempts}, retry in {PauseMs} ms)",
                host.LocalEndpoint, host.PartitionId, getNodeState(), endpoint, cause, attempts, pauseMs);
    }

    /// <summary>
    /// Backoff base: one heartbeat interval, floored at 100 ms so a zero/near-zero test interval
    /// still produces a real pause instead of a spin.
    /// </summary>
    private long BasePauseMs() =>
        Math.Max(100, (long)host.Configuration.HeartbeatInterval.TotalMilliseconds);

    private static long MsToTicks(long ms) => ms * Stopwatch.Frequency / 1000;

    private async Task TrySendSnapshotAsync(RaftNode node, long snapshotIndex, long leaderTerm, long lastIncludedTerm)
    {
        const int chunkSize = 3 * 1024 * 1024;

        // Per-step watchdog: every awaited external step — the application export, one stream
        // read, one chunk send — must finish inside SnapshotTransferStepTimeout. A step that
        // completes resets the clock, so a large snapshot on a slow link is never cut off; only a
        // step that stops moving trips it. Without this bound, one hung export or one
        // deadline-less install RPC parked this task forever, the pendingSnapshotEndpoints entry
        // never released, and CanAttempt silently vetoed every later rescue for this follower.
        TimeSpan stepTimeout = host.Configuration.SnapshotTransferStepTimeout;
        using CancellationTokenSource transferCts = new();

        try
        {
            // Ask before exporting. An install of this partition may already be queued or running
            // on the follower — the one an earlier attempt of this leader sent, or a previous
            // leader's — and a second snapshot cannot be installed until it has finished. Waiting
            // for it costs nothing here; exporting again costs the whole partition on this node
            // and a second copy of it on the follower.
            unresolvedInstalls.TryGetValue(node.Endpoint, out AwaitedInstall remembered);

            SnapshotResponse known = await QueryInstallAsync(
                node, remembered.SessionId ?? "", snapshotIndex, leaderTerm, stepTimeout, transferCts).ConfigureAwait(false);

            switch (known.Outcome)
            {
                case SnapshotInstallOutcome.InstallPending:
                {
                    SnapshotResponse? ended = await AwaitInstallAsync(
                        node, known, snapshotIndex, leaderTerm, stepTimeout, transferCts, sentByThisTransfer: false).ConfigureAwait(false);

                    if (ended is not null)
                        CompleteTransfer(node, ended.Outcome, InstalledIndex(ended, snapshotIndex), chunksSent: 0);

                    return;
                }

                case SnapshotInstallOutcome.Installed or SnapshotInstallOutcome.SkippedAlreadyCovered:
                    // The install this leader sent earlier and never heard back about has finished.
                    unresolvedInstalls.TryRemove(node.Endpoint, out _);
                    CompleteTransfer(node, known.Outcome, InstalledIndex(known, snapshotIndex), chunksSent: 0);
                    return;

                case SnapshotInstallOutcome.NoInstall:
                case SnapshotInstallOutcome.Rejected when known.InstallIndex > 0:
                    // The follower no longer knows the remembered install, or it failed there:
                    // nothing to wait for, the transfer starts over.
                    unresolvedInstalls.TryRemove(node.Endpoint, out _);
                    break;

                // Anything else is no answer at all (a receiver that predates the question, or a
                // call that failed): nothing is known, so the transfer proceeds as it always did
                // and the remembered install, if any, is asked about again next time.
            }

            CachedExport? cached = TryGetReusableExport(snapshotIndex);

            Stream? snapshot = null;
            SnapshotKind kind;
            if (cached is not null)
                kind = cached.Kind;
            else
            {
                (Stream Stream, SnapshotKind Kind)? export =
                    await ExportSnapshotStreamAsync(node, snapshotIndex, stepTimeout, transferCts).ConfigureAwait(false);
                if (export is null)
                    return; // failure recorded (or unproducible reported) by the export step
                (snapshot, kind) = export.Value;
            }

            string sessionId = Guid.NewGuid().ToString("N");

            // Hashed incrementally as the snapshot's bytes are first seen, so the digest costs one
            // pass over bytes already in hand rather than a second read of the whole snapshot. The
            // receiver hashes the same way, which is why the digest can only travel on the
            // terminal chunk. Unused when replaying cached chunks — their digest was computed at
            // drain time and travels in the cache entry.
            using IncrementalHash snapshotHash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);

            SnapshotResponse answer;
            int chunksSent;
            try
            {
                List<byte[]>? overflowPrefix = null;
                if (snapshot is not null && host.Configuration.SnapshotExportRetryCacheMaxBytes > 0)
                {
                    (cached, overflowPrefix) = await DrainExportAsync(
                        snapshot, snapshotIndex, kind, chunkSize, snapshotHash, stepTimeout, transferCts).ConfigureAwait(false);

                    if (cached is not null)
                    {
                        // Fully drained: release the application's stream (and whatever buffer
                        // backs it) BEFORE any chunk is sent, and publish the cache slot even if
                        // every send below fails — a retry at this index then costs no export.
                        await snapshot.DisposeAsync().ConfigureAwait(false);
                        snapshot = null;
                        Volatile.Write(ref exportCache, cached);
                    }
                }

                (answer, chunksSent) = cached is not null
                    ? await SendCachedChunksAsync(node, cached, sessionId, leaderTerm, lastIncludedTerm, stepTimeout, transferCts).ConfigureAwait(false)
                    : await StreamChunksAsync(node, snapshot!, overflowPrefix, sessionId, snapshotIndex, kind, leaderTerm, lastIncludedTerm, chunkSize, snapshotHash, stepTimeout, transferCts).ConfigureAwait(false);
            }
            finally
            {
                if (snapshot is not null)
                    await snapshot.DisposeAsync().ConfigureAwait(false);
            }

            // The receiver has the snapshot (or refused to stage it because another install of the
            // partition is running) and the install's outcome is a separate question: wait for it.
            // The export stream and the read buffer are released by now; only the retry cache, if
            // the export fitted it, outlives the wait.
            if (answer.Outcome == SnapshotInstallOutcome.InstallPending)
            {
                SnapshotResponse? ended = await AwaitInstallAsync(
                    node, answer, snapshotIndex, leaderTerm, stepTimeout, transferCts,
                    sentByThisTransfer: string.Equals(answer.InstallSessionId, sessionId, StringComparison.Ordinal)).ConfigureAwait(false);

                if (ended is null)
                    return; // failure recorded, or the wait was given up, by AwaitInstallAsync

                answer = ended;
            }

            // The terminal chunk's answer is the only statement about installation. A receiver
            // that acknowledged it as a mere staged chunk never ran the install: that is not a
            // seeded follower, and treating it as one is exactly how a follower that imported
            // nothing was logged as seeded while its acknowledged writes went missing.
            if (answer.Outcome == SnapshotInstallOutcome.ChunkAccepted)
            {
                RecordFailure(node.Endpoint, cause: "terminal_chunk_without_install",
                    error: $"the receiver acknowledged the terminal chunk for index {snapshotIndex} without an install outcome (an older receiver, or a chunk pipeline that answered before the install ran)",
                    unproducible: false);
                return;
            }

            if (answer.Outcome is SnapshotInstallOutcome.Installed or SnapshotInstallOutcome.SkippedAlreadyCovered)
            {
                unresolvedInstalls.TryRemove(node.Endpoint, out _);
                CompleteTransfer(node, answer.Outcome, InstalledIndex(answer, snapshotIndex), chunksSent);
            }
        }
        catch (TimeoutException ex)
        {
            // A hung step. The attempt is abandoned (the zombie step task is never awaited again)
            // and recorded as a normal failure, so the backoff paces a retry instead of the old
            // behaviour: an eternal in-flight guard that silently vetoed every later rescue.
            RecordFailure(node.Endpoint, cause: "step_timeout", error: ex.Message, unproducible: false);
        }
        catch (Exception ex)
        {
            RecordFailure(node.Endpoint, cause: "transfer_error",
                error: $"unhandled snapshot transfer error: {ex.Message}",
                unproducible: false);
        }
        finally
        {
            pendingSnapshotEndpoints.TryRemove(node.Endpoint, out _);
            forcedEndpoints.TryRemove(node.Endpoint, out _);
        }
    }

    /// <summary>
    /// The index the follower's boundary now covers: the one the install carried when the receiver
    /// names it (an awaited install can be an earlier attempt's, at an older checkpoint), else the
    /// index this transfer sent.
    /// </summary>
    private static long InstalledIndex(SnapshotResponse answer, long snapshotIndex) =>
        answer.InstallIndex > 0 ? answer.InstallIndex : snapshotIndex;

    /// <summary>
    /// Closes a transfer whose follower reports the snapshot installed or already covered at
    /// <paramref name="installedIndex"/>: clears the failure episode, arms the post-success pause and
    /// the convergence accounting, logs the outcome, and posts the cursor advance to the executor.
    /// <paramref name="chunksSent"/> is 0 when this transfer sent nothing and only waited for an
    /// install that was already running.
    /// <para>The index is trusted whoever sent that install. It is a checkpoint of the leader that
    /// exported it, so it is committed and this leader holds the same entries through it; and the
    /// receiver validated that sender's term and membership before it imported anything.</para>
    /// </summary>
    private void CompleteTransfer(RaftNode node, SnapshotInstallOutcome outcome, long installedIndex, int chunksSent)
    {
        failureStates.TryRemove(node.Endpoint, out _);

        // Arm the post-success pause before the pending guard is released (the caller's finally):
        // the refusal-path escalation can fire again on the very next ack, and the follower
        // legitimately keeps reporting a below-floor frontier until the install lands.
        successPauseUntilTicks[node.Endpoint] =
            host.GetMonotonicTimestamp() + MsToTicks(BasePauseMs());

        // Convergence accounting must be armed before the pending guard is released too:
        // if the next escalation for this endpoint pairs with this install, that is one
        // non-converging rescue cycle (see RescueCycleAdmits).
        RecordInstallForConvergenceTracking(node.Endpoint);

        // Warning outside the cooldown: this line ends a below-the-floor rescue incident
        // and must be visible at the default consumer log level (see RescueWarnCooldownMs).
        // "Seeded" is written only for an install the receiver reports as an import; a
        // skip says so, because nothing on the receiver changed.
        bool warn = TryOpenWarnWindow(lastInstallWarnTicks, node.Endpoint);
        if (outcome == SnapshotInstallOutcome.Installed)
        {
            if (warn)
                logger.LogWarnSnapshotInstalled(host.LocalEndpoint, host.PartitionId, getNodeState(), node.Endpoint, installedIndex, chunksSent);
            else if (logger.IsEnabled(LogLevel.Debug))
                logger.LogDebugSnapshotInstalled(host.LocalEndpoint, host.PartitionId, getNodeState(), node.Endpoint, installedIndex, chunksSent);
        }
        else
        {
            if (warn)
                logger.LogWarnSnapshotSkippedAlreadyCovered(host.LocalEndpoint, host.PartitionId, getNodeState(), node.Endpoint, installedIndex, chunksSent);
            else if (logger.IsEnabled(LogLevel.Debug))
                logger.LogDebugSnapshotSkippedAlreadyCovered(host.LocalEndpoint, host.PartitionId, getNodeState(), node.Endpoint, installedIndex, chunksSent);
        }

        getPostToExecutor()?.Invoke(new RaftRequest(
            RaftRequestType.SnapshotInstalled,
            commitIndex: installedIndex,
            endpoint: node.Endpoint));
    }

    /// <summary>
    /// Asks the follower what became of an install of this partition: the one named by
    /// <paramref name="sessionId"/>, or, with an empty id, whether any is queued or running. One
    /// transport call bounded like a chunk acknowledgement; a call that fails reads as
    /// <see cref="SnapshotInstallOutcome.Rejected"/> with no install named.
    /// </summary>
    private Task<SnapshotResponse> QueryInstallAsync(
        RaftNode node,
        string sessionId,
        long snapshotIndex,
        long leaderTerm,
        TimeSpan stepTimeout,
        CancellationTokenSource transferCts)
    {
        SnapshotRequest query = new()
        {
            StatusQuery = true,
            InstallPolling = true,
            SessionId = sessionId,
            PartitionId = host.PartitionId,
            SnapshotIndex = snapshotIndex,
            FollowerEndpoint = node.Endpoint,
            LeaderTerm = leaderTerm,
            LeaderEndpoint = host.LocalEndpoint,
            // Not a chunk. A receiver that predates the query reads a negative index as an
            // invalid chunk and refuses it without opening or touching a session.
            ChunkIndex = -1,
        };

        return AwaitStepAsync(
            host.QuerySnapshotInstallAsync(node, query, transferCts.Token),
            ChunkAckTimeout(stepTimeout), transferCts, "install status query");
    }

    /// <summary>
    /// Waits for the follower-side install described by <paramref name="pending"/> and returns the
    /// answer that reports it <see cref="SnapshotInstallOutcome.Installed"/> or
    /// <see cref="SnapshotInstallOutcome.SkippedAlreadyCovered"/>; returns <see langword="null"/>
    /// when the wait ended any other way (the failure is recorded here, or the wait was given up
    /// because this node stopped leading).
    ///
    /// <para><b>Bounded by progress.</b> The follower reports how far the install has read into its
    /// staged snapshot. The wait ends with a <see cref="TimeoutException"/> — recorded by the caller
    /// as <c>step_timeout</c>, like any step that stopped moving — only when that figure has not
    /// changed for <see cref="RaftConfiguration.SnapshotTransferStepTimeout"/>. How long the install
    /// takes in total is the application's business: a 200,000-record import on a disk at 90% busy
    /// takes tens of seconds, and a fixed bound sized for a chunk cut every one of them off.</para>
    ///
    /// <para><b>What ends it otherwise.</b> The follower reports the install failed
    /// (<c>install_failed</c>); it no longer knows the install, because it restarted
    /// (<c>install_lost</c>); or it has not answered any poll for
    /// <see cref="RaftConfiguration.SnapshotChunkAckTimeout"/> (<c>install_status_unanswered</c>).
    /// In the last case, and on a timeout, the install stays remembered in
    /// <see cref="unresolvedInstalls"/> and the next attempt asks about it before exporting.</para>
    ///
    /// <para>The follower runs one install per partition, so the install being waited for can be
    /// replaced by another while this loop runs (the first ended, a different sender's began). The
    /// loop then waits for that one: it is what stands between this follower and a new transfer.</para>
    /// </summary>
    private async Task<SnapshotResponse?> AwaitInstallAsync(
        RaftNode node,
        SnapshotResponse pending,
        long snapshotIndex,
        long leaderTerm,
        TimeSpan stepTimeout,
        CancellationTokenSource transferCts,
        bool sentByThisTransfer)
    {
        AwaitedInstall awaited = new(pending.InstallSessionId, pending.InstallIndex);
        unresolvedInstalls[node.Endpoint] = awaited;
        awaitingInstallIndexes[node.Endpoint] = awaited.SnapshotIndex;

        if (!sentByThisTransfer)
            LogWaitingForRunningInstall(node.Endpoint, snapshotIndex, pending);
        else if (logger.IsEnabled(LogLevel.Debug))
            logger.LogDebug(
                "[{LocalEndpoint}/{PartitionId}/{State}] Snapshot for {Endpoint} at index {Index} is staged there; waiting for its install",
                host.LocalEndpoint, host.PartitionId, getNodeState(), node.Endpoint, awaited.SnapshotIndex);

        long progress = pending.InstallProgress;
        long lastProgressTicks = host.GetMonotonicTimestamp();
        long lastAnswerTicks = lastProgressTicks;
        TimeSpan pause = InstallPollMinPause;
        TimeSpan unansweredBound = ChunkAckTimeout(stepTimeout);

        try
        {
            while (true)
            {
                await PauseAsync(pause).ConfigureAwait(false);
                if (pause < InstallPollMaxPause)
                    pause = pause + pause < InstallPollMaxPause ? pause + pause : InstallPollMaxPause;

                // A deposed leader has no use for the outcome: its successor asks the follower
                // itself. The install stays remembered in case this node leads again. A node that
                // has been disposed has no use for anything.
                if (host.IsStopped || getNodeState() != RaftNodeState.Leader)
                {
                    if (logger.IsEnabled(LogLevel.Debug))
                        logger.LogDebug(
                            "[{LocalEndpoint}/{PartitionId}/{State}] No longer waiting for the snapshot install on {Endpoint} at index {Index}: this node stopped leading",
                            host.LocalEndpoint, host.PartitionId, getNodeState(), node.Endpoint, awaited.SnapshotIndex);
                    return null;
                }

                SnapshotResponse answer = await QueryInstallAsync(
                    node, awaited.SessionId, snapshotIndex, leaderTerm, stepTimeout, transferCts).ConfigureAwait(false);

                long now = host.GetMonotonicTimestamp();

                switch (answer.Outcome)
                {
                    case SnapshotInstallOutcome.InstallPending:
                        lastAnswerTicks = now;

                        if (!string.Equals(answer.InstallSessionId, awaited.SessionId, StringComparison.Ordinal))
                        {
                            awaited = new AwaitedInstall(answer.InstallSessionId, answer.InstallIndex);
                            unresolvedInstalls[node.Endpoint] = awaited;
                            awaitingInstallIndexes[node.Endpoint] = awaited.SnapshotIndex;
                            progress = answer.InstallProgress;
                            lastProgressTicks = now;
                        }
                        else if (answer.InstallProgress != progress)
                        {
                            progress = answer.InstallProgress;
                            lastProgressTicks = now;
                        }
                        else if (Stopwatch.GetElapsedTime(lastProgressTicks, now) >= stepTimeout)
                        {
                            throw new TimeoutException(
                                $"the snapshot install at index {awaited.SnapshotIndex} on the follower made no progress within {stepTimeout.TotalSeconds:0.##}s (SnapshotTransferStepTimeout; {progress} staged bytes read); it is still running there and the next attempt waits for it instead of sending again");
                        }

                        continue;

                    case SnapshotInstallOutcome.Installed or SnapshotInstallOutcome.SkippedAlreadyCovered:
                        unresolvedInstalls.TryRemove(node.Endpoint, out _);
                        return answer;

                    case SnapshotInstallOutcome.NoInstall:
                        unresolvedInstalls.TryRemove(node.Endpoint, out _);
                        RecordFailure(node.Endpoint, cause: "install_lost",
                            error: $"the follower no longer reports the snapshot install at index {awaited.SnapshotIndex} that was running there (it restarted, or another install replaced it and ended)",
                            unproducible: false);
                        return null;

                    case SnapshotInstallOutcome.Rejected when answer.InstallIndex > 0:
                        unresolvedInstalls.TryRemove(node.Endpoint, out _);
                        RecordFailure(node.Endpoint, cause: "install_failed",
                            error: $"the snapshot install at index {answer.InstallIndex} failed on the follower (its log says why)",
                            unproducible: false);
                        return null;

                    default:
                        // No answer: the call failed or the follower refused the question. The
                        // install may well be running; keep asking until the follower has been
                        // silent for as long as a chunk acknowledgement may take.
                        if (Stopwatch.GetElapsedTime(lastAnswerTicks, now) >= unansweredBound)
                        {
                            RecordFailure(node.Endpoint, cause: "install_status_unanswered",
                                error: $"the follower has not answered for {unansweredBound.TotalSeconds:0.##}s what became of the snapshot install at index {awaited.SnapshotIndex} (SnapshotChunkAckTimeout); the next attempt asks again before sending anything",
                                unproducible: false);
                            return null;
                        }

                        continue;
                }
            }
        }
        finally
        {
            awaitingInstallIndexes.TryRemove(node.Endpoint, out _);
        }
    }

    /// <summary>
    /// One line when a transfer finds an install it did not send already running on the follower and
    /// waits for it. Warning outside the cooldown: the line explains why an escalation at one index
    /// is followed by no export and by an install reported at another.
    /// </summary>
    private void LogWaitingForRunningInstall(string endpoint, long snapshotIndex, SnapshotResponse pending)
    {
        if (TryOpenWarnWindow(lastWaitWarnTicks, endpoint))
            logger.LogWarning(
                "[{LocalEndpoint}/{PartitionId}/{State}] Snapshot transfer to {Endpoint} at index {Index} is waiting for an install already running there (index {InstallIndex}, sent by {InstallLeader} in term {InstallTerm}, {Progress} staged bytes read). Nothing more is staged on the follower until that install ends; its outcome decides whether a transfer is still needed",
                host.LocalEndpoint, host.PartitionId, getNodeState(), endpoint, snapshotIndex,
                pending.InstallIndex, pending.InstallLeaderEndpoint, pending.InstallLeaderTerm, pending.InstallProgress);
        else if (logger.IsEnabled(LogLevel.Debug))
            logger.LogDebug(
                "[{LocalEndpoint}/{PartitionId}/{State}] Snapshot transfer to {Endpoint} at index {Index} is waiting for the install already running there at index {InstallIndex}",
                host.LocalEndpoint, host.PartitionId, getNodeState(), endpoint, snapshotIndex, pending.InstallIndex);
    }

    /// <summary>
    /// Returns the cached export for <paramref name="snapshotIndex"/> when one exists and is
    /// fresh; clears a stale or superseded slot so multi-megabyte chunk arrays are not retained
    /// past their useful window.
    /// </summary>
    private CachedExport? TryGetReusableExport(long snapshotIndex)
    {
        CachedExport? cached = Volatile.Read(ref exportCache);
        if (cached is null)
            return null;

        if (cached.SnapshotIndex == snapshotIndex
            && host.GetMonotonicTimestamp() - cached.CreatedTicks <= MsToTicks(ExportCacheTtlMs))
            return cached;

        Interlocked.CompareExchange(ref exportCache, null, cached);
        return null;
    }

    /// <summary>
    /// Produces the export stream for one transfer, choosing among the registered transfer kinds.
    /// Returns <see langword="null"/> after recording the failure (or reporting the follower
    /// unproducible), so the caller simply stops; a hung export propagates as
    /// <see cref="TimeoutException"/> to the transfer-level handler.
    /// </summary>
    private async Task<(Stream Stream, SnapshotKind Kind)?> ExportSnapshotStreamAsync(
        RaftNode node, long snapshotIndex, TimeSpan stepTimeout, CancellationTokenSource transferCts)
    {
        bool useSystemState = host.PartitionId == RaftSystemConfig.SystemPartition
                              && host.SystemStateTransfer is not null;

        if (useSystemState)
        {
            try
            {
                Stream snapshot = await AwaitStepAsync(
                    host.SystemStateTransfer!.ExportPartitionState(host.PartitionId, snapshotIndex, transferCts.Token),
                    stepTimeout, transferCts, "ExportPartitionState (system)").ConfigureAwait(false);
                return (snapshot, SnapshotKind.SystemState);
            }
            catch (TimeoutException)
            {
                throw;
            }
            catch (Exception ex)
            {
                RecordFailure(node.Endpoint, cause: "export",
                    error: $"ExportPartitionState failed: {ex.Message}",
                    unproducible: ex is NotSupportedException);
                return null;
            }
        }

        if (host.PartitionStateTransfer is { } partitionTransfer)
        {
            // Preferred user-partition path: a dedicated whole-partition export, so applications
            // never have to serve "the entire partition" through the split-shaped ExportRange
            // plan below.
            try
            {
                Stream snapshot = await AwaitStepAsync(
                    partitionTransfer.ExportPartitionState(host.PartitionId, snapshotIndex, transferCts.Token),
                    stepTimeout, transferCts, "ExportPartitionState").ConfigureAwait(false);
                return (snapshot, SnapshotKind.PartitionState);
            }
            catch (TimeoutException)
            {
                throw;
            }
            catch (Exception ex)
            {
                RecordFailure(node.Endpoint, cause: "export",
                    error: $"ExportPartitionState failed: {ex.Message}",
                    unproducible: ex is NotSupportedException);
                return null;
            }
        }

        // Legacy fallback: overload the split/merge transfer with a boundless plan
        // (TargetPartitionId only) meaning "export this entire partition". Kept for
        // applications whose range transfer can serve whole-partition exports.
        IRaftStateMachineTransfer? transfer = host.StateMachineTransfer;
        if (transfer is null)
        {
            // Only reachable when a transfer was unregistered after the heartbeat gate
            // saw one; the steady no-transfer condition is reported by
            // ReportUnproducible from the heartbeat path itself.
            ReportUnproducible(node);
            return null;
        }

        RaftSplitPlan plan = new() { TargetPartitionId = host.PartitionId };
        try
        {
            Stream snapshot = await AwaitStepAsync(
                transfer.ExportRange(plan, snapshotIndex, transferCts.Token),
                stepTimeout, transferCts, "ExportRange").ConfigureAwait(false);
            return (snapshot, SnapshotKind.Range);
        }
        catch (TimeoutException)
        {
            throw;
        }
        catch (Exception ex)
        {
            RecordFailure(node.Endpoint, cause: "export",
                error: $"ExportRange failed: {ex.Message}",
                unproducible: ex is NotSupportedException);
            return null;
        }
    }

    /// <summary>
    /// Drains the export stream into exact-size chunk arrays for the retry cache, hashing as it
    /// goes. Two outcomes: the whole export fit inside
    /// <see cref="RaftConfiguration.SnapshotExportRetryCacheMaxBytes"/> and a complete
    /// <see cref="CachedExport"/> (checksum included) is returned; or the bound was crossed and
    /// the chunks read so far come back as an overflow prefix — all full-size, so none of them is
    /// terminal — for the caller to send ahead of the remaining live stream, uncached.
    /// </summary>
    private async Task<(CachedExport? Cache, List<byte[]>? OverflowPrefix)> DrainExportAsync(
        Stream snapshot,
        long snapshotIndex,
        SnapshotKind kind,
        int chunkSize,
        IncrementalHash snapshotHash,
        TimeSpan stepTimeout,
        CancellationTokenSource transferCts)
    {
        long cacheCap = host.Configuration.SnapshotExportRetryCacheMaxBytes;
        List<byte[]> chunks = [];
        long totalBytes = 0;

        byte[] buffer = ArrayPool<byte>.Shared.Rent(chunkSize);
        bool bufferDetached = false;
        try
        {
            while (true)
            {
                bufferDetached = true;
                int bytesRead = await AwaitStepAsync(
                    StreamUtils.ReadExactAsync(snapshot, buffer, chunkSize, transferCts.Token).AsTask(),
                    stepTimeout, transferCts, "snapshot stream read").ConfigureAwait(false);
                bufferDetached = false;

                if (bytesRead > 0)
                    snapshotHash.AppendData(buffer, 0, bytesRead);

                chunks.Add(buffer.AsSpan(0, bytesRead).ToArray());
                totalBytes += bytesRead;

                // A short (possibly empty) read is the terminal chunk — the whole export is in
                // hand, mirroring the streaming loop's isLast condition.
                if (bytesRead < chunkSize)
                    return (new CachedExport(
                        snapshotIndex, kind, chunks,
                        Convert.ToHexString(snapshotHash.GetHashAndReset()),
                        host.GetMonotonicTimestamp()), null);

                if (totalBytes > cacheCap)
                    return (null, chunks);
            }
        }
        finally
        {
            // An abandoned read step may still touch the rented buffer from its zombie task;
            // returning it to the pool would hand live memory to an unrelated renter. Leak it
            // deliberately in that case — the GC reclaims it when the zombie ends.
            if (!bufferDetached)
                ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    /// <summary>
    /// Replays a fully cached export chunk by chunk; no stream and no rented buffer are involved.
    /// Returns the terminal chunk's answer and the number of chunks sent — or, from the chunk that
    /// stopped the transfer, a <see cref="SnapshotInstallOutcome.Rejected"/> answer or the
    /// <see cref="SnapshotInstallOutcome.InstallPending"/> one of a receiver that stages nothing
    /// while another install of the partition runs.
    /// </summary>
    private async Task<(SnapshotResponse Answer, int ChunksSent)> SendCachedChunksAsync(
        RaftNode node,
        CachedExport cached,
        string sessionId,
        long leaderTerm,
        long lastIncludedTerm,
        TimeSpan stepTimeout,
        CancellationTokenSource transferCts)
    {
        IReadOnlyList<byte[]> chunks = cached.Chunks;
        for (int chunkIndex = 0; chunkIndex < chunks.Count; chunkIndex++)
        {
            bool isLast = chunkIndex == chunks.Count - 1;
            SnapshotResponse chunkAnswer = await SendOneChunkAsync(
                node, sessionId, cached.SnapshotIndex, cached.Kind, leaderTerm, lastIncludedTerm,
                chunkIndex, isLast, chunks[chunkIndex],
                isLast ? cached.Checksum : "",
                stepTimeout, transferCts).ConfigureAwait(false);

            // The terminal chunk's answer is the transfer's, whatever it says; a refused one was
            // not delivered and does not count as sent.
            if (isLast)
                return (chunkAnswer, chunkAnswer.Outcome == SnapshotInstallOutcome.Rejected ? chunkIndex : chunks.Count);

            if (StopsTheTransfer(chunkAnswer))
                return (chunkAnswer, chunkIndex);
        }

        return (new SnapshotResponse(SnapshotInstallOutcome.Rejected), chunks.Count);
    }

    /// <summary>
    /// Whether a chunk's answer ends the chunk loop before the terminal chunk: the receiver refused
    /// the chunk, or it is staging nothing for this partition because another install is running.
    /// </summary>
    private static bool StopsTheTransfer(SnapshotResponse chunkAnswer) =>
        chunkAnswer.Outcome is SnapshotInstallOutcome.Rejected or SnapshotInstallOutcome.InstallPending;

    /// <summary>
    /// The streaming send path: the cache is disabled, or the export crossed the cache bound
    /// mid-drain (then <paramref name="overflowPrefix"/> carries the already-read full-size chunks
    /// to send first, already hashed into <paramref name="snapshotHash"/>). Returns the terminal
    /// chunk's answer and the number of chunks sent, or the answer of the chunk that stopped the
    /// transfer (see <see cref="StopsTheTransfer"/>).
    /// </summary>
    private async Task<(SnapshotResponse Answer, int ChunksSent)> StreamChunksAsync(
        RaftNode node,
        Stream snapshot,
        List<byte[]>? overflowPrefix,
        string sessionId,
        long snapshotIndex,
        SnapshotKind kind,
        long leaderTerm,
        long lastIncludedTerm,
        int chunkSize,
        IncrementalHash snapshotHash,
        TimeSpan stepTimeout,
        CancellationTokenSource transferCts)
    {
        int chunkIndex = 0;

        if (overflowPrefix is not null)
        {
            foreach (byte[] data in overflowPrefix)
            {
                // Never terminal: the drain stops at full-size chunks only, so at least one more
                // read (possibly returning zero bytes) always follows below.
                SnapshotResponse prefixAnswer = await SendOneChunkAsync(
                    node, sessionId, snapshotIndex, kind, leaderTerm, lastIncludedTerm,
                    chunkIndex, isLast: false, data, "", stepTimeout, transferCts).ConfigureAwait(false);

                if (StopsTheTransfer(prefixAnswer))
                    return (prefixAnswer, chunkIndex);

                chunkIndex++;
            }
        }

        // Rent the read buffer instead of allocating a fresh 3 MiB (LOH) array per transfer; return
        // it once the transfer ends. The rented buffer may be larger than chunkSize — every read and
        // the chunk view are bounded to chunkSize, never buffer.Length.
        byte[] buffer = ArrayPool<byte>.Shared.Rent(chunkSize);
        bool bufferDetached = false;
        try
        {
            while (true)
            {
                bufferDetached = true;
                int bytesRead = await AwaitStepAsync(
                    StreamUtils.ReadExactAsync(snapshot, buffer, chunkSize, transferCts.Token).AsTask(),
                    stepTimeout, transferCts, "snapshot stream read").ConfigureAwait(false);
                bufferDetached = false;
                bool isLast = bytesRead < chunkSize;

                if (bytesRead > 0)
                    snapshotHash.AppendData(buffer, 0, bytesRead);

                // Terminal chunk only: this is the first point at which the digest over the whole
                // snapshot is known. GetHashAndReset is safe to call here because the loop ends
                // immediately after a successful last chunk.
                string checksum = isLast ? Convert.ToHexString(snapshotHash.GetHashAndReset()) : "";

                // Zero-copy view over the reused buffer. Safe because the send is awaited before
                // the next iteration overwrites the buffer, and every transport consumes Data
                // synchronously within that send (see SnapshotRequest.Data remarks).
                bufferDetached = true;
                SnapshotResponse chunkAnswer = await SendOneChunkAsync(
                    node, sessionId, snapshotIndex, kind, leaderTerm, lastIncludedTerm,
                    chunkIndex, isLast, buffer.AsMemory(0, bytesRead), checksum,
                    stepTimeout, transferCts).ConfigureAwait(false);
                bufferDetached = false;

                // The terminal chunk's answer is the transfer's, whatever it says; a refused one
                // was not delivered and does not count as sent.
                if (isLast)
                    return (chunkAnswer, chunkAnswer.Outcome == SnapshotInstallOutcome.Rejected ? chunkIndex : chunkIndex + 1);

                if (StopsTheTransfer(chunkAnswer))
                    return (chunkAnswer, chunkIndex);

                chunkIndex++;
            }
        }
        finally
        {
            // An abandoned read/send step may still touch the rented buffer from its zombie
            // task; returning it to the pool would hand live memory to an unrelated renter.
            // Leak it deliberately in that case — the GC reclaims it when the zombie ends.
            if (!bufferDetached)
                ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    /// <summary>
    /// Sends one chunk and awaits its acknowledgment under the chunk bound — the smaller of the
    /// step timeout and <see cref="RaftConfiguration.SnapshotChunkAckTimeout"/>, because a chunk to
    /// a reachable node is seconds of work and a receiver whose chunk path is wedged must fail
    /// the transfer in seconds, not minutes. A rejection records the failure; every answer is
    /// returned as the receiver reported it; a hung send propagates as
    /// <see cref="TimeoutException"/> to the transfer-level handler.
    /// <para>A terminal chunk that is not answered, or is answered with a bare rejection, may still
    /// have started the install — the answer can be lost after the receiver staged the chunk. Its
    /// session is remembered in <see cref="unresolvedInstalls"/> so the next attempt asks the
    /// follower about it before it exports anything.</para>
    /// </summary>
    private async Task<SnapshotResponse> SendOneChunkAsync(
        RaftNode node,
        string sessionId,
        long snapshotIndex,
        SnapshotKind kind,
        long leaderTerm,
        long lastIncludedTerm,
        int chunkIndex,
        bool isLast,
        ReadOnlyMemory<byte> data,
        string checksum,
        TimeSpan stepTimeout,
        CancellationTokenSource transferCts)
    {
        SnapshotRequest chunk = new()
        {
            SessionId = sessionId,
            PartitionId = host.PartitionId,
            SnapshotIndex = snapshotIndex,
            FollowerEndpoint = node.Endpoint,
            // Session metadata — identical on every chunk of this session. The receiver
            // rejects a session whose later chunks disagree, so these must not vary.
            LeaderTerm = leaderTerm,
            LeaderEndpoint = host.LocalEndpoint,
            LastIncludedTerm = lastIncludedTerm,
            ChunkIndex = chunkIndex,
            IsLast = isLast,
            Data = data,
            Kind = kind,
            SnapshotChecksum = checksum,
            Forced = forcedEndpoints.ContainsKey(node.Endpoint),
            InstallPolling = true,
        };

        SnapshotResponse response;
        try
        {
            response = await AwaitStepAsync(
                host.SendInstallSnapshotAsync(node, chunk, transferCts.Token),
                ChunkAckTimeout(stepTimeout), transferCts, $"install chunk {chunkIndex}").ConfigureAwait(false);
        }
        catch (TimeoutException) when (isLast)
        {
            unresolvedInstalls[node.Endpoint] = new AwaitedInstall(sessionId, snapshotIndex);
            throw;
        }

        if (response.Outcome == SnapshotInstallOutcome.Rejected)
        {
            if (isLast && response.InstallIndex <= 0)
                unresolvedInstalls[node.Endpoint] = new AwaitedInstall(sessionId, snapshotIndex);

            RecordFailure(node.Endpoint, cause: "chunk_rejected",
                error: $"snapshot chunk {chunkIndex} for index {snapshotIndex} was rejected by the follower",
                unproducible: false);
        }

        return response;
    }

    /// <summary>
    /// The bound on one acknowledgement from the follower — a chunk's, or a status answer's: the
    /// smaller of <see cref="RaftConfiguration.SnapshotChunkAckTimeout"/> and the step timeout.
    /// </summary>
    private TimeSpan ChunkAckTimeout(TimeSpan stepTimeout)
    {
        TimeSpan chunkTimeout = host.Configuration.SnapshotChunkAckTimeout;
        return chunkTimeout > stepTimeout ? stepTimeout : chunkTimeout;
    }

    /// <summary>
    /// Opens the Warning window for <paramref name="endpoint"/> in <paramref name="lastWarnTicks"/>
    /// if no Warning was logged inside the last <see cref="RescueWarnCooldownMs"/>. Returns true
    /// when the caller should log at Warning; false demotes the repeat to Debug.
    /// </summary>
    private bool TryOpenWarnWindow(ConcurrentDictionary<string, long> lastWarnTicks, string endpoint)
    {
        long now = host.GetMonotonicTimestamp();
        if (lastWarnTicks.TryGetValue(endpoint, out long last) && now - last < MsToTicks(RescueWarnCooldownMs))
            return false;

        lastWarnTicks[endpoint] = now;
        return true;
    }

    /// <summary>
    /// Longest real-time wait between two watchdog checks of one transfer step. See
    /// <see cref="AwaitStepAsync{T}"/>.
    /// </summary>
    private static readonly TimeSpan StepWatchdogMaxPoll = TimeSpan.FromMilliseconds(50);

    /// <summary>
    /// Awaits <paramref name="step"/> for at most <paramref name="timeout"/>. On timeout the
    /// transfer's cancellation source is cancelled — so a token-honouring callee stops too — and a
    /// <see cref="TimeoutException"/> naming <paramref name="stepName"/> is thrown; the abandoned
    /// step task keeps running as a detached zombie and must not share resources with the caller
    /// afterwards (see the rented-buffer handling in <see cref="StreamChunksAsync"/>).
    ///
    /// <para><b>The timeout reads the partition's tick source, not a timer.</b> The decision is
    /// <c>elapsed(host monotonic ticks) &gt;= timeout</c>, and a short real-time wait
    /// (<see cref="StepWatchdogMaxPoll"/>) only schedules the next check. In production the tick
    /// source is the process monotonic clock, so the bound is the same as a plain timer, with at
    /// most one poll interval of extra latency on a step that really hung. Under deterministic
    /// simulation the tick source is virtual, so the bound is measured in simulated time like every
    /// other elapsed-time gate. A single delay timer of the full timeout measured real time instead: a
    /// simulated cluster ran hundreds of steps inside one real second, so whether a hung step was
    /// abandoned inside a scenario's step budget depended on the speed of the machine.</para>
    ///
    /// <para>The poll never ends a step early. A late check only costs latency; the check itself
    /// cannot fire before the tick source says the timeout has passed.</para>
    /// </summary>
    private async Task<T> AwaitStepAsync<T>(Task<T> step, TimeSpan timeout, CancellationTokenSource transferCts, string stepName)
    {
        long startedTicks = host.GetMonotonicTimestamp();
        TimeSpan poll = timeout < StepWatchdogMaxPoll ? timeout : StepWatchdogMaxPoll;

        while (!step.IsCompleted)
        {
            // A plain tick difference, not RaftMonotonic.Elapsed: that helper reads an anchor of 0 as
            // "never set" and reports an infinite age, which would end every step at once on a
            // host whose tick source starts at 0.
            if (Stopwatch.GetElapsedTime(startedTicks, host.GetMonotonicTimestamp()) >= timeout)
            {
                await transferCts.CancelAsync().ConfigureAwait(false);
                throw new TimeoutException(
                    $"snapshot transfer step '{stepName}' made no progress within {timeout.TotalSeconds:0.##}s (SnapshotTransferStepTimeout / SnapshotChunkAckTimeout)");
            }

            await Task.WhenAny(step, RealTimePoll(poll)).ConfigureAwait(false);
        }

        return await step.ConfigureAwait(false);
    }

    /// <summary>
    /// Waits until <paramref name="duration"/> has passed on the partition's tick source. Like
    /// <see cref="AwaitStepAsync{T}"/>, the decision reads the tick source and the real-time wait only
    /// schedules the next check, so under simulation the pause between two polls of a running install
    /// is simulated time and the number of polls does not depend on the speed of the machine.
    /// Returns early once the node is disposed: a simulated clock stops with its run, and the pause
    /// would otherwise never end.
    /// </summary>
    private async Task PauseAsync(TimeSpan duration)
    {
        long startedTicks = host.GetMonotonicTimestamp();
        TimeSpan poll = duration < StepWatchdogMaxPoll ? duration : StepWatchdogMaxPoll;

        while (!host.IsStopped && Stopwatch.GetElapsedTime(startedTicks, host.GetMonotonicTimestamp()) < duration)
            await RealTimePoll(poll).ConfigureAwait(false);
    }

    /// <summary>
    /// The one real-time wait in this type: it schedules the next look at the tick source and never
    /// decides anything itself. No cancellation token: it is at most one poll interval, and a
    /// cancelled token would complete it at once and spin the loop that awaits it.
    /// </summary>
    private static Task RealTimePoll(TimeSpan poll) => Task.Delay(poll);
}
