
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
        Action<string, long> onSnapshotInstalled)
    {
        this.host = host;
        this.logger = logger;
        this.getNodeState = getNodeState;
        this.getPostToExecutor = getPostToExecutor;
        this.onSnapshotInstalled = onSnapshotInstalled;
    }

    /// <summary>
    /// Called on the executor thread by the refused-backfill escalation in <c>BackfillSender</c>.
    /// Fires a background snapshot transfer to <paramref name="node"/> if the convergence breaker
    /// admits the attempt, the follower is not inside a failure backoff window or the post-success
    /// pause, and no transfer is already in progress for that endpoint (guarded by
    /// <c>pendingSnapshotEndpoints.TryAdd</c>). The entry is removed in the <c>finally</c> block of
    /// <see cref="TrySendSnapshotAsync"/> so a later refusal can retry on failure — paced by the
    /// recorded backoff rather than per refusal.
    /// </summary>
    internal void TrySend(RaftNode node, long snapshotIndex, long leaderTerm, long lastIncludedTerm)
    {
        if (!RescueCycleAdmits(node.Endpoint))
            return;

        if (IsBackedOff(node.Endpoint) || IsInSuccessPause(node.Endpoint))
            return;

        if (pendingSnapshotEndpoints.TryAdd(node.Endpoint, host.GetMonotonicTimestamp()))
        {
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
            (bool notConverging, int cycles) = ReadRescueView(endpoint);
            statuses.Add(new RaftSnapshotStatus
            {
                FollowerEndpoint = endpoint,
                FailedAttempts = state.FailedAttempts,
                LastError = state.LastError,
                Unproducible = state.Unproducible,
                InFlight = inFlight,
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

            (bool notConverging, int cycles) = ReadRescueView(endpoint);
            statuses.Add(new RaftSnapshotStatus
            {
                FollowerEndpoint = endpoint,
                InFlight = true,
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

            bool success;
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

                (success, chunksSent) = cached is not null
                    ? await SendCachedChunksAsync(node, cached, sessionId, leaderTerm, lastIncludedTerm, stepTimeout, transferCts).ConfigureAwait(false)
                    : await StreamChunksAsync(node, snapshot!, overflowPrefix, sessionId, snapshotIndex, kind, leaderTerm, lastIncludedTerm, chunkSize, snapshotHash, stepTimeout, transferCts).ConfigureAwait(false);
            }
            finally
            {
                if (snapshot is not null)
                    await snapshot.DisposeAsync().ConfigureAwait(false);
            }

            if (success)
            {
                failureStates.TryRemove(node.Endpoint, out _);

                // Arm the post-success pause before the pending guard is released (finally below):
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
                if (TryOpenWarnWindow(lastInstallWarnTicks, node.Endpoint))
                    logger.LogWarnSnapshotInstalled(host.LocalEndpoint, host.PartitionId, getNodeState(), node.Endpoint, snapshotIndex, chunksSent);
                else if (logger.IsEnabled(LogLevel.Debug))
                    logger.LogDebugSnapshotInstalled(host.LocalEndpoint, host.PartitionId, getNodeState(), node.Endpoint, snapshotIndex, chunksSent);

                getPostToExecutor()?.Invoke(new RaftRequest(
                    RaftRequestType.SnapshotInstalled,
                    commitIndex: snapshotIndex,
                    endpoint: node.Endpoint));
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
        }
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

    /// <summary>Replays a fully cached export chunk-by-chunk. No stream and no rented buffer are involved.</summary>
    private async Task<(bool Success, int ChunksSent)> SendCachedChunksAsync(
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
            bool sent = await SendOneChunkAsync(
                node, sessionId, cached.SnapshotIndex, cached.Kind, leaderTerm, lastIncludedTerm,
                chunkIndex, isLast, chunks[chunkIndex],
                isLast ? cached.Checksum : "",
                stepTimeout, transferCts).ConfigureAwait(false);

            if (!sent)
                return (false, chunkIndex);
        }

        return (true, chunks.Count);
    }

    /// <summary>
    /// The streaming send path: the cache is disabled, or the export crossed the cache bound
    /// mid-drain (then <paramref name="overflowPrefix"/> carries the already-read full-size chunks
    /// to send first, already hashed into <paramref name="snapshotHash"/>).
    /// </summary>
    private async Task<(bool Success, int ChunksSent)> StreamChunksAsync(
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
                bool sent = await SendOneChunkAsync(
                    node, sessionId, snapshotIndex, kind, leaderTerm, lastIncludedTerm,
                    chunkIndex, isLast: false, data, "", stepTimeout, transferCts).ConfigureAwait(false);

                if (!sent)
                    return (false, chunkIndex);

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
                bool sent = await SendOneChunkAsync(
                    node, sessionId, snapshotIndex, kind, leaderTerm, lastIncludedTerm,
                    chunkIndex, isLast, buffer.AsMemory(0, bytesRead), checksum,
                    stepTimeout, transferCts).ConfigureAwait(false);
                bufferDetached = false;

                if (!sent)
                    return (false, chunkIndex);

                chunkIndex++;
                if (isLast)
                    return (true, chunkIndex);
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
    /// Sends one chunk and awaits its acknowledgment under the step timeout. A rejection records
    /// the failure and returns <see langword="false"/>; a hung send propagates as
    /// <see cref="TimeoutException"/> to the transfer-level handler.
    /// </summary>
    private async Task<bool> SendOneChunkAsync(
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
        };

        SnapshotResponse response = await AwaitStepAsync(
            host.SendInstallSnapshotAsync(node, chunk, transferCts.Token),
            stepTimeout, transferCts, $"install chunk {chunkIndex}").ConfigureAwait(false);

        if (!response.Success)
        {
            RecordFailure(node.Endpoint, cause: "chunk_rejected",
                error: $"snapshot chunk {chunkIndex} for index {snapshotIndex} was rejected by the follower",
                unproducible: false);
            return false;
        }

        return true;
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
    /// Awaits <paramref name="step"/> for at most <paramref name="timeout"/>. On timeout the
    /// transfer's cancellation source is cancelled — so a token-honouring callee stops too — and a
    /// <see cref="TimeoutException"/> naming <paramref name="stepName"/> is thrown; the abandoned
    /// step task keeps running as a detached zombie and must not share resources with the caller
    /// afterwards (see the rented-buffer handling in <see cref="StreamChunksAsync"/>).
    /// </summary>
    private static async Task<T> AwaitStepAsync<T>(Task<T> step, TimeSpan timeout, CancellationTokenSource transferCts, string stepName)
    {
        Task completed = await Task.WhenAny(step, Task.Delay(timeout, transferCts.Token)).ConfigureAwait(false);
        if (completed != step)
        {
            await transferCts.CancelAsync().ConfigureAwait(false);
            throw new TimeoutException(
                $"snapshot transfer step '{stepName}' made no progress within {timeout.TotalSeconds:0}s (SnapshotTransferStepTimeout)");
        }

        return await step.ConfigureAwait(false);
    }
}
