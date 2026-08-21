
using System.Buffers;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Security.Cryptography;
using Kommander.Data;
using Kommander.Diagnostics;
using Kommander.Logging;
using Kommander.Scheduling;
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
    /// Per-follower pause armed after a SUCCESSFUL transfer. The refusal-path escalation fires per
    /// refused batch — on the ack fast-path that is per ack — and a follower keeps reporting a
    /// below-floor frontier until it finishes installing and its next ack reflects the seeded
    /// state. Without this pause, every ack in that window fired another full multi-chunk transfer
    /// back to back. One base pause restores the old heartbeat-interval pacing without touching
    /// <see cref="failureStates"/>, so a success still clears the failure-status surface.
    /// </summary>
    private readonly ConcurrentDictionary<string, long> successPauseUntilTicks = new();

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
    /// Fires a background snapshot transfer to <paramref name="node"/> if the follower is not
    /// inside a failure backoff window or the post-success pause and no transfer is already in
    /// progress for that endpoint (guarded by <c>pendingSnapshotEndpoints.TryAdd</c>). The entry
    /// is removed in the <c>finally</c> block of <see cref="TrySendSnapshotAsync"/> so a later
    /// refusal can retry on failure — paced by the recorded backoff rather than per refusal.
    /// </summary>
    internal void TrySend(RaftNode node, long snapshotIndex, long leaderTerm, long lastIncludedTerm)
    {
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

            _ = TrySendSnapshotAsync(node, snapshotIndex, leaderTerm, lastIncludedTerm);
        }
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
    /// already in flight or the follower sits inside a failure/unproducible backoff window. The
    /// caller uses it to skip the WAL checkpoint read on the per-ack hot path — <see cref="TrySend"/>
    /// re-checks both guards itself, so this is an optimization, never the correctness gate.
    /// </summary>
    internal bool CanAttempt(string endpoint) =>
        !pendingSnapshotEndpoints.ContainsKey(endpoint) && !IsBackedOff(endpoint) && !IsInSuccessPause(endpoint);

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
    /// Point-in-time snapshot-transfer status for every follower with an in-flight transfer or a
    /// recorded failure episode. Empty on a healthy partition. Safe to call from any thread.
    /// </summary>
    internal IReadOnlyList<RaftSnapshotStatus> GetStatuses()
    {
        if (failureStates.IsEmpty && pendingSnapshotEndpoints.IsEmpty)
            return [];

        List<RaftSnapshotStatus> statuses = [];
        long now = host.GetMonotonicTimestamp();

        foreach ((string endpoint, FollowerSnapshotState state) in failureStates)
        {
            long remainingTicks = Volatile.Read(ref state.PausedUntilTicks) - now;
            bool inFlight = pendingSnapshotEndpoints.TryGetValue(endpoint, out long startedTicks);
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
            });
        }

        foreach ((string endpoint, long startedTicks) in pendingSnapshotEndpoints)
        {
            if (!failureStates.ContainsKey(endpoint))
                statuses.Add(new RaftSnapshotStatus
                {
                    FollowerEndpoint = endpoint,
                    InFlight = true,
                    InFlightFor = TimeSpan.FromSeconds((double)(now - startedTicks) / Stopwatch.Frequency),
                });
        }

        return statuses;
    }

    /// <summary>
    /// Advances the <c>lastCommitIndexes</c> entry for the follower after the background
    /// snapshot task confirmed successful installation. Always called on the executor thread
    /// via the <c>postToExecutor</c> callback, preserving the single-owner invariant.
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
            bool useSystemState = host.PartitionId == RaftSystemConfig.SystemPartition
                                  && host.SystemStateTransfer is not null;

            Stream snapshot;
            SnapshotKind kind;
            if (useSystemState)
            {
                kind = SnapshotKind.SystemState;
                try
                {
                    snapshot = await AwaitStepAsync(
                        host.SystemStateTransfer!.ExportPartitionState(host.PartitionId, snapshotIndex, transferCts.Token),
                        stepTimeout, transferCts, "ExportPartitionState (system)").ConfigureAwait(false);
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
                    return;
                }
            }
            else if (host.PartitionStateTransfer is { } partitionTransfer)
            {
                // Preferred user-partition path: a dedicated whole-partition export, so
                // applications never have to serve "the entire partition" through the
                // split-shaped ExportRange plan below.
                kind = SnapshotKind.PartitionState;
                try
                {
                    snapshot = await AwaitStepAsync(
                        partitionTransfer.ExportPartitionState(host.PartitionId, snapshotIndex, transferCts.Token),
                        stepTimeout, transferCts, "ExportPartitionState").ConfigureAwait(false);
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
                    return;
                }
            }
            else
            {
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
                    return;
                }

                kind = SnapshotKind.Range;
                RaftSplitPlan plan = new() { TargetPartitionId = host.PartitionId };
                try
                {
                    snapshot = await AwaitStepAsync(
                        transfer.ExportRange(plan, snapshotIndex, transferCts.Token),
                        stepTimeout, transferCts, "ExportRange").ConfigureAwait(false);
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
                    return;
                }
            }

            string sessionId = Guid.NewGuid().ToString("N");
            // Rent the read buffer instead of allocating a fresh 3 MiB (LOH) array per transfer; return
            // it once the transfer ends. The rented buffer may be larger than chunkSize — every read and
            // the chunk view are bounded to chunkSize, never buffer.Length.
            byte[] buffer = ArrayPool<byte>.Shared.Rent(chunkSize);
            int chunkIndex = 0;
            bool success = false;
            bool bufferDetached = false;

            // Hashed incrementally as the snapshot streams, so the digest costs one pass over bytes
            // already in hand rather than a second read of the whole snapshot. The receiver hashes
            // the same way, which is why the digest can only travel on the terminal chunk.
            using IncrementalHash snapshotHash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);

            try
            {
                await using (snapshot.ConfigureAwait(false))
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
                            // Zero-copy view over the reused buffer. Safe because the send below is awaited
                            // before the next iteration overwrites the buffer, and every transport consumes
                            // Data synchronously within that send (see SnapshotRequest.Data remarks).
                            Data = buffer.AsMemory(0, bytesRead),
                            Kind = kind,
                            // Terminal chunk only: this is the first point at which the digest over
                            // the whole snapshot is known. GetHashAndReset is safe to call here
                            // because the loop breaks immediately after a successful last chunk.
                            SnapshotChecksum = isLast
                                ? Convert.ToHexString(snapshotHash.GetHashAndReset())
                                : "",
                        };

                        bufferDetached = true;
                        SnapshotResponse response = await AwaitStepAsync(
                            host.SendInstallSnapshotAsync(node, chunk, transferCts.Token),
                            stepTimeout, transferCts, $"install chunk {chunkIndex}").ConfigureAwait(false);
                        bufferDetached = false;
                        if (!response.Success)
                        {
                            RecordFailure(node.Endpoint, cause: "chunk_rejected",
                                error: $"snapshot chunk {chunkIndex} for index {snapshotIndex} was rejected by the follower",
                                unproducible: false);
                            return;
                        }

                        if (isLast)
                        {
                            success = true;
                            break;
                        }

                        chunkIndex++;
                    }
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

            if (success)
            {
                failureStates.TryRemove(node.Endpoint, out _);

                // Arm the post-success pause before the pending guard is released (finally below):
                // the refusal-path escalation can fire again on the very next ack, and the follower
                // legitimately keeps reporting a below-floor frontier until the install lands.
                successPauseUntilTicks[node.Endpoint] =
                    host.GetMonotonicTimestamp() + MsToTicks(BasePauseMs());

                // Warning outside the cooldown: this line ends a below-the-floor rescue incident
                // and must be visible at the default consumer log level (see RescueWarnCooldownMs).
                if (TryOpenWarnWindow(lastInstallWarnTicks, node.Endpoint))
                    logger.LogWarnSnapshotInstalled(host.LocalEndpoint, host.PartitionId, getNodeState(), node.Endpoint, snapshotIndex, chunkIndex + 1);
                else if (logger.IsEnabled(LogLevel.Debug))
                    logger.LogDebugSnapshotInstalled(host.LocalEndpoint, host.PartitionId, getNodeState(), node.Endpoint, snapshotIndex, chunkIndex + 1);

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
    /// Awaits <paramref name="step"/> for at most <paramref name="timeout"/>. On timeout the
    /// transfer's cancellation source is cancelled — so a token-honouring callee stops too — and a
    /// <see cref="TimeoutException"/> naming <paramref name="stepName"/> is thrown; the abandoned
    /// step task keeps running as a detached zombie and must not share resources with the caller
    /// afterwards (see the rented-buffer handling in <see cref="TrySendSnapshotAsync"/>).
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
