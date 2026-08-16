
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

    private readonly ConcurrentDictionary<string, byte> pendingSnapshotEndpoints = new();

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
    /// Called on the executor thread each heartbeat cycle. Fires a background snapshot
    /// transfer to <paramref name="node"/> if the follower is not inside a failure backoff
    /// window and no transfer is already in progress for that endpoint (guarded by
    /// <c>pendingSnapshotEndpoints.TryAdd</c>). The entry is removed in the <c>finally</c>
    /// block of <see cref="TrySendSnapshotAsync"/> so a later heartbeat can retry on failure —
    /// paced by the recorded backoff rather than every heartbeat.
    /// </summary>
    internal void TrySend(RaftNode node, long snapshotIndex, long leaderTerm, long lastIncludedTerm)
    {
        if (IsBackedOff(node.Endpoint))
            return;

        if (pendingSnapshotEndpoints.TryAdd(node.Endpoint, 0))
        {
            // Guard the Information log so the getNodeState() delegate is not invoked when
            // the level is disabled (CA1873).
            if (logger.IsEnabled(LogLevel.Information))
                logger.LogInfoStartingSnapshotTransfer(
                    host.LocalEndpoint, host.PartitionId, getNodeState(), node.Endpoint, snapshotIndex);
            _ = TrySendSnapshotAsync(node, snapshotIndex, leaderTerm, lastIncludedTerm);
        }
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
            statuses.Add(new RaftSnapshotStatus
            {
                FollowerEndpoint = endpoint,
                FailedAttempts = state.FailedAttempts,
                LastError = state.LastError,
                Unproducible = state.Unproducible,
                InFlight = pendingSnapshotEndpoints.ContainsKey(endpoint),
                FirstFailureAt = state.FirstFailureAt,
                LastFailureAt = state.LastFailureAt,
                RetryBackoffRemaining = remainingTicks > 0
                    ? TimeSpan.FromSeconds((double)remainingTicks / Stopwatch.Frequency)
                    : TimeSpan.Zero,
            });
        }

        foreach (string endpoint in pendingSnapshotEndpoints.Keys)
        {
            if (!failureStates.ContainsKey(endpoint))
                statuses.Add(new RaftSnapshotStatus { FollowerEndpoint = endpoint, InFlight = true });
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
                    snapshot = await host.SystemStateTransfer!
                        .ExportPartitionState(host.PartitionId, snapshotIndex, CancellationToken.None)
                        .ConfigureAwait(false);
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
                    snapshot = await partitionTransfer
                        .ExportPartitionState(host.PartitionId, snapshotIndex, CancellationToken.None)
                        .ConfigureAwait(false);
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
                    snapshot = await transfer.ExportRange(plan, snapshotIndex, CancellationToken.None).ConfigureAwait(false);
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
                        int bytesRead = await StreamUtils.ReadExactAsync(snapshot, buffer, chunkSize, CancellationToken.None).ConfigureAwait(false);
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

                        SnapshotResponse response = await host.SendInstallSnapshotAsync(node, chunk, CancellationToken.None).ConfigureAwait(false);
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
                ArrayPool<byte>.Shared.Return(buffer);
            }

            if (success)
            {
                failureStates.TryRemove(node.Endpoint, out _);

                // Guard the Information log so the getNodeState() delegate is not invoked when
                // the level is disabled (CA1873).
                if (logger.IsEnabled(LogLevel.Information))
                    logger.LogInfoSnapshotInstalled(host.LocalEndpoint, host.PartitionId, getNodeState(), node.Endpoint, snapshotIndex, chunkIndex + 1);

                getPostToExecutor()?.Invoke(new RaftRequest(
                    RaftRequestType.SnapshotInstalled,
                    commitIndex: snapshotIndex,
                    endpoint: node.Endpoint));
            }
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
}
