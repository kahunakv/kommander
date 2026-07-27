
using System.Diagnostics;
using Kommander.Data;
using Kommander.Logging;
using Kommander.WAL;
using Microsoft.Extensions.Logging;

namespace Kommander;

/// <summary>
/// Identity of an in-progress snapshot-receive session. Keyed by the claimed sending endpoint,
/// the partition, and the session id together so two different leaders (or two terms of the same
/// leader) cannot alias one <see cref="SnapshotRequest.SessionId"/> and corrupt each other's buffers.
/// </summary>
internal readonly record struct SnapshotSessionKey(string LeaderEndpoint, int PartitionId, string SessionId);

/// <summary>
/// Owns the in-progress snapshot-receive session buffers on behalf of <see cref="RaftManager"/>.
///
/// <para>Each session accumulates the leader's chunked application snapshot until
/// <see cref="SnapshotRequest.IsLast"/>, then imports it and seeds a <c>CommittedCheckpoint</c>.
/// Sessions are keyed by <see cref="SnapshotSessionKey"/> and carry immutable transfer metadata
/// (leader term, last-included term, snapshot index, kind); a later chunk that disagrees with the
/// captured metadata, skips/reorders the chunk index, or arrives after the terminal chunk causes the
/// session to be rejected and dropped.</para>
///
/// <para><b>Bounded memory.</b> All mutable state is guarded by a single receiver lock. On every
/// receipt the receiver lazily expires idle sessions (past <c>sessionTtlTicks</c> of inactivity) and
/// enforces two global caps — a maximum session count and a maximum total buffered byte count —
/// by deterministically evicting the oldest (least-recently-active, then lowest composite key)
/// sessions and disposing their buffers. Expiry is lazy by design: an abandoned session's memory is
/// reclaimed on the next receipt or an explicit <see cref="SweepForTesting"/>, and is bounded in the
/// meantime by the byte cap.</para>
///
/// <para><b>Note (increment A).</b> Final validation still imports and writes the checkpoint on this
/// path; increment B moves acceptance and WAL mutation onto the partition executor's single-writer
/// path. Term-based rejection is therefore not yet enforced here.</para>
/// </summary>
internal sealed class SnapshotReceiver
{
    private readonly Dictionary<SnapshotSessionKey, SnapshotReceiveSession> _sessions = new();
    private readonly object _pendingSnapshotsLock = new();
    private long _totalPendingBytes;

    private readonly Func<bool> isDisposed;
    private readonly Func<IRaftSystemStateTransfer?> getSystemTransfer;
    private readonly Func<IRaftStateMachineTransfer?> getRangeTransfer;
    private readonly IWAL walAdapter;
    private readonly ILogger<IRaft> logger;
    private readonly string localEndpoint;

    private readonly long sessionTtlTicks;
    private readonly int maxPendingSessions;
    private readonly long maxPendingBytes;
    private readonly Func<long> getMonotonicTimestamp;

    internal SnapshotReceiver(
        Func<bool> isDisposed,
        Func<IRaftSystemStateTransfer?> getSystemTransfer,
        Func<IRaftStateMachineTransfer?> getRangeTransfer,
        IWAL walAdapter,
        ILogger<IRaft> logger,
        string localEndpoint,
        long sessionTtlTicks,
        int maxPendingSessions,
        long maxPendingBytes,
        Func<long> getMonotonicTimestamp)
    {
        this.isDisposed = isDisposed;
        this.getSystemTransfer = getSystemTransfer;
        this.getRangeTransfer = getRangeTransfer;
        this.walAdapter = walAdapter;
        this.logger = logger;
        this.localEndpoint = localEndpoint;
        this.sessionTtlTicks = sessionTtlTicks > 0 ? sessionTtlTicks : 1;
        this.maxPendingSessions = maxPendingSessions > 0 ? maxPendingSessions : 1;
        this.maxPendingBytes = maxPendingBytes > 0 ? maxPendingBytes : 1;
        this.getMonotonicTimestamp = getMonotonicTimestamp;
    }

    /// <summary>Converts a wall-clock duration to the <see cref="Stopwatch"/>-tick units used for TTL.</summary>
    internal static long TicksForDuration(TimeSpan duration)
    {
        long ticks = (long)(duration.TotalSeconds * Stopwatch.Frequency);
        return ticks > 0 ? ticks : 1;
    }

    /// <summary>
    /// Accumulates one snapshot chunk. Returns success for a well-ordered non-final chunk; on the
    /// terminal chunk it detaches the completed session (so its buffer is no longer counted against the
    /// caps or visible to eviction) and then imports outside the lock. A protocol violation
    /// (metadata change, skipped/out-of-order/negative chunk index, byte-budget overflow) drops the
    /// session and returns failure; an exact duplicate of the immediately-previous chunk is an
    /// idempotent success that is not appended again.
    /// </summary>
    internal async Task<SnapshotResponse> ReceiveInstallSnapshot(
        SnapshotRequest request,
        CancellationToken cancellationToken = default)
    {
        if (isDisposed())
            return new SnapshotResponse(false);

        bool isSystemState = request.Kind == SnapshotKind.SystemState;
        IRaftSystemStateTransfer? systemTransfer = null;
        IRaftStateMachineTransfer? rangeTransfer = null;
        if (isSystemState)
        {
            systemTransfer = getSystemTransfer();
            if (systemTransfer is null)
                return new SnapshotResponse(false);
        }
        else
        {
            rangeTransfer = getRangeTransfer();
            if (rangeTransfer is null)
                return new SnapshotResponse(false);
        }

        cancellationToken.ThrowIfCancellationRequested();

        // Already installed at or above this index: retried chunks after a successful install are an
        // idempotent success (the durable checkpoint is the source of truth, not the session buffer).
        long currentMax = walAdapter.GetMaxLog(request.PartitionId);
        if (currentMax >= request.SnapshotIndex)
            return new SnapshotResponse(true);

        MemoryStream completeBuffer;
        lock (_pendingSnapshotsLock)
        {
            if (isDisposed())
                return new SnapshotResponse(false);

            long now = getMonotonicTimestamp();
            ExpireIdleSessionsLocked(now);

            SnapshotSessionKey key = new(request.LeaderEndpoint ?? "", request.PartitionId, request.SessionId ?? "");

            if (request.ChunkIndex < 0)
            {
                // Negative index is never valid; drop any session it claims to belong to.
                if (_sessions.TryGetValue(key, out SnapshotReceiveSession? bad))
                    RemoveSessionLocked(key, bad);
                return new SnapshotResponse(false);
            }

            if (!_sessions.TryGetValue(key, out SnapshotReceiveSession? session))
            {
                // A fresh session must begin at chunk 0. A non-zero first chunk means we lost the
                // session (skipped opener, or a late chunk after the terminal chunk detached it).
                if (request.ChunkIndex != 0)
                    return new SnapshotResponse(false);

                EvictForSessionCapacityLocked();

                session = new SnapshotReceiveSession
                {
                    Key = key,
                    LeaderTerm = request.LeaderTerm,
                    LastIncludedTerm = request.LastIncludedTerm,
                    SnapshotIndex = request.SnapshotIndex,
                    Kind = request.Kind,
                    NextExpectedChunkIndex = 0,
                    Buffer = new MemoryStream(),
                    CreatedTimestamp = now,
                    LastActivityTimestamp = now,
                };
                _sessions[key] = session;
            }
            else
            {
                // Metadata must be identical across every chunk of a session.
                if (!MetadataMatches(session, request))
                {
                    RemoveSessionLocked(key, session);
                    return new SnapshotResponse(false);
                }

                // Exact duplicate of the immediately-previous chunk: idempotent success, do not append.
                if (session.NextExpectedChunkIndex > 0 && request.ChunkIndex == session.NextExpectedChunkIndex - 1)
                {
                    session.LastActivityTimestamp = now;
                    return new SnapshotResponse(true);
                }

                // Anything other than the exact next chunk is a skip/reorder: drop the session.
                if (request.ChunkIndex != session.NextExpectedChunkIndex)
                {
                    RemoveSessionLocked(key, session);
                    return new SnapshotResponse(false);
                }
            }

            int incoming = request.Data.Length;
            if (!EnsureByteCapacityLocked(key, incoming))
            {
                // Even after evicting every other session this chunk does not fit: reject and drop.
                RemoveSessionLocked(key, session);
                return new SnapshotResponse(false);
            }

            if (incoming > 0)
            {
                session.Buffer.Write(request.Data.Span);
                session.AccumulatedBytes += incoming;
                _totalPendingBytes += incoming;
            }

            session.NextExpectedChunkIndex++;
            session.LastActivityTimestamp = now;

            if (!request.IsLast)
                return new SnapshotResponse(true);

            // Terminal chunk: detach the completed session before the (awaited) import so it is neither
            // counted against the caps nor eligible for eviction while the import runs.
            _sessions.Remove(key);
            _totalPendingBytes -= session.AccumulatedBytes;
            completeBuffer = session.Buffer;
            completeBuffer.Position = 0;
        }

        try
        {
            if (isSystemState)
                await systemTransfer!.ImportPartitionState(request.PartitionId, completeBuffer, cancellationToken).ConfigureAwait(false);
            else
                await rangeTransfer!.ImportRange(request.PartitionId, completeBuffer, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            string importMethod = isSystemState ? "ImportPartitionState" : "ImportRange";
            logger.LogError(
                "[{Endpoint}] ReceiveInstallSnapshot: {Method} partition={PartitionId} index={Index} failed: {Message}",
                localEndpoint, importMethod, request.PartitionId, request.SnapshotIndex, ex.Message);
            return new SnapshotResponse(false);
        }
        finally
        {
            await completeBuffer.DisposeAsync().ConfigureAwait(false);
        }

        RaftLog checkpointLog = new()
        {
            Id = request.SnapshotIndex,
            Type = RaftLogType.CommittedCheckpoint,
            Term = walAdapter.GetCurrentTerm(request.PartitionId),
        };

        walAdapter.Write([(request.PartitionId, [checkpointLog])]);

        logger.LogInfoReceiveInstallSnapshot(localEndpoint, request.PartitionId, request.SnapshotIndex);

        return new SnapshotResponse(true);
    }

    private static bool MetadataMatches(SnapshotReceiveSession session, SnapshotRequest request) =>
        session.LeaderTerm == request.LeaderTerm
        && session.LastIncludedTerm == request.LastIncludedTerm
        && session.SnapshotIndex == request.SnapshotIndex
        && session.Kind == request.Kind;

    /// <summary>Removes idle sessions whose last activity is older than the TTL. Must hold the lock.</summary>
    private void ExpireIdleSessionsLocked(long now)
    {
        List<SnapshotSessionKey>? expired = null;
        foreach (KeyValuePair<SnapshotSessionKey, SnapshotReceiveSession> pair in _sessions)
        {
            if (now - pair.Value.LastActivityTimestamp > sessionTtlTicks)
                (expired ??= []).Add(pair.Key);
        }

        if (expired is null)
            return;

        foreach (SnapshotSessionKey key in expired)
        {
            if (_sessions.TryGetValue(key, out SnapshotReceiveSession? session))
                RemoveSessionLocked(key, session);
        }
    }

    /// <summary>Evicts oldest sessions until a new one can be added within the count cap. Must hold the lock.</summary>
    private void EvictForSessionCapacityLocked()
    {
        while (_sessions.Count >= maxPendingSessions)
        {
            SnapshotReceiveSession? victim = OldestEvictableLocked(default, hasExclude: false);
            if (victim is null)
                break;
            RemoveSessionLocked(victim.Key, victim);
        }
    }

    /// <summary>
    /// Evicts other sessions until <paramref name="incoming"/> more bytes fit within the global byte cap.
    /// Returns false if the incoming chunk cannot fit even after evicting everything else (i.e. this
    /// session alone would exceed the budget). Must hold the lock.
    /// </summary>
    private bool EnsureByteCapacityLocked(SnapshotSessionKey currentKey, int incoming)
    {
        if (incoming <= 0)
            return true;

        while (_totalPendingBytes + incoming > maxPendingBytes)
        {
            SnapshotReceiveSession? victim = OldestEvictableLocked(currentKey, hasExclude: true);
            if (victim is null)
                break;
            RemoveSessionLocked(victim.Key, victim);
        }

        return _totalPendingBytes + incoming <= maxPendingBytes;
    }

    /// <summary>
    /// Returns the session to evict first — least-recently-active, breaking ties by the composite key —
    /// optionally excluding <paramref name="exclude"/>. Deterministic. Must hold the lock.
    /// </summary>
    private SnapshotReceiveSession? OldestEvictableLocked(SnapshotSessionKey exclude, bool hasExclude)
    {
        SnapshotReceiveSession? best = null;
        foreach (KeyValuePair<SnapshotSessionKey, SnapshotReceiveSession> pair in _sessions)
        {
            if (hasExclude && pair.Key.Equals(exclude))
                continue;

            if (best is null
                || pair.Value.LastActivityTimestamp < best.LastActivityTimestamp
                || (pair.Value.LastActivityTimestamp == best.LastActivityTimestamp
                    && CompareKeys(pair.Key, best.Key) < 0))
            {
                best = pair.Value;
            }
        }

        return best;
    }

    private static int CompareKeys(SnapshotSessionKey a, SnapshotSessionKey b)
    {
        int c = string.CompareOrdinal(a.LeaderEndpoint, b.LeaderEndpoint);
        if (c != 0)
            return c;
        c = a.PartitionId.CompareTo(b.PartitionId);
        if (c != 0)
            return c;
        return string.CompareOrdinal(a.SessionId, b.SessionId);
    }

    private void RemoveSessionLocked(SnapshotSessionKey key, SnapshotReceiveSession session)
    {
        if (_sessions.Remove(key))
            _totalPendingBytes -= session.AccumulatedBytes;
        session.Buffer.Dispose();
    }

    /// <summary>Returns the count of active receive sessions. For test assertions only.</summary>
    internal int PendingSessionCount
    {
        get { lock (_pendingSnapshotsLock) return _sessions.Count; }
    }

    /// <summary>Returns total buffered snapshot bytes across active sessions. For test assertions only.</summary>
    internal long PendingByteCount
    {
        get { lock (_pendingSnapshotsLock) return _totalPendingBytes; }
    }

    /// <summary>
    /// Runs the lazy idle-expiry sweep on demand. Exists so tests (which drive a controllable monotonic
    /// clock) can force expiry of abandoned sessions without another receipt; production relies on the
    /// per-receipt sweep.
    /// </summary>
    internal void SweepForTesting()
    {
        lock (_pendingSnapshotsLock)
            ExpireIdleSessionsLocked(getMonotonicTimestamp());
    }

    /// <summary>
    /// Drains and disposes all in-progress receive buffers. Called during
    /// <see cref="RaftManager.Dispose"/> after the timer is stopped and before
    /// partition queues are drained.
    /// </summary>
    internal void DisposePendingSnapshots()
    {
        List<MemoryStream> snapshots = [];

        lock (_pendingSnapshotsLock)
        {
            foreach (KeyValuePair<SnapshotSessionKey, SnapshotReceiveSession> pending in _sessions)
                snapshots.Add(pending.Value.Buffer);

            _sessions.Clear();
            _totalPendingBytes = 0;
        }

        foreach (MemoryStream snapshot in snapshots)
            snapshot.Dispose();
    }

    /// <summary>
    /// Mutable state for one in-progress snapshot-receive session. Instances are only touched under the
    /// receiver lock. The metadata fields are captured from the first chunk and treated as immutable.
    /// </summary>
    private sealed class SnapshotReceiveSession
    {
        internal required SnapshotSessionKey Key { get; init; }
        internal required long LeaderTerm { get; init; }
        internal required long LastIncludedTerm { get; init; }
        internal required long SnapshotIndex { get; init; }
        internal required SnapshotKind Kind { get; init; }
        internal required MemoryStream Buffer { get; init; }
        internal required long CreatedTimestamp { get; init; }
        internal int NextExpectedChunkIndex;
        internal long AccumulatedBytes;
        internal long LastActivityTimestamp;
    }
}
