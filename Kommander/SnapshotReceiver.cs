
using System.Diagnostics;
using System.Security.Cryptography;
using Kommander.Data;
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
/// <para><b>Buffering only.</b> This class does not import or writes the WAL. On the terminal
/// chunk it hands the staged buffer plus session metadata to <c>installOnExecutor</c>, which routes the
/// install through the partition's single-writer executor where term validation, application import, and
/// the durable WAL boundary run serialized against every other partition operation.</para>
/// </summary>
internal sealed class SnapshotReceiver
{
    private readonly Dictionary<SnapshotSessionKey, SnapshotReceiveSession> _sessions = new();
    private readonly object _pendingSnapshotsLock = new();
    private long _totalPendingBytes;

    // Bytes/count of terminal buffers that have been detached from _sessions but are still live while their
    // install runs on the (single) partition executor. They MUST stay in the capacity accounting until the
    // install completes and the buffer is disposed — otherwise a sender whose installs block behind the
    // executor can retain unbounded full snapshot payloads despite the pending-session/byte caps.
    private long _inInstallBytes;
    private int _inInstallCount;

    private readonly Func<bool> isDisposed;
    private readonly Func<SnapshotInstallRequest, Task<SnapshotResponse>> installOnExecutor;
    private readonly ILogger<IRaft> logger;
    private readonly string localEndpoint;

    private readonly long sessionTtlTicks;
    private readonly int maxPendingSessions;
    private readonly long maxPendingBytes;
    private readonly Func<long> getMonotonicTimestamp;
    private readonly Func<bool> allowLegacySenders;

    internal SnapshotReceiver(
        Func<bool> isDisposed,
        Func<SnapshotInstallRequest, Task<SnapshotResponse>> installOnExecutor,
        ILogger<IRaft> logger,
        string localEndpoint,
        long sessionTtlTicks,
        int maxPendingSessions,
        long maxPendingBytes,
        Func<long> getMonotonicTimestamp,
        Func<bool>? allowLegacySenders = null)
    {
        this.isDisposed = isDisposed;
        this.installOnExecutor = installOnExecutor;
        this.logger = logger;
        this.localEndpoint = localEndpoint;
        this.sessionTtlTicks = sessionTtlTicks > 0 ? sessionTtlTicks : 1;
        this.maxPendingSessions = maxPendingSessions > 0 ? maxPendingSessions : 1;
        this.maxPendingBytes = maxPendingBytes > 0 ? maxPendingBytes : 1;
        this.getMonotonicTimestamp = getMonotonicTimestamp;
        // Read through a delegate rather than captured once: the flag lives on RaftConfiguration,
        // which tests flip after construction (see the legacy-sender cases in TestSnapshotInstallExecutor).
        this.allowLegacySenders = allowLegacySenders ?? (static () => false);
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

        cancellationToken.ThrowIfCancellationRequested();

        MemoryStream completeBuffer;
        SnapshotReceiveSession completedSession;
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
                    Hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256),
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
                // Hashed on the same branch that appends, so the digest tracks exactly the bytes that
                // were staged: the duplicate-chunk and reject paths above return before reaching here
                // and so must not advance the hash either.
                session.Hash.AppendData(request.Data.Span);
                session.AccumulatedBytes += incoming;
                _totalPendingBytes += incoming;
            }

            session.NextExpectedChunkIndex++;
            session.LastActivityTimestamp = now;

            if (!request.IsLast)
                return new SnapshotResponse(true);

            // Integrity gate, immediately before the assembled bytes become eligible for install.
            // Everything checked until now is structural (term, fence, chunk order) and says nothing
            // about content, so this is the only check that would catch a tampered payload or a
            // silently truncated transfer that still satisfied the index rules.
            if (!VerifyChecksumLocked(session, request))
            {
                RemoveSessionLocked(key, session);
                return new SnapshotResponse(false);
            }

            // Terminal chunk: detach the completed session from _sessions (so it is not eligible for idle
            // eviction and a late/duplicate chunk cannot re-match it) but keep its bytes in the capacity
            // accounting — MOVE them from the pending pool to the in-install pool rather than dropping them —
            // so the buffer that stays live while its install runs on the executor still counts against the
            // caps. Its bytes/count are released only when the install completes and the buffer is disposed.
            _sessions.Remove(key);
            _totalPendingBytes -= session.AccumulatedBytes;
            _inInstallBytes += session.AccumulatedBytes;
            _inInstallCount++;
            completedSession = session;
            completeBuffer = session.Buffer;
            completeBuffer.Position = 0;
        }

        // Hand the staged snapshot to the partition executor's single-writer install path. All term
        // validation, application import, and durable WAL mutation happen there — this class only buffers.
        SnapshotInstallRequest install = new()
        {
            PartitionId = request.PartitionId,
            SnapshotIndex = completedSession.SnapshotIndex,
            LastIncludedTerm = completedSession.LastIncludedTerm,
            LeaderTerm = completedSession.LeaderTerm,
            LeaderEndpoint = completedSession.Key.LeaderEndpoint,
            Kind = completedSession.Kind,
            Snapshot = completeBuffer,
        };

        try
        {
            return await installOnExecutor(install).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogError(
                "[{Endpoint}] ReceiveInstallSnapshot: install partition={PartitionId} index={Index} failed: {Message}",
                localEndpoint, request.PartitionId, request.SnapshotIndex, ex.Message);
            return new SnapshotResponse(false);
        }
        finally
        {
            // Release the in-install reservation, then dispose the buffer. The executor read the stream to
            // completion before installOnExecutor returned, so the buffer is safe to release here (see
            // RaftPartition.InstallSnapshotAsync — it uses no-cancellation Ask).
            lock (_pendingSnapshotsLock)
            {
                _inInstallBytes -= completedSession.AccumulatedBytes;
                _inInstallCount--;
            }
            await completeBuffer.DisposeAsync().ConfigureAwait(false);
            // The completed session was detached from _sessions above, so RemoveSessionLocked never
            // runs for it and this is the only place its hash is released.
            completedSession.Hash.Dispose();
        }
    }

    /// <summary>
    /// Verifies the digest carried on the terminal chunk against the bytes actually staged. Must hold
    /// the lock. Returns false when the transfer must be rejected.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A missing digest is a legacy sender. It is refused unless <c>AllowLegacySnapshotSenders</c> is
    /// on, matching how the other post-hoc session fields (leader term, leader endpoint, last-included
    /// term) are handled — the alternative, accepting unverified snapshots by default, would leave the
    /// control with no effect on exactly the deployments that never set the flag.
    /// </para>
    /// <para>
    /// Compared with <see cref="CryptographicOperations.FixedTimeEquals"/> over the raw digests rather
    /// than by string comparison. The timing channel is not the real concern here — a snapshot digest
    /// is not a secret — but decoding first also rejects malformed hex outright instead of letting a
    /// case or formatting difference read as a content mismatch.
    /// </para>
    /// </remarks>
    private bool VerifyChecksumLocked(SnapshotReceiveSession session, SnapshotRequest request)
    {
        byte[] actual = session.Hash.GetHashAndReset();

        if (string.IsNullOrEmpty(request.SnapshotChecksum))
        {
            if (allowLegacySenders())
            {
                logger.LogWarning(
                    "[{Endpoint}] ReceiveInstallSnapshot: partition={PartitionId} index={Index} arrived with no "
                    + "checksum (legacy sender) and was accepted unverified because AllowLegacySnapshotSenders is on.",
                    localEndpoint, request.PartitionId, request.SnapshotIndex);
                return true;
            }

            logger.LogWarning(
                "[{Endpoint}] ReceiveInstallSnapshot rejected: partition={PartitionId} index={Index} carries no "
                + "SnapshotChecksum (legacy sender) and AllowLegacySnapshotSenders is off.",
                localEndpoint, request.PartitionId, request.SnapshotIndex);
            return false;
        }

        byte[] expected;

        try
        {
            expected = Convert.FromHexString(request.SnapshotChecksum);
        }
        catch (FormatException)
        {
            logger.LogWarning(
                "[{Endpoint}] ReceiveInstallSnapshot rejected: partition={PartitionId} index={Index} carries a "
                + "malformed SnapshotChecksum.",
                localEndpoint, request.PartitionId, request.SnapshotIndex);
            return false;
        }

        if (CryptographicOperations.FixedTimeEquals(actual, expected))
            return true;

        logger.LogWarning(
            "[{Endpoint}] ReceiveInstallSnapshot rejected: partition={PartitionId} index={Index} failed its "
            + "integrity check over {Bytes} staged bytes — the snapshot was corrupted or tampered with in transit.",
            localEndpoint, request.PartitionId, request.SnapshotIndex, session.AccumulatedBytes);

        return false;
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
        // In-install sessions still occupy a live buffer, so they count toward the session cap even though
        // they are no longer in _sessions (and cannot be evicted).
        while (_sessions.Count + _inInstallCount >= maxPendingSessions)
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

        // Total live staged bytes = pending sessions + in-install buffers. In-install buffers cannot be
        // evicted (their install is running), so only pending sessions are eviction candidates; if the
        // in-install pool alone leaves no room, the incoming chunk is rejected (bounded memory).
        while (_totalPendingBytes + _inInstallBytes + incoming > maxPendingBytes)
        {
            SnapshotReceiveSession? victim = OldestEvictableLocked(currentKey, hasExclude: true);
            if (victim is null)
                break;
            RemoveSessionLocked(victim.Key, victim);
        }

        return _totalPendingBytes + _inInstallBytes + incoming <= maxPendingBytes;
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
        session.Hash.Dispose();
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
    /// Total live staged bytes — active sessions plus completed buffers still installing. This is what the
    /// byte cap actually bounds. For test assertions only.
    /// </summary>
    internal long TotalStagedByteCount
    {
        get { lock (_pendingSnapshotsLock) return _totalPendingBytes + _inInstallBytes; }
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
        List<SnapshotReceiveSession> pendingSessions = [];

        lock (_pendingSnapshotsLock)
        {
            foreach (KeyValuePair<SnapshotSessionKey, SnapshotReceiveSession> pending in _sessions)
                pendingSessions.Add(pending.Value);

            _sessions.Clear();
            _totalPendingBytes = 0;
        }

        foreach (SnapshotReceiveSession session in pendingSessions)
        {
            session.Buffer.Dispose();
            session.Hash.Dispose();
        }
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

        /// <summary>
        /// Running SHA-256 over the staged bytes, advanced on every appended chunk and compared
        /// against the sender's digest on the terminal chunk. Hashing incrementally avoids a second
        /// pass over an assembled snapshot that may be hundreds of megabytes.
        /// </summary>
        internal required IncrementalHash Hash { get; init; }

        internal required long CreatedTimestamp { get; init; }
        internal int NextExpectedChunkIndex;
        internal long AccumulatedBytes;
        internal long LastActivityTimestamp;
    }
}
