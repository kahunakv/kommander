
using System.Diagnostics;
using System.Security.Cryptography;
using Kommander.Data;
using Microsoft.Extensions.Logging;

using Kommander.Diagnostics;
using Kommander.Support.Parallelization;

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
/// <para>The caps are accounted in payload bytes, and <see cref="SnapshotReceiveBuffer"/> is what makes
/// that also a statement about physical memory: it stores a session in fixed-size segments, so its
/// allocated capacity exceeds its payload by less than one segment and never doubles. The earlier
/// <c>MemoryStream</c> could hold close to twice the payload, plus a transient copy of it, none of which
/// the byte cap saw. <see cref="TotalStagedCapacityByteCount"/> reports the allocated total.</para>
///
/// <para><b>Superseded retries.</b> A sender opens a new session for every attempt, so a retry used to
/// leave the abandoned attempt's partial buffer staged until the byte cap or the idle TTL reclaimed it —
/// several copies of one partition's snapshot at once while a slow install ran. Opening a session now
/// drops the partition's older pending sessions from the same leader at or below its snapshot index, and
/// any pending session of the partition from a lower leader term: none of them can still complete.</para>
///
/// <para><b>Spilling to disk.</b> With a staging directory configured, the bytes staged in memory across
/// all sessions (pending and installing) are held within a memory budget: a chunk that would take the
/// in-memory total past it moves its session into a temporary file first (see
/// <see cref="SnapshotReceiveBuffer.SpillTo"/>). The byte cap then bounds staging on disk and in memory
/// together, and the memory budget bounds what is resident, so the largest partition that can be seeded is
/// no longer limited by memory. A failed spill or file write fails that session only. Without a staging
/// directory every session stays in memory, as before.</para>
///
/// <para><b>Locking.</b> Chunk bytes are appended — and a session spilled — outside the receiver lock: the
/// lock reserves the bytes and marks the session busy, the append runs, and the lock is re-taken to
/// commit. A session evicted, expired or superseded while busy leaves the map at once (its bytes released
/// from the accounting) and is disposed by the appender when it finishes. One chunk at a time per session:
/// a chunk that arrives while the session's previous chunk is still being appended is refused.</para>
///
/// <para><b>Buffering only.</b> This class does not import or writes the WAL. On the terminal
/// chunk it hands the staged buffer plus session metadata to <c>installOnExecutor</c>, which routes the
/// install through the partition's single-writer executor where term validation, application import, and
/// the durable WAL boundary run serialized against every other partition operation.</para>
///
/// <para><b>The install is a step of its own.</b> The terminal chunk starts the install as a task this
/// class owns (<see cref="RunInstallAsync"/>) and answers a sender that polls
/// (<see cref="SnapshotRequest.InstallPolling"/>) at once with
/// <see cref="SnapshotInstallOutcome.InstallPending"/>; the sender then asks for the outcome with
/// <see cref="SnapshotRequest.StatusQuery"/>. An install takes as long as the application's import does —
/// tens of seconds for a large partition on a busy disk — and a chunk acknowledgement may take
/// <see cref="RaftConfiguration.SnapshotChunkAckTimeout"/>. Answering the terminal chunk only when the
/// install had finished made every slow install read as a rejected last chunk, and the sender answered
/// that with a new export at a newer index while this node was still importing the first one. A sender
/// that does not poll keeps the old answer: its terminal chunk is held until the install completes.</para>
///
/// <para><b>One install per partition.</b> While an install of a partition is queued or running, no other
/// session of that partition is staged: its pending sessions are dropped when the install starts, and a
/// chunk that would open or continue another one is answered with
/// <see cref="SnapshotInstallOutcome.InstallPending"/> naming the running install (or refused, for a
/// sender that does not poll). A second snapshot staged beside the running one could not be installed
/// until the first finished, and by then its sender has usually moved to a newer index; all it did was
/// hold a second copy of the partition in memory next to the import's working set. The record of the
/// last install of each partition is kept after it completes, so a sender whose call was cut off can
/// still learn the outcome by naming the session.</para>
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

    // Bytes staged in memory rather than in a spill file, across pending sessions and in-install buffers
    // alike. Held within stagingMemoryBytes when a staging directory is configured.
    private long _inMemoryBytes;

    // The install of each partition that is queued or running, and after it completes its outcome, until
    // the partition's next install replaces it. At most one per partition is not completed. Guarded by
    // the receiver lock.
    private readonly Dictionary<int, SnapshotInstallRecord> _installs = new();

    // Highest managed-heap size seen at the start or end of an install, or when a sender asked about one
    // that was running. Published as a gauge so a run can report what an install cost without a profiler.
    private long _installPeakHeapBytes;

    /// <summary>Directory for spill files, or null to keep every session in memory.</summary>
    private readonly string? stagingDirectory;

    /// <summary>Budget for <see cref="_inMemoryBytes"/> when <see cref="stagingDirectory"/> is set.</summary>
    private readonly long stagingMemoryBytes;

    /// <summary>File extension of spill files; the startup sweep removes only files carrying it.</summary>
    internal const string StagingFileExtension = ".snapshot-staging";

    private readonly Func<bool> isDisposed;
    private readonly Func<SnapshotInstallRequest, Task<SnapshotResponse>> installOnExecutor;
    private readonly ILogger<IRaft> logger;
    private readonly string localEndpoint;

    private readonly long sessionTtlTicks;
    private readonly int maxPendingSessions;
    private readonly long maxPendingBytes;
    private readonly Func<long> getMonotonicTimestamp;
    private readonly Func<bool> allowLegacySenders;

    /// <summary>
    /// Age, in ms, of a partition's oldest WAL write the local storage engine has not answered, and
    /// the age at which a new snapshot session is refused. A snapshot cannot be installed until the
    /// disk answers, so accepting one during a stall only buffers it here for the stall's length —
    /// the memory that OOM-killed a follower six seconds after its heal (CamusDB run sd8). The
    /// leader defers on the peer's own report first; this is the receiver's guard for a leader that
    /// has not heard it (a fresh leader, a report lost in transit).
    /// </summary>
    private readonly Func<int, double> partitionWalStallAgeMs;
    private readonly Func<double> walStallRefuseThresholdMs;

    /// <summary>Last refusal Warning per partition (monotonic ticks) — one line per 10 s per partition.</summary>
    private readonly Dictionary<int, long> lastStallRefusalWarnTicks = [];

    internal SnapshotReceiver(
        Func<bool> isDisposed,
        Func<SnapshotInstallRequest, Task<SnapshotResponse>> installOnExecutor,
        ILogger<IRaft> logger,
        string localEndpoint,
        long sessionTtlTicks,
        int maxPendingSessions,
        long maxPendingBytes,
        Func<long> getMonotonicTimestamp,
        Func<bool>? allowLegacySenders = null,
        Func<int, double>? partitionWalStallAgeMs = null,
        Func<double>? walStallRefuseThresholdMs = null,
        string? stagingDirectory = null,
        long stagingMemoryBytes = long.MaxValue)
    {
        this.partitionWalStallAgeMs = partitionWalStallAgeMs ?? (static _ => 0);
        this.walStallRefuseThresholdMs = walStallRefuseThresholdMs ?? (static () => 0);
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
        this.stagingMemoryBytes = Math.Max(0, stagingMemoryBytes);

        if (!string.IsNullOrWhiteSpace(stagingDirectory))
        {
            this.stagingDirectory = stagingDirectory;
            PrepareStagingDirectory(stagingDirectory);
        }

        KommanderMetrics.RegisterSnapshotReceiver(this);
    }

    /// <summary>
    /// Creates the staging directory and removes spill files a previous process left behind. They are never
    /// reusable: a sender restarts a transfer from its first chunk, and a spill file is deleted when its
    /// session ends, so any file present at startup belongs to a session that died with that process. The
    /// directory must be private to this node — the sweep would otherwise delete another live node's files.
    /// </summary>
    private void PrepareStagingDirectory(string directory)
    {
        try
        {
            Directory.CreateDirectory(directory);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            throw new RaftException(
                $"[Kommander] SnapshotStagingDirectory '{directory}' could not be created: {ex.Message}");
        }

        foreach (string leftover in Directory.EnumerateFiles(directory, "*" + StagingFileExtension))
        {
            try
            {
                File.Delete(leftover);
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
            {
                logger.LogWarning(
                    "[{Endpoint}] Could not remove leftover snapshot staging file {Path}: {Message}",
                    localEndpoint, leftover, ex.Message);
            }
        }
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
    /// caps or visible to eviction) and starts the install outside the lock. A protocol violation
    /// (metadata change, skipped/out-of-order/negative chunk index, byte-budget overflow) drops the
    /// session and returns failure; an exact duplicate of the immediately-previous chunk is an
    /// idempotent success that is not appended again.
    /// <para>The terminal chunk of a sender that polls is answered as soon as the install is handed to
    /// the executor; one that does not poll is answered when the install completes. A
    /// <see cref="SnapshotRequest.StatusQuery"/> stages nothing and reports the partition's install.
    /// See the class summary for both, and for what a chunk meets while an install is running.</para>
    /// </summary>
    internal async Task<SnapshotResponse> ReceiveInstallSnapshot(
        SnapshotRequest request,
        CancellationToken cancellationToken = default)
    {
        if (isDisposed())
            return new SnapshotResponse(false);

        cancellationToken.ThrowIfCancellationRequested();

        if (request.StatusQuery)
            return AnswerStatusQuery(request);

        SnapshotReceiveBuffer completeBuffer;
        SnapshotReceiveSession completedSession;
        SnapshotInstallRecord installRecord;
        SnapshotInstallRecord? duplicateOf = null;
        SnapshotReceiveSession? session = null;
        SnapshotSessionKey key;
        ReadOnlyMemory<byte> data = request.Data;
        int incoming = data.Length;
        bool spill;

        // ── Reserve, under the lock: validate the chunk against the session, make room for its bytes, and
        // mark the session busy so nothing disposes it while the bytes are appended outside the lock. ──
        lock (_pendingSnapshotsLock)
        {
            if (isDisposed())
                return new SnapshotResponse(false);

            long now = getMonotonicTimestamp();
            ExpireIdleSessionsLocked(now);

            key = new(request.LeaderEndpoint ?? "", request.PartitionId, request.SessionId ?? "");

            if (request.ChunkIndex < 0)
            {
                // Negative index is never valid; drop any session it claims to belong to.
                if (_sessions.TryGetValue(key, out SnapshotReceiveSession? bad))
                    RemoveSessionLocked(key, bad);
                return new SnapshotResponse(false);
            }

            _installs.TryGetValue(request.PartitionId, out SnapshotInstallRecord? partitionInstall);

            if (_sessions.TryGetValue(key, out SnapshotReceiveSession? existing))
            {
                // Handled below, with the session in hand.
            }
            else if (request.IsLast
                     && partitionInstall is not null
                     && partitionInstall.Key.Equals(key)
                     && request.ChunkIndex == partitionInstall.TerminalChunkIndex)
            {
                // The terminal chunk again, for the session whose install it already started: the
                // sender (or its transport) never saw the first answer. The snapshot is staged and
                // the install is running or done, so this is a question about that install, not a
                // chunk of a session that was lost.
                duplicateOf = partitionInstall;
            }
            else if (partitionInstall is { Completed: false })
            {
                // An install of this partition is queued or running. Nothing else of the partition is
                // staged meanwhile: a second snapshot could not be installed until the first has
                // finished, and would only sit beside it in memory. Covers the opener of a new
                // session and a later chunk of one that was dropped when the install started.
                KommanderMetrics.RecordSnapshotReceiveSessionRefusedInstalling(request.PartitionId);

                if (logger.IsEnabled(LogLevel.Debug))
                    logger.LogDebug(
                        "[{Endpoint}] Not staging chunk {Chunk} of a snapshot session for partition {PartitionId} at index {Index} from {Leader}: the install at index {InstallIndex} from {InstallLeader} is still running",
                        localEndpoint, request.ChunkIndex, request.PartitionId, request.SnapshotIndex, request.LeaderEndpoint,
                        partitionInstall.SnapshotIndex, partitionInstall.Key.LeaderEndpoint);

                return request.InstallPolling
                    ? DescribeInstallLocked(partitionInstall)
                    : new SnapshotResponse(false);
            }
            else if (request.ChunkIndex != 0)
            {
                // A fresh session must begin at chunk 0. A non-zero first chunk means we lost the
                // session (skipped opener, or a late chunk after the terminal chunk detached it).
                return new SnapshotResponse(false);
            }

            if (duplicateOf is not null)
            {
                // Answered below, outside the lock: a sender that does not poll waits for the install.
            }
            else if (existing is null)
            {
                // Refuse to OPEN a session while this node's own disk is stalled (see the field
                // summary). Checked at the opener only: a stall that begins mid-transfer keeps the
                // bytes already staged — dropping them would waste the transfer for a hiccup, and
                // the byte cap bounds them regardless. The sender records a chunk rejection and
                // retries on its backoff; the log line here says why.
                double stallMs = partitionWalStallAgeMs(request.PartitionId);
                double refuseAt = walStallRefuseThresholdMs();
                if (refuseAt > 0 && stallMs >= refuseAt)
                {
                    KommanderMetrics.RecordSnapshotInstallRefusedForWalStall(request.PartitionId);

                    if (!lastStallRefusalWarnTicks.TryGetValue(request.PartitionId, out long lastWarn)
                        || now - lastWarn >= TicksForDuration(TimeSpan.FromSeconds(10)))
                    {
                        lastStallRefusalWarnTicks[request.PartitionId] = now;
                        logger.LogWarning(
                            "[{Endpoint}] Refusing to open snapshot session for partition {PartitionId} at index {Index} from {Leader}: the local durable-write stall is {StallMs:F0} ms (threshold {ThresholdMs:F0} ms). The install could not run until the disk answers and the chunks would only be buffered here; the leader retries once the stall clears",
                            localEndpoint, request.PartitionId, request.SnapshotIndex, request.LeaderEndpoint, stallMs, refuseAt);
                    }

                    return new SnapshotResponse(false);
                }

                // Earlier attempts of this partition's transfer that this one replaces go first, so their bytes
                // are released before the caps are applied to the new session.
                SupersedeLocked(key, request);

                EvictForSessionCapacityLocked();

                session = new SnapshotReceiveSession
                {
                    Key = key,
                    LeaderTerm = request.LeaderTerm,
                    LastIncludedTerm = request.LastIncludedTerm,
                    SnapshotIndex = request.SnapshotIndex,
                    Kind = request.Kind,
                    Forced = request.Forced,
                    NextExpectedChunkIndex = 0,
                    Buffer = new SnapshotReceiveBuffer(),
                    Hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256),
                    CreatedTimestamp = now,
                    LastActivityTimestamp = now,
                };
                _sessions[key] = session;
            }
            else
            {
                session = existing;

                // The previous chunk is still being appended: this one cannot be ordered against it.
                if (session.Busy)
                    return new SnapshotResponse(false);

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
                    return new SnapshotResponse(SnapshotInstallOutcome.ChunkAccepted);
                }

                // Anything other than the exact next chunk is a skip/reorder: drop the session.
                if (request.ChunkIndex != session.NextExpectedChunkIndex)
                {
                    RemoveSessionLocked(key, session);
                    return new SnapshotResponse(false);
                }
            }

            spill = false;

            if (session is not null)
            {
                if (!EnsureByteCapacityLocked(key, incoming))
                {
                    // Even after evicting every other session this chunk does not fit: reject and drop.
                    RemoveSessionLocked(key, session);
                    return new SnapshotResponse(false);
                }

                // A chunk that would take the in-memory total past the budget moves its session to disk first.
                // Its bytes already staged stay counted as in memory until the spill has actually released them
                // (at commit), so nothing else is admitted into memory against bytes that are still resident.
                spill = incoming > 0
                        && session.InMemory
                        && stagingDirectory is not null
                        && _inMemoryBytes + incoming > stagingMemoryBytes;

                if (spill)
                {
                    session.InMemory = false;
                }
                else if (session.InMemory)
                {
                    session.InMemoryBytes += incoming;
                    _inMemoryBytes += incoming;
                }

                session.AccumulatedBytes += incoming;
                _totalPendingBytes += incoming;
                session.LastActivityTimestamp = now;
                session.Busy = true;
            }
        }

        if (session is null)
            return await AnswerForInstallAsync(duplicateOf!, request.InstallPolling).ConfigureAwait(false);

        // ── Append, outside the lock: the file I/O of a spill or a spilled session's write must not hold up
        // every other session's chunks. Only this caller touches the session's buffer and hash while busy. ──
        Exception? appendFailure = null;

        try
        {
            if (spill)
            {
                session.Buffer.SpillTo(Path.Combine(stagingDirectory!, $"snapshot-p{request.PartitionId}-{Guid.NewGuid():N}{StagingFileExtension}"));
                KommanderMetrics.RecordSnapshotReceiveSessionSpilled(request.PartitionId);
                logger.LogInformation(
                    "[{Endpoint}] Snapshot session for partition {PartitionId} at index {Index} moved to disk: staging it in memory would exceed the {Budget}-byte staging memory budget",
                    localEndpoint, request.PartitionId, request.SnapshotIndex, stagingMemoryBytes);
            }

            if (incoming > 0)
            {
                session.Buffer.Write(data.Span);
                // Hashed on the same path that appends, so the digest tracks exactly the bytes that were
                // staged: the duplicate-chunk and reject paths above return before reaching here.
                session.Hash.AppendData(data.Span);
            }
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            appendFailure = ex;
        }

        // ── Commit, under the lock. ──
        lock (_pendingSnapshotsLock)
        {
            session.Busy = false;

            // Evicted, expired or superseded while the bytes were appended (or the receiver was disposed):
            // the accounting already released it, and disposal was left to this caller.
            if (session.Removed)
            {
                DisposeSessionResources(session);

                // Dropped because another session's terminal chunk started the partition's install
                // meanwhile: a sender that polls waits for that install instead of failing.
                if (request.InstallPolling
                    && _installs.TryGetValue(request.PartitionId, out SnapshotInstallRecord? started)
                    && !started.Completed)
                    return DescribeInstallLocked(started);

                return new SnapshotResponse(false);
            }

            // A completed spill released the session's segments: its bytes leave the in-memory accounting.
            if (spill && appendFailure is null)
            {
                _inMemoryBytes -= session.InMemoryBytes;
                session.InMemoryBytes = 0;
            }

            if (appendFailure is not null)
            {
                logger.LogWarning(
                    "[{Endpoint}] Snapshot session for partition {PartitionId} at index {Index} dropped: its staged bytes could not be written ({Message}); the sender retries the transfer",
                    localEndpoint, request.PartitionId, request.SnapshotIndex, appendFailure.Message);
                RemoveSessionLocked(key, session);
                return new SnapshotResponse(false);
            }

            session.NextExpectedChunkIndex++;
            session.LastActivityTimestamp = getMonotonicTimestamp();

            // A staged chunk is not an install: the outcome says so, and the sender treats a
            // terminal chunk answered this way as a failed transfer rather than a seeded follower.
            if (!request.IsLast)
                return new SnapshotResponse(SnapshotInstallOutcome.ChunkAccepted);

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
            // Its in-memory bytes, if any, stay counted against the staging memory budget likewise.
            _sessions.Remove(key);
            _totalPendingBytes -= session.AccumulatedBytes;
            _inInstallBytes += session.AccumulatedBytes;
            _inInstallCount++;
            completedSession = session;
            completeBuffer = session.Buffer;
            completeBuffer.Position = 0;

            // The partition's other pending sessions go now: none of them can be installed while
            // this one runs (see the class summary), and their senders learn to wait at their next chunk.
            DropPartitionSessionsLocked(request.PartitionId, completedSession);

            installRecord = new SnapshotInstallRecord
            {
                Key = key,
                SnapshotIndex = completedSession.SnapshotIndex,
                LeaderTerm = completedSession.LeaderTerm,
                TerminalChunkIndex = request.ChunkIndex,
                StagedBytes = completedSession.AccumulatedBytes,
                InMemoryBytes = completedSession.InMemoryBytes,
                StartedTimestamp = getMonotonicTimestamp(),
                Buffer = completeBuffer,
            };

            // Replaces the record of the partition's previous install, which has completed: a sender
            // still waiting for that one is told there is a new install and waits for it instead.
            _installs[request.PartitionId] = installRecord;
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
            Forced = completedSession.Forced,
            Snapshot = completeBuffer,
        };

        // The install runs as a task of its own, owned by its record: it outlives this call when the
        // sender polls, and it must finish (and release the buffer) even if this call is abandoned.
        FireAndForget.Observe(RunInstallAsync(installRecord, completedSession, install), logger, "SnapshotReceiver.RunInstall");

        return await AnswerForInstallAsync(installRecord, request.InstallPolling).ConfigureAwait(false);
    }

    /// <summary>
    /// Runs one install on the partition executor and records its outcome. Never throws: a failed or
    /// faulted install is a <see cref="SnapshotInstallOutcome.Rejected"/> outcome on the record, and the
    /// staged buffer and its accounting are released on every path.
    /// </summary>
    private async Task RunInstallAsync(
        SnapshotInstallRecord record,
        SnapshotReceiveSession completedSession,
        SnapshotInstallRequest install)
    {
        SnapshotInstallOutcome outcome = SnapshotInstallOutcome.Rejected;
        long heapBefore = SampleInstallHeap();

        try
        {
            SnapshotResponse response = await installOnExecutor(install).ConfigureAwait(false);
            outcome = response.Outcome;
        }
        catch (Exception ex)
        {
            logger.LogError(
                "[{Endpoint}] ReceiveInstallSnapshot: install partition={PartitionId} index={Index} failed: {Message}",
                localEndpoint, install.PartitionId, install.SnapshotIndex, ex.Message);
        }
        finally
        {
            long heapAfter = SampleInstallHeap();
            SnapshotReceiveBuffer buffer = record.Buffer!;
            SnapshotResponse answer;
            long elapsedTicks;

            // Release the in-install reservation, then dispose the buffer. The executor read the stream to
            // completion before installOnExecutor returned, so the buffer is safe to release here (see
            // RaftPartition.InstallSnapshotAsync — it uses no-cancellation Ask). Disposal is synchronous:
            // the buffer holds managed segments, or a spill file that is deleted on close.
            lock (_pendingSnapshotsLock)
            {
                _inInstallBytes -= completedSession.AccumulatedBytes;
                _inInstallCount--;

                _inMemoryBytes -= completedSession.InMemoryBytes;
                completedSession.InMemoryBytes = 0;

                // The record stops reading the buffer before it is disposed: progress is frozen at
                // what the importer had consumed.
                record.FinalProgress = buffer.ConsumedBytes;
                record.Buffer = null;
                record.Outcome = outcome;
                record.Completed = true;

                elapsedTicks = getMonotonicTimestamp() - record.StartedTimestamp;
                answer = DescribeInstallLocked(record);
            }

            buffer.Dispose();
            // The completed session was detached from _sessions above, so RemoveSessionLocked never
            // runs for it and this is the only place its hash is released.
            completedSession.Hash.Dispose();

            double elapsedMs = elapsedTicks * 1000.0 / Stopwatch.Frequency;
            KommanderMetrics.RecordSnapshotInstallDuration(install.PartitionId, outcome, elapsedMs);

            if (logger.IsEnabled(LogLevel.Information))
                logger.LogInformation(
                    "[{Endpoint}] Snapshot install for partition {PartitionId} at index {Index} from {Leader} ended {Outcome} after {ElapsedMs:F0} ms: {StagedBytes} staged bytes ({InMemoryBytes} in memory), managed heap {HeapBefore} -> {HeapAfter} bytes (highest seen during any install {PeakHeap})",
                    localEndpoint, install.PartitionId, install.SnapshotIndex, install.LeaderEndpoint, outcome, elapsedMs,
                    record.StagedBytes, record.InMemoryBytes, heapBefore, heapAfter, Volatile.Read(ref _installPeakHeapBytes));

            record.Completion.TrySetResult(answer);
        }
    }

    /// <summary>
    /// The answer for a chunk that belongs to <paramref name="record"/>'s install: a sender that polls
    /// is told where the install stands right now, one that does not is answered when it completes.
    /// </summary>
    private async Task<SnapshotResponse> AnswerForInstallAsync(SnapshotInstallRecord record, bool installPolling)
    {
        if (!installPolling)
            return await record.Completion.Task.ConfigureAwait(false);

        lock (_pendingSnapshotsLock)
            return DescribeInstallLocked(record);
    }

    /// <summary>
    /// Answers a <see cref="SnapshotRequest.StatusQuery"/>. An install of the partition that is queued
    /// or running is reported whichever session it belongs to — that is what the asker has to wait for.
    /// A completed install is reported only to a query that names its session: its outcome is an
    /// answer for the sender that was waiting for it, and not a statement about a transfer that
    /// has not been sent yet (a re-seed at the same index must still be sent).
    /// </summary>
    private SnapshotResponse AnswerStatusQuery(SnapshotRequest request)
    {
        lock (_pendingSnapshotsLock)
        {
            if (isDisposed())
                return new SnapshotResponse(false);

            if (!_installs.TryGetValue(request.PartitionId, out SnapshotInstallRecord? record))
                return new SnapshotResponse(SnapshotInstallOutcome.NoInstall);

            if (!record.Completed)
            {
                SampleInstallHeap();
                return DescribeInstallLocked(record);
            }

            if (!string.IsNullOrEmpty(request.SessionId)
                && string.Equals(record.Key.SessionId, request.SessionId, StringComparison.Ordinal))
                return DescribeInstallLocked(record);

            return new SnapshotResponse(SnapshotInstallOutcome.NoInstall);
        }
    }

    /// <summary>
    /// The response that describes <paramref name="record"/>: <see cref="SnapshotInstallOutcome.InstallPending"/>
    /// while it is queued or running, its outcome once it has completed. Must hold the lock.
    /// </summary>
    private static SnapshotResponse DescribeInstallLocked(SnapshotInstallRecord record) =>
        new(record.Completed ? record.Outcome : SnapshotInstallOutcome.InstallPending)
        {
            InstallSessionId = record.Key.SessionId,
            InstallIndex = record.SnapshotIndex,
            InstallLeaderTerm = record.LeaderTerm,
            InstallLeaderEndpoint = record.Key.LeaderEndpoint,
            InstallProgress = record.Buffer?.ConsumedBytes ?? record.FinalProgress,
        };

    /// <summary>
    /// Reads the managed heap size and raises <see cref="_installPeakHeapBytes"/> to it. Called when an
    /// install starts, when it ends, and whenever a sender asks about one that is running — a sender
    /// that polls asks several times a second, so the high-water mark follows the import closely
    /// without a timer of its own.
    /// </summary>
    private long SampleInstallHeap()
    {
        long heap = GC.GetTotalMemory(forceFullCollection: false);

        long seen = Volatile.Read(ref _installPeakHeapBytes);
        while (heap > seen)
        {
            long previous = Interlocked.CompareExchange(ref _installPeakHeapBytes, heap, seen);
            if (previous == seen)
                break;
            seen = previous;
        }

        return heap;
    }

    /// <summary>
    /// Drops every pending session of <paramref name="partitionId"/> except <paramref name="keep"/>.
    /// Called when an install of the partition starts. Must hold the lock.
    /// </summary>
    private void DropPartitionSessionsLocked(int partitionId, SnapshotReceiveSession keep)
    {
        List<SnapshotReceiveSession>? dropped = null;

        foreach (KeyValuePair<SnapshotSessionKey, SnapshotReceiveSession> pair in _sessions)
        {
            if (pair.Key.PartitionId == partitionId && !ReferenceEquals(pair.Value, keep))
                (dropped ??= []).Add(pair.Value);
        }

        if (dropped is null)
            return;

        foreach (SnapshotReceiveSession session in dropped)
        {
            RemoveSessionLocked(session.Key, session);
            KommanderMetrics.RecordSnapshotReceiveSessionSuperseded(partitionId);

            if (logger.IsEnabled(LogLevel.Debug))
                logger.LogDebug(
                    "[{Endpoint}] Snapshot session {Old} for partition {PartitionId} (index {OldIndex}, {Bytes} bytes staged) dropped: the install at index {Index} from {Leader} started",
                    localEndpoint, session.Key.SessionId, partitionId, session.SnapshotIndex, session.AccumulatedBytes,
                    keep.SnapshotIndex, keep.Key.LeaderEndpoint);
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
            // A busy session is receiving a chunk right now, so it is not idle whatever its timestamp says.
            if (!pair.Value.Busy && now - pair.Value.LastActivityTimestamp > sessionTtlTicks)
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

            // A session mid-append is actively receiving; evicting it would discard work in progress
            // in favour of whatever asked for room, so it is never the victim.
            if (pair.Value.Busy)
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

    /// <summary>
    /// Removes a pending session and releases its bytes from the accounting. Its buffer and hash are disposed
    /// here, unless an append is in progress on them outside the lock: then the session is only marked
    /// removed, and the appender disposes it when it re-takes the lock. Must hold the lock.
    /// </summary>
    private void RemoveSessionLocked(SnapshotSessionKey key, SnapshotReceiveSession session)
    {
        if (_sessions.Remove(key))
        {
            _totalPendingBytes -= session.AccumulatedBytes;
            _inMemoryBytes -= session.InMemoryBytes;
            session.InMemoryBytes = 0;
        }

        if (session.Busy)
        {
            session.Removed = true;
            return;
        }

        DisposeSessionResources(session);
    }

    private static void DisposeSessionResources(SnapshotReceiveSession session)
    {
        session.Buffer.Dispose();
        session.Hash.Dispose();
    }

    /// <summary>
    /// Drops the pending sessions a newly opened session of the same partition makes obsolete: those from the
    /// same leader at or below its snapshot index — the sender abandoned them when it started this attempt —
    /// and those from a lower leader term, whose install the new term's leader has superseded. A sender that
    /// predates leader terms (term 0) supersedes only by the same-leader rule. Must hold the lock.
    /// </summary>
    private void SupersedeLocked(SnapshotSessionKey newKey, SnapshotRequest opener)
    {
        List<SnapshotReceiveSession>? superseded = null;

        foreach (KeyValuePair<SnapshotSessionKey, SnapshotReceiveSession> pair in _sessions)
        {
            SnapshotSessionKey key = pair.Key;
            SnapshotReceiveSession session = pair.Value;

            if (key.PartitionId != newKey.PartitionId || key.Equals(newKey))
                continue;

            bool sameLeaderOlderAttempt =
                string.Equals(key.LeaderEndpoint, newKey.LeaderEndpoint, StringComparison.Ordinal)
                && session.SnapshotIndex <= opener.SnapshotIndex;

            bool lowerLeaderTerm = opener.LeaderTerm > 0 && session.LeaderTerm < opener.LeaderTerm;

            if (sameLeaderOlderAttempt || lowerLeaderTerm)
                (superseded ??= []).Add(session);
        }

        if (superseded is null)
            return;

        foreach (SnapshotReceiveSession session in superseded)
        {
            RemoveSessionLocked(session.Key, session);
            KommanderMetrics.RecordSnapshotReceiveSessionSuperseded(newKey.PartitionId);

            if (logger.IsEnabled(LogLevel.Debug))
                logger.LogDebug(
                    "[{Endpoint}] Snapshot session {Old} for partition {PartitionId} (index {OldIndex}, term {OldTerm}, {Bytes} bytes staged) superseded by a new session at index {Index}, term {Term} from {Leader}",
                    localEndpoint, session.Key.SessionId, newKey.PartitionId, session.SnapshotIndex, session.LeaderTerm,
                    session.AccumulatedBytes, opener.SnapshotIndex, opener.LeaderTerm, newKey.LeaderEndpoint);
        }
    }

    /// <summary>Returns the count of active receive sessions. For test assertions only.</summary>
    internal int PendingSessionCount
    {
        get { lock (_pendingSnapshotsLock) return _sessions.Count; }
    }

    /// <summary>Total buffered snapshot bytes across active sessions. Published as a gauge.</summary>
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
    /// Bytes actually allocated by the pending sessions' buffers, as opposed to the payload bytes the caps
    /// count. Reports only the sessions still in the map; a completed buffer whose install is running is
    /// no longer reachable from here, so read this together with <see cref="TotalStagedByteCount"/>.
    /// Exists so a test can assert the segment overshoot stays bounded rather than take it on faith.
    /// </summary>
    internal long TotalStagedCapacityByteCount
    {
        get
        {
            lock (_pendingSnapshotsLock)
            {
                long capacity = 0;

                foreach (KeyValuePair<SnapshotSessionKey, SnapshotReceiveSession> pair in _sessions)
                    capacity += pair.Value.Buffer.AllocatedByteCount;

                return capacity;
            }
        }
    }

    /// <summary>
    /// Bytes staged in memory rather than in spill files, across pending sessions and buffers still
    /// installing. Published as a gauge.
    /// </summary>
    internal long InMemoryStagedByteCount
    {
        get { lock (_pendingSnapshotsLock) return _inMemoryBytes; }
    }

    /// <summary>
    /// Staged bytes of buffers whose install is queued or running. Published as a gauge.
    /// </summary>
    internal long InstallingByteCount
    {
        get { lock (_pendingSnapshotsLock) return _inInstallBytes; }
    }

    /// <summary>Installs queued or running across all partitions. For test assertions only.</summary>
    internal int ActiveInstallCount
    {
        get { lock (_pendingSnapshotsLock) return _inInstallCount; }
    }

    /// <summary>
    /// Highest managed-heap size sampled while an install was starting, running or ending on this
    /// node (see <see cref="SampleInstallHeap"/>); 0 before the first install. Published as a gauge.
    /// </summary>
    internal long InstallPeakHeapBytes => Volatile.Read(ref _installPeakHeapBytes);

    /// <summary>Pending sessions whose bytes have moved to a spill file. For test assertions only.</summary>
    internal int SpilledSessionCount
    {
        get
        {
            lock (_pendingSnapshotsLock)
            {
                int spilled = 0;

                foreach (KeyValuePair<SnapshotSessionKey, SnapshotReceiveSession> pair in _sessions)
                {
                    if (!pair.Value.InMemory)
                        spilled++;
                }

                return spilled;
            }
        }
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
            {
                SnapshotReceiveSession session = pending.Value;

                _inMemoryBytes -= session.InMemoryBytes;
                session.InMemoryBytes = 0;

                // An append in progress disposes its own session when it finishes (see RemoveSessionLocked).
                if (session.Busy)
                    session.Removed = true;
                else
                    pendingSessions.Add(session);
            }

            _sessions.Clear();
            _totalPendingBytes = 0;
        }

        foreach (SnapshotReceiveSession session in pendingSessions)
            DisposeSessionResources(session);
    }

    /// <summary>
    /// One install of a partition: queued or running until <see cref="Completed"/>, then the record of
    /// how it ended. Mutable fields are only touched under the receiver lock.
    /// </summary>
    private sealed class SnapshotInstallRecord
    {
        internal required SnapshotSessionKey Key { get; init; }
        internal required long SnapshotIndex { get; init; }
        internal required long LeaderTerm { get; init; }

        /// <summary>Index of the session's terminal chunk: a repeat of exactly that chunk asks about this install.</summary>
        internal required int TerminalChunkIndex { get; init; }

        internal required long StagedBytes { get; init; }

        /// <summary>The part of <see cref="StagedBytes"/> held in memory rather than in a spill file.</summary>
        internal required long InMemoryBytes { get; init; }

        internal required long StartedTimestamp { get; init; }

        /// <summary>The staged snapshot while the install reads it; null once it has been released.</summary>
        internal SnapshotReceiveBuffer? Buffer;

        internal bool Completed;

        /// <summary>How the install ended. Meaningful once <see cref="Completed"/>.</summary>
        internal SnapshotInstallOutcome Outcome;

        /// <summary>Bytes the importer had consumed when the install ended.</summary>
        internal long FinalProgress;

        /// <summary>
        /// Completes with the install's answer. Continuations run asynchronously: the install completes
        /// on the path that released the partition executor, and a waiting transport call must not
        /// resume inline there.
        /// </summary>
        internal TaskCompletionSource<SnapshotResponse> Completion { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    /// <summary>
    /// Mutable state for one in-progress snapshot-receive session. Its fields are only touched under the
    /// receiver lock; its buffer and hash are also written outside it, by the one caller that marked the
    /// session <see cref="Busy"/>. The metadata fields are captured from the first chunk and treated as
    /// immutable.
    /// </summary>
    private sealed class SnapshotReceiveSession
    {
        internal required SnapshotSessionKey Key { get; init; }
        internal required long LeaderTerm { get; init; }
        internal required long LastIncludedTerm { get; init; }
        internal required long SnapshotIndex { get; init; }
        internal required SnapshotKind Kind { get; init; }
        internal bool Forced { get; init; }
        internal required SnapshotReceiveBuffer Buffer { get; init; }

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

        /// <summary>Whether this session stages in memory; false once it has been chosen to spill.</summary>
        internal bool InMemory = true;

        /// <summary>This session's bytes counted in the receiver's in-memory total — resident until a spill
        /// completes, then zero.</summary>
        internal long InMemoryBytes;

        /// <summary>A chunk is being appended outside the lock; the buffer and hash belong to that caller.</summary>
        internal bool Busy;

        /// <summary>Removed from the receiver while busy; the appender disposes it when it finishes.</summary>
        internal bool Removed;
    }
}
