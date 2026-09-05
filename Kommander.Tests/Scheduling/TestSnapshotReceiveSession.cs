
using System.Security.Cryptography;
using Kommander;
using Kommander.Data;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.Scheduling;

/// <summary>
/// Direct unit tests for <see cref="SnapshotReceiver"/>'s session-record model and bounded buffering.
/// These construct the receiver in isolation with a controllable monotonic clock, small TTL/count/byte
/// limits, and a fake install callback (standing in for the partition executor's install path) so
/// expiry, eviction, and the chunk protocol are deterministic. The receiver itself no longer imports or
/// touches the WAL (the executor owns the install); on the terminal chunk it hands the staged
/// buffer + metadata to the callback.
/// </summary>
public class TestSnapshotReceiveSession
{
    // ── helpers ────────────────────────────────────────────────────────────────

    private sealed class ClockBox
    {
        public long Value;
        public ClockBox(long start) => Value = start;
        public long Read() => Value;
    }

    /// <summary>
    /// Stands in for the executor install path: records every install request and the bytes it carried,
    /// and returns a configurable success/failure result.
    /// </summary>
    private sealed class CapturingInstaller
    {
        public int InstallCallCount { get; private set; }
        public byte[] ReceivedBytes { get; private set; } = [];
        public SnapshotInstallRequest? Last { get; private set; }
        public bool Result { get; set; } = true;

        public async Task<SnapshotResponse> Install(SnapshotInstallRequest request)
        {
            InstallCallCount++;
            Last = request;
            using MemoryStream ms = new();
            await request.Snapshot.CopyToAsync(ms);
            ReceivedBytes = ms.ToArray();
            return new SnapshotResponse(Result);
        }
    }

    private static SnapshotReceiver NewReceiver(
        Func<SnapshotInstallRequest, Task<SnapshotResponse>> installOnExecutor,
        Func<long> clock,
        long ttlTicks = 1_000_000,
        int maxSessions = 8,
        long maxBytes = 1_000_000,
        bool allowLegacySenders = false) =>
        new(
            isDisposed: () => false,
            installOnExecutor: installOnExecutor,
            logger: NullLogger<IRaft>.Instance,
            localEndpoint: "test:1",
            sessionTtlTicks: ttlTicks,
            maxPendingSessions: maxSessions,
            maxPendingBytes: maxBytes,
            getMonotonicTimestamp: clock,
            allowLegacySenders: () => allowLegacySenders);

    /// <summary>
    /// Builds one chunk as a sender would, including the integrity digest on the terminal chunk.
    /// </summary>
    /// <param name="wholeSnapshot">
    /// The bytes the receiver is expected to have staged across the whole session, used for the
    /// terminal chunk's checksum. Defaults to <paramref name="data"/>, which is correct for a
    /// single-chunk session; multi-chunk sessions must pass the concatenation.
    /// </param>
    /// <param name="checksumOverride">
    /// Replaces the computed digest verbatim — for the legacy-sender (empty) and malformed-value
    /// cases, which cannot be expressed by choosing different bytes.
    /// </param>
    private static SnapshotRequest Chunk(
        string session, int chunkIndex, bool isLast, byte[] data,
        string leader = "leader:1", int partitionId = 1, long snapshotIndex = 100,
        long leaderTerm = 3, long lastIncludedTerm = 2, byte[]? wholeSnapshot = null,
        string? checksumOverride = null) =>
        new()
        {
            SessionId = session,
            PartitionId = partitionId,
            SnapshotIndex = snapshotIndex,
            FollowerEndpoint = "test:1",
            LeaderEndpoint = leader,
            LeaderTerm = leaderTerm,
            LastIncludedTerm = lastIncludedTerm,
            ChunkIndex = chunkIndex,
            IsLast = isLast,
            Data = data,
            SnapshotChecksum = checksumOverride ?? (isLast ? Checksum(wholeSnapshot ?? data) : ""),
        };

    /// <summary>Digest in the wire encoding the receiver expects: uppercase hex SHA-256.</summary>
    private static string Checksum(byte[] snapshot) => Convert.ToHexString(SHA256.HashData(snapshot));

    // ── happy path ───────────────────────────────────────────────────────────

    [Fact]
    public async Task InOrderChunks_InstallsOnceWithConcatenatedBytesAndMetadata()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        CapturingInstaller installer = new();
        SnapshotReceiver r = NewReceiver(installer.Install, () => 1000);

        // All chunks of a session share identical metadata (leader:1, snapshotIndex 100, terms 3/2).
        Assert.True((await r.ReceiveInstallSnapshot(Chunk("s", 0, false, [1, 2]), ct)).Success);
        Assert.True((await r.ReceiveInstallSnapshot(Chunk("s", 1, false, [3]), ct)).Success);
        Assert.Equal(0, installer.InstallCallCount);

        Assert.True((await r.ReceiveInstallSnapshot(Chunk("s", 2, true, [4], wholeSnapshot: [1, 2, 3, 4]), ct)).Success);
        Assert.Equal(1, installer.InstallCallCount);
        Assert.Equal([1, 2, 3, 4], installer.ReceivedBytes);

        // Session metadata (captured from the first chunk) is forwarded verbatim to the install path.
        Assert.NotNull(installer.Last);
        Assert.Equal(1, installer.Last!.PartitionId);
        Assert.Equal(100, installer.Last.SnapshotIndex);
        Assert.Equal(3, installer.Last.LeaderTerm);
        Assert.Equal(2, installer.Last.LastIncludedTerm);
        Assert.Equal("leader:1", installer.Last.LeaderEndpoint);

        Assert.Equal(0, r.PendingSessionCount);
        Assert.Equal(0, r.PendingByteCount);
    }

    /// <summary>
    /// The byte caps are accounted in payload bytes, which is only a statement about physical memory
    /// because the receive buffer stores a session in fixed-size segments. This pins that relationship:
    /// allocated capacity must cover the payload and overshoot it by less than one segment, no matter how
    /// many chunks the payload arrived in. A doubling buffer would fail the upper bound.
    /// </summary>
    [Fact]
    public async Task StagedCapacity_TracksThePayloadWithinOneSegment()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        CapturingInstaller installer = new();
        SnapshotReceiver r = NewReceiver(installer.Install, () => 1000, maxBytes: 64 * 1024 * 1024);

        byte[] chunk = new byte[96 * 1024];
        for (int i = 0; i < chunk.Length; i++)
            chunk[i] = (byte)(i & 0xFF);

        for (int index = 0; index < 30; index++)
            Assert.True((await r.ReceiveInstallSnapshot(Chunk("cap", index, false, chunk), ct)).Success);

        long payload = r.PendingByteCount;
        long capacity = r.TotalStagedCapacityByteCount;

        Assert.Equal(30L * chunk.Length, payload);
        Assert.True(capacity >= payload, $"capacity {capacity} must cover the payload {payload}");
        Assert.True(
            capacity - payload < SnapshotReceiveBuffer.SegmentSize,
            $"capacity {capacity} overshoots the payload {payload} by a whole segment");
    }

    [Fact]
    public async Task InstallFailure_IsReportedAndSessionDetached()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        CapturingInstaller installer = new() { Result = false };
        SnapshotReceiver r = NewReceiver(installer.Install, () => 1000);

        Assert.False((await r.ReceiveInstallSnapshot(Chunk("s", 0, true, [1]), ct)).Success);
        Assert.Equal(1, installer.InstallCallCount);
        Assert.Equal(0, r.PendingSessionCount);
    }

    // ── chunk-protocol edge cases ──────────────────────────────────────────────

    [Fact]
    public async Task DuplicateOfPreviousChunk_IsIdempotentAndNotReAppended()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        CapturingInstaller installer = new();
        SnapshotReceiver r = NewReceiver(installer.Install, () => 1000);

        Assert.True((await r.ReceiveInstallSnapshot(Chunk("s", 0, false, [1]), ct)).Success);
        Assert.True((await r.ReceiveInstallSnapshot(Chunk("s", 1, false, [2]), ct)).Success);
        // Exact duplicate of the immediately-previous chunk: idempotent success, not appended again.
        Assert.True((await r.ReceiveInstallSnapshot(Chunk("s", 1, false, [2]), ct)).Success);
        Assert.True((await r.ReceiveInstallSnapshot(Chunk("s", 2, true, [3], wholeSnapshot: [1, 2, 3]), ct)).Success);

        Assert.Equal([1, 2, 3], installer.ReceivedBytes);
    }

    [Fact]
    public async Task SkippedChunkIndex_RejectedAndSessionDropped()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        CapturingInstaller installer = new();
        SnapshotReceiver r = NewReceiver(installer.Install, () => 1000);

        Assert.True((await r.ReceiveInstallSnapshot(Chunk("s", 0, false, [1]), ct)).Success);
        // Skip chunk 1 → out of order.
        Assert.False((await r.ReceiveInstallSnapshot(Chunk("s", 2, false, [9]), ct)).Success);
        Assert.Equal(0, installer.InstallCallCount);
        Assert.Equal(0, r.PendingSessionCount);
    }

    [Fact]
    public async Task ReorderedOlderChunk_RejectedAndSessionDropped()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        CapturingInstaller installer = new();
        SnapshotReceiver r = NewReceiver(installer.Install, () => 1000);

        Assert.True((await r.ReceiveInstallSnapshot(Chunk("s", 0, false, [1]), ct)).Success);
        Assert.True((await r.ReceiveInstallSnapshot(Chunk("s", 1, false, [2]), ct)).Success);
        // Chunk 0 again — not the immediately-previous (1) and not the next expected (2): reorder.
        Assert.False((await r.ReceiveInstallSnapshot(Chunk("s", 0, false, [1]), ct)).Success);
        Assert.Equal(0, r.PendingSessionCount);
    }

    [Fact]
    public async Task MismatchedMetadata_RejectedAndSessionDropped()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        CapturingInstaller installer = new();
        SnapshotReceiver r = NewReceiver(installer.Install, () => 1000);

        Assert.True((await r.ReceiveInstallSnapshot(Chunk("s", 0, false, [1], leaderTerm: 5), ct)).Success);
        // Same session id but a different LeaderTerm — a later chunk must not change metadata.
        Assert.False((await r.ReceiveInstallSnapshot(Chunk("s", 1, false, [2], leaderTerm: 6), ct)).Success);
        Assert.Equal(0, installer.InstallCallCount);
        Assert.Equal(0, r.PendingSessionCount);
    }

    // ── integrity ──────────────────────────────────────────────────────────────

    /// <summary>
    /// The H6 core case: bytes that do not match the sender's digest must never reach the install
    /// path. Everything else the receiver checks is structural — term, fence, chunk order — and a
    /// tampered or corrupted payload satisfies all of it.
    /// </summary>
    [Fact]
    public async Task TamperedPayload_RejectedBeforeInstall()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        CapturingInstaller installer = new();
        SnapshotReceiver r = NewReceiver(installer.Install, () => 1000);

        // The digest claims [1, 2, 3]; the bytes actually staged are [1, 2, 99].
        SnapshotRequest tampered = Chunk("s", 0, true, [1, 2, 99], wholeSnapshot: [1, 2, 3]);

        Assert.False((await r.ReceiveInstallSnapshot(tampered, ct)).Success);
        Assert.Equal(0, installer.InstallCallCount);
        Assert.Equal(0, r.PendingSessionCount);
        Assert.Equal(0, r.PendingByteCount);
    }

    /// <summary>
    /// Tampering with an earlier chunk is caught too — the digest covers the whole session, not just
    /// the terminal chunk, so a mid-transfer rewrite cannot slip through by leaving the last chunk
    /// intact.
    /// </summary>
    [Fact]
    public async Task TamperedEarlierChunk_RejectedAtTerminalChunk()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        CapturingInstaller installer = new();
        SnapshotReceiver r = NewReceiver(installer.Install, () => 1000);

        // Chunk 0 was rewritten in flight: [9] instead of the [1] the sender hashed.
        Assert.True((await r.ReceiveInstallSnapshot(Chunk("s", 0, false, [9]), ct)).Success);

        SnapshotRequest terminal = Chunk("s", 1, true, [2], wholeSnapshot: [1, 2]);

        Assert.False((await r.ReceiveInstallSnapshot(terminal, ct)).Success);
        Assert.Equal(0, installer.InstallCallCount);
        Assert.Equal(0, r.PendingSessionCount);
    }

    /// <summary>
    /// A truncated transfer that still satisfies the chunk-index rules — the sender's last chunk was
    /// lost and an earlier one was marked terminal — is caught by the digest. No adversary required;
    /// this is the silent-corruption case.
    /// </summary>
    [Fact]
    public async Task TruncatedTransfer_RejectedByDigest()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        CapturingInstaller installer = new();
        SnapshotReceiver r = NewReceiver(installer.Install, () => 1000);

        // Digest is over [1, 2, 3] but only [1, 2] ever arrives, with chunk 1 flagged terminal.
        Assert.True((await r.ReceiveInstallSnapshot(Chunk("s", 0, false, [1]), ct)).Success);
        Assert.False((await r.ReceiveInstallSnapshot(
            Chunk("s", 1, true, [2], wholeSnapshot: [1, 2, 3]), ct)).Success);

        Assert.Equal(0, installer.InstallCallCount);
    }

    /// <summary>
    /// A sender that predates the checksum field is refused by default, matching how the other
    /// post-hoc session fields are handled. Accepting unverified snapshots by default would leave
    /// the check with no effect on the deployments that never touch the compatibility switch.
    /// </summary>
    [Fact]
    public async Task MissingChecksum_RejectedWhenLegacySendersDisallowed()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        CapturingInstaller installer = new();
        SnapshotReceiver r = NewReceiver(installer.Install, () => 1000);

        SnapshotRequest legacy = Chunk("s", 0, true, [1, 2, 3], checksumOverride: "");

        Assert.False((await r.ReceiveInstallSnapshot(legacy, ct)).Success);
        Assert.Equal(0, installer.InstallCallCount);
    }

    /// <summary>
    /// …and accepted when the operator has opted into the compatibility window, so a mixed-version
    /// cluster has a way through.
    /// </summary>
    [Fact]
    public async Task MissingChecksum_AcceptedWhenLegacySendersAllowed()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        CapturingInstaller installer = new();
        SnapshotReceiver r = NewReceiver(installer.Install, () => 1000, allowLegacySenders: true);

        SnapshotRequest legacy = Chunk("s", 0, true, [1, 2, 3], checksumOverride: "");

        Assert.True((await r.ReceiveInstallSnapshot(legacy, ct)).Success);
        Assert.Equal(1, installer.InstallCallCount);
        Assert.Equal([1, 2, 3], installer.ReceivedBytes);
    }

    /// <summary>
    /// The legacy switch is an escape hatch for a <i>missing</i> digest, not a licence to ignore one
    /// that is present and wrong.
    /// </summary>
    [Fact]
    public async Task TamperedPayload_StillRejectedWhenLegacySendersAllowed()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        CapturingInstaller installer = new();
        SnapshotReceiver r = NewReceiver(installer.Install, () => 1000, allowLegacySenders: true);

        SnapshotRequest tampered = Chunk("s", 0, true, [1, 2, 99], wholeSnapshot: [1, 2, 3]);

        Assert.False((await r.ReceiveInstallSnapshot(tampered, ct)).Success);
        Assert.Equal(0, installer.InstallCallCount);
    }

    /// <summary>
    /// A malformed digest is rejected outright rather than being compared and reported as a content
    /// mismatch, so an operator reading the log can tell a wire-format problem from a corrupt payload.
    /// </summary>
    [Theory]
    [InlineData("nothexatall")]
    [InlineData("ABC")]              // odd length
    [InlineData("AABBCCDD")]         // well-formed hex, wrong length for SHA-256
    public async Task MalformedChecksum_Rejected(string checksum)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        CapturingInstaller installer = new();
        SnapshotReceiver r = NewReceiver(installer.Install, () => 1000);

        SnapshotRequest bad = Chunk("s", 0, true, [1, 2, 3], checksumOverride: checksum);

        Assert.False((await r.ReceiveInstallSnapshot(bad, ct)).Success);
        Assert.Equal(0, installer.InstallCallCount);
    }

    /// <summary>
    /// Digest comparison is case-insensitive in effect: it decodes the hex rather than comparing the
    /// strings, so a peer that emits lowercase is not rejected as corrupt.
    /// </summary>
    [Fact]
    public async Task LowercaseChecksum_Accepted()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        CapturingInstaller installer = new();
        SnapshotReceiver r = NewReceiver(installer.Install, () => 1000);

        SnapshotRequest lowercase = Chunk(
            "s", 0, true, [1, 2, 3], checksumOverride: Checksum([1, 2, 3]).ToLowerInvariant());

        Assert.True((await r.ReceiveInstallSnapshot(lowercase, ct)).Success);
        Assert.Equal(1, installer.InstallCallCount);
    }

    /// <summary>
    /// The duplicate-chunk fast path must not advance the running digest: it does not append the
    /// bytes either, and a hash that counted them would reject every retried transfer.
    /// </summary>
    [Fact]
    public async Task DuplicateChunk_DoesNotCorruptTheDigest()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        CapturingInstaller installer = new();
        SnapshotReceiver r = NewReceiver(installer.Install, () => 1000);

        Assert.True((await r.ReceiveInstallSnapshot(Chunk("s", 0, false, [1]), ct)).Success);
        Assert.True((await r.ReceiveInstallSnapshot(Chunk("s", 1, false, [2]), ct)).Success);
        Assert.True((await r.ReceiveInstallSnapshot(Chunk("s", 1, false, [2]), ct)).Success);

        Assert.True((await r.ReceiveInstallSnapshot(
            Chunk("s", 2, true, [3], wholeSnapshot: [1, 2, 3]), ct)).Success);

        Assert.Equal([1, 2, 3], installer.ReceivedBytes);
    }

    [Fact]
    public async Task PostTerminalChunk_Rejected()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        CapturingInstaller installer = new();
        SnapshotReceiver r = NewReceiver(installer.Install, () => 1000);

        Assert.True((await r.ReceiveInstallSnapshot(Chunk("s", 0, true, [1]), ct)).Success);
        Assert.Equal(1, installer.InstallCallCount);
        // A chunk arriving after the terminal chunk has no live session; a non-zero opener is rejected.
        Assert.False((await r.ReceiveInstallSnapshot(Chunk("s", 1, true, [2]), ct)).Success);
        Assert.Equal(1, installer.InstallCallCount);
        Assert.Equal(0, r.PendingSessionCount);
    }

    [Fact]
    public async Task NegativeChunkIndex_Rejected()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        CapturingInstaller installer = new();
        SnapshotReceiver r = NewReceiver(installer.Install, () => 1000);

        Assert.False((await r.ReceiveInstallSnapshot(Chunk("s", -1, false, [1]), ct)).Success);
        Assert.Equal(0, r.PendingSessionCount);
    }

    [Fact]
    public async Task NonZeroFirstChunk_Rejected()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        CapturingInstaller installer = new();
        SnapshotReceiver r = NewReceiver(installer.Install, () => 1000);

        Assert.False((await r.ReceiveInstallSnapshot(Chunk("s", 3, false, [1]), ct)).Success);
        Assert.Equal(0, r.PendingSessionCount);
    }

    // ── TTL expiry ─────────────────────────────────────────────────────────────

    [Fact]
    public async Task IdleSession_ExpiresAfterTtlOnSweep()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        ClockBox clock = new(1000);
        SnapshotReceiver r = NewReceiver(new CapturingInstaller().Install, clock.Read, ttlTicks: 500);

        Assert.True((await r.ReceiveInstallSnapshot(Chunk("s", 0, false, [1, 2, 3]), ct)).Success);
        Assert.Equal(1, r.PendingSessionCount);
        Assert.Equal(3, r.PendingByteCount);

        // Still within TTL: sweep keeps it.
        clock.Value = 1000 + 500;
        r.SweepForTesting();
        Assert.Equal(1, r.PendingSessionCount);

        // Past TTL: sweep discards it and releases its bytes.
        clock.Value = 1000 + 501;
        r.SweepForTesting();
        Assert.Equal(0, r.PendingSessionCount);
        Assert.Equal(0, r.PendingByteCount);
    }

    // ── capacity eviction ──────────────────────────────────────────────────────

    [Fact]
    public async Task SessionCountCap_EvictsOldestDeterministically()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        ClockBox clock = new(1);
        SnapshotReceiver r = NewReceiver(new CapturingInstaller().Install, clock.Read, maxSessions: 2);

        clock.Value = 1;
        Assert.True((await r.ReceiveInstallSnapshot(Chunk("A", 0, false, [1], leader: "a"), ct)).Success);
        clock.Value = 2;
        Assert.True((await r.ReceiveInstallSnapshot(Chunk("B", 0, false, [1], leader: "b"), ct)).Success);
        Assert.Equal(2, r.PendingSessionCount);

        // Third session evicts the oldest (A, last-activity=1).
        clock.Value = 3;
        Assert.True((await r.ReceiveInstallSnapshot(Chunk("C", 0, false, [1], leader: "c"), ct)).Success);
        Assert.Equal(2, r.PendingSessionCount);

        // A is gone: continuing it (chunk 1) has no session and is rejected.
        Assert.False((await r.ReceiveInstallSnapshot(Chunk("A", 1, false, [2], leader: "a"), ct)).Success);
        // B survived: continuing it succeeds.
        Assert.True((await r.ReceiveInstallSnapshot(Chunk("B", 1, false, [2], leader: "b"), ct)).Success);
    }

    [Fact]
    public async Task ByteCap_EvictsOldestSessionToMakeRoom()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        ClockBox clock = new(1);
        SnapshotReceiver r = NewReceiver(new CapturingInstaller().Install, clock.Read, maxBytes: 10);

        clock.Value = 1;
        Assert.True((await r.ReceiveInstallSnapshot(Chunk("A", 0, false, new byte[6], leader: "a"), ct)).Success);
        Assert.Equal(6, r.PendingByteCount);

        // B's 5 bytes would make 11 > 10, so the oldest other session (A) is evicted first.
        clock.Value = 2;
        Assert.True((await r.ReceiveInstallSnapshot(Chunk("B", 0, false, new byte[5], leader: "b"), ct)).Success);
        Assert.Equal(1, r.PendingSessionCount);
        Assert.Equal(5, r.PendingByteCount);
    }

    [Fact]
    public async Task SingleChunkLargerThanBudget_Rejected()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        SnapshotReceiver r = NewReceiver(new CapturingInstaller().Install, () => 1, maxBytes: 4);

        Assert.False((await r.ReceiveInstallSnapshot(Chunk("A", 0, false, new byte[5], leader: "a"), ct)).Success);
        Assert.Equal(0, r.PendingSessionCount);
        Assert.Equal(0, r.PendingByteCount);
    }

    [Fact]
    public async Task AbandonedSessions_StayWithinCountAndByteLimits()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        ClockBox clock = new(0);
        SnapshotReceiver r = NewReceiver(new CapturingInstaller().Install, clock.Read,
            ttlTicks: 100_000, maxSessions: 3, maxBytes: 100);

        // Open ten sessions that never finish, each buffering 20 bytes.
        for (int i = 0; i < 10; i++)
        {
            clock.Value = i + 1;
            SnapshotResponse resp = await r.ReceiveInstallSnapshot(
                Chunk($"s{i}", 0, false, new byte[20], leader: $"L{i}"), ct);
            Assert.True(resp.Success);
        }

        Assert.True(r.PendingSessionCount <= 3, $"count {r.PendingSessionCount} exceeds cap");
        Assert.True(r.PendingByteCount <= 100, $"bytes {r.PendingByteCount} exceeds cap");

        // After the TTL elapses, an explicit sweep clears everything.
        clock.Value = 1_000_000;
        r.SweepForTesting();
        Assert.Equal(0, r.PendingSessionCount);
        Assert.Equal(0, r.PendingByteCount);
    }

    // ── disposal ───────────────────────────────────────────────────────────────

    [Fact]
    public async Task DisposePendingSnapshots_ClearsSessionsAndResetsBytes()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        SnapshotReceiver r = NewReceiver(new CapturingInstaller().Install, () => 1000);

        Assert.True((await r.ReceiveInstallSnapshot(Chunk("A", 0, false, [1, 2], leader: "a"), ct)).Success);
        Assert.True((await r.ReceiveInstallSnapshot(Chunk("B", 0, false, [3], leader: "b"), ct)).Success);
        Assert.Equal(2, r.PendingSessionCount);
        Assert.Equal(3, r.PendingByteCount);

        r.DisposePendingSnapshots();

        Assert.Equal(0, r.PendingSessionCount);
        Assert.Equal(0, r.PendingByteCount);
    }

    // ── keying ─────────────────────────────────────────────────────────────────

    [Fact]
    public async Task SameSessionIdDifferentLeaders_AreDistinctSessions()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        SnapshotReceiver r = NewReceiver(new CapturingInstaller().Install, () => 1000);

        // Identical session id "dup" but different leaders → two independent sessions, both at chunk 0.
        Assert.True((await r.ReceiveInstallSnapshot(Chunk("dup", 0, false, [1], leader: "a"), ct)).Success);
        Assert.True((await r.ReceiveInstallSnapshot(Chunk("dup", 0, false, [2], leader: "b"), ct)).Success);
        Assert.Equal(2, r.PendingSessionCount);
    }

    // ── bounded buffering across a blocked install ───────────────────────────────

    [Fact]
    public async Task CompletedBufferStillCountsAgainstCap_WhileItsInstallBlocks()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        // The first install blocks (as it would behind a busy single partition executor); later installs pass.
        TaskCompletionSource firstInstallGate = new(TaskCreationOptions.RunContinuationsAsynchronously);
        int installCount = 0;
        async Task<SnapshotResponse> Installer(SnapshotInstallRequest _)
        {
            if (Interlocked.Increment(ref installCount) == 1)
                await firstInstallGate.Task;
            return new SnapshotResponse(true);
        }

        // Cap of 10 bytes; each session carries 6.
        SnapshotReceiver r = NewReceiver(Installer, () => 1000, maxSessions: 8, maxBytes: 10);

        // Session A: a single terminal chunk of 6 bytes. Its install blocks, so the completed 6-byte buffer
        // stays live — and must remain in the accounting even though it is no longer a pending session.
        Task<SnapshotResponse> aTask = r.ReceiveInstallSnapshot(
            Chunk("sA", 0, isLast: true, [1, 2, 3, 4, 5, 6], leader: "A:1"), ct);

        await WaitUntil(() => r.TotalStagedByteCount == 6, ct);
        Assert.Equal(0, r.PendingByteCount);      // detached from the pending pool…
        Assert.Equal(6, r.TotalStagedByteCount);  // …but still counted as live staged bytes.

        // Session B (6 bytes) would push live staged bytes to 12 > cap(10). Because the in-install buffer
        // still counts, B is rejected — the completed buffer did not silently escape the limit.
        SnapshotResponse b = await r.ReceiveInstallSnapshot(
            Chunk("sB", 0, isLast: true, [7, 8, 9, 10, 11, 12], leader: "B:1"), ct);
        Assert.False(b.Success);
        Assert.True(r.TotalStagedByteCount <= 10,
            $"live staged bytes ({r.TotalStagedByteCount}) must stay within the cap while an install blocks");

        // Release the first install; A completes and its reservation is freed.
        firstInstallGate.SetResult();
        Assert.True((await aTask).Success);
        await WaitUntil(() => r.TotalStagedByteCount == 0, ct);
    }

    private static async Task WaitUntil(Func<bool> condition, CancellationToken ct, int timeoutMs = 5000)
    {
        for (int elapsed = 0; elapsed < timeoutMs; elapsed += 10)
        {
            if (condition())
                return;
            await Task.Delay(10, ct);
        }
        throw new TimeoutException("condition not met");
    }
}
