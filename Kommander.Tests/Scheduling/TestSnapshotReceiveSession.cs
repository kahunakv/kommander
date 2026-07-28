
using Kommander;
using Kommander.Data;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.Scheduling;

/// <summary>
/// Direct unit tests for <see cref="SnapshotReceiver"/>'s session-record model and bounded buffering.
/// These construct the receiver in isolation with a controllable monotonic clock, small TTL/count/byte
/// limits, and a fake install callback (standing in for the partition executor's install path) so
/// expiry, eviction, and the chunk protocol are deterministic. The receiver itself no longer imports or
/// touches the WAL (increment B moved that to the executor); on the terminal chunk it hands the staged
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
        long maxBytes = 1_000_000) =>
        new(
            isDisposed: () => false,
            installOnExecutor: installOnExecutor,
            logger: NullLogger<IRaft>.Instance,
            localEndpoint: "test:1",
            sessionTtlTicks: ttlTicks,
            maxPendingSessions: maxSessions,
            maxPendingBytes: maxBytes,
            getMonotonicTimestamp: clock);

    private static SnapshotRequest Chunk(
        string session, int chunkIndex, bool isLast, byte[] data,
        string leader = "leader:1", int partitionId = 1, long snapshotIndex = 100,
        long leaderTerm = 3, long lastIncludedTerm = 2) =>
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
        };

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

        Assert.True((await r.ReceiveInstallSnapshot(Chunk("s", 2, true, [4]), ct)).Success);
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
        Assert.True((await r.ReceiveInstallSnapshot(Chunk("s", 2, true, [3]), ct)).Success);

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
}
