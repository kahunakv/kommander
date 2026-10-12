using System.Security.Cryptography;

using Kommander;
using Kommander.Data;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.Scheduling;

/// <summary>
/// Failure and boundary edges of <see cref="SnapshotReceiver"/> that the session, integrity and
/// install-as-its-own-step suites do not reach: entry-level cancellation and disposal, a staging
/// directory that cannot be created or swept, the session-count cap across a blocked install,
/// deterministic tie-breaking between equally idle sessions, the supersession scope of a term-0
/// (legacy) sender, and the dispose-time cleanup of the in-memory accounting and spill files.
/// The receiver is constructed in isolation with a fake install callback, as in
/// <see cref="TestSnapshotReceiveSession"/>.
/// </summary>
public class TestSnapshotReceiverInstallEdges
{
    // ── helpers ────────────────────────────────────────────────────────────────

    /// <summary>
    /// Stands in for the partition executor's install path. While held, an install waits for
    /// <see cref="Release"/>; it then drains the staged snapshot and answers success.
    /// </summary>
    private sealed class GatedInstaller
    {
        private volatile TaskCompletionSource? gate;
        private int started;

        public int Started => Volatile.Read(ref started);

        public void Hold() => gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        public void Release() => gate?.TrySetResult();

        public async Task<SnapshotResponse> Install(SnapshotInstallRequest request)
        {
            Interlocked.Increment(ref started);

            TaskCompletionSource? held = gate;
            if (held is not null)
                await held.Task.ConfigureAwait(false);

            await request.Snapshot.CopyToAsync(Stream.Null).ConfigureAwait(false);
            return new SnapshotResponse(true);
        }
    }

    private static SnapshotReceiver NewReceiver(
        Func<SnapshotInstallRequest, Task<SnapshotResponse>> installOnExecutor,
        Func<long> clock,
        long ttlTicks = 1_000_000,
        int maxSessions = 8,
        long maxBytes = 1_000_000,
        Func<bool>? isDisposed = null,
        string? stagingDirectory = null,
        long stagingMemoryBytes = long.MaxValue) =>
        new(
            isDisposed: isDisposed ?? (static () => false),
            installOnExecutor: installOnExecutor,
            logger: NullLogger<IRaft>.Instance,
            localEndpoint: "test:1",
            sessionTtlTicks: ttlTicks,
            maxPendingSessions: maxSessions,
            maxPendingBytes: maxBytes,
            getMonotonicTimestamp: clock,
            stagingDirectory: stagingDirectory,
            stagingMemoryBytes: stagingMemoryBytes);

    private static SnapshotRequest Chunk(
        string session, int chunkIndex, bool isLast, byte[] data,
        string leader = "leader:1", int partitionId = 1, long snapshotIndex = 100,
        long leaderTerm = 3, long lastIncludedTerm = 2, byte[]? wholeSnapshot = null,
        bool installPolling = false) =>
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
            SnapshotChecksum = isLast ? Checksum(wholeSnapshot ?? data) : "",
            InstallPolling = installPolling,
        };

    private static SnapshotRequest StatusQuery(string session, int partitionId = 1) =>
        new() { StatusQuery = true, SessionId = session, PartitionId = partitionId, ChunkIndex = -1 };

    private static string Checksum(byte[] snapshot) => Convert.ToHexString(SHA256.HashData(snapshot));

    private static string NewStagingDirectory() =>
        Path.Combine(Path.GetTempPath(), "kommander-staging-" + Guid.NewGuid().ToString("N"));

    private static void DeleteDirectory(string directory)
    {
        try
        {
            if (Directory.Exists(directory))
            {
                File.SetUnixFileMode(directory, UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute);
                Directory.Delete(directory, recursive: true);
            }
        }
        catch
        {
            // Best-effort temp cleanup.
        }
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

    // ── entry-level cancellation and disposal ──────────────────────────────────

    /// <summary>
    /// A cancelled call stages nothing and opens no session: the cancellation is honoured before
    /// the receiver touches any of its state, so a timed-out transport call cannot leave a
    /// half-opened session behind.
    /// </summary>
    [Fact]
    public async Task CancelledCall_ThrowsBeforeAnythingIsStaged()
    {
        GatedInstaller installer = new();
        SnapshotReceiver r = NewReceiver(installer.Install, () => 1000);
        CancellationToken cancelled = new(canceled: true);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => r.ReceiveInstallSnapshot(Chunk("s", 0, isLast: true, [1, 2, 3]), cancelled));

        Assert.Equal(0, installer.Started);
        Assert.Equal(0, r.PendingSessionCount);
        Assert.Equal(0, r.TotalStagedByteCount);
    }

    /// <summary>
    /// A disposed node's receiver refuses chunks and status questions alike: an in-flight sender
    /// learns the call failed instead of its answer outliving the node.
    /// </summary>
    [Fact]
    public async Task DisposedReceiver_RefusesChunksAndStatusQueries()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        bool disposed = false;
        GatedInstaller installer = new();
        SnapshotReceiver r = NewReceiver(installer.Install, () => 1000, isDisposed: () => disposed);

        Assert.Equal(SnapshotInstallOutcome.NoInstall,
            (await r.ReceiveInstallSnapshot(StatusQuery(""), ct)).Outcome);

        disposed = true;

        SnapshotResponse chunk = await r.ReceiveInstallSnapshot(Chunk("s", 0, isLast: true, [1]), CancellationToken.None);
        Assert.False(chunk.Success);
        Assert.False((await r.ReceiveInstallSnapshot(StatusQuery(""), CancellationToken.None)).Success);
        Assert.Equal(0, installer.Started);
        Assert.Equal(0, r.PendingSessionCount);
    }

    // ── staging directory edges ────────────────────────────────────────────────

    /// <summary>
    /// The staging directory is a hard precondition for spilling: a path occupied by a regular
    /// file cannot be created, and the failure must surface at construction rather than at the
    /// first spill of a large snapshot.
    /// </summary>
    [Fact]
    public void StagingPathOccupiedByAFile_FailsConstruction()
    {
        string occupied = Path.Combine(Path.GetTempPath(), "kommander-staging-" + Guid.NewGuid().ToString("N"));

        try
        {
            File.WriteAllBytes(occupied, [1, 2, 3]);

            RaftException failure = Assert.Throws<RaftException>(
                () => NewReceiver(new GatedInstaller().Install, () => 1000, stagingDirectory: occupied));

            Assert.Contains(occupied, failure.Message, StringComparison.Ordinal);
        }
        finally
        {
            try
            {
                File.Delete(occupied);
            }
            catch
            {
                // Best-effort temp cleanup.
            }
        }
    }

    /// <summary>
    /// A leftover spill file the startup sweep cannot delete (here: a directory the process may
    /// not write) is warned about and lived with — construction must not fail, and the receiver
    /// keeps working for sessions that stay inside the memory budget.
    /// </summary>
    [Fact]
    public async Task UndeletableLeftoverStagingFile_IsSurvived()
    {
        Assert.SkipWhen(Environment.UserName == "root", "root ignores directory permissions");

        CancellationToken ct = TestContext.Current.CancellationToken;
        string directory = NewStagingDirectory();

        try
        {
            Directory.CreateDirectory(directory);
            string leftover = Path.Combine(directory, "snapshot-p1-dead" + SnapshotReceiver.StagingFileExtension);
            File.WriteAllBytes(leftover, [1, 2, 3]);
            File.SetUnixFileMode(directory, UnixFileMode.UserRead | UnixFileMode.UserExecute);

            SnapshotReceiver r = NewReceiver(new GatedInstaller().Install, () => 1000,
                stagingDirectory: directory, stagingMemoryBytes: 1_000_000);

            Assert.True(File.Exists(leftover), "an undeletable leftover must be left in place, not fatal");

            Assert.True((await r.ReceiveInstallSnapshot(Chunk("inmem", 0, false, [4, 5, 6]), ct)).Success);
            Assert.Equal(3, r.PendingByteCount);
        }
        finally
        {
            DeleteDirectory(directory);
        }
    }

    // ── capacity across a blocked install ──────────────────────────────────────

    /// <summary>
    /// A completed buffer whose install is blocked still occupies a session slot: with a cap of
    /// two and one install running, a second pending session opens only by evicting the first —
    /// a sender whose installs block on the executor cannot stack pending sessions around them.
    /// The byte cap's treatment of in-install buffers is already pinned; this is the count cap's.
    /// </summary>
    [Fact]
    public async Task BlockedInstall_CountsAgainstTheSessionCap_SoPendingSessionsMakeRoomForNewOnes()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        GatedInstaller installer = new();
        installer.Hold();
        SnapshotReceiver r = NewReceiver(installer.Install, () => 1000, maxSessions: 2);

        Assert.Equal(SnapshotInstallOutcome.InstallPending,
            (await r.ReceiveInstallSnapshot(Chunk("a", 0, true, [1], leader: "a:1", installPolling: true), ct)).Outcome);
        await WaitUntil(() => installer.Started == 1, ct);

        // Other partitions' sessions share the same cap; one slot is free beside the blocked install.
        Assert.True((await r.ReceiveInstallSnapshot(
            Chunk("b", 0, false, [2], leader: "b:1", partitionId: 2), ct)).Success);
        Assert.Equal(1, r.PendingSessionCount);

        Assert.True((await r.ReceiveInstallSnapshot(
            Chunk("c", 0, false, [3], leader: "c:1", partitionId: 2), ct)).Success);
        Assert.Equal(1, r.PendingSessionCount);

        Assert.False((await r.ReceiveInstallSnapshot(
            Chunk("b", 1, false, [2], leader: "b:1", partitionId: 2), ct)).Success);
        Assert.True((await r.ReceiveInstallSnapshot(
            Chunk("c", 1, false, [3], leader: "c:1", partitionId: 2), ct)).Success);

        installer.Release();
        await WaitUntil(() => r.ActiveInstallCount == 0, ct);
    }

    /// <summary>
    /// Two sessions whose last activity is the same tick (a frozen clock) are ordered by their
    /// composite key — leader endpoint, then partition, then session id — so the eviction choice
    /// is deterministic instead of dictionary-order luck.
    /// </summary>
    [Fact]
    public async Task EquallyIdleSessions_EvictInCompositeKeyOrder()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        ClockBox clock = new(1000);
        SnapshotReceiver r = NewReceiver(new GatedInstaller().Install, clock.Read, maxSessions: 2);

        Assert.True((await r.ReceiveInstallSnapshot(Chunk("first", 0, false, [1], leader: "b:1"), ct)).Success);
        Assert.True((await r.ReceiveInstallSnapshot(Chunk("second", 0, false, [1], leader: "a:1"), ct)).Success);
        Assert.Equal(2, r.PendingSessionCount);

        // The third open (same frozen tick) must evict leader "a:1" — the lower composite key.
        Assert.True((await r.ReceiveInstallSnapshot(Chunk("third", 0, false, [1], leader: "c:1"), ct)).Success);
        Assert.Equal(2, r.PendingSessionCount);

        Assert.False((await r.ReceiveInstallSnapshot(Chunk("second", 1, false, [2], leader: "a:1"), ct)).Success);
        Assert.True((await r.ReceiveInstallSnapshot(Chunk("first", 1, false, [2], leader: "b:1"), ct)).Success);
        Assert.True((await r.ReceiveInstallSnapshot(Chunk("third", 1, false, [2], leader: "c:1"), ct)).Success);
    }

    // ── supersession scope of a legacy sender ──────────────────────────────────

    /// <summary>
    /// A sender that predates leader terms (term 0) supersedes only its own earlier attempts, by
    /// the same-leader rule: it must not drop a term-5 leader's pending session on the strength of
    /// a term it does not carry.
    /// </summary>
    [Fact]
    public async Task TermZeroSender_SupersedesOnlyItsOwnEarlierAttempts()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        SnapshotReceiver r = NewReceiver(new GatedInstaller().Install, () => 1000);

        Assert.True((await r.ReceiveInstallSnapshot(
            Chunk("t5", 0, false, [1], leader: "L1", leaderTerm: 5), ct)).Success);

        Assert.True((await r.ReceiveInstallSnapshot(
            Chunk("legacy-1", 0, false, [1], leader: "L2", leaderTerm: 0), ct)).Success);
        Assert.Equal(2, r.PendingSessionCount);

        // The same legacy leader's newer attempt replaces its own older one…
        Assert.True((await r.ReceiveInstallSnapshot(
            Chunk("legacy-2", 0, false, [1], leader: "L2", leaderTerm: 0, snapshotIndex: 150), ct)).Success);
        Assert.Equal(2, r.PendingSessionCount);

        // …the term-5 session survived both.
        Assert.False((await r.ReceiveInstallSnapshot(
            Chunk("legacy-1", 1, false, [1], leader: "L2", leaderTerm: 0), ct)).Success);
        Assert.True((await r.ReceiveInstallSnapshot(
            Chunk("t5", 1, false, [2], leader: "L1", leaderTerm: 5), ct)).Success);
        Assert.True((await r.ReceiveInstallSnapshot(
            Chunk("legacy-2", 1, false, [2], leader: "L2", leaderTerm: 0, snapshotIndex: 150), ct)).Success);
    }

    // ── dispose-time cleanup ───────────────────────────────────────────────────

    /// <summary>
    /// Disposing the receiver releases the in-memory accounting of sessions that never spilled,
    /// and a session that had spilled to disk has its file deleted — an abandoned staging
    /// directory must not accumulate files from a node that shut down cleanly.
    /// </summary>
    [Fact]
    public async Task DisposePendingSnapshots_ReleasesInMemoryBytes_AndDeletesSpillFiles()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string directory = NewStagingDirectory();

        try
        {
            SnapshotReceiver r = NewReceiver(new GatedInstaller().Install, () => 1000,
                stagingDirectory: directory, stagingMemoryBytes: 4);

            // One session stays inside the memory budget; the other would cross it and spills.
            Assert.True((await r.ReceiveInstallSnapshot(Chunk("mem", 0, false, [1, 2, 3], leader: "a"), ct)).Success);
            Assert.True((await r.ReceiveInstallSnapshot(Chunk("spill", 0, false, [4, 5, 6], leader: "b"), ct)).Success);

            Assert.Equal(3, r.InMemoryStagedByteCount);
            Assert.Equal(2, r.PendingSessionCount);
            Assert.Single(Directory.GetFiles(directory));

            r.DisposePendingSnapshots();

            Assert.Equal(0, r.PendingSessionCount);
            Assert.Equal(0, r.PendingByteCount);
            Assert.Equal(0, r.InMemoryStagedByteCount);
            Assert.Empty(Directory.GetFiles(directory));
        }
        finally
        {
            DeleteDirectory(directory);
        }
    }

    private sealed class ClockBox
    {
        public long Value;
        public ClockBox(long start) => Value = start;
        public long Read() => Value;
    }
}
