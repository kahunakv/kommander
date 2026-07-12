
using System.Text.Json;
using Kommander;
using Kommander.Communication;
using Kommander.Communication.Memory;
using Kommander.Communication.Rest;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.Communication;

/// <summary>
/// Tests for the follower-side snapshot install path:
/// <list type="bullet">
///   <item>SystemState happy path: <see cref="IRaftSystemStateTransfer.ImportPartitionState"/> is called
///     and a <c>CommittedCheckpoint</c> is seeded at <see cref="SnapshotRequest.SnapshotIndex"/>.</item>
///   <item>Idempotent re-send: the second session with the same SnapshotIndex fast-paths without
///     re-calling ImportPartitionState.</item>
///   <item>Atomicity: a throw in ImportPartitionState returns Success=false and does NOT seed the
///     CommittedCheckpoint, so a retry starts clean.</item>
///   <item>No-transfer guard: a SystemState request with no registered system-state transfer returns
///     Success=false immediately without touching any state.</item>
///   <item>REST round-trip: SystemState and Range requests survive the JSON serialization layer;
///     a request with no Kind field deserializes as Range.</item>
/// </list>
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
public class TestFollowerSnapshotInstall
{
    // ── Helpers ───────────────────────────────────────────────────────────────

    private static (RaftManager manager, InMemoryWAL wal) BuildManager()
    {
        InMemoryWAL wal = new(NullLogger<IRaft>.Instance);
        RaftManager manager = new(
            new RaftConfiguration { Host = "localhost", Port = 19400, InitialPartitions = 1 },
            new StaticDiscovery([]),
            wal,
            new InMemoryCommunication(),
            new HybridLogicalClock(),
            NullLogger<IRaft>.Instance);
        return (manager, wal);
    }

    // ── Happy path ────────────────────────────────────────────────────────────

    /// <summary>
    /// A single-chunk SystemState snapshot: ImportPartitionState is called once and a
    /// CommittedCheckpoint is seeded at the snapshot index so normal backfill can resume.
    /// </summary>
    [Fact]
    public async Task SystemState_HappyPath_ImportsAndSeedsCheckpoint()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        var (manager, wal) = BuildManager();
        try
        {
            CapturingSystemTransfer systemTransfer = new();
            manager.RegisterSystemStateTransfer(systemTransfer);

            SnapshotRequest request = new()
            {
                SessionId = "session-1",
                PartitionId = 0,
                SnapshotIndex = 100,
                FollowerEndpoint = "localhost:19400",
                ChunkIndex = 0,
                IsLast = true,
                Data = new byte[] { 0xAA, 0xBB, 0xCC },
                Kind = SnapshotKind.SystemState,
            };

            SnapshotResponse response = await manager.ReceiveInstallSnapshot(request, ct);

            Assert.True(response.Success);
            Assert.Equal(1, systemTransfer.ImportCount);
            // Checkpoint must be seeded so backfill resumes at SnapshotIndex + 1.
            Assert.Equal(100L, wal.GetLastCheckpoint(partitionId: 0));
        }
        finally
        {
            manager.Dispose();
        }
    }

    /// <summary>
    /// Multi-chunk SystemState: chunks accumulate across calls; import runs only on IsLast.
    /// </summary>
    [Fact]
    public async Task SystemState_MultiChunk_AccumulatesAndImportsOnLast()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        var (manager, wal) = BuildManager();
        try
        {
            CapturingSystemTransfer systemTransfer = new();
            manager.RegisterSystemStateTransfer(systemTransfer);

            SnapshotResponse r0 = await manager.ReceiveInstallSnapshot(new()
            {
                SessionId = "session-mc",
                PartitionId = 0,
                SnapshotIndex = 200,
                FollowerEndpoint = "localhost:19400",
                ChunkIndex = 0,
                IsLast = false,
                Data = new byte[] { 0x01, 0x02 },
                Kind = SnapshotKind.SystemState,
            }, ct);

            Assert.True(r0.Success);
            Assert.Equal(0, systemTransfer.ImportCount); // not yet applied

            SnapshotResponse r1 = await manager.ReceiveInstallSnapshot(new()
            {
                SessionId = "session-mc",
                PartitionId = 0,
                SnapshotIndex = 200,
                FollowerEndpoint = "localhost:19400",
                ChunkIndex = 1,
                IsLast = true,
                Data = new byte[] { 0x03, 0x04 },
                Kind = SnapshotKind.SystemState,
            }, ct);

            Assert.True(r1.Success);
            Assert.Equal(1, systemTransfer.ImportCount);
            // Verify all bytes were delivered in order.
            Assert.Equal([0x01, 0x02, 0x03, 0x04], systemTransfer.ReceivedBytes);
            Assert.Equal(200L, wal.GetLastCheckpoint(partitionId: 0));
        }
        finally
        {
            manager.Dispose();
        }
    }

    // ── Idempotent re-send ────────────────────────────────────────────────────

    /// <summary>
    /// A re-sent snapshot for an index the follower has already installed returns Success=true
    /// immediately without calling ImportPartitionState a second time.
    /// </summary>
    [Fact]
    public async Task SystemState_IdempotentResend_FastPathNoSecondImport()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        var (manager, wal) = BuildManager();
        try
        {
            CapturingSystemTransfer systemTransfer = new();
            manager.RegisterSystemStateTransfer(systemTransfer);

            SnapshotRequest firstRequest = new()
            {
                SessionId = "session-idem",
                PartitionId = 0,
                SnapshotIndex = 50,
                FollowerEndpoint = "localhost:19400",
                ChunkIndex = 0,
                IsLast = true,
                Data = new byte[] { 0xFF },
                Kind = SnapshotKind.SystemState,
            };

            SnapshotResponse first = await manager.ReceiveInstallSnapshot(firstRequest, ct);
            Assert.True(first.Success);
            Assert.Equal(1, systemTransfer.ImportCount);

            // Re-send the same snapshot index with a new session — must fast-path.
            SnapshotResponse second = await manager.ReceiveInstallSnapshot(new()
            {
                SessionId = "session-idem-2",
                PartitionId = 0,
                SnapshotIndex = 50,
                FollowerEndpoint = "localhost:19400",
                ChunkIndex = 0,
                IsLast = true,
                Data = new byte[] { 0xFF },
                Kind = SnapshotKind.SystemState,
            }, ct);
            Assert.True(second.Success);
            Assert.Equal(1, systemTransfer.ImportCount); // still 1 — import was not called again
        }
        finally
        {
            manager.Dispose();
        }
    }

    // ── Atomicity: throw in ImportPartitionState ──────────────────────────────

    /// <summary>
    /// When ImportPartitionState throws, ReceiveInstallSnapshot returns Success=false and does NOT
    /// seed the CommittedCheckpoint.  A subsequent retry with the same SnapshotIndex re-enters the
    /// import path (proving the WAL was not poisoned by a partial install).
    /// </summary>
    [Fact]
    public async Task SystemState_ImportThrows_NoCheckpointSeededAndRetryRuns()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        var (manager, wal) = BuildManager();
        try
        {
            ThrowingSystemTransfer throwingTransfer = new();
            manager.RegisterSystemStateTransfer(throwingTransfer);

            SnapshotRequest request = new()
            {
                SessionId = "session-throw-1",
                PartitionId = 0,
                SnapshotIndex = 75,
                FollowerEndpoint = "localhost:19400",
                ChunkIndex = 0,
                IsLast = true,
                Data = new byte[] { 0xDE, 0xAD },
                Kind = SnapshotKind.SystemState,
            };

            // First attempt: import throws → must fail.
            SnapshotResponse failed = await manager.ReceiveInstallSnapshot(request, ct);
            Assert.False(failed.Success);

            // Verify no checkpoint was seeded (WAL max stays below SnapshotIndex).
            long maxAfterFail = wal.GetMaxLog(partitionId: 0);
            Assert.True(maxAfterFail < 75,
                $"CommittedCheckpoint must not be seeded after a failed import; GetMaxLog={maxAfterFail}");

            // Retry with a succeeding transfer — the retry must actually run ImportPartitionState,
            // proving the idempotency fast-path was not triggered (no checkpoint was seeded).
            CapturingSystemTransfer succeedingTransfer = new();
            manager.RegisterSystemStateTransfer(succeedingTransfer);

            SnapshotResponse retry = await manager.ReceiveInstallSnapshot(new()
            {
                SessionId = "session-throw-retry",
                PartitionId = 0,
                SnapshotIndex = 75,
                FollowerEndpoint = "localhost:19400",
                ChunkIndex = 0,
                IsLast = true,
                Data = new byte[] { 0xDE, 0xAD },
                Kind = SnapshotKind.SystemState,
            }, ct);

            Assert.True(retry.Success);
            Assert.Equal(1, succeedingTransfer.ImportCount); // import ran on retry
            Assert.Equal(75L, wal.GetLastCheckpoint(partitionId: 0));
        }
        finally
        {
            manager.Dispose();
        }
    }

    // ── No-transfer guard ─────────────────────────────────────────────────────

    /// <summary>
    /// A SystemState request with no registered system-state transfer returns Success=false
    /// without accumulating any chunk state.
    /// </summary>
    [Fact]
    public async Task SystemState_NoTransferRegistered_ReturnsFalse()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        var (manager, wal) = BuildManager();
        try
        {
            // No RegisterSystemStateTransfer call.

            SnapshotResponse response = await manager.ReceiveInstallSnapshot(new()
            {
                SessionId = "session-notr",
                PartitionId = 0,
                SnapshotIndex = 10,
                FollowerEndpoint = "localhost:19400",
                ChunkIndex = 0,
                IsLast = true,
                Data = new byte[] { 0x01 },
                Kind = SnapshotKind.SystemState,
            }, ct);

            Assert.False(response.Success);
            Assert.Equal(0L, wal.GetMaxLog(partitionId: 0));
        }
        finally
        {
            manager.Dispose();
        }
    }

    // ── REST round-trip ───────────────────────────────────────────────────────

    /// <summary>
    /// End-to-end REST round-trip: a <see cref="RestCommunication"/> sender POSTs a
    /// <see cref="SnapshotKind.SystemState"/> chunk to a real Kestrel receiver, and the receiver
    /// routes the request to <see cref="IRaftSystemStateTransfer.ImportPartitionState"/>.
    /// Covers the REST serialization path for the snapshot kind discriminator.
    /// </summary>
    [Fact]
    public async Task Rest_SystemState_RoundTrips_ToImportPartitionState()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        using ILoggerFactory logFactory = LoggerFactory.Create(b => b.SetMinimumLevel(LogLevel.Warning));

        const int receiverPort = 19410;
        const int senderPort   = 19411;
        List<RaftNode> peers = [new($"localhost:{receiverPort}"), new($"localhost:{senderPort}")];

        CapturingSystemTransfer systemTransfer = new();

        await using RestTestNode receiver = RestTestNode.Create(receiverPort, peers, logFactory);
        await using RestTestNode sender   = RestTestNode.Create(senderPort,   peers, logFactory);

        await receiver.StartAsync(ct);
        await sender.StartAsync(ct);

        receiver.Manager.RegisterSystemStateTransfer(systemTransfer);

        SnapshotRequest request = new()
        {
            SessionId = "rest-system",
            PartitionId = 0,
            SnapshotIndex = 5,
            FollowerEndpoint = receiver.Endpoint,
            ChunkIndex = 0,
            IsLast = true,
            Data = new byte[] { 0xAB },
            Kind = SnapshotKind.SystemState,
        };

        RestCommunication rest = new();
        SnapshotResponse response = await rest.SendInstallSnapshot(
            sender.Manager, new RaftNode(receiver.Endpoint), request, ct);

        Assert.True(response.Success);
        Assert.Equal(1, systemTransfer.ImportCount);
    }

    /// <summary>
    /// A REST request where Kind == Range (the C# default, 0) serializes as <c>"kind":0</c>
    /// and routes to <see cref="IRaftStateMachineTransfer.ImportRange"/> on the receiver.
    /// This is the normal path; the genuine wire-compat scenario (old peer emitting no kind
    /// property at all) is covered by <see cref="Rest_MissingKindProperty_DeserializesAsRange"/>.
    /// </summary>
    [Fact]
    public async Task Rest_ZeroKindValue_RoutesToImportRange()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        using ILoggerFactory logFactory = LoggerFactory.Create(b => b.SetMinimumLevel(LogLevel.Warning));

        const int receiverPort = 19412;
        const int senderPort   = 19413;
        List<RaftNode> peers = [new($"localhost:{receiverPort}"), new($"localhost:{senderPort}")];

        CapturingRangeTransfer rangeTransfer = new();

        await using RestTestNode receiver = RestTestNode.Create(receiverPort, peers, logFactory);
        await using RestTestNode sender   = RestTestNode.Create(senderPort,   peers, logFactory);

        await receiver.StartAsync(ct);
        await sender.StartAsync(ct);

        receiver.Manager.RegisterStateMachineTransfer(rangeTransfer);

        SnapshotRequest request = new()
        {
            SessionId = "rest-zero-kind",
            PartitionId = 0,
            SnapshotIndex = 1,
            FollowerEndpoint = receiver.Endpoint,
            ChunkIndex = 0,
            IsLast = true,
            Data = new byte[] { 0xFF },
            Kind = SnapshotKind.Range,
        };

        RestCommunication rest = new();
        SnapshotResponse response = await rest.SendInstallSnapshot(
            sender.Manager, new RaftNode(receiver.Endpoint), request, ct);

        Assert.True(response.Success);
        Assert.Equal(1, rangeTransfer.ImportCount);
    }

    /// <summary>
    /// Wire-compat: a JSON body from an old peer that predates the <c>kind</c> field (the
    /// property is absent rather than present with value 0) must deserialize as
    /// <see cref="SnapshotKind.Range"/> through <see cref="RestJsonContext"/>.
    /// </summary>
    [Fact]
    public void Rest_MissingKindProperty_DeserializesAsRange()
    {
        const string json = """
            {
                "sessionId": "old-peer",
                "partitionId": 0,
                "snapshotIndex": 42,
                "followerEndpoint": "localhost:9000",
                "chunkIndex": 0,
                "isLast": true,
                "data": "AA=="
            }
            """;

        SnapshotRequest? request = JsonSerializer.Deserialize(json, RestJsonContext.Default.SnapshotRequest);

        Assert.NotNull(request);
        Assert.Equal(SnapshotKind.Range, request.Kind);
    }

    // ── Stubs ─────────────────────────────────────────────────────────────────

    private sealed class CapturingSystemTransfer : IRaftSystemStateTransfer
    {
        public int ImportCount;
        public byte[] ReceivedBytes { get; private set; } = [];

        public Task<Stream> ExportPartitionState(int partitionId, long upToIndex, CancellationToken ct) =>
            Task.FromResult<Stream>(new MemoryStream());

        public async Task ImportPartitionState(int partitionId, Stream snapshot, CancellationToken ct)
        {
            Interlocked.Increment(ref ImportCount);
            using MemoryStream ms = new();
            await snapshot.CopyToAsync(ms, ct);
            ReceivedBytes = ms.ToArray();
        }
    }

    private sealed class ThrowingSystemTransfer : IRaftSystemStateTransfer
    {
        public Task<Stream> ExportPartitionState(int partitionId, long upToIndex, CancellationToken ct) =>
            Task.FromResult<Stream>(new MemoryStream());

        public Task ImportPartitionState(int partitionId, Stream snapshot, CancellationToken ct) =>
            throw new InvalidOperationException("simulated import failure");
    }

    private sealed class CapturingRangeTransfer : IRaftStateMachineTransfer
    {
        public int ImportCount;

        public Task<Stream> ExportRange(RaftSplitPlan plan, long upToIndex, CancellationToken ct) =>
            Task.FromResult<Stream>(new MemoryStream());

        public Task ImportRange(int targetPartitionId, Stream snapshot, CancellationToken ct)
        {
            Interlocked.Increment(ref ImportCount);
            return Task.CompletedTask;
        }
    }
}
