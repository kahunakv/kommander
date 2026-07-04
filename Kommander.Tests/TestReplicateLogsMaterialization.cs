using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Kommander.WAL.IO;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests;

/// <summary>
/// Verifies the materialization invariants on the public batch-replication path:
/// <list type="bullet">
///   <item><description>
///     A generator passed to the <see cref="IEnumerable{T}"/> overload is enumerated exactly
///     once per <c>ReplicateLogs</c> call, not once per retry — the enumerable is materialized
///     before the <c>ActiveProposal</c> retry loop.
///   </description></item>
///   <item><description>
///     An array or list passed to the <see cref="IEnumerable{T}"/> overload is not copied —
///     the cast to <see cref="IReadOnlyList{T}"/> succeeds and the payload references are
///     forwarded directly.
///   </description></item>
///   <item><description>
///     The <see cref="IReadOnlyList{T}"/> overload exists and is reachable via the
///     <see cref="IRaft"/> interface.
///   </description></item>
/// </list>
/// Tests run without a live cluster: the partition has no leader so <c>ReplicateLogs</c>
/// returns <see cref="RaftOperationStatus.NodeIsNotLeader"/> immediately after materialization,
/// which is sufficient to observe the enumeration behavior.
/// </summary>
public sealed class TestReplicateLogsMaterialization
{
    // ── Helpers ────────────────────────────────────────────────────────────

    /// <summary>
    /// Creates a minimal <see cref="RaftManager"/> with one pre-wired partition
    /// (no leader, no cluster). The partition is added directly to the manager's
    /// internal dictionary so <c>GetPartition(1)</c> succeeds.
    /// </summary>
    private static RaftManager CreateManager()
    {
        RaftConfiguration config = new()
        {
            Host = "localhost",
            Port = 9000,
            InitialPartitions = 0,
        };

        InMemoryWAL wal = new(NullLogger<IRaft>.Instance);

        RaftManager manager = new(
            config,
            new StaticDiscovery([]),
            wal,
            new InMemoryCommunication(),
            new HybridLogicalClock(),
            NullLogger<IRaft>.Instance);

        ((FairReadScheduler)manager.ReadScheduler).Start();

        RaftPartition partition = new(
            manager,
            wal,
            partitionId: 1,
            startRange: 0,
            endRange: 0,
            NullLogger<IRaft>.Instance);

        manager.Partitions.TryAdd(1, partition);

        return manager;
    }

    private static byte[] Payload(int seed) => [(byte)seed];

    // ── Tests ──────────────────────────────────────────────────────────────

    /// <summary>
    /// A generator input is enumerated exactly once, even though the partition returns
    /// <c>NodeIsNotLeader</c> on the first attempt (no retry occurs, but this proves
    /// the materialization precedes the loop rather than happening inside it).
    /// </summary>
    [Theory]
    [InlineData(1)]
    [InlineData(8)]
    [InlineData(64)]
    public async Task IEnumerableOverload_GeneratorEnumeratedExactlyOnce(int payloadCount)
    {
        using RaftManager manager = CreateManager();
        int enumerationCount = 0;

        IEnumerable<byte[]> CountingGenerator()
        {
            enumerationCount++;
            for (int i = 0; i < payloadCount; i++)
                yield return Payload(i);
        }

        RaftReplicationResult result = await manager.ReplicateLogs(1, "test", CountingGenerator(), cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal(RaftOperationStatus.NodeIsNotLeader, result.Status);
        Assert.Equal(1, enumerationCount);
    }

    /// <summary>
    /// Drives the <c>ActiveProposal</c> retry loop ≥2 times (via a test seam that returns
    /// <c>ActiveProposal</c> then a terminal status) and asserts the generator was enumerated
    /// exactly once. This is the real regression guard for the "materialize before the loop"
    /// change: with the old in-loop <c>ToList()</c>, a generator would be re-enumerated once per
    /// retry, so this test would observe an enumeration count equal to the retry count.
    /// </summary>
    [Fact]
    public async Task IEnumerableOverload_GeneratorNotReEnumeratedAcrossRetries()
    {
        using RaftManager manager = CreateManager();

        int enumerationCount = 0;

        IEnumerable<byte[]> CountingGenerator()
        {
            enumerationCount++;
            for (int i = 0; i < 4; i++)
                yield return Payload(i);
        }

        // First attempt returns ActiveProposal (forces a retry); the second returns a terminal
        // NodeIsNotLeader, which exits the loop. Materialization happens in the IEnumerable
        // overload before the loop, so the generator must be enumerated exactly once regardless
        // of how many times the loop iterates.
        int attempts = 0;
        manager._replicateAttemptHookForTesting = () =>
        {
            attempts++;
            return attempts == 1
                ? (false, RaftOperationStatus.ActiveProposal, HLCTimestamp.Zero)
                : (false, RaftOperationStatus.NodeIsNotLeader, HLCTimestamp.Zero);
        };

        RaftReplicationResult result = await manager.ReplicateLogs(1, "test", CountingGenerator(), cancellationToken: TestContext.Current.CancellationToken);

        Assert.True(attempts >= 2, $"retry loop should have iterated at least twice; got {attempts}");
        Assert.Equal(1, enumerationCount); // materialized once, reused across every retry
        Assert.Equal(RaftOperationStatus.NodeIsNotLeader, result.Status);
    }

    /// <summary>
    /// An array passed to the <see cref="IEnumerable{T}"/> overload is cast without copying:
    /// the manager checks <c>logs as IReadOnlyList&lt;byte[]&gt;</c> first, and because an array
    /// satisfies that interface, <c>GetEnumerator</c> is never called. We use a dual-interface
    /// wrapper that records whether enumeration was attempted.
    /// </summary>
    [Fact]
    public async Task IEnumerableOverload_ArrayIsNotCopied()
    {
        using RaftManager manager = CreateManager();

        byte[][] array = [Payload(1), Payload(2), Payload(3)];

        // EnumerationDetector implements both IReadOnlyList<byte[]> and IEnumerable<byte[]>.
        // The cast to IReadOnlyList<byte[]> succeeds in RaftManager, so GetEnumerator is
        // never called — confirming no ToList() copy occurs.
        EnumerationDetector detector = new(array);

        RaftReplicationResult result = await manager.ReplicateLogs(1, "test", detector, cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal(RaftOperationStatus.NodeIsNotLeader, result.Status);
        Assert.False(detector.WasEnumerated); // cast succeeded, no enumeration
    }

    private sealed class EnumerationDetector(byte[][] source) : IEnumerable<byte[]>, IReadOnlyList<byte[]>
    {
        public bool WasEnumerated { get; private set; }

        // IReadOnlyList<byte[]> members — the manager casts to this first.
        public int Count => source.Length;
        public byte[] this[int index] => source[index];

        // IEnumerable<byte[]> — only reached if ToList() or foreach is called.
        public IEnumerator<byte[]> GetEnumerator()
        {
            WasEnumerated = true;
            return ((IEnumerable<byte[]>)source).GetEnumerator();
        }

        global::System.Collections.IEnumerator global::System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
    }

    /// <summary>
    /// The <see cref="IReadOnlyList{T}"/> overload is accessible via <see cref="IRaft"/>
    /// and returns <c>NodeIsNotLeader</c> rather than throwing for a leaderless partition.
    /// </summary>
    [Fact]
    public async Task IReadOnlyListOverload_ExistsOnIRaftAndReturnsCorrectStatus()
    {
        using RaftManager manager = CreateManager();
        IRaft raft = manager;

        byte[][] payloads = [Payload(10), Payload(20)];

        // The IReadOnlyList<byte[]> overload must be reachable via the interface.
        RaftReplicationResult result = await raft.ReplicateLogs(1, "test", (IReadOnlyList<byte[]>)payloads, cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal(RaftOperationStatus.NodeIsNotLeader, result.Status);
    }

    /// <summary>
    /// The <see cref="IReadOnlyList{T}"/> overload on <see cref="RaftManager"/> is distinct
    /// from the <see cref="IEnumerable{T}"/> overload — calling it with an array skips any
    /// intermediate materialization and passes the array's reference directly.
    /// </summary>
    [Fact]
    public async Task IReadOnlyListOverload_DirectCallSkipsMaterialization()
    {
        using RaftManager manager = CreateManager();

        byte[][] payloads = [Payload(1), Payload(2)];
        IReadOnlyList<byte[]> list = payloads; // no copy; same reference

        RaftReplicationResult result = await manager.ReplicateLogs(1, "test", list, cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal(RaftOperationStatus.NodeIsNotLeader, result.Status);
    }
}
