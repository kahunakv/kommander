using Kommander.Data;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging.Abstractions;
using RocksDbSharp;

namespace Kommander.Tests.WAL;

/// <summary>
/// The filesystem-probe gate of <see cref="RocksDbWAL"/>'s storage-failure recovery.
///
/// <para>Recovery asks two questions before reopening the engine: does the volume accept a small
/// durable write now (the probe file), and does the engine accept writes now (the canary). While
/// the volume is still full the FIRST question fails, so no reopen may happen even though the
/// engine has latched its error — a premature reopen on a full volume would fail anyway and leave
/// the WAL closed. These tests pin that gate against a genuinely full filesystem
/// (<see cref="EnospcVolume"/>), skipping where none is available, and the recovery that follows
/// once space returns.</para>
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
public sealed class TestRocksDbWalStorageFailureGate
{
    private const int Partition = 1;

    private const string WalCreationFailureMarker = "open a file for appending";

    /// <summary>
    /// While the volume refuses writes, failed writes keep surfacing <see cref="RaftOperationStatus.Errored"/>
    /// (never an escaping native exception), the failure is recorded for diagnostics, already-acknowledged
    /// entries stay readable, and crucially the engine is NOT reopened — the probe cannot pass on a full
    /// volume. Once space returns, the next retries recover in the same process: when the engine had
    /// latched (the unrecoverable WAL-creation failure), recovery must go through an engine reopen, the
    /// retried entry lands exactly once, and the pre-episode prefix is intact and contiguous.
    /// </summary>
    [Fact]
    public void FullVolume_FailuresNeverReopenTheEngine_ProbePassesRecoveryReopensAndKeepsPrefix()
    {
        using EnospcVolume? volume = EnospcVolume.TryCreate();
        Assert.SkipWhen(volume is null, EnospcVolume.SkipReason);

        const int entries = 25;

        using RocksDbWAL wal = new(volume.Root, "wal", NullLogger<IRaft>.Instance, syncWrites: true);
        IWAL hardState = wal; // PersistHardState / TryGetHardState are interface default members

        for (long id = 1; id <= entries; id++)
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Entry(id)])]));

        Assert.True(hardState.PersistHardState(Partition, currentTerm: 2, votedFor: "node-a"));

        volume.Fill();

        // Fail a write on the full volume (RocksDB may satisfy appends from preallocated WAL space,
        // so a forced memtable switch is interleaved to reach a refused file creation).
        long failedId = WriteUntilErrored(wal, Partition, entries + 1);

        // Force the memtable switch that needs to create a fresh WAL file: on a volume that refuses
        // file creation this latches the engine with the unrecoverable error.
        string? flushError = null;
        try
        {
            wal.FlushMemTablesForTesting();
        }
        catch (RocksDbException ex)
        {
            flushError = ex.Message;
        }

        // The gate: the volume is still full, so however many more failures arrive, none of them may
        // reopen the engine (the probe file cannot be written), and every write must report Errored
        // as a status rather than throwing.
        for (int attempt = 0; attempt < 3; attempt++)
            Assert.Equal(RaftOperationStatus.Errored, wal.Write([(Partition, [Entry(failedId)])]));

        Assert.Equal(0, wal.EngineReopenCount);
        Assert.NotNull(wal.LastStorageFailureMessage);

        // Reads do not take the failing write path: everything acknowledged before the outage is
        // still readable through the latched engine.
        List<RaftLog> duringOutage = wal.ReadLogs(Partition);
        Assert.Equal(entries, duringOutage.Count);
        Assert.Equal(entries, duringOutage[^1].Id);

        // Hard-state persistence reports failure as false, not as an exception.
        Assert.False(hardState.PersistHardState(Partition, currentTerm: 3, votedFor: "node-a"));

        volume.Free();
        Assert.True(volume.AvailableFreeSpace > 100L * 1024 * 1024, "the filler was not released");

        // The retry loop a real node runs on its own cadence. A latched engine recovers through the
        // reopen immediately once the probe passes; an append-failure shape may resume on its own.
        RaftOperationStatus status = RaftOperationStatus.Errored;
        DateTime deadline = DateTime.UtcNow + TestTimeouts.Scale(TimeSpan.FromSeconds(60));

        while (status != RaftOperationStatus.Success && DateTime.UtcNow < deadline)
        {
            status = wal.Write([(Partition, [Entry(failedId)])]);
            if (status != RaftOperationStatus.Success)
                Thread.Sleep(250);
        }

        Assert.Equal(RaftOperationStatus.Success, status);

        bool latched = flushError?.Contains(WalCreationFailureMarker, StringComparison.Ordinal) == true
                       || wal.LastStorageFailureMessage?.Contains(WalCreationFailureMarker, StringComparison.Ordinal) == true;

        TestContext.Current.TestOutputHelper?.WriteLine(
            $"latched={latched} reopens={wal.EngineReopenCount} flushError={flushError}");

        if (latched)
            Assert.True(wal.EngineReopenCount >= 1, "a latched engine can only recover through a reopen");

        // The retried entry landed exactly once and nothing acknowledged before or during the
        // episode was lost: ids 1..failedId, contiguous, each present exactly once.
        List<RaftLog> logs = wal.ReadLogs(Partition);
        Assert.Equal(failedId, logs.Count);
        for (int i = 0; i < logs.Count; i++)
            Assert.Equal(i + 1, logs[i].Id);

        Assert.Equal(failedId, wal.GetMaxLog(Partition));

        // The engine is fully usable again, not just for appends.
        Assert.True(hardState.PersistHardState(Partition, currentTerm: 4, votedFor: "node-b"));
        Assert.True(hardState.TryGetHardState(Partition, out long term, out string? votedFor));
        Assert.Equal(4, term);
        Assert.Equal("node-b", votedFor);
    }

    // ── Harness ────────────────────────────────────────────────────────────

    private static RaftLog Entry(long id) => new()
    {
        Id = id,
        Term = 1,
        Type = RaftLogType.Committed,
        LogType = "Greeting",
        LogData = new byte[4096],
        Time = HLCTimestamp.Zero,
    };

    /// <summary>
    /// Writes entries from <paramref name="firstId"/> upward until the WAL reports Errored and returns
    /// the id that failed. RocksDB preallocates space for its WAL file (about 70 MiB with the default
    /// 64 MiB write buffer), and a filesystem that honours <c>fallocate</c> lets every append land
    /// inside that reservation long after the volume is full, so every
    /// <see cref="FlushInterval"/> entries a memtable switch is forced: that creates a fresh WAL file,
    /// which a volume that refuses file creation rejects at once.
    /// </summary>
    private static long WriteUntilErrored(RocksDbWAL wal, int partition, long firstId)
    {
        const int maxEntries = 8192;

        for (long id = firstId; id < firstId + maxEntries; id++)
        {
            if (wal.Write([(partition, [Entry(id)])]) == RaftOperationStatus.Errored)
                return id;

            if ((id - firstId + 1) % FlushInterval == 0)
            {
                try { wal.FlushMemTablesForTesting(); }
                catch (RocksDbException) { }
            }
        }

        throw new InvalidOperationException("The WAL never reported Errored on a full volume.");
    }

    /// <summary>Entries between two forced memtable switches in <see cref="WriteUntilErrored"/>.</summary>
    private const int FlushInterval = 256;
}
