
using Kommander;
using Kommander.Data;
using Kommander.WAL;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests;

/// <summary>
/// GetLastCheckpoint must report the highest CommittedCheckpoint index. A stubbed -1 keeps the
/// leader's snapshot fallback and the checkpoint-driven compaction pass disabled, which leaves a
/// far-behind follower to recover only by one-index-at-a-time AppendLogs backtracking (a warning
/// storm under load).
/// </summary>
public sealed class TestInMemoryWalCheckpoint
{
    private static RaftLog Log(long id, RaftLogType type) => new()
    {
        Id = id,
        Type = type,
        Term = 1,
        LogType = "",
        LogData = [],
    };

    private static InMemoryWAL NewWal() => new(NullLogger<IRaft>.Instance);

    [Fact]
    public void EmptyWal_ReturnsMinusOne() =>
        Assert.Equal(-1, NewWal().GetLastCheckpoint(1));

    [Fact]
    public void NoCheckpointEntries_ReturnsMinusOne()
    {
        InMemoryWAL wal = NewWal();
        wal.Write([(1, [Log(1, RaftLogType.Committed), Log(2, RaftLogType.Committed)])]);

        Assert.Equal(-1, wal.GetLastCheckpoint(1));
    }

    [Fact]
    public void SingleCheckpoint_ReturnsItsIndex()
    {
        InMemoryWAL wal = NewWal();
        wal.Write([(1, [
            Log(1, RaftLogType.Committed),
            Log(2, RaftLogType.CommittedCheckpoint),
            Log(3, RaftLogType.Committed),
        ])]);

        Assert.Equal(2, wal.GetLastCheckpoint(1));
    }

    [Fact]
    public void MultipleCheckpoints_ReturnsHighest()
    {
        InMemoryWAL wal = NewWal();
        wal.Write([(1, [
            Log(10, RaftLogType.CommittedCheckpoint),
            Log(20, RaftLogType.Committed),
            Log(30, RaftLogType.CommittedCheckpoint),
            Log(40, RaftLogType.Committed),
        ])]);

        Assert.Equal(30, wal.GetLastCheckpoint(1));
    }

    [Fact]
    public void CheckpointIsPerPartition()
    {
        InMemoryWAL wal = NewWal();
        wal.Write([(1, [Log(5, RaftLogType.CommittedCheckpoint)])]);
        wal.Write([(2, [Log(9, RaftLogType.CommittedCheckpoint)])]);

        Assert.Equal(5, wal.GetLastCheckpoint(1));
        Assert.Equal(9, wal.GetLastCheckpoint(2));
        Assert.Equal(-1, wal.GetLastCheckpoint(3));
    }

    /// <summary>
    /// Compaction removes entries strictly below the checkpoint; the checkpoint entry itself is
    /// retained, so GetLastCheckpoint still reports it after a compaction pass.
    /// </summary>
    [Fact]
    public void SurvivesCompactionBelowCheckpoint()
    {
        InMemoryWAL wal = NewWal();
        wal.Write([(1, [
            Log(1, RaftLogType.Committed),
            Log(2, RaftLogType.Committed),
            Log(3, RaftLogType.CommittedCheckpoint),
            Log(4, RaftLogType.Committed),
        ])]);

        wal.CompactLogsOlderThan(1, lastCheckpoint: 3, compactNumberEntries: 100);

        Assert.Equal(3, wal.GetLastCheckpoint(1));
    }
}
