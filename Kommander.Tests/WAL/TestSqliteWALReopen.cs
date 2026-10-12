using Kommander.Data;
using Kommander.WAL;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.WAL;

/// <summary>
/// Crash-restart and shard-configuration edges of <see cref="SqliteWAL"/>: everything an operator
/// relies on when a process dies and a fresh instance opens the same data directory — per-shard log
/// rows, the persisted replay floor and cluster metadata must all reconstruct from the shard and
/// metadata database files, and a partition wiped before the restart must not resurrect state (or a
/// stale checkpoint row) for the next occupant of its id. Also pins the shard-count resolution rules
/// for a fresh directory and the fail-fast on a corrupted persisted count.
/// </summary>
public sealed class TestSqliteWALReopen
{
    /// <summary>
    /// A close/reopen of the same data directory must reconstruct every partition's log from its
    /// shard file, the recorded last-checkpoint (the replay floor), and the cluster metadata — for
    /// partitions spread across different shards, since each shard is a separate file and connection.
    /// </summary>
    [Fact]
    public void ReopenAfterDispose_RecoversLogsCheckpointsAndMetadataAcrossShards()
    {
        string path = CreateTempWalPath();

        try
        {
            using (SqliteWAL wal = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false, shardCount: 3))
            {
                // Partitions 1, 2 and 3 map to shards 1, 2 and 0 — three distinct shard files.
                Assert.Equal(RaftOperationStatus.Success, wal.Write([(1, [Log(1), Log(2), Log(3)])]));
                Assert.Equal(RaftOperationStatus.Success, wal.Write([(2, [Log(1), Log(2), Log(3), Log(4), Checkpoint(5), Log(6)])]));
                Assert.Equal(RaftOperationStatus.Success, wal.Write([(3, [Checkpoint(1), Log(2)])]));

                Assert.Equal(5, wal.GetLastCheckpoint(2));
                Assert.Equal([5L, 6L], wal.ReadLogs(2).Select(l => l.Id));
                Assert.True(wal.SetMetaData("cluster-name", "reopen-test"));
            }

            // shardCount=0 accepts the persisted count, so the reopening instance routes partitions
            // to exactly the shard files the first instance wrote.
            using SqliteWAL reopened = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false, shardCount: 0);

            Assert.Equal([1L, 2L, 3L], reopened.ReadLogs(1).Select(l => l.Id));
            Assert.Equal(3, reopened.GetMaxLog(1));

            // The persisted replay floor survived: restore still starts at the checkpoint entry.
            Assert.Equal([5L, 6L], reopened.ReadLogs(2).Select(l => l.Id));
            Assert.Equal(5, reopened.GetLastCheckpoint(2));
            Assert.Equal(6, reopened.GetMaxLog(2));

            Assert.Equal([1L, 2L], reopened.ReadLogs(3).Select(l => l.Id));
            Assert.Equal(1, reopened.GetLastCheckpoint(3));

            Assert.Equal("reopen-test", reopened.GetMetaData("cluster-name"));

            // The reopened instance keeps appending above the recovered tails.
            Assert.Equal(RaftOperationStatus.Success, reopened.Write([(1, [Log(4)])]));
            Assert.Equal(4, reopened.GetMaxLog(1));
            Assert.Equal([1L, 2L, 3L, 4L], reopened.ReadLogs(1).Select(l => l.Id));
        }
        finally
        {
            DeleteTempWalPath(path);
        }
    }

    /// <summary>
    /// A partition wipe must be durable across a restart even though its shard file survives (the
    /// shard-mate still lives in it): after reopen the deleted partition reads empty, reports no
    /// checkpoint, and a reused id starts from id 1 without inheriting the previous occupant's
    /// replay floor.
    /// </summary>
    [Fact]
    public void DeletePartitionWAL_SurvivesReopen_DeletedPartitionStartsEmptyWithoutStaleCheckpoint()
    {
        string path = CreateTempWalPath();

        try
        {
            using (SqliteWAL wal = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false, shardCount: 1))
            {
                // Both partitions share the single shard file.
                Assert.Equal(RaftOperationStatus.Success, wal.Write([(10, [Log(1), Log(2), Checkpoint(3)])]));
                Assert.Equal(RaftOperationStatus.Success, wal.Write([(20, [Log(1), Checkpoint(2), Log(3)])]));

                Assert.Equal(RaftOperationStatus.Success, wal.DeletePartitionWAL(10));
            }

            using SqliteWAL reopened = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false, shardCount: 1);

            Assert.Empty(reopened.ReadLogs(10));
            Assert.Equal(0, reopened.GetMaxLog(10));
            Assert.Equal(-1, reopened.GetLastCheckpoint(10));

            Assert.Equal([2L, 3L], reopened.ReadLogs(20).Select(l => l.Id));
            Assert.Equal(2, reopened.GetLastCheckpoint(20));
            Assert.Equal(3, reopened.GetMaxLog(20));

            // The reused partition id writes from low ids again and its fresh rows are all visible:
            // a stale recorded checkpoint would have hidden them behind a phantom replay floor.
            Assert.Equal(RaftOperationStatus.Success, reopened.Write([(10, [Log(1), Log(2)])]));
            Assert.Equal([1L, 2L], reopened.ReadLogs(10).Select(l => l.Id));
            Assert.Equal(-1, reopened.GetLastCheckpoint(10));
        }
        finally
        {
            DeleteTempWalPath(path);
        }
    }

    /// <summary>
    /// A fresh directory resolves a zero (unset) or negative shard count to
    /// <see cref="Environment.ProcessorCount"/> and persists that value, so later opens agree on the
    /// partition-to-shard routing without any caller passing an explicit count.
    /// </summary>
    [Fact]
    public void FreshDirectory_ZeroOrNegativeShardCount_ResolvesToProcessorCount()
    {
        string zeroPath = CreateTempWalPath();
        string negativePath = CreateTempWalPath();

        try
        {
            using (SqliteWAL wal = new(zeroPath, "wal", NullLogger<IRaft>.Instance, syncWrites: false, shardCount: 0))
                Assert.Equal(Environment.ProcessorCount.ToString(), wal.GetMetaData("shard_count"));

            using (SqliteWAL wal = new(negativePath, "wal", NullLogger<IRaft>.Instance, syncWrites: false, shardCount: -7))
                Assert.Equal(Environment.ProcessorCount.ToString(), wal.GetMetaData("shard_count"));
        }
        finally
        {
            DeleteTempWalPath(zeroPath);
            DeleteTempWalPath(negativePath);
        }
    }

    /// <summary>
    /// A persisted shard_count that is not a positive integer must fail construction with a message
    /// naming the offending stored value, rather than mapping partitions through an unparsable count.
    /// </summary>
    [Fact]
    public void CorruptedPersistedShardCount_FailsFastNamingTheStoredValue()
    {
        string path = CreateTempWalPath();

        try
        {
            using (SqliteWAL wal = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false, shardCount: 2))
                wal.Write([(1, [Log(1)])]);

            // Corrupt the persisted count directly in the metadata database, the way a torn write or
            // an operator edit would.
            using SqliteConnection connection = new($"Data Source={Path.Combine(path, "raft_metadata_wal.db")};Pooling=False");
            connection.Open();
            using SqliteCommand corrupt = new("UPDATE metadata SET value = 'not-a-number' WHERE key = 'shard_count'", connection);
            Assert.Equal(1, corrupt.ExecuteNonQuery());

            InvalidOperationException ex = Assert.Throws<InvalidOperationException>(
                () => new SqliteWAL(path, "wal", NullLogger<IRaft>.Instance));

            Assert.Contains("not-a-number", ex.Message);
            Assert.Contains("shard_count", ex.Message);
        }
        finally
        {
            DeleteTempWalPath(path);
        }
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    private static RaftLog Log(long id, long term = 1) =>
        new() { Id = id, Term = term, Type = RaftLogType.Committed, LogType = "reopen", LogData = [1, 2, 3] };

    private static RaftLog Checkpoint(long id, long term = 1) =>
        new() { Id = id, Term = term, Type = RaftLogType.CommittedCheckpoint, LogType = "checkpoint" };

    private static string CreateTempWalPath()
    {
        string path = Path.Combine(Path.GetTempPath(), $"kommander-sqlite-reopen-{Guid.NewGuid():N}");
        Directory.CreateDirectory(path);
        return path;
    }

    private static void DeleteTempWalPath(string path)
    {
        try
        {
            if (Directory.Exists(path))
                Directory.Delete(path, recursive: true);
        }
        catch
        {
            // Best-effort cleanup: a connection the constructor leaked while failing fast can hold
            // the metadata file open on platforms that lock deleted files.
        }
    }
}
