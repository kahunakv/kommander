using Kommander.WAL;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.WAL;

/// <summary>
/// The Raft hard state of <see cref="SqliteWAL"/> lives in its metadata database. The election path
/// sends a vote only after <see cref="IWAL.PersistHardState"/> returns, so that database must follow
/// <c>syncWrites</c>: a vote that reached a candidate but not the disk lets the node grant the same
/// term twice after a power loss. The same path must also see a storage failure as a status, because
/// an escaping exception abandons the election half-way.
/// </summary>
public sealed class TestSqliteWalHardStateDurability : IDisposable
{
    private const long SynchronousNormal = 1;
    private const long SynchronousFull = 2;

    private readonly string path = Path.Combine(Path.GetTempPath(), $"kommander-sqlite-hard-state-{Guid.NewGuid():N}");

    public void Dispose()
    {
        if (Directory.Exists(path))
            Directory.Delete(path, recursive: true);
    }

    [Theory]
    [InlineData(true, SynchronousFull)]
    [InlineData(false, SynchronousNormal)]
    public void MetadataDatabase_FollowsSyncWrites(bool syncWrites, long expectedMode)
    {
        using SqliteWAL wal = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: syncWrites, shardCount: 1);

        Assert.Equal(expectedMode, wal.GetMetaDataSynchronousModeForTesting());
    }

    [Fact]
    public void PersistHardState_SurvivesReopenWithSyncedMetadata()
    {
        IWAL first = new SqliteWAL(path, "wal", NullLogger<IRaft>.Instance, shardCount: 1);
        Assert.True(first.PersistHardState(1, currentTerm: 7, votedFor: "node-b"));
        ((IDisposable)first).Dispose();

        using SqliteWAL reopened = new(path, "wal", NullLogger<IRaft>.Instance, shardCount: 1);
        IWAL wal = reopened;

        Assert.True(wal.TryGetHardState(1, out long term, out string? votedFor));
        Assert.Equal(7, term);
        Assert.Equal("node-b", votedFor);
    }

    /// <summary>
    /// A storage failure must surface as <see langword="false"/>. The missing table stands in for a
    /// failing disk: it makes the real upsert fail inside SQLite, with no test seam in the WAL.
    /// </summary>
    [Fact]
    public void SetMetaData_WhenStorageRefusesTheWrite_ReturnsFalseAndDoesNotThrow()
    {
        using SqliteWAL wal = new(path, "wal", NullLogger<IRaft>.Instance, shardCount: 1);
        Assert.True(wal.SetMetaData("key", "value"));

        using (SqliteConnection connection = new($"Data Source={path}/raft_metadata_wal.db;Pooling=False"))
        {
            connection.Open();
            using SqliteCommand drop = new("DROP TABLE metadata;", connection);
            drop.ExecuteNonQuery();
        }

        Assert.False(wal.SetMetaData("key", "other"));
        Assert.False(((IWAL)wal).PersistHardState(1, currentTerm: 8, votedFor: "node-c"));
    }
}
