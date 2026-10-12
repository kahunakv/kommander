using Kommander.WAL;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.WAL;

/// <summary>
/// Construction-time guards of <see cref="RocksDbWAL"/>: the engine refuses to open a data
/// directory whose persisted format version it cannot interpret (opening it anyway would let a
/// newer binary misread every row of an older layout), and it refuses a tuning that would
/// configure the engine into a degenerate state.
/// </summary>
public sealed class TestRocksDbWALFormatGuards
{
    /// <summary>
    /// A directory whose persisted <c>version</c> metadata differs from the version this binary
    /// writes must fail construction with a message naming both values, rather than opening the
    /// data and silently misinterpreting it. The failing constructor must also close the engine
    /// it opened for the check, so the directory is not left with a live lock.
    /// </summary>
    [Fact]
    public void Reopen_AgainstAnIncompatiblePersistedFormatVersion_FailsFast()
    {
        string path = CreateTempWalPath();

        try
        {
            using (RocksDbWAL wal = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false))
            {
                // What an older binary would have left behind in the same directory.
                Assert.Equal("2.0.0", wal.GetMetaData("version"));
                Assert.True(wal.SetMetaData("version", "1.0.0"));
            }

            InvalidOperationException ex = Assert.Throws<InvalidOperationException>(
                () => new RocksDbWAL(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false));

            Assert.Contains("1.0.0", ex.Message);
            Assert.Contains("2.0.0", ex.Message);
        }
        finally
        {
            DeleteTempWalPath(path);
        }
    }

    /// <summary>
    /// A negative <c>MaxTotalWalSizeFlushUnits</c> is neither a cap nor the documented
    /// "0 = RocksDB default"; it would be applied as a negative byte bound on the alive-log size.
    /// The constructor must reject it before the engine is opened.
    /// </summary>
    [Fact]
    public void Tuning_WithANegativeAliveLogCap_IsRejected()
    {
        string path = CreateTempWalPath();

        try
        {
            RocksDbWalTuning invalid = RocksDbWalTuning.Default with { MaxTotalWalSizeFlushUnits = -1 };

            ArgumentOutOfRangeException ex = Assert.Throws<ArgumentOutOfRangeException>(
                () => new RocksDbWAL(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false, tuning: invalid));

            Assert.Contains("MaxTotalWalSizeFlushUnits", ex.Message);
        }
        finally
        {
            DeleteTempWalPath(path);
        }
    }

    private static string CreateTempWalPath()
    {
        string path = Path.Combine(Path.GetTempPath(), $"kommander-rocksdb-guards-{Guid.NewGuid():N}");
        Directory.CreateDirectory(path);
        return path;
    }

    private static void DeleteTempWalPath(string path)
    {
        if (Directory.Exists(path))
            Directory.Delete(path, recursive: true);
    }
}
