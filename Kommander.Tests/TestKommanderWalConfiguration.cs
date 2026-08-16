
using Kommander;
using Kommander.Server;

namespace Kommander.Tests;

/// <summary>
/// Covers WAL backend selection: <c>--wal-adapter</c> is honoured, and each adapter reads the
/// options named after it.
/// </summary>
/// <remarks>
/// Both were previously untrue — the server always constructed RocksDB, and configured it from the
/// <c>--sqlite-*</c> options, so <c>--rocksdb-wal-path</c> did nothing whatsoever. The failure mode is
/// the same family as an unsanitized revision: no error, the WAL is simply not where the operator
/// believes it is.
/// </remarks>
public sealed class TestKommanderWalConfiguration
{
    // Expected adapter passed by name: xUnit requires public test methods, and the adapter enum is
    // internal to Kommander.Server.
    [Theory]
    [InlineData("rocksdb", "RocksDb")]
    [InlineData("RocksDb", "RocksDb")]
    [InlineData("  sqlite ", "Sqlite")]
    [InlineData("SQLITE", "Sqlite")]
    public void Resolve_HonoursTheAdapterFlag(string adapter, string expected)
    {
        KommanderWalSelection selection = KommanderWalConfiguration.Resolve(
            adapter, "/rocks", "r1", "/sqlite", "s1");

        Assert.Equal(expected, selection.Adapter.ToString());
    }

    /// <summary>
    /// An unset adapter keeps today's behaviour rather than failing, so existing command lines that
    /// never passed the flag continue to work.
    /// </summary>
    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void Resolve_DefaultsToRocksDb(string? adapter)
    {
        Assert.Equal(
            "RocksDb",
            KommanderWalConfiguration.Resolve(adapter, "/rocks", "r1", "/sqlite", "s1").Adapter.ToString());
    }

    [Fact]
    public void Resolve_RejectsUnknownAdapter()
    {
        RaftException exception = Assert.Throws<RaftException>(
            () => KommanderWalConfiguration.Resolve("leveldb", "/rocks", "r1", "/sqlite", "s1"));

        Assert.Contains("leveldb", exception.Message);
        Assert.Contains("--wal-adapter", exception.Message);
    }

    [Fact]
    public void Resolve_UsesTheOptionsNamedAfterTheAdapter()
    {
        KommanderWalSelection rocks = KommanderWalConfiguration.Resolve(
            "rocksdb", "/rocks", "r1", "/sqlite", "s1");

        Assert.Equal("/rocks", rocks.Path);
        Assert.Equal("r1", rocks.Revision);
        Assert.False(rocks.UsedMismatchedOptionNames);

        KommanderWalSelection sqlite = KommanderWalConfiguration.Resolve(
            "sqlite", "/rocks", "r1", "/sqlite", "s1");

        Assert.Equal("/sqlite", sqlite.Path);
        Assert.Equal("s1", sqlite.Revision);
        Assert.False(sqlite.UsedMismatchedOptionNames);
    }

    /// <summary>
    /// The compatibility case, and the reason the fallback exists: every deployment predating this
    /// change configures RocksDB through the sqlite-named options, because that was the only thing
    /// the server read. Honouring the flag without this would relocate their WAL on upgrade.
    /// </summary>
    [Fact]
    public void Resolve_FallsBackToLegacyOptionNames_WhenTheAdaptersOwnAreUnset()
    {
        KommanderWalSelection selection = KommanderWalConfiguration.Resolve(
            "rocksdb", rocksDbPath: "", rocksDbRevision: "", sqlitePath: "/app/data", sqliteRevision: "v2");

        Assert.Equal("/app/data", selection.Path);
        Assert.Equal("v2", selection.Revision);
        Assert.True(selection.UsedMismatchedOptionNames);
    }

    /// <summary>
    /// Path and revision come from the same source. Mixing them would compose a location neither
    /// flag describes — the worst possible outcome for a change about knowing where the data is.
    /// </summary>
    [Fact]
    public void Resolve_TakesPathAndRevisionFromTheSameSource()
    {
        KommanderWalSelection selection = KommanderWalConfiguration.Resolve(
            "rocksdb", rocksDbPath: "", rocksDbRevision: "ignored", sqlitePath: "/app/data", sqliteRevision: "v2");

        Assert.Equal("/app/data", selection.Path);
        Assert.Equal("v2", selection.Revision);
    }

    [Fact]
    public void Resolve_DoesNotFallBack_WhenTheAdaptersOwnPathIsSet()
    {
        KommanderWalSelection selection = KommanderWalConfiguration.Resolve(
            "rocksdb", rocksDbPath: "/rocks", rocksDbRevision: "", sqlitePath: "/app/data", sqliteRevision: "v2");

        Assert.Equal("/rocks", selection.Path);
        Assert.Equal(string.Empty, selection.Revision);
        Assert.False(selection.UsedMismatchedOptionNames);
    }

    [Fact]
    public void DescribeMismatch_NamesBothSetsOfFlags()
    {
        KommanderWalSelection selection = KommanderWalConfiguration.Resolve(
            "rocksdb", "", "", "/app/data", "v2");

        string? warning = KommanderWalConfiguration.DescribeMismatch(selection);

        Assert.NotNull(warning);
        Assert.Contains("--rocksdb-wal-path", warning);
        Assert.Contains("--sqlite-wal-path", warning);
        Assert.Contains("/app/data", warning);
    }

    [Fact]
    public void DescribeMismatch_IsSilentForAConsistentConfiguration()
    {
        KommanderWalSelection selection = KommanderWalConfiguration.Resolve(
            "rocksdb", "/rocks", "r1", "/sqlite", "s1");

        Assert.Null(KommanderWalConfiguration.DescribeMismatch(selection));
    }
}
