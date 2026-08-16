
namespace Kommander.Server;

/// <summary>WAL backend the server should construct.</summary>
internal enum KommanderWalAdapter
{
    RocksDb,
    Sqlite,
}

/// <summary>
/// The resolved WAL backend and its storage location.
/// </summary>
/// <param name="Adapter">Backend to construct.</param>
/// <param name="Path">Directory the WAL lives in.</param>
/// <param name="Revision">Single path component distinguishing this WAL generation.</param>
/// <param name="UsedMismatchedOptionNames">
/// True when the location came from the other adapter's options because the selected adapter's were
/// empty — a configuration worth warning about, since the flags then do not say what they mean.
/// </param>
internal readonly record struct KommanderWalSelection(
    KommanderWalAdapter Adapter,
    string Path,
    string Revision,
    bool UsedMismatchedOptionNames);

/// <summary>
/// Resolves which WAL backend to build and where it stores its data.
/// </summary>
/// <remarks>
/// <para>
/// Exists because <c>--wal-adapter</c> was parsed and then ignored: the server always constructed
/// RocksDB, and it configured it from the <c>--sqlite-*</c> options, so <c>--rocksdb-wal-path</c>
/// — the flag that looks correct — did nothing at all. Two separate ways for an operator to end up
/// with their data somewhere they did not intend, in the same family of failure as an unsanitized
/// revision: nothing errors, the WAL is simply elsewhere.
/// </para>
/// <para>
/// Separate from <c>Program.cs</c> so the resolution is testable; top-level statements are not.
/// </para>
/// </remarks>
internal static class KommanderWalConfiguration
{
    /// <summary>
    /// Resolves the adapter and its storage location from the parsed options.
    /// </summary>
    /// <remarks>
    /// Falling back to the other adapter's path/revision options is deliberate backwards
    /// compatibility, not sloppiness: every existing deployment configures RocksDB through the
    /// <c>--sqlite-*</c> options, because that is the only thing that worked. Honouring the flag
    /// without the fallback would relocate their WAL on upgrade — the exact failure this is meant to
    /// prevent. The caller warns when the fallback is used so the mismatch is visible and fixable.
    /// </remarks>
    /// <exception cref="RaftException">The adapter name is not recognized.</exception>
    internal static KommanderWalSelection Resolve(
        string? adapter,
        string? rocksDbPath,
        string? rocksDbRevision,
        string? sqlitePath,
        string? sqliteRevision)
    {
        KommanderWalAdapter resolved = ParseAdapter(adapter);

        (string? preferredPath, string? preferredRevision, string? fallbackPath, string? fallbackRevision) =
            resolved == KommanderWalAdapter.RocksDb
                ? (rocksDbPath, rocksDbRevision, sqlitePath, sqliteRevision)
                : (sqlitePath, sqliteRevision, rocksDbPath, rocksDbRevision);

        // Path and revision are decided together: taking the path from one adapter's options and the
        // revision from the other's would compose a location neither flag describes.
        bool mismatched = string.IsNullOrWhiteSpace(preferredPath)
            && !string.IsNullOrWhiteSpace(fallbackPath);

        return new KommanderWalSelection(
            resolved,
            (mismatched ? fallbackPath : preferredPath) ?? string.Empty,
            (mismatched ? fallbackRevision : preferredRevision) ?? string.Empty,
            mismatched);
    }

    /// <summary>
    /// Builds the operator warning for a mismatched configuration, or null when there is nothing to say.
    /// </summary>
    internal static string? DescribeMismatch(KommanderWalSelection selection)
    {
        if (!selection.UsedMismatchedOptionNames)
            return null;

        (string used, string expected) = selection.Adapter == KommanderWalAdapter.RocksDb
            ? ("--sqlite-wal-path/--sqlite-wal-revision", "--rocksdb-wal-path/--rocksdb-wal-revision")
            : ("--rocksdb-wal-path/--rocksdb-wal-revision", "--sqlite-wal-path/--sqlite-wal-revision");

        return $"[Kommander] WARNING: --wal-adapter is {selection.Adapter} but {expected} were not set, "
            + $"so {used} were used instead ('{selection.Path}', revision '{selection.Revision}'). "
            + $"Set {expected} to make the configuration say what it does; this fallback will be removed.";
    }

    private static KommanderWalAdapter ParseAdapter(string? adapter)
    {
        if (string.IsNullOrWhiteSpace(adapter))
            return KommanderWalAdapter.RocksDb;

        return adapter.Trim().ToLowerInvariant() switch
        {
            "rocksdb" => KommanderWalAdapter.RocksDb,
            "sqlite" => KommanderWalAdapter.Sqlite,
            _ => throw new RaftException(
                $"Unknown --wal-adapter value '{adapter}'. Supported adapters: rocksdb, sqlite."),
        };
    }
}
