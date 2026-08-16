
using System.Runtime.InteropServices;

namespace Kommander.WAL;

/// <summary>
/// Validation and permission helpers shared by the on-disk WAL backends.
/// </summary>
/// <remarks>
/// Both <see cref="SqliteWAL"/> and <see cref="RocksDbWAL"/> compose their storage location by
/// interpolating an operator-supplied <c>revision</c> into a path. The rules for what may appear
/// there, and the permissions the resulting files get, are properties of "where a WAL lives" rather
/// than of either backend, so they live here — one definition the two cannot drift apart on.
/// </remarks>
internal static class WalStoragePaths
{
    /// <summary>
    /// Validates a <c>revision</c> before it is interpolated into a filesystem path.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The value is operator configuration, never network input, so this is not a remote attack
    /// surface — which is why it is a low-severity concern rather than a traversal vulnerability.
    /// It is still worth rejecting, because the failure is silent: a revision containing a separator
    /// or <c>..</c> places the cluster's entire durable state somewhere outside the configured data
    /// directory, and the node starts up perfectly happily having done so. The operator discovers it
    /// when a volume fills, a backup misses the data, or a restart cannot find the log.
    /// </para>
    /// <para>
    /// Empty is allowed: both backends accept an empty revision today and compose a path from it, so
    /// forbidding it here would break existing deployments for no safety gain.
    /// </para>
    /// </remarks>
    /// <exception cref="RaftException">
    /// The revision contains a directory separator, a relative-path segment, or a character the
    /// platform rejects in a file name.
    /// </exception>
    internal static void ValidateRevision(string? revision, string parameterName)
    {
        if (string.IsNullOrEmpty(revision))
            return;

        if (revision.Contains('/') || revision.Contains('\\'))
        {
            throw new RaftException(
                $"WAL {parameterName} '{revision}' contains a directory separator. The revision is a "
                + "single path component appended to the WAL directory; a separator would place the "
                + "WAL outside it.");
        }

        // Caught explicitly rather than left to the invalid-character check, because "." and ".."
        // are composed entirely of legal filename characters and would otherwise pass.
        if (revision is "." or ".." || revision.Contains(".."))
        {
            throw new RaftException(
                $"WAL {parameterName} '{revision}' contains a relative path segment. The revision is a "
                + "single path component appended to the WAL directory.");
        }

        char[] invalid = Path.GetInvalidFileNameChars();

        foreach (char c in revision)
        {
            if (Array.IndexOf(invalid, c) >= 0)
            {
                throw new RaftException(
                    $"WAL {parameterName} '{revision}' contains a character that is not valid in a file "
                    + $"name (U+{(int)c:X4}).");
            }
        }
    }

    /// <summary>
    /// Creates the WAL directory if needed and restricts it to the current user.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A WAL directory holds the complete replicated state — every application payload ever
    /// committed through Raft. Created under a typical <c>0022</c> umask it is world-readable, so on
    /// a shared host any local user can read the cluster's data at rest.
    /// </para>
    /// <para>
    /// The mode is applied to the <b>directory</b> rather than to individual files on purpose:
    /// SQLite creates <c>-wal</c> and <c>-shm</c> sidecars, and RocksDB creates and rotates many
    /// files of its own, none of which pass through Kommander. A restrictive directory covers
    /// everything either engine puts inside it, now and later.
    /// </para>
    /// <para>
    /// Unix only. Windows governs access through inherited ACLs, which <see cref="UnixFileMode"/>
    /// cannot express and which a default per-user data directory already restricts; attempting to
    /// set a Unix mode there throws.
    /// </para>
    /// <para>
    /// The mode applies at <b>creation</b> only. An existing directory is left as it is, deliberately:
    /// tightening one in place would silently break a deployment that had granted access on purpose
    /// — a backup agent running as another user, for instance. Existing clusters therefore keep their
    /// current permissions until the data directory is recreated, and an operator who wants them
    /// narrowed should <c>chmod</c> it themselves.
    /// </para>
    /// </remarks>
    internal static void EnsureDirectory(string path)
    {
        if (string.IsNullOrWhiteSpace(path))
            return;

        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
            Directory.CreateDirectory(path);
            return;
        }

        // Owner-only: read/write/execute for the user running the node, nothing for group or other.
        Directory.CreateDirectory(path, UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute);
    }
}
