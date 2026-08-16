
using System.Runtime.InteropServices;
using Kommander;
using Kommander.WAL;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.WAL;

/// <summary>
/// Covers the two storage-hygiene rules the on-disk WAL backends share: a <c>revision</c> must be a
/// single path component, and the data directory must not be world-readable.
/// </summary>
/// <remarks>
/// Neither is a remote attack surface — the revision is operator configuration, not network input.
/// Both matter because their failure is silent: a revision containing a separator puts the cluster's
/// durable state outside the configured directory and the node starts fine, and a default umask
/// leaves every committed payload readable by any local user on a shared host.
/// </remarks>
public sealed class TestWalStoragePaths
{
    [Theory]
    [InlineData("v1")]
    [InlineData("v2-rc1")]
    [InlineData("2026_08_16")]
    [InlineData("")]          // empty is accepted: both backends compose a path from it today
    [InlineData(null)]
    public void ValidateRevision_AcceptsSinglePathComponents(string? revision)
    {
        WalStoragePaths.ValidateRevision(revision, "revision");
    }

    [Theory]
    [InlineData("../escape")]
    [InlineData("..")]
    [InlineData(".")]
    [InlineData("a/b")]
    [InlineData("a\\b")]
    [InlineData("/absolute")]
    [InlineData("v1/../../etc")]
    public void ValidateRevision_RejectsAnythingThatWouldLeaveTheDirectory(string revision)
    {
        RaftException exception = Assert.Throws<RaftException>(
            () => WalStoragePaths.ValidateRevision(revision, "revision"));

        // The message must name the offending value; "invalid revision" alone leaves the operator
        // guessing which of several configured revisions is at fault.
        Assert.Contains(revision, exception.Message);
    }

    [Fact]
    public void ValidateRevision_RejectsNullCharacter()
    {
        Assert.Throws<RaftException>(() => WalStoragePaths.ValidateRevision("v1\0evil", "revision"));
    }

    /// <summary>
    /// Both on-disk backends must reject at construction, not at first write: the point is to fail
    /// while the operator is still reading the startup output.
    /// </summary>
    [Theory]
    [InlineData("../escape")]
    [InlineData("a/b")]
    public void RocksDbWal_RejectsTraversalRevisionAtConstruction(string revision)
    {
        Assert.Throws<RaftException>(
            () => new RocksDbWAL("/tmp/kommander-test", revision, NullLogger<IRaft>.Instance));
    }

    [Theory]
    [InlineData("../escape")]
    [InlineData("a/b")]
    public void SqliteWal_RejectsTraversalRevisionAtConstruction(string revision)
    {
        Assert.Throws<RaftException>(
            () => new SqliteWAL("/tmp/kommander-test", revision, NullLogger<IRaft>.Instance));
    }

    /// <summary>
    /// A newly created WAL directory is owner-only. Applied to the directory rather than to files
    /// because SQLite's <c>-wal</c>/<c>-shm</c> sidecars and RocksDB's rotating files never pass
    /// through Kommander — only the containing directory covers them all.
    /// </summary>
    [Fact]
    public void EnsureDirectory_CreatesOwnerOnlyDirectory()
    {
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            return; // Windows uses inherited ACLs; UnixFileMode does not apply.

        string path = Path.Combine(Path.GetTempPath(), $"kommander-wal-{Guid.NewGuid():N}");

        try
        {
            WalStoragePaths.EnsureDirectory(path);

            Assert.True(Directory.Exists(path));

            UnixFileMode mode = File.GetUnixFileMode(path);

            Assert.Equal(
                UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute,
                mode);

            // The property that matters: nothing for group or other.
            Assert.False(mode.HasFlag(UnixFileMode.GroupRead));
            Assert.False(mode.HasFlag(UnixFileMode.OtherRead));
        }
        finally
        {
            if (Directory.Exists(path))
                Directory.Delete(path, recursive: true);
        }
    }

    /// <summary>
    /// An existing directory is left alone. Tightening one in place would silently break a deployment
    /// that had granted access deliberately — a backup agent running as another user, say.
    /// </summary>
    [Fact]
    public void EnsureDirectory_LeavesAnExistingDirectoryAlone()
    {
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            return;

        string path = Path.Combine(Path.GetTempPath(), $"kommander-wal-{Guid.NewGuid():N}");

        try
        {
            Directory.CreateDirectory(path);

            UnixFileMode permissive =
                UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute
                | UnixFileMode.GroupRead | UnixFileMode.GroupExecute;

            File.SetUnixFileMode(path, permissive);

            WalStoragePaths.EnsureDirectory(path);

            Assert.Equal(permissive, File.GetUnixFileMode(path));
        }
        finally
        {
            if (Directory.Exists(path))
                Directory.Delete(path, recursive: true);
        }
    }

    [Fact]
    public void EnsureDirectory_IsIdempotent()
    {
        string path = Path.Combine(Path.GetTempPath(), $"kommander-wal-{Guid.NewGuid():N}");

        try
        {
            WalStoragePaths.EnsureDirectory(path);
            WalStoragePaths.EnsureDirectory(path);

            Assert.True(Directory.Exists(path));
        }
        finally
        {
            if (Directory.Exists(path))
                Directory.Delete(path, recursive: true);
        }
    }
}
