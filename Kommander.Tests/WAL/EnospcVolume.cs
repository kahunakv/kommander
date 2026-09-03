using System.Diagnostics;

namespace Kommander.Tests.WAL;

/// <summary>
/// A small, genuinely fillable filesystem for ENOSPC tests.
///
/// <para>The RocksDB behaviour under test (a storage error the engine latches and never clears on its
/// own) lives inside native code, so it cannot be simulated through a managed WAL double: the test
/// needs a real volume that really runs out of space. Two sources are supported:</para>
/// <list type="bullet">
///   <item><c>KOMMANDER_ENOSPC_VOLUME</c>: the path of a pre-mounted small filesystem. CI mounts a tmpfs
///   with a small <c>size</c> AND a small <c>nr_inodes</c>; the inode limit matters, because tmpfs still
///   creates an empty file on a block-full volume, and the latched error under test needs file
///   <b>creation</b> to fail.</item>
///   <item>macOS with no variable set: an HFS+ ramdisk is created with <c>hdiutil</c> / <c>diskutil</c>,
///   which needs no root. A block-full HFS+ volume refuses file creation once its catalog needs to
///   grow, which the filler reaches by creating many small files.</item>
/// </list>
/// <para>Anywhere else the tests skip with <see cref="SkipReason"/>.</para>
/// </summary>
internal sealed class EnospcVolume : IDisposable
{
    public const string SkipReason =
        "Needs a small fillable filesystem: set KOMMANDER_ENOSPC_VOLUME to a mounted tmpfs/loopback path, or run on macOS (ramdisk via hdiutil).";

    /// <summary>A fresh directory on the volume for this test's files.</summary>
    public string Root { get; }

    private readonly string? ramDiskDevice;

    private readonly string fillerDirectory;

    private EnospcVolume(string root, string? ramDiskDevice)
    {
        Root = root;
        this.ramDiskDevice = ramDiskDevice;
        fillerDirectory = Path.Combine(root, "filler");
        Directory.CreateDirectory(root);
    }

    /// <summary>
    /// Returns a volume of roughly <paramref name="sizeMiB"/>, or <see langword="null"/> when none can be
    /// provided here. The size must exceed RocksDB's default 64 MiB write buffer: the engine's own
    /// free-space poll refuses to resume below that, and the reopen path must be observed on its own.
    /// </summary>
    public static EnospcVolume? TryCreate(int sizeMiB = 192)
    {
        string? preset = Environment.GetEnvironmentVariable("KOMMANDER_ENOSPC_VOLUME");

        if (!string.IsNullOrEmpty(preset) && Directory.Exists(preset))
            return new EnospcVolume(Path.Combine(preset, $"enospc-{Guid.NewGuid():N}"), ramDiskDevice: null);

        if (OperatingSystem.IsMacOS())
            return TryCreateMacRamDisk(sizeMiB);

        return null;
    }

    private static EnospcVolume? TryCreateMacRamDisk(int sizeMiB)
    {
        // ram:// takes 512-byte sectors.
        string? device = Run("hdiutil", $"attach -nomount ram://{(long)sizeMiB * 2048}")?.Trim();

        if (string.IsNullOrEmpty(device) || !device.StartsWith("/dev/disk", StringComparison.Ordinal))
            return null;

        string name = $"kenospc{Environment.ProcessId}";

        if (Run("diskutil", $"erasevolume HFS+ {name} {device}") is null)
        {
            Run("hdiutil", $"detach {device} -force");
            return null;
        }

        string mount = $"/Volumes/{name}";

        if (!Directory.Exists(mount))
        {
            Run("hdiutil", $"detach {device} -force");
            return null;
        }

        return new EnospcVolume(Path.Combine(mount, "t"), device);
    }

    /// <summary>
    /// Fills the volume until the filesystem refuses writes AND, as far as the filesystem allows, refuses
    /// to create files: one large file up to the first ENOSPC, then small files up to a creation failure
    /// or a cap. The small-file phase is what pushes the volume from "no blocks" to "no new files"
    /// (catalog growth on HFS+, inode exhaustion on an inode-limited tmpfs), which is the condition that
    /// makes RocksDB's next WAL file creation fail.
    /// </summary>
    public void Fill()
    {
        Directory.CreateDirectory(fillerDirectory);

        byte[] chunk = new byte[1 << 20];

        using (FileStream big = new(Path.Combine(fillerDirectory, "big"), FileMode.Create, FileAccess.Write))
        {
            try
            {
                while (true)
                {
                    big.Write(chunk);
                    big.Flush(flushToDisk: true);
                }
            }
            catch (IOException)
            {
                // The volume is out of blocks.
            }
        }

        const int cap = 20_000;

        for (int i = 0; i < cap; i++)
        {
            FileStream small;

            try
            {
                small = new FileStream(Path.Combine(fillerDirectory, $"s{i}"), FileMode.CreateNew, FileAccess.Write);
            }
            catch (IOException)
            {
                // The filesystem refuses to create files: the goal.
                return;
            }

            using (small)
            {
                try
                {
                    small.Write(chunk, 0, 512);
                    small.Flush(flushToDisk: true);
                }
                catch (IOException)
                {
                    // Out of blocks but the file exists; keep it so it consumes an inode / catalog slot.
                }
            }
        }
    }

    /// <summary>Removes the filler so the volume has space again.</summary>
    public void Free()
    {
        if (Directory.Exists(fillerDirectory))
            Directory.Delete(fillerDirectory, recursive: true);
    }

    public long AvailableFreeSpace => new DriveInfo(Root).AvailableFreeSpace;

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(Root))
                Directory.Delete(Root, recursive: true);
        }
        catch (IOException)
        {
            // Best effort: a ramdisk is discarded below anyway.
        }
        catch (UnauthorizedAccessException)
        {
        }

        if (ramDiskDevice is not null)
            Run("hdiutil", $"detach {ramDiskDevice} -force");
    }

    private static string? Run(string fileName, string arguments)
    {
        try
        {
            using Process process = Process.Start(new ProcessStartInfo(fileName, arguments)
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
            })!;

            string output = process.StandardOutput.ReadToEnd();
            process.StandardError.ReadToEnd();

            if (!process.WaitForExit(TimeSpan.FromSeconds(60)) || process.ExitCode != 0)
                return null;

            return output;
        }
        catch (Exception)
        {
            return null;
        }
    }
}
