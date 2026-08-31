using System.Globalization;

namespace Kommander.Tests.Simulation.Scenarios.Random;

/// <summary>
/// The seeds a random search always runs, and the sweep it runs beside them.
///
/// <para><b>Why keep a corpus at all.</b> A pure sweep explores a different set of schedules every
/// night, which is what a search should do, and means a schedule that once found a defect is never
/// run again. The corpus is the other half: the seeds that earned their place stay in the run
/// forever, so a regression in a state the harness already reached is caught by the same seed that
/// reached it.</para>
///
/// <para>The file is the source of record because a reason can be written beside a seed there. The
/// compiled list is a fallback for a run whose working directory does not carry the file.</para>
/// </summary>
public static class RandomSeedCorpus
{
    /// <summary>Name of the corpus file, copied beside the test binary.</summary>
    public const string FileName = "seed-corpus.txt";

    /// <summary>Environment variable naming how many swept seeds to run beyond the corpus.</summary>
    public const string SeedCountVariable = "KOMMANDER_DST_SEED_COUNT";

    /// <summary>Environment variable naming where the sweep starts.</summary>
    public const string SeedBaseVariable = "KOMMANDER_DST_SEED_BASE";

    /// <summary>Swept seeds a run performs when the environment says nothing.</summary>
    public const int DefaultSeedCount = 2;

    /// <summary>Where the sweep starts when the environment says nothing.</summary>
    public const ulong DefaultSeedBase = 20260901;

    /// <summary>The corpus as compiled in, used when the file cannot be read.</summary>
    public static IReadOnlyList<ulong> Builtin { get; } =
        [20260901, 20260902, 20260903, 20260904, 7, 999];

    /// <summary>
    /// Reads the corpus file beside the test binary, falling back to <see cref="Builtin"/>.
    /// </summary>
    public static IReadOnlyList<ulong> Load()
    {
        string path = Path.Combine(AppContext.BaseDirectory, FileName);

        if (!File.Exists(path))
            return Builtin;

        List<ulong> seeds = [];

        foreach (string line in File.ReadAllLines(path))
        {
            string text = line.Trim();

            if (text.Length == 0 || text.StartsWith('#'))
                continue;

            // A comment may follow the seed on its own line, which is where the reason lives.
            string token = text.Split('#', 2)[0].Trim();

            if (ulong.TryParse(token, NumberStyles.None, CultureInfo.InvariantCulture, out ulong seed))
                seeds.Add(seed);
        }

        return seeds.Count > 0 ? seeds : Builtin;
    }

    /// <summary>
    /// How many swept seeds this run should use. A nightly job raises it; a pull request leaves it
    /// alone.
    /// </summary>
    public static int ConfiguredSeedCount()
    {
        string? text = Environment.GetEnvironmentVariable(SeedCountVariable);

        return int.TryParse(text, NumberStyles.None, CultureInfo.InvariantCulture, out int count) && count >= 0
            ? count
            : DefaultSeedCount;
    }

    /// <summary>Where this run's sweep starts. Changing it moves the whole sweep somewhere new.</summary>
    public static ulong ConfiguredSeedBase()
    {
        string? text = Environment.GetEnvironmentVariable(SeedBaseVariable);

        return ulong.TryParse(text, NumberStyles.None, CultureInfo.InvariantCulture, out ulong seed)
            ? seed
            : DefaultSeedBase;
    }

    /// <summary>
    /// Spreads <paramref name="count"/> seeds out from <paramref name="baseSeed"/>.
    ///
    /// <para>Mixed rather than counted upwards. Adjacent seeds fed to the same generator produce
    /// schedules that begin the same way, so a sweep of consecutive numbers explores far less than
    /// its length suggests.</para>
    /// </summary>
    public static IReadOnlyList<ulong> Sweep(ulong baseSeed, int count)
    {
        List<ulong> seeds = [];
        ulong state = baseSeed;

        for (int index = 0; index < count; index++)
        {
            state += 0x9E3779B97F4A7C15UL;

            ulong mixed = state;
            mixed = (mixed ^ (mixed >> 30)) * 0xBF58476D1CE4E5B9UL;
            mixed = (mixed ^ (mixed >> 27)) * 0x94D049BB133111EBUL;
            mixed ^= mixed >> 31;

            seeds.Add(mixed);
        }

        return seeds;
    }
}
