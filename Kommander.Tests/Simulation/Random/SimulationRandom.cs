namespace Kommander.Tests.Simulation.Random;

/// <summary>
/// Deterministic PRNG for simulation runs. Every choice is recorded and can be replayed exactly.
/// Uses SplitMix64 so results are stable across platforms for a given seed.
/// </summary>
public sealed class SimulationRandom
{
    private readonly List<SimulationRandomChoice> _recordedChoices = [];
    private readonly Replay.ReplayLogReader? _replayReader;
    private ulong _state;
    private int _step;
    private long _logicalTime;

    public SimulationRandom(ulong seed, Replay.ReplayLogReader? replayReader = null)
    {
        Seed = seed;
        _state = seed;
        _replayReader = replayReader;
    }

    /// <summary>Seed used to initialize this generator.</summary>
    public ulong Seed { get; }

    /// <summary>Choices produced or consumed during this run.</summary>
    public IReadOnlyList<SimulationRandomChoice> RecordedChoices => _recordedChoices;

    /// <summary>Updates step and logical time stamped onto subsequent random choices.</summary>
    public void SetContext(int step, long logicalTime)
    {
        _step = step;
        _logicalTime = logicalTime;
    }

    /// <summary>Returns a deterministic integer in [<paramref name="minInclusive"/>, <paramref name="maxExclusive"/>).</summary>
    public int NextInt(string choiceName, int minInclusive, int maxExclusive)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(choiceName);

        if (minInclusive >= maxExclusive)
            throw new ArgumentOutOfRangeException(nameof(minInclusive), "minInclusive must be less than maxExclusive.");

        int value;
        if (_replayReader is not null)
        {
            value = _replayReader.ConsumeRandomChoice(choiceName, minInclusive, maxExclusive, _step);
        }
        else
        {
            value = minInclusive + (int)(NextUInt64() % (ulong)(maxExclusive - minInclusive));
        }

        _recordedChoices.Add(new SimulationRandomChoice
        {
            Step = _step,
            LogicalTime = _logicalTime,
            ChoiceName = choiceName,
            MinInclusive = minInclusive,
            MaxExclusive = maxExclusive,
            Value = value,
        });

        return value;
    }

    private ulong NextUInt64()
    {
        ulong value = NextSplitMix64();
        _state = NextSplitMix64();
        return value;
    }

    private ulong NextSplitMix64()
    {
        _state += 0x9E3779B97F4A7C15UL;
        ulong z = _state;
        z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9UL;
        z = (z ^ (z >> 27)) * 0x94D049BB133111EBUL;
        return z ^ (z >> 31);
    }
}
