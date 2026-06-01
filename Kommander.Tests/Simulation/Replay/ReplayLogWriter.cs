using System.IO;
using System.Text.Json;
using System.Text.Json.Serialization;
using Kommander.Tests.Simulation.Random;

namespace Kommander.Tests.Simulation.Replay;

/// <summary>
/// Appends simulation replay records as JSON lines.
/// </summary>
public sealed class ReplayLogWriter : IDisposable
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    private readonly StreamWriter _writer;
    private readonly SimulationScenario _scenario;
    private bool _disposed;

    public ReplayLogWriter(string path, SimulationScenario scenario)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);
        ArgumentNullException.ThrowIfNull(scenario);

        _scenario = scenario;
        string fullPath = Path.GetFullPath(path);
        string? directory = Path.GetDirectoryName(fullPath);
        if (!string.IsNullOrEmpty(directory))
            Directory.CreateDirectory(directory);

        _writer = new StreamWriter(fullPath, append: false);
        FilePath = fullPath;
    }

    public string FilePath { get; }

    public void WriteRandomChoice(SimulationRandomChoice choice) =>
        WriteEntry(ReplayLogEntry.ForRandomChoice(_scenario, choice));

    public void WriteEventSelection(
        int step,
        long logicalTime,
        int enabledEventCount,
        SimulationEvent selectedEvent,
        string snapshotHashBefore,
        string snapshotHashAfter) =>
        WriteEntry(ReplayLogEntry.ForEventSelection(
            _scenario,
            step,
            logicalTime,
            enabledEventCount,
            selectedEvent,
            snapshotHashBefore,
            snapshotHashAfter));

    public void WriteEntry(ReplayLogEntry entry)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        string line = JsonSerializer.Serialize(entry, JsonOptions);
        _writer.WriteLine(line);
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        _writer.Dispose();
        _disposed = true;
    }
}
