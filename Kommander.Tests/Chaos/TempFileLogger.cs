using Microsoft.Extensions.Logging;

namespace Kommander.Tests.Chaos;

/// <summary>
/// TEMPORARY diagnostic logger for chaos-scenario debugging: appends every log line, with a
/// timestamp, to one shared file. Delete this file (and its wiring in ChaosClusterHarness) when
/// the investigation ends.
/// </summary>
public sealed class TempFileLogger<T> : ILogger<T>
{
    private static readonly object Gate = new();

    public static string Path { get; } =
        Environment.GetEnvironmentVariable("CHAOS_DIAG_LOG")
        ?? global::System.IO.Path.Combine(global::System.IO.Path.GetTempPath(), "chaos-diag.log");

    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

    public bool IsEnabled(LogLevel logLevel) => logLevel >= LogLevel.Information;

    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
    {
        if (!IsEnabled(logLevel))
            return;

        string line = $"{DateTime.UtcNow:HH:mm:ss.fff} {logLevel switch { LogLevel.Error => "fail", LogLevel.Warning => "warn", _ => "info" }}: {formatter(state, exception)}";

        lock (Gate)
            File.AppendAllText(Path, line + Environment.NewLine);
    }
}
