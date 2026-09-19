using Microsoft.Extensions.Logging;

namespace Kommander.Tests.Chaos;

/// <summary>
/// TEMPORARY diagnostic logger for chaos-scenario debugging: appends every log line, with a
/// timestamp, to one shared file. Delete this file (and its wiring in ChaosClusterHarness) when
/// the investigation ends.
/// </summary>
public sealed class TempFileLogger<T> : ILogger<T>
{
    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

    public bool IsEnabled(LogLevel logLevel) => logLevel >= LogLevel.Information;

    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
    {
        if (!IsEnabled(logLevel))
            return;

        string line = $"{DateTime.UtcNow:HH:mm:ss.fff} {logLevel switch { LogLevel.Error => "fail", LogLevel.Warning => "warn", _ => "info" }}: {formatter(state, exception)}";

        lock (TempFileLogger.Gate)
            File.AppendAllText(TempFileLogger.Path, line + Environment.NewLine);
    }
}

/// <summary>
/// Shared state for <see cref="TempFileLogger{T}"/>. It lives on a non-generic type because a
/// static on a generic type is duplicated per closed type: every <c>T</c> would get its own lock
/// and could interleave appends to the one shared file.
/// </summary>
public static class TempFileLogger
{
    internal static readonly object Gate = new();

    public static string Path { get; } =
        Environment.GetEnvironmentVariable("CHAOS_DIAG_LOG")
        ?? global::System.IO.Path.Combine(global::System.IO.Path.GetTempPath(), "chaos-diag.log");
}
