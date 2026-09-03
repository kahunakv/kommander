using Microsoft.Extensions.Logging;

namespace Kommander.Tests.Simulation.Diagnostics;

/// <summary>
/// Captures only the log lines that contain a given substring.
///
/// <para><b>Why a filter and not a log level.</b> The library's diagnostic traces are written at
/// <c>Debug</c> under one category, so raising the level to reach one of them turns on every other
/// one as well. A run of a three-node cluster then produces tens of megabytes, and the line worth
/// reading is somewhere inside it. Every category and level is enabled here, and the substring
/// decides what is kept — so the interesting trace arrives on its own.</para>
///
/// <para><b>Why this is a diagnostic and not part of a test.</b> It exists for the moment a finding
/// needs a value the harness does not expose. A rule that a test asserts on should be read from a
/// view or a snapshot, not scraped out of a log message, because a message is not a contract.</para>
/// </summary>
public sealed class SubstringFileLogger(string substring) : ILoggerProvider
{
    private readonly List<string> lines = [];
    private readonly object gate = new();

    /// <summary>Everything captured so far, oldest first.</summary>
    public IReadOnlyList<string> Lines
    {
        get
        {
            lock (gate)
                return [.. lines];
        }
    }

    /// <summary>Forgets what was captured, so one run's lines do not appear in the next.</summary>
    public void Clear()
    {
        lock (gate)
            lines.Clear();
    }

    public ILogger CreateLogger(string categoryName) => new Sink(this);

    public void Dispose()
    {
    }

    private void Capture(string message)
    {
        if (!message.Contains(substring, StringComparison.Ordinal))
            return;

        lock (gate)
            lines.Add(message);
    }

    private sealed class Sink(SubstringFileLogger owner) : ILogger
    {
        public IDisposable BeginScope<TState>(TState state) where TState : notnull => Empty.Instance;

        // Every level, because the trace being hunted is usually the quietest one in the build.
        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            ArgumentNullException.ThrowIfNull(formatter);

            owner.Capture(formatter(state, exception));
        }

        private sealed class Empty : IDisposable
        {
            public static readonly Empty Instance = new();

            public void Dispose()
            {
            }
        }
    }
}
