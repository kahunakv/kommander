using Kommander.Support.Parallelization;
using Microsoft.Extensions.Logging;

namespace Kommander.Tests;

/// <summary>
/// Fault observation for fire-and-forget tasks. Reading <c>task.Exception</c> is what prevents
/// an unobserved exception from re-raising on the finalizer thread (which aborts the process
/// under memory exhaustion); the log line is the operator-visible half. Both halves must hold
/// without ever propagating a failure back to the caller.
/// </summary>
public sealed class TestFireAndForget
{
    [Fact]
    public void Observe_AlreadyFaultedTask_LogsTheFaultWithContext()
    {
        RecordingLogger sink = new();

        FireAndForget.Observe(
            Task.FromException(new InvalidOperationException("disk vanished")),
            sink,
            "TestLoop");

        Assert.Single(sink.Entries);
        Assert.Equal(LogLevel.Error, sink.Entries[0].Level);
        Assert.Contains("TestLoop", sink.Entries[0].Message);
        Assert.Contains("InvalidOperationException", sink.Entries[0].Message);
        Assert.Contains("disk vanished", sink.Entries[0].Message);
    }

    [Fact]
    public void Observe_TaskFaultingLater_ObservesThroughTheContinuation()
    {
        // The continuation is attached while the task is still pending; the fault arrives
        // afterwards. With ExecuteSynchronously the log write happens on the thread that
        // completes the task, so the entry is visible immediately after SetException.
        RecordingLogger sink = new();
        TaskCompletionSource pending = new();

        FireAndForget.Observe(pending.Task, sink, "LaterFault");

        Assert.Empty(sink.Entries);

        pending.SetException(new ArgumentException("late failure"));

        Assert.Single(sink.Entries);
        Assert.Contains("LaterFault", sink.Entries[0].Message);
        Assert.Contains("late failure", sink.Entries[0].Message);
    }

    [Fact]
    public void Observe_CancelledTask_LogsNothingAndDoesNotThrow()
    {
        // OnlyOnFaulted turns the continuation of a cancelled antecedent into a cancelled
        // task holding no exception: cancellation of a fire-and-forget operation is not a fault.
        RecordingLogger sink = new();
        Task cancelled = Task.FromCanceled(new CancellationToken(canceled: true));

        FireAndForget.Observe(cancelled, sink, "CancelledLoop");

        Assert.Empty(sink.Entries);
    }

    [Fact]
    public void Observe_CompletedTask_LogsNothing()
    {
        RecordingLogger sink = new();

        FireAndForget.Observe(Task.CompletedTask, sink, "SuccessLoop");

        Assert.Empty(sink.Entries);
    }

    [Fact]
    public void Observe_NullLogger_DoesNotThrow()
    {
        // Many call sites have no logger in scope; observation must not require one.
        FireAndForget.Observe(
            Task.FromException(new InvalidOperationException("no logger")),
            logger: null,
            context: "NoLoggerLoop");
    }

    [Fact]
    public void Observe_LoggerThatThrows_DoesNotPropagate()
    {
        // Logging under memory exhaustion may itself throw. The exception is already observed
        // by the time the logger runs, so the logger's failure must be swallowed — otherwise
        // observing a fault would crash the very path meant to prevent the crash.
        ThrowingLogger logger = new();

        FireAndForget.Observe(
            Task.FromException(new InvalidOperationException("fault despite broken sink")),
            logger,
            "BrokenSinkLoop");
    }

    [Fact]
    public void Observe_NullLoggerAndTaskFaultingLater_DoesNotThrow()
    {
        TaskCompletionSource pending = new();

        FireAndForget.Observe(pending.Task, logger: null, context: "NoLoggerLaterLoop");

        pending.SetException(new InvalidOperationException("later, still no logger"));
    }

    private sealed record LogEntry(LogLevel Level, string Message);

    private sealed class RecordingLogger : ILogger
    {
        internal List<LogEntry> Entries { get; } = [];

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            Entries.Add(new LogEntry(logLevel, formatter(state, exception)));
        }

        public bool IsEnabled(LogLevel logLevel) => true;

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
    }

    private sealed class ThrowingLogger : ILogger
    {
        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
            => throw new InvalidOperationException("the sink itself is broken");

        public bool IsEnabled(LogLevel logLevel) => true;

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
    }
}
