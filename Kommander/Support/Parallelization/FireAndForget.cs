
using Microsoft.Extensions.Logging;

namespace Kommander.Support.Parallelization;

/// <summary>
/// Observes the exception of a deliberately fire-and-forget task.
///
/// <para><b>Why this exists.</b> A faulted task whose <c>Exception</c> is never read is re-raised
/// by <c>TaskExceptionHolder</c> on the finalizer thread. Raising it there allocates (the
/// <c>AggregateException</c> wrap and the event args), and under memory exhaustion that secondary
/// allocation throws <c>OutOfMemoryException</c> out of the finalizer — which aborts the whole
/// process with SIGABRT. The Caraxes run Q leader died exactly this way: the OOM itself was
/// survivable, the unobserved-task finalization was not. Every <c>_ = SomethingAsync()</c> in this
/// library must route through <see cref="Observe"/> so a fault degrades into a log line instead of
/// a process abort.</para>
/// </summary>
internal static class FireAndForget
{
    /// <summary>
    /// Attaches a fault observer to <paramref name="task"/>. Reading <c>task.Exception</c> marks
    /// the exception observed, which is the load-bearing effect; the log write is best-effort and
    /// may itself fail under memory exhaustion without consequence.
    /// </summary>
    /// <param name="task">The task whose outcome nobody awaits.</param>
    /// <param name="logger">Sink for the fault report; may be <see langword="null"/> when no logger is in scope.</param>
    /// <param name="context">Short caller identity for the fault report, e.g. <c>"AutoRejoinLoop"</c>.</param>
    internal static void Observe(Task task, ILogger? logger, string context)
    {
        if (task.IsCompleted)
        {
            ObserveCompleted(task, logger, context);
            return;
        }

        // OnlyOnFaulted: a successful or cancelled antecedent turns this continuation into a
        // cancelled task, which holds no exception and never reaches the finalizer path itself.
        task.ContinueWith(
            static (t, state) =>
            {
                (ILogger? logger, string context) s = ((ILogger?, string))state!;
                ObserveCompleted(t, s.logger, s.context);
            },
            (logger, context),
            CancellationToken.None,
            TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);
    }

    private static void ObserveCompleted(Task task, ILogger? logger, string context)
    {
        AggregateException? exception = task.Exception; // the read is what marks it observed

        if (exception is null)
            return;

        try
        {
            logger?.LogError(
                "[FireAndForget/{Context}] {Type}: {Message}",
                context, exception.InnerException?.GetType().Name ?? exception.GetType().Name,
                exception.InnerException?.Message ?? exception.Message);
        }
        catch
        {
            // Logging under memory exhaustion may throw; the exception is already observed.
        }
    }
}
