
using Microsoft.Extensions.Logging;

namespace Kommander.Communication;

/// <summary>
/// Rate-limits the log of one peer's outbound send failures: one Error line when an episode
/// opens, one Warning summary per interval with the counts while it lasts, one Information
/// line when a send succeeds again, and the full text of every failure at Debug.
///
/// <para><b>Why.</b> A dead link fails every frame the dispatcher hands it, and the dispatcher
/// sends a leader's whole replication stream: the 1.13.3 inst3 soak logged about 45,000 lines a
/// minute, each with a stack trace, for 53 minutes from one stuck channel. The flood was the only
/// visible symptom and it buried the lines that said what was wrong. One line per interval keeps
/// the fault attributable from the sender's log without making the log the fault.</para>
///
/// <para>Single-threaded by contract: the endpoint worker that owns an instance calls it from its
/// one send loop (or, in manual mode, from the one caller driving <c>FlushAsync</c>). The clock
/// is injectable so the interval logic is testable without waiting it out.</para>
/// </summary>
internal sealed class TransportFailureEpisode
{
    /// <summary>Default spacing of the summary lines while an episode is open.</summary>
    public static readonly TimeSpan DefaultSummaryInterval = TimeSpan.FromSeconds(10);

    private readonly ILogger logger;
    private readonly string endpoint;
    private readonly long intervalMs;
    private readonly Func<long> clockMs;

    // An explicit flag, not a sentinel clock value: the clock is whatever the host gives
    // (Environment.TickCount64 can be anything, a test clock starts at 0).
    private bool open;

    /// <summary>Clock reading when the open episode started.</summary>
    private long episodeStartMs;

    private long lastSummaryMs;
    private long failuresInEpisode;
    private long failuresSinceSummary;

    public TransportFailureEpisode(ILogger logger, string endpoint, TimeSpan? summaryInterval = null, Func<long>? clockMs = null)
    {
        this.logger = logger;
        this.endpoint = endpoint;
        intervalMs = (long)(summaryInterval ?? DefaultSummaryInterval).TotalMilliseconds;
        this.clockMs = clockMs ?? (static () => Environment.TickCount64);
    }

    /// <summary>True while failures have been seen with no success since.</summary>
    public bool IsOpen => open;

    /// <summary>Failures counted since the open episode started; 0 when none is open.</summary>
    public long FailuresInEpisode => failuresInEpisode;

    /// <summary>
    /// Records one failed send. <paramref name="stage"/> names the loop that failed
    /// (<c>send</c>, <c>flush</c>, <c>drain</c>) so the line reads as the one it replaces.
    /// </summary>
    public void NoteFailure(Exception exception, string stage)
    {
        long now = clockMs();

        if (!open)
        {
            open = true;
            episodeStartMs = now;
            lastSummaryMs = now;
            failuresInEpisode = 1;
            failuresSinceSummary = 0;

            if (logger.IsEnabled(LogLevel.Error))
                logger.LogError(
                    "[RaftTransportDispatcher/{Endpoint}] {Stage} {Type}: {Message}; further failures to this peer are summarised every {IntervalSeconds} s and logged in full at Debug\n{StackTrace}",
                    endpoint, stage, exception.GetType().Name, exception.Message, intervalMs / 1000.0, exception.StackTrace);
            return;
        }

        failuresInEpisode++;
        failuresSinceSummary++;

        if (now - lastSummaryMs >= intervalMs)
        {
            logger.LogWarning(
                "[RaftTransportDispatcher/{Endpoint}] {Stage} still failing: {Recent} failures in the last {RecentSeconds:F0} s, {Total} since the episode opened {EpisodeSeconds:F0} s ago; last {Type}: {Message}",
                endpoint, stage, failuresSinceSummary, (now - lastSummaryMs) / 1000.0, failuresInEpisode, (now - episodeStartMs) / 1000.0, exception.GetType().Name, exception.Message);

            lastSummaryMs = now;
            failuresSinceSummary = 0;
            return;
        }

        if (logger.IsEnabled(LogLevel.Debug))
            logger.LogDebug(
                "[RaftTransportDispatcher/{Endpoint}] {Stage} {Type}: {Message}",
                endpoint, stage, exception.GetType().Name, exception.Message);
    }

    /// <summary>Records a successful send: closes the open episode, if any, with its totals. Free when none is open.</summary>
    public void NoteSuccess()
    {
        if (!open)
            return;

        long now = clockMs();

        logger.LogInformation(
            "[RaftTransportDispatcher/{Endpoint}] sends recovered after {Total} failures over {Seconds:F0} s",
            endpoint, failuresInEpisode, (now - episodeStartMs) / 1000.0);

        open = false;
        episodeStartMs = 0;
        lastSummaryMs = 0;
        failuresInEpisode = 0;
        failuresSinceSummary = 0;
    }
}
