
using Kommander.Communication;
using Microsoft.Extensions.Logging;

namespace Kommander.Tests.Communication;

/// <summary>
/// The rate limit on a peer's send-failure log: one Error when an episode opens, one Warning
/// summary per interval carrying the counts, Debug for everything in between, one Information
/// line when sends recover. Driven on a scripted clock so no interval is waited out.
/// </summary>
public sealed class TestTransportFailureEpisode
{
    private sealed class CapturingLogger : ILogger
    {
        public readonly List<(LogLevel Level, string Message)> Lines = [];

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
            Lines.Add((logLevel, formatter(state, exception)));

        public int Count(LogLevel level) => Lines.Count(l => l.Level == level);
    }

    private static readonly TimeSpan Interval = TimeSpan.FromSeconds(10);

    [Fact]
    public void FirstFailureOpensTheEpisodeAtError_RepeatsInsideTheIntervalGoToDebug()
    {
        CapturingLogger logger = new();
        long now = 1_000;
        TransportFailureEpisode episode = new(logger, "peer:1", Interval, () => now);

        for (int i = 0; i < 50; i++)
        {
            episode.NoteFailure(new InvalidOperationException("link down"), "send");
            now += 100;
        }

        Assert.True(episode.IsOpen);
        Assert.Equal(50, episode.FailuresInEpisode);
        Assert.Equal(1, logger.Count(LogLevel.Error));
        Assert.Equal(0, logger.Count(LogLevel.Warning));
        Assert.Equal(49, logger.Count(LogLevel.Debug));
        Assert.Contains("[RaftTransportDispatcher/peer:1] send InvalidOperationException: link down", logger.Lines[0].Message);
    }

    [Fact]
    public void OneWarningSummaryPerInterval_CarriesTheCounts()
    {
        CapturingLogger logger = new();
        long now = 0;
        TransportFailureEpisode episode = new(logger, "peer:2", Interval, () => now);

        // Opens the episode (Error), then 9 failures a second apart stay inside the interval.
        for (int i = 0; i < 10; i++)
        {
            episode.NoteFailure(new TimeoutException("no route"), "send");
            now += 1_000;
        }

        Assert.Equal(0, logger.Count(LogLevel.Warning));

        // The failure at the interval boundary is summarised.
        episode.NoteFailure(new TimeoutException("no route"), "send");
        Assert.Equal(1, logger.Count(LogLevel.Warning));
        string summary = logger.Lines.Last(l => l.Level == LogLevel.Warning).Message;
        Assert.Contains("still failing: 10 failures in the last 10 s, 11 since the episode opened 10 s ago", summary);
        Assert.Contains("TimeoutException: no route", summary);

        // Another interval, another single summary.
        for (int i = 0; i < 10; i++)
        {
            now += 1_000;
            episode.NoteFailure(new TimeoutException("no route"), "send");
        }

        Assert.Equal(2, logger.Count(LogLevel.Warning));
        Assert.Equal(1, logger.Count(LogLevel.Error));
    }

    [Fact]
    public void SuccessClosesTheEpisodeAtInformation_AndTheNextFailureOpensANewOne()
    {
        CapturingLogger logger = new();
        long now = 0;
        TransportFailureEpisode episode = new(logger, "peer:3", Interval, () => now);

        episode.NoteFailure(new InvalidOperationException("a"), "send");
        now += 500;
        episode.NoteFailure(new InvalidOperationException("b"), "flush");
        now += 2_500;
        episode.NoteSuccess();

        Assert.False(episode.IsOpen);
        Assert.Equal(0, episode.FailuresInEpisode);
        Assert.Equal(1, logger.Count(LogLevel.Information));
        Assert.Contains("sends recovered after 2 failures over 3 s", logger.Lines.Last().Message);

        episode.NoteFailure(new InvalidOperationException("c"), "drain");

        Assert.True(episode.IsOpen);
        Assert.Equal(1, episode.FailuresInEpisode);
        Assert.Equal(2, logger.Count(LogLevel.Error));
        Assert.Contains("drain InvalidOperationException: c", logger.Lines.Last().Message);
    }

    [Fact]
    public void SuccessWithNoOpenEpisode_LogsNothing()
    {
        CapturingLogger logger = new();
        TransportFailureEpisode episode = new(logger, "peer:4", Interval, static () => 0);

        episode.NoteSuccess();
        episode.NoteSuccess();

        Assert.Empty(logger.Lines);
    }
}
