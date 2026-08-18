using System.Collections.Concurrent;
using System.Diagnostics;
using Kommander.Data;
using Kommander.Logging;
using Kommander.Scheduling;
using Microsoft.Extensions.Logging;

namespace Kommander.Consensus;

/// <summary>
/// Tracks the leader's open refusals to ship a follower a non-contiguous anchored backfill batch,
/// and serves them as operator-visible status.
///
/// <para><b>Why episodes rather than occurrences.</b> The refusal condition re-fires every
/// heartbeat for as long as the underlying range stays unrepaired (a peer permanently below the
/// compaction floor never recovers on its own), so logging per attempt produced hundreds of
/// thousands of identical lines and buried the signal. An episode is identified by its
/// <c>(From, FirstId)</c> pair: the first refusal logs at Warning, identical repeats log at Debug
/// with a count, and a changed pair is a genuinely different condition that opens a new episode.
/// The condition is never suppressed — an open episode stays queryable through
/// <see cref="GetStatuses"/> for as long as it keeps firing.</para>
///
/// <para><b>Concurrency.</b> The episode map is a <see cref="ConcurrentDictionary{TKey,TValue}"/>
/// on purpose: the refusal path mutates it on the executor thread while <see cref="GetStatuses"/>
/// serves the operator query off it — the same split <c>SnapshotSender</c>'s failure states live
/// under. Do not "simplify" it to a plain dictionary on the grounds that the partition machine is
/// single-threaded. Every field here is diagnostic, so a benign race yields a slightly stale
/// reading and never a wrong decision; nothing in this type gates behavior.</para>
/// </summary>
internal sealed class NonContiguousBackfillTracker
{
    /// <summary>
    /// How long an unobserved refusal episode is still reported before it is treated as gone.
    /// Generous relative to a heartbeat interval: a live condition re-fires every round, so anything
    /// this quiet has stopped happening.
    /// </summary>
    private const long StaleBackfillEpisodeMs = 60_000;

    private readonly IRaftPartitionHost host;
    private readonly IRaftWalFacade wal;
    private readonly RaftPartitionCoreState coreState;
    private readonly ILogger<IRaft> logger;

    /// <summary>
    /// Open non-contiguous-backfill episodes, keyed by follower endpoint. An entry exists while the
    /// leader is refusing to ship that peer an anchored batch because no committed entry sits at its
    /// anchor.
    /// </summary>
    private readonly ConcurrentDictionary<string, NonContiguousBackfillEpisode> nonContiguousEpisodes = new();

    /// <summary>
    /// One follower's open refusal episode. Identity is the <c>(From, FirstId)</c> pair: while it
    /// holds, repeats are the same episode and log at Debug; a changed pair is a genuinely different
    /// condition and opens a new episode with its own Warning.
    /// </summary>
    private sealed class NonContiguousBackfillEpisode
    {
        public long From;
        public long FirstId;
        public long LastCheckpoint;
        public int Occurrences;
        public long LastSeenTicks;
        public DateTimeOffset FirstSeenAt;
        public DateTimeOffset LastSeenAt;
    }

    public NonContiguousBackfillTracker(
        IRaftPartitionHost host,
        IRaftWalFacade wal,
        RaftPartitionCoreState coreState,
        ILogger<IRaft> logger)
    {
        this.host = host;
        this.wal = wal;
        this.coreState = coreState;
        this.logger = logger;
    }

    /// <summary>
    /// Records one refusal to ship a non-contiguous anchored batch to <paramref name="endpoint"/>
    /// and logs it at <b>episode</b> scope rather than per attempt.
    ///
    /// <para>The first refusal of an episode — and any refusal whose <c>(from, firstId)</c> pair
    /// differs from the open one, i.e. a genuinely changed condition — logs at Warning. Identical
    /// repeats log at Debug carrying the occurrence count.</para>
    ///
    /// <para>The last-checkpoint read happens only on the Warning path, so the diagnostic that
    /// separates the two causes (an uncommitted run at the anchor versus a compaction floor above
    /// it) never becomes a per-heartbeat WAL read.</para>
    /// </summary>
    public async Task ReportAsync(string endpoint, long from, long firstId)
    {
        long nowTicks = host.GetMonotonicTimestamp();

        if (nonContiguousEpisodes.TryGetValue(endpoint, out NonContiguousBackfillEpisode? open)
            && open.From == from && open.FirstId == firstId)
        {
            int occurrences = Interlocked.Increment(ref open.Occurrences);
            Volatile.Write(ref open.LastSeenTicks, nowTicks);
            open.LastSeenAt = DateTimeOffset.UtcNow;

            if (logger.IsEnabled(LogLevel.Debug))
                logger.LogDebugBackfillNonContiguousRepeat(
                    host.LocalEndpoint, host.PartitionId, coreState.NodeState, endpoint, from, firstId, occurrences);

            return;
        }

        long lastCheckpoint = await wal.GetLastCheckpointAsync().ConfigureAwait(false);

        nonContiguousEpisodes[endpoint] = new NonContiguousBackfillEpisode
        {
            From = from,
            FirstId = firstId,
            LastCheckpoint = lastCheckpoint,
            Occurrences = 1,
            LastSeenTicks = nowTicks,
            FirstSeenAt = DateTimeOffset.UtcNow,
            LastSeenAt = DateTimeOffset.UtcNow,
        };

        logger.LogWarnBackfillNonContiguous(
            host.LocalEndpoint, host.PartitionId, coreState.NodeState, endpoint, from, firstId, lastCheckpoint);
    }

    /// <summary>
    /// Closes any open refusal episode for <paramref name="endpoint"/>, logging why it ended.
    /// Recovery is stated rather than left to be inferred from the absence of further warnings —
    /// silence is what the old per-heartbeat logging made unreadable in the first place.
    /// </summary>
    public void Clear(string endpoint, string reason)
    {
        if (!nonContiguousEpisodes.TryRemove(endpoint, out NonContiguousBackfillEpisode? episode))
            return;

        logger.LogInfoBackfillNonContiguousCleared(
            host.LocalEndpoint, host.PartitionId, coreState.NodeState, endpoint,
            episode.From, episode.FirstId, episode.Occurrences, reason);
    }

    /// <summary>
    /// Leader-side non-contiguous-backfill status per follower — see
    /// <see cref="IRaft.GetBackfillStatuses"/>. Safe to call off the executor thread.
    ///
    /// <para>Episodes not seen for <see cref="StaleBackfillEpisodeMs"/> are dropped here rather than
    /// reported: a peer that stopped triggering backfill entirely (seeded another way, removed from
    /// the cluster, or this node stepped down) never reaches the clear path, and a condition that is
    /// no longer being observed must not be reported as live.</para>
    /// </summary>
    public IReadOnlyList<RaftBackfillStatus> GetStatuses()
    {
        if (nonContiguousEpisodes.IsEmpty)
            return [];

        long now = host.GetMonotonicTimestamp();
        long staleTicks = StaleBackfillEpisodeMs * Stopwatch.Frequency / 1000;
        List<RaftBackfillStatus> statuses = [];

        foreach ((string endpoint, NonContiguousBackfillEpisode episode) in nonContiguousEpisodes)
        {
            if (now - Volatile.Read(ref episode.LastSeenTicks) > staleTicks)
            {
                nonContiguousEpisodes.TryRemove(endpoint, out _);
                continue;
            }

            statuses.Add(new RaftBackfillStatus
            {
                FollowerEndpoint = endpoint,
                AnchorIndex = episode.From,
                FirstAvailableIndex = episode.FirstId,
                LastCheckpoint = episode.LastCheckpoint,
                Occurrences = Volatile.Read(ref episode.Occurrences),
                FirstRefusedAt = episode.FirstSeenAt,
                LastRefusedAt = episode.LastSeenAt,
            });
        }

        return statuses;
    }
}
