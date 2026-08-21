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

    /// <summary>
    /// Window inside which a re-open of an already-warned (endpoint, from, firstId) condition logs
    /// at Debug rather than Warning. Episode identity alone did not bound the log volume in the
    /// Caraxes soak (feature d11fd5f9): a wedged peer produced 24,253 Warnings from 4 distinct
    /// conditions, because episode churn (clears, replacements, overlapped reports) re-opened the
    /// same condition thousands of times. The cooldown slides on every open, so a condition that
    /// keeps re-opening warns exactly once, and a condition that stays quiet for the window warns
    /// again on its next occurrence.
    /// </summary>
    private const long RewarnCooldownMs = 10_000;

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
    /// Monotonic tick of the last time each (endpoint, from, firstId) condition was opened as an
    /// episode. Backs the <see cref="RewarnCooldownMs"/> demotion. Entries older than the cooldown
    /// are pruned opportunistically on new opens, so the map stays bounded by the set of conditions
    /// live inside one window.
    /// </summary>
    private readonly ConcurrentDictionary<(string Endpoint, long From, long FirstId), long> recentOpenTicks = new();

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
    /// repeats log at Debug carrying the occurrence count. A re-open of a condition that already
    /// warned inside <see cref="RewarnCooldownMs"/> also logs at Debug: episode identity alone did
    /// not bound the volume under episode churn (see the constant's remarks).</para>
    ///
    /// <para>The last-checkpoint read happens only on the new-episode path, so the diagnostic that
    /// separates the two causes (an uncommitted run at the anchor versus a compaction floor above
    /// it) never becomes a per-heartbeat WAL read.</para>
    /// </summary>
    public async Task ReportAsync(string endpoint, long from, long firstId)
    {
        long nowTicks = host.GetMonotonicTimestamp();

        if (TryRecordRepeat(endpoint, from, firstId, nowTicks))
            return;

        long lastCheckpoint = await wal.GetLastCheckpointAsync().ConfigureAwait(false);

        // Re-check after the await. The checkpoint read runs off the executor thread, so a second
        // report for the same condition can complete its insert while this one is suspended. Without
        // the re-check every overlapped caller misses the lookup, re-inserts, and re-warns — the
        // suppression fails exactly where it matters, under a slow read on a loaded node.
        if (TryRecordRepeat(endpoint, from, firstId, nowTicks))
            return;

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

        if (WasOpenedInsideCooldown(endpoint, from, firstId, nowTicks))
        {
            logger.LogDebugBackfillNonContiguousReopened(
                host.LocalEndpoint, host.PartitionId, coreState.NodeState, endpoint, from, firstId, lastCheckpoint);
            return;
        }

        logger.LogWarnBackfillNonContiguous(
            host.LocalEndpoint, host.PartitionId, coreState.NodeState, endpoint, from, firstId, lastCheckpoint);
    }

    /// <summary>
    /// Records the refusal against an already-open matching episode. Returns <see langword="true"/>
    /// when the open episode for <paramref name="endpoint"/> carries the same
    /// (<paramref name="from"/>, <paramref name="firstId"/>) pair — the repeat path, which logs at
    /// Debug and never re-warns. Called twice by <see cref="ReportAsync"/>: once before and once
    /// after its awaited checkpoint read, because a concurrent report can insert the episode during
    /// the await.
    /// </summary>
    private bool TryRecordRepeat(string endpoint, long from, long firstId, long nowTicks)
    {
        if (!nonContiguousEpisodes.TryGetValue(endpoint, out NonContiguousBackfillEpisode? open)
            || open.From != from || open.FirstId != firstId)
            return false;

        int occurrences = Interlocked.Increment(ref open.Occurrences);
        Volatile.Write(ref open.LastSeenTicks, nowTicks);
        open.LastSeenAt = DateTimeOffset.UtcNow;

        if (logger.IsEnabled(LogLevel.Debug))
            logger.LogDebugBackfillNonContiguousRepeat(
                host.LocalEndpoint, host.PartitionId, coreState.NodeState, endpoint, from, firstId, occurrences);

        return true;
    }

    /// <summary>
    /// Applies the <see cref="RewarnCooldownMs"/> demotion for one episode open, and slides the
    /// condition's window. Returns <see langword="true"/> when the same condition already opened an
    /// episode inside the window, so the caller logs the open at Debug instead of Warning. The
    /// window slides on every open: a condition that keeps re-opening (episode churn on a wedged
    /// peer) therefore warns exactly once, however long the churn lasts.
    /// </summary>
    private bool WasOpenedInsideCooldown(string endpoint, long from, long firstId, long nowTicks)
    {
        long cooldownTicks = RewarnCooldownMs * Stopwatch.Frequency / 1000;

        // Opportunistic prune keeps the map bounded by the conditions live inside one window.
        if (recentOpenTicks.Count > 16)
        {
            foreach (((string, long, long) key, long ticks) in recentOpenTicks)
            {
                if (nowTicks - ticks > cooldownTicks)
                    recentOpenTicks.TryRemove(key, out _);
            }
        }

        bool insideCooldown = recentOpenTicks.TryGetValue((endpoint, from, firstId), out long lastOpen)
                              && nowTicks - lastOpen <= cooldownTicks;
        recentOpenTicks[(endpoint, from, firstId)] = nowTicks;
        return insideCooldown;
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
    /// Closes an open refusal episode for <paramref name="endpoint"/> only when the shipped batch
    /// actually served the episode's anchor, i.e. <paramref name="shippedFrom"/> is at or below it.
    ///
    /// <para>The two backfill anchors diverge on a wedged peer: <c>nextIndex</c> tracks the
    /// monotonic <c>matchIndex</c>, while the refusal anchors at the reported commit frontier. A
    /// contiguous batch shipped from the high <c>nextIndex</c> anchor proves nothing about the
    /// frontier anchor below the compaction floor — the refusal condition still holds, the peer is
    /// still pinned, and closing the episode on that ship makes the status flap and re-opens the
    /// episode (with a fresh Warning) on the very next frontier-anchored attempt. The Caraxes soak
    /// (feature d11fd5f9) showed this shape sustained for hours.</para>
    /// </summary>
    public void ClearIfCovered(string endpoint, long shippedFrom, string reason)
    {
        if (nonContiguousEpisodes.TryGetValue(endpoint, out NonContiguousBackfillEpisode? episode)
            && shippedFrom <= episode.From)
            Clear(endpoint, reason);
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
