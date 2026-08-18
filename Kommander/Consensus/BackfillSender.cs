using Kommander.Communication.Grpc;
using Kommander.Data;
using Kommander.Logging;
using Kommander.Scheduling;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kommander.Consensus;

/// <summary>
/// The leader's outbound AppendEntries path: enqueueing a batch to one peer, and reading the
/// bounded committed range that brings a lagging peer forward.
///
/// <para><b>Why the saturation check lives here.</b> This type is the single choke point every
/// entry-carrying batch passes through — the heartbeat round, the ack fast-path re-supply, and the
/// forced heartbeat after every leadership publication all funnel into it. The observed saturation
/// storm was driven by election churn rather than by a timer, so a throttle attached to the
/// heartbeat interval would not have caught it.</para>
///
/// <para><b>The anchor-contiguity refusal is load-bearing.</b> An anchored batch asserts, through
/// its <c>(prevLogIndex, prevLogTerm)</c> pair, that its entries immediately follow the anchor.
/// When the WAL read comes back starting above the anchor, no committed entry sits there, and
/// shipping it anyway lands the batch over the follower's gap, advances nothing, and repeats
/// forever with no error surfacing anywhere — the observed Jepsen wedge. Refusing and routing to a
/// snapshot is the correct response; do not "fix" the refusal by relaxing the anchor.</para>
///
/// <para><b>Concurrency.</b> Invoked only on the partition executor thread; holds no locks by
/// design.</para>
/// </summary>
internal sealed class BackfillSender
{
    private readonly IRaftPartitionHost host;
    private readonly IRaftWalFacade wal;
    private readonly RaftPartitionCoreState coreState;
    private readonly ReplicationTracker tracker;
    private readonly NonContiguousBackfillTracker backfillTracker;
    private readonly ILogger<IRaft> logger;

    public BackfillSender(
        IRaftPartitionHost host,
        IRaftWalFacade wal,
        RaftPartitionCoreState coreState,
        ReplicationTracker tracker,
        NonContiguousBackfillTracker backfillTracker,
        ILogger<IRaft> logger)
    {
        this.host = host;
        this.wal = wal;
        this.coreState = coreState;
        this.tracker = tracker;
        this.backfillTracker = backfillTracker;
        this.logger = logger;
    }

    /// <summary>
    /// Appends logs to a specific node in the cluster.
    /// <paramref name="prevLogIndex"/> and <paramref name="prevLogTerm"/> are the Log Matching
    /// anchors: the id and term of the entry immediately preceding the first entry in
    /// <paramref name="logs"/>.  Both default to 0 (no anchor check) for heartbeats and live
    /// proposals where the leader knows the follower is in sync; they are set for backfill batches
    /// so a divergent follower can reject with <see cref="RaftOperationStatus.LogMismatch"/> and
    /// enable leader-side backtracking via <c>nextIndex</c>.
    /// </summary>
    /// <param name="node"></param>
    /// <param name="timestamp"></param>
    /// <param name="logs"></param>
    /// <param name="prevLogIndex">Id of the entry immediately before the first entry in <paramref name="logs"/>; 0 skips the check.</param>
    /// <param name="prevLogTerm">Term of the entry at <paramref name="prevLogIndex"/>; 0 when index is 0.</param>
    public void AppendLogToNode(
        RaftNode node,
        HLCTimestamp timestamp,
        List<RaftLog>? logs,
        long prevLogIndex = 0,
        long prevLogTerm = 0,
        bool quiesce = false,
        AppendLogsGrpcLogCache? grpcLogCache = null)
    {
        AppendLogsRequest request;

        if (logs is null || logs.Count == 0)
            request = new(host.PartitionId, coreState.CurrentTerm, timestamp, host.LocalEndpoint) { Quiesce = quiesce };
        else
        {
            request = new(host.PartitionId, coreState.CurrentTerm, timestamp, host.LocalEndpoint, logs, prevLogIndex, prevLogTerm)
            {
                Quiesce = quiesce,
                GrpcLogCache = grpcLogCache,
            };

            if (logger.IsEnabled(LogLevel.Debug))
                logger.LogDebug(
                    "[{LocalEndpoint}/{PartitionId}/{State}] Enqueued entries for {Endpoint} {Timestamp} PrevLogIndex={PrevLogIndex} Logs={Logs}",
                    host.LocalEndpoint,
                    host.PartitionId,
                    coreState.NodeState,
                    node.Endpoint,
                    timestamp,
                    prevLogIndex,
                    string.Join(',', logs.Select(x => x.Id.ToString()))
                );
        }

        host.EnqueueResponse(node.Endpoint, new(RaftResponderRequestType.AppendLogs, node, request));
    }

    /// <summary>
    /// Reads a bounded committed range for <paramref name="node"/> from the WAL and ships it via
    /// <see cref="AppendLogToNode"/> with the correct Log Matching anchors.
    /// Returns <see cref="BackfillSendResult.Sent"/> when at least one entry was shipped; otherwise
    /// the specific reason nothing was sent, because the causes demand different reactions at the
    /// heartbeat call site: only <see cref="BackfillSendResult.CompactionFloor"/> and the deliberate
    /// <see cref="BackfillSendResult.NonContiguous"/> refusal may fall back to a snapshot transfer,
    /// while <see cref="BackfillSendResult.SaturationPaused"/> must simply wait — a saturated
    /// follower is not below the floor, and shipping it a full snapshot only adds load.
    /// </summary>
    /// <param name="node">The peer to send the batch to.</param>
    /// <param name="followerMaxLog">The highest committed index the leader believes the follower holds;
    /// used as the fallback start when <c>nextIndex</c> has not been backtracked below it.</param>
    /// <param name="timestamp">HLC timestamp to stamp the outbound request.</param>
    /// <param name="anchorToFollowerFrontier">
    /// Ignores <c>nextIndex</c> and starts the batch at <paramref name="followerMaxLog"/> + 1.
    /// Required by the fast-path re-supply of a <b>regressed</b> follower: <c>nextIndex</c> is
    /// derived from the monotonic <c>matchIndex</c> and so still points above the frontier the
    /// follower just reported, which is precisely the range that must be re-shipped.
    /// </param>
    /// <param name="round">
    /// Optional per-round memo (see <see cref="BackfillRoundBatches"/>). When supplied, a range already
    /// read in this heartbeat round is reused instead of being re-read and re-decoded from the WAL for
    /// each follower anchored at the same index — the common shape of a multi-follower catch-up. Pass
    /// <see langword="null"/> from one-off call sites, where there is nothing to share with.
    /// </param>
    public async Task<BackfillSendResult> TrySendBackfillBatchAsync(
        RaftNode node,
        long followerMaxLog,
        HLCTimestamp timestamp,
        bool anchorToFollowerFrontier = false,
        BackfillRoundBatches? round = null)
    {
        // Saturation backoff. This peer refused a batch because its WAL queue was full, and it
        // needs an interval in which to drain before another one arrives. Checked here rather
        // than at the call sites because this is the single choke point every entry-carrying
        // batch passes through — the heartbeat round, the ack fast-path re-supply, and the
        // forced heartbeat that follows every leadership publication all funnel into it. That
        // last one matters: the observed storm was driven by election churn, not by a timer, so
        // a throttle attached to the heartbeat interval would not have caught it.
        if (tracker.IsBackfillPaused(node.Endpoint))
            return BackfillSendResult.SaturationPaused;

        long from = !anchorToFollowerFrontier && tracker.TryGetNextIndex(node.Endpoint, out long ni) && ni <= coreState.LocalCommittedIndex
            ? ni
            : followerMaxLog + 1;

        long prevIdx = from - 1;

        if (round is not null && round.TryGet(from, out BackfillRoundBatches.Batch? cached))
        {
            if (cached!.Logs.Count == 0)
                return cached.EmptyResult;

            logger.LogDebugBackfilling(host.LocalEndpoint, host.PartitionId, coreState.NodeState, cached.Logs.Count, node.Endpoint, from, prevIdx, coreState.LocalCommittedIndex);

            backfillTracker.Clear(node.Endpoint, "a contiguous batch was shipped");
            AppendLogToNode(node, timestamp, cached.Logs, prevIdx, cached.PrevTerm, grpcLogCache: cached.GrpcLogCache);
            return BackfillSendResult.Sent;
        }

        List<RaftLog> backfill = await wal.GetRangeAsync(from, host.Configuration.MaxBackfillEntriesPerRound).ConfigureAwait(false);

        if (backfill.Count == 0)
        {
            // Memoize the empty result as well: every follower anchored here would otherwise repeat the
            // same read before falling through to the snapshot path.
            round?.Add(from, backfill, 0);
            return BackfillSendResult.CompactionFloor;
        }

        // Anchor-contiguity guard: an anchored batch asserts, via (prevIdx, prevTerm), that its
        // entries IMMEDIATELY follow the anchor. The read came back with a first id ABOVE `from`,
        // so no committed entry exists at the anchor — shipping it anchored at from-1 would land it
        // over the follower's gap, advance nothing, and repeat forever with no error anywhere (the
        // observed Jepsen wedge). Two causes produce this identical batch shape and the guard cannot
        // tell them apart: a Proposed run starting exactly at `from` (an inherited range whose
        // commit markers were lost and not yet re-committed — repaired by the inherited-tail
        // re-commit in DrainInheritedAppliesAsync), or a compaction floor above the anchor (the peer
        // is below the floor and only a snapshot can seed it). The reported message must not claim
        // one over the other; it carries the anchor, the first id, and the last checkpoint so a
        // reader can tell. Refuse to ship: NonContiguous deliberately routes the heartbeat path to
        // its snapshot fallback.
        //
        // The condition must stay visible, but "never suppress" was previously implemented as "warn
        // on every heartbeat forever", which buries the signal where the repair never lands.
        // ReportNonContiguousBackfillAsync scopes it to one Warning per episode and keeps the live
        // condition queryable through GetBackfillStatuses — the SnapshotSender discipline, applied
        // to the refusal that routes into it.
        if (backfill[0].Id != from)
        {
            await backfillTracker.ReportAsync(node.Endpoint, from, backfill[0].Id).ConfigureAwait(false);
            round?.Add(from, [], 0, BackfillSendResult.NonContiguous);
            return BackfillSendResult.NonContiguous;
        }

        long prevTerm = prevIdx > 0 ? await wal.GetAnyTermAtAsync(prevIdx).ConfigureAwait(false) : 0;

        BackfillRoundBatches.Batch? shared = round?.Add(from, backfill, prevTerm);

        logger.LogDebugBackfilling(host.LocalEndpoint, host.PartitionId, coreState.NodeState, backfill.Count, node.Endpoint, from, prevIdx, coreState.LocalCommittedIndex);

        backfillTracker.Clear(node.Endpoint, "a contiguous batch was shipped");
        AppendLogToNode(node, timestamp, backfill, prevIdx, prevTerm, grpcLogCache: shared?.GrpcLogCache);
        return BackfillSendResult.Sent;
    }

    /// <summary>
    /// Called when a follower has acknowledged (or rejected) an AppendLogs request.
    /// On <see cref="RaftOperationStatus.Success"/> advances <c>matchIndex</c> and
    /// <c>nextIndex</c> for the peer and immediately ships the next bounded backfill
    /// batch if the follower is still behind, so convergence does not wait a full heartbeat
    /// interval per batch.
    /// On <see cref="RaftOperationStatus.LogMismatch"/> backtracks <c>nextIndex</c> using
    /// <c>max(1, min(nextIndex-1, followerMax+1))</c>, which always steps back
    /// at least one position even when the follower's max equals the anchor we sent.
    /// </summary>
    /// <param name="endpoint"></param>
    /// <param name="timestamp"></param>
    /// <param name="status"></param>
    /// <param name="committedIndex"></param>
    /// <param name="responseTerm">
    /// The term the acknowledging follower stamped on its reply. A follower ACK is only meaningful to
    /// the node that is currently the leader of that term; a delayed ACK from an earlier term must not
    /// repopulate progress/backfill/startCommitIndexes state after a step-down or term change. A value
    /// &lt; 0 means "not set" (legacy / in-process / test callers) and bypasses the fence, mirroring
    /// <see cref="CompleteWalOperationAsync"/>.
    /// </param>
    /// <summary>
    /// True when at least one peer is a voter. Replaces LINQ <c>Any</c> with a plain loop on
    /// paths that run per propose/commit — the capturing lambda allocated a closure per call.
    /// </summary>
    public bool HasVoterPeer()
    {
        IReadOnlyList<RaftNode> nodes = host.Nodes;
        for (int i = 0; i < nodes.Count; i++)
        {
            if (host.IsVoter(nodes[i].Endpoint))
                return true;
        }

        return false;
    }
}
