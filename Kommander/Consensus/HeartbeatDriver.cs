using System.Diagnostics;
using Kommander.Communication.Grpc;
using Kommander.Data;
using Kommander.Diagnostics;
using Kommander.Logging;
using Kommander.Scheduling;
using Kommander.System;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kommander.Consensus;

/// <summary>
/// The leader's periodic outbound beat, and the handshake that seeds a peer's log position.
///
/// <para><b>A heartbeat is not just liveness.</b> It is also the only catch-up path once writes go
/// quiet: the live-propose broadcast is one-shot, so a follower that missed a committed tail entry
/// is repaired here or not at all. That is why the round carries two backfill triggers — a
/// threshold-gated one for an actively-behind peer, and a single-entry one that fires only once
/// live replication is quiet AND a commit exists above this term's floor, which keeps a leader from
/// pushing merely-restored state while still healing a genuine tail gap.</para>
///
/// <para>Proposal retry rides the beat for the same reason: Raft leaders retry replication until it
/// succeeds, and a partition with unresolved proposals can never quiesce, so this site is always
/// reached while anything needs retrying.</para>
///
/// <para><b>Concurrency.</b> Invoked only on the partition executor thread; holds no locks by
/// design. The snapshot fallback it kicks off is the one path that continues on a background
/// thread, and it reports back through the executor.</para>
/// </summary>
internal sealed class HeartbeatDriver
{
    private readonly IRaftPartitionHost host;
    private readonly IRaftWalFacade wal;
    private readonly RaftPartitionCoreState coreState;
    private readonly ReplicationTracker tracker;
    private readonly ProposalRegistry proposals;
    private readonly BackfillSender sender;
    private readonly RaftPartitionLogThrottle logThrottle;
    private readonly ILogger<IRaft> logger;

    public HeartbeatDriver(
        IRaftPartitionHost host,
        IRaftWalFacade wal,
        RaftPartitionCoreState coreState,
        ReplicationTracker tracker,
        ProposalRegistry proposals,
        BackfillSender sender,
        RaftPartitionLogThrottle logThrottle,
        ILogger<IRaft> logger)
    {
        this.host = host;
        this.wal = wal;
        this.coreState = coreState;
        this.tracker = tracker;
        this.proposals = proposals;
        this.sender = sender;
        this.logThrottle = logThrottle;
        this.logger = logger;
    }

    /// <summary>
    /// Sends a heartbeat message to follower nodes to indicate that the leader node in the partition is still alive.
    /// </summary>
    /// <param name="force"></param>
    /// <exception cref="RaftException"></exception>
    public async Task SendHeartbeat(bool force)
    {
        if (!force && heartbeatsSuspendedForTesting)
            return;
        if (!force && coreState.Quiesced)
            return;

        IReadOnlyList<RaftNode> nodes = host.Nodes;

        if (nodes.Count == 0)
        {
            logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] No other nodes availables to send hearthbeat", host.LocalEndpoint, host.PartitionId, coreState.NodeState);
            return;
        }

        HLCTimestamp prevHeartbeat = coreState.LastHeartbeat;
        coreState.LastHeartbeat = host.HybridLogicalClock.TrySendOrLocalEvent(host.LocalNodeId);
        long nowTicks = host.GetMonotonicTimestamp();
        coreState.LastHeartbeatTicks = nowTicks;

        // "Live replication is quiet": no proposal has been issued for at least one heartbeat
        // interval (or we have never proposed as this leader). While writes are flowing, a follower
        // that trails by a few entries is simply mid-flight on the live-propose broadcast and will
        // converge on its own, so the small-gap backfill below stays disabled to avoid redundant WAL
        // reads. Once writes pause, that live path can no longer heal a residual tail gap — a follower
        // that missed the final committed entry (e.g. it was briefly unreachable at commit time) would
        // otherwise stay permanently behind, because empty heartbeats carry no entries and the
        // threshold-gated backfill never fires for a sub-threshold gap.
        bool liveReplicationQuiet = coreState.LastProposalAtTicks == 0
            || RaftMonotonic.Elapsed(coreState.LastProposalAtTicks, nowTicks) >= host.Configuration.HeartbeatInterval;

        if (coreState.NodeState != RaftNodeState.Leader && coreState.NodeState != RaftNodeState.Candidate)
            return;

        // Raft leaders retry replication until it succeeds; the live-propose broadcast alone is
        // one-shot. Riding the heartbeat keeps the retry paced, and a partition with unresolved
        // proposals can never quiesce (the quiesce gate requires activeProposals empty), so this
        // site is always reached while anything needs retrying.
        if (coreState.NodeState == RaftNodeState.Leader)
            proposals.RetryUnresolved(coreState.LastHeartbeat);

        TagList heartbeatTags = new() { { "partition_id", host.PartitionId } };
        KommanderMetrics.HeartbeatsSentTotal.Add(1, heartbeatTags);

        if (prevHeartbeat != HLCTimestamp.Zero)
            KommanderMetrics.HeartbeatDelayMs.Record(
                (coreState.LastHeartbeat - prevHeartbeat).TotalMilliseconds, heartbeatTags);

        // Shared across this round's followers: lagging peers are usually anchored at the same index,
        // so the range is read from the WAL (and Protobuf-encoded) once instead of once per follower.
        // Allocated lazily — a healthy round backfills nobody.
        BackfillRoundBatches? backfillRound = null;

        foreach (RaftNode node in nodes)
        {
            if (node.Endpoint == host.LocalEndpoint)
                throw new RaftException("Corrupted nodes");

            if (host.PartitionId != RaftSystemConfig.SystemPartition && !force)
            {
                HLCTimestamp lastHearthBeatToNode = host.GetLastNodeHearthbeat(node.Endpoint, host.PartitionId);

                if (lastHearthBeatToNode != HLCTimestamp.Zero && ((coreState.LastHeartbeat - lastHearthBeatToNode) <= host.Configuration.RecentHeartbeat))
                    continue;
            }

            host.UpdateLastHeartbeat(node.Endpoint, host.PartitionId, coreState.LastHeartbeat);

            // Backfill: ship up to MaxBackfillEntriesPerRound committed entries instead of an empty
            // heartbeat so the follower converges without waiting for new writes.
            // TrySendBackfillBatchAsync handles nextIndex selection and the Log Matching anchors.
            //
            // Two triggers:
            //   * gap > BackfillThreshold — an actively-behind follower (join catch-up, long
            //     partition) is streamed forward regardless of write activity.
            //   * gap >= 1 && liveReplicationQuiet && a live commit exists above coreState.LiveCommitFloor —
            //     once writes pause, even a single missed tail entry must be re-shipped explicitly;
            //     the live-propose broadcast is done and empty heartbeats can never deliver it. The
            //     coreState.LiveCommitFloor guard confines this to entries committed during this term: a leader
            //     does not push merely-restored committed state to a follower until a new write occurs
            //     (that is the highest-WAL election-preference contract). Gating on quiet also keeps
            //     steady-state writes free of the per-heartbeat WAL read a healthy in-flight follower
            //     would otherwise incur.
            // coreState.LocalCommittedIndex is in-memory and always reflects only durably committed entries.
            //
            // Non-voter peers (a placement Learner, a roster Learner on a legacy range) are exempt
            // from the coreState.LiveCommitFloor confinement: the floor exists so a leader does not push
            // merely-RESTORED committed state at voters, whose logs were populated under the previous
            // leadership and whose divergence the highest-WAL election preference arbitrates. A
            // learner starts empty and is explicitly expected to receive this range's full log —
            // without the exemption, a learner added to an idle range whose leader was elected after
            // the last write (frontier == floor) with a gap at or under BackfillThreshold was never
            // shipped anything, and the placement promotion driver waited on its lag forever.
            // followerMaxLog can be -1: the seed recorded for a contacted peer whose Success acks
            // carry no frontier report (the legacy two-fsync heartbeat ack always reports -1).
            // That value means "position unknown", not "position 0" — deriving the gap and the
            // backfill anchor from it shipped a batch anchored at 0, which a compacted WAL can
            // never serve contiguously, and the refusal then escalated to a full snapshot for a
            // peer that may be perfectly in sync (the Caraxes "anchored at 1" storm was this
            // shape, produced on the ack fast path). Substitute the best positional evidence the
            // leader holds — where the peer's log started per its handshake/vote, advanced by
            // every ack that carried a max — so an in-sync peer measures no gap and a genuinely
            // behind one is backfilled from a position it actually holds. A peer with no evidence
            // at all falls back to 0, whose anchored read either ships from entry 1 or refuses
            // into the snapshot rescue — the correct outcome for a blank follower.
            bool frontierKnown = tracker.TryGetCommitFrontier(node.Endpoint, out long followerMaxLog);
            long effectiveFloor = followerMaxLog >= 0
                ? followerMaxLog
                : Math.Max(tracker.GetStartCommitIndexOrDefault(node.Endpoint, 0), 0);
            long followerGap = frontierKnown ? coreState.LocalCommittedIndex - effectiveFloor : 0;
            bool idleTailGap = followerGap > 0 && liveReplicationQuiet
                && (coreState.LocalCommittedIndex > coreState.LiveCommitFloor || !host.IsVoter(node.Endpoint));

            // Crash-restart re-supply (paced): CompleteAppendLogsAsync recorded that this peer reported a
            // committed frontier below its recorded matchIndex (lost lazy markers on restart). The repair
            // runs here, once per heartbeat, rather than inline on every ack — the inline form livelocked
            // the cluster under load. Anchor at the recorded frontier (nextIndex tracks the monotonic
            // matchIndex and still points ABOVE the regressed range, so it would skip exactly what
            // regressed). The note is cleared whether or not a batch went out: if the peer is still
            // behind, its next ack re-records it; if the WAL read came back empty (compacted past the
            // frontier), the snapshot fallback below takes over.
            bool regressed = tracker.TryTakeRegressedFrontier(node.Endpoint, out long regressedFrontier);

            // BackfillEnabled short-circuits ALL three triggers. BackfillThreshold gates only the
            // first, so a consumer that raised it to int.MaxValue meaning "off" still got backfill
            // from idleTailGap the moment writes paused — the inverse of what an idle node needs.
            // The snapshot fallback below is inside this branch by design: "no backfill" also means
            // no repeated, always-discarded partition-state exports for a peer nobody is catching up.
            bool willBackfill = coreState.NodeState == RaftNodeState.Leader
                && host.Configuration.BackfillEnabled
                && coreState.LocalCommittedIndex >= 0
                && (followerGap > host.Configuration.BackfillThreshold || idleTailGap || regressed);

            // DIAGNOSTIC (the numbered findings live in the Jepsen harness
            // repository at ~/kommander-jepsen/FINDINGS.md, not in this one): records every input to
            // the decision above, so a run in which replicas stop advancing can be read for *why*
            // the leader sent nothing rather than inferred from its silence. `followerMaxLog` is the
            // interesting one — it is the leader's belief about the peer, and every trigger here is
            // derived from it. The probe logs at Debug, and drops idle no-op rounds before its own
            // per-second throttle — see RaftPartitionLogThrottle.LogBackfillDecision.
            logThrottle.LogBackfillDecision(node.Endpoint, willBackfill, followerMaxLog, followerGap,
                                idleTailGap, regressed, liveReplicationQuiet);

            if (willBackfill)
            {
                long anchorFrom = regressed ? regressedFrontier : effectiveFloor;
                backfillRound ??= new();
                BackfillSendResult backfillResult = await sender.TrySendBackfillBatchAsync(
                    node, anchorFrom, coreState.LastHeartbeat, anchorToFollowerFrontier: regressed, round: backfillRound).ConfigureAwait(false);
                if (backfillResult == BackfillSendResult.Sent)
                    continue;

                // Nothing was shipped. The reaction by cause happens inside
                // TrySendBackfillBatchAsync itself: a compaction-floor or non-contiguous refusal
                // escalates to a snapshot transfer there, and a saturation pause waits. The
                // escalation used to live here, which left every other caller of the sender — the
                // ack fast-path re-supply in particular — refusing without ever escalating: the
                // Caraxes soak wedged a 3-voter cluster permanently that way.
            }

            sender.AppendLogToNode(node, coreState.LastHeartbeat, null);
        }
    }

    /// <summary>
    /// True when at least one peer is known — or not yet known — to hold less than this leader's
    /// committed frontier. Gates quiescence, on both entry (the idle check) and re-arm (the
    /// quiesced leader's periodic tick).
    ///
    /// <para><b>Why this gate is load-bearing.</b> Quiescing stops <see cref="SendHeartbeat"/>, and
    /// <see cref="SendHeartbeat"/> hosts the <em>only</em> catch-up path (both the
    /// <c>BackfillThreshold</c> stream and the idle-tail-gap re-ship). A leader that quiesces while a
    /// follower or learner is still behind therefore strands that peer permanently: empty heartbeats
    /// stop, no propose broadcast is coming on an idle partition, and nothing else will ever ship the
    /// missing entries. The partition goes silent with every executor idle and never converges — the
    /// exact signature of the hang this guard fixes. Only quiesce once every peer has demonstrably
    /// reached our frontier.</para>
    ///
    /// <para>A peer with no recorded progress counts as lagging <b>unconditionally</b> — even on a
    /// partition with nothing committed. An earlier version short-circuited on
    /// <c>LocalCommittedIndex &lt;= 0</c> to keep an empty partition quiescible, but that made every
    /// peer invisible to this gate: a placement-added Learner joining an idle, never-written range
    /// whose leader had already quiesced was never contacted, so the leader's progress table never
    /// gained an entry for it, the placement promotion driver (which reads that table) measured
    /// "never acked" forever, and the range stayed transitional until the decommission drain timed
    /// out (the Jepsen <c>register/placement</c> finding). First contact is required from every peer
    /// before quiescing; the empty partition still quiesces one ack round-trip later, because a
    /// contacted peer's reported frontier (clamped at 0 — a fresh peer's seed report is −1) is never
    /// below an empty leader's frontier of 0.</para>
    /// </summary>
    public bool HasLaggingPeer()
    {
        foreach (RaftNode node in host.Nodes)
        {
            if (node.Endpoint == host.LocalEndpoint)
                continue;

            if (!tracker.TryGetCommitFrontier(node.Endpoint, out long peerCommittedIndex)
                || Math.Max(0, peerCommittedIndex) < coreState.LocalCommittedIndex)
                return true;
        }

        return false;
    }

    /// <summary>
    /// Broadcasts a quiesce-flagged empty AppendLogs to all peers, signalling them to switch from
    /// the heartbeat timer to SWIM-based election gating.  Called once when the leader decides
    /// to suppress per-partition heartbeats for an idle partition.
    /// </summary>
    public void SendQuiesceMarker(HLCTimestamp timestamp)
    {
        foreach (RaftNode node in host.Nodes)
        {
            if (node.Endpoint == host.LocalEndpoint)
                throw new RaftException("Corrupted nodes");
            sender.AppendLogToNode(node, timestamp, null, quiesce: true);
        }
    }

    /// <summary>
    /// After the partition startup a handshake is sent to the other nodes to
    /// verify if we have the most recent logs and the node id is unique
    /// </summary>
    /// <param name="remoteNodeId"></param>
    /// <param name="endpoint"></param>
    /// <param name="remoteMaxLogId"></param>
    public void ReceiveHandshake(int remoteNodeId, string endpoint, long remoteMaxLogId)
    {
        // Membership fence: handshakes are best-effort (droppable, re-sent) and a joiner is a
        // committed Learner before its partitions start, so a non-member's handshake can be safely
        // ignored. Checked before the NodeId-collision exit so an unadmitted node with a duplicated
        // NodeId cannot kill a cluster member's process, and before startCommitIndexes so a
        // non-member never pollutes step-down target selection.
        if (!host.IsMember(endpoint))
        {
            logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Ignoring Handshake from non-member {Endpoint} NodeId={NodeId}", host.LocalEndpoint, host.PartitionId, coreState.NodeState, endpoint, remoteNodeId);
            return;
        }

        if (host.LocalNodeId == remoteNodeId)
        {
            logger.LogCritSameNodeId(host.LocalEndpoint, host.PartitionId, coreState.NodeState, host.LocalNodeId, remoteNodeId);

            Environment.Exit(1);
            return;
        }

        logger.LogInfoReceivedHandshake(host.LocalEndpoint, host.PartitionId, coreState.NodeState, endpoint, remoteNodeId, remoteMaxLogId);

        tracker.SetStartCommitIndex(endpoint, remoteMaxLogId);
    }

    /// <summary>
    /// Sends a handshake to every node available in the cluster to verify if we have the most recent logs.
    /// </summary>
    /// <exception cref="RaftException"></exception>
    public async Task SendHandshakeAsync()
    {
        IReadOnlyList<RaftNode> nodes = host.Nodes;
        
        if (nodes.Count == 0)
        {
            logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] No other nodes availables to send handshake", host.LocalEndpoint, host.PartitionId, coreState.NodeState);
            return;
        }
        
        long localMaxId = await wal.GetMaxLogAsync().ConfigureAwait(false);
        
        HandshakeRequest request = new(host.LocalNodeId, host.PartitionId, localMaxId, host.LocalEndpoint);
        
        int number = 0;
        
        foreach (RaftNode node in nodes)
        {
            if (node.Endpoint == host.LocalEndpoint)
                throw new RaftException("Corrupted nodes");
            
            logger.LogDebugSendingHandshake(host.LocalEndpoint, host.PartitionId, coreState.NodeState, node.Endpoint, ++number);
            
            host.EnqueueResponse(node.Endpoint, new(RaftResponderRequestType.Handshake, node, request));
        }
    }

    // B3: monotonic local-clock shadows of the HLC duration anchors above. Every elapsed-time GATE
    // (follower election timeout, leader heartbeat interval, voting timeout, quiesce-after, votation
    // back-off, the pre-vote "is our leader still fresh" check) measures against these ticks instead of
    // subtracting HLC timestamps — HLC subtraction is frozen by a remote peer's clock skew and stalls
    // elections. The HLC fields are retained ONLY where a timestamp is stamped onto the wire / WAL for
    // ordering. A value of 0 means "unset" (mirrors HLCTimestamp.Zero); Stopwatch.GetTimestamp never
    // returns 0 in practice, so the sentinel is unambiguous.
    private bool heartbeatsSuspendedForTesting;

    /// <summary>
    /// Test-only: suppresses unforced heartbeats so a test can hold a partition silent without
    /// stopping its executor. <c>force: true</c> deliberately bypasses it — the read-index round and
    /// the post-promotion heartbeat are correctness paths, not timer-driven ones.
    /// </summary>
    public void SetHeartbeatsSuspendedForTesting(bool value) => heartbeatsSuspendedForTesting = value;
}
