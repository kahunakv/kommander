using System.Buffers;
using System.Diagnostics;
using Kommander.Communication.Grpc;
using Kommander.Consensus;
using Kommander.Data;
using Kommander.Diagnostics;
using Kommander.Gossip;
using Kommander.Logging;
using Kommander.Scheduling;
using Kommander.Support.Collections;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL.Data;
using Microsoft.Extensions.Logging;

namespace Kommander;

/// <summary>
/// Plain Raft partition state machine extracted from <see cref="RaftStateActor"/>.
/// Has no Nixie dependency and can be instantiated directly in tests.
/// </summary>
public sealed class RaftPartitionStateMachine
{
    private readonly IRaftPartitionHost host;
    private readonly IRaftWalFacade wal;
    private readonly IRaftOperationReplySink replySink;
    private readonly ILogger<IRaft> logger;












    /// <summary>
    /// Encapsulates in-flight snapshot-send guard, chunked send loop, and
    /// install-complete callback. Initialized in constructor after <c>postToExecutor</c>
    /// is wired so the sender can always read the current value via the closure.
    /// </summary>
    private readonly SnapshotSender snapshotSender;

    /// <summary>
    /// Posts a <see cref="RaftRequest"/> back to the partition executor from a background thread
    /// so completions such as <see cref="RaftRequestType.SnapshotInstalled"/> can update state
    /// under the single-owner guarantee without introducing re-entrancy.
    /// Set by <see cref="RaftPartition"/> at construction time; <see langword="null"/> in unit tests.
    /// </summary>
    private Action<RaftRequest>? postToExecutor;


    /// <summary>
    /// The consensus nucleus — term, role, commit/apply frontiers, quiescence, promotion barrier.
    /// Shared with every extracted collaborator; see <see cref="RaftPartitionCoreState"/> for why
    /// this is the one piece of state that is shared rather than owned.
    /// </summary>
    private readonly RaftPartitionCoreState coreState = new();

    /// <summary>Rate-limited diagnostic logging for the three storm-prone conditions on this
    /// partition's hot paths; see <see cref="RaftPartitionLogThrottle"/>.</summary>
    private readonly RaftPartitionLogThrottle logThrottle;

    /// <summary>Open non-contiguous-backfill refusal episodes for this leader's followers;
    /// see <see cref="NonContiguousBackfillTracker"/>.</summary>
    private readonly NonContiguousBackfillTracker backfillTracker;

    /// <summary>Read-index leadership confirmation and check-quorum bookkeeping;
    /// see <see cref="ReadIndexCoordinator"/>.</summary>
    private readonly ReadIndexCoordinator readIndex;

    /// <summary>Per-follower replication progress, reported commit frontiers, and repair notes;
    /// see <see cref="ReplicationTracker"/>.</summary>
    private readonly ReplicationTracker tracker;

    /// <summary>Follower-side snapshot install (Raft Rule 7); see
    /// <see cref="SnapshotInstaller"/>.</summary>
    private readonly SnapshotInstaller snapshotInstaller;

    /// <summary>In-flight proposals, client tickets and pending WAL operations;
    /// see <see cref="ProposalRegistry"/>.</summary>
    private readonly ProposalRegistry proposals;

    /// <summary>Pre-vote, election, and the vote-granting side of RequestVote;
    /// see <see cref="ElectionCoordinator"/>.</summary>
    private readonly ElectionCoordinator election;

    /// <summary>Committed-entry delivery to the consumer and its ordering rules;
    /// see <see cref="LogApplicator"/>.</summary>
    private readonly LogApplicator applier;

    /// <summary>Outbound AppendEntries and bounded catch-up reads; see
    /// <see cref="BackfillSender"/>.</summary>
    private readonly BackfillSender sender;

    /// <summary>The periodic leader beat, its backfill triggers, and the handshake;
    /// see <see cref="HeartbeatDriver"/>.</summary>
    private readonly HeartbeatDriver heartbeats;

    /// <summary>The follower side of AppendEntries; see <see cref="FollowerAppendHandler"/>.</summary>
    private readonly FollowerAppendHandler followerAppend;

    /// <summary>Leader-side handling of follower acks; see <see cref="ReplicationAckProcessor"/>.</summary>
    private readonly ReplicationAckProcessor ackProcessor;

    /// <summary>Leader write entry points (propose/checkpoint/commit/rollback);
    /// see <see cref="LogReplicator"/>.</summary>
    private readonly LogReplicator replicator;

    /// <summary>Routes durable WAL completions back to the operation that enqueued them;
    /// see <see cref="WalCompletionRouter"/>.</summary>
    private readonly WalCompletionRouter walCompletions;























    /// <summary>
    /// Externally visible node state (served to <c>GetNodeState</c>, which backs the
    /// <c>AmILeader</c> fallback path). Reports <see cref="RaftNodeState.Candidate"/> while this
    /// node has won an election but has not yet published leadership (promotion barrier pending):
    /// the raw <c>NodeState</c> is already <c>Leader</c> so replication acks and heartbeats work,
    /// but leaking <c>Leader</c> here would reopen the inherited-entry serving hole that gating
    /// <see cref="IRaftPartitionHost.Leader"/> closes — <c>AmILeaderQuick</c> treats a
    /// <c>Leader</c> state reply as authoritative.
    /// <para>Safe to read from any thread: <see cref="RaftPartitionCoreState.NodeState"/> is volatile-published and
    /// <see cref="IRaftPartitionHost.Leader"/> is a volatile reference, so an off-thread reader
    /// sees a recent (possibly one-transition-stale) role but never a torn or resurrected one.
    /// The <c>Leader != LocalEndpoint</c> demotion to <c>Candidate</c> is what makes the
    /// off-thread read safe to expose: a role read that races ahead of the leadership
    /// publication point degrades to <c>Candidate</c>, never to a premature <c>Leader</c>.</para>
    /// </summary>
    public RaftNodeState NodeState =>
        coreState.NodeState == RaftNodeState.Leader && host.Leader != host.LocalEndpoint
            ? RaftNodeState.Candidate
            : coreState.NodeState;
    public long CurrentTerm => coreState.CurrentTerm;

    /// <summary>
    /// Test-only: snapshots this partition's consensus state into an immutable <see cref="RaftPartitionView"/>.
    /// Runs on the executor's single-writer thread (dispatched via <see cref="RaftRequestType.GetPartitionView"/>),
    /// so all mutable fields are read consistently and never observed torn by a polling thread. The WAL max
    /// index is read through the facade on the same thread.
    /// </summary>
    public async Task<RaftPartitionView> GetPartitionView()
    {
        long maxWal = await wal.GetMaxLogAsync().ConfigureAwait(false);
        return new RaftPartitionView(
            Endpoint: host.LocalEndpoint,
            Partition: host.PartitionId,
            Role: coreState.NodeState,
            Term: coreState.CurrentTerm,
            Leader: host.Leader,
            CommitIndex: wal.GetCommitIndex(),
            LastAppliedIndex: coreState.LastAppliedIndex,
            MaxWalIndex: maxWal,
            Quiesced: coreState.Quiesced,
            MemberRole: host.LocalRole);
    }

    /// <summary>
    /// The current election timeout for this partition. Exposed so callers with access to a seeded
    /// configuration can verify reproducibility without depending on wall-clock behaviour.
    /// </summary>
    public TimeSpan ElectionTimeout => coreState.ElectionTimeout;


    public RaftPartitionStateMachine(
        IRaftPartitionHost host,
        IRaftWalFacade wal,
        IRaftOperationReplySink replySink,
        ILogger<IRaft> logger)
    {
        this.host = host;
        this.wal = wal;
        this.replySink = replySink;
        this.logger = logger;

        // Collaborators are constructed in dependency order: state-owning leaves first, then the
        // ones that compose them. Everything here is called ON the executor thread and shares
        // `coreState`; nothing takes a reference back to this machine, so where a collaborator needs
        // core behaviour (promotion, step-down, heartbeat) it receives a named delegate instead.
        tracker = new ReplicationTracker(host);
        logThrottle = new RaftPartitionLogThrottle(host, coreState, logger);
        backfillTracker = new NonContiguousBackfillTracker(host, wal, coreState, logger);
        sender = new BackfillSender(host, wal, coreState, tracker, backfillTracker, logger);
        proposals = new ProposalRegistry(host, coreState, logger, (node, ticket, logs) => sender.AppendLogToNode(node, ticket, logs));

        snapshotSender = new SnapshotSender(
            host,
            logger,
            () => coreState.NodeState,
            () => postToExecutor,
            (endpoint, idx) =>
            {
                tracker.AdvanceCommitFrontier(endpoint, idx);
            });

        heartbeats = new HeartbeatDriver(host, wal, coreState, tracker, proposals, sender, logThrottle, snapshotSender, logger);
        readIndex = new ReadIndexCoordinator(host, coreState, replySink, logger, heartbeats.SendHeartbeat);
        applier = new LogApplicator(host, wal, coreState, proposals, readIndex, logger);
        snapshotInstaller = new SnapshotInstaller(host, wal, coreState, logger, AdoptLeaderAsync);
        election = new ElectionCoordinator(host, wal, coreState, tracker, logger, BecomeLeaderAsync, FailAllActiveProposalWaiters, heartbeats.SendHeartbeat);
        followerAppend = new FollowerAppendHandler(host, wal, coreState, proposals, logThrottle, replySink, logger, AdoptLeaderAsync);
        ackProcessor = new ReplicationAckProcessor(host, wal, coreState, tracker, proposals, readIndex, sender, election, logThrottle, logger, FailAllActiveProposalWaiters);
        replicator = new LogReplicator(host, wal, coreState, proposals, sender, replySink, logger);
        walCompletions = new WalCompletionRouter(host, wal, coreState, proposals, applier, sender, replySink, logger, RevertUnpublishedPromotionAsync);
    }

    /// <summary>
    /// Wires the callback used to post messages back to the partition executor from background
    /// tasks (e.g., <see cref="RaftRequestType.SnapshotInstalled"/>).  Called once by
    /// <see cref="RaftPartition"/> immediately after the executor is created.
    /// </summary>
    public void SetPostToExecutor(Action<RaftRequest> post) => postToExecutor = post;

    /// <summary>
    /// Wires the quiesce-state-change callback.  Called once by <see cref="RaftPartition"/>
    /// at construction time so the manager's hot set stays in sync.
    /// </summary>
    internal void SetOnQuiesceChanged(Action<bool> callback) => coreState.OnQuiesceChanged = callback;


    private void CompleteReply(ulong? correlationId, RaftResponse response)
    {
        if (correlationId is not null)
            replySink.TryComplete(correlationId.Value, response);
    }


    /// <summary>
    /// Phase 1 of the nonblocking restore.  Initialises the heartbeat timestamp and
    /// loads the raw WAL entries through the I/O scheduler.  The returned list must be
    /// delivered back to the executor as a
    /// <see cref="RaftRequestType.RestoreLogsLoaded"/> maintenance event so that
    /// <see cref="CompleteRestoreAsync"/> runs under the single-owner guarantee.
    /// </summary>
    public ValueTask<IReadOnlyList<RaftLog>> StartRestoreAsync()
    {
        coreState.LastHeartbeat = host.HybridLogicalClock.TrySendOrLocalEvent(host.LocalNodeId);
        coreState.LastHeartbeatTicks = host.GetMonotonicTimestamp();
        return wal.LoadRestoreLogsAsync();
    }

    /// <summary>
    /// Phase 2 of the nonblocking restore.  Called on the executor thread after
    /// <see cref="StartRestoreAsync"/> has loaded logs from storage.  Replays the
    /// committed entries via the application replication callbacks, updates the
    /// current term, and sends the initial handshake.
    /// </summary>
    public async ValueTask CompleteRestoreAsync(IReadOnlyList<RaftLog> logs)
    {
        if (coreState.Restored)
            return;

        await wal.CompleteRestoreAsync(logs).ConfigureAwait(false);

        coreState.CurrentTerm = await wal.GetCurrentTermAsync().ConfigureAwait(false);

        // B2b: seed durable Raft hard state. The term inferred from the log tail (above) can LAG the true
        // term — a crash after a term bump or a granted vote but before the next log write leaves the tail
        // behind. Trusting the tail alone would let the node regress its term and re-vote for a different
        // candidate in a term it already voted in (a split-brain hazard). The persisted hard state is
        // authoritative for the term; votedFor is restored into expectedLeaders so the "already voted for
        // someone else this term" guard in VoteAsync rejects a different candidate after a restart.
        // (Durability is the lighter, WAL-cadence guarantee — the very last vote before a power loss may
        // not have reached disk; see IWAL.PersistHardState.)
        (long CurrentTerm, string? VotedFor)? hardState = await wal.LoadHardStateAsync().ConfigureAwait(false);
        if (hardState is { } hs)
        {
            if (hs.CurrentTerm > coreState.CurrentTerm)
                coreState.CurrentTerm = hs.CurrentTerm;

            if (!string.IsNullOrEmpty(hs.VotedFor))
                election.RecordExpectedLeader(hs.CurrentTerm, hs.VotedFor);
        }

        // Seed the applied cursor to the frontier restore just replayed. wal.CompleteRestoreAsync
        // delivered every committed entry below the reconstructed commit frontier to the consumer (via
        // InvokeLogRestored / InvokeSystemLogRestored), but coreState.LastAppliedIndex stayed at its -1 init.
        // Without this seed, a restarted node that later wins an election would re-drain the entire
        // retained log from index 0 on promotion (BecomeLeaderAsync → DrainCommittedAppliesAsync),
        // delivering every committed entry to the consumer a SECOND time and holding the serial
        // partition executor for the full backlog before sending its first heartbeat — long enough to
        // risk another election round. GetCommitIndex() returns the highest committed id restore
        // applied (0 when none, since log ids start at 1), and ApplyLogToConsumerAsync applies the
        // identical committed-only filter, so seeding here makes that promotion drain a precise no-op
        // for already-restored entries while still draining anything committed after restore.
        coreState.LastAppliedIndex = wal.GetCommitIndex();

        logger.LogInfoWalRestored(host.LocalEndpoint, host.PartitionId, coreState.NodeState, logs.Count, 0L);

        await heartbeats.SendHandshakeAsync().ConfigureAwait(false);

        coreState.Restored = true;
    }

    /// <summary>
    /// Periodically checks partition leadership and drives elections when necessary.
    /// <para>
    /// <b>Quiesced followers</b> (when <see cref="RaftConfiguration.EnableQuiescence"/> is on
    /// and <see cref="RaftPartitionCoreState.Quiesced"/> is <see langword="true"/>): the per-partition heartbeat timer
    /// is ignored.  Instead, an election is triggered only when the SWIM failure detector marks
    /// the expected leader's node as <see cref="MemberLivenessState.Suspect"/> or
    /// <see cref="MemberLivenessState.Dead"/> — i.e. <c>GetNodeLiveness(leader) != Alive</c>.
    /// This relies on the invariant <c>PingInterval &lt; StartElectionTimeout</c>
    /// (validated at startup by <see cref="RaftConfiguration.Validate"/>): a SWIM Suspect fires
    /// after approximately one <c>PingInterval</c>, comfortably under <c>StartElectionTimeout</c>,
    /// so quiesced failover is not slower than normal election timeout.
    /// </para>
    /// <para>
    /// <b>Live-but-stalled-partition caveat:</b> if a leader's node stays <c>Alive</c> in SWIM
    /// but stops driving a specific partition (e.g. its executor wedges) while still answering
    /// SWIM pings, quiesced followers will not elect a new leader for that partition.  Accepted
    /// limitation for v1; mitigation would require per-partition sequence numbers in the SWIM
    /// ping payload.
    /// </para>
    /// </summary>
    public async Task CheckPartitionLeadershipAsync()
    {
        HLCTimestamp currentTime = host.HybridLogicalClock.TrySendOrLocalEvent(host.LocalNodeId);
        long nowTicks = host.GetMonotonicTimestamp();

        // Read-index expiry runs in every node state, before any early return (including the
        // leader's quiesced one): on the leader it bounds rounds and chained waiters; on
        // followers it bounds WaitLocalApplication applied-frontier waiters — a stable follower
        // whose apply stalls has no leadership-loss transition to fail them, so the tick is the
        // only bound on their wait.
        if (readIndex.HasOutstandingWork)
            await readIndex.ExpireWaitersAsync(nowTicks).ConfigureAwait(false);

        // Retry a withheld committed drain. DrainCommittedAppliesAsync stops without delivering
        // when the next id is absent above the snapshot floor or is still Proposed — correct, since
        // delivering past it would skip it — on the understanding that a later drain picks the
        // entries up. On a follower the *only* thing that starts a drain is a WAL write completing
        // for this partition, and that is precisely what stops arriving: the leader ships entries
        // until the follower's commit index catches up, which happens whether or not anything was
        // applied, and from then on sends empty heartbeats that enqueue no write and complete
        // nothing. The blocking condition clears with nothing left to notice, and the applied
        // frontier stays where it stopped for good.
        //
        // Observed in Jepsen run 31750742525 as a node holding 74 entries having delivered none of
        // them, while the leader correctly computed gap=0 from the commit index that follower
        // itself reported. Every one of the 889 losses in that run was of this kind.
        //
        // Leaders are excluded: their mid-tenure delivery runs through CompleteLeaderCommit and the
        // deferred-applies buffer, and a second drain racing that is how the finding-1 hole was
        // created in the first place. This is a no-op when the frontier is already covered.
        if (coreState.NodeState != RaftNodeState.Leader)
        {
            long commitFrontier = wal.GetCommitIndex();

            if (commitFrontier > coreState.LastAppliedIndex)
                await applier.DrainCommittedAppliesAsync(commitFrontier).ConfigureAwait(false);
        }

        switch (coreState.NodeState)
        {
            // if node is leader just send hearthbeats every Configuration.HeartbeatInterval
            case RaftNodeState.Leader:
            {
                // Promotion-barrier liveness bound: a leader whose barrier no-op never commits
                // (quorum lost right after the election) would otherwise heartbeat forever without
                // ever publishing leadership — followers stay suppressed and the partition wedges
                // with no serving leader. Revert to Follower so the election timeout can pick a
                // replacement (or re-elect this node, which arms a fresh barrier).
                if (coreState.LeadershipBarrierTicket != HLCTimestamp.Zero
                    && MonotonicElapsed(coreState.LeadershipBarrierArmedTicks, nowTicks) >= host.Configuration.LeadershipBarrierTimeout)
                {
                    await RevertUnpublishedPromotionAsync("barrier commit timed out").ConfigureAwait(false);
                    return;
                }

                // Check-quorum: step down once no majority of voters has acked within the window.
                // This does not close the stale-read hole (ConfirmLeadershipAsync does); it bounds
                // how long an isolated leader lingers so minority-side callers fail fast.
                if (host.Configuration.EnableCheckQuorum && readIndex.ShouldStepDownOnQuorumLoss(nowTicks))
                {
                    await StepDownOnQuorumLossAsync().ConfigureAwait(false);
                    return;
                }

                if (coreState.Quiesced)
                {
                    // Gating entry into quiescence is only half the guarantee. A peer can appear or fall
                    // behind AFTER we quiesced — a node joining an idle cluster is the common case — and
                    // quiescence suppresses SendHeartbeat, the only catch-up path, so that peer would be
                    // stranded permanently with no propose traffic coming to wake anything up. Re-arm
                    // heartbeats as soon as any peer is behind (or newly present with no recorded
                    // progress); we quiesce again on a later tick once everyone has converged.
                    if (!heartbeats.HasLaggingPeer())
                        return;

                    coreState.SetQuiesced(false);
                }

                // B3: heartbeat cadence measured on the monotonic clock — a heartbeat received from a
                // skewed peer must not inflate the interval and suppress our own heartbeats.
                if (currentTime != HLCTimestamp.Zero && (MonotonicElapsed(coreState.LastHeartbeatTicks, nowTicks) >= host.Configuration.HeartbeatInterval))
                {
                    // Drain settled proposals on the heartbeat cadence. Under load ReplicateLogs already
                    // sweeps; this covers the idle tail — a leader that stopped proposing would otherwise
                    // retain its last batch's log payloads and, because a non-empty map blocks the quiesce
                    // gate below, never quiesce. Once drained, proposals.ActiveCount reaches 0 and the
                    // quiesce check can fire in this same tick.
                    if (proposals.ActiveCount > 0)
                        proposals.PruneSettled(currentTime);

                    // When quiescence is on and the partition has been idle longer than QuiesceAfter,
                    // send a quiesce marker to followers and stop heartbeating.  Followers switch to
                    // SWIM-based election gating once they receive the marker.
                    if (host.Configuration.EnableQuiescence
                        && !coreState.Quiesced
                        && proposals.ActiveCount == 0
                        && coreState.LastProposalAtTicks != 0
                        && !heartbeats.HasLaggingPeer()
                        && (MonotonicElapsed(coreState.LastProposalAtTicks, nowTicks) >= host.Configuration.QuiesceAfter))
                    {
                        coreState.SetQuiesced(true);
                        coreState.LastHeartbeat = currentTime;
                        coreState.LastHeartbeatTicks = nowTicks;
                        heartbeats.SendQuiesceMarker(currentTime);
                    }
                    else
                    {
                        await heartbeats.SendHeartbeat(false).ConfigureAwait(false);
                    }
                }

                return;
            }

            // Wait Configuration.VotingTimeout seconds after the voting process starts to check if a quorum is available
            case RaftNodeState.Candidate when coreState.VotingStartedTicks != 0 && MonotonicElapsed(coreState.VotingStartedTicks, nowTicks) < host.Configuration.VotingTimeout:
                return;

            case RaftNodeState.Candidate:

                double votingElapsedMs = MonotonicElapsed(coreState.VotingStartedTicks, nowTicks).TotalMilliseconds;
                logger.LogInfoVotingConcluded(host.LocalEndpoint, host.PartitionId, coreState.NodeState, votingElapsedMs);

                coreState.NodeState = RaftNodeState.Follower;
                host.Leader = "";
                coreState.LastHeartbeat = currentTime;
                coreState.LastHeartbeatTicks = nowTicks;
                // Pick a fresh random timeout in the full [StartElectionTimeout, EndElectionTimeout)
                // range rather than capping an incremented value. Incremental backoff converges
                // both nodes to EndElectionTimeout after just one or two failed elections, causing
                // a persistent split-vote livelock because they fire at the same instant every time.
                election.RandomizeElectionTimeout();
                election.ClearExpectedLeaders();
                tracker.ClearAll();
                coreState.LocalCommittedIndex = -1;
                FailAllActiveProposalWaiters();
                coreState.LastProposalAt = HLCTimestamp.Zero;
                coreState.LastProposalAtTicks = 0;
                coreState.SetQuiesced(false);
                election.ResetPreVoteRound();

                await host.InvokeLeaderChanged(host.PartitionId, "");
                return;
            
            // Quiesced follower: per-partition heartbeat timer is suppressed.
            // Gate elections on SWIM node state instead — Suspect or Dead triggers failover.
            case RaftNodeState.Follower when coreState.Quiesced && host.Configuration.EnableQuiescence:
            {
                string expectedLeaderNode = election.GetExpectedLeader(coreState.CurrentTerm);
                if (string.IsNullOrEmpty(expectedLeaderNode) ||
                    host.GetNodeLiveness(expectedLeaderNode) == MemberLivenessState.Alive)
                    return; // leader's node is Alive per SWIM — stay calm
                // Leader node is Suspect or Dead — un-quiesce and challenge leadership.
                coreState.SetQuiesced(false);
                await election.StartPreVoteAsync(currentTime).ConfigureAwait(false);
                break;
            }

            // if node is follower and leader is not sending hearthbeats, start an election.
            // B3: elapsed-since-last-contact is measured on the monotonic clock, so a leader whose HLC
            // ran ahead of ours cannot freeze this gate and delay failover for the length of the skew.
            case RaftNodeState.Follower when (coreState.LastHeartbeatTicks != 0 && (MonotonicElapsed(coreState.LastHeartbeatTicks, nowTicks) < coreState.ElectionTimeout)):
                return;

            case RaftNodeState.Follower:
                // Run a side-effect-free pre-vote first; only a pre-vote quorum promotes to a
                // real election (Raft §9.6), so a stale node can't disrupt a healthy leader.
                await election.StartPreVoteAsync(currentTime).ConfigureAwait(false);
                break;
            
            default:
                logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Unknown node state. Term={CurrentTerm}", host.LocalEndpoint, host.PartitionId, coreState.NodeState, coreState.CurrentTerm);
                break;
        }
    }

    public async Task StepDownAsync(ulong? replyCorrelationId)
    {
        if (coreState.NodeState != RaftNodeState.Leader || host.Leader != host.LocalEndpoint)
        {
            CompleteReply(replyCorrelationId, new(RaftResponseType.None, RaftOperationStatus.NodeIsNotLeader, 0L));
            return;
        }

        HLCTimestamp currentTime = host.HybridLogicalClock.TrySendOrLocalEvent(host.LocalNodeId);
        RaftNode? stepDownTarget = SelectStepDownTarget();

        long nowTicks = host.GetMonotonicTimestamp();

        coreState.NodeState = RaftNodeState.Follower;
        host.Leader = "";
        coreState.LastHeartbeat = currentTime;
        coreState.LastVotation = currentTime;
        coreState.LastHeartbeatTicks = nowTicks;
        coreState.LastVotationTicks = nowTicks;
        coreState.VotingStartedAt = HLCTimestamp.Zero;
        coreState.VotingStartedTicks = 0;
        election.ClearExpectedLeaders();
        tracker.ClearAll();
        coreState.LocalCommittedIndex = -1;
        FailAllActiveProposalWaiters();
        coreState.LastProposalAt = HLCTimestamp.Zero;
        coreState.LastProposalAtTicks = 0;
        coreState.SetQuiesced(false);

        await host.InvokeLeaderChanged(host.PartitionId, "").ConfigureAwait(false);

        if (stepDownTarget is not null)
        {
            host.EnqueueResponse(stepDownTarget.Endpoint, new(
                RaftResponderRequestType.StepDownNotice,
                stepDownTarget,
                new StepDownNoticeRequest(host.PartitionId, coreState.CurrentTerm, currentTime, host.LocalEndpoint)));
        }

        CompleteReply(replyCorrelationId, new(RaftResponseType.None, RaftOperationStatus.Pending, 0L));
    }

    public async Task TransferLeadershipAsync(string targetEndpoint, ulong? replyCorrelationId)
    {
        if (coreState.NodeState != RaftNodeState.Leader || host.Leader != host.LocalEndpoint)
        {
            CompleteReply(replyCorrelationId, new(RaftResponseType.None, RaftOperationStatus.NodeIsNotLeader, 0L));
            return;
        }

        if (string.IsNullOrWhiteSpace(targetEndpoint))
        {
            CompleteReply(replyCorrelationId, new(RaftResponseType.None, RaftOperationStatus.Errored, 0L));
            return;
        }

        if (targetEndpoint == host.LocalEndpoint)
        {
            CompleteReply(replyCorrelationId, new(RaftResponseType.None, RaftOperationStatus.LeaderAlreadyElected, 0L));
            return;
        }

        RaftNode? targetNode = host.Nodes.FirstOrDefault(node => node.Endpoint == targetEndpoint);
        if (targetNode is null)
        {
            CompleteReply(replyCorrelationId, new(RaftResponseType.None, RaftOperationStatus.Errored, 0L));
            return;
        }

        // The adapter read races the WAL write queue (appends still queued in the write scheduler
        // are invisible to it), so take the enqueue-advanced presence frontier when it is higher —
        // an understated local max would hand leadership to a target that is actually behind.
        long localMaxLogId = await wal.GetMaxLogAsync().ConfigureAwait(false);
        long presentIndex = wal.GetPresentIndex();
        if (presentIndex > localMaxLogId)
            localMaxLogId = presentIndex;

        long targetMaxLogId = GetKnownRemoteMaxLogId(targetEndpoint);
        if (targetMaxLogId < localMaxLogId)
        {
            CompleteReply(replyCorrelationId, new(RaftResponseType.None, RaftOperationStatus.ReplicationFailed, 0L));
            return;
        }

        HLCTimestamp currentTime = host.HybridLogicalClock.TrySendOrLocalEvent(host.LocalNodeId);
        long nowTicks = host.GetMonotonicTimestamp();
        long targetTerm = coreState.CurrentTerm + 1;

        coreState.NodeState = RaftNodeState.Follower;
        host.Leader = "";
        coreState.LastHeartbeat = currentTime;
        coreState.LastVotation = currentTime;
        coreState.LastHeartbeatTicks = nowTicks;
        coreState.LastVotationTicks = nowTicks;
        coreState.VotingStartedAt = HLCTimestamp.Zero;
        coreState.VotingStartedTicks = 0;
        election.ClearExpectedLeaders();
        election.RecordExpectedLeader(targetTerm, targetEndpoint);
        tracker.ClearAll();
        coreState.LocalCommittedIndex = -1;
        FailAllActiveProposalWaiters();

        await host.InvokeLeaderChanged(host.PartitionId, "").ConfigureAwait(false);

        host.EnqueueResponse(targetNode.Endpoint, new(
            RaftResponderRequestType.TransferLeadership,
            targetNode,
            new TransferLeadershipRequest(host.PartitionId, coreState.CurrentTerm, currentTime, host.LocalEndpoint, targetEndpoint)));

        CompleteReply(replyCorrelationId, new(RaftResponseType.None, RaftOperationStatus.Pending, 0L));
    }

    public Task SuspendHeartbeatsAsync(ulong? replyCorrelationId)
    {
        if (coreState.NodeState != RaftNodeState.Leader || host.Leader != host.LocalEndpoint)
        {
            CompleteReply(replyCorrelationId, new(RaftResponseType.None, RaftOperationStatus.NodeIsNotLeader, 0L));
            return Task.CompletedTask;
        }

        heartbeats.SetHeartbeatsSuspendedForTesting(true);
        CompleteReply(replyCorrelationId, new(RaftResponseType.None, RaftOperationStatus.Success, 0L));
        return Task.CompletedTask;
    }

    public async Task ResumeHeartbeatsAsync(ulong? replyCorrelationId)
    {
        heartbeats.SetHeartbeatsSuspendedForTesting(false);

        if (coreState.NodeState == RaftNodeState.Leader && host.Leader == host.LocalEndpoint)
            await heartbeats.SendHeartbeat(true).ConfigureAwait(false);

        CompleteReply(replyCorrelationId, new(RaftResponseType.None, RaftOperationStatus.Success, 0L));
    }

    public void ResetTestingState()
    {
        heartbeats.SetHeartbeatsSuspendedForTesting(false);
    }

    /// <summary>
    /// Forces the partition into quiesced state for unit testing without going through the
    /// full leader-side quiesce path that suppresses heartbeats.  Also records the expected
    /// leader so the quiesced follower branch can look up the SWIM state.
    /// </summary>
    public void SetQuiescedForTesting(bool value, string? leaderEndpoint = null, long term = 1)
    {
        coreState.SetQuiesced(value);
        if (value && leaderEndpoint is not null)
            election.RecordExpectedLeader(term, leaderEndpoint);
    }

    /// <summary>
    /// Forces the leader's committed frontier for unit testing so the quiesce gate's
    /// <see cref="HasLaggingPeer"/> check can be exercised without driving a full propose/commit
    /// cycle. A peer with no recorded progress counts as lagging once this is above zero, which is
    /// what re-arms heartbeats on the periodic tick after the leader has quiesced.
    /// </summary>
    public void SetLocalCommittedIndexForTesting(long committedIndex)
    {
        coreState.LocalCommittedIndex = committedIndex;
    }

    /// <summary>
    /// Seeds the state shared by all become-leader paths: advances the HLC, marks the node as
    /// Leader, records the durable committed index for backfill, and starts both the heartbeat
    /// timer and the idle-quiesce clock at the same election timestamp.  Per-follower cursors
    /// (<c>nextIndex</c>, <c>matchIndex</c>) remain the caller's responsibility
    /// because they differ between the single-node fast-path and the quorum-win path.
    /// <para>
    /// Seeding <see cref="RaftPartitionCoreState.LastProposalAt"/> here ensures that a partition that wins an election
    /// and receives no client writes still quiesces after <see cref="RaftConfiguration.QuiesceAfter"/>,
    /// which is the common case for idle partitions in large multi-partition deployments.
    /// </para>
    /// </summary>
    private HLCTimestamp BecomeLeader()
    {
        HLCTimestamp ts = host.HybridLogicalClock.TrySendOrLocalEvent(host.LocalNodeId);
        long nowTicks = host.GetMonotonicTimestamp();
        coreState.NodeState = RaftNodeState.Leader;
        coreState.LocalCommittedIndex = wal.GetCommitIndex();
        coreState.LiveCommitFloor = coreState.LocalCommittedIndex;
        host.Leader = host.LocalEndpoint;
        coreState.LastHeartbeat = ts;
        coreState.LastProposalAt = ts;
        coreState.LastHeartbeatTicks = nowTicks;
        coreState.LastProposalAtTicks = nowTicks;
        readIndex.ResetForNewLeadership(nowTicks);
        coreState.SetQuiesced(false);
        return ts;
    }

    /// <summary>
    /// Synchronously forces the node into Leader state for the given term.  Test-only.
    ///
    /// <para>Intentionally uses the synchronous <see cref="BecomeLeader"/> (no WAL drain)
    /// rather than <see cref="BecomeLeaderAsync"/>.  Tests that use this helper are testing
    /// quiescence, heartbeat timing, or other leader-side behaviour that does not depend on
    /// the apply-before-leader-changed ordering guarantee.  Using the async drain path here
    /// would introduce ReadScheduler round-trips and consumer callbacks that are irrelevant
    /// to — and would slow — those tests.</para>
    ///
    /// <para>Tests that need to exercise the real promotion sequence (drain ordering,
    /// inherited-entry delivery, etc.) must use <see cref="ForceLeaderForTestingAsync"/>
    /// instead, which calls <see cref="BecomeLeaderAsync"/>.</para>
    /// </summary>
    public void SetLeaderForTesting(long term)
    {
        coreState.CurrentTerm = term;
        BecomeLeader();
    }

    /// <summary>
    /// Promotion helper used by all real election paths. Performs the same internal
    /// bookkeeping as <see cref="BecomeLeader"/> but defers publishing
    /// <see cref="IRaftPartitionHost.Leader"/> until the consumer projection provably
    /// covers every entry committed before this node was promoted.
    ///
    /// <para>Two cases, split on whether the WAL tail extends past the known commit frontier:</para>
    ///
    /// <para><b>No inherited tail</b> (presence frontier &lt;= commit frontier): every entry the previous
    /// leader could have committed is already commit-marked locally — election log-freshness
    /// guarantees the winner's log contains every quorum-durable entry, so an empty tail proves
    /// there is nothing inherited. The committed drain (<see cref="DrainCommittedAppliesAsync"/>)
    /// suffices and leadership is published before returning (<see langword="true"/>), exactly the
    /// pre-barrier behavior. This keeps the common idle-failover and clean single-node startup at
    /// zero added latency.</para>
    ///
    /// <para><b>Inherited tail present</b>: entries above the frontier are prior-term
    /// <c>Proposed</c> entries that may be quorum-committed elsewhere (their commit broadcast never
    /// reached this node — e.g. it raced the previous leader's death, or the single-fsync fast path
    /// crashed before writing lazy commit markers). Serving before applying them is the
    /// inherited-entry hole: a lock consumer would double-grant, a KV consumer would serve a stale
    /// read. Raft's remedy is committing a no-op in the new term, which commits the whole prior-term
    /// prefix. This method proposes that no-op (a consumer-invisible
    /// <see cref="RaftSystemConfig.LeadershipBarrierLogType"/> entry, auto-commit) through the
    /// normal proposal path and returns <see langword="false"/> WITHOUT publishing leadership. The
    /// quorum acks for the no-op arrive as later executor operations, so the commit cannot be
    /// awaited here — <see cref="CompleteLeaderCommit"/> publishes <c>host.Leader</c> when the
    /// barrier ticket commits, after its inherited-entry drain has applied every prior-term entry.
    /// Callers must fire <c>InvokeLeaderChanged(self)</c> only when this returns
    /// <see langword="true"/>; the barrier completion fires it otherwise.</para>
    ///
    /// <para>During the barrier window <c>NodeState == Leader</c> (so acks, heartbeats and the
    /// leader tick run — heartbeats suppress rival elections) but <c>host.Leader</c> is unset and
    /// <see cref="NodeState"/> reports <c>Candidate</c>, so both <c>AmILeader</c> paths stay
    /// closed. Writes routed here are refused at the manager layer for the same reason; the window
    /// is bounded by <see cref="RaftConfiguration.LeadershipBarrierTimeout"/>, after which the
    /// leader tick reverts the node to Follower (see <see cref="RevertUnpublishedPromotionAsync"/>).</para>
    ///
    /// <para><b>Atomicity:</b> the promotion is all-or-nothing. If the drain or the barrier propose
    /// throws (WAL read backpressure, scheduler shutdown, etc.), the node is reverted to
    /// <see cref="RaftNodeState.Follower"/> so it is never left in a half-promoted state.  The
    /// exception is re-thrown; the cluster self-heals by electing a replacement in the next
    /// term. Barrier failures after this method returns (propose/commit failure, rollback,
    /// timeout) revert through <see cref="RevertUnpublishedPromotionAsync"/>.</para>
    ///
    /// <para><b>Latency note:</b> the drain holds the partition executor for the full
    /// duration of the backlog (one <c>ReadScheduler</c> round-trip per 512-entry batch
    /// plus one consumer callback per committed entry).  <c>SendHeartbeat</c> runs only
    /// after this method returns, so a node with a large unapplied backlog will delay
    /// its first heartbeat and risk triggering a further election round.  No explicit
    /// bound on the drain is enforced today; a future improvement could cap the drain
    /// and resume it in background once leadership is established.</para>
    /// </summary>
    /// <returns><see langword="true"/> when leadership was published before returning;
    /// <see langword="false"/> when a promotion barrier is pending and
    /// <see cref="CompleteLeaderCommit"/> will publish it.</returns>
    private async Task<bool> BecomeLeaderAsync()
    {
        HLCTimestamp ts = host.HybridLogicalClock.TrySendOrLocalEvent(host.LocalNodeId);
        long nowTicks = host.GetMonotonicTimestamp();
        coreState.NodeState = RaftNodeState.Leader;
        long commitFrontier = wal.GetCommitIndex();
        coreState.LocalCommittedIndex = commitFrontier;
        coreState.LiveCommitFloor = commitFrontier;
        coreState.LastHeartbeat = ts;
        coreState.LastProposalAt = ts;
        coreState.LastHeartbeatTicks = nowTicks;
        coreState.LastProposalAtTicks = nowTicks;
        readIndex.ResetForNewLeadership(nowTicks);
        coreState.SetQuiesced(false);

        long maxLog;

        try
        {
            // Drain all committed entries up to the promotion frontier. By the time this
            // succeeds every InvokeReplicationReceived call has completed, so the consumer
            // projection is current before the partition is advertised as the serving leader.
            // Consumer exceptions are caught inside ApplyLogToConsumerAsync and do not
            // propagate here; only WAL-level errors (backpressure, shutdown) can reach this
            // catch block.
            //
            // The drain withholds rather than skips, so it reports whether it actually REACHED
            // the frontier — and its backend reads race the WAL write queue (the in-memory
            // frontiers advance at enqueue time), so an entry still queued in the write scheduler
            // is invisible and indistinguishable from a hole. Retry until the writes land and the
            // drain covers the frontier, bounded by the barrier timeout; each retry makes forward
            // progress as writes apply and exits as soon as the frontier is reached, so the common
            // case adds no latency. A frontier still unreached at the deadline means entries below
            // it are genuinely absent or unresolved: serving would fix an incomplete consumer
            // projection for the whole tenure — a leader is never backfilled.
            // A sole voter (e.g. the last survivor of graceful leaves) needs only its own write
            // queue to drain — nothing external can arrive — so its bound is short; with voter
            // peers the full barrier timeout is worth spending, because refusing hands leadership
            // to a peer that may hold the missing entries.
            bool hasVoterPeers = sender.HasVoterPeer();
            TimeSpan drainBound = hasVoterPeers ? host.Configuration.LeadershipBarrierTimeout : TimeSpan.FromMilliseconds(250);
            long drainDeadlineTicks = Stopwatch.GetTimestamp();

            while (!await applier.DrainCommittedAppliesAsync(commitFrontier).ConfigureAwait(false))
            {
                if (Stopwatch.GetElapsedTime(drainDeadlineTicks) > drainBound)
                {
                    // Refuse only when another voter exists that could hold the missing entries —
                    // reverting lets it win the next term with a complete log. A sole voter has no
                    // such peer: the departed quorum took the entries with it, refusing forever
                    // would leave the partition permanently leaderless, and the gap is
                    // unrecoverable either way. Serve, and say so loudly.
                    if (hasVoterPeers)
                        throw new RaftException(
                            $"Promotion refused: committed drain stopped at {coreState.LastAppliedIndex} below the frontier {commitFrontier}");

                    logger.LogError("[{LocalEndpoint}/{PartitionId}/{State}] Committed drain stopped at {LastApplied} below the frontier {Frontier} with no voter peers to defer to — proceeding as sole voter; entries in the gap are unrecoverable.",
                        host.LocalEndpoint, host.PartitionId, coreState.NodeState, coreState.LastAppliedIndex, commitFrontier);

                    // Deliver everything this survivor DOES hold past the gap, so only the
                    // genuinely absent entries are lost rather than the whole suffix.
                    await applier.DrainCommittedAppliesAsync(commitFrontier, skipGaps: true).ConfigureAwait(false);
                    break;
                }

                await Task.Delay(2).ConfigureAwait(false);
            }

            maxLog = await wal.GetMaxLogAsync().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            // Revert to Follower so the node is not left in a half-promoted state.
            // host.Leader was never set (we had not reached that line), so the gate for
            // AmILeader / leader-routed reads remains closed. The election timeout will
            // trigger a new round in the next term.
            logger.LogError("[{LocalEndpoint}/{PartitionId}/{State}] Promotion drain failed — reverting to Follower. {Message}\n{Stacktrace}",
                host.LocalEndpoint, host.PartitionId, coreState.NodeState, ex.Message, ex.StackTrace);
            coreState.NodeState = RaftNodeState.Follower;
            coreState.LocalCommittedIndex = -1;
            throw;
        }

        // Completeness gate: a WAL hole below maxLog means this node is missing entries that may be
        // committed elsewhere (the lone-high-entry-over-a-gap shape the unanchored live-propose
        // broadcast can leave). Serving as leader would fix an incomplete consumer projection for
        // the whole tenure — a leader is never backfilled, and neither drain can deliver entries it
        // does not have. Revert so a node with a contiguous log wins the next term; the gap-aware
        // election freshness normally prevents this node from winning at all, so this gate is the
        // defense-in-depth backstop (e.g. a hole opened by an append that raced the election).
        long presentId = wal.GetPresentIndex();
        if (presentId >= 0 && presentId < maxLog)
        {
            logger.LogError("[{LocalEndpoint}/{PartitionId}/{State}] Promotion refused: WAL has a hole below the max id (contiguous through {PresentId}, max {MaxLog}) — reverting to Follower.",
                host.LocalEndpoint, host.PartitionId, coreState.NodeState, presentId, maxLog);
            coreState.NodeState = RaftNodeState.Follower;
            coreState.LocalCommittedIndex = -1;
            throw new RaftException($"Promotion refused: WAL hole below max id (contiguous through {presentId}, max {maxLog})");
        }

        // The inherited-tail decision must come from the enqueue-advanced presence frontier, not
        // the adapter read: GetMaxLogAsync goes through the ReadScheduler straight to the backend
        // and cannot see an append still queued in the write scheduler, so under write-worker
        // latency it under-reports and would skip the barrier — publishing leadership while a
        // prior-term entry that is committed on quorum sits unapplied for the entire tenure (a
        // leader is never backfilled). GetPresentIndex advances at enqueue time and can never
        // under-report a queued append, and the hole gate above already established
        // presentId >= maxLog whenever presence is tracked. Facades that do not track presence
        // (presentId == -1, test stubs) fall back to the adapter read, which is exact for them.
        long inheritedTail = presentId >= 0 ? presentId : maxLog;

        // Seed the propose-id allocator to exactly one above this node's log tail (Raft §5.3: a
        // leader appends at lastLogIndex + 1). A follower stint can leave the allocator anywhere —
        // LOW after accepting an unresolved prior-term band (stamping from there reissues durably
        // occupied indices, committing two different values at one index: the Jepsen Log Matching
        // violation of run 31805148040), or HIGH after truncated-away stale proposes (stamping
        // from there opens a permanent hole below the first new entry). The hole gate above has
        // already proven the log contiguous through the tail, which is what makes the exact set
        // safe at this point and nowhere else. Seeded BEFORE the barrier propose so the barrier
        // no-op itself is stamped correctly.
        wal.SeedProposeAllocator(Math.Max(inheritedTail, commitFrontier) + 1);

        if (inheritedTail <= commitFrontier)
        {
            // No inherited tail: the drain above already proved the consumer projection complete.
            // Publish leader status only after the drain. host.Leader is the gate for
            // AmILeader, so no external observer can see leader == self while the drain
            // is in progress.
            host.Leader = host.LocalEndpoint;
            return true;
        }

        // Inherited prior-term entries exist above the commit frontier. Commit a new-term no-op
        // before serving: CompleteLeaderCommit's inherited drain applies the whole prefix, then
        // publishes leadership. The entry rides the normal proposal path so quorum, durability
        // and commit broadcast all behave exactly like a client write.
        RaftLog barrierLog = new()
        {
            LogType = RaftSystemConfig.LeadershipBarrierLogType,
            LogData = [],
        };

        RaftOperationStatus status;
        HLCTimestamp barrierTicket;

        try
        {
            (status, barrierTicket) = ReplicateLogs([barrierLog], autoCommit: true);
        }
        catch (Exception ex)
        {
            // Same all-or-nothing contract as the drain: a barrier that cannot even be enqueued
            // (WAL backpressure, shutdown) must not leave a Leader that will never publish.
            logger.LogError("[{LocalEndpoint}/{PartitionId}/{State}] Promotion barrier propose failed — reverting to Follower. {Message}\n{Stacktrace}",
                host.LocalEndpoint, host.PartitionId, coreState.NodeState, ex.Message, ex.StackTrace);
            coreState.NodeState = RaftNodeState.Follower;
            coreState.LocalCommittedIndex = -1;
            throw;
        }

        if (status != RaftOperationStatus.Pending)
        {
            logger.LogError("[{LocalEndpoint}/{PartitionId}/{State}] Promotion barrier propose rejected ({Status}) — reverting to Follower.",
                host.LocalEndpoint, host.PartitionId, coreState.NodeState, status);
            coreState.NodeState = RaftNodeState.Follower;
            coreState.LocalCommittedIndex = -1;
            throw new RaftException($"Promotion barrier propose rejected: {status}");
        }

        coreState.LeadershipBarrierTicket = barrierTicket;
        coreState.LeadershipBarrierTerm = coreState.CurrentTerm;
        coreState.LeadershipBarrierArmedTicks = host.GetMonotonicTimestamp();

        if (logger.IsEnabled(LogLevel.Information))
            logger.LogInformation("[{LocalEndpoint}/{PartitionId}/{State}] Promotion barrier armed at ticket {Ticket} (inherited tail {Frontier}..{MaxLog}); leadership publishes on commit",
                host.LocalEndpoint, host.PartitionId, coreState.NodeState, barrierTicket, commitFrontier + 1, inheritedTail);

        return false;
    }

    /// <summary>
    /// Reverts a promoted-but-unpublished leader (promotion barrier pending) back to Follower.
    /// Mirrors the bookkeeping of the Candidate-timeout demotion in
    /// <see cref="CheckPartitionLeadershipAsync"/>: leadership was never published
    /// (<c>host.Leader</c> was never set to self), so consumers already observe an empty leader —
    /// the <c>InvokeLeaderChanged("")</c> here is a harmless re-notification kept for consistency
    /// with every other demotion path. The election timeout then drives a fresh election in a new
    /// term (possibly won by this same node, which will arm a new barrier).
    /// </summary>
    private async Task RevertUnpublishedPromotionAsync(string reason)
    {
        logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Reverting unpublished promotion (barrier {Ticket}, term {Term}): {Reason}",
            host.LocalEndpoint, host.PartitionId, coreState.NodeState, coreState.LeadershipBarrierTicket, coreState.LeadershipBarrierTerm, reason);

        HLCTimestamp currentTime = host.HybridLogicalClock.TrySendOrLocalEvent(host.LocalNodeId);
        long nowTicks = host.GetMonotonicTimestamp();

        coreState.NodeState = RaftNodeState.Follower;
        host.Leader = "";
        coreState.LastHeartbeat = currentTime;
        coreState.LastHeartbeatTicks = nowTicks;
        election.ClearExpectedLeaders();
        tracker.ClearAll();
        coreState.LocalCommittedIndex = -1;
        FailAllActiveProposalWaiters();     // also clears the barrier fields
        coreState.LastProposalAt = HLCTimestamp.Zero;
        coreState.LastProposalAtTicks = 0;
        coreState.SetQuiesced(false);

        await host.InvokeLeaderChanged(host.PartitionId, "").ConfigureAwait(false);
    }








    /// <summary>
    /// Resets <see cref="RaftPartitionCoreState.LastProposalAt"/> to <see cref="HLCTimestamp.Zero"/>.  Test-only;
    /// used to assert that the quiesce guard correctly blocks when no proposal history exists.
    /// </summary>
    public void ClearLastProposalAtForTesting()
    {
        coreState.LastProposalAt = HLCTimestamp.Zero;
        coreState.LastProposalAtTicks = 0;
    }

    private RaftNode? SelectStepDownTarget()
    {
        RaftNode? selected = null;
        long selectedCommitIndex = long.MinValue;

        foreach (RaftNode node in host.Nodes)
        {
            if (node.Endpoint == host.LocalEndpoint)
                continue;

            long commitIndex = tracker.GetCommitFrontierOrDefault(
                node.Endpoint,
                tracker.GetStartCommitIndexOrDefault(node.Endpoint, 0));

            if (selected is null ||
                commitIndex > selectedCommitIndex ||
                (commitIndex == selectedCommitIndex &&
                 string.CompareOrdinal(node.Endpoint, selected.Endpoint) < 0))
            {
                selected = node;
                selectedCommitIndex = commitIndex;
            }
        }

        return selected;
    }

    public async Task ReceiveStepDownNoticeAsync(StepDownNoticeRequest request)
    {
        if (coreState.CurrentTerm > request.Term)
            return;

        // Membership fence: only a committed roster member can have been a leader, so a step-down
        // notice from a non-member must not be able to clear our leader and force an election.
        if (!host.IsMember(request.Endpoint))
        {
            logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Ignoring StepDownNotice from non-member {Endpoint} Term={Term}", host.LocalEndpoint, host.PartitionId, coreState.NodeState, request.Endpoint, request.Term);
            return;
        }

        if (!string.IsNullOrEmpty(host.Leader) && host.Leader != request.Endpoint)
            return;

        HLCTimestamp currentTime = host.HybridLogicalClock.ReceiveEvent(host.LocalNodeId, request.Time);

        coreState.NodeState = RaftNodeState.Follower;
        host.Leader = "";
        coreState.CurrentTerm = Math.Max(coreState.CurrentTerm, request.Term);
        coreState.VotingStartedAt = HLCTimestamp.Zero;
        coreState.VotingStartedTicks = 0;
        election.ClearExpectedLeaders();
        tracker.ClearAll();
        coreState.LocalCommittedIndex = -1;
        proposals.ClearWithoutFailingWaiters();
        coreState.LastHeartbeat = HLCTimestamp.Zero;
        coreState.LastHeartbeatTicks = 0;

        await host.InvokeLeaderChanged(host.PartitionId, "").ConfigureAwait(false);
        await election.StartElectionAsync(currentTime, ignoreRecentVoteCooldown: true).ConfigureAwait(false);
    }

    public async Task ReceiveTransferLeadershipAsync(TransferLeadershipRequest request)
    {
        if (request.TargetEndpoint != host.LocalEndpoint)
            return;

        if (coreState.CurrentTerm > request.Term)
            return;

        // Membership fence: only the current leader (necessarily a roster member) may hand us
        // leadership; a non-member must not be able to trigger a disruptive election.
        if (!host.IsMember(request.Endpoint))
        {
            logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Ignoring TransferLeadership from non-member {Endpoint} Term={Term}", host.LocalEndpoint, host.PartitionId, coreState.NodeState, request.Endpoint, request.Term);
            return;
        }

        if (!string.IsNullOrEmpty(host.Leader) && host.Leader != request.Endpoint)
            return;

        HLCTimestamp currentTime = host.HybridLogicalClock.ReceiveEvent(host.LocalNodeId, request.Time);

        coreState.NodeState = RaftNodeState.Follower;
        host.Leader = "";
        coreState.CurrentTerm = Math.Max(coreState.CurrentTerm, request.Term);
        coreState.VotingStartedAt = HLCTimestamp.Zero;
        coreState.VotingStartedTicks = 0;
        election.ClearExpectedLeaders();
        tracker.ClearAll();
        coreState.LocalCommittedIndex = -1;
        FailAllActiveProposalWaiters();
        coreState.LastHeartbeat = HLCTimestamp.Zero;
        coreState.LastHeartbeatTicks = 0;

        await election.StartElectionAsync(currentTime, ignoreRecentVoteCooldown: true).ConfigureAwait(false);
    }

    public async Task ForceLeaderForTestingAsync(ulong? replyCorrelationId)
    {
        if (coreState.NodeState == RaftNodeState.Leader && host.Leader == host.LocalEndpoint)
        {
            CompleteReply(replyCorrelationId, new(RaftResponseType.None, RaftOperationStatus.Success, 0L));
            return;
        }

        HLCTimestamp currentTime = host.HybridLogicalClock.TrySendOrLocalEvent(host.LocalNodeId);

        election.ClearExpectedLeaders();
        tracker.ClearAll();
        coreState.LocalCommittedIndex = -1;
        election.ClearVotes();
        proposals.ClearWithoutFailingWaiters();

        long nowTicks = host.GetMonotonicTimestamp();

        coreState.NodeState = RaftNodeState.Candidate;
        host.Leader = "";
        coreState.VotingStartedAt = currentTime;
        coreState.VotingStartedTicks = nowTicks;
        coreState.LastHeartbeat = currentTime;
        coreState.LastHeartbeatTicks = nowTicks;
        coreState.CurrentTerm++;

        election.IncreaseVotes(host.LocalEndpoint, coreState.CurrentTerm);

        // B2b: durably record the new term and our self-vote before we solicit votes or become leader, so
        // a crash mid-election cannot restart at a stale term or let us vote for someone else this term.
        await wal.PersistHardStateAsync(coreState.CurrentTerm, host.LocalEndpoint).ConfigureAwait(false);

        await host.InvokeLeaderChanged(host.PartitionId, "").ConfigureAwait(false);

        if (host.Nodes.Count == 0)
        {
            // published == false means a promotion barrier is pending; with no peers the barrier
            // commits locally via the WAL scheduler (self-quorum) and CompleteLeaderCommit fires
            // both the publish and the LeaderChanged notification shortly after.
            bool published = await BecomeLeaderAsync().ConfigureAwait(false);
            if (published)
                await host.InvokeLeaderChanged(host.PartitionId, host.LocalEndpoint).ConfigureAwait(false);
            await heartbeats.SendHeartbeat(true).ConfigureAwait(false);

            CompleteReply(replyCorrelationId, new(RaftResponseType.None, RaftOperationStatus.Success, 0L));
            return;
        }

        await election.RequestVotesAsync(currentTime, coreState.CurrentTerm).ConfigureAwait(false);
        CompleteReply(replyCorrelationId, new(RaftResponseType.None, RaftOperationStatus.Pending, 0L));
    }

    private long GetKnownRemoteMaxLogId(string endpoint) => tracker.GetKnownRemoteMaxLogId(endpoint);

    /// <summary>
    /// Raft §5.4.1 log-freshness comparison: returns <see langword="true"/> when the candidate's
    /// <c>(lastLogTerm, lastLogIndex)</c> is <b>strictly less up-to-date</b> than our local
    /// <c>(localLastLogTerm, localMaxId)</c> — i.e. a strictly higher last-log term wins, and only on
    /// an equal last-log term does the higher index win. This is the check that stops a candidate whose
    /// larger index hides an older last term from being elected over a more current voter.
    /// <para><b>Wire compatibility (B5 rollout):</b> a <paramref name="remoteLastLogTerm"/> ≤ 0 means the
    /// peer predates the last-log-term field (sends 0) or genuinely has an empty log; in both cases we
    /// cannot trust the term and fall back to index-only comparison, exactly matching the pre-B5 ordering
    /// so a mixed-version cluster never mis-orders. An empty-log candidate (index 0) loses that fallback
    /// against any non-empty voter anyway.</para>
    /// </summary>
    /// <summary>
    /// B3: local elapsed time since a monotonic anchor. Returns <see cref="TimeSpan.MaxValue"/> when the
    /// anchor is unset (0) so an "unset" anchor never satisfies a "&lt; timeout" freshness guard — matching
    /// the historical <c>coreState.LastHeartbeat != HLCTimestamp.Zero &amp;&amp; …</c> shape. Callers still gate on the
    /// explicit <c>anchorTicks != 0</c> where the old code gated on <c>!= Zero</c>, for symmetry.
    /// </summary>
    private static TimeSpan MonotonicElapsed(long anchorTicks, long nowTicks) =>
        RaftMonotonic.Elapsed(anchorTicks, nowTicks);



    /// <summary>
    /// Returns the last commit index for <paramref name="endpoint"/>:
    /// • If endpoint equals <see cref="IRaftHost.LocalEndpoint"/> (i.e., this is the leader asking about itself),
    ///   returns <see cref="RaftPartitionCoreState.LocalCommittedIndex"/> — the leader's own durable commit frontier.
    /// • Otherwise returns the last index reported by that follower via <c>CompleteAppendLogs</c>,
    ///   or -1 when no acknowledgement has been received yet.
    /// Must be called on the executor thread (reads private state machine fields).
    /// </summary>
    /// <summary>
    /// Returns the follower's last committed index, or <c>long.MinValue</c> when the follower
    /// has never sent a <c>CompleteAppendLogs</c> for this partition (key absent from
    /// <c>lastCommitIndexes</c>).
    /// <para>
    /// The <c>long.MinValue</c> sentinel lets callers distinguish "not a participant" from
    /// "participant with no committed entries yet (−1)".  <see cref="RaftPartition"/> maps it
    /// to −1 for the non-nullable API and to <c>null</c> for the nullable API.
    /// </para>
    /// </summary>
    internal long GetFollowerCommittedIndex(string endpoint)
    {
        if (endpoint == host.LocalEndpoint)
            return coreState.LocalCommittedIndex;
        if (tracker.TryGetCommitFrontier(endpoint, out long idx))
            return idx;
        return long.MinValue; // sentinel: never heard from on this partition
    }








    /// <summary>
    /// Discards every piece of per-follower replication progress recorded for
    /// <paramref name="endpoint"/>. Invoked when the committed roster (re)admits the member:
    /// retained progress predates the (re)admission and may describe a log the member no longer
    /// holds (an evicted node typically rejoins with reset state). Stale progress is worse than
    /// none — <see cref="HasLaggingPeer"/> would read the member as caught-up, keep the partition
    /// quiesced, and starve it of the heartbeats that are its only catch-up path. After the reset
    /// the member counts as lagging, so a quiesced leader re-arms heartbeats immediately (the
    /// un-quiesce also re-enters the manager's hot set via the quiesce callback) and backfill
    /// re-anchors from the frontier the follower actually reports.
    /// No-op on non-leaders: followers/candidates rebuild this state on their next election win.
    /// </summary>
    public void ResetFollowerProgress(string endpoint)
    {
        if (coreState.NodeState != RaftNodeState.Leader || string.IsNullOrEmpty(endpoint) || endpoint == host.LocalEndpoint)
            return;

        bool hadProgress = tracker.RemovePeer(endpoint);

        if ((hadProgress || coreState.Quiesced) && logger.IsEnabled(LogLevel.Information))
            logger.LogInformation(
                "[{LocalEndpoint}/{PartitionId}/{State}] Reset replication progress for (re)admitted member {Endpoint} (hadProgress={HadProgress}, wasQuiesced={WasQuiesced})",
                host.LocalEndpoint, host.PartitionId, coreState.NodeState, endpoint, hadProgress, coreState.Quiesced);

        // Waking here (rather than waiting for the next safety sweep to notice the lagging peer)
        // bounds the member's starvation window by the heartbeat interval instead of the sweep period.
        if (coreState.Quiesced)
            coreState.SetQuiesced(false);
    }




    /// <summary>
    /// Appends logs to the Write-Ahead Log and updates the state of the node based on the leader's term.
    /// This method usually runs on follower nodes.
    /// </summary>
    /// <param name="endpoint"></param>
    /// <param name="leaderTerm"></param>
    /// <param name="timestamp"></param>
    /// <param name="logs"></param>
    /// <returns></returns>
    public Task AppendLogsAsync(string endpoint, long term, HLCTimestamp timestamp, List<RaftLog>? logs, long prevLogIndex = 0, long prevLogTerm = 0, ulong? replyCorrelationId = null, bool quiesce = false) =>
        followerAppend.AppendLogsCoreAsync(endpoint, term, timestamp, logs, prevLogIndex, prevLogTerm, replyCorrelationId, quiesce);











    


    










    /// <summary>
    /// Returns the event-driven completion task for an active proposal so that callers can
    /// await it directly instead of polling <see cref="CheckTicketCompletion"/>.
    /// Returns <c>null</c> when the proposal is not found in <see cref="activeProposals"/>
    /// (already cleaned up or never registered), in which case the caller should fall back
    /// to a single <see cref="CheckTicketCompletion"/> poll.
    /// </summary>
    public Task<(RaftProposalTicketState, long)>? GetTicketWaiterTask(HLCTimestamp timestamp) =>
        proposals.GetTicketWaiterTask(timestamp);

    /// <summary>
    /// Checks whether a proposal has been completed/committed or not.
    /// </summary>
    /// <param name="timestamp"></param>
    /// <param name="autoCommit"></param>
    /// <returns></returns>
    public (RaftProposalTicketState state, long commitIndex) CheckTicketCompletion(HLCTimestamp timestamp) =>
        proposals.CheckTicketCompletion(timestamp);

    /// <summary>
    /// Single-fsync commit fast path (<see cref="RaftConfiguration.WalSingleFsyncCommit"/>).
    /// For an <c>autoCommit</c> proposal whose propose quorum is already durable, advances the
    /// leader's commit frontier and moves the proposal to <see cref="RaftProposalState.Committed"/>
    /// <b>before</b> the commit fsync is enqueued, so the client ticket (<see cref="CheckTicketCompletion"/>)
    /// is released on quorum-durable rather than on the leader's own second fsync. The per-entry
    /// <c>Committed</c> record is still written by the subsequent <c>EnqueueCommit</c>; only the
    /// acknowledgement point moves earlier.
    /// <para>
    /// Safe because propose-quorum-durable is the true Raft commit point — a quorum holds the entry
    /// on disk — so "acked ⇒ durable on a quorum" is preserved. The frontier value reused here
    /// (<see cref="RaftProposalQuorum.LastLogIndex"/>) is exactly what <see cref="CompleteLeaderCommit"/>
    /// would set from <c>completion.MaxLogIndex</c>, and a quorum acking this proposal implies it
    /// holds every lower-id entry too (followers append contiguously), so advancing the frontier
    /// here cannot skip an unreplicated predecessor. <see cref="CompleteLeaderCommit"/> still runs
    /// afterward and re-applies the same (idempotent) advance.
    /// </para>
    /// <para>No-op unless the flag is on or the proposal is not <c>autoCommit</c>; the explicit
    /// two-phase path is untouched.</para>
    /// </summary>
    /// <summary>
    /// Completes the event-driven waiters for all active proposals with a failure result
    /// so that any caller awaiting them via <c>WaitForQuorum</c> is unblocked immediately
    /// when leadership is lost. Must be called before <c>activeProposals.Clear()</c> on
    /// every leader→follower transition so the proposal objects are still reachable.
    /// </summary>
    private void FailAllActiveProposalWaiters()
    {
        // Every call site is a leadership-loss (or failed-candidacy) transition, so any pending
        // promotion barrier is dead with the proposals: clear it here so no later completion for
        // its ticket can publish leadership for a term this node no longer leads. The publish path
        // is additionally fenced on coreState.NodeState/term, so this is defense in depth, not the only guard.
        //
        // Read-index waiters die with leadership for the same reason: a confirmation must never
        // survive the term it was requested in.
        readIndex.FailAllWaiters();

        coreState.LeadershipBarrierTicket = HLCTimestamp.Zero;
        coreState.LeadershipBarrierTerm = -1;
        coreState.LeadershipBarrierArmedTicks = 0;

        proposals.FailAllWaitersAndClear();
    }



    /// <summary>
    /// Check-quorum step-down: this leader has not heard same-term acks from a majority of voters
    /// for the configured window, so it is almost certainly isolated and possibly already deposed.
    /// Mirrors the bookkeeping of <see cref="StepDownAsync"/> but sends no step-down notice — the
    /// peers are unreachable by hypothesis, and the majority side elects on its own timeout.
    /// Setting <c>coreState.LastHeartbeatTicks</c> here means this node waits a full election timeout before
    /// campaigning, giving a majority-side leader time to adopt it as a follower first.
    /// </summary>
    private async Task StepDownOnQuorumLossAsync()
    {
        logger.LogWarning(
            "[{LocalEndpoint}/{PartitionId}/{State}] Check-quorum: no majority of voter acks within {Window} — stepping down. Term={CurrentTerm}",
            host.LocalEndpoint, host.PartitionId, coreState.NodeState,
            host.Configuration.HeartbeatInterval * host.Configuration.CheckQuorumIntervalMultiplier, coreState.CurrentTerm);

        HLCTimestamp currentTime = host.HybridLogicalClock.TrySendOrLocalEvent(host.LocalNodeId);
        long nowTicks = host.GetMonotonicTimestamp();

        coreState.NodeState = RaftNodeState.Follower;
        host.Leader = "";
        coreState.LastHeartbeat = currentTime;
        coreState.LastVotation = currentTime;
        coreState.LastHeartbeatTicks = nowTicks;
        coreState.LastVotationTicks = nowTicks;
        election.ClearExpectedLeaders();
        tracker.ClearAll();
        coreState.LocalCommittedIndex = -1;
        FailAllActiveProposalWaiters();
        coreState.LastProposalAt = HLCTimestamp.Zero;
        coreState.LastProposalAtTicks = 0;
        coreState.SetQuiesced(false);

        await host.InvokeLeaderChanged(host.PartitionId, "").ConfigureAwait(false);
    }


    /// <summary>
    /// Follower-side snapshot install (Raft "Rule 7") — see
    /// <see cref="SnapshotInstaller.InstallSnapshotAsync"/>.
    /// </summary>
    public Task<RaftResponse> InstallSnapshotAsync(SnapshotInstallRequest request) =>
        snapshotInstaller.InstallSnapshotAsync(request);

    /// <summary>
    /// Adopts <paramref name="leaderEndpoint"/> as this partition's leader for
    /// <paramref name="leaderTerm"/> and takes the durable step-down that goes with it. Shared by
    /// both leader RPCs that can identify a term's leader — AppendEntries and InstallSnapshot.
    /// <para>Kept in the core rather than moved into <see cref="SnapshotInstaller"/>: the
    /// transition spans election bookkeeping, proposal waiters and replication progress, so it
    /// belongs where those collaborators are composed. The installer drives it through a
    /// delegate.</para>
    /// </summary>
    private async Task AdoptLeaderAsync(string leaderEndpoint, long leaderTerm)
    {
        logger.LogInfoLeaderIsNow(host.LocalEndpoint, host.PartitionId, coreState.NodeState, leaderEndpoint, leaderTerm);

        coreState.NodeState = RaftNodeState.Follower;
        host.Leader = leaderEndpoint;
        coreState.CurrentTerm = leaderTerm;
        tracker.ClearAll();
        coreState.LocalCommittedIndex = -1;
        FailAllActiveProposalWaiters();
        election.RecordExpectedLeader(leaderTerm, leaderEndpoint);
        election.ResetPreVoteRound();

        await host.InvokeLeaderChanged(host.PartitionId, leaderEndpoint);
        await wal.PersistHardStateAsync(leaderTerm, leaderEndpoint).ConfigureAwait(false);
    }

    /// <summary>
    /// Advances <c>lastCommitIndexes</c> for <paramref name="endpoint"/> after the background
    /// snapshot task confirmed successful installation. Called on the executor thread via the
    /// <c>postToExecutor</c> callback; delegates ownership update to <see cref="snapshotSender"/>.
    /// </summary>
    public void CompleteSnapshotInstalled(string endpoint, long snapshotIndex)
    {
        // A seeded follower is above the anchor that could not be served, so any open refusal
        // episode for it is over — this is the resolution the fallback exists to produce, and it
        // must be as observable as the refusal was.
        backfillTracker.Clear(endpoint, "the follower was seeded by snapshot install");
        snapshotSender.CompleteSnapshotInstalled(endpoint, snapshotIndex);
    }

    /// <summary>
    /// Leader-side snapshot-transfer status per follower — see <see cref="IRaft.GetSnapshotStatuses"/>.
    /// Reads thread-safe sender state directly; safe to call off the executor thread.
    /// </summary>
    public IReadOnlyList<RaftSnapshotStatus> GetSnapshotStatuses() => snapshotSender.GetStatuses();

    /// <summary>
    /// Leader-side non-contiguous-backfill status per follower — see
    /// <see cref="IRaft.GetBackfillStatuses"/>. Safe to call off the executor thread; the tracker
    /// keeps the episode map concurrent for exactly this query.
    /// </summary>
    public IReadOnlyList<RaftBackfillStatus> GetBackfillStatuses() => backfillTracker.GetStatuses();
    /// <summary>
    /// Read-index leadership confirmation (Raft dissertation §6.4) — see
    /// <see cref="ReadIndexCoordinator.ConfirmLeadershipAsync"/>.
    /// </summary>
    public Task ConfirmLeadershipAsync(ulong? replyCorrelationId) => readIndex.ConfirmLeadershipAsync(replyCorrelationId);

    /// <summary>
    /// Non-leader half of <c>IRaft.ConfirmLocalApplicationAsync</c> — see
    /// <see cref="ReadIndexCoordinator.WaitLocalApplication"/>.
    /// </summary>
    public void WaitLocalApplication(long requiredIndex, ulong? replyCorrelationId) =>
        readIndex.WaitLocalApplication(requiredIndex, replyCorrelationId);
    /// <summary>
    /// Grants or denies a vote (Raft §5.2/§5.4.1, pre-vote §9.6) — see
    /// <see cref="ElectionCoordinator.VoteAsync"/>.
    /// </summary>
    public Task VoteAsync(RaftNode node, long voteTerm, long remoteMaxLogId, HLCTimestamp timestamp, bool preVote = false, long remoteLastLogTerm = 0) =>
        election.VoteAsync(node, voteTerm, remoteMaxLogId, timestamp, preVote, remoteLastLogTerm);

    /// <summary>
    /// Tallies a received (pre-)vote grant — see <see cref="ElectionCoordinator.ReceivedVoteAsync"/>.
    /// </summary>
    public Task ReceivedVoteAsync(string endpoint, long voteTerm, long remoteMaxLogId, bool preVote = false) =>
        election.ReceivedVoteAsync(endpoint, voteTerm, remoteMaxLogId, preVote);
    /// <summary>
    /// Records a peer's identity and log position from its handshake — see
    /// <see cref="HeartbeatDriver.ReceiveHandshake"/>.
    /// </summary>
    public void ReceiveHandshake(int remoteNodeId, string endpoint, long remoteMaxLogId) =>
        heartbeats.ReceiveHandshake(remoteNodeId, endpoint, remoteMaxLogId);
    /// <summary>
    /// Handles one follower's AppendEntries acknowledgement — see
    /// <see cref="ReplicationAckProcessor.CompleteAppendLogsAsync"/>.
    /// </summary>
    public ValueTask CompleteAppendLogsAsync(string endpoint, HLCTimestamp timestamp, RaftOperationStatus status, long committedIndex, long responseTerm = -1) =>
        ackProcessor.CompleteAppendLogsAsync(endpoint, timestamp, status, committedIndex, responseTerm);
    /// <summary>Proposes a batch of log entries — see <see cref="LogReplicator.ReplicateLogsAsync"/>.</summary>
    public Task ReplicateLogsAsync(List<RaftLog>? logs, bool autoCommit, ulong? replyCorrelationId) =>
        replicator.ReplicateLogsAsync(logs, autoCommit, replyCorrelationId);

    /// <summary>Proposes a batch of log entries — see <see cref="LogReplicator.ReplicateLogs"/>.</summary>
    public (RaftOperationStatus, HLCTimestamp ticketId) ReplicateLogs(List<RaftLog>? logs, bool autoCommit, ulong? replyCorrelationId = null) =>
        replicator.ReplicateLogs(logs, autoCommit, replyCorrelationId);

    /// <summary>Proposes several batches in one pass — see <see cref="LogReplicator.ReplicateLogsBatchAsync"/>.</summary>
    public Task ReplicateLogsBatchAsync(IReadOnlyList<(List<RaftLog>? Logs, bool AutoCommit, ulong? ReplyCorrelationId)> messages) =>
        replicator.ReplicateLogsBatchAsync(messages);

    /// <summary>Proposes a checkpoint marker — see <see cref="LogReplicator.ReplicateCheckpointAsync"/>.</summary>
    public Task ReplicateCheckpointAsync(ulong? replyCorrelationId) => replicator.ReplicateCheckpointAsync(replyCorrelationId);

    /// <summary>Commits a proposed ticket — see <see cref="LogReplicator.CommitLogsAsync"/>.</summary>
    public Task CommitLogsAsync(HLCTimestamp ticketId, ulong? replyCorrelationId) =>
        replicator.CommitLogsAsync(ticketId, replyCorrelationId);

    /// <summary>Rolls back a proposed ticket — see <see cref="LogReplicator.RollbackLogsAsync"/>.</summary>
    public Task RollbackLogsAsync(HLCTimestamp ticketId, ulong? replyCorrelationId) =>
        replicator.RollbackLogsAsync(ticketId, replyCorrelationId);
    /// <summary>
    /// Routes a durable WAL completion back to the operation that enqueued it — see
    /// <see cref="WalCompletionRouter.CompleteWalOperationAsync"/>.
    /// </summary>
    public Task CompleteWalOperationAsync(RaftWalCompletion? completion) =>
        walCompletions.CompleteWalOperationAsync(completion);
}
