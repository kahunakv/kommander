using System.Collections.Concurrent;
using System.Diagnostics;
using Kommander.Data;
using Kommander.Diagnostics;
using Kommander.Gossip;
using Kommander.Logging;
using Kommander.Scheduling;
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

    private readonly Dictionary<long, HashSet<string>> votes = [];
    private readonly Dictionary<string, long> lastCommitIndexes = [];
    private readonly Dictionary<string, long> startCommitIndexes = [];

    /// <summary>
    /// Per-follower replication cursor: the index of the next log entry to send to each peer.
    /// Seeded to <c>leaderMaxLog + 1</c> on election win (optimistic: assume peer is in sync).
    /// Backtracked on <see cref="RaftOperationStatus.LogMismatch"/> and advanced on
    /// <see cref="RaftOperationStatus.Success"/> replies.  Only meaningful while this node is
    /// Leader; cleared on every leader→follower transition so stale progress never leaks across terms.
    /// </summary>
    private readonly Dictionary<string, long> nextIndex = [];

    /// <summary>
    /// Per-follower highest log index known to be replicated on that peer.
    /// Zero until the peer confirms receipt of at least one entry.  Advanced in lock-step with
    /// <see cref="nextIndex"/> on a success reply; used to detect full catch-up
    /// (<c>matchIndex[peer] == leaderMaxLog</c>).
    /// </summary>
    private readonly Dictionary<string, long> matchIndex = [];
    private readonly Dictionary<long, string> expectedLeaders = [];
    private readonly Dictionary<HLCTimestamp, RaftProposalQuorum> activeProposals = [];
    private readonly Dictionary<long, Scheduling.RaftPendingWalOperation> pendingWalOperations = [];

    /// <summary>
    /// Tracks in-flight snapshot transfers keyed by follower endpoint.
    /// Prevents concurrent duplicate snapshots to the same node while one is already
    /// in progress.  Entries are removed when the background task completes.
    /// </summary>
    private readonly ConcurrentDictionary<string, byte> pendingSnapshotEndpoints = new();

    /// <summary>
    /// Posts a <see cref="RaftRequest"/> back to the partition executor from a background thread
    /// so completions such as <see cref="RaftRequestType.SnapshotInstalled"/> can update state
    /// under the single-owner guarantee without introducing re-entrancy.
    /// Set by <see cref="RaftPartition"/> at construction time; <see langword="null"/> in unit tests.
    /// </summary>
    private Action<RaftRequest>? postToExecutor;

    private readonly Random random;

    private RaftNodeState nodeState = RaftNodeState.Follower;
    private long currentTerm;

    /// <summary>
    /// Gates whether an incoming <c>Vote(PreVote=true)</c> reply is tallied as a pre-grant.
    /// Pre-vote-only bookkeeping: it is never persisted and answering a pre-vote never mutates it.
    /// </summary>
    private RaftElectionPhase electionPhase = RaftElectionPhase.None;

    /// <summary>
    /// The hypothetical <c>currentTerm + 1</c> the currently-open pre-vote round is for.
    /// <c>-1</c> when no round is open. Pre-vote-only and side-effect-free: the real
    /// <see cref="currentTerm"/> is only bumped once a pre-vote quorum promotes to a real election.
    /// </summary>
    private long preVoteTerm = -1;

    /// <summary>
    /// Endpoints (including self) that pre-granted for <see cref="preVoteTerm"/>.
    /// Pre-vote-only and side-effect-free; separate from the real-election <see cref="votes"/> tally.
    /// </summary>
    private readonly HashSet<string> preVotes = [];
    private HLCTimestamp lastHeartbeat = HLCTimestamp.Zero;
    private HLCTimestamp lastVotation = HLCTimestamp.Zero;
    private HLCTimestamp votingStartedAt = HLCTimestamp.Zero;
    private TimeSpan electionTimeout;
    private bool heartbeatsSuspendedForTesting;
    private bool restored;

    /// <summary>
    /// When <see langword="true"/> this partition is quiesced: no per-partition heartbeats are
    /// expected or sent.  Followers gate elections on SWIM node state instead of the heartbeat
    /// timer.  Only set when <see cref="RaftConfiguration.EnableQuiescence"/> is on.
    /// </summary>
    private bool quiesced;

    /// <summary>
    /// HLC timestamp of the last real proposal enqueued on this leader.
    /// Zero until the first proposal arrives after winning election.
    /// Used to determine when the partition has been idle long enough to quiesce:
    /// once <c>now - lastProposalAt &gt; QuiesceAfter</c> and no proposals are in flight,
    /// the leader sends a quiesce marker and stops heartbeating.
    /// </summary>
    private HLCTimestamp lastProposalAt;


    /// <summary>
    /// Highest log index the leader has durably committed (set by <see cref="CompleteLeaderCommit"/>).
    /// Compared against <see cref="lastCommitIndexes"/> in <c>SendHeartbeat</c> to decide whether
    /// a follower gap warrants backfill. Intentionally excludes in-flight proposed-but-uncommitted
    /// entries so healthy followers don't trigger spurious WAL reads under write load.
    /// Reset to -1 on every leader→follower transition.
    /// </summary>
    private long localCommittedIndex = -1;

    public RaftNodeState NodeState => nodeState;
    public long CurrentTerm => currentTerm;

    /// <summary>
    /// The current election timeout for this partition. Exposed so callers with access to a seeded
    /// configuration can verify reproducibility without depending on wall-clock behaviour.
    /// </summary>
    public TimeSpan ElectionTimeout => electionTimeout;

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

        random = host.Configuration.ElectionTimeoutSeed is int seed
            ? new Random(seed ^ host.PartitionId)
            : Random.Shared;

        electionTimeout = TimeSpan.FromMilliseconds(random.Next(
            host.Configuration.StartElectionTimeout,
            host.Configuration.EndElectionTimeout));
    }

    /// <summary>
    /// Wires the callback used to post messages back to the partition executor from background
    /// tasks (e.g., <see cref="RaftRequestType.SnapshotInstalled"/>).  Called once by
    /// <see cref="RaftPartition"/> immediately after the executor is created.
    /// </summary>
    public void SetPostToExecutor(Action<RaftRequest> post) => postToExecutor = post;

    private void CompleteReply(ulong? correlationId, RaftResponse response)
    {
        if (correlationId is not null)
            replySink.TryComplete(correlationId.Value, response);
    }

    /// <summary>
    /// Clears all bookkeeping for the current pre-vote round. Called when a candidacy is
    /// abandoned (Candidate→Follower) and when a real election begins, so a stale pre-vote
    /// round for an old term can never leak into and falsely promote a later term.
    /// Side-effect-free with respect to real Raft state (term/votes/leader are untouched).
    /// </summary>
    private void ResetPreVoteRound()
    {
        preVotes.Clear();
        preVoteTerm = -1;
        electionPhase = RaftElectionPhase.None;
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
        lastHeartbeat = host.HybridLogicalClock.TrySendOrLocalEvent(host.LocalNodeId);
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
        if (restored)
            return;

        await wal.CompleteRestoreAsync(logs).ConfigureAwait(false);

        currentTerm = await wal.GetCurrentTermAsync().ConfigureAwait(false);

        logger.LogInfoWalRestored(host.LocalEndpoint, host.PartitionId, nodeState, logs.Count, 0L);

        await SendHandshakeAsync().ConfigureAwait(false);

        restored = true;
    }

    /// <summary>
    /// Periodically checks partition leadership and drives elections when necessary.
    /// <para>
    /// <b>Quiesced followers</b> (when <see cref="RaftConfiguration.EnableQuiescence"/> is on
    /// and <see cref="quiesced"/> is <see langword="true"/>): the per-partition heartbeat timer
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

        switch (nodeState)
        {
            // if node is leader just send hearthbeats every Configuration.HeartbeatInterval
            case RaftNodeState.Leader:
            {
                if (quiesced)
                    return;

                if (currentTime != HLCTimestamp.Zero && ((currentTime - lastHeartbeat) >= host.Configuration.HeartbeatInterval))
                {
                    // When quiescence is on and the partition has been idle longer than QuiesceAfter,
                    // send a quiesce marker to followers and stop heartbeating.  Followers switch to
                    // SWIM-based election gating once they receive the marker.
                    if (host.Configuration.EnableQuiescence
                        && !quiesced
                        && activeProposals.Count == 0
                        && lastProposalAt != HLCTimestamp.Zero
                        && (currentTime - lastProposalAt) >= host.Configuration.QuiesceAfter)
                    {
                        quiesced = true;
                        lastHeartbeat = currentTime;
                        SendQuiesceMarker(currentTime);
                    }
                    else
                    {
                        await SendHeartbeat(false).ConfigureAwait(false);
                    }
                }

                return;
            }
            
            // Wait Configuration.VotingTimeout seconds after the voting process starts to check if a quorum is available
            case RaftNodeState.Candidate when votingStartedAt != HLCTimestamp.Zero && (currentTime - votingStartedAt) < host.Configuration.VotingTimeout:
                return;
            
            case RaftNodeState.Candidate:
                
                logger.LogInfoVotingConcluded(host.LocalEndpoint, host.PartitionId, nodeState, (currentTime - votingStartedAt).TotalMilliseconds);
            
                nodeState = RaftNodeState.Follower;
                host.Leader = "";
                lastHeartbeat = currentTime;
                // Pick a fresh random timeout in the full [StartElectionTimeout, EndElectionTimeout)
                // range rather than capping an incremented value. Incremental backoff converges
                // both nodes to EndElectionTimeout after just one or two failed elections, causing
                // a persistent split-vote livelock because they fire at the same instant every time.
                electionTimeout = TimeSpan.FromMilliseconds(
                    random.Next(host.Configuration.StartElectionTimeout, host.Configuration.EndElectionTimeout));
                expectedLeaders.Clear();
                lastCommitIndexes.Clear();
                nextIndex.Clear();
                matchIndex.Clear();
                localCommittedIndex = -1;
                activeProposals.Clear();
                lastProposalAt = HLCTimestamp.Zero;
                quiesced = false;
                ResetPreVoteRound();

                await host.InvokeLeaderChanged(host.PartitionId, "");
                return;
            
            // Quiesced follower: per-partition heartbeat timer is suppressed.
            // Gate elections on SWIM node state instead — Suspect or Dead triggers failover.
            case RaftNodeState.Follower when quiesced && host.Configuration.EnableQuiescence:
            {
                string expectedLeaderNode = expectedLeaders.GetValueOrDefault(currentTerm, "");
                if (string.IsNullOrEmpty(expectedLeaderNode) ||
                    host.GetNodeLiveness(expectedLeaderNode) == MemberLivenessState.Alive)
                    return; // leader's node is Alive per SWIM — stay calm
                // Leader node is Suspect or Dead — un-quiesce and challenge leadership.
                quiesced = false;
                await StartPreVoteAsync(currentTime).ConfigureAwait(false);
                break;
            }

            // if node is follower and leader is not sending hearthbeats, start an election
            case RaftNodeState.Follower when (lastHeartbeat != HLCTimestamp.Zero && ((currentTime - lastHeartbeat) < electionTimeout)):
                return;

            case RaftNodeState.Follower:
                // Run a side-effect-free pre-vote first; only a pre-vote quorum promotes to a
                // real election (Raft §9.6), so a stale node can't disrupt a healthy leader.
                await StartPreVoteAsync(currentTime).ConfigureAwait(false);
                break;
            
            default:
                logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Unknown node state. Term={CurrentTerm}", host.LocalEndpoint, host.PartitionId, nodeState, currentTerm);
                break;
        }
    }

    public async Task StepDownAsync(ulong? replyCorrelationId)
    {
        if (nodeState != RaftNodeState.Leader || host.Leader != host.LocalEndpoint)
        {
            CompleteReply(replyCorrelationId, new(RaftResponseType.None, RaftOperationStatus.NodeIsNotLeader, 0L));
            return;
        }

        HLCTimestamp currentTime = host.HybridLogicalClock.TrySendOrLocalEvent(host.LocalNodeId);
        RaftNode? stepDownTarget = SelectStepDownTarget();

        nodeState = RaftNodeState.Follower;
        host.Leader = "";
        lastHeartbeat = currentTime;
        lastVotation = currentTime;
        votingStartedAt = HLCTimestamp.Zero;
        expectedLeaders.Clear();
        lastCommitIndexes.Clear();
        nextIndex.Clear();
        matchIndex.Clear();
        localCommittedIndex = -1;
        activeProposals.Clear();
        lastProposalAt = HLCTimestamp.Zero;
        quiesced = false;

        await host.InvokeLeaderChanged(host.PartitionId, "").ConfigureAwait(false);

        if (stepDownTarget is not null)
        {
            host.EnqueueResponse(stepDownTarget.Endpoint, new(
                RaftResponderRequestType.StepDownNotice,
                stepDownTarget,
                new StepDownNoticeRequest(host.PartitionId, currentTerm, currentTime, host.LocalEndpoint)));
        }

        CompleteReply(replyCorrelationId, new(RaftResponseType.None, RaftOperationStatus.Pending, 0L));
    }

    public async Task TransferLeadershipAsync(string targetEndpoint, ulong? replyCorrelationId)
    {
        if (nodeState != RaftNodeState.Leader || host.Leader != host.LocalEndpoint)
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

        long localMaxLogId = await wal.GetMaxLogAsync().ConfigureAwait(false);
        long targetMaxLogId = GetKnownRemoteMaxLogId(targetEndpoint);
        if (targetMaxLogId < localMaxLogId)
        {
            CompleteReply(replyCorrelationId, new(RaftResponseType.None, RaftOperationStatus.ReplicationFailed, 0L));
            return;
        }

        HLCTimestamp currentTime = host.HybridLogicalClock.TrySendOrLocalEvent(host.LocalNodeId);
        long targetTerm = currentTerm + 1;

        nodeState = RaftNodeState.Follower;
        host.Leader = "";
        lastHeartbeat = currentTime;
        lastVotation = currentTime;
        votingStartedAt = HLCTimestamp.Zero;
        expectedLeaders.Clear();
        expectedLeaders[targetTerm] = targetEndpoint;
        lastCommitIndexes.Clear();
        nextIndex.Clear();
        matchIndex.Clear();
        localCommittedIndex = -1;
        activeProposals.Clear();

        await host.InvokeLeaderChanged(host.PartitionId, "").ConfigureAwait(false);

        host.EnqueueResponse(targetNode.Endpoint, new(
            RaftResponderRequestType.TransferLeadership,
            targetNode,
            new TransferLeadershipRequest(host.PartitionId, currentTerm, currentTime, host.LocalEndpoint, targetEndpoint)));

        CompleteReply(replyCorrelationId, new(RaftResponseType.None, RaftOperationStatus.Pending, 0L));
    }

    public Task SuspendHeartbeatsAsync(ulong? replyCorrelationId)
    {
        if (nodeState != RaftNodeState.Leader || host.Leader != host.LocalEndpoint)
        {
            CompleteReply(replyCorrelationId, new(RaftResponseType.None, RaftOperationStatus.NodeIsNotLeader, 0L));
            return Task.CompletedTask;
        }

        heartbeatsSuspendedForTesting = true;
        CompleteReply(replyCorrelationId, new(RaftResponseType.None, RaftOperationStatus.Success, 0L));
        return Task.CompletedTask;
    }

    public async Task ResumeHeartbeatsAsync(ulong? replyCorrelationId)
    {
        heartbeatsSuspendedForTesting = false;

        if (nodeState == RaftNodeState.Leader && host.Leader == host.LocalEndpoint)
            await SendHeartbeat(true).ConfigureAwait(false);

        CompleteReply(replyCorrelationId, new(RaftResponseType.None, RaftOperationStatus.Success, 0L));
    }

    public void ResetTestingState()
    {
        heartbeatsSuspendedForTesting = false;
    }

    /// <summary>
    /// Forces the partition into quiesced state for unit testing without going through the
    /// full leader-side quiesce path that suppresses heartbeats.  Also records the expected
    /// leader so the quiesced follower branch can look up the SWIM state.
    /// </summary>
    public void SetQuiescedForTesting(bool value, string? leaderEndpoint = null, long term = 1)
    {
        quiesced = value;
        if (value && leaderEndpoint is not null)
            expectedLeaders[term] = leaderEndpoint;
    }

    /// <summary>
    /// Seeds the state shared by all become-leader paths: advances the HLC, marks the node as
    /// Leader, records the durable committed index for backfill, and starts both the heartbeat
    /// timer and the idle-quiesce clock at the same election timestamp.  Per-follower cursors
    /// (<see cref="nextIndex"/>, <see cref="matchIndex"/>) remain the caller's responsibility
    /// because they differ between the single-node fast-path and the quorum-win path.
    /// <para>
    /// Seeding <see cref="lastProposalAt"/> here ensures that a partition that wins an election
    /// and receives no client writes still quiesces after <see cref="RaftConfiguration.QuiesceAfter"/>,
    /// which is the common case for idle partitions in large multi-partition deployments.
    /// </para>
    /// </summary>
    private HLCTimestamp BecomeLeader()
    {
        HLCTimestamp ts = host.HybridLogicalClock.TrySendOrLocalEvent(host.LocalNodeId);
        nodeState = RaftNodeState.Leader;
        localCommittedIndex = wal.GetCommitIndex();
        host.Leader = host.LocalEndpoint;
        lastHeartbeat = ts;
        lastProposalAt = ts;
        quiesced = false;
        return ts;
    }

    /// <summary>
    /// Forces the node into Leader state for the given term.  Test-only; delegates to
    /// <see cref="BecomeLeader"/> so the test path exercises the same bookkeeping as the
    /// real election paths.
    /// </summary>
    public void SetLeaderForTesting(long term)
    {
        currentTerm = term;
        BecomeLeader();
    }

    /// <summary>
    /// Resets <see cref="lastProposalAt"/> to <see cref="HLCTimestamp.Zero"/>.  Test-only;
    /// used to assert that the quiesce guard correctly blocks when no proposal history exists.
    /// </summary>
    public void ClearLastProposalAtForTesting() => lastProposalAt = HLCTimestamp.Zero;

    private RaftNode? SelectStepDownTarget()
    {
        RaftNode? selected = null;
        long selectedCommitIndex = long.MinValue;

        foreach (RaftNode node in host.Nodes)
        {
            if (node.Endpoint == host.LocalEndpoint)
                continue;

            long commitIndex = lastCommitIndexes.GetValueOrDefault(
                node.Endpoint,
                startCommitIndexes.GetValueOrDefault(node.Endpoint, 0));

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
        if (currentTerm > request.Term)
            return;

        if (!string.IsNullOrEmpty(host.Leader) && host.Leader != request.Endpoint)
            return;

        HLCTimestamp currentTime = host.HybridLogicalClock.ReceiveEvent(host.LocalNodeId, request.Time);

        nodeState = RaftNodeState.Follower;
        host.Leader = "";
        currentTerm = Math.Max(currentTerm, request.Term);
        votingStartedAt = HLCTimestamp.Zero;
        expectedLeaders.Clear();
        lastCommitIndexes.Clear();
        nextIndex.Clear();
        matchIndex.Clear();
        localCommittedIndex = -1;
        activeProposals.Clear();
        lastHeartbeat = HLCTimestamp.Zero;

        await host.InvokeLeaderChanged(host.PartitionId, "").ConfigureAwait(false);
        await StartElectionAsync(currentTime, ignoreRecentVoteCooldown: true).ConfigureAwait(false);
    }

    public async Task ReceiveTransferLeadershipAsync(TransferLeadershipRequest request)
    {
        if (request.TargetEndpoint != host.LocalEndpoint)
            return;

        if (currentTerm > request.Term)
            return;

        if (!string.IsNullOrEmpty(host.Leader) && host.Leader != request.Endpoint)
            return;

        HLCTimestamp currentTime = host.HybridLogicalClock.ReceiveEvent(host.LocalNodeId, request.Time);

        nodeState = RaftNodeState.Follower;
        host.Leader = "";
        currentTerm = Math.Max(currentTerm, request.Term);
        votingStartedAt = HLCTimestamp.Zero;
        expectedLeaders.Clear();
        lastCommitIndexes.Clear();
        nextIndex.Clear();
        matchIndex.Clear();
        localCommittedIndex = -1;
        activeProposals.Clear();
        lastHeartbeat = HLCTimestamp.Zero;

        await StartElectionAsync(currentTime, ignoreRecentVoteCooldown: true).ConfigureAwait(false);
    }

    public async Task ForceLeaderForTestingAsync(ulong? replyCorrelationId)
    {
        if (nodeState == RaftNodeState.Leader && host.Leader == host.LocalEndpoint)
        {
            CompleteReply(replyCorrelationId, new(RaftResponseType.None, RaftOperationStatus.Success, 0L));
            return;
        }

        if (await AmIOutdatedAsync().ConfigureAwait(false))
        {
            CompleteReply(replyCorrelationId, new(RaftResponseType.None, RaftOperationStatus.ReplicationFailed, 0L));
            return;
        }

        HLCTimestamp currentTime = host.HybridLogicalClock.TrySendOrLocalEvent(host.LocalNodeId);

        expectedLeaders.Clear();
        lastCommitIndexes.Clear();
        nextIndex.Clear();
        matchIndex.Clear();
        localCommittedIndex = -1;
        votes.Clear();
        activeProposals.Clear();

        nodeState = RaftNodeState.Candidate;
        host.Leader = "";
        votingStartedAt = currentTime;
        lastHeartbeat = currentTime;
        currentTerm++;

        IncreaseVotes(host.LocalEndpoint, currentTerm);

        await host.InvokeLeaderChanged(host.PartitionId, "").ConfigureAwait(false);

        if (host.Nodes.Count == 0)
        {
            BecomeLeader();
            await host.InvokeLeaderChanged(host.PartitionId, host.LocalEndpoint).ConfigureAwait(false);
            await SendHeartbeat(true).ConfigureAwait(false);

            CompleteReply(replyCorrelationId, new(RaftResponseType.None, RaftOperationStatus.Success, 0L));
            return;
        }

        await RequestVotesAsync(currentTime, currentTerm).ConfigureAwait(false);
        CompleteReply(replyCorrelationId, new(RaftResponseType.None, RaftOperationStatus.Pending, 0L));
    }

    /// <summary>
    /// Compares the current log id with the log id of the other nodes in the partition to determine if the node is outdated.
    /// An outdated node cannot become leader
    /// </summary>
    /// <returns></returns>
    private async Task<bool> AmIOutdatedAsync()
    {
        // if we don't have info about other nodes, we can't be outdated?
        if (startCommitIndexes.Count == 0)
            return false;

        long maxIndex = -1;
        
        foreach (KeyValuePair<string, long> startCommitIndex in startCommitIndexes)
        {
            if (startCommitIndex.Value >= maxIndex)
                maxIndex = startCommitIndex.Value;
        }
        
        long localMaxId = await wal.GetMaxLogAsync().ConfigureAwait(false);
        
        return localMaxId < maxIndex;
    }

    private long GetKnownRemoteMaxLogId(string endpoint) =>
        Math.Max(
            lastCommitIndexes.GetValueOrDefault(endpoint, -1),
            startCommitIndexes.GetValueOrDefault(endpoint, -1));

    /// <summary>
    /// Returns the last commit index for <paramref name="endpoint"/>:
    /// • If endpoint equals <see cref="IRaftHost.LocalEndpoint"/> (i.e., this is the leader asking about itself),
    ///   returns <see cref="localCommittedIndex"/> — the leader's own durable commit frontier.
    /// • Otherwise returns the last index reported by that follower via <c>CompleteAppendLogs</c>,
    ///   or -1 when no acknowledgement has been received yet.
    /// Must be called on the executor thread (reads private state machine fields).
    /// </summary>
    /// <summary>
    /// Returns the follower's last committed index, or <c>long.MinValue</c> when the follower
    /// has never sent a <c>CompleteAppendLogs</c> for this partition (key absent from
    /// <see cref="lastCommitIndexes"/>).
    /// <para>
    /// The <c>long.MinValue</c> sentinel lets callers distinguish "not a participant" from
    /// "participant with no committed entries yet (−1)".  <see cref="RaftPartition"/> maps it
    /// to −1 for the non-nullable API and to <c>null</c> for the nullable API.
    /// </para>
    /// </summary>
    internal long GetFollowerCommittedIndex(string endpoint)
    {
        if (endpoint == host.LocalEndpoint)
            return localCommittedIndex;
        if (lastCommitIndexes.TryGetValue(endpoint, out long idx))
            return idx;
        return long.MinValue; // sentinel: never heard from on this partition
    }

    private async Task StartElectionAsync(HLCTimestamp currentTime, bool ignoreRecentVoteCooldown)
    {
        if (host.LocalRole != ClusterMemberRole.Voter)
        {
            logger.LogDebugSuppressingElection(host.LocalEndpoint, host.PartitionId, nodeState, host.LocalRole);
            return;
        }

        if (!ignoreRecentVoteCooldown)
        {
            if ((lastVotation != HLCTimestamp.Zero && ((currentTime - lastVotation) < (electionTimeout * 2))))
                return;

            string expectedLeader = expectedLeaders.GetValueOrDefault(currentTerm, "");
            if (!string.IsNullOrEmpty(expectedLeader))
            {
                HLCTimestamp lastKnownHeartbeat = host.GetLastNodeActivity(expectedLeader, host.PartitionId);

                if (lastKnownHeartbeat != HLCTimestamp.Zero && ((currentTime - lastKnownHeartbeat) < electionTimeout))
                {
                    lastHeartbeat = lastKnownHeartbeat;
                    return;
                }
            }
        }

        if (await AmIOutdatedAsync().ConfigureAwait(false))
        {
            electionTimeout += TimeSpan.FromMilliseconds(random.Next(host.Configuration.StartElectionTimeoutIncrement, host.Configuration.EndElectionTimeoutIncrement));

            logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] We're outdated, cannot become leader...", host.LocalEndpoint, host.PartitionId, nodeState);
            return;
        }

        // A real election is starting: discard any open pre-vote round so a stale
        // pre-grant set for an old hypothetical term can't bleed into this one.
        ResetPreVoteRound();

        nodeState = RaftNodeState.Candidate;
        host.Leader = "";
        expectedLeaders.Clear();
        votingStartedAt = currentTime;

        await host.InvokeLeaderChanged(host.PartitionId, "");

        currentTerm++;

        IncreaseVotes(host.LocalEndpoint, currentTerm);

        double delayMs = lastHeartbeat != HLCTimestamp.Zero
            ? (currentTime - lastHeartbeat).TotalMilliseconds
            : 0;

        TagList electionTags = new() { { "partition_id", host.PartitionId } };
        KommanderMetrics.ElectionsStartedTotal.Add(1, electionTags);
        KommanderMetrics.ElectionDelayMs.Record(delayMs, electionTags);

        logger.LogWarnVotedToBecomeLeader(host.LocalEndpoint, host.PartitionId, nodeState, delayMs, currentTerm);

        if (host.Nodes.Count == 0)
        {
            nextIndex.Clear();
            matchIndex.Clear();
            BecomeLeader();
            await host.InvokeLeaderChanged(host.PartitionId, host.LocalEndpoint).ConfigureAwait(false);
            await SendHeartbeat(true).ConfigureAwait(false);
            return;
        }

        await RequestVotesAsync(currentTime, currentTerm).ConfigureAwait(false);
    }

    /// <summary>
    /// The pre-election (Raft §9.6) that gates a real election. Before bumping the term and
    /// becoming a Candidate, a follower whose leader went silent first runs a side-effect-free
    /// probe: it asks peers whether they *would* vote for it at <c>currentTerm + 1</c> given its
    /// current log, WITHOUT changing its own term/state. Only a pre-vote quorum (tallied in
    /// <see cref="ReceivedVoteAsync"/>) promotes to <see cref="StartElectionAsync"/>. This is what
    /// stops a stale or partitioned node from repeatedly inflating its term and disrupting a healthy
    /// leader — the livelock this whole change targets.
    /// </summary>
    private async Task StartPreVoteAsync(HLCTimestamp currentTime)
    {
        if (host.LocalRole != ClusterMemberRole.Voter)
        {
            logger.LogDebugSuppressingPreVote(host.LocalEndpoint, host.PartitionId, nodeState, host.LocalRole);
            return;
        }

        // Same "should I even try?" guards as a real election. These guards do NOT touch any Raft
        // consensus state (currentTerm / votes / expectedLeaders / nodeState) — that is the whole
        // point of pre-vote. The one local write below (lastHeartbeat) is a back-off bookkeeping
        // refresh on the "leader still fresh" path, mirroring StartElectionAsync, not a consensus
        // mutation: it just records that we observed the leader so we don't immediately re-trigger.
        if (lastVotation != HLCTimestamp.Zero && ((currentTime - lastVotation) < (electionTimeout * 2)))
            return;

        string expectedLeader = expectedLeaders.GetValueOrDefault(currentTerm, "");
        if (!string.IsNullOrEmpty(expectedLeader))
        {
            HLCTimestamp lastKnownHeartbeat = host.GetLastNodeActivity(expectedLeader, host.PartitionId);

            if (lastKnownHeartbeat != HLCTimestamp.Zero && ((currentTime - lastKnownHeartbeat) < electionTimeout))
            {
                // Intentional: back off and remember we saw the leader. Not a consensus mutation.
                lastHeartbeat = lastKnownHeartbeat;
                return;
            }
        }

        if (await AmIOutdatedAsync().ConfigureAwait(false))
        {
            electionTimeout += TimeSpan.FromMilliseconds(random.Next(host.Configuration.StartElectionTimeoutIncrement, host.Configuration.EndElectionTimeoutIncrement));

            logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] We're outdated, skipping pre-vote...", host.LocalEndpoint, host.PartitionId, nodeState);
            return;
        }

        // No peers to probe: there is nothing a pre-vote can tell us, so go straight to a real election.
        if (host.Nodes.Count == 0)
        {
            await StartElectionAsync(currentTime, ignoreRecentVoteCooldown: true).ConfigureAwait(false);
            return;
        }

        // Open a fresh pre-vote round for the hypothetical next term and seed our own pre-grant.
        electionPhase = RaftElectionPhase.PreVote;
        preVoteTerm = currentTerm + 1;
        preVotes.Clear();
        preVotes.Add(host.LocalEndpoint);

        logger.LogInfoStartingPreVoteRound(host.LocalEndpoint, host.PartitionId, nodeState, preVoteTerm);

        await RequestVotesAsync(currentTime, preVoteTerm, preVote: true).ConfigureAwait(false);
    }

    /// <summary>
    /// Requests votes from the other known nodes in the cluster. Shared by both the real election
    /// (<paramref name="preVote"/> = false) and the side-effect-free pre-vote probe
    /// (<paramref name="preVote"/> = true, Raft §9.6). The only difference on the wire is the
    /// <see cref="RequestVotesRequest.PreVote"/> flag and the <paramref name="term"/> used (the
    /// real <see cref="currentTerm"/> for an election, the hypothetical <c>currentTerm + 1</c> for a probe).
    /// </summary>
    /// <param name="timestamp"></param>
    /// <param name="term">Term to advertise: <see cref="currentTerm"/> for a real election, the hypothetical next term for a pre-vote.</param>
    /// <param name="preVote">When true the outbound request is marked as a pre-vote probe.</param>
    /// <exception cref="RaftException"></exception>
    private async Task RequestVotesAsync(HLCTimestamp timestamp, long term, bool preVote = false)
    {
        IReadOnlyList<RaftNode> nodes = host.Nodes;

        if (nodes.Count == 0)
        {
            logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] No other nodes availables to vote", host.LocalEndpoint, host.PartitionId, nodeState);
            return;
        }

        long currentMaxLog = await wal.GetMaxLogAsync().ConfigureAwait(false);

        RequestVotesRequest request = new(host.PartitionId, term, currentMaxLog, timestamp, host.LocalEndpoint, preVote);

        foreach (RaftNode node in nodes)
        {
            if (node.Endpoint == host.LocalEndpoint)
                throw new RaftException("Corrupted nodes");

            logger.LogInfoAskedForVotes(host.LocalEndpoint, host.PartitionId, nodeState, node.Endpoint, term);

            host.EnqueueResponse(node.Endpoint, new(RaftResponderRequestType.RequestVotes, node, request));
        }
    }

    /// <summary>
    /// Sends a heartbeat message to follower nodes to indicate that the leader node in the partition is still alive.
    /// </summary>
    /// <param name="force"></param>
    /// <exception cref="RaftException"></exception>
    private async Task SendHeartbeat(bool force)
    {
        if (!force && heartbeatsSuspendedForTesting)
            return;
        if (!force && quiesced)
            return;

        IReadOnlyList<RaftNode> nodes = host.Nodes;

        if (nodes.Count == 0)
        {
            logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] No other nodes availables to send hearthbeat", host.LocalEndpoint, host.PartitionId, nodeState);
            return;
        }

        HLCTimestamp prevHeartbeat = lastHeartbeat;
        lastHeartbeat = host.HybridLogicalClock.TrySendOrLocalEvent(host.LocalNodeId);

        if (nodeState != RaftNodeState.Leader && nodeState != RaftNodeState.Candidate)
            return;

        TagList heartbeatTags = new() { { "partition_id", host.PartitionId } };
        KommanderMetrics.HeartbeatsSentTotal.Add(1, heartbeatTags);

        if (prevHeartbeat != HLCTimestamp.Zero)
            KommanderMetrics.HeartbeatDelayMs.Record(
                (lastHeartbeat - prevHeartbeat).TotalMilliseconds, heartbeatTags);

        foreach (RaftNode node in nodes)
        {
            if (node.Endpoint == host.LocalEndpoint)
                throw new RaftException("Corrupted nodes");

            if (host.PartitionId != RaftSystemConfig.SystemPartition && !force)
            {
                HLCTimestamp lastHearthBeatToNode = host.GetLastNodeHearthbeat(node.Endpoint, host.PartitionId);

                if (lastHearthBeatToNode != HLCTimestamp.Zero && ((lastHeartbeat - lastHearthBeatToNode) <= host.Configuration.RecentHeartbeat))
                    continue;
            }

            host.UpdateLastHeartbeat(node.Endpoint, host.PartitionId, lastHeartbeat);

            // Backfill: if the follower's acknowledged committed log trails the leader's committed
            // index by more than BackfillThreshold, ship up to MaxBackfillEntriesPerRound committed
            // entries instead of an empty heartbeat so it converges without waiting for new writes.
            // localCommittedIndex is in-memory and always reflects only durably committed entries,
            // so healthy followers with in-flight proposals do not trigger spurious WAL reads.
            // TrySendBackfillBatchAsync handles nextIndex selection and the Log Matching anchors.
            if (nodeState == RaftNodeState.Leader
                && localCommittedIndex >= 0
                && lastCommitIndexes.TryGetValue(node.Endpoint, out long followerMaxLog)
                && localCommittedIndex - followerMaxLog > host.Configuration.BackfillThreshold)
            {
                if (await TrySendBackfillBatchAsync(node, followerMaxLog, lastHeartbeat).ConfigureAwait(false))
                    continue;

                // Empty batch: the leader has compacted past followerMaxLog+1.
                // If a StateMachineTransfer is registered and no snapshot is already in flight
                // for this follower, kick off an async snapshot transfer. The in-flight guard
                // prevents duplicate transfers; the postToExecutor callback will advance
                // lastCommitIndexes[endpoint] once the follower confirms installation.
                long lastCheckpoint = await wal.GetLastCheckpointAsync().ConfigureAwait(false);
                if (lastCheckpoint > 0 && host.StateMachineTransfer is not null)
                {
                    RaftNode capturedNode = node;
                    if (pendingSnapshotEndpoints.TryAdd(capturedNode.Endpoint, 0))
                    {
                        logger.LogInfoStartingSnapshotTransfer(host.LocalEndpoint, host.PartitionId, nodeState, capturedNode.Endpoint, lastCheckpoint);

                        _ = TrySendSnapshotAsync(capturedNode, lastCheckpoint);
                    }
                }
            }

            AppendLogToNode(node, lastHeartbeat, null);
        }
    }

    /// <summary>
    /// Broadcasts a quiesce-flagged empty AppendLogs to all peers, signalling them to switch from
    /// the heartbeat timer to SWIM-based election gating.  Called once when the leader decides
    /// to suppress per-partition heartbeats for an idle partition.
    /// </summary>
    private void SendQuiesceMarker(HLCTimestamp timestamp)
    {
        foreach (RaftNode node in host.Nodes)
        {
            if (node.Endpoint == host.LocalEndpoint)
                throw new RaftException("Corrupted nodes");
            AppendLogToNode(node, timestamp, null, quiesce: true);
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
        if (host.LocalNodeId == remoteNodeId)
        {
            logger.LogCritSameNodeId(host.LocalEndpoint, host.PartitionId, nodeState, host.LocalNodeId, remoteNodeId);
            
            Environment.Exit(1);
            return;
        }
        
        logger.LogInfoReceivedHandshake(host.LocalEndpoint, host.PartitionId, nodeState, endpoint, remoteNodeId, remoteMaxLogId);
        
        startCommitIndexes[endpoint] = remoteMaxLogId;
    }

    /// <summary>
    /// Sends a handshake to every node available in the cluster to verify if we have the most recent logs.
    /// </summary>
    /// <exception cref="RaftException"></exception>
    private async Task SendHandshakeAsync()
    {
        IReadOnlyList<RaftNode> nodes = host.Nodes;
        
        if (nodes.Count == 0)
        {
            logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] No other nodes availables to send handshake", host.LocalEndpoint, host.PartitionId, nodeState);
            return;
        }
        
        long localMaxId = await wal.GetMaxLogAsync().ConfigureAwait(false);
        
        HandshakeRequest request = new(host.LocalNodeId, host.PartitionId, localMaxId, host.LocalEndpoint);
        
        int number = 0;
        
        foreach (RaftNode node in nodes)
        {
            if (node.Endpoint == host.LocalEndpoint)
                throw new RaftException("Corrupted nodes");
            
            logger.LogDebugSendingHandshake(host.LocalEndpoint, host.PartitionId, nodeState, node.Endpoint, ++number);
            
            host.EnqueueResponse(node.Endpoint, new(RaftResponderRequestType.Handshake, node, request));
        }
    }

    /// <summary>
    /// When another node requests our vote, we verify that the term is valid and the commitIndex is
    /// higher than ours to ensure we don't elect outdated nodes as leaders.
    ///
    /// When <paramref name="preVote"/> is true this answers a side-effect-free pre-election probe
    /// (Raft §9.6): we evaluate the §3 grant predicate and, on grant, reply with a
    /// <see cref="VoteRequest"/> carrying <c>PreVote=true</c> — but we must NOT mutate any real
    /// state (<see cref="currentTerm"/>, <see cref="votes"/>, <see cref="expectedLeaders"/>,
    /// <see cref="lastVotation"/>, <see cref="lastHeartbeat"/>, <see cref="nodeState"/>). This is
    /// what lets a stale/partitioned node probe its electability without disrupting a healthy leader.
    /// </summary>
    /// <param name="node"></param>
    /// <param name="voteTerm"></param>
    /// <param name="remoteMaxLogId"></param>
    /// <param name="timestamp"></param>
    /// <param name="preVote">When true, evaluate as a pure pre-vote probe and never persist state.</param>
    public async Task VoteAsync(RaftNode node, long voteTerm, long remoteMaxLogId, HLCTimestamp timestamp, bool preVote = false)
    {
        if (preVote)
        {
            // Side-effect-free pre-vote (Raft §9.6). NOTHING below this branch's `return`
            // may mutate state: we only read term/log/leader-freshness and, on grant, reply.

            if (!host.IsVoter(node.Endpoint))
            {
                logger.LogDebugDenyingPreVoteNotVoter(host.LocalEndpoint, host.PartitionId, nodeState, node.Endpoint, voteTerm);
                return;
            }

            // A live leader never helps a challenger unseat it.
            if (nodeState == RaftNodeState.Leader)
            {
                logger.LogDebugDenyingPreVoteWeAreLeader(host.LocalEndpoint, host.PartitionId, nodeState, node.Endpoint, voteTerm);
                return;
            }

            // Deny if we still consider a current leader fresh — i.e. we ourselves would not be
            // willing to start an election right now. Mirrors the StartElectionAsync guard.
            string preVoteExpectedLeader = expectedLeaders.GetValueOrDefault(currentTerm, "");
            if (!string.IsNullOrEmpty(preVoteExpectedLeader))
            {
                HLCTimestamp lastKnownHeartbeat = host.GetLastNodeActivity(preVoteExpectedLeader, host.PartitionId);
                if (lastKnownHeartbeat != HLCTimestamp.Zero && ((timestamp - lastKnownHeartbeat) < electionTimeout))
                {
                    logger.LogDebugDenyingPreVoteLeaderFresh(host.LocalEndpoint, host.PartitionId, nodeState, node.Endpoint, voteTerm, preVoteExpectedLeader);
                    return;
                }
            }

            // The hypothetical term must not be stale.
            if (voteTerm < currentTerm)
            {
                logger.LogDebugDenyingPreVoteStaleTerm(host.LocalEndpoint, host.PartitionId, nodeState, node.Endpoint, voteTerm, currentTerm);
                return;
            }

            long preVoteLocalMaxId = await wal.GetMaxLogAsync().ConfigureAwait(false);

            // The candidate's log must be at least as up-to-date. Note: `>=`, not the real-vote
            // path's strict rejection — a pre-vote only probes electability, it doesn't elect.
            if (remoteMaxLogId < preVoteLocalMaxId)
            {
                logger.LogDebugDenyingPreVoteOutdatedLog(host.LocalEndpoint, host.PartitionId, nodeState, node.Endpoint, voteTerm, remoteMaxLogId, preVoteLocalMaxId);
                return;
            }

            logger.LogDebugGrantingPreVote(host.LocalEndpoint, host.PartitionId, nodeState, node.Endpoint, voteTerm);

            VoteRequest preGrant = new(host.PartitionId, voteTerm, preVoteLocalMaxId, timestamp, host.LocalEndpoint, preVote: true);
            host.EnqueueResponse(node.Endpoint, new(RaftResponderRequestType.Vote, node, preGrant));
            return;
        }

        if (!host.IsVoter(node.Endpoint))
        {
            logger.LogDebugDenyingVoteNotVoter(host.LocalEndpoint, host.PartitionId, nodeState, node.Endpoint, voteTerm);
            return;
        }

        if (votes.ContainsKey(voteTerm))
        {
            logger.LogInfoAlreadyVotedInTerm(host.LocalEndpoint, host.PartitionId, nodeState, node.Endpoint, voteTerm);
            return;
        }

        if (nodeState != RaftNodeState.Follower && voteTerm == currentTerm)
        {
            logger.LogInfoCandidateOrLeaderSameTerm(host.LocalEndpoint, host.PartitionId, nodeState, node.Endpoint, voteTerm);
            return;
        }

        if (currentTerm > voteTerm)
        {
            logger.LogInfoVoteOnPreviousTerm(host.LocalEndpoint, host.PartitionId, nodeState, node.Endpoint, voteTerm);
            return;
        }

        string expectedLeader = expectedLeaders.GetValueOrDefault(voteTerm, "");
        
        if (!string.IsNullOrEmpty(expectedLeader) && expectedLeader != node.Endpoint)
        {
            logger.LogInfoAlreadyVotedForOther(host.LocalEndpoint, host.PartitionId, nodeState, node.Endpoint, expectedLeader);
            return;
        }
        
        long localMaxId = await wal.GetMaxLogAsync().ConfigureAwait(false);

        if (localMaxId > remoteMaxLogId)
        {
            // Reject a real vote for a candidate whose log is behind ours (Raft §5.4.1). We do NOT
            // bump our own term here: with PreVote (§9.6) in place a stale candidate can no longer
            // reach this real-vote path with an inflated term, so the old `currentTerm++` heuristic
            // that forced us to be elected is no longer needed and only risked spurious term churn.
            logger.LogInfoVoteOutdatedLog(host.LocalEndpoint, host.PartitionId, nodeState, node.Endpoint, remoteMaxLogId, localMaxId);
            return;
        }
        
        lastHeartbeat = host.HybridLogicalClock.ReceiveEvent(host.LocalNodeId, timestamp);
        lastVotation = lastHeartbeat;
        
        expectedLeaders[voteTerm] = node.Endpoint;

        logger.LogInfoSendingVote(host.LocalEndpoint, host.PartitionId, nodeState, node.Endpoint, voteTerm);

        VoteRequest request = new(host.PartitionId, voteTerm, localMaxId, timestamp, host.LocalEndpoint);
        
        host.EnqueueResponse(node.Endpoint, new(RaftResponderRequestType.Vote, node, request));
    }

    /// <summary>
    /// Processes a vote received in the Raft consensus protocol.
    ///
    /// When <paramref name="preVote"/> is true this tallies a side-effect-free pre-grant (Raft §9.6)
    /// against the currently-open pre-vote round. A node running a pre-vote round is still a Follower,
    /// so this branch sits before the normal "didn't ask for it" Follower early-return. Reaching a
    /// pre-vote quorum promotes to a real election exactly once; no real vote/commit bookkeeping is
    /// touched on the pre-vote path.
    /// </summary>
    /// <param name="endpoint">The identifier of the remote node sending the vote.</param>
    /// <param name="voteTerm">The term associated with the received vote.</param>
    /// <param name="remoteMaxLogId">The highest log ID from the remote node.</param>
    /// <param name="preVote">When true, tally as a pre-vote grant for the open pre-vote round.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    public async Task ReceivedVoteAsync(string endpoint, long voteTerm, long remoteMaxLogId, bool preVote = false)
    {
        // Symmetric guard: discard grants from any endpoint that is not a committed voter.
        // Normal operation is safe without this (candidates only solicit host.Nodes, which is
        // voters-only), but an unsolicited or stale grant from a non-roster node should not
        // be tallied toward quorum.
        if (!host.IsVoter(endpoint))
        {
            logger.LogDebugIgnoringVoteGrantNotVoter(host.LocalEndpoint, host.PartitionId, nodeState, preVote ? "pre-" : "", endpoint, voteTerm);
            return;
        }

        if (preVote)
        {
            // Tally a pre-grant. Placed before the Follower early-return because a node running a
            // pre-vote round is still a Follower. Touches only pre-vote state until quorum promotes.
            if (electionPhase != RaftElectionPhase.PreVote || voteTerm != preVoteTerm)
            {
                logger.LogDebugIgnoringPreVoteGrantNoRound(host.LocalEndpoint, host.PartitionId, nodeState, endpoint, voteTerm, electionPhase, preVoteTerm);
                return;
            }

            preVotes.Add(endpoint);
            // Quorum is computed over voters only; learners in host.Nodes must not inflate the denominator.
            int preVoterTotal = host.Nodes.Count(n => host.IsVoter(n.Endpoint)) + 1; // +1 for self
            int preVoteQuorum = Math.Max(2, (preVoterTotal / 2) + 1);

            logger.LogInfoReceivedPreVote(host.LocalEndpoint, host.PartitionId, nodeState, endpoint, voteTerm, preVotes.Count, preVoteQuorum, preVoterTotal);

            if (preVotes.Count < preVoteQuorum)
                return;

            // Pre-vote quorum reached: promote to a real election (which bumps the term and casts
            // the self-vote). StartElectionAsync resets the round itself once it commits to candidacy,
            // but it can also bail out early (e.g. the AmIOutdatedAsync guard) *before* that reset, so
            // we reset again here unconditionally to guarantee the round can't be tallied twice.
            HLCTimestamp currentTime = host.HybridLogicalClock.TrySendOrLocalEvent(host.LocalNodeId);
            await StartElectionAsync(currentTime, ignoreRecentVoteCooldown: true).ConfigureAwait(false);
            ResetPreVoteRound();
            return;
        }

        if (nodeState == RaftNodeState.Follower)
        {
            logger.LogInfoReceivedUnsolicitedVote(host.LocalEndpoint, host.PartitionId, nodeState, endpoint, voteTerm);
            return;
        }

        if (voteTerm < currentTerm)
        {
            logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Received vote from {Endpoint} on previous term Term={Term}. Ignoring...", host.LocalEndpoint, host.PartitionId, nodeState, endpoint, voteTerm);
            return;
        }
        
        if (nodeState == RaftNodeState.Leader)
        {
            lastCommitIndexes[endpoint] = remoteMaxLogId;
            startCommitIndexes[endpoint] = remoteMaxLogId;
            
            logger.LogInfoReceivedVoteAlreadyLeader(host.LocalEndpoint, host.PartitionId, nodeState, endpoint, voteTerm);
            return;
        }
        
        long maxLogResponse = await wal.GetMaxLogAsync().ConfigureAwait(false);

        if (maxLogResponse < remoteMaxLogId)
        {
            logger.LogWarning(
                "[{LocalEndpoint}/{PartitionId}/{State}] Received vote from {Endpoint} but remote node is on a higher RemoteCommitId={CommitId} Local={LocalCommitId}. Ignoring...", 
                host.LocalEndpoint, 
                host.PartitionId, 
                nodeState, 
                endpoint, 
                remoteMaxLogId, 
                maxLogResponse
            );
            return;
        }

        int numberVotes = IncreaseVotes(endpoint, voteTerm);
        // Quorum is computed over voters only; learners in host.Nodes must not inflate the denominator.
        int voterTotal = host.Nodes.Count(n => host.IsVoter(n.Endpoint)) + 1; // +1 for self
        int quorum = Math.Max(2, (voterTotal / 2) + 1);

        lastCommitIndexes[endpoint] = remoteMaxLogId;
        startCommitIndexes[endpoint] = remoteMaxLogId;

        logger.LogInfoReceivedVote(host.LocalEndpoint, host.PartitionId, nodeState, endpoint, voteTerm, numberVotes, quorum, voterTotal, remoteMaxLogId, maxLogResponse);

        if (numberVotes < quorum)
            return;
        
        // Here quorum was achieved and we can mark ourselves as leader in the partition.
        // Seed per-follower replication progress. nextIndex is optimistic (leaderMaxLog + 1);
        // it will be corrected by LogMismatch replies if any peer is behind.
        nextIndex.Clear();
        matchIndex.Clear();
        foreach (RaftNode peer in host.Nodes)
        {
            nextIndex[peer.Endpoint] = maxLogResponse + 1;
            matchIndex[peer.Endpoint] = 0;
        }

        HLCTimestamp electionTs = BecomeLeader();

        logger.LogInfoReceivedVoteProclaimedLeader(host.LocalEndpoint, host.PartitionId, nodeState, endpoint, (electionTs - votingStartedAt).TotalMilliseconds, voteTerm, numberVotes, quorum, host.Nodes.Count + 1, remoteMaxLogId, maxLogResponse);

        await host.InvokeLeaderChanged(host.PartitionId, host.LocalEndpoint);

        await SendHeartbeat(true).ConfigureAwait(false);
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
        AppendLogsCoreAsync(endpoint, term, timestamp, logs, prevLogIndex, prevLogTerm, replyCorrelationId, quiesce);

    private async Task AppendLogsCoreAsync(
        string endpoint,
        long leaderTerm,
        HLCTimestamp timestamp,
        List<RaftLog>? logs,
        long prevLogIndex = 0,
        long prevLogTerm = 0,
        ulong? replyCorrelationId = null,
        bool quiesce = false
    )
    {
        if (currentTerm > leaderTerm)
        {
            logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Received logs from a leader {Endpoint} with old ReceivedTerm={Term} CurrentTerm={CurrentTerm}. Ignoring...", host.LocalEndpoint, host.PartitionId, nodeState, endpoint, leaderTerm, currentTerm);
            
            host.EnqueueResponse(endpoint, new(
                RaftResponderRequestType.CompleteAppendLogs, 
                new(endpoint), 
                new CompleteAppendLogsRequest(host.PartitionId, leaderTerm, timestamp, host.LocalEndpoint, RaftOperationStatus.LeaderInOldTerm, -1)
            ));
            
            return;
        }
        
        // leaderTerm >= currentTerm is guaranteed here (the currentTerm > leaderTerm case returned
        // above). A valid AppendEntries authoritatively identifies the single leader of leaderTerm,
        // so adopt it regardless of whom we voted for this term. Granting a vote to a candidate does
        // not make it the leader: under a vote split a different candidate can win the term with
        // another quorum, so gating leader acceptance on our vote record (expectedLeaders) would
        // make this follower reject the real leader forever and wedge the partition. expectedLeaders
        // must constrain voting only, never leader acceptance.
        if (host.Leader != endpoint || currentTerm != leaderTerm || nodeState != RaftNodeState.Follower)
        {
            logger.LogInfoLeaderIsNow(host.LocalEndpoint, host.PartitionId, nodeState, endpoint, leaderTerm);

            nodeState = RaftNodeState.Follower;
            host.Leader = endpoint;
            currentTerm = leaderTerm;
            lastCommitIndexes.Clear();
            nextIndex.Clear();
            matchIndex.Clear();
            localCommittedIndex = -1;
            activeProposals.Clear();
            expectedLeaders[leaderTerm] = endpoint;   // overwrite any stale vote target with the real leader
            ResetPreVoteRound();                       // break the pre-vote livelock on adoption

            await host.InvokeLeaderChanged(host.PartitionId, endpoint);
        }

        lastHeartbeat = host.HybridLogicalClock.ReceiveEvent(host.LocalNodeId, timestamp);
        // A quiesce-flagged message tells us to stop expecting heartbeats and gate elections
        // on SWIM liveness instead.  Any non-quiesce AppendLogs (real logs or normal heartbeat)
        // wakes us back up by clearing the flag.
        quiesced = quiesce;

        // Log Matching Property check: the follower must hold an entry at prevLogIndex with
        // prevLogTerm before it can safely append the incoming batch.  A mismatch means the
        // follower's log diverges from the leader's at this position; the leader must backtrack
        // its nextIndex for this peer and retry with an earlier anchor.
        // NOTE: GetAnyTermAtAsync is used (not GetRangeAsync) so that a Proposed-but-uncommitted
        // entry at prevLogIndex is matched correctly; GetRangeAsync filters uncommitted entries.
        if (prevLogIndex > 0 && logs is not null && logs.Count > 0)
        {
            long localMaxLog = await wal.GetMaxLogAsync().ConfigureAwait(false);

            if (prevLogIndex > localMaxLog)
            {
                logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Log Matching rejection from {Endpoint}: prevLogIndex={PrevLogIndex} > localMaxLog={LocalMaxLog}", host.LocalEndpoint, host.PartitionId, nodeState, endpoint, prevLogIndex, localMaxLog);

                host.EnqueueResponse(endpoint, new(
                    RaftResponderRequestType.CompleteAppendLogs,
                    new(endpoint),
                    new CompleteAppendLogsRequest(host.PartitionId, leaderTerm, timestamp, host.LocalEndpoint, RaftOperationStatus.LogMismatch, localMaxLog)
                ));
                return;
            }

            long localTermAtPrev = await wal.GetAnyTermAtAsync(prevLogIndex).ConfigureAwait(false);
            if (localTermAtPrev != prevLogTerm)
            {
                logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Log Matching rejection from {Endpoint}: prevLogIndex={PrevLogIndex} localTerm={LocalTerm} != prevLogTerm={PrevLogTerm}", host.LocalEndpoint, host.PartitionId, nodeState, endpoint, prevLogIndex, localTermAtPrev, prevLogTerm);

                host.EnqueueResponse(endpoint, new(
                    RaftResponderRequestType.CompleteAppendLogs,
                    new(endpoint),
                    new CompleteAppendLogsRequest(host.PartitionId, leaderTerm, timestamp, host.LocalEndpoint, RaftOperationStatus.LogMismatch, localMaxLog)
                ));
                return;
            }
        }

        if (logs is not null && logs.Count > 0)
        {
            if (logger.IsEnabled(LogLevel.Debug))
                logger.LogDebugReceivedLogs(
                    host.LocalEndpoint,
                    host.PartitionId,
                    nodeState,
                    endpoint,
                    leaderTerm,
                    timestamp,
                    string.Join(',', logs.Select(x => x.Id.ToString()))
                );

            WALWriteOperation? operation = wal.EnqueueProposeOrCommit(logs, timestamp, endpoint, leaderTerm);

            if (operation is not null)
            {
                pendingWalOperations[operation.OperationId] = new()
                {
                    ReplyCorrelationId = replyCorrelationId,
                    Logs = logs,
                    Endpoint = endpoint,
                    Timestamp = timestamp,
                };
                return;
            }

            /*(RaftOperationStatus Status, long Index) response = await wal.ProposeOrCommit(logs).ConfigureAwait(false);
            
            if (response.Status != RaftOperationStatus.Success)
            {
                logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Couldn't append logs from leader {Endpoint} with Term={Term} Status={Status} Logs={Logs}", host.LocalEndpoint, host.PartitionId, nodeState, endpoint, leaderTerm, response.Status, logs.Count);
                
                host.EnqueueResponse(endpoint, new(
                    RaftResponderRequestType.CompleteAppendLogs, 
                    new(endpoint), 
                    new CompleteAppendLogsRequest(host.PartitionId, leaderTerm, timestamp, host.LocalEndpoint, response.Status, -1)
                ));
                return;
            }
            
            foreach (HLCTimestamp logTimestamp in logs.Select(x => x.Time).Distinct())
            {
                host.EnqueueResponse(endpoint, new(
                    RaftResponderRequestType.CompleteAppendLogs, 
                    new(endpoint), 
                    new CompleteAppendLogsRequest(host.PartitionId, leaderTerm, logTimestamp, host.LocalEndpoint, RaftOperationStatus.Success, response.Index)
                ));    
            }*/
            
            return;
        }
        
        host.EnqueueResponse(endpoint, new(
            RaftResponderRequestType.CompleteAppendLogs, 
            new(endpoint), 
            new CompleteAppendLogsRequest(host.PartitionId, leaderTerm, timestamp, host.LocalEndpoint, RaftOperationStatus.Success, -1)
        ));

        CompleteReply(replyCorrelationId, RaftResponseStatic.NoneResponse);
    }

    /// <summary>
    /// Replicates logs to other nodes in the cluster when the node is the leader.
    /// </summary>
    /// <param name="logs"></param>
    /// <param name="autoCommit"></param>
    /// <returns></returns>
    /// <exception cref="RaftException"></exception>
    public Task ReplicateLogsAsync(List<RaftLog>? logs, bool autoCommit, ulong? replyCorrelationId)
    {
        (RaftOperationStatus status, HLCTimestamp ticketId) = ReplicateLogs(logs, autoCommit, replyCorrelationId);

        if (status != RaftOperationStatus.Pending)
            CompleteReply(replyCorrelationId, new(RaftResponseType.None, status, ticketId));

        return Task.CompletedTask;
    }

    public (RaftOperationStatus, HLCTimestamp ticketId) ReplicateLogs(
        List<RaftLog>? logs,
        bool autoCommit,
        ulong? replyCorrelationId = null
    )
    {
        if (logs is null || logs.Count == 0)
            return (RaftOperationStatus.Success, HLCTimestamp.Zero);

        if (nodeState != RaftNodeState.Leader)
            return (RaftOperationStatus.NodeIsNotLeader, HLCTimestamp.Zero);

        HLCTimestamp currentTime = host.HybridLogicalClock.SendOrLocalEvent(host.LocalNodeId);
        lastProposalAt = currentTime;
        quiesced = false; // un-quiesce on new proposal: resume normal heartbeating

        // Try to clear and reuse expired proposals
        if (activeProposals.Count > 5)
        {
            TimeSpan range = TimeSpan.FromSeconds(30);
            Dictionary<HLCTimestamp, RaftProposalQuorum> tickets = new();            

            foreach (KeyValuePair<HLCTimestamp, RaftProposalQuorum> proposal in activeProposals)
            {
                if (proposal.Value.HasQuorum() && currentTime - proposal.Value.StartTimestamp > range)
                    tickets.Add(proposal.Key, proposal.Value);
            }

            if (tickets.Count > 0)
            {
                foreach (KeyValuePair<HLCTimestamp, RaftProposalQuorum> kv in tickets)
                {
                    RaftProposalQuorumPool.Return(kv.Value);
                    activeProposals.Remove(kv.Key);
                }                               
            }
        }

        // No peers: a single-node leader is its own quorum. Rather than rejecting the proposal,
        // we enqueue it to the local WAL exactly like the multi-node path; CompleteLeaderPropose
        // then drives the commit immediately (quorum = self), so JoinCluster/IsInitialized can
        // complete on a single-node cluster. The self-only commit shortcut lives entirely behind
        // the Nodes.Count == 0 guard in CompleteLeaderPropose, so multi-node safety is unaffected.

        // Snapshot Type and Time before mutation so we can restore atomically if
        // the WAL scheduler rejects the operation (e.g. BackpressureExceededException).
        RaftLog[] logsArray = logs.ToArray();
        RaftLogType[] savedTypes = Array.ConvertAll(logsArray, l => l.Type);
        HLCTimestamp[] savedTimes = Array.ConvertAll(logsArray, l => l.Time);

        foreach (RaftLog log in logsArray)
        {
            log.Type = RaftLogType.Proposed;
            log.Time = currentTime;
        }

        WALWriteOperation operation;
        try
        {
            operation = wal.EnqueuePropose(currentTerm, logs, currentTime, autoCommit);
        }
        catch
        {
            for (int i = 0; i < logsArray.Length; i++)
            {
                logsArray[i].Type = savedTypes[i];
                logsArray[i].Time = savedTimes[i];
            }
            throw;
        }

        pendingWalOperations[operation.OperationId] = new()
        {
            ReplyCorrelationId = replyCorrelationId,
            TicketId = currentTime,
            Logs = logs,
            AutoCommit = autoCommit,
        };

        return (RaftOperationStatus.Pending, currentTime);

        // Append proposal logs to the Write-Ahead Log
        /*(RaftOperationStatus Status, long) proposeResponse = await wal.Propose(currentTerm, logs).ConfigureAwait(false);
        
        if (proposeResponse.Status != RaftOperationStatus.Success)
        {
            logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Couldn't save proposed logs to local persistence", host.LocalEndpoint, host.PartitionId, nodeState);
            
            return (RaftOperationStatus.Errored, HLCTimestamp.Zero);
        }

        RaftProposalQuorum proposalQuorum = RaftProposalQuorumPool.Rent(logs, autoCommit, currentTime); // new(logs, autoCommit, currentTime);
        
        // Mark itself as completed
        proposalQuorum.MarkNodeCompleted(host.LocalEndpoint);

        foreach (RaftNode node in nodes)
        {
            if (node.Endpoint == host.LocalEndpoint)
                throw new RaftException("Corrupted nodes");
            
            proposalQuorum.AddExpectedNodeCompletion(node.Endpoint);
            
            AppendLogToNode(node, currentTime, logs);
        }

        if (!activeProposals.TryAdd(currentTime, proposalQuorum))
            return (RaftOperationStatus.Errored, HLCTimestamp.Zero);
        
        if (logger.IsEnabled(LogLevel.Debug))
            logger.LogDebugProposedLogs(host.LocalEndpoint, host.PartitionId, nodeState, currentTime, string.Join(',', logs.Select(x => x.Id.ToString())));

        return (RaftOperationStatus.Success, currentTime);*/
    }
    
    /// <summary>
    /// Puts together a plan to replicate logs to other nodes in the cluster when the node is the leader.
    /// </summary>
    /// <param name="logs"></param>
    /// <param name="autoCommit"></param>
    /// <returns></returns>
    /// <exception cref="RaftException"></exception>
    public async Task ReplicateLogsBatchAsync(IReadOnlyList<(List<RaftLog>? Logs, bool AutoCommit, ulong? ReplyCorrelationId)> messages)
    {
        Dictionary<bool, List<RaftLog>> logsPlan = new();
        
        foreach ((List<RaftLog>? logs, bool autoCommit, ulong? replyCorrelationId) message in messages)
        {
            if (logsPlan.TryGetValue(message.autoCommit, out List<RaftLog>? logs))
            {
                if (message.logs is not null && message.logs.Count > 0)
                    logs.AddRange(message.logs);
            }
            else
            {
                if (message.logs is not null && message.logs.Count > 0)
                {
                    logs = [];
                    logs.AddRange(message.logs);
                    logsPlan.Add(message.autoCommit, logs);
                }
            }
        }

        foreach (KeyValuePair<bool, List<RaftLog>> kv in logsPlan)
        {
            foreach ((List<RaftLog>? logs, bool autoCommit, ulong? replyCorrelationId) item in messages)
            {
                if (item.autoCommit == kv.Key)
                    await ReplicateLogsAsync(item.logs, item.autoCommit, item.replyCorrelationId).ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// Replicates the checkpoint to other nodes in the cluster when the node is the leader.
    /// </summary>
    /// <returns></returns>
    public Task ReplicateCheckpointAsync(ulong? replyCorrelationId)
    {
        (RaftOperationStatus status, HLCTimestamp ticketId) = ReplicateCheckpoint(replyCorrelationId);

        if (status != RaftOperationStatus.Pending)
            CompleteReply(replyCorrelationId, new(RaftResponseType.None, status, ticketId));

        return Task.CompletedTask;
    }

    private (RaftOperationStatus status, HLCTimestamp ticketId) ReplicateCheckpoint(
        ulong? replyCorrelationId = null
    )
    {
        if (nodeState != RaftNodeState.Leader)
            return (RaftOperationStatus.NodeIsNotLeader, HLCTimestamp.Zero);
        
        foreach (KeyValuePair<HLCTimestamp, RaftProposalQuorum> proposal in activeProposals)
        {
            if (!proposal.Value.HasQuorum())
                return (RaftOperationStatus.ActiveProposal, HLCTimestamp.Zero);
        }
        
        // No peers: a single-node leader is its own quorum. We enqueue the checkpoint to the local
        // WAL exactly like the multi-node path and let CompleteLeaderPropose drive the commit
        // immediately (quorum = self), behind its Nodes.Count == 0 guard. Without this a single-node
        // cluster could never checkpoint, and since compaction is checkpoint-driven its WAL would
        // grow without bound. Multi-node safety is unaffected: with peers, the same proposal still
        // requires a real quorum.

        // We need a proper HLC sequence to determine a consistent order of the logs
        HLCTimestamp currentTime = host.HybridLogicalClock.SendOrLocalEvent(host.LocalNodeId);

        List<RaftLog> checkpointLogs = [new()
        {
            Id = 0,
            Term = currentTerm,
            Type = RaftLogType.ProposedCheckpoint,
            Time = currentTime,
            LogType = "",
            LogData = []
        }];

        WALWriteOperation operation = wal.EnqueuePropose(currentTerm, checkpointLogs, currentTime, true);
        pendingWalOperations[operation.OperationId] = new()
        {
            ReplyCorrelationId = replyCorrelationId,
            TicketId = currentTime,
            Logs = checkpointLogs,
            AutoCommit = true,
        };

        return (RaftOperationStatus.Pending, currentTime);
        
        // Append proposal logs to the Write-Ahead Log
        /*(RaftOperationStatus Status, long) proposeResponse = await wal.Propose(context.Self, currentTerm, checkpointLogs).ConfigureAwait(false);
        
        if (proposeResponse.Status != RaftOperationStatus.Success)
        {
            logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Couldn't save proposed logs to local persistence", host.LocalEndpoint, host.PartitionId, nodeState);
            
            return (RaftOperationStatus.Errored, HLCTimestamp.Zero);
        }

        RaftProposalQuorum proposalQuorum = RaftProposalQuorumPool.Rent(checkpointLogs, true, currentTime);

        foreach (RaftNode node in nodes)
        {
            if (node.Endpoint == host.LocalEndpoint)
                throw new RaftException("Corrupted nodes");
            
            proposalQuorum.AddExpectedNodeCompletion(node.Endpoint);
            
            AppendLogToNode(node, currentTime, checkpointLogs);
        }

        activeProposals.TryAdd(currentTime, proposalQuorum);
        
        logger.LogInfoProposedCheckpointLogs(
            host.LocalEndpoint, 
            host.PartitionId, 
            nodeState, 
            currentTime, 
            checkpointLogs.Count
        );

        return (RaftOperationStatus.Success, currentTime);*/
    }

    /// <summary>
    /// Marks proposals as committed
    /// </summary>
    /// <param name="ticketId"></param>
    /// <returns></returns>
    public Task CommitLogsAsync(HLCTimestamp ticketId, ulong? replyCorrelationId)
    {
        (RaftOperationStatus status, long commitIndex) = CommitLogs(ticketId, replyCorrelationId);

        if (status != RaftOperationStatus.Pending)
            CompleteReply(replyCorrelationId, new(RaftResponseType.None, status, commitIndex));

        return Task.CompletedTask;
    }

    private (RaftOperationStatus, long commitIndex) CommitLogs(
        HLCTimestamp ticketId,
        ulong? replyCorrelationId = null
    )
    {
        if (nodeState != RaftNodeState.Leader)
            return (RaftOperationStatus.NodeIsNotLeader, 0);
        
        if (!activeProposals.TryGetValue(ticketId, out RaftProposalQuorum? proposal))
            return (RaftOperationStatus.ProposalNotFound, 0);

        if (!proposal.HasQuorum())
        {
            logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Trying to commit proposal {Timestamp} without quorum...", host.LocalEndpoint, host.PartitionId, nodeState, ticketId);
            
            return (RaftOperationStatus.Errored, 0);
        }
        
        if (proposal.State != RaftProposalState.Completed)
        {
            logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Trying to commit proposal {Timestamp} in state {State}...", host.LocalEndpoint, host.PartitionId, nodeState, ticketId, proposal.State);
            
            return (RaftOperationStatus.Errored, 0);
        }

        WALWriteOperation operation = wal.EnqueueCommit(proposal.Logs);
        pendingWalOperations[operation.OperationId] = new()
        {
            ReplyCorrelationId = replyCorrelationId,
            Proposal = proposal,
            TicketId = ticketId
        };

        return (RaftOperationStatus.Pending, operation.LogIndex);
    }
    
    /// <summary>
    /// Marks proposals as rolled back
    /// </summary>
    /// <param name="ticketId"></param>
    /// <returns></returns>
    public Task RollbackLogsAsync(HLCTimestamp ticketId, ulong? replyCorrelationId)
    {
        (RaftOperationStatus status, long commitIndex) = RollbackLogs(ticketId, replyCorrelationId);

        if (status != RaftOperationStatus.Pending)
            CompleteReply(replyCorrelationId, new(RaftResponseType.None, status, commitIndex));

        return Task.CompletedTask;
    }

    private (RaftOperationStatus, long commitIndex) RollbackLogs(
        HLCTimestamp ticketId,
        ulong? replyCorrelationId = null
    )
    {
        if (nodeState != RaftNodeState.Leader)
            return (RaftOperationStatus.NodeIsNotLeader, 0);
        
        if (!activeProposals.TryGetValue(ticketId, out RaftProposalQuorum? proposal))
            return (RaftOperationStatus.ProposalNotFound, 0);
        
        if (!proposal.HasQuorum())
        {
            logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Trying to rollback proposal {Timestamp} without quorum...", host.LocalEndpoint, host.PartitionId, nodeState, ticketId);
            
            return (RaftOperationStatus.Errored, 0);
        }
        
        if (proposal.State != RaftProposalState.Completed)
        {
            logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Trying to rollback proposal {Timestamp} in state {State}...", host.LocalEndpoint, host.PartitionId, nodeState, ticketId, proposal.State);
            
            return (RaftOperationStatus.Errored, 0);
        }
        
        WALWriteOperation operation = wal.EnqueueRollback(proposal.Logs);
        pendingWalOperations[operation.OperationId] = new()
        {
            ReplyCorrelationId = replyCorrelationId,
            Proposal = proposal,
            TicketId = ticketId
        };

        return (RaftOperationStatus.Pending, operation.LogIndex);
    }

    /// <summary>
    /// Increases the number of votes for a given term.
    /// </summary>
    /// <param name="endpoint"></param>
    /// <param name="term"></param>
    /// <returns></returns>
    private int IncreaseVotes(string endpoint, long term)
    {
        if (votes.TryGetValue(term, out HashSet<string>? votesPerEndpoint))
            votesPerEndpoint.Add(endpoint);
        else
            votes[term] = [endpoint];

        return votes[term].Count;
    }
    
    /// <summary>
    /// Appends logs to a specific node in the cluster.
    /// <paramref name="prevLogIndex"/> and <paramref name="prevLogTerm"/> are the Log Matching
    /// anchors: the id and term of the entry immediately preceding the first entry in
    /// <paramref name="logs"/>.  Both default to 0 (no anchor check) for heartbeats and live
    /// proposals where the leader knows the follower is in sync; they are set for backfill batches
    /// so a divergent follower can reject with <see cref="RaftOperationStatus.LogMismatch"/> and
    /// enable leader-side backtracking via <see cref="nextIndex"/>.
    /// </summary>
    /// <param name="node"></param>
    /// <param name="timestamp"></param>
    /// <param name="logs"></param>
    /// <param name="prevLogIndex">Id of the entry immediately before the first entry in <paramref name="logs"/>; 0 skips the check.</param>
    /// <param name="prevLogTerm">Term of the entry at <paramref name="prevLogIndex"/>; 0 when index is 0.</param>
    private void AppendLogToNode(RaftNode node, HLCTimestamp timestamp, List<RaftLog>? logs, long prevLogIndex = 0, long prevLogTerm = 0, bool quiesce = false)
    {
        AppendLogsRequest request;

        if (logs is null || logs.Count == 0)
            request = new(host.PartitionId, currentTerm, timestamp, host.LocalEndpoint) { Quiesce = quiesce };
        else
        {
            request = new(host.PartitionId, currentTerm, timestamp, host.LocalEndpoint, logs, prevLogIndex, prevLogTerm) { Quiesce = quiesce };

            if (logger.IsEnabled(LogLevel.Debug))
                logger.LogDebug(
                    "[{LocalEndpoint}/{PartitionId}/{State}] Enqueued entries for {Endpoint} {Timestamp} PrevLogIndex={PrevLogIndex} Logs={Logs}",
                    host.LocalEndpoint,
                    host.PartitionId,
                    nodeState,
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
    /// Returns <see langword="true"/> when at least one entry was sent; <see langword="false"/> when
    /// the WAL read returns empty (compaction floor reached — caller decides whether to fall back to
    /// a snapshot transfer).
    /// </summary>
    /// <param name="node">The peer to send the batch to.</param>
    /// <param name="followerMaxLog">The highest committed index the leader believes the follower holds;
    /// used as the fallback start when <see cref="nextIndex"/> has not been backtracked below it.</param>
    /// <param name="timestamp">HLC timestamp to stamp the outbound request.</param>
    private async Task<bool> TrySendBackfillBatchAsync(RaftNode node, long followerMaxLog, HLCTimestamp timestamp)
    {
        long from = nextIndex.TryGetValue(node.Endpoint, out long ni) && ni <= localCommittedIndex
            ? ni
            : followerMaxLog + 1;

        List<RaftLog> backfill = await wal.GetRangeAsync(from, host.Configuration.MaxBackfillEntriesPerRound).ConfigureAwait(false);

        if (backfill.Count == 0)
            return false;

        long prevIdx  = from - 1;
        long prevTerm = prevIdx > 0 ? await wal.GetAnyTermAtAsync(prevIdx).ConfigureAwait(false) : 0;

        logger.LogDebugBackfilling(host.LocalEndpoint, host.PartitionId, nodeState, backfill.Count, node.Endpoint, from, prevIdx, localCommittedIndex);

        AppendLogToNode(node, timestamp, backfill, prevIdx, prevTerm);
        return true;
    }

    /// <summary>
    /// Called when a follower has acknowledged (or rejected) an AppendLogs request.
    /// On <see cref="RaftOperationStatus.Success"/> advances <see cref="matchIndex"/> and
    /// <see cref="nextIndex"/> for the peer and immediately ships the next bounded backfill
    /// batch if the follower is still behind, so convergence does not wait a full heartbeat
    /// interval per batch.
    /// On <see cref="RaftOperationStatus.LogMismatch"/> backtracks <see cref="nextIndex"/> using
    /// the spec formula <c>max(1, min(nextIndex-1, followerMax+1))</c>, which always steps back
    /// at least one position even when the follower's max equals the anchor we sent.
    /// </summary>
    /// <param name="endpoint"></param>
    /// <param name="timestamp"></param>
    /// <param name="status"></param>
    /// <param name="committedIndex"></param>
    public async ValueTask CompleteAppendLogsAsync(string endpoint, HLCTimestamp timestamp, RaftOperationStatus status, long committedIndex)
    {
        HLCTimestamp currentTime = host.HybridLogicalClock.ReceiveEvent(host.LocalNodeId, timestamp);

        if (endpoint != host.LocalEndpoint)
            host.UpdateLastNodeActivity(endpoint, host.PartitionId, currentTime);
        
        // Always register the follower's committed index so the backfill loop can detect lag
        // via TryGetValue. A fresh follower with no log entries reports -1; without this
        // the TryGetValue check in SendHeartbeat would fail and backfill would never fire.
        if (!lastCommitIndexes.TryGetValue(endpoint, out long currentIndex) || committedIndex > currentIndex)
            lastCommitIndexes[endpoint] = committedIndex;

        // LogMismatch: the follower's log diverges at the prevLogIndex we sent.
        // committedIndex carries the follower's local max log at the time of rejection.
        // Spec formula: max(1, min(nextIndex[peer]-1, committedIndex+1)).
        // Taking min ensures we step back at least one position even when the follower's
        // max equals the anchor we just tried, preventing a livelock on repeated rejection
        // at the same anchor point.
        if (status == RaftOperationStatus.LogMismatch)
        {
            long currentNext  = nextIndex.GetValueOrDefault(endpoint, committedIndex + 2);
            long backtracked  = Math.Max(1, Math.Min(currentNext - 1, committedIndex + 1));
            nextIndex[endpoint] = backtracked;

            logger.LogWarning(
                "[{LocalEndpoint}/{PartitionId}/{State}] LogMismatch from {Endpoint}: backtracking nextIndex {CurrentNext} → {NextIndex} (followerMax={CommittedIndex})",
                host.LocalEndpoint,
                host.PartitionId,
                nodeState,
                endpoint,
                currentNext,
                backtracked,
                committedIndex
            );

            return;
        }

        if (committedIndex > 0)
        {
            if (startCommitIndexes.TryGetValue(endpoint, out currentIndex))
            {
                if (committedIndex > currentIndex)
                    startCommitIndexes[endpoint] = committedIndex;
            }
            else
                startCommitIndexes[endpoint] = committedIndex;

            logger.LogInfoSuccessfullyCompletedLogs(host.LocalEndpoint, host.PartitionId, nodeState, endpoint, timestamp, committedIndex, (currentTime - timestamp).TotalMilliseconds);
        }

        if (status != RaftOperationStatus.Success)
        {
            logger.LogWarning(
                "[{LocalEndpoint}/{PartitionId}/{State}] Got {Status} from {Endpoint} Timestamp={Timestamp} CommittedIndex={CommittedIndex}",
                host.LocalEndpoint,
                host.PartitionId,
                nodeState,
                status,
                endpoint,
                timestamp,
                committedIndex
            );

            return;
        }

        // Success: advance matchIndex and nextIndex for this peer so the backfill loop
        // knows the follower has caught up to at least committedIndex.
        if (!matchIndex.TryGetValue(endpoint, out long currentMatchIndex) || committedIndex > currentMatchIndex)
            matchIndex[endpoint] = committedIndex;
        nextIndex[endpoint] = matchIndex[endpoint] + 1;

        // Immediately ship the next bounded batch only while an active catch-up is in progress,
        // so a multi-batch backfill converges without stalling a full heartbeat per batch. This
        // must honour the same BackfillThreshold gate as the heartbeat path: a follower lagging by
        // ≤ threshold is intentionally not actively backfilled (small lag rides on normal
        // replication), and eagerly catching it up here would, e.g., make a barely-behind node look
        // fresh enough to receive a leadership transfer it should not.
        if (nodeState == RaftNodeState.Leader
            && localCommittedIndex - matchIndex[endpoint] > host.Configuration.BackfillThreshold)
        {
            RaftNode? behindNode = host.Nodes.FirstOrDefault(n => n.Endpoint == endpoint);
            if (behindNode is not null)
                await TrySendBackfillBatchAsync(behindNode, committedIndex, host.HybridLogicalClock.TrySendOrLocalEvent(host.LocalNodeId)).ConfigureAwait(false);
        }

        if (!activeProposals.TryGetValue(timestamp, out RaftProposalQuorum? proposal))
            return;

        if (proposal.State != RaftProposalState.Incomplete)
            return;

        proposal.MarkNodeCompleted(endpoint);

        if (!proposal.HasQuorum())
        {
            logger.LogInfoProposalPartiallyCompletedAt(host.LocalEndpoint, host.PartitionId, nodeState, timestamp, (currentTime - proposal.StartTimestamp).TotalMilliseconds);
            return;
        }

        logger.LogInfoProposalCompletedAt(host.LocalEndpoint, host.PartitionId, nodeState, timestamp, (currentTime - proposal.StartTimestamp).TotalMilliseconds);

        proposal.SetState(RaftProposalState.Completed);

        if (!proposal.AutoCommit)
        {
            logger.LogInfoProposalNoAutoCommit(host.LocalEndpoint, host.PartitionId, nodeState, timestamp);
            return;
        }

        WALWriteOperation operation = wal.EnqueueCommit(proposal.Logs);
        pendingWalOperations[operation.OperationId] = new()
        {
            Proposal = proposal,
            TicketId = timestamp
        };
    }

    public async Task CompleteWalOperationAsync(RaftWalCompletion? completion)
    {
        if (completion is null)
            return;

        // ── Partition fence ────────────────────────────────────────────────────
        // A completion for a different partition must never drive our state machine.
        // This can happen during the transition period if a completion is mis-routed.
        if (completion.PartitionId != host.PartitionId)
        {
            logger.LogWarning(
                "[{LocalEndpoint}/{PartitionId}/{State}] WAL completion for partition {CompletionPartition} delivered to partition {HostPartition}; discarding stale completion.",
                host.LocalEndpoint, host.PartitionId, nodeState,
                completion.PartitionId, host.PartitionId);
            KommanderMetrics.StaleCompletionsTotal.Add(1,
                new KeyValuePair<string, object?>("reason", "partition_mismatch"));
            return;
        }

        // ── Term fence ─────────────────────────────────────────────────────────
        // A completion submitted when the node was in an earlier term must not
        // advance state after a leadership or followership change.  Term -1 means
        // "not set" (legacy / test paths) and bypasses the fence.
        if (completion.Term >= 0 && completion.Term != currentTerm)
        {
            logger.LogWarning(
                "[{LocalEndpoint}/{PartitionId}/{State}] WAL completion for term {CompletionTerm} delivered in term {CurrentTerm}; discarding stale completion (op {OperationId}).",
                host.LocalEndpoint, host.PartitionId, nodeState,
                completion.Term, currentTerm, completion.OperationId);
            pendingWalOperations.Remove(completion.OperationId, out _);
            KommanderMetrics.StaleCompletionsTotal.Add(1,
                new KeyValuePair<string, object?>("reason", "term_mismatch"));
            return;
        }

        // ── Log-range validation ───────────────────────────────────────────────
        if (completion.MinLogIndex >= 0 && completion.MaxLogIndex >= 0 && completion.MinLogIndex > completion.MaxLogIndex)
        {
            logger.LogWarning(
                "[{LocalEndpoint}/{PartitionId}/{State}] WAL completion op {OperationId} has inverted log range [{Min},{Max}]; discarding.",
                host.LocalEndpoint, host.PartitionId, nodeState,
                completion.OperationId, completion.MinLogIndex, completion.MaxLogIndex);
            return;
        }

        // ── Pending-operation fence ────────────────────────────────────────────
        // Use the envelope OperationId (authoritative) as the lookup key.
        // All operation types that carry per-operation data in pending (leader and
        // follower paths) require the pending entry: a completion for an operation
        // that was never registered — or was already processed — must not drive
        // further state transitions; that would create orphaned proposals and
        // mis-routed client replies.  Only Compaction is fire-and-forget.
        bool found = pendingWalOperations.Remove(completion.OperationId, out RaftPendingWalOperation? pending);

        if (!found && completion.OperationType is
            WALWriteOperationType.LeaderPropose or
            WALWriteOperationType.LeaderCommit or
            WALWriteOperationType.LeaderRollback or
            WALWriteOperationType.FollowerAppend)
        {
            logger.LogWarning(
                "[{LocalEndpoint}/{PartitionId}/{State}] WAL completion op {OperationId} ({Type}) is not in pendingWalOperations; discarding unknown/superseded completion.",
                host.LocalEndpoint, host.PartitionId, nodeState,
                completion.OperationId, completion.OperationType);
            return;
        }

        // ── Min-log cross-check against pending entry ──────────────────────────
        if (pending?.Logs is { Count: > 0 } pendingLogs && completion.MinLogIndex >= 0)
        {
            long actualMin = pendingLogs.Min(l => l.Id);
            if (actualMin != completion.MinLogIndex)
            {
                logger.LogWarning(
                    "[{LocalEndpoint}/{PartitionId}/{State}] WAL completion op {OperationId} min-log-index mismatch: envelope {EnvelopeMin} vs actual {ActualMin}; discarding.",
                    host.LocalEndpoint, host.PartitionId, nodeState,
                    completion.OperationId, completion.MinLogIndex, actualMin);
                return;
            }
        }

        switch (completion.OperationType)
        {
            case WALWriteOperationType.LeaderPropose:
                CompleteLeaderPropose(completion, pending);
                break;

            case WALWriteOperationType.LeaderCommit:
                CompleteLeaderCommit(completion, pending);
                break;

            case WALWriteOperationType.LeaderRollback:
                CompleteLeaderRollback(completion, pending);
                break;

            case WALWriteOperationType.FollowerAppend:
                await CompleteFollowerAppend(completion, pending).ConfigureAwait(false);
                break;

            case WALWriteOperationType.Compaction:
            default:
                CompleteReply(pending?.ReplyCorrelationId, RaftResponseStatic.NoneResponse);
                break;
        }
    }

    /// <summary>
    /// Completes a leader propose WAL write by broadcasting the proposed entries to all peers.
    /// <para>
    /// The live-replication broadcast deliberately carries no Log Matching anchors. A follower that
    /// is transiently behind (e.g. a node still catching up during a concurrent join) would reject
    /// an anchored live proposal with <see cref="RaftOperationStatus.LogMismatch"/>, but the
    /// live-proposal quorum path has no recovery for a rejected proposal — it simply never reaches
    /// quorum and times out (<c>ProposalTimeout</c>), and under load this livelocks. Log Matching is
    /// therefore enforced only on the backfill path, which has <c>nextIndex</c> backtracking to
    /// recover; the leader never ships a non-contiguous live batch, so contiguity holds by
    /// construction on this path.
    /// </para>
    /// </summary>
    private void CompleteLeaderPropose(RaftWalCompletion completion, RaftPendingWalOperation? pending)
    {
        HLCTimestamp ticketId = pending?.TicketId ?? HLCTimestamp.Zero;
        List<RaftLog> logs = pending?.Logs ?? [];
        bool autoCommit = pending?.AutoCommit ?? false;

        if (completion.Status != RaftOperationStatus.Success)
        {
            CompleteReply(pending?.ReplyCorrelationId, new(RaftResponseType.None, completion.Status, ticketId));
            return;
        }

        RaftProposalQuorum proposalQuorum = RaftProposalQuorumPool.Rent(logs, autoCommit, ticketId);

        // Register the local leader as a voter participant and mark it completed immediately.
        // Must be done via AddExpectedNodeCompletion so MarkNodeCompleted (which now only
        // updates existing keys) correctly counts the self-vote in the quorum denominator.
        proposalQuorum.AddExpectedNodeCompletion(host.LocalEndpoint);
        proposalQuorum.MarkNodeCompleted(host.LocalEndpoint);

        foreach (RaftNode node in host.Nodes)
        {
            if (node.Endpoint == host.LocalEndpoint)
                throw new RaftException("Corrupted nodes");

            // Learners receive log entries for catch-up but must not count toward quorum.
            // Only add voters to the quorum set; AppendLogToNode is called for all nodes.
            if (host.IsVoter(node.Endpoint))
                proposalQuorum.AddExpectedNodeCompletion(node.Endpoint);
            AppendLogToNode(node, ticketId, logs);
        }

        if (!activeProposals.TryAdd(ticketId, proposalQuorum))
        {
            CompleteReply(pending?.ReplyCorrelationId, new(RaftResponseType.None, RaftOperationStatus.Errored, HLCTimestamp.Zero));
            return;
        }

        if (logger.IsEnabled(LogLevel.Debug))
            logger.LogDebugProposedLogs(host.LocalEndpoint, host.PartitionId, nodeState, ticketId, string.Join(',', logs.Select(x => x.Id.ToString())));

        // Single-voter leader (no voter peers): the self-completion above already satisfies
        // quorum, but no voter ack will arrive to drive CompleteAppendLogsAsync. Drive the
        // Completed → (auto)commit transition here. Guarded to voter-only peers so learner-only
        // peers (which never ack for quorum) don't silently prevent single-voter commit.
        if (!host.Nodes.Any(n => host.IsVoter(n.Endpoint)))
        {
            proposalQuorum.SetState(RaftProposalState.Completed);

            if (autoCommit)
            {
                WALWriteOperation commitOperation = wal.EnqueueCommit(proposalQuorum.Logs);
                pendingWalOperations[commitOperation.OperationId] = new()
                {
                    Proposal = proposalQuorum,
                    TicketId = ticketId
                };
            }
        }

        CompleteReply(pending?.ReplyCorrelationId, new(RaftResponseType.None, RaftOperationStatus.Success, ticketId));
    }

    /// <summary>
    /// Commits the proposal locally and broadcasts the committed entries to all peers.
    /// <para>
    /// The commit broadcast deliberately carries no Log Matching anchors: it re-ships ids that
    /// were already validated and accepted by each follower during the (anchored) propose, so a
    /// per-commit anchor adds no safety. It would, however, force a WAL term read
    /// (<c>GetAnyTermAtAsync</c>) on this hot completion path before fan-out, which stalls commit
    /// propagation under load and caused proposal timeouts. Divergence is detected and repaired on
    /// the propose and backfill paths, which remain anchored.
    /// </para>
    /// </summary>
    private void CompleteLeaderCommit(RaftWalCompletion completion, RaftPendingWalOperation? pending)
    {
        RaftProposalQuorum? proposal = pending?.Proposal;
        HLCTimestamp ticketId = pending?.TicketId ?? HLCTimestamp.Zero;

        if (completion.Status != RaftOperationStatus.Success || proposal is null)
        {
            logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Couldn't commit proposal {Timestamp}", host.LocalEndpoint, host.PartitionId, nodeState, ticketId);
            CompleteReply(pending?.ReplyCorrelationId, new(RaftResponseType.None, completion.Status, 0));
            return;
        }

        proposal.SetState(RaftProposalState.Committed);
        HLCTimestamp currentTime = host.HybridLogicalClock.TrySendOrLocalEvent(host.LocalNodeId);

        if (completion.MaxLogIndex > localCommittedIndex)
            localCommittedIndex = completion.MaxLogIndex;

        // Send committed entries to ALL peers (voters + learners). proposal.Nodes only tracks
        // quorum voters; learners were excluded from quorum but still need log delivery so their
        // WAL stays in sync. host.Nodes already excludes self, so no self-skip is needed here.
        foreach (RaftNode node in host.Nodes)
            AppendLogToNode(node, ticketId, proposal.Logs);

        if (logger.IsEnabled(LogLevel.Debug))
            logger.LogDebugCommittedLogs(
                host.LocalEndpoint,
                host.PartitionId,
                nodeState,
                ticketId,
                string.Join(',', proposal.Logs.Select(x => x.Id.ToString())),
                (currentTime - proposal.StartTimestamp).TotalMilliseconds
            );

        wal.NotifyCommitted();

        CompleteReply(pending?.ReplyCorrelationId, new(RaftResponseType.None, RaftOperationStatus.Success, completion.MaxLogIndex));
    }

    /// <summary>
    /// Rolls back the proposal locally and broadcasts the rolled-back entries to all peers.
    /// Like <see cref="CompleteLeaderCommit"/>, this delivery carries no Log Matching anchors:
    /// it targets ids the follower already saw during the anchored propose, and adding a WAL term
    /// read on this completion path would stall propagation. LMP remains enforced on propose/backfill.
    /// </summary>
    private void CompleteLeaderRollback(RaftWalCompletion completion, RaftPendingWalOperation? pending)
    {
        RaftProposalQuorum? proposal = pending?.Proposal;
        HLCTimestamp ticketId = pending?.TicketId ?? HLCTimestamp.Zero;

        if (completion.Status != RaftOperationStatus.Success || proposal is null)
        {
            logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Couldn't rollback proposal {Timestamp}", host.LocalEndpoint, host.PartitionId, nodeState, ticketId);
            CompleteReply(pending?.ReplyCorrelationId, new(RaftResponseType.None, completion.Status, 0));
            return;
        }

        proposal.SetState(RaftProposalState.RolledBack);

        // Same as CompleteLeaderCommit: deliver rollback to all peers, not just quorum voters.
        foreach (RaftNode node in host.Nodes)
            AppendLogToNode(node, ticketId, proposal.Logs);

        if (logger.IsEnabled(LogLevel.Debug))
            logger.LogDebugRolledbackLogs(
                host.LocalEndpoint,
                host.PartitionId,
                nodeState,
                ticketId,
                string.Join(',', proposal.Logs.Select(x => x.Id.ToString()))
            );

        CompleteReply(pending?.ReplyCorrelationId, new(RaftResponseType.None, RaftOperationStatus.Success, completion.MaxLogIndex));
    }

    /// <summary>
    /// Finalises a follower append after the WAL write completes. Dispatches each committed
    /// log entry to the appropriate callback: entries on P0 with <c>LogType == "_RaftSystem"</c>
    /// go to <c>InvokeSystemReplicationReceived</c> (system coordinator); all other entries —
    /// including non-system types on P0 — go to <c>InvokeReplicationReceived</c> (consumer).
    /// This type-based routing is what allows P0 to host consumer data alongside coordinator
    /// entries without any WAL format change.
    /// </summary>
    private async Task CompleteFollowerAppend(RaftWalCompletion completion, RaftPendingWalOperation? pending)
    {
        string endpoint = pending!.Endpoint ?? "";
        long leaderTerm = completion.Term;
        HLCTimestamp timestamp = pending.Timestamp;
        long committedIndex = completion.Status == RaftOperationStatus.Success ? completion.MaxLogIndex : -1;

        if (completion.Status == RaftOperationStatus.Success)
        {
            foreach (RaftLog log in pending.Logs ?? [])
            {
                if (log.Type != RaftLogType.Committed)
                    continue;

                if (host.PartitionId == RaftSystemConfig.SystemPartition && log.LogType == RaftSystemConfig.RaftLogType)
                {
                    if (!await host.InvokeSystemReplicationReceived(host.PartitionId, log).ConfigureAwait(false))
                        host.InvokeReplicationError(host.PartitionId, log);
                }
                else
                {
                    if (!await host.InvokeReplicationReceived(host.PartitionId, log).ConfigureAwait(false))
                        host.InvokeReplicationError(host.PartitionId, log);
                }
            }

            wal.NotifyCommitted();
        }

        if (!string.IsNullOrEmpty(endpoint))
        {
            host.EnqueueResponse(endpoint, new(
                RaftResponderRequestType.CompleteAppendLogs,
                new(endpoint),
                new CompleteAppendLogsRequest(host.PartitionId, leaderTerm, timestamp, host.LocalEndpoint, completion.Status, committedIndex)
            ));
        }

        CompleteReply(pending.ReplyCorrelationId, RaftResponseStatic.NoneResponse);
    }

    /// <summary>
    /// Checks whether a proposal has been completed/committed or not.
    /// </summary>
    /// <param name="timestamp"></param>
    /// <param name="autoCommit"></param>
    /// <returns></returns>
    public (RaftProposalTicketState state, long commitIndex) CheckTicketCompletion(HLCTimestamp timestamp)
    {
        if (!activeProposals.TryGetValue(timestamp, out RaftProposalQuorum? proposal))
            return (RaftProposalTicketState.NotFound, -1);

        if (proposal is { AutoCommit: false, State: RaftProposalState.Completed } or { AutoCommit: true, State: RaftProposalState.Committed } or { AutoCommit: false, State: RaftProposalState.Committed })
            return (RaftProposalTicketState.Committed, proposal.LastLogIndex);

        return (RaftProposalTicketState.Proposed, -1);
    }

    /// <summary>
    /// Background task: exports the partition state via <see cref="IRaftStateMachineTransfer.ExportRange"/>
    /// and ships it to the lagging follower in bounded chunks so no single message exceeds the
    /// gRPC default receive limit.  On success it posts <see cref="RaftRequestType.SnapshotInstalled"/>
    /// back to the executor so <see cref="CompleteSnapshotInstalled"/> can safely advance
    /// <c>lastCommitIndexes</c> under the single-owner guarantee.
    /// Always removes <paramref name="node"/>'s endpoint from <see cref="pendingSnapshotEndpoints"/>
    /// when done so a retry can be attempted on the next heartbeat cycle.
    /// </summary>
    private async Task TrySendSnapshotAsync(RaftNode node, long snapshotIndex)
    {
        // Stay well under gRPC's 4 MB default with room for message framing overhead.
        const int chunkSize = 3 * 1024 * 1024;

        try
        {
            IRaftStateMachineTransfer? transfer = host.StateMachineTransfer;
            if (transfer is null)
                return;

            RaftSplitPlan plan = new() { TargetPartitionId = host.PartitionId };
            Stream snapshot;
            try
            {
                snapshot = await transfer.ExportRange(plan, snapshotIndex, CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                logger.LogError(
                    "[{LocalEndpoint}/{PartitionId}/{State}] TrySendSnapshotAsync: ExportRange failed for {Endpoint}: {Message}",
                    host.LocalEndpoint, host.PartitionId, nodeState, node.Endpoint, ex.Message);
                return;
            }

            string sessionId = Guid.NewGuid().ToString("N");
            byte[] buffer = new byte[chunkSize];
            int chunkIndex = 0;
            bool success = false;

            await using (snapshot.ConfigureAwait(false))
            {
                while (true)
                {
                    int bytesRead = await ReadExactAsync(snapshot, buffer, chunkSize, CancellationToken.None).ConfigureAwait(false);
                    bool isLast = bytesRead < chunkSize;

                    SnapshotRequest chunk = new()
                    {
                        SessionId = sessionId,
                        PartitionId = host.PartitionId,
                        SnapshotIndex = snapshotIndex,
                        FollowerEndpoint = node.Endpoint,
                        ChunkIndex = chunkIndex,
                        IsLast = isLast,
                        Data = buffer[..bytesRead],
                    };

                    SnapshotResponse response = await host.SendInstallSnapshotAsync(node, chunk, CancellationToken.None).ConfigureAwait(false);
                    if (!response.Success)
                    {
                        logger.LogWarning(
                            "[{LocalEndpoint}/{PartitionId}/{State}] Snapshot chunk {ChunkIndex} to {Endpoint} was rejected",
                            host.LocalEndpoint, host.PartitionId, nodeState, chunkIndex, node.Endpoint);
                        return;
                    }

                    if (isLast)
                    {
                        success = true;
                        break;
                    }

                    chunkIndex++;
                }
            }

            if (success)
            {
                logger.LogInfoSnapshotInstalled(host.LocalEndpoint, host.PartitionId, nodeState, node.Endpoint, snapshotIndex, chunkIndex + 1);

                // Notify the executor thread so it can safely update lastCommitIndexes.
                postToExecutor?.Invoke(new RaftRequest(
                    RaftRequestType.SnapshotInstalled,
                    commitIndex: snapshotIndex,
                    endpoint: node.Endpoint));
            }
        }
        catch (Exception ex)
        {
            logger.LogError(
                "[{LocalEndpoint}/{PartitionId}/{State}] TrySendSnapshotAsync: unhandled error for {Endpoint}: {Message}",
                host.LocalEndpoint, host.PartitionId, nodeState, node.Endpoint, ex.Message);
        }
        finally
        {
            pendingSnapshotEndpoints.TryRemove(node.Endpoint, out _);
        }
    }

    /// <summary>
    /// Reads up to <paramref name="count"/> bytes from <paramref name="stream"/> into
    /// <paramref name="buffer"/>, issuing repeated reads until the buffer is full or the
    /// stream is exhausted.  Returns the total number of bytes read.
    /// </summary>
    private static async ValueTask<int> ReadExactAsync(Stream stream, byte[] buffer, int count, CancellationToken ct)
    {
        int total = 0;
        while (total < count)
        {
            int n = await stream.ReadAsync(buffer.AsMemory(total, count - total), ct).ConfigureAwait(false);
            if (n == 0) break;
            total += n;
        }
        return total;
    }

    /// <summary>
    /// Advances <c>lastCommitIndexes</c> for <paramref name="endpoint"/> to <paramref name="snapshotIndex"/>
    /// after the background snapshot task confirmed successful installation on the follower.
    /// Always called on the executor thread via the <see cref="postToExecutor"/> callback.
    /// </summary>
    public void CompleteSnapshotInstalled(string endpoint, long snapshotIndex)
    {
        if (!lastCommitIndexes.TryGetValue(endpoint, out long current) || snapshotIndex > current)
            lastCommitIndexes[endpoint] = snapshotIndex;
    }

}
