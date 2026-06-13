using System.Diagnostics;
using Kommander.Data;
using Kommander.Diagnostics;
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
    private readonly Dictionary<long, string> expectedLeaders = [];
    private readonly Dictionary<HLCTimestamp, RaftProposalQuorum> activeProposals = [];
    private readonly Dictionary<long, Scheduling.RaftPendingWalOperation> pendingWalOperations = [];

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
    /// Periodically, it checks the leadership status of the partition and, based on timeouts,
    /// decides whether to start a new election process.
    /// </summary>
    public async Task CheckPartitionLeadershipAsync()
    {
        HLCTimestamp currentTime = host.HybridLogicalClock.TrySendOrLocalEvent(host.LocalNodeId);

        switch (nodeState)
        {
            // if node is leader just send hearthbeats every Configuration.HeartbeatInterval
            case RaftNodeState.Leader:
            {
                if (currentTime != HLCTimestamp.Zero && ((currentTime - lastHeartbeat) >= host.Configuration.HeartbeatInterval))
                    await SendHeartbeat(false).ConfigureAwait(false);
            
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
                localCommittedIndex = -1;
                activeProposals.Clear();
                ResetPreVoteRound();

                await host.InvokeLeaderChanged(host.PartitionId, "");
                return;
            
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
        localCommittedIndex = -1;
        activeProposals.Clear();

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
            nodeState = RaftNodeState.Leader;
            host.Leader = host.LocalEndpoint;
            lastHeartbeat = host.HybridLogicalClock.TrySendOrLocalEvent(host.LocalNodeId);

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

    private async Task StartElectionAsync(HLCTimestamp currentTime, bool ignoreRecentVoteCooldown)
    {
        if (host.LocalRole != ClusterMemberRole.Voter)
        {
            logger.LogDebug("[{LocalEndpoint}/{PartitionId}/{State}] Suppressing election: local role is {Role}, not Voter", host.LocalEndpoint, host.PartitionId, nodeState, host.LocalRole);
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
            nodeState = RaftNodeState.Leader;
            host.Leader = host.LocalEndpoint;
            lastHeartbeat = host.HybridLogicalClock.TrySendOrLocalEvent(host.LocalNodeId);

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
            logger.LogDebug("[{LocalEndpoint}/{PartitionId}/{State}] Suppressing pre-vote: local role is {Role}, not Voter", host.LocalEndpoint, host.PartitionId, nodeState, host.LocalRole);
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

        logger.LogInformation("[{LocalEndpoint}/{PartitionId}/{State}] Starting pre-vote round for Term={PreVoteTerm}", host.LocalEndpoint, host.PartitionId, nodeState, preVoteTerm);

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
            if (nodeState == RaftNodeState.Leader
                && localCommittedIndex >= 0
                && lastCommitIndexes.TryGetValue(node.Endpoint, out long followerMaxLog)
                && localCommittedIndex - followerMaxLog > host.Configuration.BackfillThreshold)
            {
                List<RaftLog> backfill = await wal.GetRangeAsync(
                    followerMaxLog + 1,
                    host.Configuration.MaxBackfillEntriesPerRound
                ).ConfigureAwait(false);

                if (backfill.Count > 0)
                {
                    logger.LogDebug(
                        "[{LocalEndpoint}/{PartitionId}/{State}] Backfilling {Count} entries to {Endpoint} from={From} leaderCommitted={LeaderCommitted}",
                        host.LocalEndpoint, host.PartitionId, nodeState,
                        backfill.Count, node.Endpoint, followerMaxLog + 1, localCommittedIndex);
                    AppendLogToNode(node, lastHeartbeat, backfill);
                    continue;
                }
                // Empty batch: the leader has compacted past followerMaxLog+1. The follower
                // cannot catch up via log replay alone and needs a snapshot transfer. Fall
                // through to a plain heartbeat until snapshot-based recovery is implemented
                // (see elastic-partitions-spec.md and GetRangeAsync for the full limitation).
            }

            AppendLogToNode(node, lastHeartbeat, null);
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
            logger.LogCritical("[{LocalEndpoint}/{PartitionId}/{State}] Same node id was found in the cluster {NodeId} {RemoteNodeId}", host.LocalEndpoint, host.PartitionId, nodeState, host.LocalNodeId, remoteNodeId);
            
            Environment.Exit(1);
            return;
        }
        
        logger.LogInformation("[{LocalEndpoint}/{PartitionId}/{State}] Received handshake from {Endpoint}/{RemoteNodeId}. WAL log at {Index}.", host.LocalEndpoint, host.PartitionId, nodeState, endpoint, remoteNodeId, remoteMaxLogId);
        
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
            
            logger.LogDebug("[{LocalEndpoint}/{PartitionId}/{State}] Sending handshake to {Node} #{Number}", host.LocalEndpoint, host.PartitionId, nodeState, node.Endpoint, ++number);
            
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
                logger.LogDebug("[{LocalEndpoint}/{PartitionId}/{State}] Denying pre-vote to {Endpoint} Term={Term}: endpoint is not a committed voter", host.LocalEndpoint, host.PartitionId, nodeState, node.Endpoint, voteTerm);
                return;
            }

            // A live leader never helps a challenger unseat it.
            if (nodeState == RaftNodeState.Leader)
            {
                logger.LogDebug("[{LocalEndpoint}/{PartitionId}/{State}] Denying pre-vote to {Endpoint} Term={Term}: we are the leader", host.LocalEndpoint, host.PartitionId, nodeState, node.Endpoint, voteTerm);
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
                    logger.LogDebug("[{LocalEndpoint}/{PartitionId}/{State}] Denying pre-vote to {Endpoint} Term={Term}: current leader {Leader} still fresh", host.LocalEndpoint, host.PartitionId, nodeState, node.Endpoint, voteTerm, preVoteExpectedLeader);
                    return;
                }
            }

            // The hypothetical term must not be stale.
            if (voteTerm < currentTerm)
            {
                logger.LogDebug("[{LocalEndpoint}/{PartitionId}/{State}] Denying pre-vote to {Endpoint}: stale Term={Term} < CurrentTerm={CurrentTerm}", host.LocalEndpoint, host.PartitionId, nodeState, node.Endpoint, voteTerm, currentTerm);
                return;
            }

            long preVoteLocalMaxId = await wal.GetMaxLogAsync().ConfigureAwait(false);

            // The candidate's log must be at least as up-to-date. Note: `>=`, not the real-vote
            // path's strict rejection — a pre-vote only probes electability, it doesn't elect.
            if (remoteMaxLogId < preVoteLocalMaxId)
            {
                logger.LogDebug("[{LocalEndpoint}/{PartitionId}/{State}] Denying pre-vote to {Endpoint} Term={Term}: outdated log RemoteMaxId={RemoteId} LocalMaxId={MaxId}", host.LocalEndpoint, host.PartitionId, nodeState, node.Endpoint, voteTerm, remoteMaxLogId, preVoteLocalMaxId);
                return;
            }

            logger.LogDebug("[{LocalEndpoint}/{PartitionId}/{State}] Granting pre-vote to {Endpoint} Term={Term}", host.LocalEndpoint, host.PartitionId, nodeState, node.Endpoint, voteTerm);

            VoteRequest preGrant = new(host.PartitionId, voteTerm, preVoteLocalMaxId, timestamp, host.LocalEndpoint, preVote: true);
            host.EnqueueResponse(node.Endpoint, new(RaftResponderRequestType.Vote, node, preGrant));
            return;
        }

        if (!host.IsVoter(node.Endpoint))
        {
            logger.LogDebug("[{LocalEndpoint}/{PartitionId}/{State}] Denying vote to {Endpoint} Term={Term}: endpoint is not a committed voter", host.LocalEndpoint, host.PartitionId, nodeState, node.Endpoint, voteTerm);
            return;
        }

        if (votes.ContainsKey(voteTerm))
        {
            logger.LogInformation("[{LocalEndpoint}/{PartitionId}/{State}] Received request to vote from {Endpoint} but already voted in that Term={Term}. Ignoring...", host.LocalEndpoint, host.PartitionId, nodeState, node.Endpoint, voteTerm);
            return;
        }

        if (nodeState != RaftNodeState.Follower && voteTerm == currentTerm)
        {
            logger.LogInformation("[{LocalEndpoint}/{PartitionId}/{State}] Received request to vote from {Endpoint} but we're candidate or leader on the same Term={Term}. Ignoring...", host.LocalEndpoint, host.PartitionId, nodeState, node.Endpoint, voteTerm);
            return;
        }

        if (currentTerm > voteTerm)
        {
            logger.LogInformation("[{LocalEndpoint}/{PartitionId}/{State}] Received request to vote on previous term from {Endpoint} Term={Term}. Ignoring...", host.LocalEndpoint, host.PartitionId, nodeState, node.Endpoint, voteTerm);
            return;
        }

        string expectedLeader = expectedLeaders.GetValueOrDefault(voteTerm, "");
        
        if (!string.IsNullOrEmpty(expectedLeader) && expectedLeader != node.Endpoint)
        {
            logger.LogInformation("[{LocalEndpoint}/{PartitionId}/{State}] Received request to vote from {Endpoint} but we already voted for {ExpectedLeader}. Ignoring...", host.LocalEndpoint, host.PartitionId, nodeState, node.Endpoint, expectedLeader);
            return;
        }
        
        long localMaxId = await wal.GetMaxLogAsync().ConfigureAwait(false);

        if (localMaxId > remoteMaxLogId)
        {
            // Reject a real vote for a candidate whose log is behind ours (Raft §5.4.1). We do NOT
            // bump our own term here: with PreVote (§9.6) in place a stale candidate can no longer
            // reach this real-vote path with an inflated term, so the old `currentTerm++` heuristic
            // that forced us to be elected is no longer needed and only risked spurious term churn.
            logger.LogInformation("[{LocalEndpoint}/{PartitionId}/{State}] Received request to vote on outdated log from {Endpoint} RemoteMaxId={RemoteId} LocalMaxId={MaxId}. Ignoring...", host.LocalEndpoint, host.PartitionId, nodeState, node.Endpoint, remoteMaxLogId, localMaxId);
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
            logger.LogDebug("[{LocalEndpoint}/{PartitionId}/{State}] Ignoring {PreVote}vote grant from {Endpoint} Term={Term}: endpoint is not a committed voter", host.LocalEndpoint, host.PartitionId, nodeState, preVote ? "pre-" : "", endpoint, voteTerm);
            return;
        }

        if (preVote)
        {
            // Tally a pre-grant. Placed before the Follower early-return because a node running a
            // pre-vote round is still a Follower. Touches only pre-vote state until quorum promotes.
            if (electionPhase != RaftElectionPhase.PreVote || voteTerm != preVoteTerm)
            {
                logger.LogDebug("[{LocalEndpoint}/{PartitionId}/{State}] Ignoring pre-vote grant from {Endpoint} Term={Term}: no matching open round (phase={Phase} preVoteTerm={PreVoteTerm})", host.LocalEndpoint, host.PartitionId, nodeState, endpoint, voteTerm, electionPhase, preVoteTerm);
                return;
            }

            preVotes.Add(endpoint);
            int preVoteQuorum = Math.Max(2, ((host.Nodes.Count + 1) / 2) + 1);

            logger.LogInformation("[{LocalEndpoint}/{PartitionId}/{State}] Received pre-vote from {Endpoint} Term={Term} PreVotes={PreVotes} Quorum={Quorum}/{Total}", host.LocalEndpoint, host.PartitionId, nodeState, endpoint, voteTerm, preVotes.Count, preVoteQuorum, host.Nodes.Count + 1);

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
            logger.LogInformation("[{LocalEndpoint}/{PartitionId}/{State}] Received vote from {Node} but we didn't ask for it Term={Term}. Ignoring...", host.LocalEndpoint, host.PartitionId, nodeState, endpoint, voteTerm);
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
            
            logger.LogInformation("[{LocalEndpoint}/{PartitionId}/{State}] Received vote from {Node} but already declared as leader Term={Term}. Ignoring...", host.LocalEndpoint, host.PartitionId, nodeState, endpoint, voteTerm);
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
        int quorum = Math.Max(2, ((host.Nodes.Count + 1) / 2) + 1);
        
        lastCommitIndexes[endpoint] = remoteMaxLogId;
        startCommitIndexes[endpoint] = remoteMaxLogId;
        
        logger.LogInformation(
            "[{LocalEndpoint}/{PartitionId}/{State}] Received vote from {Endpoint} Term={Term} Votes={Votes} Quorum={Quorum}/{Total} RemoteCommitId={CommitId} Local={LocalCommitId}", 
            host.LocalEndpoint, 
            host.PartitionId, 
            nodeState, 
            endpoint, 
            voteTerm, 
            numberVotes, 
            quorum, 
            host.Nodes.Count + 1, 
            remoteMaxLogId, 
            maxLogResponse
        );

        if (numberVotes < quorum)
            return;
        
        // Here quorum was achieved and we can mark ourselves as leader in the partition
        nodeState = RaftNodeState.Leader;
        host.Leader = host.LocalEndpoint;

        lastHeartbeat = host.HybridLogicalClock.TrySendOrLocalEvent(host.LocalNodeId);

        logger.LogInformation(
            "[{LocalEndpoint}/{PartitionId}/{State}] Received vote from {Endpoint} and proclamed leader in {Elapsed}ms Term={Term} Votes={Votes} Quorum={Quorum}/{Total} RemoteCommitId={CommitId} Local={LocalCommitId}", 
            host.LocalEndpoint, 
            host.PartitionId, 
            nodeState, 
            endpoint, 
            (lastHeartbeat - votingStartedAt).TotalMilliseconds, 
            voteTerm, 
            numberVotes, 
            quorum, 
            host.Nodes.Count + 1,
            remoteMaxLogId, 
            maxLogResponse
        );

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
    public Task AppendLogsAsync(string endpoint, long term, HLCTimestamp timestamp, List<RaftLog>? logs, ulong? replyCorrelationId = null) =>
        AppendLogsCoreAsync(endpoint, term, timestamp, logs, replyCorrelationId);

    private async Task AppendLogsCoreAsync(
        string endpoint,
        long leaderTerm,
        HLCTimestamp timestamp,
        List<RaftLog>? logs,
        ulong? replyCorrelationId = null
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
        
        // Validate if we voted in the current term and we expect a different leader
        string expectedLeader = expectedLeaders.GetValueOrDefault(leaderTerm, "");

        if (endpoint == expectedLeader || string.IsNullOrEmpty(expectedLeader))
        {
            if (host.Leader != endpoint)
            {
                logger.LogInformation("[{LocalEndpoint}/{PartitionId}/{State}] Leader is now {Endpoint} LeaderTerm={Term}", host.LocalEndpoint, host.PartitionId, nodeState, endpoint, leaderTerm);

                nodeState = RaftNodeState.Follower;
                host.Leader = endpoint;
                currentTerm = leaderTerm;
                lastCommitIndexes.Clear();
                localCommittedIndex = -1;
                activeProposals.Clear();
                expectedLeaders.TryAdd(leaderTerm, endpoint);
                
                await host.InvokeLeaderChanged(host.PartitionId, endpoint);
            }
        }
        else
        {
            if (endpoint != expectedLeader)
            {
                logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Received logs from another leader {Endpoint} (current leader {CurrentLeader}) Term={Term}. Ignoring...", host.LocalEndpoint, host.PartitionId, nodeState, endpoint, expectedLeader, leaderTerm);
                
                host.EnqueueResponse(endpoint, new(
                    RaftResponderRequestType.CompleteAppendLogs, 
                    new(endpoint), 
                    new CompleteAppendLogsRequest(host.PartitionId, leaderTerm, timestamp, host.LocalEndpoint, RaftOperationStatus.LogsFromAnotherLeader, -1)
                ));
                return;
            }
        }
        
        lastHeartbeat = host.HybridLogicalClock.ReceiveEvent(host.LocalNodeId, timestamp);
        
        host.UpdateLastNodeActivity(expectedLeader, host.PartitionId, lastHeartbeat);

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
        
        IReadOnlyList<RaftNode> nodes = host.Nodes;
        
        if (nodes.Count == 0)
        {
            logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] No quorum available to propose logs", host.LocalEndpoint, host.PartitionId, nodeState);
            
            return (RaftOperationStatus.Errored, HLCTimestamp.Zero);
        }
        
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
    /// </summary>
    /// <param name="node"></param>
    /// <param name="timestamp"></param>
    /// <param name="logs"></param>
    private void AppendLogToNode(RaftNode node, HLCTimestamp timestamp, List<RaftLog>? logs)
    {
        AppendLogsRequest request;

        if (logs is null || logs.Count == 0)
            request = new(host.PartitionId, currentTerm, timestamp, host.LocalEndpoint);
        else
        {
            /*long lastCommitIndex = lastCommitIndexes.GetValueOrDefault(node.Endpoint, 0);

            lastCommitIndex -= 3;
            if (lastCommitIndex < 0)
                lastCommitIndex = 0;

            RaftWALResponse getRangeResponse = await walActor.Ask(new(RaftWALActionType.GetRange, currentTerm, lastCommitIndex)).ConfigureAwait(false);
            if (getRangeResponse.Logs is null)
            {
                logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Failed to get logs range {Timestamp} From={From}", host.LocalEndpoint, host.PartitionId, nodeState, timestamp, lastCommitIndex);

                return;
            }*/

            request = new(host.PartitionId, currentTerm, timestamp, host.LocalEndpoint, logs);

            if (logger.IsEnabled(LogLevel.Debug))
                logger.LogDebug(
                    "[{LocalEndpoint}/{PartitionId}/{State}] Enqueued entries for {Endpoint} {Timestamp} From={From} Logs={Logs}", 
                    host.LocalEndpoint, 
                    host.PartitionId, 
                    nodeState, 
                    node.Endpoint, 
                    timestamp, 
                    0, 
                    string.Join(',', logs.Select(x => x.Id.ToString()))
                );
        }

        /*if (request.Logs is null || request.Logs.Count == 0)
        {
            host.ResponseBatcherActor.Send(new(RaftResponderRequestType.AppendLogs, node, request));
            return;
        }*/

        host.EnqueueResponse(node.Endpoint, new(RaftResponderRequestType.AppendLogs, node, request));
    }

    /// <summary>
    /// Called when a node completes an append log operation
    /// </summary>
    /// <param name="endpoint"></param>
    /// <param name="timestamp"></param>
    /// <param name="status"></param>
    /// <param name="committedIndex"></param>
    public ValueTask CompleteAppendLogsAsync(string endpoint, HLCTimestamp timestamp, RaftOperationStatus status, long committedIndex)
    {
        HLCTimestamp currentTime = host.HybridLogicalClock.ReceiveEvent(host.LocalNodeId, timestamp);

        if (endpoint != host.LocalEndpoint)
            host.UpdateLastNodeActivity(endpoint, host.PartitionId, currentTime);
        
        if (committedIndex > 0)
        {
            if (lastCommitIndexes.TryGetValue(endpoint, out long currentIndex))
            {
                if (committedIndex > currentIndex)
                    lastCommitIndexes[endpoint] = committedIndex;
            }
            else
                lastCommitIndexes[endpoint] = committedIndex;

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

            return ValueTask.CompletedTask;
        }

        if (!activeProposals.TryGetValue(timestamp, out RaftProposalQuorum? proposal))
            return ValueTask.CompletedTask;
        
        if (proposal.State != RaftProposalState.Incomplete)
            return ValueTask.CompletedTask;

        proposal.MarkNodeCompleted(endpoint);

        if (!proposal.HasQuorum())
        {
            logger.LogInfoProposalPartiallyCompletedAt(host.LocalEndpoint, host.PartitionId, nodeState, timestamp, (currentTime - proposal.StartTimestamp).TotalMilliseconds);
            return ValueTask.CompletedTask;
        }
        
        logger.LogInfoProposalCompletedAt(host.LocalEndpoint, host.PartitionId, nodeState, timestamp, (currentTime - proposal.StartTimestamp).TotalMilliseconds);

        proposal.SetState(RaftProposalState.Completed);

        if (!proposal.AutoCommit)
        {
            logger.LogInformation("[{LocalEndpoint}/{PartitionId}/{State}] Proposal {Timestamp} doesn't have auto-commit", host.LocalEndpoint, host.PartitionId, nodeState, timestamp);
            return ValueTask.CompletedTask;
        }

        WALWriteOperation operation = wal.EnqueueCommit(proposal.Logs);
        pendingWalOperations[operation.OperationId] = new()
        {
            Proposal = proposal,
            TicketId = timestamp
        };

        return ValueTask.CompletedTask;
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
        proposalQuorum.MarkNodeCompleted(host.LocalEndpoint);

        foreach (RaftNode node in host.Nodes)
        {
            if (node.Endpoint == host.LocalEndpoint)
                throw new RaftException("Corrupted nodes");

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

        // Single-node leader (no peers): the self-completion above already satisfies quorum, but no
        // peer ack will ever arrive to drive CompleteAppendLogsAsync. Drive the identical
        // Completed → (auto)commit transition here so autoCommit and the WAL completion envelope
        // behave exactly like the multi-node quorum path. Guarded by Nodes.Count == 0 so it can
        // never short-circuit a real quorum when peers exist.
        if (host.Nodes.Count == 0)
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

        foreach (string node in proposal.Nodes)
        {
            if (node == host.LocalEndpoint)
                continue;

            AppendLogToNode(new(node), ticketId, proposal.Logs);
        }

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

        foreach (string node in proposal.Nodes)
        {
            if (node == host.LocalEndpoint)
                continue;

            AppendLogToNode(new(node), ticketId, proposal.Logs);
        }

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

}
