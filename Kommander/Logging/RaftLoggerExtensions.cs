
using Kommander.Data;
using Kommander.System;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kommander.Logging;

/// <summary>
/// Provides extension methods for logging various Raft-specific events in a high-performant way
/// in implementations of the <see cref="IRaft"/> interface using the <see cref="ILogger"/> framework.
/// </summary>
public static partial class RaftLoggerExtensions
{
    [LoggerMessage(Level = LogLevel.Information, Message = "[{LocalEndpoint}/{PartitionId}/{State}] WAL restored at #{NextId} in {ElapsedMs}ms")]
    public static partial void LogInfoWalRestored(this ILogger<IRaft> logger, string localEndpoint, int partitionId, RaftNodeState state, long nextId, long elapsedMs);

    [LoggerMessage(Level = LogLevel.Information, Message = "[{LocalEndpoint}/{PartitionId}/{State}] Voting concluded after {Elapsed}ms. No quorum available")]
    public static partial void LogInfoVotingConcluded(this ILogger<IRaft> logger, string localEndpoint, int partitionId, RaftNodeState state, double elapsed);

    [LoggerMessage(Level = LogLevel.Warning, Message = "[{LocalEndpoint}/{PartitionId}/{State}] Voted to become leader after {LastHeartbeat}ms. Term={CurrentTerm}")]
    public static partial void LogWarnVotedToBecomeLeader(this ILogger<IRaft> logger, string localEndpoint, int partitionId, RaftNodeState state, double lastHeartbeat, long currentTerm);

    [LoggerMessage(Level = LogLevel.Debug, Message = "[{LocalEndpoint}/{PartitionId}/{State}] Proposed logs {Timestamp} Logs={Logs}")]
    public static partial void LogDebugProposedLogs(this ILogger<IRaft> logger, string localEndpoint, int partitionId, RaftNodeState state, HLCTimestamp timestamp, string logs);

    [LoggerMessage(Level = LogLevel.Information, Message = "[{LocalEndpoint}/{PartitionId}/{State}] Proposed checkpoint logs {Timestamp} Logs={Logs}")]
    public static partial void LogInfoProposedCheckpointLogs(this ILogger<IRaft> logger, string localEndpoint, int partitionId, RaftNodeState state, HLCTimestamp timestamp, int logs);

    [LoggerMessage(Level = LogLevel.Information, Message = "[{LocalEndpoint}/{PartitionId}/{State}] Successfully completed logs from {Endpoint} Timestamp={Timestamp} CommitedIndex={Index} Time={Elapsed}ms")]
    public static partial void LogInfoSuccessfullyCompletedLogs(this ILogger<IRaft> logger, string localEndpoint, int partitionId, RaftNodeState state, string endpoint, HLCTimestamp timestamp, long index, double elapsed);

    [LoggerMessage(Level = LogLevel.Information, Message = "[{LocalEndpoint}/{PartitionId}/{State}] Proposal partially completed at {Timestamp} Time={Elapsed}ms")]
    public static partial void LogInfoProposalPartiallyCompletedAt(this ILogger<IRaft> logger, string localEndpoint, int partitionId, RaftNodeState state, HLCTimestamp timestamp, double elapsed);

    [LoggerMessage(Level = LogLevel.Information, Message = "[{LocalEndpoint}/{PartitionId}/{State}] Proposal completed at {Timestamp} Time={Elapsed}ms")]
    public static partial void LogInfoProposalCompletedAt(this ILogger<IRaft> logger, string localEndpoint, int partitionId, RaftNodeState state, HLCTimestamp timestamp, double elapsed);

    [LoggerMessage(Level = LogLevel.Debug, Message = "[{LocalEndpoint}/{PartitionId}/{State}] Committed proposal {Timestamp} Logs={Logs} Time={Elapsed}ms")]
    public static partial void LogDebugCommittedLogs(this ILogger<IRaft> logger, string localEndpoint, int partitionId, RaftNodeState state, HLCTimestamp timestamp, string logs, double elapsed);

    [LoggerMessage(Level = LogLevel.Debug, Message = "[{LocalEndpoint}/{PartitionId}/{State}] Rolled back proposal {Timestamp} Logs={Logs}")]
    public static partial void LogDebugRolledbackLogs(this ILogger<IRaft> logger, string localEndpoint, int partitionId, RaftNodeState state, HLCTimestamp timestamp, string logs);

    [LoggerMessage(Level = LogLevel.Information, Message = "[{LocalEndpoint}/{PartitionId}/{State}] Asked {Endpoint} for votes on Term={CurrentTerm}")]
    public static partial void LogInfoAskedForVotes(this ILogger<IRaft> logger, string localEndpoint, int partitionId, RaftNodeState state, string endpoint, long currentTerm);

    [LoggerMessage(Level = LogLevel.Information, Message = "[{LocalEndpoint}/{PartitionId}/{State}] Sending vote to {Endpoint} on Term={CurrentTerm}")]
    public static partial void LogInfoSendingVote(this ILogger<IRaft> logger, string localEndpoint, int partitionId, RaftNodeState state, string endpoint, long currentTerm);

    [LoggerMessage(Level = LogLevel.Debug, Message = "[{LocalEndpoint}/{PartitionId}/{State}] Received logs from leader {Endpoint} with Term={Term} Timestamp={Timestamp} Logs={Logs}")]
    public static partial void LogDebugReceivedLogs(this ILogger<IRaft> logger, string localEndpoint, int partitionId, RaftNodeState state, string endpoint, long term, HLCTimestamp timestamp, string logs);

    [LoggerMessage(Level = LogLevel.Debug, Message = "[{Endpoint}/{PartitionId}] Committed log #{Id}")]
    public static partial void LogDebugCommittedLogs(this ILogger<IRaft> logger, string endpoint, int partitionId, long id);

    [LoggerMessage(Level = LogLevel.Debug, Message = "[{Endpoint}/{PartitionId}] Proposed log #{Id}")]
    public static partial void LogDebugProposedLogs(this ILogger<IRaft> logger, string endpoint, int partitionId, long id);

    [LoggerMessage(Level = LogLevel.Debug, Message = "[{Endpoint}] Write of {Batch} took {Elapsed}ms")]
    public static partial void LogDebugWriteOf(this ILogger<IRaft> logger, string endpoint, int batch, long elapsed);

    // ── RaftPartitionStateMachine ─────────────────────────────────────────

    [LoggerMessage(Level = LogLevel.Debug, Message = "[{LocalEndpoint}/{PartitionId}/{State}] Suppressing election: local role is {Role}, not Voter")]
    public static partial void LogDebugSuppressingElection(this ILogger<IRaft> logger, string localEndpoint, int partitionId, RaftNodeState state, ClusterMemberRole role);

    [LoggerMessage(Level = LogLevel.Debug, Message = "[{LocalEndpoint}/{PartitionId}/{State}] Suppressing pre-vote: local role is {Role}, not Voter")]
    public static partial void LogDebugSuppressingPreVote(this ILogger<IRaft> logger, string localEndpoint, int partitionId, RaftNodeState state, ClusterMemberRole role);

    [LoggerMessage(Level = LogLevel.Information, Message = "[{LocalEndpoint}/{PartitionId}/{State}] Starting pre-vote round for Term={PreVoteTerm}")]
    public static partial void LogInfoStartingPreVoteRound(this ILogger<IRaft> logger, string localEndpoint, int partitionId, RaftNodeState state, long preVoteTerm);

    [LoggerMessage(Level = LogLevel.Information, Message = "[{LocalEndpoint}/{PartitionId}/{State}] Starting snapshot transfer to {Endpoint} at index {Index}")]
    public static partial void LogInfoStartingSnapshotTransfer(this ILogger<IRaft> logger, string localEndpoint, int partitionId, RaftNodeState state, string endpoint, long index);

    [LoggerMessage(Level = LogLevel.Critical, Message = "[{LocalEndpoint}/{PartitionId}/{State}] Same node id was found in the cluster {NodeId} {RemoteNodeId}")]
    public static partial void LogCritSameNodeId(this ILogger<IRaft> logger, string localEndpoint, int partitionId, RaftNodeState state, int nodeId, int remoteNodeId);

    [LoggerMessage(Level = LogLevel.Information, Message = "[{LocalEndpoint}/{PartitionId}/{State}] Received handshake from {Endpoint}/{RemoteNodeId}. WAL log at {Index}.")]
    public static partial void LogInfoReceivedHandshake(this ILogger<IRaft> logger, string localEndpoint, int partitionId, RaftNodeState state, string endpoint, int remoteNodeId, long index);

    [LoggerMessage(Level = LogLevel.Debug, Message = "[{LocalEndpoint}/{PartitionId}/{State}] Sending handshake to {Node} #{Number}")]
    public static partial void LogDebugSendingHandshake(this ILogger<IRaft> logger, string localEndpoint, int partitionId, RaftNodeState state, string node, int number);

    [LoggerMessage(Level = LogLevel.Debug, Message = "[{LocalEndpoint}/{PartitionId}/{State}] LogMismatch from {Endpoint}: backtracking nextIndex {CurrentNext} → {NextIndex} (followerMax={CommittedIndex})")]
    public static partial void LogDebugBacktrackingNextIndex(this ILogger<IRaft> logger, string localEndpoint, int partitionId, RaftNodeState state, string endpoint, long currentNext, long nextIndex, long committedIndex);

    [LoggerMessage(Level = LogLevel.Debug, Message = "[{LocalEndpoint}/{PartitionId}/{State}] Denying pre-vote to {Endpoint} Term={Term}: endpoint is not a committed voter")]
    public static partial void LogDebugDenyingPreVoteNotVoter(this ILogger<IRaft> logger, string localEndpoint, int partitionId, RaftNodeState state, string endpoint, long term);

    [LoggerMessage(Level = LogLevel.Debug, Message = "[{LocalEndpoint}/{PartitionId}/{State}] Denying pre-vote to {Endpoint} Term={Term}: we are the leader")]
    public static partial void LogDebugDenyingPreVoteWeAreLeader(this ILogger<IRaft> logger, string localEndpoint, int partitionId, RaftNodeState state, string endpoint, long term);

    [LoggerMessage(Level = LogLevel.Debug, Message = "[{LocalEndpoint}/{PartitionId}/{State}] Denying pre-vote to {Endpoint} Term={Term}: current leader {Leader} still fresh")]
    public static partial void LogDebugDenyingPreVoteLeaderFresh(this ILogger<IRaft> logger, string localEndpoint, int partitionId, RaftNodeState state, string endpoint, long term, string leader);

    [LoggerMessage(Level = LogLevel.Debug, Message = "[{LocalEndpoint}/{PartitionId}/{State}] Denying pre-vote to {Endpoint}: stale Term={Term} < CurrentTerm={CurrentTerm}")]
    public static partial void LogDebugDenyingPreVoteStaleTerm(this ILogger<IRaft> logger, string localEndpoint, int partitionId, RaftNodeState state, string endpoint, long term, long currentTerm);

    [LoggerMessage(Level = LogLevel.Debug, Message = "[{LocalEndpoint}/{PartitionId}/{State}] Denying pre-vote to {Endpoint} Term={Term}: outdated log RemoteMaxId={RemoteId} LocalMaxId={MaxId}")]
    public static partial void LogDebugDenyingPreVoteOutdatedLog(this ILogger<IRaft> logger, string localEndpoint, int partitionId, RaftNodeState state, string endpoint, long term, long remoteId, long maxId);

    [LoggerMessage(Level = LogLevel.Debug, Message = "[{LocalEndpoint}/{PartitionId}/{State}] Granting pre-vote to {Endpoint} Term={Term}")]
    public static partial void LogDebugGrantingPreVote(this ILogger<IRaft> logger, string localEndpoint, int partitionId, RaftNodeState state, string endpoint, long term);

    [LoggerMessage(Level = LogLevel.Debug, Message = "[{LocalEndpoint}/{PartitionId}/{State}] Denying vote to {Endpoint} Term={Term}: endpoint is not a committed voter")]
    public static partial void LogDebugDenyingVoteNotVoter(this ILogger<IRaft> logger, string localEndpoint, int partitionId, RaftNodeState state, string endpoint, long term);

    [LoggerMessage(Level = LogLevel.Information, Message = "[{LocalEndpoint}/{PartitionId}/{State}] Received request to vote from {Endpoint} but already voted in that Term={Term}. Ignoring...")]
    public static partial void LogInfoAlreadyVotedInTerm(this ILogger<IRaft> logger, string localEndpoint, int partitionId, RaftNodeState state, string endpoint, long term);

    [LoggerMessage(Level = LogLevel.Information, Message = "[{LocalEndpoint}/{PartitionId}/{State}] Received request to vote from {Endpoint} but we're candidate or leader on the same Term={Term}. Ignoring...")]
    public static partial void LogInfoCandidateOrLeaderSameTerm(this ILogger<IRaft> logger, string localEndpoint, int partitionId, RaftNodeState state, string endpoint, long term);

    [LoggerMessage(Level = LogLevel.Information, Message = "[{LocalEndpoint}/{PartitionId}/{State}] Received request to vote on previous term from {Endpoint} Term={Term}. Ignoring...")]
    public static partial void LogInfoVoteOnPreviousTerm(this ILogger<IRaft> logger, string localEndpoint, int partitionId, RaftNodeState state, string endpoint, long term);

    [LoggerMessage(Level = LogLevel.Information, Message = "[{LocalEndpoint}/{PartitionId}/{State}] Received request to vote from {Endpoint} but we already voted for {ExpectedLeader}. Ignoring...")]
    public static partial void LogInfoAlreadyVotedForOther(this ILogger<IRaft> logger, string localEndpoint, int partitionId, RaftNodeState state, string endpoint, string expectedLeader);

    [LoggerMessage(Level = LogLevel.Information, Message = "[{LocalEndpoint}/{PartitionId}/{State}] Received request to vote on outdated log from {Endpoint} RemoteMaxId={RemoteId} LocalMaxId={MaxId}. Ignoring...")]
    public static partial void LogInfoVoteOutdatedLog(this ILogger<IRaft> logger, string localEndpoint, int partitionId, RaftNodeState state, string endpoint, long remoteId, long maxId);

    [LoggerMessage(Level = LogLevel.Debug, Message = "[{LocalEndpoint}/{PartitionId}/{State}] Ignoring {PreVote}vote grant from {Endpoint} Term={Term}: endpoint is not a committed voter")]
    public static partial void LogDebugIgnoringVoteGrantNotVoter(this ILogger<IRaft> logger, string localEndpoint, int partitionId, RaftNodeState state, string preVote, string endpoint, long term);

    [LoggerMessage(Level = LogLevel.Debug, Message = "[{LocalEndpoint}/{PartitionId}/{State}] Ignoring pre-vote grant from {Endpoint} Term={Term}: no matching open round (phase={Phase} preVoteTerm={PreVoteTerm})")]
    public static partial void LogDebugIgnoringPreVoteGrantNoRound(this ILogger<IRaft> logger, string localEndpoint, int partitionId, RaftNodeState state, string endpoint, long term, RaftElectionPhase phase, long preVoteTerm);

    [LoggerMessage(Level = LogLevel.Information, Message = "[{LocalEndpoint}/{PartitionId}/{State}] Received pre-vote from {Endpoint} Term={Term} PreVotes={PreVotes} Quorum={Quorum}/{Total}")]
    public static partial void LogInfoReceivedPreVote(this ILogger<IRaft> logger, string localEndpoint, int partitionId, RaftNodeState state, string endpoint, long term, int preVotes, int quorum, int total);

    [LoggerMessage(Level = LogLevel.Information, Message = "[{LocalEndpoint}/{PartitionId}/{State}] Received vote from {Node} but we didn't ask for it Term={Term}. Ignoring...")]
    public static partial void LogInfoReceivedUnsolicitedVote(this ILogger<IRaft> logger, string localEndpoint, int partitionId, RaftNodeState state, string node, long term);

    [LoggerMessage(Level = LogLevel.Information, Message = "[{LocalEndpoint}/{PartitionId}/{State}] Received vote from {Node} but already declared as leader Term={Term}. Ignoring...")]
    public static partial void LogInfoReceivedVoteAlreadyLeader(this ILogger<IRaft> logger, string localEndpoint, int partitionId, RaftNodeState state, string node, long term);

    [LoggerMessage(Level = LogLevel.Information, Message = "[{LocalEndpoint}/{PartitionId}/{State}] Received vote from {Endpoint} Term={Term} Votes={Votes} Quorum={Quorum}/{Total} RemoteCommitId={CommitId} Local={LocalCommitId}")]
    public static partial void LogInfoReceivedVote(this ILogger<IRaft> logger, string localEndpoint, int partitionId, RaftNodeState state, string endpoint, long term, int votes, int quorum, int total, long commitId, long localCommitId);

    [LoggerMessage(Level = LogLevel.Information, Message = "[{LocalEndpoint}/{PartitionId}/{State}] Received vote from {Endpoint} and proclamed leader in {Elapsed}ms Term={Term} Votes={Votes} Quorum={Quorum}/{Total} RemoteCommitId={CommitId} Local={LocalCommitId}")]
    public static partial void LogInfoReceivedVoteProclaimedLeader(this ILogger<IRaft> logger, string localEndpoint, int partitionId, RaftNodeState state, string endpoint, double elapsed, long term, int votes, int quorum, int total, long commitId, long localCommitId);

    [LoggerMessage(Level = LogLevel.Information, Message = "[{LocalEndpoint}/{PartitionId}/{State}] Leader is now {Endpoint} LeaderTerm={Term}")]
    public static partial void LogInfoLeaderIsNow(this ILogger<IRaft> logger, string localEndpoint, int partitionId, RaftNodeState state, string endpoint, long term);

    [LoggerMessage(Level = LogLevel.Debug, Message = "[{LocalEndpoint}/{PartitionId}/{State}] Backfilling {Count} entries to {Endpoint} from={From} prevLogIndex={PrevLogIndex} leaderCommitted={LeaderCommitted}")]
    public static partial void LogDebugBackfilling(this ILogger<IRaft> logger, string localEndpoint, int partitionId, RaftNodeState state, int count, string endpoint, long from, long prevLogIndex, long leaderCommitted);

    [LoggerMessage(Level = LogLevel.Information, Message = "[{LocalEndpoint}/{PartitionId}/{State}] Proposal {Timestamp} doesn't have auto-commit")]
    public static partial void LogInfoProposalNoAutoCommit(this ILogger<IRaft> logger, string localEndpoint, int partitionId, RaftNodeState state, HLCTimestamp timestamp);

    [LoggerMessage(Level = LogLevel.Information, Message = "[{LocalEndpoint}/{PartitionId}/{State}] Snapshot installed on {Endpoint} at index {Index} ({Chunks} chunk(s))")]
    public static partial void LogInfoSnapshotInstalled(this ILogger<IRaft> logger, string localEndpoint, int partitionId, RaftNodeState state, string endpoint, long index, int chunks);

    // ── RaftWriteAhead ────────────────────────────────────────────────────

    [LoggerMessage(Level = LogLevel.Information, Message = "[{Endpoint}/{Partition}] Recovered {LogsCount} logs")]
    public static partial void LogInfoRecoveredLogs(this ILogger<IRaft> logger, string endpoint, int partition, int logsCount);

    [LoggerMessage(Level = LogLevel.Debug, Message = "[{Endpoint}/{Partition}] Rolledback log #{Id}")]
    public static partial void LogDebugRolledbackLog(this ILogger<IRaft> logger, string endpoint, int partition, long id);

    [LoggerMessage(Level = LogLevel.Debug, Message = "[{Endpoint}/{Partition}] Proposed checkpoint log #{Id}")]
    public static partial void LogDebugProposedCheckpointLog(this ILogger<IRaft> logger, string endpoint, int partition, long id);

    [LoggerMessage(Level = LogLevel.Debug, Message = "[{Endpoint}/{Partition}] Rolled back checkpoint log #{Id}")]
    public static partial void LogDebugRolledBackCheckpointLog(this ILogger<IRaft> logger, string endpoint, int partition, long id);

    [LoggerMessage(Level = LogLevel.Debug, Message = "[{Endpoint}/{Partition}] Committed checkpoint log #{Id}")]
    public static partial void LogDebugCommittedCheckpointLog(this ILogger<IRaft> logger, string endpoint, int partition, long id);

    [LoggerMessage(Level = LogLevel.Information, Message = "[{Endpoint}/{Partition}] Compaction process started LastCheckpoint={LastCheckpoint}")]
    public static partial void LogInfoCompactionStarted(this ILogger<IRaft> logger, string endpoint, int partition, long lastCheckpoint);

    [LoggerMessage(Level = LogLevel.Information, Message = "[{Endpoint}/{Partition}] Compaction finished Removed={RemovedTotal} LastCheckpoint={LastCheckpoint}")]
    public static partial void LogInfoCompactionFinished(this ILogger<IRaft> logger, string endpoint, int partition, int removedTotal, long lastCheckpoint);

    // ── RaftManager ───────────────────────────────────────────────────────

    [LoggerMessage(Level = LogLevel.Debug, Message = "SystemLogRestored: skipping entry on system partition (LogType={LogType}, DataNull={DataNull}) — non-system types are routed to consumer callbacks upstream")]
    public static partial void LogDebugSystemLogRestoredSkip(this ILogger<IRaft> logger, string? logType, bool dataNull);

    [LoggerMessage(Level = LogLevel.Debug, Message = "SystemReplicationReceived: skipping entry on system partition (LogType={LogType}, DataNull={DataNull}) — non-system types are routed to consumer callbacks upstream")]
    public static partial void LogDebugSystemReplicationReceivedSkip(this ILogger<IRaft> logger, string? logType, bool dataNull);

    [LoggerMessage(Level = LogLevel.Information, Message = "JoinCluster: admitted as Learner at roster version {Version}")]
    public static partial void LogInfoJoinClusterAdmittedAsLearner(this ILogger<IRaft> logger, long version);

    [LoggerMessage(Level = LogLevel.Information, Message = "[{Endpoint}] ReceiveInstallSnapshot: partition={PartitionId} installed snapshot at index={Index}")]
    public static partial void LogInfoReceiveInstallSnapshot(this ILogger<IRaft> logger, string endpoint, int partitionId, long index);

    [LoggerMessage(Level = LogLevel.Information, Message = "ReceiveGossip: self was Suspect in gossip; refuting with incarnation {Inc}")]
    public static partial void LogInfoReceiveGossipRefuting(this ILogger<IRaft> logger, long inc);

    [LoggerMessage(Level = LogLevel.Debug, Message = "GossipAsync: failed to contact {Peer}: {Message}")]
    public static partial void LogDebugGossipAsyncFailed(this ILogger<IRaft> logger, string peer, string message);

    [LoggerMessage(Level = LogLevel.Information, Message = "LeaveCluster: leave permanently rejected by hint {Hint}; proceeding to stop.")]
    public static partial void LogInfoLeaveClusterRejectedByHint(this ILogger<IRaft> logger, string hint);

    [LoggerMessage(Level = LogLevel.Information, Message = "LeaveCluster: P0 leader unknown after {Polls} polls; proceeding to stop.")]
    public static partial void LogInfoLeaveClusterLeaderUnknown(this ILogger<IRaft> logger, int polls);

    // ── RaftSystemCoordinator ─────────────────────────────────────────────

    [LoggerMessage(Level = LogLevel.Information, Message = "Restored system configuration: {Key}")]
    public static partial void LogInfoRestoredSystemConfiguration(this ILogger<IRaft> logger, string key);

    [LoggerMessage(Level = LogLevel.Information, Message = "Replicated system configuration: {Key}")]
    public static partial void LogInfoReplicatedSystemConfiguration(this ILogger<IRaft> logger, string key);

    [LoggerMessage(Level = LogLevel.Information, Message = "InitializePartitions: Re-enqueuing MergePartitionCommit for stuck Draining partition {Src} → survivor {Surv}")]
    public static partial void LogInfoReEnqueuingMergePartitionCommit(this ILogger<IRaft> logger, int src, int surv);

    [LoggerMessage(Level = LogLevel.Information, Message = "Successfully replicated initial partitions {Status} {LogIndex}")]
    public static partial void LogInfoSuccessfullyReplicatedInitialPartitions(this ILogger<IRaft> logger, RaftOperationStatus status, long logIndex);

    [LoggerMessage(Level = LogLevel.Information, Message = "TrySplitPartition: Phase 1 committed for partition {Partition}")]
    public static partial void LogInfoSplitPartitionPhase1Committed(this ILogger<IRaft> logger, int partition);

    [LoggerMessage(Level = LogLevel.Information, Message = "TrySplitPartition: Snapshot transfer complete — source={Src} snapshotIndex={Idx}, target={Tgt} checkpoint committed")]
    public static partial void LogInfoSplitPartitionSnapshotTransferComplete(this ILogger<IRaft> logger, int src, long idx, int tgt);

    [LoggerMessage(Level = LogLevel.Information, Message = "TrySplitPartitionCommit: Phase 2 committed for partition {Partition}")]
    public static partial void LogInfoSplitPartitionCommitPhase2Committed(this ILogger<IRaft> logger, int partition);

    [LoggerMessage(Level = LogLevel.Information, Message = "TryMergePartitions: Phase 1 committed — source {Src} is Draining")]
    public static partial void LogInfoMergePartitionsPhase1Committed(this ILogger<IRaft> logger, int src);

    [LoggerMessage(Level = LogLevel.Information, Message = "TryMergePartitionCommit: Phase 2 committed — source {Src} removed, survivor {Surv} expanded")]
    public static partial void LogInfoMergePartitionCommitPhase2Committed(this ILogger<IRaft> logger, int src, int surv);

    [LoggerMessage(Level = LogLevel.Information, Message = "TryCreatePartition: Partition {Id} already active at generation {Gen}")]
    public static partial void LogInfoCreatePartitionAlreadyActive(this ILogger<IRaft> logger, int id, long gen);

    [LoggerMessage(Level = LogLevel.Information, Message = "TryRemovePartition: Partition {Id} already removed")]
    public static partial void LogInfoRemovePartitionAlreadyRemoved(this ILogger<IRaft> logger, int id);

    [LoggerMessage(Level = LogLevel.Information, Message = "TryRemovePartition: Partition {Id} removed and WAL reclaimed")]
    public static partial void LogInfoRemovePartitionReclaimedWal(this ILogger<IRaft> logger, int id);

    [LoggerMessage(Level = LogLevel.Information, Message = "[RaftSystemCoordinator] Seeded initial membership roster with {Count} voter(s)")]
    public static partial void LogInfoSeededInitialMembership(this ILogger<IRaft> logger, int count);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Gossip: updating local cache from v{Old} to v{New} via peer push")]
    public static partial void LogDebugGossipUpdatingCache(this ILogger<IRaft> logger, long old, long @new);

    [LoggerMessage(Level = LogLevel.Information, Message = "[RaftSystemCoordinator] Promoting Learner {Endpoint} to Voter (stable for {Duration:g})")]
    public static partial void LogInfoPromotingLearnerToVoter(this ILogger<IRaft> logger, string endpoint, TimeSpan duration);

    [LoggerMessage(Level = LogLevel.Information, Message = "TryAddMember: Endpoint {Endpoint} is already in the roster; treating as idempotent success at version {Version}")]
    public static partial void LogInfoAddMemberAlreadyInRoster(this ILogger<IRaft> logger, string endpoint, long version);

    [LoggerMessage(Level = LogLevel.Information, Message = "TryAddMember: Adding {Endpoint} as Learner at version {Version}")]
    public static partial void LogInfoAddMemberAddingLearner(this ILogger<IRaft> logger, string endpoint, long version);

    [LoggerMessage(Level = LogLevel.Information, Message = "TryPromoteMember: Promoting {Endpoint} to Voter at version {Version}")]
    public static partial void LogInfoPromoteMemberToVoter(this ILogger<IRaft> logger, string endpoint, long version);

    [LoggerMessage(Level = LogLevel.Information, Message = "TryRemoveMember: Endpoint {Endpoint} is not in the roster; treating as idempotent success at version {Version}")]
    public static partial void LogInfoRemoveMemberNotInRoster(this ILogger<IRaft> logger, string endpoint, long version);

    [LoggerMessage(Level = LogLevel.Information, Message = "TryRemoveMember: Removing {Endpoint} at version {Version}")]
    public static partial void LogInfoRemoveMember(this ILogger<IRaft> logger, string endpoint, long version);

    // ── RestCommunication ─────────────────────────────────────────────────

    [LoggerMessage(Level = LogLevel.Debug, Message = "[{Endpoint}/{Partition}] Logs replicated to {RemoteEndpoint}")]
    public static partial void LogDebugLogsReplicated(this ILogger<IRaft> logger, string endpoint, int partition, string remoteEndpoint);

    // ── TransferLeadershipSuggestion drop diagnostics ─────────────────────

    [LoggerMessage(Level = LogLevel.Debug, Message = "TransferSuggestion p{Partition} term={Term}: dropped — not leader (current={Leader}), suggested by {SuggestedBy}")]
    public static partial void LogDebugTransferSuggestionDroppedNotLeader(this ILogger<IRaft> logger, int partition, long term, string leader, string suggestedBy);

    [LoggerMessage(Level = LogLevel.Debug, Message = "TransferSuggestion p{Partition} term={Term}: dropped — partition state={State}, suggested by {SuggestedBy}")]
    public static partial void LogDebugTransferSuggestionDroppedNotActive(this ILogger<IRaft> logger, int partition, long term, Kommander.System.RaftPartitionState state, string suggestedBy);

    [LoggerMessage(Level = LogLevel.Debug, Message = "TransferSuggestion p{Partition} term={Term}: dropped — target {Target} is not a live voter, suggested by {SuggestedBy}")]
    public static partial void LogDebugTransferSuggestionDroppedNotVoter(this ILogger<IRaft> logger, int partition, long term, string target, string suggestedBy);

    [LoggerMessage(Level = LogLevel.Debug, Message = "TransferSuggestion p{Partition} term={Term}: dropped — target {Target} is suspect/dead, suggested by {SuggestedBy}")]
    public static partial void LogDebugTransferSuggestionDroppedSuspect(this ILogger<IRaft> logger, int partition, long term, string target, string suggestedBy);

    // ── RaftTransportDispatcher ───────────────────────────────────────────

    [LoggerMessage(Level = LogLevel.Trace, Message = "[RaftTransportDispatcher/{Endpoint}] Sending batch of {Count} messages")]
    public static partial void LogTraceSendingBatch(this ILogger<IRaft> logger, string endpoint, int count);

    // ── WAL ───────────────────────────────────────────────────────────────

    [LoggerMessage(Level = LogLevel.Debug, Message = "Removed {Count} from WAL for partition {PartitionId}")]
    public static partial void LogDebugRemovedFromWal(this ILogger<IRaft> logger, int count, int partitionId);
}
