using System.Buffers;
using Kommander.Communication.Grpc;
using Kommander.Data;
using Kommander.Logging;
using Kommander.Scheduling;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL.Data;
using Microsoft.Extensions.Logging;

namespace Kommander.Consensus;

/// <summary>
/// The leader's write entry points: propose, checkpoint, commit and rollback. Each one validates
/// leadership, registers a proposal with the quorum it must reach, enqueues the WAL operation, and
/// broadcasts to peers — then returns a ticket the caller resolves later. Nothing here waits; the
/// completion side runs in <see cref="WalCompletionRouter"/>.
///
/// <para><b>Why the ticket is returned before durability.</b> The partition executor is
/// single-threaded, so blocking here would stall every other operation on the partition, including
/// the acks this proposal needs to reach quorum. The two halves are split across the WAL completion
/// callback for that reason, and the pending-operation record carries the reply correlation between
/// them.</para>
///
/// <para><b>Concurrency.</b> Invoked only on the partition executor thread; holds no locks by
/// design.</para>
/// </summary>
internal sealed class LogReplicator
{
    private readonly IRaftPartitionHost host;
    private readonly IRaftWalFacade wal;
    private readonly RaftPartitionCoreState coreState;
    private readonly ProposalRegistry proposals;
    private readonly BackfillSender sender;
    private readonly IRaftOperationReplySink replySink;
    private readonly ILogger<IRaft> logger;

    public LogReplicator(
        IRaftPartitionHost host,
        IRaftWalFacade wal,
        RaftPartitionCoreState coreState,
        ProposalRegistry proposals,
        BackfillSender sender,
        IRaftOperationReplySink replySink,
        ILogger<IRaft> logger)
    {
        this.host = host;
        this.wal = wal;
        this.coreState = coreState;
        this.proposals = proposals;
        this.sender = sender;
        this.replySink = replySink;
        this.logger = logger;
    }

    private void CompleteReply(ulong? correlationId, RaftResponse response)
    {
        if (correlationId is not null)
            replySink.TryComplete(correlationId.Value, response);
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

        if (coreState.NodeState != RaftNodeState.Leader)
            return (RaftOperationStatus.NodeIsNotLeader, HLCTimestamp.Zero);

        HLCTimestamp currentTime = host.HybridLogicalClock.SendOrLocalEvent(host.LocalNodeId);
        coreState.LastProposalAt = currentTime;
        coreState.LastProposalAtTicks = host.GetMonotonicTimestamp(); // B3: quiesce-after measured on monotonic clock
        coreState.SetQuiesced(false); // un-quiesce on new proposal: resume normal heartbeating

        // Try to clear and reuse settled proposals. Only worth scanning once a few have accumulated;
        // the periodic Leader tick (CheckPartitionLeadershipAsync) runs the same drain unconditionally
        // so an idle leader that stops proposing still releases its settled proposals rather than
        // retaining their log payloads until the next leadership change.
        if (proposals.ActiveCount > 5)
            proposals.PruneSettled(currentTime);

        // No peers: a single-node leader is its own quorum. Rather than rejecting the proposal,
        // we enqueue it to the local WAL exactly like the multi-node path; CompleteLeaderPropose
        // then drives the commit immediately (quorum = self), so JoinCluster/IsInitialized can
        // complete on a single-node cluster. The self-only commit shortcut lives entirely behind
        // the Nodes.Count == 0 guard in CompleteLeaderPropose, so multi-node safety is unaffected.

        // Snapshot Type and Time before mutation so we can restore atomically if the WAL scheduler
        // rejects the operation (e.g. BackpressureExceededException). Uses one pooled (Type, Time) buffer
        // rather than logs.ToArray() + two Array.ConvertAll copies; logs is an indexable List<RaftLog>, so
        // it is read and restored by index. Every entry is snapshotted first and only then mutated — the
        // same all-reads-before-any-writes order as the previous code — so the rollback is identical even
        // if the same entry appears twice. The buffer holds only value types (no references to clear), and
        // rent/return stay in this method via try/finally, never escaping onto a field.
        int logCount = logs.Count;
        (RaftLogType Type, HLCTimestamp Time)[] snapshot =
            ArrayPool<(RaftLogType, HLCTimestamp)>.Shared.Rent(logCount);
        try
        {
            for (int i = 0; i < logCount; i++)
                snapshot[i] = (logs[i].Type, logs[i].Time);

            for (int i = 0; i < logCount; i++)
            {
                logs[i].Type = RaftLogType.Proposed;
                logs[i].Time = currentTime;
            }

            WALWriteOperation operation;
            try
            {
                operation = wal.EnqueuePropose(coreState.CurrentTerm, logs, currentTime, autoCommit);
            }
            catch
            {
                for (int i = 0; i < logCount; i++)
                {
                    logs[i].Type = snapshot[i].Type;
                    logs[i].Time = snapshot[i].Time;
                }
                throw;
            }

            Scheduling.RaftPendingWalOperation pendingPropose = proposals.RentPending();
            pendingPropose.ReplyCorrelationId = replyCorrelationId;
            pendingPropose.TicketId = currentTime;
            pendingPropose.Logs = logs;
            pendingPropose.AutoCommit = autoCommit;
            proposals.TrackPending(operation.OperationId, pendingPropose);

            return (RaftOperationStatus.Pending, currentTime);
        }
        finally
        {
            ArrayPool<(RaftLogType, HLCTimestamp)>.Shared.Return(snapshot);
        }

        // Append proposal logs to the Write-Ahead Log
        /*(RaftOperationStatus Status, long) proposeResponse = await wal.Propose(coreState.CurrentTerm, logs).ConfigureAwait(false);
        
        if (proposeResponse.Status != RaftOperationStatus.Success)
        {
            logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Couldn't save proposed logs to local persistence", host.LocalEndpoint, host.PartitionId, coreState.NodeState);
            
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
            
            sender.AppendLogToNode(node, currentTime, logs);
        }

        if (!proposals.TryAdd(currentTime, proposalQuorum))
            return (RaftOperationStatus.Errored, HLCTimestamp.Zero);
        
        if (logger.IsEnabled(LogLevel.Debug))
            logger.LogDebugProposedLogs(host.LocalEndpoint, host.PartitionId, coreState.NodeState, currentTime, string.Join(',', logs.Select(x => x.Id.ToString())));

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
        // Determine which autoCommit values to dispatch, in first-seen order among messages that carry
        // logs — a batch whose messages all have null/empty logs dispatches nothing. Only the KEYS drive
        // the dispatch loop below (each message keeps its own log list), so no per-key log aggregation
        // is needed on this per-batch hot path.
        bool sawFirstKey = false;
        bool sawSecondKey = false;
        bool firstKey = false;

        foreach ((List<RaftLog>? logs, bool autoCommit, ulong? replyCorrelationId) message in messages)
        {
            if (message.logs is null || message.logs.Count == 0)
                continue;

            if (!sawFirstKey)
            {
                sawFirstKey = true;
                firstKey = message.autoCommit;
            }
            else if (message.autoCommit != firstKey)
                sawSecondKey = true;
        }

        for (int keyIndex = 0; keyIndex < 2; keyIndex++)
        {
            if (keyIndex == 0 && !sawFirstKey)
                break;

            if (keyIndex == 1 && !sawSecondKey)
                break;

            bool key = keyIndex == 0 ? firstKey : !firstKey;

            foreach ((List<RaftLog>? logs, bool autoCommit, ulong? replyCorrelationId) item in messages)
            {
                if (item.autoCommit == key)
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

    public (RaftOperationStatus status, HLCTimestamp ticketId) ReplicateCheckpoint(
        ulong? replyCorrelationId = null
    )
    {
        if (coreState.NodeState != RaftNodeState.Leader)
            return (RaftOperationStatus.NodeIsNotLeader, HLCTimestamp.Zero);
        
        if (proposals.HasUnresolvedProposal())
            return (RaftOperationStatus.ActiveProposal, HLCTimestamp.Zero);
        
        // No peers: a single-node leader is its own quorum. We enqueue the checkpoint to the local
        // WAL exactly like the multi-node path and let CompleteLeaderPropose drive the commit
        // immediately (quorum = self), behind its Nodes.Count == 0 guard. Without this a single-node
        // cluster could never checkpoint, and since compaction is checkpoint-driven its WAL would
        // grow without bound. Multi-node safety is unaffected: with peers, the same proposal still
        // requires a real quorum.

        // We need a proper HLC sequence to determine a consistent order of the logs
        HLCTimestamp currentTime = host.HybridLogicalClock.SendOrLocalEvent(host.LocalNodeId);

        // P0 checkpoints carry the current system-configuration snapshot (roster + partition
        // map) so a restart that replays from this checkpoint reconstructs them even after
        // compaction removed the original config delta entries below it. The payload is inert
        // on the live path (CommittedCheckpoint entries are never delivered to consumers) and
        // replicates to followers like any other entry.
        byte[]? systemPayload = host.PartitionId == RaftSystemConfig.SystemPartition
            ? host.GetSystemCheckpointPayload()
            : null;

        List<RaftLog> checkpointLogs = [new()
        {
            Id = 0,
            Term = coreState.CurrentTerm,
            Type = RaftLogType.ProposedCheckpoint,
            Time = currentTime,
            LogType = systemPayload is null ? "" : RaftSystemConfig.CheckpointLogType,
            LogData = systemPayload ?? []
        }];

        WALWriteOperation operation = wal.EnqueuePropose(coreState.CurrentTerm, checkpointLogs, currentTime, true);
        Scheduling.RaftPendingWalOperation pendingCheckpoint = proposals.RentPending();
        pendingCheckpoint.ReplyCorrelationId = replyCorrelationId;
        pendingCheckpoint.TicketId = currentTime;
        pendingCheckpoint.Logs = checkpointLogs;
        pendingCheckpoint.AutoCommit = true;
        proposals.TrackPending(operation.OperationId, pendingCheckpoint);

        return (RaftOperationStatus.Pending, currentTime);
        
        // Append proposal logs to the Write-Ahead Log
        /*(RaftOperationStatus Status, long) proposeResponse = await wal.Propose(context.Self, coreState.CurrentTerm, checkpointLogs).ConfigureAwait(false);
        
        if (proposeResponse.Status != RaftOperationStatus.Success)
        {
            logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Couldn't save proposed logs to local persistence", host.LocalEndpoint, host.PartitionId, coreState.NodeState);
            
            return (RaftOperationStatus.Errored, HLCTimestamp.Zero);
        }

        RaftProposalQuorum proposalQuorum = RaftProposalQuorumPool.Rent(checkpointLogs, true, currentTime);

        foreach (RaftNode node in nodes)
        {
            if (node.Endpoint == host.LocalEndpoint)
                throw new RaftException("Corrupted nodes");
            
            proposalQuorum.AddExpectedNodeCompletion(node.Endpoint);
            
            sender.AppendLogToNode(node, currentTime, checkpointLogs);
        }

        proposals.TryAdd(currentTime, proposalQuorum);
        
        logger.LogInfoProposedCheckpointLogs(
            host.LocalEndpoint, 
            host.PartitionId, 
            coreState.NodeState, 
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

    /// <summary>
    /// Idempotent: if the proposal is already <see cref="RaftProposalState.Committed"/>, returns
    /// <see cref="RaftOperationStatus.Success"/> with the committed index immediately (no second WAL
    /// write).  If already <see cref="RaftProposalState.RolledBack"/>, returns
    /// <see cref="RaftOperationStatus.Errored"/> — the settled outcome wins and a commit is not
    /// applied.  The first terminal transition (Committed <em>or</em> RolledBack) is the one that
    /// persists; any subsequent opposite request reflects that settled state.
    /// </summary>
    public (RaftOperationStatus, long commitIndex) CommitLogs(
        HLCTimestamp ticketId,
        ulong? replyCorrelationId = null
    )
    {
        // Post-propose refusal: the ticket's entries are already appended (phase 1 of the manual
        // two-phase), so a step-down here leaves their outcome genuinely open — the next leader's
        // §5.4.2 inherited commit can still commit them. Indeterminate, never NodeIsNotLeader
        // (clients read that as "definitely did not take effect").
        if (coreState.NodeState != RaftNodeState.Leader)
            return (RaftOperationStatus.ProposalOutcomeUnknown, 0);

        if (!proposals.TryGet(ticketId, out RaftProposalQuorum? proposal))
            return (RaftOperationStatus.ProposalNotFound, 0);

        if (proposal.State == RaftProposalState.Committed)
            return (RaftOperationStatus.Success, proposal.LastLogIndex);

        if (proposal.State == RaftProposalState.RolledBack)
            return (RaftOperationStatus.Errored, 0);

        if (!proposal.HasQuorum())
        {
            logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Trying to commit proposal {Timestamp} without quorum...", host.LocalEndpoint, host.PartitionId, coreState.NodeState, ticketId);

            return (RaftOperationStatus.Errored, 0);
        }

        if (proposal.State != RaftProposalState.Completed)
        {
            logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Trying to commit proposal {Timestamp} in state {State}...", host.LocalEndpoint, host.PartitionId, coreState.NodeState, ticketId, proposal.State);

            return (RaftOperationStatus.Errored, 0);
        }

        WALWriteOperation operation = wal.EnqueueCommit(proposal.Logs);
        Scheduling.RaftPendingWalOperation pendingCommit = proposals.RentPending();
        pendingCommit.ReplyCorrelationId = replyCorrelationId;
        pendingCommit.Proposal = proposal;
        pendingCommit.TicketId = ticketId;
        proposals.TrackPending(operation.OperationId, pendingCommit);

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

    /// <summary>
    /// Idempotent: if the proposal is already <see cref="RaftProposalState.RolledBack"/>, returns
    /// <see cref="RaftOperationStatus.Success"/> immediately (no second WAL write).  If already
    /// <see cref="RaftProposalState.Committed"/>, returns <see cref="RaftOperationStatus.Errored"/>
    /// — the settled outcome wins and a rollback is not applied.  The first terminal transition
    /// (Committed <em>or</em> RolledBack) is the one that persists; any subsequent opposite request
    /// reflects that settled state.
    /// </summary>
    public (RaftOperationStatus, long commitIndex) RollbackLogs(
        HLCTimestamp ticketId,
        ulong? replyCorrelationId = null
    )
    {
        // Same post-propose contract as CommitLogs: a refused ABORT leaves appended entries whose
        // fate the next leader decides — indeterminate for the caller.
        if (coreState.NodeState != RaftNodeState.Leader)
            return (RaftOperationStatus.ProposalOutcomeUnknown, 0);

        if (!proposals.TryGet(ticketId, out RaftProposalQuorum? proposal))
            return (RaftOperationStatus.ProposalNotFound, 0);

        if (proposal.State == RaftProposalState.RolledBack)
            return (RaftOperationStatus.Success, 0);

        if (proposal.State == RaftProposalState.Committed)
            return (RaftOperationStatus.Errored, 0);

        if (!proposal.HasQuorum())
        {
            logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Trying to rollback proposal {Timestamp} without quorum...", host.LocalEndpoint, host.PartitionId, coreState.NodeState, ticketId);

            return (RaftOperationStatus.Errored, 0);
        }

        if (proposal.State != RaftProposalState.Completed)
        {
            logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Trying to rollback proposal {Timestamp} in state {State}...", host.LocalEndpoint, host.PartitionId, coreState.NodeState, ticketId, proposal.State);

            return (RaftOperationStatus.Errored, 0);
        }

        WALWriteOperation operation = wal.EnqueueRollback(proposal.Logs);
        Scheduling.RaftPendingWalOperation pendingRollback = proposals.RentPending();
        pendingRollback.ReplyCorrelationId = replyCorrelationId;
        pendingRollback.Proposal = proposal;
        pendingRollback.TicketId = ticketId;
        proposals.TrackPending(operation.OperationId, pendingRollback);

        return (RaftOperationStatus.Pending, operation.LogIndex);
    }
}
