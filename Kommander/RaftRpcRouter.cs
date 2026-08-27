
using Kommander.Data;
using Kommander.Gossip;
using Kommander.Support.Parallelization;
using Kommander.Logging;
using Kommander.System;
using Kommander.WAL;
using Microsoft.Extensions.Logging;

namespace Kommander;

/// <summary>
/// Routes inbound consensus RPCs (handshake, election, replication) to the partition they name,
/// and delivers the one outbound advisory message that has no other home — the balancer's
/// leadership-transfer suggestion.
/// <para>
/// Every handler here is deliberately drop-on-miss rather than throw-on-miss. These run inline on
/// the transport path, where a peer's messages for several partitions are coalesced into one
/// batch and awaited in order; throwing for a data partition this node has not materialized yet
/// would abort the sibling messages behind it — including the system-partition heartbeats and
/// votes the node needs in order to finish assembling. The sender re-sends on its next election
/// timeout or heartbeat, so dropping is always safe and never head-of-line-stalls a batch.
/// </para>
/// <para>
/// Holds no mutable state of its own; all state it reads belongs to the partition registry, the
/// committed roster, or the liveness table.
/// </para>
/// </summary>
internal sealed class RaftRpcRouter
{
    private readonly IPartitionProvider partitionProvider;
    private readonly IWAL walAdapter;
    private readonly Func<ClusterMembership> getMembership;
    private readonly Func<LivenessTable> getLiveness;
    private readonly Action<string, RaftResponderRequest> enqueueResponse;
    private readonly ILogger<IRaft> logger;
    private readonly string localEndpoint;
    private readonly int localNodeId;

    internal RaftRpcRouter(
        IPartitionProvider partitionProvider,
        IWAL walAdapter,
        Func<ClusterMembership> getMembership,
        Func<LivenessTable> getLiveness,
        Action<string, RaftResponderRequest> enqueueResponse,
        ILogger<IRaft> logger,
        string localEndpoint,
        int localNodeId)
    {
        this.partitionProvider = partitionProvider;
        this.walAdapter = walAdapter;
        this.getMembership = getMembership;
        this.getLiveness = getLiveness;
        this.enqueueResponse = enqueueResponse;
        this.logger = logger;
        this.localEndpoint = localEndpoint;
        this.localNodeId = localNodeId;
    }

    /// <summary>
    /// Passes the handshake to the addressed partition, dropping it when that partition has not
    /// been created here yet.
    /// <para>
    /// A previous version instead spun on <c>while (!IsInitialized) await Task.Delay(100)</c>,
    /// which deadlocked join: a user-partition handshake to a joining node blocked the
    /// <c>AppendLogs</c>/<c>CompleteAppendLogs</c> that followed it in the same batch — the very
    /// system-partition entries the node needs in order to become initialized. Handshake waits for
    /// init; init needs those appends; the appends are trapped behind the handshake.
    /// </para>
    /// </summary>
    internal Task Handshake(HandshakeRequest request)
    {
        if (partitionProvider.TryGetPartition(request.Partition, out RaftPartition? partition))
            partition!.Handshake(request);

        return Task.CompletedTask;
    }

    /// <summary>
    /// Builds this node's handshake reply for a partition: its identity plus the highest log id
    /// it holds for that partition, which the peer uses to seed catch-up.
    /// </summary>
    internal HandshakeResponse GetHandshakeResponse(int partitionId)
    {
        long maxLogId = walAdapter.GetMaxLog(partitionId);
        return new(localNodeId, maxLogId, localEndpoint);
    }

    /// <summary>
    /// Passes a vote request to the addressed partition. Dropped when that partition does not
    /// exist here yet; the candidate retries on its next election timeout.
    /// </summary>
    internal void RequestVote(RequestVotesRequest request)
    {
        if (partitionProvider.TryGetPartition(request.Partition, out RaftPartition? partition))
            partition!.RequestVote(request);
    }

    /// <summary>
    /// Passes a vote response to the addressed partition. Dropped when that partition does not
    /// exist here yet, so it cannot abort sibling messages in the same endpoint batch.
    /// </summary>
    internal void Vote(VoteRequest request)
    {
        if (partitionProvider.TryGetPartition(request.Partition, out RaftPartition? partition))
            partition!.Vote(request);
    }

    /// <summary>Passes a step-down notice to the addressed partition.</summary>
    internal void StepDownNotice(StepDownNoticeRequest request)
    {
        if (partitionProvider.TryGetPartition(request.Partition, out RaftPartition? partition))
            partition!.StepDownNotice(request);
    }

    /// <summary>Passes a leadership-transfer command to the addressed partition.</summary>
    internal void TransferLeadership(TransferLeadershipRequest request)
    {
        if (partitionProvider.TryGetPartition(request.Partition, out RaftPartition? partition))
            partition!.TransferLeadership(request);
    }

    /// <summary>
    /// Appends replicated logs to the addressed partition. Dropping the append when the partition
    /// does not exist here yet is safe because the leader retries on its next heartbeat.
    /// </summary>
    internal void AppendLogs(AppendLogsRequest request)
    {
        if (partitionProvider.TryGetPartition(request.Partition, out RaftPartition? partition))
            partition!.AppendLogs(request);
    }

    /// <summary>
    /// Completes an append-logs round on the addressed partition. Dropped when the partition does
    /// not exist here yet, so it cannot abort sibling messages in the same endpoint batch.
    /// </summary>
    internal void CompleteAppendLogs(CompleteAppendLogsRequest request)
    {
        if (partitionProvider.TryGetPartition(request.Partition, out RaftPartition? partition))
            partition!.CompleteAppendLogs(request);
    }

    /// <summary>
    /// Receives an advisory leadership-transfer suggestion from the balancer running on the
    /// system-partition leader. Validates that this node currently leads the partition, that the
    /// partition is <see cref="RaftPartitionState.Active"/>, and that the requested target is a
    /// live voter — then fires the local transfer fire-and-forget. Drops silently on any
    /// validation failure so a stale or misdirected suggestion is always safe.
    /// </summary>
    internal void ReceiveTransferLeadershipSuggestion(TransferLeadershipSuggestionRequest request)
    {
        if (!partitionProvider.TryGetDataPartition(request.Partition, out RaftPartition? partition) || partition is null)
            return;

        // Only act if we currently lead this partition.
        if (!string.Equals(partition.Leader, localEndpoint, StringComparison.Ordinal))
        {
            logger.LogDebugTransferSuggestionDroppedNotLeader(
                request.Partition, request.Term, partition.Leader ?? "(none)", request.SuggestedBy);
            return;
        }

        // Only move Active partitions.
        if (partition.State != RaftPartitionState.Active)
        {
            logger.LogDebugTransferSuggestionDroppedNotActive(
                request.Partition, request.Term, partition.State, request.SuggestedBy);
            return;
        }

        // Target must be a live voter.
        ClusterMembership membership = getMembership();
        bool targetIsVoter = membership.Members.Exists(m =>
            string.Equals(m.Endpoint, request.TargetEndpoint, StringComparison.Ordinal) &&
            m.Role == ClusterMemberRole.Voter);

        if (!targetIsVoter)
        {
            logger.LogDebugTransferSuggestionDroppedNotVoter(
                request.Partition, request.Term, request.TargetEndpoint, request.SuggestedBy);
            return;
        }

        if (getLiveness().GetState(request.TargetEndpoint) >= MemberLivenessState.Suspect)
        {
            logger.LogDebugTransferSuggestionDroppedSuspect(
                request.Partition, request.Term, request.TargetEndpoint, request.SuggestedBy);
            return;
        }

        // Fire-and-forget: the executor serialises the transfer; we don't await here.
        FireAndForget.Observe(partition.TransferLeadershipAsync(request.TargetEndpoint, CancellationToken.None), logger, "TransferLeadershipSuggestion");
    }

    /// <summary>
    /// Sends an advisory leadership-transfer suggestion to the node at
    /// <paramref name="ownerEndpoint"/> via the existing responder transport. Fire-and-forget; a
    /// failed delivery is silently ignored and the suggestion times out in the balancer's
    /// outstanding-move tracking table.
    /// <para>
    /// When the owner is this node itself — the common case where the balancer leader also leads
    /// the overloaded partition — the suggestion is delivered in-process. The peer transport
    /// cannot be used for self-delivery: a node is not its own peer
    /// (<see cref="ClusterHandler.IsNode"/> excludes the local endpoint), so a self-addressed
    /// responder message is dropped on the wire. Without this short-circuit the balancer could
    /// never rebalance partitions led by the node running it.
    /// </para>
    /// </summary>
    internal void SendTransferLeadershipSuggestion(string ownerEndpoint, TransferLeadershipSuggestionRequest request)
    {
        if (string.Equals(ownerEndpoint, localEndpoint, StringComparison.Ordinal))
        {
            ReceiveTransferLeadershipSuggestion(request);
            return;
        }

        RaftNode node = new(ownerEndpoint);
        enqueueResponse(ownerEndpoint, new RaftResponderRequest(
            RaftResponderRequestType.TransferLeadershipSuggestion, node, request));
    }

    /// <summary>Queues an outbound responder message for the given endpoint.</summary>
    internal void EnqueueResponse(string endpoint, RaftResponderRequest request) =>
        enqueueResponse(endpoint, request);
}
