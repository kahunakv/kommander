
using Kommander.Data;
using Kommander.System;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kommander;

/// <summary>
/// The write path into consensus: every way a caller gets bytes proposed, quorum-acked, and
/// committed on a partition — single log, batch of logs, heterogeneous per-entry batch,
/// checkpoint — plus the manual commit/rollback of a ticket the caller kept open, and the
/// forwarding fallback for a range this node does not host.
/// <para>
/// Two contracts run through all of it. First, <see cref="RaftOperationStatus.ActiveProposal"/> is
/// not a failure: the partition already has a proposal in flight, so the call retries after a
/// short delay rather than surfacing an error. Second, a caller-visible result is only produced
/// after <see cref="WaitForQuorum"/>, so a returned success always means durable on a quorum —
/// the propose reply alone never is.
/// </para>
/// <para>
/// Concurrency: this type holds no mutable state beyond the test hook; ordering and the
/// single-proposal-at-a-time invariant belong to the partition executor. Retry loops are
/// therefore safe to run concurrently for different partitions and correctly serialise for the
/// same one.
/// </para>
/// </summary>
internal sealed class ReplicationGateway
{
    /// <summary>Backoff between attempts while a partition already has a proposal in flight.</summary>
    private static readonly TimeSpan ProposalRetryDelay = TimeSpan.FromMilliseconds(10);

    /// <summary>
    /// Forwards a proposal to a peer that replicates the range. Mirrors
    /// <c>ICommunication.ForwardReplicateLogs</c>; returns <see langword="null"/> when the peer is
    /// unreachable or the transport has no forwarding support.
    /// </summary>
    internal delegate Task<RaftReplicationResult?> ForwardReplicateLogsDelegate(
        RaftNode node,
        int partitionId,
        string type,
        IReadOnlyList<byte[]> logs,
        bool autoCommit,
        long expectedGeneration,
        CancellationToken cancellationToken);

    /// <summary>
    /// Test-only seam. When non-null, replaces the per-attempt partition call inside the
    /// <see cref="ReplicateLogs(int,string,IReadOnlyList{byte[]},bool,long,CancellationToken)"/>
    /// retry loop, so a test can return <see cref="RaftOperationStatus.ActiveProposal"/> then a
    /// terminal status to drive ≥2 iterations and assert the materialized payload list is reused
    /// across retries (not re-enumerated). Left null in production; the only production cost is one
    /// field read per replication call.
    /// </summary>
    internal Func<(bool success, RaftOperationStatus status, HLCTimestamp ticketId)>? ReplicateAttemptHookForTesting;

    private readonly IPartitionProvider partitionProvider;
    private readonly PartitionRoutingTable routingTable;
    private readonly ForwardReplicateLogsDelegate forwardReplicateLogs;
    private readonly ILogger<IRaft> logger;
    private readonly string localEndpoint;

    internal ReplicationGateway(
        IPartitionProvider partitionProvider,
        PartitionRoutingTable routingTable,
        ForwardReplicateLogsDelegate forwardReplicateLogs,
        ILogger<IRaft> logger,
        string localEndpoint)
    {
        this.partitionProvider = partitionProvider;
        this.routingTable = routingTable;
        this.forwardReplicateLogs = forwardReplicateLogs;
        this.logger = logger;
        this.localEndpoint = localEndpoint;
    }

    /// <summary>
    /// Replicate a single log to the follower nodes in the system partition
    /// </summary>
    internal async Task<RaftReplicationResult> ReplicateSystemLogs(string type, byte[] data, bool autoCommit = true, CancellationToken cancellationToken = default)
    {
        RaftPartition? systemPartition = partitionProvider.SystemPartition;

        if (systemPartition is null)
            throw new RaftException("System partition not initialized.");

        bool success;
        HLCTimestamp ticketId;
        RaftOperationStatus status;

        do
        {
            (success, status, ticketId) = await systemPartition.ReplicateLogs(type, data, autoCommit).ConfigureAwait(false);

            if (status == RaftOperationStatus.ActiveProposal)
                await Task.Delay(ProposalRetryDelay, cancellationToken).ConfigureAwait(false);

        } while (status == RaftOperationStatus.ActiveProposal);

        if (!success)
            return new(success, status, ticketId, -1);

        return await WaitForQuorum(systemPartition, ticketId, autoCommit, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Replicates a single log entry to the follower nodes in the specified partition.
    /// P0 routes committed entries by log type: <c>_RaftSystem</c> entries go to the system
    /// coordinator; all other types go to consumer callbacks (<c>OnReplicationReceived</c> /
    /// <c>OnLogRestored</c>).  Passing <c>type == "_RaftSystem"</c> on partition 0 is rejected
    /// with <see cref="RaftException"/> to prevent userland from forging coordinator entries.
    /// P0 is never a valid target for create, split, merge, or remove.
    /// </summary>
    internal async Task<RaftReplicationResult> ReplicateLogs(int partitionId, string type, byte[] data, bool autoCommit = true, long expectedGeneration = 0, CancellationToken cancellationToken = default)
    {
        if (partitionId == RaftSystemConfig.SystemPartition && type == RaftSystemConfig.RaftLogType)
            throw new RaftException("System log type is reserved on the system partition");

        // Single lookup instead of a hosts-check plus a re-resolve on the per-write path.
        RaftPartition? partition = null;

        if (partitionId != RaftSystemConfig.SystemPartition && !partitionProvider.TryGetDataPartition(partitionId, out partition))
        {
            RaftReplicationResult? forwarded = await ForwardToReplicaAsync(
                partitionId, type, [data], autoCommit, expectedGeneration, cancellationToken).ConfigureAwait(false);
            if (forwarded is not null)
                return forwarded;
        }

        partition ??= partitionProvider.GetPartition(partitionId);

        bool success;
        HLCTimestamp ticketId;
        RaftOperationStatus status;

        do
        {
            (success, status, ticketId) = await partition.ReplicateLogs(type, data, autoCommit, expectedGeneration).ConfigureAwait(false);

            if (status == RaftOperationStatus.ActiveProposal)
                await Task.Delay(ProposalRetryDelay, cancellationToken).ConfigureAwait(false);

        } while (status == RaftOperationStatus.ActiveProposal);

        if (!success)
            return new(success, status, ticketId, -1);

        return await WaitForQuorum(partition, ticketId, autoCommit, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Replicates a batch of log entries to the follower nodes in the specified partition.
    /// See the <see cref="IReadOnlyList{T}"/> overload for the full contract.
    /// </summary>
    internal Task<RaftReplicationResult> ReplicateLogs(
        int partitionId,
        string type,
        IEnumerable<byte[]> logs,
        bool autoCommit = true,
        long expectedGeneration = 0,
        CancellationToken cancellationToken = default
    )
    {
        // Materialize once before the retry loop so generator inputs are not re-enumerated on each
        // retry, and list/array inputs skip the copy.
        IReadOnlyList<byte[]> materializedLogs = logs as IReadOnlyList<byte[]> ?? logs.ToList();
        return ReplicateLogs(partitionId, type, materializedLogs, autoCommit, expectedGeneration, cancellationToken);
    }

    /// <summary>
    /// Replicates a batch of log entries to the follower nodes in the specified partition.
    /// Accepts an already-materialized list or array and avoids the intermediate copy
    /// incurred by the <see cref="IEnumerable{T}"/> overload for array and list callers.
    /// P0 routes committed entries by log type: <c>_RaftSystem</c> entries go to the system
    /// coordinator; all other types go to consumer callbacks (<c>OnReplicationReceived</c> /
    /// <c>OnLogRestored</c>).  Passing <c>type == "_RaftSystem"</c> on partition 0 is rejected
    /// with <see cref="RaftException"/> to prevent userland from forging coordinator entries.
    /// P0 is never a valid target for create, split, merge, or remove.
    /// </summary>
    internal async Task<RaftReplicationResult> ReplicateLogs(
        int partitionId,
        string type,
        IReadOnlyList<byte[]> logs,
        bool autoCommit = true,
        long expectedGeneration = 0,
        CancellationToken cancellationToken = default
    )
    {
        if (partitionId == RaftSystemConfig.SystemPartition && type == RaftSystemConfig.RaftLogType)
            throw new RaftException("System log type is reserved on the system partition");

        // Single lookup instead of a hosts-check plus a re-resolve on the per-write path.
        RaftPartition? partition = null;

        if (partitionId != RaftSystemConfig.SystemPartition && !partitionProvider.TryGetDataPartition(partitionId, out partition))
        {
            RaftReplicationResult? forwarded = await ForwardToReplicaAsync(
                partitionId, type, logs, autoCommit, expectedGeneration, cancellationToken).ConfigureAwait(false);
            if (forwarded is not null)
                return forwarded;
        }

        partition ??= partitionProvider.GetPartition(partitionId);

        bool success;
        HLCTimestamp ticketId;
        RaftOperationStatus status;

        do
        {
            // Test seam (null in production): lets a test drive the ActiveProposal retry loop
            // deterministically without a live leader, to prove the payload list is materialized
            // once before the loop and reused across retries rather than re-enumerated.
            (success, status, ticketId) = ReplicateAttemptHookForTesting is { } hook
                ? hook()
                : await partition.ReplicateLogs(type, logs, autoCommit, expectedGeneration).ConfigureAwait(false);

            if (status == RaftOperationStatus.ActiveProposal)
                await Task.Delay(ProposalRetryDelay, cancellationToken).ConfigureAwait(false);

        } while (status == RaftOperationStatus.ActiveProposal);

        if (!success)
            return new(success, status, ticketId, -1);

        return await WaitForQuorum(partition, ticketId, autoCommit, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Forwards a proposal for a range this node does not host to one of the range's replicas
    /// (voters first — the leader is always a voter). Each attempt runs through the remote
    /// node's own <c>ReplicateLogs</c> path, so leader checks and the generation fence apply
    /// there; <c>NodeIsNotLeader</c> moves on to the next replica, any other outcome is final.
    /// Returns <see langword="null"/> when the range has no committed replica set (legacy
    /// full replication — the local "invalid partition" throw is the right diagnosis) or the
    /// transport does not support forwarding, in which case consumers must route directly via
    /// the range's replica list.
    /// </summary>
    private async Task<RaftReplicationResult?> ForwardToReplicaAsync(
        int partitionId,
        string type,
        IReadOnlyList<byte[]> logs,
        bool autoCommit,
        long expectedGeneration,
        CancellationToken cancellationToken)
    {
        IReadOnlyList<RaftReplica>? replicas = routingTable.TryGetPlacementReplicas(partitionId);
        if (replicas is null)
            return null;

        RaftReplicationResult? lastRejection = null;

        foreach (RaftReplica replica in replicas
                     .OrderBy(r => r.Role == RaftReplicaRole.Voter ? 0 : 1))
        {
            if (replica.Endpoint == localEndpoint)
                continue;

            RaftReplicationResult? result = await forwardReplicateLogs(
                new RaftNode(replica.Endpoint), partitionId, type, logs,
                autoCommit, expectedGeneration, cancellationToken).ConfigureAwait(false);

            if (result is null)
                continue; // unreachable or transport lacks forwarding — try the next replica

            if (result.Status == RaftOperationStatus.NodeIsNotLeader)
            {
                lastRejection = result;
                continue;
            }

            return result;
        }

        return lastRejection;
    }

    /// <summary>
    /// Replicates a heterogeneous, per-entry-typed batch to one partition (<see cref="IRaft.ReplicateEntries"/>).
    /// A leading auto-commit group plus an optional single <b>trailing</b> manual group, with a
    /// <b>per-entry</b> generation fence. See <see cref="RaftProposalEntry"/> /
    /// <see cref="RaftBatchReplicationResult"/>.
    /// <para>
    /// <b>Per-entry fence.</b> Each entry's <see cref="RaftProposalEntry.ExpectedGeneration"/> is evaluated
    /// independently against the partition's current committed generation: a non-zero value that no longer
    /// matches fences <i>that</i> entry out (reported <see cref="RaftOperationStatus.PartitionMoved"/>, not
    /// appended) while its siblings proceed; zero opts the entry out of the fence entirely. A batch may thus
    /// mix hash-routed (generation-0) entries with fenced key-range entries. The classification reads the
    /// generation once here (it is <see cref="Interlocked"/>-published, so the read is safe off the executor
    /// thread). To also close the classify→append window for an unambiguous key-range batch — every admitted
    /// entry non-zero, none hash-routed — the admitted proposal carries that shared generation as an
    /// executor-side backstop, so a split/merge landing mid-flight fences the whole admitted group rather than
    /// admitting stale writes. A <i>mixed</i> admitted set (hash-routed + key-range) cannot use that backstop
    /// without also fencing the hash-routed entries, so its fence is best-effort at classification time.
    /// </para>
    /// <para>
    /// <b>Sequential commit.</b> The auto group commits <i>before</i> the manual group is proposed. Were they
    /// posted concurrently to coalesce the fsync, a failure to commit the auto group after the manual propose
    /// was already durable would let a later manual <c>CommitLogs</c> advance the commit index past the
    /// uncommitted auto entries. Committing auto first makes the manual group a clean uncommitted suffix on a
    /// committed prefix, so its ticket rolls back or commits without touching the auto entries. Flush
    /// coalescing is therefore opportunistic (scheduler linger), not guaranteed.
    /// </para>
    /// <para>
    /// Per-entry <see cref="RaftEntryResult.LogIndex"/> values come from the <see cref="RaftLog.Id"/> assigned
    /// in place during propose (not from a ticket <c>commitIndex</c>): each <see cref="RaftLog"/> list built
    /// here is the same instance set the state machine mutates, and the propose reply only resolves once those
    /// indices are durable, so reading them back is race-free. Results are index-aligned to the input list;
    /// fenced entries keep their input slot with <c>LogIndex = -1</c>.
    /// </para>
    /// </summary>
    internal async Task<RaftBatchReplicationResult> ReplicateEntries(int partitionId, IReadOnlyList<RaftProposalEntry> entries, CancellationToken cancellationToken = default)
    {
        if (entries is null || entries.Count == 0)
            return new(true, RaftOperationStatus.Success, HLCTimestamp.Zero, []);

        // ── Batch-level validation (shape) — reject before any append, no partial state. ──
        // An optional auto-commit prefix followed by an optional single trailing manual group: once a manual
        // (autoCommit:false) entry is seen, no later entry may be auto-commit, else the manual entries would
        // not form one contiguous truncatable suffix. Reserved system-log-type guard mirrors the
        // single-type ReplicateLogs path. Generations are NOT validated here — they are fenced per entry below.
        bool seenManual = false;

        for (int i = 0; i < entries.Count; i++)
        {
            RaftProposalEntry entry = entries[i];

            if (partitionId == RaftSystemConfig.SystemPartition && entry.Type == RaftSystemConfig.RaftLogType)
                throw new RaftException("System log type is reserved on the system partition");

            if (!entry.AutoCommit)
                seenManual = true;
            else if (seenManual)
                return RejectBatch(entries.Count, RaftOperationStatus.Errored); // auto-commit after manual
        }

        RaftPartition partition = partitionProvider.GetPartition(partitionId);
        long currentGeneration = partition.Generation;

        // ── Per-entry fence classification. Fenced entries take their result slot now (PartitionMoved) and are
        //    excluded from the append; admitted entries are split into the auto prefix and trailing manual
        //    group, each RaftLog carrying its input index so ids read back after propose stay index-aligned. ──
        RaftEntryResult[] results = new RaftEntryResult[entries.Count];

        List<RaftLog> autoLogs = [];
        List<int> autoInputIndex = [];
        List<RaftLog> manualLogs = [];
        List<int> manualInputIndex = [];
        bool admittedHasKeyRange = false; // any admitted entry with a non-zero (fenced) generation
        bool admittedHasHashRouted = false; // any admitted entry with generation 0

        for (int i = 0; i < entries.Count; i++)
        {
            RaftProposalEntry entry = entries[i];

            if (entry.ExpectedGeneration != 0 && entry.ExpectedGeneration != currentGeneration)
            {
                results[i] = new(RaftOperationStatus.PartitionMoved, -1, HLCTimestamp.Zero);
                continue;
            }

            RaftLog log = new() { Type = RaftLogType.Proposed, LogType = entry.Type, LogData = entry.Data };

            if (entry.AutoCommit)
            {
                autoLogs.Add(log);
                autoInputIndex.Add(i);
            }
            else
            {
                manualLogs.Add(log);
                manualInputIndex.Add(i);
            }

            if (entry.ExpectedGeneration == 0)
                admittedHasHashRouted = true;
            else
                admittedHasKeyRange = true;
        }

        // Admission backstop: only for an unambiguous key-range batch (all admitted entries fenced, none
        // hash-routed) can we re-assert the generation at executor admission without wrongly fencing a
        // hash-routed sibling. currentGeneration is safe here because every admitted key-range entry expects
        // exactly it (any other non-zero expectation was fenced out above).
        long admissionBackstop = admittedHasKeyRange && !admittedHasHashRouted ? currentGeneration : 0;

        // Nothing admitted (every entry fenced): report PartitionMoved overall so the caller refreshes the map.
        if (autoLogs.Count == 0 && manualLogs.Count == 0)
            return new(false, RaftOperationStatus.PartitionMoved, HLCTimestamp.Zero, results);

        HLCTimestamp batchTicket = HLCTimestamp.Zero;

        // ── Auto group: propose and commit with the batch. ──
        if (autoLogs.Count > 0)
        {
            (bool autoOk, RaftOperationStatus autoStatus, HLCTimestamp autoTicket) =
                await partition.ReplicateEntries(autoLogs, admissionBackstop, autoCommit: true).ConfigureAwait(false);

            if (!autoOk)
                return FailBatch(results, autoInputIndex, manualInputIndex, autoStatus);

            RaftReplicationResult autoQuorum = await WaitForQuorum(partition, autoTicket, true, cancellationToken).ConfigureAwait(false);

            if (!autoQuorum.Success)
                return FailBatch(results, autoInputIndex, manualInputIndex, autoQuorum.Status);

            for (int j = 0; j < autoLogs.Count; j++)
                results[autoInputIndex[j]] = new(RaftOperationStatus.Success, autoLogs[j].Id, HLCTimestamp.Zero);

            batchTicket = autoTicket;
        }

        // ── Manual group (optional): proposed only after the auto group committed, so it is a clean suffix.
        //    Its ticket is returned to the caller (Pending), who commits or rolls it back later. ──
        if (manualLogs.Count > 0)
        {
            (bool manualOk, RaftOperationStatus manualStatus, HLCTimestamp manualTicket) =
                await partition.ReplicateEntries(manualLogs, admissionBackstop, autoCommit: false).ConfigureAwait(false);

            if (!manualOk)
                return FailBatch(results, [], manualInputIndex, manualStatus);

            // For a manual proposal, WaitForQuorum resolves at propose-quorum durability (the ticket stays
            // live in activeProposals for the caller's later CommitLogs/RollbackLogs).
            RaftReplicationResult manualQuorum = await WaitForQuorum(partition, manualTicket, false, cancellationToken).ConfigureAwait(false);

            if (!manualQuorum.Success)
                return FailBatch(results, [], manualInputIndex, manualQuorum.Status);

            for (int j = 0; j < manualLogs.Count; j++)
                results[manualInputIndex[j]] = new(RaftOperationStatus.Pending, manualLogs[j].Id, manualTicket);

            // The manual ticket is the actionable one for the caller; surface it as the batch ticket.
            batchTicket = manualTicket;
        }

        // Some entries may still be PartitionMoved (per-entry fence); overall success reflects that at least
        // one entry was admitted and committed/queued.
        return new(true, RaftOperationStatus.Success, batchTicket, results);
    }

    /// <summary>
    /// Builds a batch-level rejection result: overall failure with <paramref name="status"/> propagated to
    /// every entry slot and <c>LogIndex = -1</c>, signalling that nothing was appended.
    /// </summary>
    private static RaftBatchReplicationResult RejectBatch(int count, RaftOperationStatus status)
    {
        RaftEntryResult[] results = new RaftEntryResult[count];
        for (int i = 0; i < count; i++)
            results[i] = new(status, -1, HLCTimestamp.Zero);

        return new(false, status, HLCTimestamp.Zero, results);
    }

    /// <summary>
    /// Marks the still-in-flight admitted entries (identified by their input indices) with a proposal
    /// <paramref name="status"/> failure while leaving already-resolved slots — per-entry
    /// <see cref="RaftOperationStatus.PartitionMoved"/> fences and any committed auto entries — untouched.
    /// Used when an admitted proposal fails after some entries were already accounted for.
    /// </summary>
    private static RaftBatchReplicationResult FailBatch(RaftEntryResult[] results, IReadOnlyList<int> autoInputIndex, IReadOnlyList<int> manualInputIndex, RaftOperationStatus status)
    {
        foreach (int i in autoInputIndex)
            results[i] = new(status, -1, HLCTimestamp.Zero);
        foreach (int i in manualInputIndex)
            results[i] = new(status, -1, HLCTimestamp.Zero);

        return new(false, status, HLCTimestamp.Zero, results);
    }

    /// <summary>
    /// Commit logs and notify followers in the partition
    /// </summary>
    internal async Task<(bool success, RaftOperationStatus status, long commitLogId)> CommitLogs(int partitionId, HLCTimestamp ticketId, CancellationToken cancellationToken = default)
    {
        RaftPartition partition = partitionProvider.GetPartition(partitionId);

        return await partition.CommitLogs(ticketId, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Rollback logs and notify followers in the partition
    /// </summary>
    internal async Task<(bool success, RaftOperationStatus status, long commitLogId)> RollbackLogs(int partitionId, HLCTimestamp ticketId, CancellationToken cancellationToken = default)
    {
        RaftPartition partition = partitionProvider.GetPartition(partitionId);

        return await partition.RollbackLogs(ticketId, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Replicates a checkpoint to the follower nodes
    /// </summary>
    internal async Task<RaftReplicationResult> ReplicateCheckpoint(int partitionId, CancellationToken cancellationToken = default)
    {
        RaftPartition partition = partitionProvider.GetPartition(partitionId);

        bool success;
        HLCTimestamp ticketId;
        RaftOperationStatus status;

        do
        {
            (success, status, ticketId) = await partition.ReplicateCheckpoint().ConfigureAwait(false);

            if (status == RaftOperationStatus.ActiveProposal)
                await Task.Delay(ProposalRetryDelay, cancellationToken).ConfigureAwait(false);

        } while (status == RaftOperationStatus.ActiveProposal);

        if (!success)
            return new(success, status, ticketId, -1);

        return await WaitForQuorum(partition, ticketId, true, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Waits for the replication proposal to be completed in the given partition using
    /// event-driven notification rather than periodic polling.
    /// <para>
    /// One executor round-trip is made to obtain the proposal's completion task; subsequent
    /// progress is delivered without executor involvement as the state machine fires
    /// <see cref="RaftProposalQuorum.CompleteWaiter"/> on commit, rollback, or leader loss.
    /// A 10-second timeout is enforced via <see cref="Task.WaitAsync(TimeSpan,CancellationToken)"/>
    /// so that the caller's wait is bounded identically to the previous polling loop.
    /// </para>
    /// <para>
    /// Falls back to a single <see cref="RaftPartition.GetTicketState"/> poll when the
    /// completion task cannot be retrieved (proposal not found in <c>activeProposals</c>),
    /// which can happen if the proposal completed and was cleaned up between the
    /// <c>ReplicateLogs</c> response and the <c>GetTicketWaiterTask</c> request.
    /// </para>
    /// </summary>
    private async Task<RaftReplicationResult> WaitForQuorum(RaftPartition partition, HLCTimestamp ticketId, bool autoCommit, CancellationToken cancellationToken)
    {
        // The proposal was already ACCEPTED (it has a ticket): if leadership moved between the
        // accept and this check, the entry is in the log and may still commit — a durable
        // Proposed row inherited by the next leader is committed by its promotion barrier
        // (Raft §5.4.2). Answering NodeIsNotLeader here told the client "definitely did not take
        // effect" for a write that could (and did) materialize; post-accept, the only honest
        // answer is indeterminate.
        if (!string.IsNullOrEmpty(partition.Leader) && partition.Leader != localEndpoint)
            return new(false, RaftOperationStatus.ProposalOutcomeUnknown, ticketId, -1);

        cancellationToken.ThrowIfCancellationRequested();

        Task<(RaftProposalTicketState, long)>? waiterTask = null;

        try
        {
            waiterTask = await partition.GetTicketWaiterTaskAsync(ticketId).ConfigureAwait(false);
        }
        catch (Exception e) when (e is not OperationCanceledException)
        {
            logger.LogError("WaitForQuorum: GetTicketWaiterTask failed: {Message}", e.Message);
        }

        if (waiterTask is null)
        {
            // Proposal is not in activeProposals — either it completed before we could retrieve
            // the waiter, or it was never registered. Fall back to a single poll.
            try
            {
                (RaftProposalTicketState state, long commitId) = await partition.GetTicketState(ticketId, autoCommit).ConfigureAwait(false);
                return state == RaftProposalTicketState.Committed
                    ? new(true, RaftOperationStatus.Success, ticketId, commitId)
                    : new(false, RaftOperationStatus.ReplicationFailed, ticketId, -1);
            }
            catch (Exception e) when (e is not OperationCanceledException)
            {
                logger.LogError("WaitForQuorum: GetTicketState fallback failed: {Message}", e.Message);
                return new(false, RaftOperationStatus.ReplicationFailed, ticketId, -1);
            }
        }

        try
        {
            // WaitAsync(TimeSpan, token) rides the shared timer queue — the previous linked
            // CancellationTokenSource allocated a CTS + registration + timer per successful
            // proposal. Caller cancellation still surfaces as OperationCanceledException;
            // the elapsed timeout surfaces as TimeoutException instead of a filtered OCE.
            (RaftProposalTicketState ticketState, long commitIndex) = await waiterTask
                .WaitAsync(TimeSpan.FromMilliseconds(10_000), cancellationToken).ConfigureAwait(false);

            return ticketState == RaftProposalTicketState.Committed
                ? new(true, RaftOperationStatus.Success, ticketId, commitIndex)
                : new(false, RaftOperationStatus.ReplicationFailed, ticketId, -1);
        }
        catch (TimeoutException)
        {
            // 10-second timeout elapsed without a terminal state transition.
            return new(false, RaftOperationStatus.ProposalTimeout, ticketId, -1);
        }
    }
}
