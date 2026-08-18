
using System.Diagnostics;
using Kommander.Data;
using Kommander.Diagnostics;
using Kommander.WAL;
using Microsoft.Extensions.Logging;

namespace Kommander;

/// <summary>
/// Everything a caller can ask or do about <i>who leads a partition</i>: believing it
/// (<see cref="AmILeader"/>), proving it (<see cref="ConfirmLeadershipAsync"/>), waiting for it
/// (<see cref="WaitForLeader"/>), and moving it (<see cref="StepDownAsync"/>,
/// <see cref="TransferLeadershipAsync"/>).
/// <para>
/// The distinction that runs through this type is belief versus proof. <see cref="AmILeader"/> and
/// <see cref="AmILeaderQuick"/> answer from local state and are fast paths only; the confirmation
/// methods never report success from local belief alone and are the ones a linearizable read may
/// depend on. Mixing the two up is the failure mode this grouping is meant to make obvious.
/// </para>
/// <para>
/// Concurrency: this type holds no mutable state beyond the test hook. Every method resolves the
/// partition fresh through <see cref="IPartitionProvider"/> and delegates ordering to the
/// partition executor, so concurrent callers cannot interleave into inconsistent state here.
/// </para>
/// </summary>
internal sealed class LeadershipService
{
    /// <summary>
    /// Cadence for polling a partition's leadership state while waiting for an election, a
    /// step-down, or a transfer to settle. Short because each poll is a cheap local read; the
    /// enclosing waits are bounded by their own 10 s budgets.
    /// </summary>
    private static readonly TimeSpan LeaderStatusPollDelay = TimeSpan.FromMilliseconds(10);

    /// <summary>
    /// Test-only seam. When non-null, replaces <see cref="AmILeaderQuick"/> so coordinator
    /// harness tests (which never join a cluster and therefore have no system partition) can
    /// exercise leader-gated paths such as the replica-placement controller pass. Left null in
    /// production; the only cost is one field read per call.
    /// </summary>
    internal Func<int, ValueTask<bool>>? AmILeaderQuickHookForTesting;

    private readonly IPartitionProvider partitionProvider;
    private readonly IWAL walAdapter;
    private readonly Func<bool> isInitialized;
    private readonly Func<bool> joined;
    private readonly Func<List<RaftNode>> getNodes;
    private readonly Func<RaftNode, GetReadIndexRequest, CancellationToken, Task<GetReadIndexResponse>> fetchReadIndex;
    private readonly Func<RaftNode, HandshakeRequest, Task<HandshakeResponse>> sendHandshake;
    private readonly ILogger<IRaft> logger;
    private readonly string localEndpoint;
    private readonly int localNodeId;

    internal LeadershipService(
        IPartitionProvider partitionProvider,
        IWAL walAdapter,
        Func<bool> isInitialized,
        Func<bool> joined,
        Func<List<RaftNode>> getNodes,
        Func<RaftNode, GetReadIndexRequest, CancellationToken, Task<GetReadIndexResponse>> fetchReadIndex,
        Func<RaftNode, HandshakeRequest, Task<HandshakeResponse>> sendHandshake,
        ILogger<IRaft> logger,
        string localEndpoint,
        int localNodeId)
    {
        this.partitionProvider = partitionProvider;
        this.walAdapter = walAdapter;
        this.isInitialized = isInitialized;
        this.joined = joined;
        this.getNodes = getNodes;
        this.fetchReadIndex = fetchReadIndex;
        this.sendHandshake = sendHandshake;
        this.logger = logger;
        this.localEndpoint = localEndpoint;
        this.localNodeId = localNodeId;
    }

    /// <summary>
    /// Checks if the local node is the leader in the given partition. Throws the typed
    /// <see cref="PartitionNotHostedException"/> for a committed range this node does not host
    /// (the documented routing contract), so callers that legitimately ask about non-hosted ranges
    /// (e.g. the placement controller) must gate on <c>HostsPartition</c> first instead of calling
    /// blind.
    /// </summary>
    internal async ValueTask<bool> AmILeaderQuick(int partitionId)
    {
        if (AmILeaderQuickHookForTesting is { } hook)
            return await hook(partitionId).ConfigureAwait(false);

        if (!isInitialized())
            return false;

        RaftPartition partition = partitionProvider.GetPartition(partitionId);

        if (!string.IsNullOrEmpty(partition.Leader) && partition.Leader == localEndpoint)
            return true;

        try
        {
            RaftNodeState response = await partition.GetState().ConfigureAwait(false);

            return response == RaftNodeState.Leader;
        }
        catch (Exception e) when (e is not OperationCanceledException)
        {
            logger.LogError("AmILeaderQuick: {Message}", e.Message);
        }

        return false;
    }

    /// <summary>
    /// Checks if the local node is the leader in the given partition, waiting up to 10 s for the
    /// partition's state to become decided.
    /// </summary>
    internal async ValueTask<bool> AmILeader(int partitionId, CancellationToken cancellationToken)
    {
        if (!isInitialized())
            return false;

        RaftPartition partition = partitionProvider.GetPartition(partitionId);

        ValueStopwatch stopwatch = ValueStopwatch.StartNew();

        while (stopwatch.GetElapsedMilliseconds() < 10000)
        {
            if (!string.IsNullOrEmpty(partition.Leader) && partition.Leader == localEndpoint)
                return true;

            cancellationToken.ThrowIfCancellationRequested();

            try
            {
                RaftNodeState response = await partition.GetState(cancellationToken).ConfigureAwait(false);

                return response == RaftNodeState.Leader;
            }
            catch (Exception e) when (e is not OperationCanceledException)
            {
                logger.LogError("AmILeader: {Message}", e.Message);
            }

            await Task.Delay(LeaderStatusPollDelay, cancellationToken).ConfigureAwait(false);
        }

        throw new RaftException("Leader couldn't be found or is not decided");
    }

    /// <summary>
    /// Read-index leadership confirmation for the given partition — see
    /// <see cref="IRaft.ConfirmLeadershipAsync"/> for the contract. Unlike
    /// <see cref="AmILeader"/> this never reports success from local belief alone: the state
    /// machine must collect a same-term quorum ack round and catch the applied frontier up to the
    /// confirmed commit index. Transport or executor errors map to <see langword="false"/> (the
    /// caller's retry path), matching how a failed quorum round is reported; caller-requested
    /// cancellation propagates as <see cref="OperationCanceledException"/>.
    /// </summary>
    internal async ValueTask<bool> ConfirmLeadershipAsync(int partitionId, CancellationToken cancellationToken = default)
    {
        if (!isInitialized())
            return false;

        RaftPartition partition = partitionProvider.GetPartition(partitionId);

        try
        {
            return await partition.ConfirmLeadershipAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception e) when (e is not OperationCanceledException)
        {
            logger.LogError("ConfirmLeadershipAsync: {Message}", e.Message);
            return false;
        }
    }

    /// <summary>
    /// Follower catch-up confirmation — see <see cref="IRaft.ConfirmLocalApplicationAsync"/> for
    /// the contract. On the (believed) leader the call degenerates to
    /// <see cref="ConfirmLeadershipAsync"/>: the leader's own applied-frontier wait is the same
    /// proof. Otherwise the classic §6.4 follower read runs: fetch a quorum-confirmed commit
    /// index from the leader (trustworthy because a deposed leader's ack round cannot complete),
    /// then wait until the local applied frontier covers it. No leader known, failed fetch,
    /// transport error, wait timeout, or restore in progress all map to <see langword="false"/>;
    /// no retries happen inside the primitive — the caller owns retry cadence, mirroring
    /// <see cref="ConfirmLeadershipAsync"/>.
    /// <para>The leader-belief routing is a fast path, not a correctness gate: a stale belief
    /// either fails the local confirmation (this node is not really the leader) or fails the
    /// remote fetch (the target is not really the leader) — both land on
    /// <see langword="false"/>, never on a false confirmation.</para>
    /// </summary>
    internal async ValueTask<bool> ConfirmLocalApplicationAsync(int partitionId, CancellationToken cancellationToken = default)
    {
        if (!isInitialized())
            return false;

        try
        {
            RaftPartition partition = partitionProvider.GetPartition(partitionId);

            string leader = partition.Leader;
            if (leader == localEndpoint)
                return await partition.ConfirmLeadershipAsync(cancellationToken).ConfigureAwait(false);

            if (string.IsNullOrEmpty(leader))
                return false;

            GetReadIndexResponse response = await fetchReadIndex(
                new RaftNode(leader), new GetReadIndexRequest(partitionId), cancellationToken).ConfigureAwait(false);

            if (!response.Success || response.ReadIndex < 0)
                return false;

            return await partition.WaitLocalApplicationAsync(response.ReadIndex, cancellationToken).ConfigureAwait(false);
        }
        catch (PartitionNotHostedException e)
        {
            // Expected under replica placement — this node simply isn't a replica of the range.
            // Fail closed without error-level noise: routers probe this per request.
            if (logger.IsEnabled(LogLevel.Debug))
                logger.LogDebug("ConfirmLocalApplicationAsync: {Message}", e.Message);
            return false;
        }
        catch (Exception e) when (e is not OperationCanceledException)
        {
            logger.LogError("ConfirmLocalApplicationAsync: {Message}", e.Message);
            return false;
        }
    }

    /// <summary>
    /// Serves a non-leader's read-index fetch: runs this node's read-index confirmation round for
    /// the partition and returns the captured commit index on success. Failure — including this
    /// node not being the partition leader — returns an unsuccessful response so the remote caller
    /// fails closed.
    /// </summary>
    internal async ValueTask<GetReadIndexResponse> ReceiveGetReadIndex(GetReadIndexRequest request, CancellationToken cancellationToken = default)
    {
        if (!isInitialized())
            return new GetReadIndexResponse(false);

        try
        {
            RaftPartition partition = partitionProvider.GetPartition(request.PartitionId);

            (RaftOperationStatus status, long readIndex) = await partition
                .GetConfirmedReadIndexAsync(cancellationToken).ConfigureAwait(false);

            return status == RaftOperationStatus.Success && readIndex >= 0
                ? new GetReadIndexResponse(true, readIndex)
                : new GetReadIndexResponse(false);
        }
        catch (Exception e) when (e is not OperationCanceledException)
        {
            logger.LogError("ReceiveGetReadIndex: {Message}", e.Message);
            return new GetReadIndexResponse(false);
        }
    }

    /// <summary>
    /// Waits for the leader to be elected in the given partition.
    /// If the leader is already elected, it returns the leader.
    ///
    /// The method first waits for the partition's WAL restore to complete. That wait is bounded
    /// only by <paramref name="cancellationToken"/> — never by a fixed budget — because during a
    /// large or slow replay the partition cannot settle on a leader and <see cref="RaftNodeState"/>
    /// stays undecided. Only after restore has finished does the fixed 10 s election budget apply,
    /// which is its original intent (bounding genuine election latency, not restore work).
    ///
    /// A faulted restore surfaces to the caller as a <see cref="RaftException"/> wrapping the real
    /// cause rather than the generic "leader couldn't be found" timeout.
    /// </summary>
    internal async ValueTask<string> WaitForLeader(int partitionId, CancellationToken cancellationToken)
    {
        // Before initialization the partition map hasn't been applied and user partitions don't
        // exist yet, so GetPartition would throw "Invalid partition" for ids that are perfectly
        // valid once assembly completes. Mirror the AmILeader/AmILeaderQuick guard, but surface
        // the condition as a typed retryable exception instead of a silent default: this method
        // must return a leader endpoint and has none to give. Without this guard a restarting
        // node that already reports Joined (set at the start of both join paths) leaks the
        // misleading "Invalid partition" error to routing callers.
        if (!isInitialized())
            throw new RaftNodeNotReadyException(
                $"Cannot resolve leader for partition {partitionId}: node has not completed cluster initialization");

        RaftPartition partition = partitionProvider.GetPartition(partitionId);

        // Wait for WAL restore before starting the election budget. This wait is bounded only by the
        // caller's cancellation token; it is deliberately outside the poll loop's swallowing catch so
        // a faulted restore is reported as its real cause instead of a generic election timeout.
        try
        {
            await partition.RestoreTask.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception e)
        {
            throw new RaftException($"Partition {partitionId} restore failed", e);
        }

        ValueStopwatch stopwatch = ValueStopwatch.StartNew();

        while (stopwatch.GetElapsedMilliseconds() < 10000)
        {
            cancellationToken.ThrowIfCancellationRequested();

            try
            {
                RaftNodeState response = await partition.GetState(cancellationToken).ConfigureAwait(false);

                if (response == RaftNodeState.Leader)
                    return localEndpoint;

                if (string.IsNullOrEmpty(partition.Leader))
                {
                    await Task.Delay(100 + Random.Shared.Next(-50, 50), cancellationToken).ConfigureAwait(false);
                    continue;
                }

                return partition.Leader;
            }
            catch (Exception e) when (e is not OperationCanceledException)
            {
                logger.LogError("WaitForLeader: {Message}", e.Message);
            }
        }

        throw new RaftException("Leader couldn't be found or is not decided");
    }

    /// <summary>
    /// Waits until the partition has held the same leader for <paramref name="minStableFor"/>,
    /// with no overall deadline beyond the caller's token.
    /// </summary>
    internal ValueTask<string> WaitForLeaderStableAsync(
        int partitionId,
        TimeSpan minStableFor,
        CancellationToken cancellationToken = default)
    {
        if (!isInitialized())
            throw new RaftNodeNotReadyException("Raft manager is not initialized");

        return partitionProvider.GetPartition(partitionId).WaitForLeaderStableAsync(minStableFor, timeout: null, cancellationToken);
    }

    /// <summary>
    /// Waits until the partition has held the same leader for <paramref name="minStableFor"/>,
    /// giving up after <paramref name="timeout"/>.
    /// </summary>
    internal ValueTask<string> WaitForLeaderStableAsync(
        int partitionId,
        TimeSpan minStableFor,
        TimeSpan timeout,
        CancellationToken cancellationToken = default)
    {
        if (!isInitialized())
            throw new RaftNodeNotReadyException("Raft manager is not initialized");

        return partitionProvider.GetPartition(partitionId).WaitForLeaderStableAsync(minStableFor, timeout, cancellationToken);
    }

    /// <summary>
    /// Test-only: forces this node to campaign for the partition, then waits up to 10 s for the
    /// outcome to settle.
    /// </summary>
    internal async Task<RaftOperationStatus> ForceLeaderForTestingAsync(
        int partitionId,
        CancellationToken cancellationToken = default)
    {
        if (!joined() || !isInitialized())
            return RaftOperationStatus.Errored;

        RaftPartition partition;

        try
        {
            partition = partitionProvider.GetPartition(partitionId);
        }
        catch (RaftException)
        {
            return RaftOperationStatus.Errored;
        }

        RaftOperationStatus status = await partition.ForceLeaderForTestingAsync(cancellationToken).ConfigureAwait(false);
        if (status != RaftOperationStatus.Pending)
            return status;

        ValueStopwatch stopwatch = ValueStopwatch.StartNew();

        while (stopwatch.GetElapsedMilliseconds() < 10000)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (!string.IsNullOrEmpty(partition.Leader))
            {
                if (partition.Leader == localEndpoint)
                    return RaftOperationStatus.Success;

                return RaftOperationStatus.LeaderAlreadyElected;
            }

            try
            {
                RaftNodeState nodeState = await partition.GetState(cancellationToken).ConfigureAwait(false);
                if (nodeState == RaftNodeState.Leader)
                    return RaftOperationStatus.Success;
            }
            catch (Exception e) when (e is not OperationCanceledException)
            {
                logger.LogWarning("ForceLeaderForTestingAsync: {Message}", e.Message);
            }

            await Task.Delay(LeaderStatusPollDelay, cancellationToken).ConfigureAwait(false);
        }

        return RaftOperationStatus.Pending;
    }

    /// <summary>
    /// Relinquishes leadership of the partition and waits up to 10 s for a successor to announce
    /// itself.
    /// </summary>
    internal async Task<RaftOperationStatus> StepDownAsync(
        int partitionId,
        CancellationToken cancellationToken = default)
    {
        if (!joined() || !isInitialized())
            return RaftOperationStatus.Errored;

        RaftPartition partition;

        try
        {
            partition = partitionProvider.GetPartition(partitionId);
        }
        catch (RaftException)
        {
            return RaftOperationStatus.Errored;
        }

        RaftOperationStatus status = await partition.StepDownAsync(cancellationToken).ConfigureAwait(false);
        if (status != RaftOperationStatus.Pending)
            return status;

        ValueStopwatch stopwatch = ValueStopwatch.StartNew();

        while (stopwatch.GetElapsedMilliseconds() < 10000)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (!string.IsNullOrEmpty(partition.Leader))
            {
                if (partition.Leader == localEndpoint)
                {
                    await Task.Delay(LeaderStatusPollDelay, cancellationToken).ConfigureAwait(false);
                    continue;
                }

                return RaftOperationStatus.Success;
            }

            try
            {
                RaftNodeState nodeState = await partition.GetState(cancellationToken).ConfigureAwait(false);
                if (nodeState != RaftNodeState.Leader && !string.IsNullOrEmpty(partition.Leader) && partition.Leader != localEndpoint)
                    return RaftOperationStatus.Success;
            }
            catch (Exception e) when (e is not OperationCanceledException)
            {
                logger.LogWarning("StepDownAsync: {Message}", e.Message);
            }

            await Task.Delay(LeaderStatusPollDelay, cancellationToken).ConfigureAwait(false);
        }

        return RaftOperationStatus.Pending;
    }

    /// <summary>
    /// Relinquishes leadership of <paramref name="partitionId"/> WITHOUT waiting for a successor to be
    /// elected. Used only by the self-removal teardown path: a node that the committed roster has
    /// already dropped from the voter set is shutting down and does not need — and must not block
    /// on — a handoff. The remaining voters elect a new leader on their own election timeout.
    ///
    /// <para>This exists because <see cref="StepDownAsync"/> polls up to 10 s for a successor to
    /// announce itself, which is correct for an in-service leadership transfer but pathological
    /// during a coordinated shutdown: no peer campaigns for the handoff, so every partition's
    /// step-down burns the full 10 s (spamming the executor as it stops), and across many
    /// partitions/nodes this serialises into multi-minute teardowns. The local step-down (state →
    /// Follower, leadership relinquished, StepDownNotice sent) is already complete when
    /// <see cref="RaftPartition.StepDownAsync"/> returns <see cref="RaftOperationStatus.Pending"/>;
    /// we simply do not wait past that point.</para>
    /// </summary>
    internal async Task<RaftOperationStatus> StepDownWithoutSuccessorWaitAsync(
        int partitionId,
        CancellationToken cancellationToken = default)
    {
        if (!joined() || !isInitialized())
            return RaftOperationStatus.Errored;

        RaftPartition partition;

        try
        {
            partition = partitionProvider.GetPartition(partitionId);
        }
        catch (RaftException)
        {
            return RaftOperationStatus.Errored;
        }

        RaftOperationStatus status = await partition.StepDownAsync(cancellationToken).ConfigureAwait(false);

        // Pending == the local step-down succeeded but a successor has not yet been confirmed. For the
        // removed-and-tearing-down node that is exactly the state we want to return on; treat it as success.
        return status == RaftOperationStatus.Pending ? RaftOperationStatus.Success : status;
    }

    /// <summary>
    /// Hands leadership of the partition to <paramref name="targetEndpoint"/>, retrying once
    /// behind a handshake probe when the first attempt fails on replication, then waiting up to
    /// 10 s for the handover to settle.
    /// </summary>
    internal async Task<RaftOperationStatus> TransferLeadershipAsync(
        int partitionId,
        string targetEndpoint,
        CancellationToken cancellationToken = default)
    {
        if (!joined() || !isInitialized())
            return RaftOperationStatus.Errored;

        RaftPartition partition;

        try
        {
            partition = partitionProvider.GetPartition(partitionId);
        }
        catch (RaftException)
        {
            return RaftOperationStatus.Errored;
        }

        RaftOperationStatus status = await partition.TransferLeadershipAsync(targetEndpoint, cancellationToken).ConfigureAwait(false);
        if (status == RaftOperationStatus.ReplicationFailed)
            status = await RetryTransferLeadershipAfterProbeAsync(partition, partitionId, targetEndpoint, cancellationToken).ConfigureAwait(false);

        if (status != RaftOperationStatus.Pending)
            return status;

        ValueStopwatch stopwatch = ValueStopwatch.StartNew();

        while (stopwatch.GetElapsedMilliseconds() < 10000)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (!string.IsNullOrEmpty(partition.Leader))
            {
                if (partition.Leader == targetEndpoint)
                    return RaftOperationStatus.Success;

                if (partition.Leader != localEndpoint)
                    return RaftOperationStatus.LeaderAlreadyElected;
            }

            try
            {
                RaftNodeState nodeState = await partition.GetState(cancellationToken).ConfigureAwait(false);
                if (nodeState != RaftNodeState.Leader && partition.Leader == targetEndpoint)
                    return RaftOperationStatus.Success;
            }
            catch (Exception e) when (e is not OperationCanceledException)
            {
                logger.LogWarning("TransferLeadershipAsync: {Message}", e.Message);
            }

            await Task.Delay(LeaderStatusPollDelay, cancellationToken).ConfigureAwait(false);
        }

        return RaftOperationStatus.Pending;
    }

    /// <summary>Stops sending heartbeats for the partition (test/diagnostic seam).</summary>
    internal async Task<RaftOperationStatus> SuspendHeartbeatsAsync(
        int partitionId,
        CancellationToken cancellationToken = default)
    {
        if (!joined() || !isInitialized())
            return RaftOperationStatus.Errored;

        try
        {
            return await partitionProvider.GetPartition(partitionId).SuspendHeartbeatsAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (RaftException)
        {
            return RaftOperationStatus.Errored;
        }
    }

    /// <summary>Resumes heartbeats for the partition (test/diagnostic seam).</summary>
    internal async Task<RaftOperationStatus> ResumeHeartbeatsAsync(
        int partitionId,
        CancellationToken cancellationToken = default)
    {
        if (!joined() || !isInitialized())
            return RaftOperationStatus.Errored;

        try
        {
            return await partitionProvider.GetPartition(partitionId).ResumeHeartbeatsAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (RaftException)
        {
            return RaftOperationStatus.Errored;
        }
    }

    /// <summary>
    /// Second attempt at a leadership transfer that failed on replication: handshakes the target
    /// directly to refresh its log position, feeds that back into the partition, drains, and
    /// retries. A transfer can fail purely because the leader's view of the target's log is stale,
    /// which the probe repairs without another election.
    /// </summary>
    private async Task<RaftOperationStatus> RetryTransferLeadershipAfterProbeAsync(
        RaftPartition partition,
        int partitionId,
        string targetEndpoint,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(targetEndpoint) || targetEndpoint == localEndpoint)
            return RaftOperationStatus.ReplicationFailed;

        RaftNode? targetNode = getNodes().FirstOrDefault(node => node.Endpoint == targetEndpoint);
        if (targetNode is null)
            return RaftOperationStatus.ReplicationFailed;

        HandshakeResponse response;

        try
        {
            response = await sendHandshake(targetNode, new HandshakeRequest(
                localNodeId,
                partitionId,
                walAdapter.GetMaxLog(partitionId),
                localEndpoint)).ConfigureAwait(false);
        }
        catch (Exception e) when (e is not OperationCanceledException)
        {
            logger.LogWarning("TransferLeadershipAsync probe: {Message}", e.Message);
            return RaftOperationStatus.ReplicationFailed;
        }

        if (string.IsNullOrEmpty(response.Endpoint))
            response = new HandshakeResponse(response.NodeId, response.MaxLogId, targetEndpoint);

        partition.Handshake(new HandshakeRequest(
            response.NodeId,
            partitionId,
            response.MaxLogId,
            response.Endpoint));

        await partition.DrainAsync(cancellationToken).ConfigureAwait(false);

        return await partition.TransferLeadershipAsync(targetEndpoint, cancellationToken).ConfigureAwait(false);
    }
}
