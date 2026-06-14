
using System.Collections.Concurrent;
using System.Text.Json;
using System.Threading.Channels;
using Kommander.System.Protos;
using Google.Protobuf;
using Kommander.Data;
using Microsoft.Extensions.Logging;
using Kommander.Discovery;

namespace Kommander.System;

/// <summary>
/// Non-actor replacement for <c>RaftSystemActor</c>.  Owns a single-consumer
/// <see cref="Channel{T}"/> so all system-partition events are processed serially
/// without any Nixie dependency.
/// </summary>
internal sealed class RaftSystemCoordinator : IDisposable
{
    private const int MaxRetries = 10;

    private readonly Dictionary<string, string> systemConfiguration = new();

    private readonly RaftManager manager;

    private readonly ILogger<IRaft> logger;

    private string? leaderNode;

    private readonly Channel<RaftSystemRequest> _channel =
        Channel.CreateUnbounded<RaftSystemRequest>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false,
            AllowSynchronousContinuations = false
        });

    private readonly CancellationTokenSource _cts = new();
    private readonly Task _loop;

    // ── Test injection points ──────────────────────────────────────────────
    // Null in production. Tests set these to intercept replication and partition
    // activation without requiring a real Raft quorum or system partition.
    internal Func<string, byte[], bool, CancellationToken, Task<RaftReplicationResult>>? ReplicateOverride;
    internal Action<List<RaftPartitionRange>>? StartPartitionsOverride;

    /// <summary>
    /// Test hook for <see cref="ReplicateCheckpointForPartition"/>.
    /// When set, replaces the call to <see cref="RaftManager.ReplicateCheckpoint"/> so unit
    /// tests can assert checkpoint replication without a live Raft quorum.
    /// </summary>
    internal Func<int, CancellationToken, Task<RaftReplicationResult>>? ReplicateCheckpointOverride;

    /// <summary>
    /// Delay between replication retries. Defaults to 5 seconds in production;
    /// tests set this to <see cref="TimeSpan.Zero"/> for fast retry cycles.
    /// </summary>
    internal TimeSpan RetryDelay = TimeSpan.FromSeconds(5);

    // Queue of drain sentinels.  Each DrainAsync() call enqueues one TCS here
    // and sends a DrainSentinel request; the loop completes it in FIFO order.
    private readonly ConcurrentQueue<TaskCompletionSource> _drainQueue = new();

    // ── Membership state ───────────────────────────────────────────────────
    // Cached view of the committed roster.  Only updated from within the single-
    // consumer loop so no locking is required.
    private ClusterMembership _cachedMembership = new() { MembershipVersion = 0, Members = [] };

    // The single-consumer channel loop already serialises membership changes: Receive
    // is awaited to completion before the next message is dequeued, so two changes can
    // never interleave through the normal path.  This flag is belt-and-suspenders for
    // code paths that bypass the loop (e.g. the MembershipChangePendingForTest hook)
    // and as a test affordance to simulate an in-flight change.
    private bool _membershipChangePending;

    /// <summary>Test hook — lets tests inject the pending-change flag directly.</summary>
    internal bool MembershipChangePendingForTest
    {
        get => _membershipChangePending;
        set => _membershipChangePending = value;
    }

    // ── Split state ────────────────────────────────────────────────────────
    // Keyed by source partition id; holds everything needed to complete Phase 2.
    private sealed record SplitInProgress(
        int TargetPartitionId,
        TaskCompletionSource<(RaftOperationStatus Status, long Generation)>? Completion);

    private readonly Dictionary<int, SplitInProgress> _pendingSplits = new();

    // ── Merge state ────────────────────────────────────────────────────────
    // Keyed by source (Draining) partition id; holds the survivor id and caller TCS.
    private sealed record MergeInProgress(
        int SurvivorPartitionId,
        TaskCompletionSource<(RaftOperationStatus Status, long Generation)>? Completion);

    private readonly Dictionary<int, MergeInProgress> _pendingMerges = new();

    // Exposed so tests can await clean loop exit after Stop().
    internal Task LoopTask => _loop;

    public RaftSystemCoordinator(RaftManager manager, ILogger<IRaft> logger)
    {
        this.manager = manager;
        this.logger = logger;
        _loop = Task.Run(RunAsync);
    }

    /// <summary>
    /// Enqueues a request for serial processing. Safe to call from any thread.
    /// </summary>
    internal void Send(RaftSystemRequest request) =>
        _channel.Writer.TryWrite(request);

    /// <summary>
    /// Returns a <see cref="Task"/> that completes only after all requests
    /// currently in the channel have been processed by the background loop.
    /// Used by tests instead of a fixed <c>Task.Delay</c>.
    /// </summary>
    internal Task DrainAsync()
    {
        TaskCompletionSource tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
        _drainQueue.Enqueue(tcs);
        _channel.Writer.TryWrite(new RaftSystemRequest(RaftSystemRequestType.DrainSentinel));
        return tcs.Task;
    }

    // ── Background loop ────────────────────────────────────────────────────

    private async Task RunAsync()
    {
        ChannelReader<RaftSystemRequest> reader = _channel.Reader;

        try
        {
            while (await reader.WaitToReadAsync(_cts.Token).ConfigureAwait(false))
            {
                while (reader.TryRead(out RaftSystemRequest? request))
                {
                    try
                    {
                        await Receive(request, _cts.Token).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException)
                    {
                        return;
                    }
                    catch (Exception ex)
                    {
                        logger.LogError(
                            "[RaftSystemCoordinator] Unhandled exception processing {Type}: {Message}\n{StackTrace}",
                            request.Type, ex.Message, ex.StackTrace);
                    }
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Graceful shutdown
        }
        catch (Exception ex)
        {
            logger.LogError(
                "[RaftSystemCoordinator] RunAsync faulted: {Message}\n{StackTrace}",
                ex.Message, ex.StackTrace);
        }

        // Drain any messages written before the channel was completed.
        // The token is already cancelled at this point so Receive will return
        // quickly without starting any new replication or split work.
        while (reader.TryRead(out RaftSystemRequest? remaining))
        {
            try
            {
                await Receive(remaining, _cts.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) { break; }
            catch (Exception ex)
            {
                logger.LogError(
                    "[RaftSystemCoordinator] drain {Type}: {Message}",
                    remaining.Type, ex.Message);
            }
        }
    }

    // ── Message handler ────────────────────────────────────────────────────

    private async Task Receive(RaftSystemRequest message, CancellationToken cancellationToken)
    {
        if (cancellationToken.IsCancellationRequested)
            return;
        switch (message.Type)
        {
            case RaftSystemRequestType.ConfigRestored:
            {
                if (message.LogData is null)
                {
                    logger.LogWarning("Restored message is null");
                    return;
                }

                RaftSystemMessage systemMessage = Unserialize(message.LogData);
                systemConfiguration[systemMessage.Key] = systemMessage.Value;
                logger.LogInformation("Restored system configuration: {Key}", systemMessage.Key);

                if (systemMessage.Key == RaftSystemConfigKeys.Members)
                    ApplyMembershipFromCache();
            }
            break;

            case RaftSystemRequestType.ConfigReplicated:
            {
                if (message.LogData is null)
                {
                    logger.LogWarning("Replication message is null");
                    return;
                }

                RaftSystemMessage systemMessage = Unserialize(message.LogData);
                systemConfiguration[systemMessage.Key] = systemMessage.Value;
                logger.LogInformation("Replicated system configuration: {Key}", systemMessage.Key);

                // Live replication: dispatch to the correct subsystem based on the key.
                // Followers must never attempt to drive Phase 2 commits, and leaders already
                // track in-progress operations via _pendingSplits/_pendingMerges.
                if (systemMessage.Key == RaftSystemConfigKeys.Partitions)
                    InitializePartitions(crashRecovery: false);
                else if (systemMessage.Key == RaftSystemConfigKeys.Members)
                    ApplyMembershipFromCache();
            }
            break;

            case RaftSystemRequestType.LeaderChanged:
                leaderNode = message.LeaderNode;

                if (string.IsNullOrEmpty(leaderNode))
                {
                    logger.LogInformation("Waiting for leader on system partition...");
                    return;
                }

                if (manager.LocalEndpoint == leaderNode)
                    await TrySetInitialPartitions(cancellationToken).ConfigureAwait(false);
                else
                    // Follower path: just apply the current map. Crash-recovery re-enqueuing
                    // (SplitPartitionCommit / MergePartitionCommit) is the new leader's
                    // responsibility — it runs via RestoreCompleted after WAL replay, before
                    // leadership is determined. Running it here on every leader election would
                    // register stale _pendingSplits/_pendingMerges entries on followers that
                    // would cause double-commits if this node later wins leadership.
                    InitializePartitions(crashRecovery: false);
                break;

            case RaftSystemRequestType.SplitPartition:
                await TrySplitPartition(message.PartitionId, message.SplitPlan, message.Completion, cancellationToken).ConfigureAwait(false);
                break;

            case RaftSystemRequestType.SplitPartitionCommit:
                await TrySplitPartitionCommit(message.PartitionId, cancellationToken).ConfigureAwait(false);
                break;

            case RaftSystemRequestType.MergePartition:
                await TryMergePartitions(message.MergePlan!, message.Completion, cancellationToken).ConfigureAwait(false);
                break;

            case RaftSystemRequestType.MergePartitionCommit:
                await TryMergePartitionCommit(message.PartitionId, cancellationToken).ConfigureAwait(false);
                break;

            case RaftSystemRequestType.RestoreCompleted:
                // WAL restore finished: apply the persisted map and run crash-recovery
                // re-enqueuing to resume any Splitting/Draining phases left incomplete
                // before the previous shutdown.
                InitializePartitions(crashRecovery: true);
                break;

            case RaftSystemRequestType.CreatePartition:
                await TryCreatePartition(message, cancellationToken).ConfigureAwait(false);
                break;

            case RaftSystemRequestType.RemovePartition:
                await TryRemovePartition(message, cancellationToken).ConfigureAwait(false);
                break;

            case RaftSystemRequestType.AddMember:
                await TryAddMember(message, cancellationToken).ConfigureAwait(false);
                break;

            case RaftSystemRequestType.PromoteMember:
                await TryPromoteMember(message, cancellationToken).ConfigureAwait(false);
                break;

            case RaftSystemRequestType.RemoveMember:
                await TryRemoveMember(message, cancellationToken).ConfigureAwait(false);
                break;

            case RaftSystemRequestType.ApplyGossipRoster:
                ApplyGossipRoster(message);
                break;

            case RaftSystemRequestType.DrainSentinel:
                if (_drainQueue.TryDequeue(out TaskCompletionSource? tcs))
                    tcs.TrySetResult();
                break;

            default:
                throw new NotImplementedException($"Unhandled RaftSystemRequestType: {message.Type}");
        }
    }

    // ── Partition helpers ──────────────────────────────────────────────────

    private Task<RaftReplicationResult> Replicate(string type, byte[] data, bool autoCommit, CancellationToken ct) =>
        ReplicateOverride is { } fn
            ? fn(type, data, autoCommit, ct)
            : manager.ReplicateSystemLogs(type, data, autoCommit, ct);

    private Task<RaftReplicationResult> ReplicateCheckpointForPartition(int partitionId, CancellationToken ct) =>
        ReplicateCheckpointOverride is { } fn
            ? fn(partitionId, ct)
            : manager.ReplicateCheckpoint(partitionId, ct);

    private void StartPartitions(List<RaftPartitionRange> ranges) =>
        (StartPartitionsOverride ?? manager.StartUserPartitions)(ranges);

    /// <param name="crashRecovery">
    /// When <see langword="true"/>, re-enqueues Phase 2 for any Splitting or Draining
    /// partition found in the map (crash-recovery path). Must only be set when this node
    /// is becoming the leader (<c>LeaderChanged</c>) or completing WAL restore
    /// (<c>RestoreCompleted</c>). Must be <see langword="false"/> for live replication
    /// events (<c>ConfigReplicated</c>) because followers must never drive Phase 2 commits,
    /// and leaders already track in-progress operations via <c>_pendingSplits</c> /
    /// <c>_pendingMerges</c>.
    /// </param>
    private void InitializePartitions(bool crashRecovery = false)
    {
        if (!systemConfiguration.TryGetValue(RaftSystemConfigKeys.Partitions, out string? partitions))
        {
            // Expected transient on a node whose systemConfiguration cache is not yet
            // warm: a follower may process LeaderChanged before the map has been
            // replicated to it, and a fresh node fires RestoreCompleted with an empty
            // WAL. No partitions are started here, but this self-heals — the leader
            // re-asserts the map on every LeaderChanged (see TrySetInitialPartitions),
            // so a Committed map entry arrives shortly via ConfigReplicated and drives
            // InitializePartitions again. Logged at Debug to avoid false alarms.
            logger.LogDebug("InitializePartitions: partition map not yet available in system configuration; awaiting replication");
            return;
        }

        RaftPartitionMap? map = JsonSerializer.Deserialize<RaftPartitionMap>(partitions);
        if (map is null)
        {
            logger.LogError("InitializePartitions: Failed to parse partition map: {Partitions}", partitions);
            return;
        }

        StartPartitions(map.Partitions);

        // On restart, any Removed tombstone may represent a crash before DeletePartitionWAL ran.
        // Re-attempt reclamation now; the call is idempotent on all three WAL adapters.
        foreach (RaftPartitionRange range in map.Partitions)
        {
            if (range.State == RaftPartitionState.Removed)
                manager.WalAdapter.DeletePartitionWAL(range.PartitionId);
        }

        if (!crashRecovery)
            return;

        // Crash recovery: if Phase 1 was committed but Phase 2 was not, the map will
        // contain Splitting pairs.  Re-enqueue Phase 2 so the leader can complete them.
        List<RaftPartitionRange> splitting = map.Partitions
            .Where(r => r.State == RaftPartitionState.Splitting)
            .ToList();

        foreach (RaftPartitionRange target in splitting.Where(r => r.Generation == 1))
        {
            // For HashRange splits: source.EndRange + 1 == target.StartRange after Phase 1 shrink.
            // For Unrouted splits: both source and target have StartRange = EndRange = 0, so the
            // range-adjacency condition (source.EndRange + 1 == 0) can never be satisfied.
            // Match by routing mode instead — any other Splitting Unrouted entry is the source.
            RaftPartitionRange? source = target.RoutingMode == RaftRoutingMode.HashRange
                ? splitting.FirstOrDefault(r =>
                    r.PartitionId != target.PartitionId &&
                    r.EndRange + 1 == target.StartRange)
                : splitting.FirstOrDefault(r =>
                    r.PartitionId != target.PartitionId &&
                    r.RoutingMode == RaftRoutingMode.Unrouted);

            if (source is not null && !_pendingSplits.ContainsKey(source.PartitionId))
            {
                _pendingSplits[source.PartitionId] = new SplitInProgress(target.PartitionId, null);
                Send(new RaftSystemRequest(RaftSystemRequestType.SplitPartitionCommit, source.PartitionId));
            }
        }

        // Crash recovery: if Phase 1 of a merge was committed but Phase 2 was not, the map
        // will contain a Draining partition with no _pendingMerges entry.  Pair it with its
        // adjacent Active survivor and re-enqueue Phase 2.
        foreach (RaftPartitionRange src in map.Partitions.Where(r => r.State == RaftPartitionState.Draining))
        {
            if (_pendingMerges.ContainsKey(src.PartitionId))
                continue;

            // For HashRange: find the adjacent Active partition (left or right neighbor).
            // For Unrouted: find any other Active Unrouted partition.
            // Both sides are checked; the first match wins — either is a valid merge target.
            //
            // DESIGN LIMITATION (Unrouted): with multiple Active Unrouted partitions,
            // FirstOrDefault picks whichever appears first in the persisted list — not
            // necessarily the partition that was the intended survivor before the crash.
            // The correct fix is to store the survivor's id on the Draining entry itself
            // (e.g., a MergeSurvivorId field on RaftPartitionRange) so crash recovery can
            // reconstruct the original plan exactly.  Until that field exists, Unrouted
            // crash recovery is best-effort: the chosen survivor is stable (list order is
            // deterministic within a map version) but may differ from the original intent.
            // HashRange is not affected — adjacency uniquely identifies the survivor.
            RaftPartitionRange? survivor = src.RoutingMode == RaftRoutingMode.HashRange
                ? map.Partitions.FirstOrDefault(r =>
                    r.State == RaftPartitionState.Active &&
                    r.RoutingMode == RaftRoutingMode.HashRange &&
                    (src.EndRange + 1 == r.StartRange || r.EndRange + 1 == src.StartRange))
                : map.Partitions.FirstOrDefault(r =>
                    r.State == RaftPartitionState.Active &&
                    r.RoutingMode == RaftRoutingMode.Unrouted &&
                    r.PartitionId != src.PartitionId);

            if (survivor is null)
            {
                logger.LogWarning(
                    "InitializePartitions: Draining partition {Id} has no adjacent Active survivor; merge cannot be resumed",
                    src.PartitionId);
                continue;
            }

            logger.LogInformation(
                "InitializePartitions: Re-enqueuing MergePartitionCommit for stuck Draining partition {Src} → survivor {Surv}",
                src.PartitionId, survivor.PartitionId);

            _pendingMerges[src.PartitionId] = new MergeInProgress(survivor.PartitionId, null);
            Send(new RaftSystemRequest(RaftSystemRequestType.MergePartitionCommit, src.PartitionId));
        }
    }

    private async Task TrySetInitialPartitions(CancellationToken cancellationToken)
    {
        List<RaftPartitionRange> initialRanges;

        if (systemConfiguration.TryGetValue(RaftSystemConfigKeys.Partitions, out string? partitions))
        {
            RaftPartitionMap? existingMap = JsonSerializer.Deserialize<RaftPartitionMap>(partitions);
            if (existingMap is not null)
            {
                // Re-assert the authoritative map to the rest of the cluster on every
                // leadership change. A follower that joined or restarted with an empty WAL
                // only learns the partition map through log replication; if its
                // LeaderChanged is processed before catch-up — or if this leader already
                // held the map and therefore never (re)created it — that follower's
                // InitializePartitions finds an empty systemConfiguration cache, logs
                // "Failed to get partitions from system configuration", and starts no
                // partitions, leaving the cluster stuck. Re-replicating the existing map
                // guarantees every follower eventually receives a fresh Committed entry
                // (→ ConfigReplicated → InitializePartitions) and converges.
                //
                // Best-effort and idempotent: the payload is byte-identical to what is
                // already committed, so followers simply overwrite their cache with the
                // same content. If we are no longer the leader the write is rejected and
                // the genuine leader will re-assert on its own LeaderChanged.
                RaftSystemMessage reassert = new()
                {
                    Key = RaftSystemConfigKeys.Partitions,
                    Value = partitions
                };

                try
                {
                    RaftReplicationResult result = await Replicate(
                        RaftSystemConfig.RaftLogType,
                        Serialize(reassert),
                        true,
                        cancellationToken
                    ).ConfigureAwait(false);

                    if (result.Status != RaftOperationStatus.Success)
                        logger.LogWarning(
                            "[RaftSystemCoordinator] Failed to re-assert existing partition map to followers: {Status}",
                            result.Status);
                }
                catch (OperationCanceledException)
                {
                    logger.LogWarning("[RaftSystemCoordinator] TrySetInitialPartitions re-assert aborted on shutdown");
                }

                StartPartitions(existingMap.Partitions);
                await TrySeedInitialMembership(cancellationToken).ConfigureAwait(false);
                return;
            }
        }

        initialRanges = DivideIntoRanges(manager.Configuration.InitialPartitions);

        // MapVersion starts at 1; bumped on every subsequent mutation.
        RaftPartitionMap newMap = new() { MapVersion = 1, Partitions = initialRanges };

        RaftSystemMessage message = new()
        {
            Key = RaftSystemConfigKeys.Partitions,
            Value = JsonSerializer.Serialize(newMap)
        };

        for (int i = 0; i < MaxRetries; i++)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                logger.LogWarning("[RaftSystemCoordinator] TrySetInitialPartitions aborted on shutdown");
                return;
            }

            RaftReplicationResult result = await Replicate(
                RaftSystemConfig.RaftLogType,
                Serialize(message),
                true,
                cancellationToken
            ).ConfigureAwait(false);

            if (result.Status != RaftOperationStatus.Success)
            {
                logger.LogWarning(
                    "Failed to replicate initial partitions {Status} {LogIndex} Retry={Retry}",
                    result.Status, result.LogIndex, i);

                // NodeIsNotLeader means another node won leadership during init.
                // Defer immediately — the new leader will drive TrySetInitialPartitions.
                if (result.Status == RaftOperationStatus.NodeIsNotLeader)
                    return;

                // Transient error: retry with backoff.  After MaxRetries the node cannot
                // start without an initial partition map, so terminate.
                try { await Task.Delay(RetryDelay, cancellationToken).ConfigureAwait(false); }
                catch (OperationCanceledException)
                {
                    logger.LogWarning("[RaftSystemCoordinator] TrySetInitialPartitions delay aborted on shutdown");
                    return;
                }
                if (i <= 8)
                    continue;

                logger.LogError(
                    "Cannot continue without initial partitions {Status} {LogIndex}",
                    result.Status, result.LogIndex);
                Environment.Exit(1);
                return;
            }

            logger.LogInformation(
                "Successfully replicated initial partitions {Status} {LogIndex}",
                result.Status, result.LogIndex);
            break;
        }

        // Keep local cache in sync so any coordinator operation that is already queued
        // behind this LeaderChanged (e.g. SplitPartition, CreatePartition) finds the map
        // immediately rather than waiting for the async ConfigReplicated callback.
        systemConfiguration[RaftSystemConfigKeys.Partitions] = message.Value;
        StartPartitions(initialRanges);
        await TrySeedInitialMembership(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Phase 1 of the two-phase split: marks both source and new-target as
    /// <see cref="RaftPartitionState.Splitting"/>, replicates the map, then
    /// either auto-enqueues Phase 2 (when <see cref="RaftSplitPlan.AutoCommit"/>
    /// is true) or leaves the split pending for the caller to commit explicitly.
    /// </summary>
    private async Task TrySplitPartition(
        int partitionId,
        RaftSplitPlan? plan,
        TaskCompletionSource<(RaftOperationStatus Status, long Generation)>? completion,
        CancellationToken cancellationToken)
    {
        if (!systemConfiguration.TryGetValue(RaftSystemConfigKeys.Partitions, out string? partitions))
        {
            logger.LogError("TrySplitPartition: Failed to get partitions from system configuration");
            completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        RaftPartitionMap? map = JsonSerializer.Deserialize<RaftPartitionMap>(partitions);
        if (map is null)
        {
            logger.LogError("TrySplitPartition: Failed to parse partition map {Partitions}", partitions);
            completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        List<RaftPartitionRange> ranges = map.Partitions;

        RaftPartitionRange? partitionRange = ranges.FirstOrDefault(r => r.PartitionId == partitionId);
        if (partitionRange is null)
        {
            logger.LogError("TrySplitPartition: Couldn't find partition range {Partition}", partitionId);
            completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        // Idempotency guard: if Phase 1 already committed for this source, reject the
        // duplicate rather than spawning a second overlapping split.
        if (partitionRange.State == RaftPartitionState.Splitting)
        {
            logger.LogWarning(
                "TrySplitPartition: Partition {Id} is already in Splitting state; rejecting duplicate request",
                partitionId);
            completion?.TrySetResult((RaftOperationStatus.Errored, partitionRange.Generation));
            return;
        }

        // A Draining partition is mid-merge and must not be split — that would create a
        // sub-range from a partition scheduled for removal.  A Removed partition is a
        // tombstone and no longer owns any range.
        if (partitionRange.State != RaftPartitionState.Active)
        {
            logger.LogError(
                "TrySplitPartition: Partition {Id} cannot be split (State={State})",
                partitionId, partitionRange.State);
            completion?.TrySetResult((RaftOperationStatus.Errored, partitionRange.Generation));
            return;
        }

        // Resolve target partition id: explicit plan value if non-zero, else auto-assign.
        RaftPartitionRange? maxPartition = ranges.MaxBy(r => r.PartitionId);
        if (maxPartition is null)
        {
            logger.LogError("TrySplitPartition: Couldn't find next partition");
            completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        int targetPartitionId = (plan?.TargetPartitionId > 0)
            ? plan.TargetPartitionId
            : maxPartition.PartitionId + 1;

        if (ranges.Any(r => r.PartitionId == targetPartitionId))
        {
            logger.LogError(
                "TrySplitPartition: Target partition id {Id} already exists in map",
                targetPartitionId);
            completion?.TrySetResult((RaftOperationStatus.Errored, partitionRange.Generation));
            return;
        }

        RaftRoutingMode targetRoutingMode = plan?.TargetRoutingMode ?? RaftRoutingMode.HashRange;

        // Capture the original end before shrinking.
        int originalEnd = partitionRange.EndRange;

        int splitBoundary;
        if (targetRoutingMode == RaftRoutingMode.HashRange)
        {
            splitBoundary = plan?.HashBoundary
                ?? (partitionRange.StartRange + (partitionRange.EndRange - partitionRange.StartRange) / 2);

            // Boundary must split the range into two non-empty halves.
            // splitBoundary <= StartRange → source gets an inverted range (EndRange < StartRange).
            // splitBoundary > EndRange    → target starts beyond the source's current end.
            // StartRange == EndRange      → midpoint == StartRange, same as the <= case.
            if (splitBoundary <= partitionRange.StartRange || splitBoundary > partitionRange.EndRange)
            {
                logger.LogError(
                    "TrySplitPartition: HashBoundary {B} is outside ({S},{E}]",
                    splitBoundary, partitionRange.StartRange, partitionRange.EndRange);
                completion?.TrySetResult((RaftOperationStatus.Errored, partitionRange.Generation));
                return;
            }

            // Shrink the existing partition so it no longer owns [splitBoundary, originalEnd].
            partitionRange.EndRange = splitBoundary - 1;
        }
        else
        {
            // Unrouted split: source keeps its full range; new partition has no hash range.
            splitBoundary = 0;
        }

        // Phase 1: both partitions enter Splitting state.
        partitionRange.Generation++;
        partitionRange.State = RaftPartitionState.Splitting;

        RaftPartitionRange newRange = new()
        {
            PartitionId = targetPartitionId,
            StartRange  = targetRoutingMode == RaftRoutingMode.HashRange ? splitBoundary : 0,
            EndRange    = targetRoutingMode == RaftRoutingMode.HashRange ? originalEnd   : 0,
            Generation  = 1,
            State       = RaftPartitionState.Splitting,
            RoutingMode = targetRoutingMode,
        };

        ranges.Add(newRange);
        map.MapVersion++;

        RaftSystemMessage message = new()
        {
            Key   = RaftSystemConfigKeys.Partitions,
            Value = JsonSerializer.Serialize(map)
        };

        for (int i = 0; i < MaxRetries; i++)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                logger.LogWarning("[RaftSystemCoordinator] TrySplitPartition aborted on shutdown");
                return;
            }

            RaftReplicationResult result = await Replicate(
                RaftSystemConfig.RaftLogType,
                Serialize(message),
                true,
                cancellationToken
            ).ConfigureAwait(false);

            if (result.Status != RaftOperationStatus.Success)
            {
                logger.LogWarning(
                    "TrySplitPartition: Phase 1 replication failed {Status} {LogIndex} Retry={Retry}",
                    result.Status, result.LogIndex, i);

                if (result.Status != RaftOperationStatus.NodeIsNotLeader)
                {
                    try { await Task.Delay(RetryDelay, cancellationToken).ConfigureAwait(false); }
                    catch (OperationCanceledException)
                    {
                        logger.LogWarning("[RaftSystemCoordinator] TrySplitPartition delay aborted on shutdown");
                        return;
                    }
                    if (i <= 8)
                        continue;
                }

                completion?.TrySetResult((result.Status, 0));
                return;
            }

            logger.LogInformation(
                "TrySplitPartition: Phase 1 committed for partition {Partition}", partitionId);
            break;
        }

        systemConfiguration[RaftSystemConfigKeys.Partitions] = message.Value;
        StartPartitions(ranges);

        // Snapshot transfer: primary path when the application has registered an
        // IRaftStateMachineTransfer.  Errors are logged and the split falls back to
        // the log-shipping path (Phase 2 completes normally; caller ships data via ReplicateLogs).
        if (manager.StateMachineTransfer is { } transfer)
        {
            RaftSplitPlan transferPlan = new()
            {
                TargetPartitionId = targetPartitionId,
                TargetRoutingMode = targetRoutingMode,
                HashBoundary      = targetRoutingMode == RaftRoutingMode.HashRange ? splitBoundary : null,
                AutoCommit        = false,
            };
            await RunSnapshotTransferAsync(partitionId, targetPartitionId, transferPlan, transfer, cancellationToken).ConfigureAwait(false);
        }

        _pendingSplits[partitionId] = new SplitInProgress(targetPartitionId, completion);

        if (plan?.AutoCommit == true)
            Send(new RaftSystemRequest(RaftSystemRequestType.SplitPartitionCommit, partitionId));
    }

    /// <summary>
    /// Executes the snapshot-transfer step of a split (Phase 4, step 2):
    /// exports a point-in-time state snapshot from the source partition, imports it into
    /// the target partition, then replicates a checkpoint into the target partition's log
    /// so all target replicas converge on the imported state.
    /// <para>
    /// This is an optimization: on error the split remains valid and continues via the
    /// log-shipping fallback. The caller is responsible for data movement before Phase 2.
    /// </para>
    /// <para>
    /// NOTE: tail catch-up (shipping entries committed after <c>snapshotIndex</c>) requires
    /// the partition-quiesce infrastructure and is deferred.
    /// </para>
    /// </summary>
    private async Task RunSnapshotTransferAsync(
        int sourcePartitionId,
        int targetPartitionId,
        RaftSplitPlan plan,
        IRaftStateMachineTransfer transfer,
        CancellationToken cancellationToken)
    {
        long snapshotIndex = manager.WalAdapter.GetMaxLog(sourcePartitionId);

        Stream snapshot;
        try
        {
            snapshot = await transfer.ExportRange(plan, snapshotIndex, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogError(
                "TrySplitPartition: ExportRange source={Src} → target={Tgt} failed: {Message}; falling back to log-shipping",
                sourcePartitionId, targetPartitionId, ex.Message);
            return;
        }

        try
        {
            await using (snapshot.ConfigureAwait(false))
                await transfer.ImportRange(targetPartitionId, snapshot, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogError(
                "TrySplitPartition: ImportRange target={Tgt} failed: {Message}; falling back to log-shipping",
                targetPartitionId, ex.Message);
            return;
        }

        // Replicate the installed snapshot as a checkpoint so all target replicas
        // converge without replaying the full source log.
        //
        // Unlike ExportRange/ImportRange failures (where the import never happened and
        // log-shipping fallback is safe), a checkpoint failure here is categorically
        // different: the state exists only in the leader's local state machine and was
        // never written to the Raft log. Followers have an empty state machine with no
        // log to replay. There is no recovery path for them if we silently continue.
        // ImportRange must be idempotent/atomic (documented on IRaftStateMachineTransfer)
        // so a checkpoint retry is safe — the local state is unchanged.
        RaftReplicationResult checkpointResult =
            await ReplicateCheckpointForPartition(targetPartitionId, cancellationToken).ConfigureAwait(false);

        if (checkpointResult.Status != RaftOperationStatus.Success)
        {
            logger.LogWarning(
                "TrySplitPartition: ReplicateCheckpoint target={Tgt} failed ({Status}); retrying once",
                targetPartitionId, checkpointResult.Status);

            checkpointResult = await ReplicateCheckpointForPartition(targetPartitionId, cancellationToken).ConfigureAwait(false);
        }

        if (checkpointResult.Status != RaftOperationStatus.Success)
            logger.LogError(
                "TrySplitPartition: ReplicateCheckpoint target={Tgt} failed after retry ({Status}); " +
                "snapshot was imported locally but followers are diverged — manual intervention required",
                targetPartitionId, checkpointResult.Status);
        else
            logger.LogInformation(
                "TrySplitPartition: Snapshot transfer complete — source={Src} snapshotIndex={Idx}, target={Tgt} checkpoint committed",
                sourcePartitionId, snapshotIndex, targetPartitionId);
    }

    /// <summary>
    /// Phase 2 of the two-phase split: transitions both source and target from
    /// <see cref="RaftPartitionState.Splitting"/> to <see cref="RaftPartitionState.Active"/>.
    /// </summary>
    private async Task TrySplitPartitionCommit(int sourcePartitionId, CancellationToken cancellationToken)
    {
        if (!_pendingSplits.TryGetValue(sourcePartitionId, out SplitInProgress? split))
        {
            logger.LogError(
                "TrySplitPartitionCommit: No pending split for partition {Partition}", sourcePartitionId);
            return;
        }

        if (!systemConfiguration.TryGetValue(RaftSystemConfigKeys.Partitions, out string? partitions))
        {
            logger.LogError("TrySplitPartitionCommit: Failed to get partitions from system configuration");
            split.Completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        RaftPartitionMap? map = JsonSerializer.Deserialize<RaftPartitionMap>(partitions);
        if (map is null)
        {
            logger.LogError("TrySplitPartitionCommit: Failed to parse partition map");
            split.Completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        RaftPartitionRange? sourceRange = map.Partitions.FirstOrDefault(r => r.PartitionId == sourcePartitionId);
        if (sourceRange is null)
        {
            logger.LogError(
                "TrySplitPartitionCommit: Source partition {Id} not found", sourcePartitionId);
            split.Completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        RaftPartitionRange? targetRange = map.Partitions.FirstOrDefault(r => r.PartitionId == split.TargetPartitionId);
        if (targetRange is null)
        {
            logger.LogError(
                "TrySplitPartitionCommit: Target partition {Id} not found", split.TargetPartitionId);
            split.Completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        // Guard against double-execution: if a previous Phase 2 already ran (e.g. a stale
        // retry or a manual re-enqueue), both partitions will be Active rather than Splitting.
        // Re-running would corrupt generations and replicate a stale map version.
        if (sourceRange.State != RaftPartitionState.Splitting || targetRange.State != RaftPartitionState.Splitting)
        {
            logger.LogWarning(
                "TrySplitPartitionCommit: Unexpected states (source={SrcState}, target={TgtState}); skipping duplicate Phase 2",
                sourceRange.State, targetRange.State);
            _pendingSplits.Remove(sourcePartitionId);
            split.Completion?.TrySetResult((RaftOperationStatus.Success, targetRange.Generation));
            return;
        }

        // Phase 2: both partitions become Active.
        sourceRange.Generation++;
        sourceRange.State = RaftPartitionState.Active;
        targetRange.Generation++;
        targetRange.State = RaftPartitionState.Active;
        map.MapVersion++;

        RaftSystemMessage message = new()
        {
            Key   = RaftSystemConfigKeys.Partitions,
            Value = JsonSerializer.Serialize(map)
        };

        for (int i = 0; i < MaxRetries; i++)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                logger.LogWarning("[RaftSystemCoordinator] TrySplitPartitionCommit aborted on shutdown");
                return;
            }

            RaftReplicationResult result = await Replicate(
                RaftSystemConfig.RaftLogType,
                Serialize(message),
                true,
                cancellationToken
            ).ConfigureAwait(false);

            if (result.Status != RaftOperationStatus.Success)
            {
                logger.LogWarning(
                    "TrySplitPartitionCommit: Phase 2 replication failed {Status} {LogIndex} Retry={Retry}",
                    result.Status, result.LogIndex, i);

                if (result.Status != RaftOperationStatus.NodeIsNotLeader)
                {
                    try { await Task.Delay(RetryDelay, cancellationToken).ConfigureAwait(false); }
                    catch (OperationCanceledException)
                    {
                        logger.LogWarning("[RaftSystemCoordinator] TrySplitPartitionCommit delay aborted on shutdown");
                        return;
                    }
                    if (i <= 8)
                        continue;
                }

                split.Completion?.TrySetResult((result.Status, 0));
                return;
            }

            logger.LogInformation(
                "TrySplitPartitionCommit: Phase 2 committed for partition {Partition}", sourcePartitionId);
            break;
        }

        systemConfiguration[RaftSystemConfigKeys.Partitions] = message.Value;
        _pendingSplits.Remove(sourcePartitionId);
        StartPartitions(map.Partitions);
        split.Completion?.TrySetResult((RaftOperationStatus.Success, targetRange.Generation));
    }

    /// <summary>
    /// Phase 1 of the two-phase merge: marks the source partition as
    /// <see cref="RaftPartitionState.Draining"/> and replicates the updated map.
    /// Phase 2 is auto-enqueued via <see cref="RaftSystemRequestType.MergePartitionCommit"/>.
    /// </summary>
    private async Task TryMergePartitions(
        RaftMergePlan plan,
        TaskCompletionSource<(RaftOperationStatus Status, long Generation)>? completion,
        CancellationToken cancellationToken)
    {
        if (!systemConfiguration.TryGetValue(RaftSystemConfigKeys.Partitions, out string? partitions))
        {
            logger.LogError("TryMergePartitions: No partition map in system configuration");
            completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        RaftPartitionMap? map = JsonSerializer.Deserialize<RaftPartitionMap>(partitions);
        if (map is null)
        {
            logger.LogError("TryMergePartitions: Failed to parse partition map");
            completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        RaftPartitionRange? src = map.Partitions.FirstOrDefault(r => r.PartitionId == plan.SourcePartitionId);
        if (src is null)
        {
            logger.LogError("TryMergePartitions: Source partition {Id} not found", plan.SourcePartitionId);
            completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        RaftPartitionRange? surv = map.Partitions.FirstOrDefault(r => r.PartitionId == plan.SurvivorPartitionId);
        if (surv is null)
        {
            logger.LogError("TryMergePartitions: Survivor partition {Id} not found", plan.SurvivorPartitionId);
            completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        if (src.State == RaftPartitionState.Draining)
        {
            logger.LogWarning(
                "TryMergePartitions: Source partition {Id} is already Draining; rejecting duplicate merge request",
                plan.SourcePartitionId);
            completion?.TrySetResult((RaftOperationStatus.Errored, src.Generation));
            return;
        }

        if (src.State != RaftPartitionState.Active)
        {
            logger.LogError(
                "TryMergePartitions: Source partition {Id} is not Active (State={State})",
                plan.SourcePartitionId, src.State);
            completion?.TrySetResult((RaftOperationStatus.Errored, src.Generation));
            return;
        }

        if (surv.State != RaftPartitionState.Active)
        {
            logger.LogError(
                "TryMergePartitions: Survivor partition {Id} is not Active (State={State})",
                plan.SurvivorPartitionId, surv.State);
            completion?.TrySetResult((RaftOperationStatus.Errored, surv.Generation));
            return;
        }

        // Adjacency validation.
        bool adjacent =
            (src.RoutingMode == RaftRoutingMode.Unrouted && surv.RoutingMode == RaftRoutingMode.Unrouted)
            || (src.RoutingMode == RaftRoutingMode.HashRange && surv.RoutingMode == RaftRoutingMode.HashRange
                && (src.EndRange + 1 == surv.StartRange || surv.EndRange + 1 == src.StartRange));

        if (!adjacent)
        {
            logger.LogError(
                "TryMergePartitions: Partitions {Src} and {Surv} are not adjacent",
                plan.SourcePartitionId, plan.SurvivorPartitionId);
            completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        // Phase 1: mark source as Draining.
        src.State = RaftPartitionState.Draining;
        src.Generation++;
        map.MapVersion++;

        RaftSystemMessage message = new()
        {
            Key   = RaftSystemConfigKeys.Partitions,
            Value = JsonSerializer.Serialize(map)
        };

        for (int i = 0; i < MaxRetries; i++)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                logger.LogWarning("[RaftSystemCoordinator] TryMergePartitions aborted on shutdown");
                return;
            }

            RaftReplicationResult result = await Replicate(
                RaftSystemConfig.RaftLogType, Serialize(message), true, cancellationToken
            ).ConfigureAwait(false);

            if (result.Status != RaftOperationStatus.Success)
            {
                logger.LogWarning(
                    "TryMergePartitions: Phase 1 replication failed {Status} {LogIndex} Retry={Retry}",
                    result.Status, result.LogIndex, i);

                if (result.Status != RaftOperationStatus.NodeIsNotLeader)
                {
                    try { await Task.Delay(RetryDelay, cancellationToken).ConfigureAwait(false); }
                    catch (OperationCanceledException)
                    {
                        logger.LogWarning("[RaftSystemCoordinator] TryMergePartitions delay aborted on shutdown");
                        return;
                    }
                    if (i <= 8) continue;
                }

                completion?.TrySetResult((result.Status, 0));
                return;
            }

            logger.LogInformation(
                "TryMergePartitions: Phase 1 committed — source {Src} is Draining",
                plan.SourcePartitionId);
            break;
        }

        systemConfiguration[RaftSystemConfigKeys.Partitions] = message.Value;
        StartPartitions(map.Partitions);

        _pendingMerges[plan.SourcePartitionId] = new MergeInProgress(plan.SurvivorPartitionId, completion);
        Send(new RaftSystemRequest(RaftSystemRequestType.MergePartitionCommit, plan.SourcePartitionId));
    }

    /// <summary>
    /// Phase 2 of the two-phase merge: absorbs the source range into the survivor,
    /// marks the source as <see cref="RaftPartitionState.Removed"/>, replicates,
    /// then stops the source partition and reclaims its WAL.
    /// </summary>
    private async Task TryMergePartitionCommit(int sourcePartitionId, CancellationToken cancellationToken)
    {
        if (!_pendingMerges.TryGetValue(sourcePartitionId, out MergeInProgress? merge))
        {
            logger.LogError(
                "TryMergePartitionCommit: No pending merge for source partition {Id}", sourcePartitionId);
            return;
        }

        if (!systemConfiguration.TryGetValue(RaftSystemConfigKeys.Partitions, out string? partitions))
        {
            logger.LogError("TryMergePartitionCommit: No partition map in system configuration");
            merge.Completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        RaftPartitionMap? map = JsonSerializer.Deserialize<RaftPartitionMap>(partitions);
        if (map is null)
        {
            logger.LogError("TryMergePartitionCommit: Failed to parse partition map");
            merge.Completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        RaftPartitionRange? src = map.Partitions.FirstOrDefault(r => r.PartitionId == sourcePartitionId);
        if (src is null)
        {
            logger.LogError("TryMergePartitionCommit: Source partition {Id} not found", sourcePartitionId);
            merge.Completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        RaftPartitionRange? surv = map.Partitions.FirstOrDefault(r => r.PartitionId == merge.SurvivorPartitionId);
        if (surv is null)
        {
            logger.LogError(
                "TryMergePartitionCommit: Survivor partition {Id} not found", merge.SurvivorPartitionId);
            merge.Completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        // Guard against double-execution: if a previous Phase 2 already ran (e.g. crash
        // recovery re-enqueued MergePartitionCommit after Phase 2 had already committed),
        // the source will be Removed rather than Draining.  Re-running would corrupt
        // generations and re-expand the survivor range.
        if (src.State != RaftPartitionState.Draining)
        {
            logger.LogWarning(
                "TryMergePartitionCommit: Source partition {Id} is not Draining (State={State}); skipping duplicate Phase 2",
                sourcePartitionId, src.State);
            _pendingMerges.Remove(sourcePartitionId);
            merge.Completion?.TrySetResult((RaftOperationStatus.Success, surv.Generation));
            return;
        }

        // Phase 2: survivor absorbs source range; source is removed.
        if (surv.RoutingMode == RaftRoutingMode.HashRange)
        {
            surv.StartRange = Math.Min(surv.StartRange, src.StartRange);
            surv.EndRange   = Math.Max(surv.EndRange,   src.EndRange);
        }

        surv.Generation++;
        surv.State = RaftPartitionState.Active;

        src.State = RaftPartitionState.Removed;
        src.Generation++;

        map.MapVersion++;

        RaftSystemMessage message = new()
        {
            Key   = RaftSystemConfigKeys.Partitions,
            Value = JsonSerializer.Serialize(map)
        };

        for (int i = 0; i < MaxRetries; i++)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                logger.LogWarning("[RaftSystemCoordinator] TryMergePartitionCommit aborted on shutdown");
                return;
            }

            RaftReplicationResult result = await Replicate(
                RaftSystemConfig.RaftLogType, Serialize(message), true, cancellationToken
            ).ConfigureAwait(false);

            if (result.Status != RaftOperationStatus.Success)
            {
                logger.LogWarning(
                    "TryMergePartitionCommit: Phase 2 replication failed {Status} {LogIndex} Retry={Retry}",
                    result.Status, result.LogIndex, i);

                if (result.Status != RaftOperationStatus.NodeIsNotLeader)
                {
                    try { await Task.Delay(RetryDelay, cancellationToken).ConfigureAwait(false); }
                    catch (OperationCanceledException)
                    {
                        logger.LogWarning("[RaftSystemCoordinator] TryMergePartitionCommit delay aborted on shutdown");
                        return;
                    }
                    if (i <= 8) continue;
                }

                merge.Completion?.TrySetResult((result.Status, 0));
                return;
            }

            logger.LogInformation(
                "TryMergePartitionCommit: Phase 2 committed — source {Src} removed, survivor {Surv} expanded",
                sourcePartitionId, merge.SurvivorPartitionId);
            break;
        }

        systemConfiguration[RaftSystemConfigKeys.Partitions] = message.Value;
        _pendingMerges.Remove(sourcePartitionId);

        // Stop and evict the source partition, then reclaim its WAL.
        if (manager.Partitions.TryGetValue(sourcePartitionId, out RaftPartition? livePartition))
        {
            await livePartition.DrainAsync(cancellationToken).ConfigureAwait(false);
            livePartition.Stop();
            manager.Partitions.TryRemove(sourcePartitionId, out _);
        }

        manager.WalAdapter.DeletePartitionWAL(sourcePartitionId);

        // Apply the updated map (source is Removed, so StartUserPartitions will skip it).
        StartPartitions(map.Partitions);

        merge.Completion?.TrySetResult((RaftOperationStatus.Success, surv.Generation));
    }

    private async Task TryCreatePartition(RaftSystemRequest message, CancellationToken cancellationToken)
    {
        TaskCompletionSource<(RaftOperationStatus, long)>? completion = message.Completion;

        if (message.PartitionId == RaftSystemConfig.SystemPartition)
        {
            logger.LogWarning("TryCreatePartition: System partition (id=0) cannot be created");
            completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        if (!systemConfiguration.TryGetValue(RaftSystemConfigKeys.Partitions, out string? partitions))
        {
            logger.LogError("TryCreatePartition: No partition map in system configuration");
            completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        RaftPartitionMap? map = JsonSerializer.Deserialize<RaftPartitionMap>(partitions);
        if (map is null)
        {
            logger.LogError("TryCreatePartition: Failed to parse partition map");
            completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        // Idempotency: if the partition already exists with Active state, return its current generation.
        RaftPartitionRange? existing = map.Partitions.FirstOrDefault(r => r.PartitionId == message.PartitionId);
        if (existing is not null)
        {
            if (existing.State == RaftPartitionState.Active)
            {
                logger.LogInformation(
                    "TryCreatePartition: Partition {Id} already active at generation {Gen}",
                    message.PartitionId, existing.Generation);
                completion?.TrySetResult((RaftOperationStatus.Success, existing.Generation));
                return;
            }

            logger.LogError(
                "TryCreatePartition: Partition {Id} exists with non-active state {State}",
                message.PartitionId, existing.State);
            completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        // For HashRange mode, bounds are required and must not overlap any existing HashRange entry.
        int newStart = 0, newEnd = 0;
        if (message.RoutingMode == RaftRoutingMode.HashRange)
        {
            if (message.HashRangeStart is null || message.HashRangeEnd is null)
            {
                logger.LogError("TryCreatePartition: HashRange mode requires start and end");
                completion?.TrySetResult((RaftOperationStatus.Errored, 0));
                return;
            }

            newStart = message.HashRangeStart.Value;
            newEnd = message.HashRangeEnd.Value;

            if (newStart > newEnd)
            {
                logger.LogError(
                    "TryCreatePartition: Invalid HashRange [{Start},{End}]",
                    newStart, newEnd);
                completion?.TrySetResult((RaftOperationStatus.Errored, 0));
                return;
            }

            foreach (RaftPartitionRange range in map.Partitions)
            {
                if (range.RoutingMode != RaftRoutingMode.HashRange)
                    continue;
                if (newStart <= range.EndRange && newEnd >= range.StartRange)
                {
                    logger.LogError(
                        "TryCreatePartition: [{Start},{End}] overlaps partition {Id} [{RStart},{REnd}]",
                        newStart, newEnd, range.PartitionId, range.StartRange, range.EndRange);
                    completion?.TrySetResult((RaftOperationStatus.Errored, 0));
                    return;
                }
            }
        }

        map.Partitions.Add(new RaftPartitionRange
        {
            PartitionId = message.PartitionId,
            StartRange = newStart,
            EndRange = newEnd,
            Generation = 1,
            State = RaftPartitionState.Active,
            RoutingMode = message.RoutingMode
        });
        map.MapVersion++;

        RaftSystemMessage sysMessage = new()
        {
            Key = RaftSystemConfigKeys.Partitions,
            Value = JsonSerializer.Serialize(map)
        };

        for (int i = 0; i < MaxRetries; i++)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                logger.LogWarning("[RaftSystemCoordinator] TryCreatePartition aborted on shutdown");
                completion?.TrySetCanceled(cancellationToken);
                return;
            }

            RaftReplicationResult result = await Replicate(
                RaftSystemConfig.RaftLogType,
                Serialize(sysMessage),
                true,
                cancellationToken
            ).ConfigureAwait(false);

            if (result.Status != RaftOperationStatus.Success)
            {
                logger.LogWarning(
                    "TryCreatePartition: Replication failed {Status} {LogIndex} Retry={Retry}",
                    result.Status, result.LogIndex, i);

                if (result.Status != RaftOperationStatus.NodeIsNotLeader)
                {
                    try { await Task.Delay(RetryDelay, cancellationToken).ConfigureAwait(false); }
                    catch (OperationCanceledException)
                    {
                        logger.LogWarning("[RaftSystemCoordinator] TryCreatePartition delay aborted on shutdown");
                        completion?.TrySetCanceled(cancellationToken);
                        return;
                    }
                    if (i <= 8)
                        continue;
                }

                completion?.TrySetResult((result.Status, 0));
                return;
            }

            break;
        }

        // Keep local cache in sync so subsequent coordinator operations on this node see the new map.
        systemConfiguration[RaftSystemConfigKeys.Partitions] = sysMessage.Value;
        StartPartitions(map.Partitions);
        completion?.TrySetResult((RaftOperationStatus.Success, 1));
    }

    private async Task TryRemovePartition(RaftSystemRequest message, CancellationToken cancellationToken)
    {
        TaskCompletionSource<(RaftOperationStatus, long)>? completion = message.Completion;
        int partitionId = message.PartitionId;

        if (partitionId == RaftSystemConfig.SystemPartition)
        {
            logger.LogWarning("TryRemovePartition: System partition (id=0) cannot be removed");
            completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        if (!systemConfiguration.TryGetValue(RaftSystemConfigKeys.Partitions, out string? partitions))
        {
            logger.LogError("TryRemovePartition: No partition map in system configuration");
            completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        RaftPartitionMap? map = JsonSerializer.Deserialize<RaftPartitionMap>(partitions);
        if (map is null)
        {
            logger.LogError("TryRemovePartition: Failed to parse partition map");
            completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        RaftPartitionRange? entry = map.Partitions.FirstOrDefault(r => r.PartitionId == partitionId);
        if (entry is null)
        {
            logger.LogError("TryRemovePartition: Partition {Id} not found in partition map", partitionId);
            completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        // Idempotency: already removed by a prior call.
        if (entry.State == RaftPartitionState.Removed)
        {
            logger.LogInformation("TryRemovePartition: Partition {Id} already removed", partitionId);
            completion?.TrySetResult((RaftOperationStatus.Success, entry.Generation));
            // Re-attempt WAL reclamation in case a prior crash prevented it.
            manager.WalAdapter.DeletePartitionWAL(partitionId);
            return;
        }

        // A partition in Splitting or Draining is mid-protocol. Removing it would orphan the
        // paired target (Splitting) or bypass MergePartitionCommit (Draining). The caller must
        // complete or abort the pending phase-transition before removing.
        if (entry.State is RaftPartitionState.Splitting or RaftPartitionState.Draining)
        {
            logger.LogError(
                "TryRemovePartition: Partition {Id} is in {State} state; complete or abort the pending phase first",
                partitionId, entry.State);
            completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        // Mark Removed, bump generation and map version, then replicate.
        entry.State = RaftPartitionState.Removed;
        entry.Generation++;
        map.MapVersion++;

        RaftSystemMessage sysMessage = new()
        {
            Key = RaftSystemConfigKeys.Partitions,
            Value = JsonSerializer.Serialize(map)
        };

        for (int i = 0; i < MaxRetries; i++)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                logger.LogWarning("[RaftSystemCoordinator] TryRemovePartition aborted on shutdown");
                completion?.TrySetCanceled(cancellationToken);
                return;
            }

            RaftReplicationResult result = await Replicate(
                RaftSystemConfig.RaftLogType,
                Serialize(sysMessage),
                true,
                cancellationToken
            ).ConfigureAwait(false);

            if (result.Status != RaftOperationStatus.Success)
            {
                logger.LogWarning(
                    "TryRemovePartition: Replication failed {Status} {LogIndex} Retry={Retry}",
                    result.Status, result.LogIndex, i);

                if (result.Status != RaftOperationStatus.NodeIsNotLeader)
                {
                    try { await Task.Delay(RetryDelay, cancellationToken).ConfigureAwait(false); }
                    catch (OperationCanceledException)
                    {
                        logger.LogWarning("[RaftSystemCoordinator] TryRemovePartition delay aborted on shutdown");
                        completion?.TrySetCanceled(cancellationToken);
                        return;
                    }
                    if (i <= 8)
                        continue;
                }

                completion?.TrySetResult((result.Status, 0));
                return;
            }

            break;
        }

        // Keep local cache in sync so subsequent coordinator operations on this node see the new map.
        systemConfiguration[RaftSystemConfigKeys.Partitions] = sysMessage.Value;

        // Tombstone is now committed. Drain and stop the local partition instance,
        // evict it from the manager dictionary, then reclaim its WAL storage.
        if (manager.Partitions.TryGetValue(partitionId, out RaftPartition? livePartition))
        {
            await livePartition.DrainAsync(cancellationToken).ConfigureAwait(false);
            livePartition.Stop();
            manager.Partitions.TryRemove(partitionId, out _);
        }

        manager.WalAdapter.DeletePartitionWAL(partitionId);

        logger.LogInformation(
            "TryRemovePartition: Partition {Id} removed and WAL reclaimed", partitionId);

        // Fire OnPartitionMapChanged on the leader, matching the contract on followers
        // (who fire it via ConfigReplicated → StartUserPartitions). The partition has
        // already been evicted from manager.Partitions, so StartUserPartitions skips the
        // Removed tombstone and the event snapshot reflects the post-remove state.
        StartPartitions(map.Partitions);

        completion?.TrySetResult((RaftOperationStatus.Success, entry.Generation));
    }

    // ── Membership seeding ─────────────────────────────────────────────────

    /// <summary>
    /// Called by the P0 leader after the partition map is established.
    /// If no <c>members</c> record exists yet, writes the current discovery set
    /// (all peers plus self) as the initial roster, all <see cref="ClusterMemberRole.Voter"/>.
    /// This preserves today's static-discovery behavior on greenfield clusters while
    /// providing a committed roster that future membership changes can build on.
    /// <para>
    /// <b>Known limitation:</b> the roster captures only the nodes visible to the P0 leader via
    /// <see cref="IDiscovery.GetNodes"/> at the instant of seeding. A node that registers in
    /// discovery after the seed is committed is absent from the roster. Because role gating
    /// suppresses elections/votes for non-voters, that node will be
    /// <see cref="ClusterMemberRole.NotMember"/> and cannot participate until it is explicitly
    /// added via the Join RPC.
    /// </para>
    /// </summary>
    private async Task TrySeedInitialMembership(CancellationToken cancellationToken)
    {
        if (systemConfiguration.ContainsKey(RaftSystemConfigKeys.Members) || _cachedMembership.MembershipVersion > 0)
            return;

        List<RaftNode> peers = manager.Discovery.GetNodes();

        List<ClusterMember> allMembers =
        [
            new()
            {
                Endpoint = manager.LocalEndpoint,
                NodeId = manager.LocalNodeId,
                Role = ClusterMemberRole.Voter,
                JoinedVersion = 1
            },
            // Peer NodeIds are provisional (0) because discovery only yields endpoints.
            // The Join RPC will replace these with each node's real configured NodeId when it
            // self-reports; the roster-derived peer set keys on Endpoint, not NodeId.
            // Self-exclusion and dedup guard against discovery backends that list the local
            // endpoint or return duplicates — both would corrupt quorum math.
            ..peers
                .Where(n => n.Endpoint != manager.LocalEndpoint)
                .DistinctBy(n => n.Endpoint)
                .Select(n => new ClusterMember
                {
                    Endpoint = n.Endpoint,
                    NodeId = 0,
                    Role = ClusterMemberRole.Voter,
                    JoinedVersion = 1
                })
        ];

        ClusterMembership seed = new() { MembershipVersion = 1, Members = allMembers };
        string json = JsonSerializer.Serialize(seed);

        RaftSystemMessage message = new() { Key = RaftSystemConfigKeys.Members, Value = json };

        if (cancellationToken.IsCancellationRequested)
            return;

        RaftReplicationResult result;
        try
        {
            result = await Replicate(
                RaftSystemConfig.RaftLogType, Serialize(message), true, cancellationToken
            ).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            logger.LogWarning("[RaftSystemCoordinator] TrySeedInitialMembership aborted on shutdown");
            return;
        }

        if (result.Status != RaftOperationStatus.Success)
        {
            logger.LogWarning(
                "[RaftSystemCoordinator] TrySeedInitialMembership: Failed to seed initial roster: {Status}",
                result.Status);
            return;
        }

        systemConfiguration[RaftSystemConfigKeys.Members] = json;
        _cachedMembership = seed;
        manager.RaiseMembershipChanged(seed);
        logger.LogInformation(
            "[RaftSystemCoordinator] Seeded initial membership roster with {Count} voter(s)",
            allMembers.Count);
    }

    // ── Membership helpers ─────────────────────────────────────────────────

    /// <summary>
    /// Returns the current cached roster. Returns an empty <see cref="ClusterMembership"/>
    /// (version 0, no members) when no roster has been committed yet.
    /// </summary>
    internal ClusterMembership GetMembership() => _cachedMembership;

    /// <summary>
    /// Applies a gossiped roster to the local cache when its version exceeds the locally
    /// committed version.  Called from the coordinator channel loop so it is serialized
    /// against all other cache mutations; the Raft log is never written.
    /// </summary>
    private void ApplyGossipRoster(RaftSystemRequest request)
    {
        ClusterMembership? roster = request.GossipedRoster;
        if (roster is null || roster.MembershipVersion <= _cachedMembership.MembershipVersion)
            return;

        logger.LogDebug(
            "Gossip: updating local cache from v{Old} to v{New} via peer push",
            _cachedMembership.MembershipVersion, roster.MembershipVersion);

        _cachedMembership = roster;
        manager.RaiseMembershipChanged(roster);
    }

    /// <summary>
    /// Resets the local membership cache to an empty roster.
    /// Intended for tests that need to simulate a node that missed committed membership
    /// entries, allowing the gossip anti-entropy path to be exercised in isolation.
    /// Must not be called in production code.
    /// </summary>
    internal void ResetMembershipCacheForTest() =>
        _cachedMembership = new ClusterMembership { MembershipVersion = 0, Members = [] };

    // ── Learner promotion driver ───────────────────────────────────────────────
    // Keyed by learner endpoint; records when the learner first appeared caught-up on all partitions.
    private readonly Dictionary<string, DateTimeOffset> _learnerCaughtUpSince = new();

    // Endpoints for which a terminal "below WAL floor, no transfer" signal has already been sent.
    // Prevents re-logging and re-signalling on every promotion tick.
    private readonly HashSet<string> _terminalBelowFloorEndpoints = new();

    /// <summary>
    /// Called from <c>RaftManager.UpdateNodes</c> on every <see cref="RaftTimerService"/>
    /// <c>UpdateNodes</c> tick.  Runs only when this node is the P0 leader.
    /// <para>
    /// For each Learner in the committed roster it measures the per-partition lag on every
    /// partition (system + user).  For partitions this node leads, lag is read directly from
    /// <c>lastCommitIndexes</c>.  For partitions led by another node the driver queries that
    /// node via <see cref="ICommunication.GetRemoteFollowerLag"/> — in the in-memory transport
    /// this is a direct call; the gRPC transport returns <see langword="null"/> (skip) until a
    /// dedicated RPC is added.  A learner that stays within
    /// <see cref="RaftConfiguration.LearnerPromotionLag"/> entries on <em>all</em> checked
    /// partitions for at least <see cref="RaftConfiguration.LearnerPromotionStableWindow"/> is
    /// promoted to Voter.  At most one membership change is in flight at a time.
    /// </para>
    /// </summary>
    internal async Task CheckLearnerPromotionsAsync(CancellationToken cancellationToken = default)
    {
        // Only the P0 leader runs the driver.
        bool amP0Leader = await manager.AmILeaderQuick(RaftSystemConfig.SystemPartition).ConfigureAwait(false);
        if (!amP0Leader)
        {
            _learnerCaughtUpSince.Clear();
            _terminalBelowFloorEndpoints.Clear();
            return;
        }

        // Belt-and-suspenders: a membership change is already committed or being replicated.
        if (_membershipChangePending)
            return;

        ClusterMembership roster = _cachedMembership;
        List<ClusterMember> learners = roster.Members
            .Where(m => m.Role == ClusterMemberRole.Learner)
            .ToList();

        if (learners.Count == 0)
        {
            _learnerCaughtUpSince.Clear();
            _terminalBelowFloorEndpoints.Clear();
            return;
        }

        // Collect all partition ids (system + user) to measure lag on.
        List<int> allPartitionIds = new(manager.Partitions.Count + 1) { RaftSystemConfig.SystemPartition };
        allPartitionIds.AddRange(manager.Partitions.Keys);

        DateTimeOffset now = DateTimeOffset.UtcNow;

        foreach (ClusterMember learner in learners)
        {
            string endpoint = learner.Endpoint;
            bool caughtUp = true;

            foreach (int partitionId in allPartitionIds)
            {
                bool isPartitionLeader = await manager.AmILeaderQuick(partitionId).ConfigureAwait(false);

                long leaderCommitted;
                long? learnerCommittedNullable;

                if (isPartitionLeader)
                {
                    // Happy path: we lead this partition and can read lastCommitIndexes directly.
                    leaderCommitted = await manager.GetFollowerCommittedIndexAsync(partitionId, manager.LocalEndpoint).ConfigureAwait(false);
                    if (leaderCommitted < 0)
                        continue;

                    // Nullable: null means the learner has never acked this partition at all —
                    // i.e., the node was not configured with this partition. Skip the lag check
                    // so a node with fewer partitions than the P0 leader is not blocked.
                    learnerCommittedNullable = await manager.GetFollowerCommittedIndexNullableAsync(partitionId, endpoint).ConfigureAwait(false);
                }
                else
                {
                    // This partition is led by another node. Query that node remotely so the
                    // promotion gate covers ALL partitions, not just those the P0 leader leads.
                    // If the transport returns null (gRPC before a dedicated RPC is added, or
                    // the leader is unknown/unreachable) we skip the partition — same as before.
                    string? leaderEndpoint = manager.GetPartitionLeaderEndpoint(partitionId);
                    if (string.IsNullOrEmpty(leaderEndpoint))
                        continue;

                    RaftNode leaderNode = new(leaderEndpoint);

                    // Ask the remote leader for its own committed index (follower = leaderEndpoint
                    // maps to localCommittedIndex on that node) and for the learner's lag.
                    long? remoteLeaderCommitted = await manager.Communication.GetRemoteFollowerLag(
                        manager, leaderNode, partitionId, leaderEndpoint).ConfigureAwait(false);
                    if (remoteLeaderCommitted is null || remoteLeaderCommitted.Value < 0)
                        continue;

                    leaderCommitted = remoteLeaderCommitted.Value;
                    learnerCommittedNullable = await manager.Communication.GetRemoteFollowerLag(
                        manager, leaderNode, partitionId, endpoint).ConfigureAwait(false);
                }

                // null here has two interpretations that we cannot distinguish today:
                //   (a) the learner genuinely has no assignment for this partition (future
                //       per-partition placement) — skipping is correct.
                //   (b) replication just started and the learner has not yet sent a
                //       CompleteAppendLogs ack — skipping creates an early-promotion window
                //       where the learner may be promoted before receiving a single entry on
                //       a freshly-replicated partition.
                // In the current join-all-partitions model (b) is the common case, so this
                // skip is a known gap. The impact is mitigated by PreVote (the new Voter
                // cannot win an election until its log is fresh enough) and by the backfill
                // path that replays outstanding entries after promotion. When per-partition
                // placement is introduced, callers should supply an explicit "expected
                // partition set" so (a) and (b) can be told apart and (b) blocked.
                if (learnerCommittedNullable is null)
                    continue;

                long lag = leaderCommitted - learnerCommittedNullable.Value;
                if (lag > manager.Configuration.LearnerPromotionLag)
                {
                    // If the learner is below the WAL compaction floor and no snapshot transfer
                    // is registered it can never catch up via log replay. Signal the joiner once
                    // so JoinCluster fails fast with a clear error instead of timing out.
                    if (!_terminalBelowFloorEndpoints.Contains(endpoint))
                    {
                        long floor = manager.WalAdapter.GetLastCheckpoint(partitionId);
                        if (floor > 0 && learnerCommittedNullable.Value < floor && manager.StateMachineTransfer is null)
                        {
                            string reason =
                                $"learner is below WAL compaction floor on partition {partitionId} " +
                                $"(learnerIndex={learnerCommittedNullable.Value}, floor={floor}). " +
                                "Register IRaftStateMachineTransfer to enable snapshot-based catch-up.";

                            logger.LogWarning(
                                "[RaftSystemCoordinator] Learner {Endpoint} permanently blocked: {Reason}",
                                endpoint, reason);

                            _terminalBelowFloorEndpoints.Add(endpoint);

                            // Route the terminal signal to the actual joiner. In-process (InMemoryCommunication)
                            // this calls SetJoinTerminalReason directly on the target manager. Over gRPC/REST the
                            // default ICommunication stub is a no-op (joiner times out after 60 s — a future task
                            // adds the wire notification).
                            await manager.Communication.NotifyJoinBlocked(manager, endpoint, reason, cancellationToken).ConfigureAwait(false);
                        }
                    }

                    caughtUp = false;
                    break;
                }
            }

            if (caughtUp)
            {
                if (!_learnerCaughtUpSince.TryGetValue(endpoint, out DateTimeOffset since))
                {
                    _learnerCaughtUpSince[endpoint] = now;
                    // Not stable yet — stable window starts now.
                }
                else if (now - since >= manager.Configuration.LearnerPromotionStableWindow)
                {
                    // Stable long enough — promote.
                    logger.LogInformation("[RaftSystemCoordinator] Promoting Learner {Endpoint} to Voter (stable for {Duration:g})", endpoint, now - since);
                    _learnerCaughtUpSince.Remove(endpoint);

                    Send(new RaftSystemRequest(
                        RaftSystemRequestType.PromoteMember,
                        endpoint,
                        learner.NodeId,
                        roster.MembershipVersion));

                    // Promote one at a time — exit; next tick handles remaining learners.
                    return;
                }
            }
            else
            {
                // Not caught up — reset the stable window for this learner.
                _learnerCaughtUpSince.Remove(endpoint);
            }
        }
    }

    /// <summary>
    /// P0-leader-only eviction driver: for each member whose SWIM liveness state has
    /// been <see cref="Gossip.MemberLivenessState.Dead"/> for at least
    /// <c>DeadMemberEvictionGrace</c>, commits a single <c>RemoveMember</c> entry.
    /// One eviction per call; caller retries on the next tick.
    ///
    /// <para>
    /// Only the P0 leader evicts — followers update their own liveness table but never
    /// commit membership changes.  The quorum-safety precondition in
    /// <see cref="TryRemoveMember"/> prevents eviction from draining the voter set below
    /// a viable majority.
    /// </para>
    /// </summary>
    internal async Task EvictDeadMembersAsync()
    {
        bool amP0Leader = await manager.AmILeaderQuick(RaftSystemConfig.SystemPartition).ConfigureAwait(false);
        if (!amP0Leader)
            return;

        if (_membershipChangePending)
            return;

        ClusterMembership roster = _cachedMembership;
        if (roster.MembershipVersion == 0)
            return;

        DateTimeOffset now = DateTimeOffset.UtcNow;
        TimeSpan grace = manager.Configuration.DeadMemberEvictionGrace;

        IReadOnlyList<string> evictable = manager.Liveness.GetEvictable(now, grace);
        if (evictable.Count == 0)
            return;

        foreach (string endpoint in evictable)
        {
            // Only evict members that are still in the roster.
            ClusterMember? member = roster.Members.FirstOrDefault(m => m.Endpoint == endpoint);
            if (member is null)
            {
                manager.Liveness.Remove(endpoint);
                continue;
            }

            // Don't evict self — a running node cannot be dead.
            if (endpoint == manager.LocalEndpoint)
            {
                manager.Liveness.Remove(endpoint);
                continue;
            }

            logger.LogWarning(
                "[RaftSystemCoordinator] Evicting Dead member {Endpoint} (dead > {Grace:g}); roster version {Version}",
                endpoint, grace, roster.MembershipVersion);

            Send(new RaftSystemRequest(RaftSystemRequestType.RemoveMember, endpoint, member.NodeId, roster.MembershipVersion));

            // One eviction per tick — the coordinator loop processes them serially.
            return;
        }
    }

    /// <summary>
    /// Reads the <see cref="RaftSystemConfigKeys.Members"/> entry from
    /// <c>systemConfiguration</c> and updates <see cref="_cachedMembership"/>.
    /// A no-op when the key is absent (pre-seed transient).
    /// </summary>
    private void ApplyMembershipFromCache()
    {
        if (!systemConfiguration.TryGetValue(RaftSystemConfigKeys.Members, out string? membersJson))
            return;

        ClusterMembership? membership = JsonSerializer.Deserialize<ClusterMembership>(membersJson);
        if (membership is null)
        {
            logger.LogError("ApplyMembershipFromCache: Failed to parse membership record");
            return;
        }

        // Accept only monotonically newer versions so that a slow ConfigReplicated replay
        // does not clobber a locally-updated cache that is already at a higher version.
        if (membership.MembershipVersion > _cachedMembership.MembershipVersion)
        {
            _cachedMembership = membership;
            manager.RaiseMembershipChanged(membership);
        }
    }

    /// <summary>
    /// Shared pre-flight checks for all membership mutations.
    /// Returns false and completes <paramref name="completion"/> with the appropriate
    /// failure status if the request should be rejected without replication.
    /// </summary>
    private bool ValidateMembershipRequest(
        long expectedVersion,
        TaskCompletionSource<(RaftOperationStatus Status, long Generation)>? completion)
    {
        if (_membershipChangePending)
        {
            logger.LogWarning("[RaftSystemCoordinator] Membership change rejected: another change is in flight");
            completion?.TrySetResult((RaftOperationStatus.ConcurrentMembershipChange, 0));
            return false;
        }

        if (_cachedMembership.MembershipVersion != expectedVersion)
        {
            logger.LogWarning(
                "[RaftSystemCoordinator] Membership change rejected: expected version {Expected} but current is {Current}",
                expectedVersion, _cachedMembership.MembershipVersion);
            completion?.TrySetResult((RaftOperationStatus.StaleMembership, _cachedMembership.MembershipVersion));
            return false;
        }

        return true;
    }

    /// <summary>
    /// Replicates a new membership record, updates the local cache, and resolves
    /// <paramref name="completion"/>. Handles the retry/backoff loop identically to
    /// partition-map mutations.
    /// <para>
    /// <see cref="_membershipChangePending"/> is NOT cleared here — callers wrap this call in
    /// <c>try/finally { _membershipChangePending = false; }</c> so the flag is always
    /// released even if this method throws unexpectedly.
    /// </para>
    /// </summary>
    private async Task ReplicateMembership(
        ClusterMembership newMembership,
        TaskCompletionSource<(RaftOperationStatus Status, long Generation)>? completion,
        CancellationToken cancellationToken)
    {
        string json = JsonSerializer.Serialize(newMembership);
        RaftSystemMessage sysMessage = new() { Key = RaftSystemConfigKeys.Members, Value = json };

        try
        {
            for (int i = 0; i < MaxRetries; i++)
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    completion?.TrySetCanceled(cancellationToken);
                    return;
                }

                RaftReplicationResult result = await Replicate(
                    RaftSystemConfig.RaftLogType, Serialize(sysMessage), true, cancellationToken
                ).ConfigureAwait(false);

                if (result.Status != RaftOperationStatus.Success)
                {
                    logger.LogWarning(
                        "ReplicateMembership: replication failed {Status} {LogIndex} Retry={Retry}",
                        result.Status, result.LogIndex, i);

                    if (result.Status == RaftOperationStatus.NodeIsNotLeader)
                    {
                        completion?.TrySetResult((result.Status, 0));
                        return;
                    }

                    try { await Task.Delay(RetryDelay, cancellationToken).ConfigureAwait(false); }
                    catch (OperationCanceledException)
                    {
                        logger.LogWarning("[RaftSystemCoordinator] ReplicateMembership delay aborted on shutdown");
                        return;
                    }

                    if (i <= 8) continue;

                    completion?.TrySetResult((result.Status, 0));
                    return;
                }

                break;
            }

            systemConfiguration[RaftSystemConfigKeys.Members] = json;
            _cachedMembership = newMembership;
            manager.RaiseMembershipChanged(newMembership);

            completion?.TrySetResult((RaftOperationStatus.Success, newMembership.MembershipVersion));
        }
        catch (OperationCanceledException)
        {
            completion?.TrySetCanceled(cancellationToken);
            throw;
        }
        catch (Exception)
        {
            completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            throw;
        }
    }

    /// <summary>
    /// Appends a new node as a <see cref="ClusterMemberRole.Learner"/> to the committed roster.
    /// Rejected if another change is in flight or the expected version is stale.
    /// </summary>
    private async Task TryAddMember(RaftSystemRequest message, CancellationToken cancellationToken)
    {
        TaskCompletionSource<(RaftOperationStatus, long)>? completion = message.Completion;

        if (!ValidateMembershipRequest(message.ExpectedMembershipVersion, completion))
            return;

        string endpoint = message.MemberEndpoint ?? "";

        if (string.IsNullOrEmpty(endpoint))
        {
            logger.LogWarning("TryAddMember: Endpoint is null or empty; rejecting");
            completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        if (_cachedMembership.Members.Any(m => m.Endpoint == endpoint))
        {
            // Idempotent: a previous AddMember committed but the JoinResponse was lost, so the
            // joiner retried. Treat this as success so the caller can proceed past the admission
            // loop without waiting for the 60 s timeout. Return the current version so the joiner
            // can log the correct roster version.
            logger.LogInformation("TryAddMember: Endpoint {Endpoint} is already in the roster; treating as idempotent success at version {Version}",
                endpoint, _cachedMembership.MembershipVersion);
            completion?.TrySetResult((RaftOperationStatus.Success, _cachedMembership.MembershipVersion));
            return;
        }

        long newVersion = _cachedMembership.MembershipVersion + 1;

        ClusterMembership newMembership = new()
        {
            MembershipVersion = newVersion,
            Members =
            [
                .._cachedMembership.Members,
                new ClusterMember
                {
                    Endpoint = endpoint,
                    NodeId = message.MemberNodeId,
                    Role = ClusterMemberRole.Learner,
                    JoinedVersion = newVersion
                }
            ]
        };

        _membershipChangePending = true;
        logger.LogInformation("TryAddMember: Adding {Endpoint} as Learner at version {Version}", endpoint, newVersion);

        try
        {
            await ReplicateMembership(newMembership, completion, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _membershipChangePending = false;
        }
    }

    /// <summary>
    /// Promotes a committed <see cref="ClusterMemberRole.Learner"/> to
    /// <see cref="ClusterMemberRole.Voter"/>. The node enters quorum at the commit point.
    /// </summary>
    private async Task TryPromoteMember(RaftSystemRequest message, CancellationToken cancellationToken)
    {
        TaskCompletionSource<(RaftOperationStatus, long)>? completion = message.Completion;

        if (!ValidateMembershipRequest(message.ExpectedMembershipVersion, completion))
            return;

        string endpoint = message.MemberEndpoint ?? "";

        ClusterMember? member = _cachedMembership.Members.FirstOrDefault(m => m.Endpoint == endpoint);
        if (member is null)
        {
            logger.LogError("TryPromoteMember: Endpoint {Endpoint} not found in roster", endpoint);
            completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        if (member.Role != ClusterMemberRole.Learner)
        {
            logger.LogError(
                "TryPromoteMember: Endpoint {Endpoint} is not a Learner (Role={Role})",
                endpoint, member.Role);
            completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        long newVersion = _cachedMembership.MembershipVersion + 1;

        ClusterMembership newMembership = new()
        {
            MembershipVersion = newVersion,
            Members = _cachedMembership.Members
                .Select(m => m.Endpoint == endpoint
                    ? new ClusterMember { Endpoint = m.Endpoint, NodeId = m.NodeId, Role = ClusterMemberRole.Voter, JoinedVersion = m.JoinedVersion }
                    : m)
                .ToList()
        };

        _membershipChangePending = true;
        logger.LogInformation("TryPromoteMember: Promoting {Endpoint} to Voter at version {Version}", endpoint, newVersion);

        try
        {
            await ReplicateMembership(newMembership, completion, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _membershipChangePending = false;
        }
    }

    /// <summary>
    /// Removes a node from the committed roster (graceful leave or failure-driven eviction).
    /// Quorum shrinks at the commit point; single-server safety is preserved because
    /// changes are applied one node at a time.
    /// </summary>
    private async Task TryRemoveMember(RaftSystemRequest message, CancellationToken cancellationToken)
    {
        TaskCompletionSource<(RaftOperationStatus, long)>? completion = message.Completion;

        if (!ValidateMembershipRequest(message.ExpectedMembershipVersion, completion))
            return;

        string endpoint = message.MemberEndpoint ?? "";

        ClusterMember? member = _cachedMembership.Members.FirstOrDefault(m => m.Endpoint == endpoint);
        if (member is null)
        {
            // Idempotent: the endpoint is already absent — a previous RemoveMember committed but
            // the response was lost before the caller saw it. Treat as success so the caller's
            // retry loop does not spin to timeout.
            logger.LogInformation("TryRemoveMember: Endpoint {Endpoint} is not in the roster; treating as idempotent success at version {Version}",
                endpoint, _cachedMembership.MembershipVersion);
            completion?.TrySetResult((RaftOperationStatus.Success, _cachedMembership.MembershipVersion));
            return;
        }

        // Quorum-safety precondition: at least 1 voter must remain after removal so the
        // cluster can still commit entries (single-node commit is supported — see commit 3fe6cae).
        // We do NOT gate non-Voter (Learner/Leaving) removals — those don't affect quorum.
        if (member.Role == ClusterMemberRole.Voter)
        {
            int remainingVoters = _cachedMembership.Members.Count(m => m.Role == ClusterMemberRole.Voter && m.Endpoint != endpoint);
            if (remainingVoters < 1)
            {
                // Would leave zero voters: cluster becomes permanently unavailable.
                // Return InsufficientVoters so the caller gives up immediately rather than
                // retrying — this is a permanent condition, not a transient one.
                logger.LogError("TryRemoveMember: Refusing to remove {Endpoint} — would leave {Remaining} voter(s), making the cluster unavailable",
                    endpoint, remainingVoters);
                completion?.TrySetResult((RaftOperationStatus.InsufficientVoters, 0));
                return;
            }
        }

        long newVersion = _cachedMembership.MembershipVersion + 1;

        ClusterMembership newMembership = new()
        {
            MembershipVersion = newVersion,
            Members = _cachedMembership.Members.Where(m => m.Endpoint != endpoint).ToList()
        };

        _membershipChangePending = true;
        logger.LogInformation("TryRemoveMember: Removing {Endpoint} at version {Version}", endpoint, newVersion);

        try
        {
            await ReplicateMembership(newMembership, completion, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _membershipChangePending = false;
        }
    }

    // ── Static helpers ─────────────────────────────────────────────────────

    private static List<RaftPartitionRange> DivideIntoRanges(int numberOfRanges)
    {
        int monotonicId = RaftSystemConfig.SystemPartition + 1;

        List<RaftPartitionRange> ranges = new(numberOfRanges);

        const long totalCount = (long)int.MaxValue + 1;
        long baseRangeSize = totalCount / numberOfRanges;
        long remainder = totalCount % numberOfRanges;

        long currentStart = 0;

        for (int i = 0; i < numberOfRanges; i++)
        {
            long currentRangeSize = baseRangeSize + (i < remainder ? 1 : 0);
            long currentEnd = currentStart + currentRangeSize - 1;

            ranges.Add(new()
            {
                PartitionId = monotonicId++,
                StartRange = (int)currentStart,
                EndRange = (int)currentEnd,
                Generation = 1,
                State = RaftPartitionState.Active,
                RoutingMode = RaftRoutingMode.HashRange
            });

            currentStart = currentEnd + 1;
        }

        return ranges;
    }

    private static byte[] Serialize(RaftSystemMessage message)
    {
        using MemoryStream memoryStream = new();
        message.WriteTo(memoryStream);
        return memoryStream.ToArray();
    }

    private static RaftSystemMessage Unserialize(byte[] serializedData)
    {
        using MemoryStream memoryStream = new(serializedData);
        return RaftSystemMessage.Parser.ParseFrom(memoryStream);
    }

    private int _stopped;

    /// <summary>
    /// Returns true once <see cref="Stop"/> has been called.
    /// Used by <c>RaftManager.ReceiveLeave</c> to fail-fast without posting
    /// to the coordinator channel, which is already completed at that point.
    /// </summary>
    internal bool IsStopped => _stopped != 0;

    // ── Lifecycle ──────────────────────────────────────────────────────────

    /// <summary>
    /// Signals the coordinator to stop: cancels in-flight async work immediately,
    /// then completes the channel so the background loop drains and exits.
    /// Idempotent — safe to call multiple times.
    /// </summary>
    internal void Stop()
    {
        if (Interlocked.Exchange(ref _stopped, 1) != 0)
            return;

        // Cancel first so any in-flight ReplicateSystemLogs / Task.Delay returns promptly.
        _cts.Cancel();
        _channel.Writer.TryComplete();
    }

    public void Dispose()
    {
        Stop(); // cancels _cts and completes channel; loop exits promptly
        try { _loop.Wait(TimeSpan.FromSeconds(5)); } catch { /* ignore shutdown races */ }
        _cts.Dispose();
    }
}
